//! Address grammar and shared transport types.
//!
//! * `tcp://host:port/path`            — raw TCP backend
//! * `ssh://user@host:port/path` or
//!   `[user@]host:path` (rsync-style)  — SSH backend
//! * anything else                     — local path

use anyhow::{bail, Context, Result};
use std::path::PathBuf;

pub mod ssh;
pub mod tcp;

/// A remote endpoint for SSH sessions.
#[derive(Debug, Clone)]
pub struct SshSpec {
    pub user: Option<String>,
    pub host: String,
    pub port: u16,
    /// Directory the session operates on (absolute on the remote machine).
    pub path: PathBuf,
}

/// A raw-TCP remote endpoint.
#[derive(Debug, Clone)]
pub struct TcpSpec {
    pub host: String,
    pub port: u16,
    /// Directory the session operates on, relative to the server root.
    pub path: String,
}

/// How a push/pull argument was parsed.
#[derive(Debug, Clone)]
pub enum RemoteRef {
    Ssh(SshSpec),
    Tcp(TcpSpec),
}

/// Returns true for Windows-style drive-letter paths (`C:\x`, `C:/x`).
fn looks_like_drive(s: &str) -> bool {
    let bytes = s.as_bytes();
    bytes.len() >= 2
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && (bytes.len() == 2 || bytes[2] == b'/' || bytes[2] == b'\\')
}

fn split_user_host(s: &str) -> (Option<String>, String) {
    match s.split_once('@') {
        Some((user, host)) => (Some(user.to_string()), host.to_string()),
        None => (None, s.to_string()),
    }
}

impl RemoteRef {
    pub fn parse(input: &str) -> Result<Self> {
        if looks_like_drive(input) {
            bail!("local path: {input} (use remote refs like user@host:/path or tcp://host:port/path)");
        }
        if let Some(rest) = input.strip_prefix("tcp://") {
            return parse_tcp(rest);
        }
        if let Some(rest) = input.strip_prefix("ssh://") {
            return parse_ssh_url(rest);
        }
        // `user@host:path` / `host:path` (rsync style). Note: never interpreted
        // as a port — use `--ssh-port` for that.
        let slash = input.find('/');
        let colon = input.find(':');
        match colon {
            Some(ci) if slash.is_none_or(|s| s > ci) => {
                let (user, host) = split_user_host(&input[..ci]);
                // host may still contain a user
                let (host_user, host) = split_user_host(&host);
                let user = user.or(host_user);
                if host.is_empty() || host.contains('/') {
                    bail!("cannot parse remote {input:?}");
                }
                Ok(RemoteRef::Ssh(SshSpec {
                    user,
                    host,
                    port: 22, // replaced by --ssh-port later
                    path: PathBuf::from(&input[ci + 1..]),
                }))
            }
            Some(_) => bail!("bad remote {input:?}: use tcp://user@host:port/path or user@host:/path"),
            None => bail!("not a remote reference: {input}"),
        }
    }

    pub fn path(&self) -> String {
        match self {
            RemoteRef::Ssh(s) => s.path.to_string_lossy().into_owned(),
            RemoteRef::Tcp(t) => t.path.clone(),
        }
    }
}

fn parse_tcp(rest: &str) -> Result<RemoteRef> {
    let (hostport, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i + 1..].to_string()),
        None => bail!("tcp endpoint needs a path after the port: tcp://host:port/dir"),
    };
    let (host, port) = parse_host_port(hostport)?;
    Ok(RemoteRef::Tcp(TcpSpec { host, port, path }))
}

fn parse_ssh_url(rest: &str) -> Result<RemoteRef> {
    // ssh://user@host:port/path — userinfo like https. The URL form carries
    // an absolute remote path: keep the leading '/' (it is part of the
    // syntax, not a separator), otherwise a target like /srv/data would
    // arrive relative and land inside the remote home directory.
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], PathBuf::from(&rest[i..])),
        None => bail!("full remote path missing: ssh://user@host/abs/path"),
    };
    let (user, hostport) = split_user_host(authority);
    let (host, port) = parse_host_port(&hostport)?;
    Ok(RemoteRef::Ssh(SshSpec { user, host, port, path }))
}

fn parse_host_port(s: &str) -> Result<(String, u16)> {
    match s.rsplit_once(':') {
        Some((h, p)) => {
            let port: u16 = p.parse().with_context(|| format!("bad port {p:?}"))?;
            Ok((h.to_string(), port))
        }
        None => Ok((s.to_string(), 22)),
    }
}

/// Host key verification mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostKeyCheck {
    /// Trust on first use against ~/.rtransfer/known_hosts (default).
    Tofu,
    /// Trust every key (skip fingerprints; convenience for automation).
    Accept,
}

impl Default for HostKeyCheck {
    fn default() -> Self {
        HostKeyCheck::Tofu
    }
}

/// CLI-provided SSH settings.
#[derive(Debug, Clone, Default)]
pub struct SshOptions {
    pub password: Option<String>,
    pub key: Option<PathBuf>,
    pub host_key_check: HostKeyCheck,
}

/// Quote for safe shell exec across sshd (single-quote environment).
pub fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_refs() {
        let r = RemoteRef::parse("tcp://1.2.3.4:7007/dir").unwrap();
        match r {
            RemoteRef::Tcp(s) => {
                assert_eq!(s.host, "1.2.3.4");
                assert_eq!(s.port, 7007);
                assert_eq!(s.path, "dir");
            }
            _ => panic!(),
        }

        let r = RemoteRef::parse("alice@box.h.com:/data/somewhere").unwrap();
        match r {
            RemoteRef::Ssh(s) => {
                assert_eq!(s.user.as_deref(), Some("alice"));
                assert_eq!(s.host, "box.h.com");
                assert_eq!(s.path, PathBuf::from("/data/somewhere"));
            }
            _ => panic!(),
        }

        let r = RemoteRef::parse("host/dir-no-colon");
        assert!(r.is_err(), "path without ':' or 'tcp://' is not a remote");

       	assert!(RemoteRef::parse("/plain/local/path").is_err());
        assert!(RemoteRef::parse("user@portless:/home/x y").is_ok());
        let r = RemoteRef::parse("ssh://bob@box:2222/abs/dir").unwrap();
        match r {
            RemoteRef::Ssh(s) => {
                assert_eq!(s.user.as_deref(), Some("bob"));
                assert_eq!(s.host, "box");
                assert_eq!(s.port, 2222);
                // URL form keeps the leading '/' → remote-absolute target.
                assert_eq!(s.path, PathBuf::from("/abs/dir"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn shell_quote_escapes() {
        assert_eq!(shell_quote("a 'b'"), r"'a '\''b'\'''");
    }
}

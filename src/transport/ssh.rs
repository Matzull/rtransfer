//! SSH transport backend (via `russh`).
//!
//! The SSH connection does two jobs:
//!
//! 1. **Bootstrap** — run `rtransfer serve` on the remote host through an
//!    `exec` channel; the session's control link is that channel itself
//!    (light protocol traffic only).
//! 2. **Data links** — heavyweight frames travel on real TCP connections
//!    direct to an announced port, or through `direct-tcpip` SSH channels
//!    when the remote port is only reachable from inside its own host.
//!
//! Host keys use trust-on-first-use against `~/.rtransfer/known_hosts`
//! (fingerprint = BLAKE3 of the key's `Debug` representation), prompts, or
//! are force-accepted with `--accept-host-key`.

use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use russh::client::{self, Handle};
use russh::keys::load_secret_key;
use tokio::io::{AsyncRead, AsyncWrite};

use super::{HostKeyCheck, SshOptions, SshSpec};

/// Big channel windows: SSH per-channel flow control otherwise throttles
/// high-BDP links. 16 MiB window; SSH packets stay at the traditional joint
/// 64 KiB cap (russh refuses larger ones, and bulk data rides on the raw
/// TCP data links anyway).
fn ssh_config() -> Arc<client::Config> {
    Arc::new(client::Config {
        window_size: 16 * 1024 * 1024,
        maximum_packet_size: 65535,
        inactivity_timeout: Some(Duration::from_secs(300)),
        keepalive_interval: Some(Duration::from_secs(30)),
        nodelay: true,
        ..Default::default()
    })
}

struct SshHandler {
    spec: SshSpec,
    opts: SshOptions,
}

impl client::Handler for SshHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        let fingerprint = fingerprint_of(server_public_key);
        match self.opts.host_key_check {
            HostKeyCheck::Accept => Ok(true),
            HostKeyCheck::Tofu => {
                if known_hosts::is_known(&self.spec.host, self.spec.port, &fingerprint) {
                    Ok(true)
                } else {
                    eprintln!(
                        "WARNING: unseen host key for {}:{}\n  fingerprint: {}\n  accept? (y/N) ",
                        self.spec.host, self.spec.port, fingerprint
                    );
                    let mut line = String::new();
                    let yes = std::io::stdin()
                        .read_line(&mut line)
                        .map(|_| matches!(line.trim(), "y" | "Y" | "yes"))
                        .unwrap_or(false);
                    if yes {
                        known_hosts::record(&self.spec.host, self.spec.port, &fingerprint);
                        Ok(true)
                    } else {
                        eprintln!(
                            "rejected host key (use --accept-host-key to skip the prompt)"
                        );
                        Ok(false)
                    }
                }
            }
        }
    }
}

fn fingerprint_of(key: &russh::keys::PublicKeyOrCertificate) -> String {
    // Hashes the Debug form — stable enough for our own host-key store and
    // avoids depending on which rsa/openssh internals russh exposes.
    let debug = format!("{key:?}");
    let hex = blake3::hash(debug.as_bytes()).to_hex();
    format!("blake3-{}", &hex[24..56].to_string())
}

mod known_hosts {
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;

    fn file() -> PathBuf {
        let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
        home.join(".rtransfer").join("known_hosts")
    }

    pub fn is_known(host: &str, port: u16, fingerprint: &str) -> bool {
        let Ok(content) = fs::read_to_string(file()) else { return false };
        content.lines().any(|l| {
            l.starts_with(&format!("{host}:{port} ")) && l.trim_end().ends_with(&format!(" {fingerprint}"))
        })
    }

    pub fn record(host: &str, port: u16, fingerprint: &str) {
        let path = file();
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&path) {
            let _ = f.write_all(format!("{host}:{port} {fingerprint}\n").as_bytes());
        } else {
            tracing::warn!("could not record host key in {:?}", path);
        }
    }
}

/// An established, authenticated SSH session.
pub struct SshSession {
    handle: Handle<SshHandler>,
}

impl SshSession {
    pub async fn connect(spec: SshSpec, opts: SshOptions) -> Result<SshSession> {
        let handler = SshHandler { spec: spec.clone(), opts: opts.clone() };
        let addrs = (spec.host.as_str(), spec.port);
        let mut handle: Handle<SshHandler> =
            client::connect(ssh_config(), addrs, handler)
                .await
                .context("SSH connect failed")?;
        authenticate(&mut handle, &spec, &opts)
            .await
            .context("SSH authentication failed")?;
        Ok(SshSession { handle })
    }

    /// Run `cmd` remotely and return the exec channel as a bidirectional
    /// [`crate::linkmux::Link`] plus everything the remote printed while the
    /// session lived (stderr, exit code) for failure diagnostics.
    pub async fn exec_link(&self, cmd: &str) -> Result<ExecChannel> {
        let mut channel = self
            .handle
            .channel_open_session()
            .await
            .context("open SSH session channel")?;
        channel
            .exec(true, cmd)
            .await
            .context("remote exec failed")?;

        let diag = Arc::new(ExecDiag::default());

        // `into_stream` silently consumes (drops) stderr and exit-status
        // frames, so pump the side channel ourselves. Protocol `Data` flows
        // to the link; `ExtendedData`/`ExitStatus` land in the diag.
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let writer = Box::new(channel.make_writer()) as Box<dyn AsyncWrite + Send + Unpin>;
        let diag2 = diag.clone();
        tokio::spawn(async move {
            let mut saw_eof = false;
            while let Some(m) = channel.wait().await {
                match m {
                    russh::ChannelMsg::Data { data } => {
                        if tx.send(data.to_vec()).is_err() {
                            break; // link disposed: stop pumping
                        }
                    }
                    russh::ChannelMsg::ExtendedData { data, .. } => {
                        // Stream remote stderr live: the staged serve logs
                        // its progress there; the operator wants that in the
                        // client log too, not only on failure.
                        let text = String::from_utf8_lossy(&data);
                        for line in text.lines() {
                            tracing::info!(target: "rtransfer::remote", "remote: {line}");
                        }
                        let mut g = diag2.stderr.lock().unwrap();
                        if g.len() < 8_192 {
                            g.push_str(&text);
                        }
                    }
                    russh::ChannelMsg::ExitStatus { exit_status } => {
                        *diag2.exit.lock().unwrap() = Some(exit_status);
                        if saw_eof {
                            break;
                        }
                    }
                    // Eof = no more stdout; ExitStatus usually follows, then
                    // Close. Keep pumping until Close so diagnostics stay
                    // complete (OpenSSH order: eof → exit-status → close).
                    russh::ChannelMsg::Eof => saw_eof = true,
                    russh::ChannelMsg::Close => break,
                    _ => {}
                }
            }
        });

        let link: crate::linkmux::Link =
            Box::new(ExecStream { rx, writer, head: Vec::new(), head_done: 0, eof: false });
        Ok(ExecChannel { link, diag })
    }

    /// Run `cmd` remotely to completion and return its trimmed stdout.
    async fn capture(&self, cmd: &str) -> Result<String> {
        let ch = self.exec_link(cmd).await?;
        let mut link = ch.link;
        let mut raw = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut link, &mut raw).await?;
        Ok(String::from_utf8_lossy(&raw).trim().to_string())
    }

    /// Stage the given executable (or this binary itself when `None`) on
    /// the remote host under a private throwaway directory and return it
    /// running `serve --stdio`. This is what makes the SSH backend work
    /// with nothing preinstalled: the staged copy is removed with
    /// [`Self::cleanup_deployment`] afterwards.
    ///
    /// `None` stages the client's own binary, which must match the remote
    /// platform. With an explicit `override_path` the platform check is
    /// skipped — a wrong-arch binary fails on the remote with a clear
    /// "exec format error" in the diagnostics.
    ///
    /// The private directory is created inside `stage_dir` (made if missing).
    pub async fn deploy_serve(&self, override_path: Option<&Path>, stage_dir: &str) -> Result<DeployedServe> {
        let (exe, expect_platform) = match override_path {
            Some(p) => (p.to_path_buf(), false),
            None => (std::env::current_exe().context("locating my own executable")?, true),
        };
        let bytes = tokio::fs::read(&exe)
            .await
            .with_context(|| format!("reading {}", exe.display()))?;

        if expect_platform {
            // The staged copy must be runnable by the remote CPU/OS. We
            // build one binary; a different platform needs a different
            // build (see docs: manual install, or --serve-binary).
            let uname = self.capture("uname -sm").await.unwrap_or_default();
            let mut words = uname.split_whitespace();
            let (ros, rarch) = (words.next(), words.next());
            let arch = match rarch {
                Some("x86_64") | Some("amd64") => "x86_64",
                Some("aarch64") | Some("arm64") => "aarch64",
                _ => "",
            };
            let local = (std::env::consts::OS, std::env::consts::ARCH);
            if ros != Some(local.0) || arch != local.1 {
                bail!(
                    "cannot stage serve: local build is {}/{} but remote is {:?}/{:?} — \
                     cross-compile a serve for the remote and pass it with --serve-binary \
                     (or install rtransfer there)",
                    local.0, local.1, ros, rarch
                );
            }
        }

        let base = stage_dir.trim_end_matches('/');
        let base = if base.is_empty() { "/" } else { base };
        let dir = self
            .capture(&format!(
                "mkdir -p {q} && mktemp -d {q}/rtransfer-stage-XXXXXX",
                q = super::shell_quote(base)
            ))
            .await
            .context("creating a staging directory on the remote host")?;
        if !dir.starts_with('/') {
            bail!("remote staging in {base:?}: unexpected mktemp output {dir:?} (the directory must be absolute and writable)");
        }
        let bin = format!("{dir}/rtransfer");

        // 1) upload: cat receives the bytes; EOF then chmod, then exit.
        let mut up = self
            .exec_link(&format!("cat > {q} && chmod 0700 {q}", q = super::shell_quote(&bin)))
            .await
            .context("opening the upload channel")?;
        {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            up.link.write_all(&bytes).await.context("uploading serve binary")?;
            up.link.flush().await?;
            up.link.shutdown().await?; // EOF so `cat` finishes
            let mut sink = Vec::new();
            up.link.read_to_end(&mut sink).await?; // wait until the channel ended
        }
        if up.diag.exit.lock().unwrap().is_some_and(|s| s != 0) {
            let why = up.diag.summarize().unwrap_or_default();
            bail!("staging the serve binary failed ({why})");
        }

        // 2) run it.
        let ctrl = self
            .exec_link(&format!("exec {} serve --stdio", super::shell_quote(&bin)))
            .await
            .context("starting the staged serve")?;
        Ok(DeployedServe { ctrl: Some(ctrl), dir })
    }

    /// Best-effort removal of a staged serve (`rm -rf` its directory).
    pub async fn cleanup_deployment(&self, dir: &str) {
        let _ = self.exec_link(&format!("rm -rf {}", super::shell_quote(dir))).await;
    }

    /// Open a `direct-tcpip` channel to a loopback port on the remote host.
    pub async fn tunnel_port(&self, port: u16, timeout: Duration) -> Result<crate::linkmux::Link> {
        let fut = self
            .handle
            .channel_open_direct_tcpip("127.0.0.1", port as u32, "127.0.0.1", 0);
        let channel = tokio::time::timeout(timeout, fut)
            .await
            .map_err(|_| anyhow!("timeout opening SSH tunnel"))??;
        let stream = channel.into_stream();
        Ok(Box::new(stream))
    }
}

fn read_secret(prompt: &str) -> Result<String> {
    rpassword::prompt_password(prompt).map_err(|e| anyhow!("password prompt failed: {e}"))
}

/// A control link opened by `exec`, carrying the remote diagnostics.
pub struct ExecChannel {
    pub link: crate::linkmux::Link,
    pub diag: Arc<ExecDiag>,
}

/// A serve staged on the remote host by [`SshSession::deploy_serve`]; its
/// directory is removed with [`SshSession::cleanup_deployment`] at session
/// end. `ctrl` is taken out by the caller to run the hello handshake.
pub struct DeployedServe {
    ctrl: Option<ExecChannel>,
    dir: String,
}

impl DeployedServe {
    pub fn take_ctrl(&mut self) -> Option<ExecChannel> {
        self.ctrl.take()
    }

    pub fn dir(&self) -> &str {
        &self.dir
    }
}

/// What the remote `serve` printed on stderr and how it ended; consulted
/// when the control link dies so the client can say *why*.
#[derive(Default)]
pub struct ExecDiag {
    stderr: std::sync::Mutex<String>,
    exit: std::sync::Mutex<Option<u32>>,
}

impl ExecDiag {
    /// A short human summary, or `None` when the channel died with no
    /// stderr at all and no exit status (pure network death).
    pub fn summarize(&self) -> Option<String> {
        let mut parts: Vec<String> = Vec::new();
        if let Some(code) = *self.exit.lock().unwrap() {
            parts.push(format!("remote exit status {code}"));
        }
        let stderr = self.stderr.lock().unwrap().trim().to_string();
        if !stderr.is_empty() {
            // Keep the tail — shells print the interesting "not found"
            // lines last.
            let count = stderr.chars().count();
            let s = if count > 400 {
                format!("…{}", stderr.chars().skip(count - 400).collect::<String>())
            } else {
                stderr
            };
            parts.push(format!("remote stderr: {s}"));
        }
        (!parts.is_empty()).then(|| parts.join("; "))
    }
}

/// Bidirectional raw stream over an SSH `exec` channel: protocol bytes come
/// from the mux queue, writes pass straight through, and no `ExtendedData`
/// frames ever pollute the stream.
struct ExecStream {
    rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    writer: Box<dyn AsyncWrite + Send + Unpin>,
    head: Vec<u8>, // current chunk, possibly partially consumed
    head_done: usize,
    eof: bool,
}

impl AsyncRead for ExecStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        if self.eof {
            return std::task::Poll::Ready(Ok(()));
        }
        if self.head_done >= self.head.len() {
            match Pin::new(&mut self.rx).poll_recv(cx) {
                std::task::Poll::Ready(Some(chunk)) => {
                    self.head = chunk;
                    self.head_done = 0;
                }
                std::task::Poll::Ready(None) => {
                    self.eof = true;
                    return std::task::Poll::Ready(Ok(()));
                }
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
        let avail = self.head.len() - self.head_done;
        let n = avail.min(buf.remaining());
        buf.put_slice(&self.head[self.head_done..self.head_done + n]);
        self.head_done += n;
        std::task::Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ExecStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}

async fn authenticate(
    handle: &mut Handle<SshHandler>,
    spec: &SshSpec,
    opts: &SshOptions,
) -> Result<()> {
    let user = spec
        .user
        .clone()
        .ok_or_else(|| anyhow!("SSH remotes need a user: user@host:/path"))?;

    // 1) explicit password
    if let Some(pw) = &opts.password {
        try_password(handle, &user, pw).await?;
        return Ok(());
    }
    // 2) explicit key or the usual defaults
    let key_path: Option<PathBuf> = match &opts.key {
        Some(k) => Some(k.clone()),
        None => {
            let home = std::env::var_os("HOME").map(PathBuf::from);
            home.and_then(|h| {
                for name in ["id_ed25519", "id_ecdsa", "id_rsa"] {
                    let p = h.join(".ssh").join(name);
                    if p.exists() {
                        return Some(p);
                    }
                }
                None
            })
        }
    };
    if let Some(kp) = &key_path {
        match load_key_and_auth(handle, &user, kp).await {
            Ok(()) => return Ok(()),
            Err(e) => tracing::info!("key {} rejected: {e}", kp.display()),
        }
    }
    // 3) ssh-agent (Unix)
    #[cfg(target_family = "unix")]
    if std::env::var_os("SSH_AUTH_SOCK").is_some() {
        match try_agent(handle, &user).await {
            Ok(()) => return Ok(()),
            Err(e) => tracing::info!("agent auth skipped: {e:#}"),
        }
    }
    // 4) interactive password
    let prompt = format!("password for {user}@{}: ", spec.host);
    let pw = read_secret(&prompt)?;
    try_password(handle, &user, &pw).await?;
    Ok(())
}

async fn try_password(handle: &mut Handle<SshHandler>, user: &str, pw: &str) -> Result<()> {
    let res = handle
        .authenticate_password(user, pw.to_string())
        .await
        .context("password auth error")?;
    if !res.success() {
        bail!("password rejected");
    }
    Ok(())
}

async fn load_key_and_auth(handle: &mut Handle<SshHandler>, user: &str, path: &Path) -> Result<()> {
    let key = match load_secret_key(path, None) {
        Ok(k) => Arc::new(k),
        Err(_) => {
            // Encrypted key: ask for the passphrase.
            let pass = read_secret(&format!("passphrase for {}: ", path.display()))?;
            Arc::new(load_secret_key(path, Some(&pass)).context("reading private key")?)
        }
    };
    let pkh = russh::keys::key::PrivateKeyWithHashAlg::new(key, None);
    let res = handle
        .authenticate_publickey(user, pkh)
        .await
        .context("publickey auth error")?;
    if !res.success() {
        bail!("key {} rejected", path.display());
    }
    Ok(())
}

#[cfg(target_family = "unix")]
async fn try_agent(handle: &mut Handle<SshHandler>, user: &str) -> Result<()> {
    use russh::keys::agent::client::AgentClient;
    let mut agent = AgentClient::connect_env().await.context("connect to ssh-agent")?;
    let identities = agent.request_identities().await.context("agent identities")?;
    let mut last_err: Option<String> = None;
    for ident in &identities {
        let pubkey = ident.public_key().into_owned();
        let res = handle
            .authenticate_publickey_with(user, pubkey, None, &mut agent)
            .await
            .map_err(|e| anyhow!("agent signer error: {e}"))?;
        if res.success() {
            return Ok(());
        }
        last_err = Some("identity rejected".into());
    }
    bail!(last_err.unwrap_or_else(|| "no agent identities".into()))
}

//! Client orchestration: argument mapping, transport setup (SSH bootstrap or
//! direct TCP), link attachment, greeting and session run.
//!
//! Kept in the library so integration tests can exercise the exact flow the
//! CLI uses without spawning processes.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio_util::sync::CancellationToken;

use crate::linkmux::{self, LinkSet, ROUTER_DEPTH};
use crate::protocol::{BenchInfo, CopyMode, Msg};
use crate::session::{attach_link_hello, RoleParams, Summary};
use crate::transport::{HostKeyCheck, RemoteRef, SshOptions, SshSpec};
use crate::{cli, fsops, ioengine};

/// Timeout for one fresh TCP dial.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the client waits for the server's ephemeral data-port announce.
pub const ANNOUNCE_WAIT: Duration = Duration::from_secs(6);

/// One crate module bundle for the client flow; each entry point runs a whole
/// session (`send_greeting` + `run_session`) to completion.
///
/// Sources for push must already be validated (`check_sources`), because the
/// caller may want a specific error surface.
pub async fn run_tcp(
    args: &cli::SyncArgs,
    mode: CopyMode,
    host: &str,
    port: u16,
    target: &str,
    srcs: &[String],
    dest: Option<PathBuf>,
    cancel: &CancellationToken,
) -> Result<Summary> {
    enforce_pull_target(mode, target)?;
    let root = receiver_root(mode, dest.clone())?;
    if mode == CopyMode::Pull && !tokio::fs::try_exists(&root).await.unwrap_or(false) {
        tokio::fs::create_dir_all(&root)
            .await
            .with_context(|| format!("cannot create destination {}", root.display()))?;
    }
    let sources = match mode {
        CopyMode::Push => check_sources(srcs)?,
        CopyMode::Pull => Vec::new(),
    };
    let params = role_params(args, mode, root, sources)?;

    let instance = new_instance();
    let (set, ctrl_rx) = LinkSet::new(instance, 256, ROUTER_DEPTH);
    let links = args.links.max(1);

    // All links dial + hello-attach before the greeting is even sent.
    for i in 0..links {
        if cancel.is_cancelled() {
            bail!("cancelled");
        }
        let stream = crate::transport::tcp::connect(host, port, CONNECT_TIMEOUT).await?;
        let link = crate::transport::tcp::as_link(stream);
        attach_link_hello(&set, link, i as u32, None).await?;
    }
    send_greeting(&set, mode, target, links, args, params.merge_scope).await?;

    crate::session::run_session(set, ctrl_rx, params, cancel.clone()).await
}

pub async fn run_ssh(
    args: &cli::SyncArgs,
    mode: CopyMode,
    mut spec: SshSpec,
    srcs: &[String],
    dest: Option<PathBuf>,
    cancel: &CancellationToken,
) -> Result<Summary> {
    enforce_pull_target(mode, spec.path.to_string_lossy().as_ref())?;
    // rsync-style refs get their port from --ssh-port; URLs keep their own.
    if spec.port == 22 && args.ssh_port != 22 {
        spec.port = args.ssh_port;
    }
    if spec.user.is_none() {
        spec.user = Some(
            std::env::var("USER")
                .or_else(|_| std::env::var("USERNAME"))
                .unwrap_or_else(|_| "root".into()),
        );
    }
    let root = receiver_root(mode, dest.clone())?;
    if mode == CopyMode::Pull && !tokio::fs::try_exists(&root).await.unwrap_or(false) {
        tokio::fs::create_dir_all(&root)
            .await
            .with_context(|| format!("cannot create destination {}", root.display()))?;
    }
    let sources = match mode {
        CopyMode::Push => check_sources(srcs)?,
        CopyMode::Pull => Vec::new(),
    };
    let params = role_params(args, mode, root, sources)?;

    let opts = SshOptions {
        password: args.password.clone(),
        key: args.ssh_key.clone(),
        host_key_check: if args.accept_host_key {
            HostKeyCheck::Accept
        } else {
            HostKeyCheck::Tofu
        },
    };
    let session = crate::transport::ssh::SshSession::connect(spec.clone(), opts).await?;

    let instance = new_instance();
    let (set, ctrl_rx) = LinkSet::new(instance, 256, ROUTER_DEPTH);
    let links = args.links.max(1);

    // Control link over the SSH exec channel. Zero-install policy: a
    // preinstalled `rtransfer` (found via a login shell → PATH) is used
    // when present; otherwise this client stages its own binary in a
    // throwaway /tmp directory on the remote and removes it afterwards.
    let mut staged: Option<crate::transport::ssh::DeployedServe> = None;
    let mut attached = false;
    if let Ok(c) = session.exec_link("sh -lc 'exec rtransfer serve --stdio'").await {
        attached = attach_link_hello(&set, c.link, 0, Some(c.diag.as_ref())).await.is_ok();
    }
    if !attached {
        tracing::info!("remote `rtransfer serve` unavailable; staging a copy of this binary on the remote host");
        let mut deployed = session
            .deploy_serve(args.serve_binary.as_deref(), &args.stage_dir)
            .await
            .context("staging a serve on the remote host failed")?;
        let ctrl = deployed
            .take_ctrl()
            .expect("a fresh deployment holds its control channel");
        if let Err(e) = attach_link_hello(&set, ctrl.link, 0, Some(ctrl.diag.as_ref())).await {
            session.cleanup_deployment(deployed.dir()).await;
            return Err(e.context("staged serve did not answer hello"));
        }
        staged = Some(deployed);
    }

    // Server announces its ephemeral data port for extra links.
    let mut ctrl_rx = ctrl_rx;
    let mut announce_port: Option<u16> = None;
    if links > 1 {
        let deadline = tokio::time::Instant::now() + ANNOUNCE_WAIT;
        loop {
            let m = tokio::select! {
                m = ctrl_rx.recv() => m,
                _ = tokio::time::sleep_until(deadline) => None,
                _ = cancel.cancelled() => bail!("cancelled"),
            };
            match m {
                Some(Msg::Announce { port }) => {
                    announce_port = Some(port);
                    break;
                }
                Some(_) => continue, // nothing else arrives pre-greeting
                None => break,
            }
        }
    }

    if let Some(port) = announce_port {
        // Direct dials run concurrently — they are the LAN fast path and
        // fail fast; the SSH tunnel fallback shares one session, so those
        // (only the dials that failed) stay sequential.
        let mut tunnels: Vec<u16> = Vec::new();
        let mut js = tokio::task::JoinSet::<(u16, Option<tokio::net::TcpStream>)>::new();
        for i in 1..links {
            if cancel.is_cancelled() {
                bail!("cancelled");
            }
            let host = spec.host.clone();
            js.spawn(async move {
                (i, crate::transport::tcp::connect(&host, port, Duration::from_secs(3)).await.ok())
            });
        }
        while let Some(Ok((i, stream))) = js.join_next().await {
            if let Some(s) = stream {
                attach_link_hello(&set, crate::transport::tcp::as_link(s), i as u32, None).await?;
            } else {
                tunnels.push(i);
            }
        }
        for i in tunnels {
            if cancel.is_cancelled() {
                bail!("cancelled");
            }
            let link = session
                .tunnel_port(port, Duration::from_secs(6))
                .await
                .context("SSH tunnel for data link")?;
            attach_link_hello(&set, link, i as u32, None).await?;
        }
    }

    send_greeting(&set, mode, &spec.path.to_string_lossy(), links, args, params.merge_scope).await?;

    let out = crate::session::run_session(set, ctrl_rx, params, cancel.clone()).await;
    // Remove a staged serve before closing the SSH handle.
    if let Some(d) = staged {
        session.cleanup_deployment(d.dir()).await;
    }
    // Drop the SSH session handle (closing the exec channel).
    drop(session);
    out
}

/// Dispatch on remote syntax: SSH specs get the ssh bootstrap, `tcp://` dials
/// directly.
pub async fn check_and_run(
    args: &cli::SyncArgs,
    mode: CopyMode,
    srcs: &[String],
    remote: &str,
    dest: Option<PathBuf>,
    cancel: &CancellationToken,
) -> Result<Summary> {
    match RemoteRef::parse(remote)? {
        RemoteRef::Ssh(spec) => run_ssh(args, mode, spec, srcs, dest, cancel).await,
        RemoteRef::Tcp(t) => {
            run_tcp(args, mode, &t.host, t.port, &t.path, srcs, dest, cancel).await
        }
    }
}

/// Validate local sources and map them to `(name, path)` pairs.
pub fn check_sources(srcs: &[String]) -> Result<Vec<(String, PathBuf)>> {
    let mut out = Vec::with_capacity(srcs.len());
    for s in srcs {
        let path = expand_tilde(Path::new(s));
        let md = std::fs::metadata(&path)
            .with_context(|| format!("source {} missing/unreadable", path.display()))?;
        let base = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "root".to_string());
        if md.is_file() && srcs.iter().filter(|o| *o == s).count() > 1 {
            bail!("source {} given twice", path.display());
        }
        out.push((base, path));
    }
    Ok(out)
}

/// `expand_tilde` levels both sync callers and tests.
pub fn expand_tilde(p: &Path) -> PathBuf {
    let s = p.to_string_lossy();
    let s: &str = &s;
    if s == "~" || s.starts_with("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            let mut pb = PathBuf::from(home);
            if s.len() > 1 {
                pb.push(PathBuf::from(&s[2..]));
            }
            return pb;
        }
    }
    p.to_path_buf()
}

pub fn enforce_pull_target(mode: CopyMode, target: &str) -> Result<()> {
    let _ = (mode, target);
    Ok(())
}

async fn send_greeting(
    set: &linkmux::Shared,
    mode: CopyMode,
    target: &str,
    links: u16,
    args: &cli::SyncArgs,
    merge: bool,
) -> Result<()> {
    // The BenchInfo is informational (target-side hint); both sides bench on
    // their own for real tuning, so keep this probe cheap.
    set.bus
        .send_control(&Msg::Greeting {
            mode,
            delete: args.delete,
            allow_delta: !args.no_delta,
            target: target.to_string(),
            links,
            bench: BenchInfo {
                cores: 1,
                hash_mb_s: 0.0,
                zstd_l3_mb_s: 0.0,
                zstd_l9_mb_s: 0.0,
            },
            include: args.include.clone(),
            exclude: args.exclude.clone(),
            merge,
            fsync: args.fsync,
        })
        .await
        .context("greeting delivery")
}

pub fn role_params(
    args: &cli::SyncArgs,
    mode: CopyMode,
    root: PathBuf,
    sources: Vec<(String, PathBuf)>,
) -> Result<RoleParams> {
    let filters = fsops::Filters::new(&args.include, &args.exclude)
        .context("parsing include/exclude patterns")?;
    let io = match args.io {
        cli::IoChoice::Auto => ioengine::IoMode::Auto,
        cli::IoChoice::Uring => ioengine::IoMode::Uring,
        cli::IoChoice::Blocking => ioengine::IoMode::Blocking,
    };
    // A single-source directory push merges into the target dir; the greeting
    // relays this scope choice. Pull always treats the served tree as the
    // merge root (the server builds a single-source manifest).
    let merge_scope = match mode {
        CopyMode::Push => {
            sources.len() == 1
                && std::fs::metadata(&sources[0].1).map(|m| m.is_dir()).unwrap_or(false)
        }
        CopyMode::Pull => true,
    };
    Ok(RoleParams {
        role: mode.client_role(),
        root,
        sources,
        filters,
        follow_symlinks: args.follow,
        io,
        chunk_override: args.chunk_bytes,
        block_base_override: args.block_bytes,
        comp_override: cli::parse_compression(&args.compression)?,
        links_override: 0,
        workers_override: args.workers,
        quiet: args.quiet,
        is_client: true,
        mode,
        delete: args.delete,
        allow_delta: !args.no_delta,
        merge_scope,
        fsync: args.fsync,
    })
}

fn receiver_root(mode: CopyMode, dest: Option<PathBuf>) -> Result<PathBuf> {
    // push: root unused by the role (the client is the sender); pull: the
    // local destination directory.
    match mode {
        CopyMode::Push => {
            let cwd = std::env::current_dir().context("cwd")?;
            Ok(cwd)
        }
        CopyMode::Pull => {
            let d = expand_tilde(dest.as_deref().context("pull needs a local destination")?);
            Ok(d)
        }
    }
}

fn new_instance() -> u128 {
    rand::random::<u128>()
}

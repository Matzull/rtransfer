//! `rtransfer serve`: the session server.
//!
//! Two modes:
//!
//! * **stdio** (used over SSH `exec`): stdin/stdout is the control link; an
//!   ephemeral loopback TCP listener is raised for extra data links and
//!   announced to the client (`Msg::Announce`).
//! * **TCP listener**: every connection multiplexes into one of possibly
//!   several concurrent sessions, keyed by the client's random instance id.
//!
//! Connection bootstrap on any link: the client sends `Msg::Hello{link_id,
//! instance}`; an existing session with that instance accepts the link or a
//! new session is registered and its `run_session` task started.

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio_util::sync::CancellationToken;

use crate::fsops::Filters;
use crate::linkmux::{self, LinkSet, Shared, ROUTER_DEPTH};
use crate::protocol::{CopyMode, Msg};
use crate::session::{run_session, RoleParams};
use crate::{ioengine, transport};

/// In-process control channel capacity per session.
const CTRL_BUFFER: usize = 256;
/// How long a bare connection may idle before the first hello.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct ServeOptions {
    /// Base root all relative targets resolve against.
    pub root: PathBuf,
    /// TCP listener bind address (None = stdio mode).
    pub bind: Option<IpAddr>,
    pub port: Option<u16>,
    /// Run one session over stdin/stdout (SSH bootstrap).
    pub stdio: bool,
}

/// Registration: open sessions by instance id (client-chosen random).
#[derive(Default)]
struct Registry(Mutex<HashMap<u128, Shared>>);

impl Registry {
    fn has(&self, instance: u128) -> bool {
        self.0.lock().unwrap().contains_key(&instance)
    }
    fn register(&self, instance: u128, set: Shared) {
        self.0.lock().unwrap().insert(instance, set);
    }
    fn get(&self, instance: u128) -> Option<Shared> {
        self.0.lock().unwrap().get(&instance).cloned()
    }
}

/// Server-side baseline params; roles/flags are overwritten by the greeting.
fn base_params(root: &PathBuf) -> RoleParams {
    RoleParams {
        role: crate::protocol::Role::Receiver,
        root: root.clone(),
        sources: Vec::new(),
        filters: Filters::default(),
        follow_symlinks: false,
        io: ioengine::IoMode::Auto,
        chunk_override: 0,
        block_base_override: 0,
        comp_override: None,
        links_override: 0,
        workers_override: None,
        quiet: true, // servers don't draw progress bars
        is_client: false,
        mode: CopyMode::Push,
        delete: false,
        allow_delta: true,
        merge_scope: false,
        fsync: false,
    }
}

pub async fn serve(opts: &ServeOptions, cancel: CancellationToken) -> Result<()> {
    // Serve root: used as the base for relative targets; create it.
    if !tokio::fs::try_exists(&opts.root).await.unwrap_or(false) {
        tokio::fs::create_dir_all(&opts.root)
            .await
            .with_context(|| format!("cannot create serve root {}", opts.root.display()))?;
    }
    let base = base_params(&opts.root);
    let registry = Arc::new(Registry::default());

    if opts.stdio {
        serve_stdio(base, registry, cancel).await
    } else {
        let bind = opts.bind.unwrap_or(IpAddr::from([127, 0, 0, 1]));
        let port = opts.port.unwrap_or(7822);
        serve_tcp(base, registry, bind, port, cancel).await
    }
}

/// One session over stdin/stdout (typically inside an SSH exec channel).
async fn serve_stdio(
    base: RoleParams,
    registry: Arc<Registry>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut sin = tokio::io::stdin();
    let mut sout = tokio::io::stdout();

    let hello = linkmux::frame::read_hello(&mut sin).await?;
    let instance = hello.instance;
    let (set, ctrl_rx) = if registry.has(instance) {
        bail!("stdio serves exactly one session; instance {instance} duplicate");
    } else {
        let (set, ctrl_rx) = LinkSet::new(instance, CTRL_BUFFER, ROUTER_DEPTH);
        registry.register(instance, set.clone());
        (set, ctrl_rx)
    };
    // Echo the hello, then attach our side.
    linkmux::frame::write_hello(&mut sout, &hello).await?;
    set.bus.attach_writer(sout);
    set.attach_reader(sin);

    // Extra data links: ephemeral loopback listener + announce (best effort;
    // the client falls back to SSH tunnels when no announce arrives).
    let _listener = tokio::spawn({
        let set = set.clone();
        let registry = registry.clone();
        let base = base.clone();
        let cancel = cancel.clone();
        async move {
            // Bind wildcard first so cross-host direct data links can dial;
            // fall back to loopback-only (always tunnelable via SSH).
            let bound = match transport::tcp::listen("0.0.0.0", 0).await {
                Ok(b) => Ok(b),
                Err(_) => transport::tcp::listen("127.0.0.1", 0).await,
            };
            match bound {
                Ok((_listener, port)) => {
                    let _ = set.bus.send_control(&Msg::Announce { port }).await;
                    tracing::debug!("stdio data listener on port {port}");
                    accept_loop(registry, _listener, base, cancel).await;
                }
                Err(e) => {
                    tracing::debug!("stdio data listener unavailable: {e:#}");
                }
            }
        }
    });

    run_session(set, ctrl_rx, base, cancel).await.map(|_summary| ())
}

/// The long-running TCP accept loop (one process may host many sessions).
async fn serve_tcp(
    base: RoleParams,
    registry: Arc<Registry>,
    bind: IpAddr,
    port: u16,
    cancel: CancellationToken,
) -> Result<()> {
    let (listener, port) = transport::tcp::listen(bind.to_string().as_str(), port)
        .await
        .context("serve listener")?;
    if !quieter_please() {
        eprintln!("rtransfer serve ready on {bind}:{port} (root {})", base.root.display());
    }
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _peer)) = accepted else { continue; };
                let registry = registry.clone();
                let base = base.clone();
                let cancel = cancel.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(registry, stream, base, cancel).await {
                        tracing::debug!("connection ended: {e:#}");
                    }
                });
            }
            _ = cancel.cancelled() => {
                tracing::info!("serve shutting down");
                return Ok(());
            }
        }
    }
}

/// Bootstrap ONE raw TCP connection: hello, registry match or a new session.
async fn handle_connection(
    registry: Arc<Registry>,
    stream: tokio::net::TcpStream,
    base: RoleParams,
    cancel: CancellationToken,
) -> Result<()> {
    // Control frames are tiny; Nagle + delayed ACK would add ~40 ms to each.
    stream.set_nodelay(true).ok();
    let (mut r, mut w) = tokio::io::split(stream);
    let hello = match tokio::time::timeout(HANDSHAKE_TIMEOUT, linkmux::frame::read_hello(&mut r)).await {
        Ok(Ok(h)) => h,
        _ => bail!("no hello within {HANDSHAKE_TIMEOUT:?}"),
    };
    let instance = hello.instance;
    if let Some(set) = registry.get(instance) {
        linkmux::frame::write_hello(&mut w, &hello).await?;
        set.bus.attach_writer(w);
        set.attach_reader(r);
        return Ok(());
    }
    // New session: this first connection is the control link.
    let (set, ctrl_rx) = LinkSet::new(instance, CTRL_BUFFER, ROUTER_DEPTH);
    registry.register(instance, set.clone());
    linkmux::frame::write_hello(&mut w, &hello).await?;
    set.bus.attach_writer(w);
    set.attach_reader(r);
    let _ = run_session(set, ctrl_rx, base, cancel).await;
    Ok(())
}

/// Accept loop shared by stdio's data listener and any other mode variants.
async fn accept_loop(
    registry: Arc<Registry>,
    listener: tokio::net::TcpListener,
    base: RoleParams,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _peer)) = accepted else { continue; };
                let registry = registry.clone();
                let base = base.clone();
                let cancel = cancel.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(registry, stream, base, cancel).await {
                        tracing::debug!("connection ended: {e:#}");
                    }
                });
            }
            _ = cancel.cancelled() => return,
        }
    }
}

fn quieter_please() -> bool {
    std::env::var_os("RTRANSFER_QUIET").map(|v| v == "1").unwrap_or(false)
}

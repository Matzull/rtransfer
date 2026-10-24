//! Session driver: greeting/verification, bandwidth probing + tuning, and
//! dispatch to the role engines.
//!
//! A **session** is one LinkSet (control link + optional data links) with a
//! single copy direction. The *client* initiates; the server mirrors. Both
//! ends enter the same [`run_session`] with a [`RoleParams`].

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::bench::CpuBench;
use crate::linkmux::{self, CtrlRx, Link, Shared};
use crate::progress::Progress;
use crate::protocol::{CopyMode, Msg, Role};
use crate::tuner::{NetEst, Tuning};
use crate::{fsops, ioengine};

pub const GREETING_TIMEOUT: Duration = Duration::from_secs(60);
pub const MANIFEST_TIMEOUT: Duration = Duration::from_secs(300);
pub const SUMMARY_TIMEOUT: Duration = Duration::from_secs(600);
pub const PROBE_MAX: Duration = Duration::from_secs(5);
pub const LINK_WAIT: Duration = Duration::from_secs(30);
/// Upper bound for flushing queued frames at session end.
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Everything a role needs to run a session.
#[derive(Clone)]
pub struct RoleParams {
    pub role: Role,
    /// Server root; the greeting `target` resolves relative to it. On the
    /// client side, `sources` live under `root` for senders.
    pub root: PathBuf,
    /// Sender-only: (display name, source path) pairs.
    pub sources: Vec<(String, PathBuf)>,
    pub filters: fsops::Filters,
    pub follow_symlinks: bool,
    pub io: ioengine::IoMode,
    /// 0 = auto for the size knobs.
    pub chunk_override: u64,
    pub block_base_override: u64,
    /// `None` = auto pick; `Some(level)` = force, `Some(none)` = disable.
    pub comp_override: Option<Option<i8>>,
    pub links_override: u16,
    pub workers_override: Option<u32>,
    pub quiet: bool,
    /// `false` on servers, which mirror the client's greeting instead.
    pub is_client: bool,
    /// Client-only: copy direction; servers derive their role from it.
    pub mode: CopyMode,
    pub delete: bool,
    pub allow_delta: bool,
    /// Single-source tree push/pull: rels carry no leading name component
    /// (the tree merges straight into the target directory); `--delete`
    /// scopes to the whole target dir. Set on the server from the greeting.
    pub merge_scope: bool,
    /// fsync the part file before rename (opt-in; see `Greeting::fsync`).
    pub fsync: bool,
}

/// Aggregated results for the CLI summary.
#[derive(Debug, Default)]
pub struct Summary {
    pub files_done: u64,
    pub files_failed: u64,
    pub files_skipped: u64,
    pub bytes_literal: u64,
    pub bytes_wire: u64,
    pub bytes_skipped: u64,
    pub elapsed_secs: f64,
    pub net_bps: f64,
    pub deleted: Vec<String>,
}

/// A fatal, session-wide error reported by the peer over the control link.
/// Watched by a background task; carries the message so the client can show
/// *why* the transfer aborted.
#[derive(Default)]
pub struct FatalWatch {
    fatal: Mutex<Option<String>>,
    tripped: AtomicBool,
}

impl FatalWatch {
    pub fn set(&self, msg: String) {
        {
            let mut g = self.fatal.lock().unwrap();
            if g.is_none() {
                *g = Some(msg);
            }
        }
        self.tripped.store(true, Ordering::SeqCst);
    }

    /// Take the fatal error if one arrived, else `Ok(())`.
    pub fn check(&self) -> Result<()> {
        match &*self.fatal.lock().unwrap() {
            Some(e) => bail!("remote aborted: {e}"),
            None => Ok(()),
        }
    }

    pub fn tripped(&self) -> bool {
        self.tripped.load(Ordering::SeqCst)
    }
}

/// Attach one newly-dialled link: exchange Hello echoes, then wire both
/// halves into the set. (The server's half already echoed on accept.)
/// `why_diag` enriches the failure message with remote stderr/exit status
/// when the remote end died before echoing (SSH exec control links).
pub async fn attach_link_hello(
    set: &Shared,
    link: Link,
    link_id: u32,
    why_diag: Option<&crate::transport::ssh::ExecDiag>,
) -> Result<()> {
    let mut link = link;
    linkmux::frame::write_msg(&mut link, &Msg::Hello { link_id, instance: set.instance }).await?;
    let echo = match linkmux::frame::read_msg(&mut link).await? {
        Some(m) => m,
        None => {
            let mut fail =
                "link closed before hello echo — did the remote `rtransfer serve` start?".to_string();
            if let Some(d) = why_diag {
                if let Some(s) = d.summarize() {
                    fail = format!("{fail} ({s})");
                }
            }
            bail!(fail);
        }
    };
    match echo {
        Msg::Hello { link_id: irl, instance: ins } if irl == link_id && ins == set.instance => {}
        other => bail!("unexpected hello echo: {other:?}"),
    }
    linkmux::attach_duplex(set, link);
    Ok(())
}

/// Typed control-plane queues carved out of the raw control stream.
/// The result/error receivers are returned separately (`spawn`) because only
/// the sender role (and the session's error watcher) ever consumes them.
pub struct Dispatcher {
    pub greeting_rx: RxCell,
    pub plan_rx: UnboundedRxCell,
    pub hash_rx: RxCell,
    pub announce_rx: RxCell,
    pub server_info_rx: RxCell,
    pub manifest_rx: RxCell,
    pub done_rx: RxCell,
    pub probe_rx: RxCell,
    pub sig_rx: UnboundedRxCell,
    pub set: Shared,
}

/// Receiver behind an async mutex: the `Dispatcher` is shared via `Arc`, but
/// each typed queue is drained from exactly one consumer at a time. Unlike a
/// `std` guard, the tokio guard is `Send`, so it composes with `select!` arms.
pub type RxCell = tokio::sync::Mutex<mpsc::Receiver<Msg>>;
/// For queues the sender consumes strictly in manifest order (plans,
/// signatures). They must never block the dispatcher: the plan loop may be
/// parked on a worker slot that only a `FileResult` — queued *behind* those
/// plans — can free. Their size is bounded by the manifest anyway.
pub type UnboundedRxCell = tokio::sync::Mutex<mpsc::UnboundedReceiver<Msg>>;

pub type Spawned = (Arc<Dispatcher>, mpsc::Receiver<Msg>, mpsc::Receiver<String>);

impl Dispatcher {
    pub fn spawn(set: Shared, mut ctrl_rx: CtrlRx, cancel: CancellationToken) -> Spawned {
        let (greeting_tx, greeting_rx) = mpsc::channel(2);
        let (plan_tx, plan_rx) = mpsc::unbounded_channel();
        let (hash_tx, hash_rx) = mpsc::channel(256);
        let (result_tx, result_rx) = mpsc::channel(256);
        let (sig_tx, sig_rx) = mpsc::unbounded_channel();
        let (announce_tx, announce_rx) = mpsc::channel(4);
        let (server_info_tx, server_info_rx) = mpsc::channel(4);
        let (manifest_tx, manifest_rx) = mpsc::channel(2);
        let (done_tx, done_rx) = mpsc::channel(2);
        let (probe_tx, probe_rx) = mpsc::channel(1024);
        let (err_tx, err_rx) = mpsc::channel(8);
        let set2 = set.clone();
        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            loop {
                let msg = tokio::select! {
                    m = ctrl_rx.recv() => match m { Some(m) => m, None => break },
                    _ = cancel2.cancelled() => break,
                };
                match msg {
                    Msg::Probe { seq, .. } => {
                        // The probed side answers immediately on the control link.
                        let _ = set2.bus.send_control(&Msg::ProbeAck { seq }).await;
                    }
                    Msg::ProbeAck { .. } => { let _ = probe_tx.send(msg).await; }
                    Msg::Plan { .. } => { let _ = plan_tx.send(msg); }
                    Msg::FileHash { .. } => { let _ = hash_tx.send(msg).await; }
                    Msg::FileResult { .. } => { tracing::debug!("dispatcher routing FileResult"); let _ = result_tx.send(msg).await; }
                    Msg::SigData { .. } => { let _ = sig_tx.send(msg); }
                    Msg::Announce { .. } => { let _ = announce_tx.send(msg).await; }
                    Msg::ServerInfo { .. } => { let _ = server_info_tx.send(msg).await; }
                    Msg::Manifest { .. } => { let _ = manifest_tx.send(msg).await; }
                    Msg::Done { .. } => { tracing::debug!("dispatcher routing Done"); let _ = done_tx.send(msg).await; break; }
                    Msg::Deleted { .. } => {}
                    Msg::Error { msg } => { let _ = err_tx.send(msg).await; }
                    Msg::Greeting { .. } => { let _ = greeting_tx.send(msg).await; }
                    Msg::Hello { .. } => {}
                    Msg::Data { .. } | Msg::Copy { .. } | Msg::SendDone { .. } => {}
                }
            }
            drop(greeting_tx);
            drop(plan_tx);
            drop(hash_tx);
            drop(result_tx);
            drop(sig_tx);
            drop(announce_tx);
            drop(server_info_tx);
            drop(manifest_tx);
            drop(done_tx);
            drop(probe_tx);
            drop(err_tx);
        });
        let d = Dispatcher {
            greeting_rx: tokio::sync::Mutex::new(greeting_rx),
            plan_rx: tokio::sync::Mutex::new(plan_rx),
            hash_rx: tokio::sync::Mutex::new(hash_rx),
            announce_rx: tokio::sync::Mutex::new(announce_rx),
            server_info_rx: tokio::sync::Mutex::new(server_info_rx),
            manifest_rx: tokio::sync::Mutex::new(manifest_rx),
            done_rx: tokio::sync::Mutex::new(done_rx),
            probe_rx: tokio::sync::Mutex::new(probe_rx),
            sig_rx: tokio::sync::Mutex::new(sig_rx),
            set,
        };
        (Arc::new(d), result_rx, err_rx)
    }
}

/// Background watcher for peer `Error` frames: records + cancels the session.
fn watch_errors(mut err_rx: mpsc::Receiver<String>, watch: Arc<FatalWatch>, cancel: CancellationToken) {
    tokio::spawn(async move {
        if let Some(msg) = err_rx.recv().await {
            watch.set(msg);
            cancel.cancel();
        }
    });
}

/// Entry point used by both ends.
pub async fn run_session(
    set: Shared,
    ctrl_rx: CtrlRx,
    mut params: RoleParams,
    cancel: CancellationToken,
) -> Result<Summary> {
    let (d, result_rx, err_rx) = Dispatcher::spawn(set.clone(), ctrl_rx, cancel.clone());
    let watch = Arc::new(FatalWatch::default());
    watch_errors(err_rx, watch.clone(), cancel.clone());
    let progress = Arc::new(Progress::new(params.quiet));

    // The server learns its role (and the real root) from the greeting.
    if !params.is_client {
        params = match await_greeting(&set, &d, params).await {
            Ok(p) => p,
            Err(e) => {
                // Tell the peer why (best effort; the link may already be gone).
                let _ = set.bus.send_control(&Msg::error(format!("{e:#}"))).await;
                return Err(e);
            }
        };
    }

    let started = std::time::Instant::now();
    let out = match params.role {
        Role::Sender => {
            crate::send::run_sender(set.clone(), &d, &params, result_rx, &watch, &progress, &cancel)
                .await
        }
        Role::Receiver => {
            drop(result_rx);
            crate::recv::run_receiver(set.clone(), &d, &params, &watch, &progress, &cancel).await
        }
    };
    tracing::debug!(?out, "session role finished");
    // Flush what the role queued (final plans/results/Done) before closing:
    // the stdio serve exits right after this returns.
    set.bus.drain(DRAIN_TIMEOUT).await;
    linkmux::shutdown(&set);
    let mut summary = out.unwrap_or_else(|_e| Summary { files_failed: 1, ..Summary::default() });
    summary.elapsed_secs = started.elapsed().as_secs_f64();
    // Prefer the peer's fatal message as the surfaced error.
    watch.check()?;
    progress.finish(&format!(
        "{} ok, {} failed ({}s)",
        summary.files_done, summary.files_failed, summary.elapsed_secs
    ));
    Ok(summary)
}

/// Server-side: read the greeting, validate, resolve the real root, reply
/// `ServerInfo`; then returns the effective params.
async fn await_greeting(
    set: &Shared,
    d: &Arc<Dispatcher>,
    params: RoleParams,
) -> Result<RoleParams> {
    let msg = tokio::time::timeout(GREETING_TIMEOUT, async {
        d.greeting_rx.lock().await.recv().await
    })
    .await
    .map_err(|_| anyhow::anyhow!("timeout waiting for greeting"))?
    .ok_or_else(|| anyhow::anyhow!("connection dropped before greeting"))?;
    let Msg::Greeting { mode, target, links, include, exclude, delete, allow_delta, merge, fsync, .. } = &msg
    else {
        bail!("expected greeting, got {msg:?}");
    };

    // All client links attach before the greeting; tolerate stragglers.
    if set.bus.count() < (*links as usize) {
        let _ = set.bus.wait_links(*links as usize, LINK_WAIT).await;
    }

    // Resolve the operating directory against our serve root.
    let root = if target.is_empty() {
        params.root.clone()
    } else if target.starts_with('/') {
        PathBuf::from(target)
    } else {
        fsops::join_rel(&params.root, target)
    };
    let role = mode.server_role();
    tracing::info!(
        "serve: mode={mode:?} role={role:?} root={} links={} delete={}",
        root.display(),
        links,
        *mode != CopyMode::Push // informational condensation; delete flag arrives separately
    );

    let mut p2 = params;
    p2.root = root.clone();
    p2.role = role;
    p2.mode = *mode;
    p2.delete = *delete;
    p2.allow_delta = *allow_delta;
    p2.merge_scope = *merge;
    // The server-side sender needs the client's globs and the source list.
    p2.fsync = *fsync;
    p2.filters = fsops::Filters::new(&include, &exclude)
        .map_err(|e| anyhow::anyhow!("invalid filter patterns from client: {e}"))?;
    if role == Role::Sender {
        let base = PathBuf::from(target)
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "root".to_string());
        p2.sources = vec![(base, root.clone())];
    }

    match role {
        Role::Receiver => {
            if !tokio::fs::try_exists(&root).await.unwrap_or(false) {
                tokio::fs::create_dir_all(&root)
                    .await
                    .with_context(|| format!("cannot create destination dir {}", root.display()))?;
            }
            let md = tokio::fs::metadata(&root).await.context("stat destination")?;
            if !md.is_dir() {
                bail!("target {} is not a directory", root.display());
            }
        }
        Role::Sender => {
            let md = tokio::fs::metadata(&root).await
                .with_context(|| format!("remote source root {} missing", root.display()))?;
            if !md.is_dir() {
                bail!("remote source {} is not a directory", root.display());
            }
        }
    }

    set.bus
        .send_control(&Msg::ServerInfo {
            agent: format!("rtransfer {}", env!("CARGO_PKG_VERSION")),
            root: root.to_string_lossy().into_owned(),
        })
        .await?;
    Ok(p2)
}

/// Round-trip probe + Tuning decision. `links` is the number of attached
/// links (including control).
///
/// Only one small probe: it measures the RTT and proves the data path works.
/// Bandwidth is deliberately not probed (it cost 8-64 MiB per session and
/// was too noisy to decide compression; that is now measured at runtime by
/// [`crate::compctl`]), so the estimate's `bps` stays 0 = unknown.
pub async fn probe_and_tune(
    set: &Shared,
    d: &Dispatcher,
    bench: &CpuBench,
    links: u16,
    cancel: &CancellationToken,
    chunk_override: u64,
    block_base_override: u64,
    comp_override: Option<Option<i8>>,
    workers_override: Option<u32>,
) -> (NetEst, Tuning) {
    // Hoisted guard: the probe acknowledgment queue belongs to this call.
    let mut probe_rx = d.probe_rx.lock().await;
    let t0 = std::time::Instant::now();
    let mut rtt_s = 0f64;
    if set.bus.send_data(Msg::Probe { seq: 1, data: vec![0u8; 4096] }).await.is_ok() {
        tokio::select! {
            ack = probe_rx.recv() => {
                if let Some(Msg::ProbeAck { .. }) = ack {
                    rtt_s = t0.elapsed().as_secs_f64();
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(2500)) => {}
            _ = cancel.cancelled() => {}
        }
    }
    let est = NetEst { bps: 0.0, rtt_s };
    (est, Tuning::decide(bench, est, links, workers_override, chunk_override, block_base_override, comp_override))
}

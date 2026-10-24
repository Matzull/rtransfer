//! Receiver role: evals destinations, sends per-file plans (and signatures),
//! then applies incoming data just-in-time (one task per file, spawned by
//! the router when the file's first frame arrives) and reports results.
//!
//! Apply discipline: frames arrive over interleaved links but every file's
//! channel preserves order, so the apply loop keeps an `expected` cursor and
//! a small parking buffer; content is hashed (blake3) as it is applied and
//! checked against the sender's end-to-end hash on `SendDone`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::sync::{mpsc, oneshot, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

use crate::delta::{full_signature, BlockSig};
use crate::ioengine::{IoCtx, Reader, Writer};
use crate::linkmux::Shared;
use crate::progress::Progress;
use crate::protocol::{Comp, FileEntry, Msg, Outcome, PlanAction, ABORT_HASH};
use crate::session::{
    Dispatcher, FatalWatch, RoleParams, Summary, MANIFEST_TIMEOUT, SUMMARY_TIMEOUT,
};
use crate::tuner::{block_for, NetEst};
use crate::{chunk, fsops};

/// Copy application read/write slice.
const COPY_SLICE: u64 = 1024 * 1024;

/// How the receiver will apply (or not) each planned file.
#[derive(Debug, Clone)]
enum Disposition {
    Skip,
    /// The sender may reuse the basis we advertised via `SigData`.
    Delta,
    Whole,
}

/// Context shared between the eval loop and the gatekeepers.
struct RecvCtx {
    set: Shared,
    root: PathBuf,
    io: IoCtx,
    chunk: u64,
    entry_of: HashMap<u32, FileEntry>,
    dispositions: Arc<Mutex<HashMap<u32, Disposition>>>,
    hash_mux: Arc<HashMux>,
    stats: Arc<RecvStats>,
    progress: Arc<Progress>,
    total_files: u64,
    deck: Arc<Deck>,
    cancel: CancellationToken,
    /// `--block-bytes` override; 0 = auto per file.
    block_override: u64,
    net: NetEst,
    allow_delta: bool,
    sig_workers: usize,
    /// fsync the part file before the rename (opt-in via --fsync).
    fsync: bool,
    /// Bounds local destination reads during eval (hashes, signatures).
    /// Never held while waiting on the sender: the sender walks files in
    /// manifest order, so a permit parked on a later file's reply could
    /// starve the file it is waiting for (deadlock).
    eval_permits: Arc<Semaphore>,
}

type SharedCtx = Arc<RecvCtx>;

/// Counted wait-until-zero primitive (applies outstanding).
#[derive(Default)]
struct Deck {
    n: AtomicU64,
    notify: Notify,
}

impl Deck {
    fn add(&self) {
        self.n.fetch_add(1, Ordering::Relaxed);
    }
    fn done(&self) {
        self.n.fetch_sub(1, Ordering::Relaxed);
        self.notify.notify_waiters();
    }
    async fn wait_zero(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.notify.notified();
            if self.n.load(Ordering::Relaxed) == 0 {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.n.load(Ordering::Relaxed) == 0;
            }
        }
    }
}

#[derive(Debug, Default)]
struct RecvStats {
    files_done: AtomicU64,
    files_failed: AtomicU64,
    files_skipped: AtomicU64,
    /// Content bytes applied (literals + copies).
    bytes_done: AtomicU64,
    bytes_skipped: AtomicU64,
    /// Partial progress of files that failed mid-apply, for the overall bar.
    bytes_partial: AtomicU64,
    /// Files with an apply task running right now (diagnostics).
    applying: AtomicU64,
}

impl RecvStats {
    fn paint(&self, progress: &Progress, total_files: u64) {
        let done = self.files_done.load(Ordering::Relaxed)
            + self.files_failed.load(Ordering::Relaxed)
            + self.files_skipped.load(Ordering::Relaxed);
        let bytes = self.bytes_done.load(Ordering::Relaxed)
            + self.bytes_skipped.load(Ordering::Relaxed)
            + self.bytes_partial.load(Ordering::Relaxed);
        progress.set(done, total_files, bytes);
    }
}

/// `FileHash` fan-out (they arrive on one shared queue; verifies overlap).
#[derive(Default)]
struct HashMux {
    regs: Mutex<HashMap<u32, oneshot::Sender<[u8; 32]>>>,
}

impl HashMux {
    fn register(self: &Arc<Self>, id: u32) -> oneshot::Receiver<[u8; 32]> {
        let (tx, rx) = oneshot::channel();
        self.regs.lock().unwrap().insert(id, tx);
        rx
    }
}

fn spawn_hash_mux(d: Arc<Dispatcher>, mux: Arc<HashMux>, cancel: CancellationToken) {
    tokio::spawn(async move {
        let mut hash_rx = d.hash_rx.lock().await;
        loop {
            let m = tokio::select! {
                m = hash_rx.recv() => m,
                _ = cancel.cancelled() => None,
            };
            let Some(Msg::FileHash { id, hash }) = m else {
                // Dropping the senders wakes every waiting verify with an error.
                mux.regs.lock().unwrap().clear();
                return;
            };
            if let Some(tx) = mux.regs.lock().unwrap().remove(&id) {
                let _ = tx.send(hash);
            }
        }
    });
}

/// Hash exactly `size` bytes of a local file through the IO engine.
async fn hash_file_exact(ctx: &IoCtx, path: &Path, size: u64, io_chunk: u64) -> Result<[u8; 32]> {
    let rdr = Reader::open(ctx, path).await.context("open file for hashing")?;
    let mut h = blake3::Hasher::new();
    let mut off = 0u64;
    while off < size {
        let len = io_chunk.min(size - off);
        let buf = rdr.read_at(off, len).await?;
        if buf.len() as u64 != len {
            bail!("file shrank during hashing");
        }
        h.update(&buf);
        off += len;
    }
    Ok(*h.finalize().as_bytes())
}

/// Positional sync reads used by the rayon-parallel signature scans.
/// Returns exactly `off..off+len` padded/truncated by what exists.
fn read_range_blocking(path: &PathBuf, off: u64, len: u64) -> std::io::Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path)?;
    f.seek(SeekFrom::Start(off))?;
    let mut buf = vec![0u8; len as usize];
    let mut done = 0usize;
    while done < buf.len() {
        let n = f.read(&mut buf[done..])?;
        if n == 0 {
            break;
        }
        done += n;
    }
    buf.truncate(done);
    Ok(buf)
}

pub async fn run_receiver(
    set: Shared,
    d: &Arc<Dispatcher>,
    params: &RoleParams,
    watch: &Arc<FatalWatch>,
    progress: &Arc<Progress>,
    cancel: &CancellationToken,
) -> Result<Summary> {
    let links = set.bus.count().max(1) as u16;

    // The receiver never compresses: tuning only needs the core count.
    let bench = crate::bench::cores_only();
    let (net, tuning) = crate::session::probe_and_tune(
        &set,
        d,
        &bench,
        links,
        cancel,
        params.chunk_override,
        params.block_base_override,
        params.comp_override,
        params.workers_override,
    )
    .await;
    tracing::info!(net_mbps = net.bps / 1024.0 / 1024.0, ?tuning, "tuning decided");

    let io_ctx = IoCtx::new(params.io, tuning.chunk);
    let root = params.root.clone();

    // The manifest comes first: every plan decision depends on it.
    let manifest = tokio::select! {
        m = tokio::time::timeout(MANIFEST_TIMEOUT, async {
            d.manifest_rx.lock().await.recv().await
        }) => m
            .map_err(|_| anyhow::anyhow!("timed out waiting for the manifest"))?,
        _ = cancel.cancelled() => bail!("cancelled while awaiting manifest"),
    };
    let Some(Msg::Manifest { files }) = manifest else {
        bail!("connection dropped before the manifest arrived");
    };
    tracing::debug!("manifest received: {} files ({})", files.len(), root.display());
    if files.is_empty() {
        let _ = set.bus.send_control(&Msg::Done { failed: false }).await;
        return Ok(Summary { net_bps: net.bps, ..Summary::default() });
    }

    let stats = Arc::new(RecvStats::default());
    let total_files = files.len() as u64;
    let total_bytes: u64 = files.iter().map(|e| e.size).sum();
    progress.start(total_files, total_bytes, "recv");

    let hash_mux = Arc::new(HashMux::default());
    spawn_hash_mux(d.clone(), hash_mux.clone(), cancel.clone());

    let entry_of: HashMap<u32, FileEntry> = files.iter().map(|e| (e.id, e.clone())).collect();
    let deck = Arc::new(Deck::default());
    let dispositions: Arc<Mutex<HashMap<u32, Disposition>>> = Arc::default();

    let ctx = Arc::new(RecvCtx {
        set: set.clone(),
        root: root.clone(),
        io: io_ctx,
        chunk: tuning.chunk,
        entry_of,
        dispositions: dispositions.clone(),
        hash_mux,
        stats: stats.clone(),
        progress: progress.clone(),
        total_files,
        deck: deck.clone(),
        cancel: cancel.clone(),
        block_override: tuning.block,
        net,
        allow_delta: params.allow_delta,
        sig_workers: tuning.workers.max(1) as usize,
        fsync: params.fsync,
        // A few thousand concurrent hash/signature readers hammer a slow
        // destination disk with random I/O and wreck the apply writers too;
        // bound the eval work to the decided worker count (with headroom).
        eval_permits: Arc::new(Semaphore::new((tuning.workers.max(1) as usize) * 2)),
    });

    // Lazily spawn a gatekeeper when a file's first frame arrives.
    let gate_ctx = ctx.clone();
    set.router.on_new_file(move |id| {
        tokio::spawn(gatekeeper(gate_ctx.clone(), id));
    });

    // Live receiver-side progress for the operator: apply bytes, file count
    // and files currently being written, every few seconds (surfaced in the
    // client log over SSH stderr). Distinguishes "slow disk" (applying stays
    // at the sender's worker count) from "pipeline stall" (applying ~0).
    let tick_stats = stats.clone();
    let tick_ctx = ctx.clone();
    let tick = tokio::spawn(async move {
        let mut int = tokio::time::interval(Duration::from_secs(5));
        let mut last = (0u64, 0u64);
        loop {
            tokio::select! {
                _ = int.tick() => {
                    let b = tick_stats.bytes_done.load(Ordering::Relaxed);
                    let f = tick_stats.files_done.load(Ordering::Relaxed);
                    if (b, f) != last {
                        tracing::info!(
                            applied_mb = b / (1024 * 1024),
                            files_done = f,
                            applying = tick_stats.applying.load(Ordering::Relaxed),
                            "receiver apply progress"
                        );
                        last = (b, f);
                    }
                }
                _ = tick_ctx.cancel.cancelled() => break,
            }
        }
    });

    // ── eval loop: decide + emit plans (hashes/signatures in parallel) ─
    let mut js: JoinSet<Result<()>> = JoinSet::new();
    for entry in &files {
        watch.check()?;
        js.spawn(eval_file(ctx.clone(), entry.clone()));
    }
    while let Some(res) = js.join_next().await {
        res.map_err(|e| anyhow::anyhow!("plan task crashed: {e:#}"))??;
    }
    watch.check()?;
    tracing::debug!("receiver: plans emitted, awaiting applications (deck n={})", deck.n.load(Ordering::Relaxed));

    // Every planned (non-skip) file holds a +1 on the deck; its gatekeeper
    // releases it after the FileResult is sent.
    if !deck.wait_zero(SUMMARY_TIMEOUT).await {
        bail!("timed out waiting for file applications");
    }
    tracing::debug!("receiver: all applications applied; finalizing");
    tick.abort();
    stats.paint(progress, total_files);

    let mut summary = Summary {
        files_done: stats.files_done.load(Ordering::Relaxed),
        files_failed: stats.files_failed.load(Ordering::Relaxed),
        files_skipped: stats.files_skipped.load(Ordering::Relaxed),
        bytes_literal: stats.bytes_done.load(Ordering::Relaxed),
        bytes_skipped: stats.bytes_skipped.load(Ordering::Relaxed),
        bytes_wire: 0, // wire-level stats are the sender's view
        net_bps: net.bps,
        ..Summary::default()
    };

    // ── delete extras, scoped to the transferred roots ────────────────
    if params.delete {
        // Tree-merge sessions own the whole target dir; forest sessions own
        // one named subtree per source (identified by its first rel part).
        let scope = if params.merge_scope {
            DeleteScope::WholeDir
        } else {
            DeleteScope::NameForest
        };
        let deleted = delete_extras_under_bases(&root, &files, scope).await;
        if !deleted.is_empty() {
            tracing::info!("deleted {} extra file(s)", deleted.len());
            let _ = set.bus.send_control(&Msg::Deleted { rels: deleted.clone() }).await;
        }
        summary.deleted = deleted;
    }

    let _ = set.bus.send_control(&Msg::Done { failed: summary.files_failed > 0 }).await;
    tracing::debug!("receiver: Done sent");
    Ok(summary)
}

// ─── per-file plan decisions ──────────────────────────────────────────────

/// Decide and emit the plan for one file. Expensive parts (destination
/// hashing, signature computation) run concurrently across files — bounded
/// by the caller so a slow destination disk is not flooded with random I/O.
async fn eval_file(ctx: SharedCtx, entry: FileEntry) -> Result<()> {
    let permit = ctx
        .eval_permits
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| anyhow::anyhow!("eval semaphore closed"))?;
    let dest = fsops::join_rel(&ctx.root, &entry.rel);
    let md = tokio::fs::metadata(&dest).await.ok();
    let is_file = md.as_ref().map(|m| m.is_file()).unwrap_or(false);
    let dest_len = md.as_ref().map(|m| m.len()).unwrap_or(0);

    // Quick path: same size + same mtime → skip outright. Must match the
    // manifest units (unix milliseconds; rsync-style 1-second mtimes would
    // false-fail and push every file through the expensive Verify path).
    let dest_mtime = md
        .as_ref()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64);
    if is_file && dest_mtime.is_some() && dest_mtime == entry.mtime && dest_len == entry.size {
        ctx.dispositions.lock().unwrap().insert(entry.id, Disposition::Skip);
        skip_counted(&ctx, entry.size);
        send_plan(&ctx, entry.id, PlanAction::Skip).await?;
        return Ok(());
    }

    if !is_file {
        // Missing (or a directory in its place): whole-file only.
        ctx.deck.add();
        ctx.dispositions.lock().unwrap().insert(entry.id, Disposition::Whole);
        send_plan(&ctx, entry.id, PlanAction::Whole).await?;
        return Ok(());
    }

    if dest_len == entry.size {
        return eval_verify(ctx, entry, dest, dest_len, permit).await;
    }

    if ctx.allow_delta {
        make_delta_plan(ctx, entry, dest, dest_len).await
    } else {
        ctx.deck.add();
        ctx.dispositions.lock().unwrap().insert(entry.id, Disposition::Whole);
        send_plan(&ctx, entry.id, PlanAction::Whole).await?;
        Ok(())
    }
}

/// Size-equal file: hash ours and send it inside `Plan::Verify`; the sender
/// compares locally (no round trip when equal) and always answers with its
/// own `FileHash`. Equal → Skip with the mtime fixed so the next run
/// quick-skips; different → follow up with SigData+Delta or Whole.
async fn eval_verify(
    ctx: SharedCtx,
    entry: FileEntry,
    dest: PathBuf,
    dest_len: u64,
    permit: OwnedSemaphorePermit,
) -> Result<()> {
    let ours = match hash_file_exact(&ctx.io, &dest, dest_len, ctx.chunk).await {
        Ok(h) => h,
        Err(e) => {
            // Unreadable destination: no basis to verify or delta against.
            tracing::warn!("hashing destination {}: {e:#}", entry.rel);
            ctx.deck.add();
            ctx.dispositions.lock().unwrap().insert(entry.id, Disposition::Whole);
            return send_plan(&ctx, entry.id, PlanAction::Whole).await;
        }
    };
    let rx = ctx.hash_mux.register(entry.id);
    send_plan(&ctx, entry.id, PlanAction::Verify { hash: ours }).await?;
    // The sender only reaches this file in manifest order; waiting with the
    // permit held could starve the earlier file it is blocked on.
    drop(permit);

    let their = tokio::select! {
        r = rx => r.map_err(|_| anyhow::anyhow!("control link closed during verify of {}", entry.rel))?,
        _ = ctx.cancel.cancelled() => bail!("cancelled during verify"),
    };

    if ours == their {
        // The sender skipped it already; adopt the source mtime so the
        // size+mtime quick check succeeds next time.
        if let Some(millis) = entry.mtime {
            fsops::set_mtime_millis(&dest, millis);
        }
        ctx.dispositions.lock().unwrap().insert(entry.id, Disposition::Skip);
        skip_counted(&ctx, entry.size);
        return Ok(());
    }
    if ctx.allow_delta {
        let _permit = ctx
            .eval_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("eval semaphore closed"))?;
        make_delta_plan(ctx, entry, dest, dest_len).await
    } else {
        ctx.deck.add();
        ctx.dispositions.lock().unwrap().insert(entry.id, Disposition::Whole);
        send_plan(&ctx, entry.id, PlanAction::Whole).await?;
        Ok(())
    }
}

async fn send_plan(ctx: &SharedCtx, id: u32, action: PlanAction) -> Result<()> {
    tracing::debug!(id, ?action, "sending plan");
    ctx.set
        .bus
        .send_control(&Msg::Plan { id, action })
        .await
        .map_err(|e| anyhow::anyhow!("send failed: {e}"))
}

/// Count one skipped file into the stats + overall bar.
fn skip_counted(ctx: &SharedCtx, size: u64) {
    ctx.stats.files_skipped.fetch_add(1, Ordering::Relaxed);
    ctx.stats.bytes_skipped.fetch_add(size, Ordering::Relaxed);
    ctx.stats.paint(&ctx.progress, ctx.total_files);
}

/// Compute the destination's signature, store the disposition, then announce
/// SigData followed by Plan::Delta (in that wire order).
async fn make_delta_plan(
    ctx: SharedCtx,
    entry: FileEntry,
    dest: PathBuf,
    dest_len: u64,
) -> Result<()> {
    let block = block_for(ctx.block_override, dest_len.max(entry.size), ctx.net);
    let workers = ctx.sig_workers;
    let job = tokio::task::spawn_blocking({
        let dest = dest.clone();
        move || -> std::io::Result<Vec<BlockSig>> {
            full_signature(
                |off, len| read_range_blocking(&dest, off, len),
                dest_len,
                block,
                workers,
            )
        }
    });
    let blocks = tokio::select! {
        r = job => r
            .map_err(|e| anyhow::anyhow!("signature task crashed: {e}"))?
            .map_err(|e| anyhow::anyhow!("signature computation failed: {e}"))?,
        _ = ctx.cancel.cancelled() => bail!("cancelled during signature computation"),
    };
    ctx.dispositions
        .lock()
        .unwrap()
        .insert(entry.id, Disposition::Delta);
    ctx.deck.add();
    ctx.set
        .bus
        .send_control(&Msg::SigData { id: entry.id, block_len: block, blocks })
        .await?;
    send_plan(&ctx, entry.id, PlanAction::Delta).await?;
    Ok(())
}

// ─── gatekeeper: apply one file as its frames stream in ───────────────────

/// One parked frame waiting for its `seq` (raw payload; decoded on apply).
enum Frame {
    Data { offset: u64, comp: Comp, orig_len: u64, data: Vec<u8> },
    Copy { new_off: u64, old_off: u64, len: u64 },
}

/// Rolling apply state of one file.
struct ApplyState {
    dest: PathBuf,
    part: PathBuf,
    writer: Option<Writer>,
    old: Option<Reader>,
    hasher: blake3::Hasher,
    expected: u64,
    content_len: u64,
}

async fn gatekeeper(ctx: SharedCtx, id: u32) {
    let Some(entry) = ctx.entry_of.get(&id).cloned() else {
        // Unknown id: absorb (and drop) any frames so link readers never
        // stall on a channel nobody consumes.
        let mut rx = match ctx.set.router.take_rx(id).await {
            Some(r) => r,
            None => return,
        };
        while rx.recv().await.is_some() {
            if ctx.cancel.is_cancelled() {
                return;
            }
        }
        return;
    };

    // Claim the file's frame channel right away (pull frames in before the
    // disposition check so a rogue sender cannot block our link readers).
    let rx = match ctx.set.router.take_rx(id).await {
        Some(r) => r,
        None => return, // links went down; nothing to report
    };

    // The disposition is stored before the plan goes out, and frames can
    // only arrive after that plan, so this resolves quickly.
    let gate;
    loop {
        let found = ctx.dispositions.lock().unwrap().get(&id).cloned();
        if let Some(d) = found {
            gate = d;
            break;
        }
        if ctx.cancel.is_cancelled() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if matches!(gate, Disposition::Skip) {
        return; // the sender never streams a skipped file; frames absorbed
    }
    tracing::debug!(id, ?gate, "gatekeeper claiming file channel");
    apply_file(ctx, id, entry, rx).await;
}

async fn apply_file(ctx: SharedCtx, id: u32, entry: FileEntry, mut rx: mpsc::Receiver<Msg>) {
    let dest = fsops::join_rel(&ctx.root, &entry.rel);
    let part = fsops::part_file(&dest);
    let pb = ctx.progress.file(&entry.rel, entry.size);
    let mut st = ApplyState {
        dest,
        part,
        writer: None,
        old: None,
        hasher: blake3::Hasher::new(),
        expected: 0,
        content_len: 0,
    };
    let mut parked: BTreeMap<u64, Frame> = BTreeMap::new();
    let mut finish: Option<(u64, [u8; 32])> = None;
    let mut cancelled = false;
    // No write-slot gating here: a file waiting for a slot would stop
    // draining its channel and stall the link readers for every other file.
    // Concurrency is bounded at the source instead — the sender keeps a
    // file's worker permit until our FileResult arrives.
    ctx.stats.applying.fetch_add(1, Ordering::Relaxed);

    let applied = 'outer: loop {
        let msg = tokio::select! {
            m = rx.recv() => m,
            _ = ctx.cancel.cancelled() => {
                cancelled = true;
                break 'outer Err("session cancelled".to_string());
            }
        };
        let Some(msg) = msg else {
            break 'outer Err("data channel closed before SendDone".to_string());
        };
        // Park out-of-order frames; SendDone may arrive before them when the
        // stream is split across links (cross-link reordering).
        match msg {
            Msg::Data { id: _, seq, offset, comp, orig_len, data } => {
                parked.insert(seq, Frame::Data { offset, comp, orig_len, data });
            }
            Msg::Copy { id: _, seq, new_off, old_off, len } => {
                parked.insert(seq, Frame::Copy { new_off, old_off, len });
            }
            Msg::SendDone { id: _, last_seq, hash } => {
                if hash == ABORT_HASH {
                    break 'outer Err("sender aborted the file".to_string());
                }
                if finish.is_none() {
                    finish = Some((last_seq, hash));
                }
            }
            other => {
                break 'outer Err(format!("unexpected frame in data stream: {other:?}"));
            }
        }
        // Apply the in-order run of parked frames.
        loop {
            let Some(frame) = parked.remove(&st.expected) else { break };
            tracing::debug!(id, seq = st.expected, "applying frame");
            if let Err(e) = apply_one(&ctx, &mut st, pb.as_ref(), frame).await {
                break 'outer Err(e);
            }
            st.expected += 1;
        }
        // Finalize only once the cursor reached the declared end; early
        // SendDone frames simply keep the finish parked.
        if let Some((last_seq, hash)) = finish.take() {
            if st.expected == last_seq {
                tracing::debug!(id, "SendDone: applied {}/{} frames", st.expected, last_seq);
                // End-to-end integrity: the sender hashes the exact content.
                // Zeros are accepted (a producer genuinely could not hash it).
                if hash != [0u8; 32] {
                    let actual = *st.hasher.finalize().as_bytes();
                    if actual != hash {
                        break 'outer Err("content hash mismatch after apply".to_string());
                    }
                }
                break 'outer Ok(());
            }
            finish = Some((last_seq, hash));
            tracing::debug!(id, "SendDone parked: applied {}/{} frames", st.expected, last_seq);
        }
    };

    let outcome = match (applied, cancelled) {
        (Ok(()), _) => match finalize(ctx.as_ref(), &mut st, &entry).await {
            Ok(()) => {
                tracing::debug!(id, rel = %entry.rel, "installed");
                ctx.stats.files_done.fetch_add(1, Ordering::Relaxed);
                ctx.stats.bytes_done.fetch_add(entry.size, Ordering::Relaxed);
                Outcome::Ok
            }
            Err(e) => {
                fsops::cleanup_part(&st.part).await;
                ctx.stats.files_failed.fetch_add(1, Ordering::Relaxed);
                tracing::warn!("install of {} failed: {e:#}", entry.rel);
                Outcome::Failed(format!("install failed: {e:#}"))
            }
        },
        (Err(e), _) => {
            if let Some(w) = st.writer.take() {
                let _ = w.close(None).await;
            }
            fsops::cleanup_part(&st.part).await;
            ctx.stats.files_failed.fetch_add(1, Ordering::Relaxed);
            ctx.stats
                .bytes_partial
                .fetch_add(st.content_len.min(entry.size), Ordering::Relaxed);
            tracing::warn!("apply of {} failed: {e}", entry.rel);
            Outcome::Failed(if cancelled { "cancelled (send aborted)".into() } else { e })
        }
    };
    ctx.stats.paint(&ctx.progress, ctx.total_files);
    if let Some(pb) = pb {
        match &outcome {
            Outcome::Ok => {
                // Finished bars linger forever in a MultiProgress; retire them.
                pb.finish_and_clear();
            }
            Outcome::Failed(e) => pb.finish_with_message(format!("failed: {e}")),
        }
    }
    ctx.stats.applying.fetch_sub(1, Ordering::Relaxed);
    tracing::debug!(id, ?outcome, "FileResult sent");
    let _ = ctx.set.bus.send_control(&Msg::FileResult { id, outcome }).await;
    // Only now: once the deck hits zero the session sends `Done` and shuts
    // the bus down, so a FileResult still unqueued here would be lost and the
    // sender would wait for it until SUMMARY_TIMEOUT.
    ctx.deck.done();
}

/// Apply one in-order cursor frame.
async fn apply_one(
    ctx: &RecvCtx,
    st: &mut ApplyState,
    pb: Option<&indicatif::ProgressBar>,
    frame: Frame,
) -> Result<(), String> {
    // Lazily open both handles first: the part-file writer is created
    // BEFORE the first old-file read so a broken source never truncates us.
    if st.writer.is_none() {
        if let Err(e) = fsops::mkdirs_parent(&st.part).await {
            return Err(format!("mkdir for {}: {e}", st.part.display()));
        }
        match Writer::create(&ctx.io, &st.part).await {
            Ok(w) => st.writer = Some(w),
            Err(e) => return Err(format!("create part file {}: {e}", st.part.display())),
        }
    }
    if st.old.is_none() && frame_needs_source(&frame) {
        match Reader::open(&ctx.io, &st.dest).await {
            Ok(r) => st.old = Some(r),
            Err(e) => return Err(format!("open old file {}: {e}", st.dest.display())),
        }
    }
    let writer = st.writer.as_ref().unwrap();

    match frame {
        Frame::Data { offset, comp, orig_len, data } => {
            let buf = chunk::decode(comp, orig_len, data)
                .map_err(|e| format!("chunk decode at {offset}: {e}"))?;
            st.hasher.update(&buf);
            st.content_len += buf.len() as u64;
            writer
                .write_at(offset, buf.clone())
                .await
                .map_err(|e| format!("write at {offset}: {e}"))?;
            if let Some(pb) = pb {
                pb.inc(buf.len() as u64);
            }
            Ok(())
        }
        Frame::Copy { new_off, old_off, len } => {
            let old = st.old.as_ref().unwrap();
            let mut done = 0u64;
            while done < len {
                let take = COPY_SLICE.min(len - done);
                let buf = old
                    .read_at(old_off + done, take)
                    .await
                    .map_err(|e| format!("copy read at {}: {e}", old_off + done))?;
                if buf.len() as u64 != take {
                    return Err(format!(
                        "old file shrank during copy (got {}/{} at {})",
                        buf.len(),
                        take,
                        old_off + done
                    ));
                }
                st.hasher.update(&buf);
                writer
                    .write_at(new_off + done, buf)
                    .await
                    .map_err(|e| format!("copy write at {}: {e}", new_off + done))?;
                done += take;
                if let Some(pb) = pb {
                    pb.inc(take);
                }
            }
            st.content_len += len;
            Ok(())
        }
    }
}

/// A `Copy` frame reads the (original) destination file as its source.
fn frame_needs_source(frame: &Frame) -> bool {
    match frame {
        Frame::Data { .. } => false,
        Frame::Copy { .. } => true,
    }
}

/// Close out the part file (fsync + exact length) and install it.
async fn finalize(ctx: &RecvCtx, st: &mut ApplyState, entry: &FileEntry) -> Result<()> {
    match st.writer.take() {
        Some(w) => {
            // Off by default (rsync's default too): flash media pays far
            // more for the flush than for the write itself.
            if ctx.fsync {
                w.sync_data().await.context("fsync part file")?;
            }
            w.close(Some(st.content_len)).await.context("close part file")?;
        }
        None => {
            // Zero-byte file: materialize the (empty) part explicitly so the
            // rename below has something to install. No frame ever ran
            // `apply_one`, so its directory may not exist yet either.
            fsops::mkdirs_parent(&st.part)
                .await
                .with_context(|| format!("mkdir for {}", st.part.display()))?;
            if let Ok(w) = Writer::create(&ctx.io, &st.part).await {
                w.close(None).await.context("close empty part file")?;
            }
            if !tokio::fs::try_exists(&st.part).await.unwrap_or(false) {
                std::fs::write(&st.part, b"").context("create empty part file")?;
            }
        }
    }
    fsops::install_file(&st.part, &st.dest, entry.mtime)
        .await
        .context("install part over destination")?;
    Ok(())
}

// ─── --delete scan ────────────────────────────────────────────────────────

/// How `--delete` bounds its search under `root`.
#[derive(Clone, Copy, Debug, PartialEq)]
enum DeleteScope {
    /// The target directory itself is the transferred tree (merge mode).
    WholeDir,
    /// Each equivalent-named subtree under `root` is a transferred tree.
    NameForest,
}

/// Files under `root` living inside one of the manifest's transfer roots but
/// missing from the manifest itself.
async fn delete_extras_under_bases(
    root: &Path,
    files: &[FileEntry],
    scope: DeleteScope,
) -> Vec<String> {
    let mut bases: HashSet<String> = HashSet::new();
    let mut kept: HashSet<String> = HashSet::new();
    for e in files {
        kept.insert(e.rel.clone());
        if scope == DeleteScope::NameForest {
            if let Some(base) = e.rel.split('/').next() {
                bases.insert(base.to_string());
            }
        }
    }
    let mut candidates: Vec<String> = Vec::new();
    for x in WalkDir::new(root).follow_links(false) {
        let Ok(x) = x else { continue };
        if !x.file_type().is_file() {
            continue;
        }
        let rel = match x.path().strip_prefix(root) {
            Ok(r) => fsops::rel_string(r),
            Err(_) => continue,
        };
        if rel.ends_with(".rpart") {
            continue; // our own detached temporary files
        }
        let in_scope = scope == DeleteScope::WholeDir
            || rel.split('/').next().map(|first| bases.contains(first)).unwrap_or(false);
        if in_scope && !kept.contains(&rel) {
            candidates.push(rel);
        }
    }
    let deleted = fsops::delete_extras(root, &candidates).await;
    // Prune directories the deletions emptied, but stay inside the
    // transferred scope so untouched sibling subtrees survive forest mode.
    match scope {
        DeleteScope::WholeDir => fsops::prune_empty_dirs(root).await,
        DeleteScope::NameForest => {
            let mut names: Vec<String> = bases.into_iter().collect();
            names.sort();
            for name in names {
                let p = root.join(name);
                if p.is_dir() {
                    fsops::prune_empty_dirs(&p).await;
                }
            }
        }
    }
    deleted
}

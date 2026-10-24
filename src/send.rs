//! Sender role: manifest, plans, delta scans and chunk streaming.
//!
//! Per file the sender either
//!
//! * streams **whole-file** chunks (read → compress → round-robin send to
//!   the links, sized by the tuner, hashing content inline in `seq` order), or
//! * **delta-encodes** against the receiver's signature (rayon-parallel
//!   rolling scan on the blocking pool) and streams literal chunks plus
//!   `Copy` instructions, with a full-file blake3 computed concurrently for
//!   end-to-end verification.
//!
//! Chunk sends are ordered (`seq`), but links are independent — the seq
//! discipline lets the receiver reassemble exactly, while the writer-bus
//! round-robin keeps every link busy and backpressures the pipeline.
//! Completed chunks are parked keyed by `seq` and applied at the moving
//! cursor, so out-of-order completions across parallel chunk tasks are fine.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use crate::compctl::{self, CompCtl};
use crate::delta::{scan_for_delta, BlockSig, SigIndex};
use crate::ioengine::{IoCtx, Reader};
use crate::linkmux::Shared;
use crate::progress::Progress;
use crate::protocol::{Comp, FileEntry, Msg, Outcome, PlanAction, ABORT_HASH};
use crate::session::{Dispatcher, FatalWatch, RoleParams, Summary, SUMMARY_TIMEOUT};
use crate::tuner::{NetEst, Tuning};
use crate::{chunk, fsops};

/// Timeout for one signature wait (the receiver sends SigData before Plan).
const SIG_TIMEOUT: Duration = Duration::from_secs(120);

/// All bookkeeping shared by the plan loop and per-file tasks.
#[derive(Debug, Default)]
struct Stats {
    files_done: AtomicU64,
    files_failed: AtomicU64,
    files_skipped: AtomicU64,
    bytes_literal: AtomicU64,
    bytes_wire: AtomicU64,
    bytes_skipped: AtomicU64,
    /// Partial progress of files that failed mid-transfer.
    bytes_partial: AtomicU64,
    /// FileResult frames still expected from the receiver.
    pending_results: AtomicU64,
    /// Wakes `results_all_in` whenever pending_results changes.
    results_notify: tokio::sync::Notify,
}

impl Stats {
    /// Refresh the overall progress bar from the shared counters.
    fn paint(&self, progress: &Progress, total_files: u64) {
        let done = self.files_done.load(Ordering::Relaxed)
            + self.files_failed.load(Ordering::Relaxed)
            + self.files_skipped.load(Ordering::Relaxed);
        let bytes = self.bytes_literal.load(Ordering::Relaxed)
            + self.bytes_skipped.load(Ordering::Relaxed)
            + self.bytes_partial.load(Ordering::Relaxed);
        progress.set(done, total_files, bytes);
    }
}

/// One finished (encoded) chunk parked until its `seq` reaches the cursor.
struct Pending {
    offset: u64,
    orig_len: u64,
    comp: Comp,
    data: Vec<u8>,
    /// The original bytes when `data` is compressed (hashed at commit time
    /// instead of decompressing `data` again); `None` when `data` is raw.
    raw: Option<Vec<u8>>,
}

/// One file's view of the session codec: once a chunk of this file proved
/// incompressible (media, archives), the rest of the file skips zstd.
#[derive(Clone)]
struct FileCodec {
    ctl: Arc<CompCtl>,
    incompressible: Arc<AtomicBool>,
}

impl FileCodec {
    fn new(ctl: &Arc<CompCtl>) -> Self {
        FileCodec { ctl: ctl.clone(), incompressible: Arc::default() }
    }

    fn pick(&self) -> Comp {
        if self.incompressible.load(Ordering::Relaxed) {
            Comp::None
        } else {
            self.ctl.current()
        }
    }

    fn observe(&self, asked: Comp, got: Comp) {
        if asked != Comp::None && got == Comp::None {
            self.incompressible.store(true, Ordering::Relaxed);
        }
    }
}

/// One splittable transfer work unit, indexed by `seq` position.
#[derive(Debug, Clone, Copy)]
enum Item {
    Copy { new_off: u64, old_off: u64, len: u64 },
    Lit { off: u64, len: u64 },
}

/// Plan buffering: plans arrive for *later* files first because the receiver
/// evaluates files in parallel; signatures travel a different queue.
#[derive(Default)]
struct PlanBook {
    pending: HashMap<u32, PlanAction>,
    pending_sigs: HashMap<u32, (u64, Vec<BlockSig>)>,
}

impl PlanBook {
    /// Wait until a Plan for `id` is available (buffering out-of-order ones).
    async fn next_plan(
        &mut self,
        d: &Arc<Dispatcher>,
        id: u32,
        cancel: &CancellationToken,
    ) -> Result<PlanAction> {
        if let Some(a) = self.pending.remove(&id) {
            return Ok(a);
        }
        let mut plan_rx = d.plan_rx.lock().await;
        loop {
            let m = tokio::select! {
                m = plan_rx.recv() => m,
                _ = cancel.cancelled() => bail!("cancelled while waiting for plans"),
            };
            let Some(m) = m else { bail!("control link closed before plans") };
            match &m {
                Msg::Plan { id: pid, action } => {
                    let pid = *pid;
                    let action = action.clone();
                    if pid == id {
                        return Ok(action);
                    }
                    self.pending.insert(pid, action);
                    continue;
                }
                other => bail!("expected plan, got {other:?}"),
            }
        }
    }

    /// Take the SigData for `id`: drain what's already queued, then wait.
    /// The receiver always sends SigData *before* the matching Plan::Delta.
    async fn take_sig(
        &mut self,
        d: &Arc<Dispatcher>,
        id: u32,
        cancel: &CancellationToken,
    ) -> Result<(u64, Vec<BlockSig>)> {
        if let Some(sig) = self.pending_sigs.remove(&id) {
            return Ok(sig);
        }
        let mut sig_rx = d.sig_rx.lock().await;
        loop {
            match sig_rx.try_recv() {
                Ok(Msg::SigData { id: sid, block_len, blocks }) => {
                    if sid == id {
                        return Ok((block_len, blocks));
                    }
                    self.pending_sigs.insert(sid, (block_len, blocks));
                }
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        let deadline = tokio::time::Instant::now() + SIG_TIMEOUT;
        loop {
            let m = tokio::select! {
                m = sig_rx.recv() => m,
                _ = cancel.cancelled() => bail!("cancelled waiting for signature"),
                _ = tokio::time::sleep_until(deadline) =>
                    bail!("timed out waiting for a file signature"),
            };
            let Some(Msg::SigData { id: sid, block_len, blocks }) = m else {
                bail!("signature stream closed unexpectedly");
            };
            if sid == id {
                return Ok((block_len, blocks));
            }
            self.pending_sigs.insert(sid, (block_len, blocks));
        }
    }
}

/// In-flight pipeline depth for chunk tasks (bounded by link count × 2).
fn pipeline_depth(links: u16) -> usize {
    (links as usize * 2).clamp(2, 16)
}

/// Hash the source file streaming exactly `size` bytes (the manifest's stat).
async fn hash_exact(ctx: &IoCtx, path: &Path, size: u64, io_chunk: u64) -> Result<[u8; 32]> {
    let rdr = Reader::open(ctx, path).await.context("open source for hashing")?;
    let mut h = blake3::Hasher::new();
    let mut off = 0u64;
    while off < size {
        let len = io_chunk.min(size - off);
        let buf = rdr.read_at(off, len).await?;
        if buf.len() as u64 != len {
            bail!("file shrank during hashing (lost {} at offset {off})", len - buf.len() as u64);
        }
        h.update(&buf);
        off += len;
    }
    Ok(*h.finalize().as_bytes())
}

/// Best-effort abort marker: the receiver fails the file as soon as it sees
/// it (whatever its cursor) and cleans up its partial file by itself.
async fn send_abort(set: &Shared, id: u32) {
    let _ = set
        .bus
        .send_data(Msg::SendDone { id, last_seq: 0, hash: ABORT_HASH })
        .await;
}

/// The `server_info_rx` gate is answered by the server once the target dir
/// is valid; gives a fast, clear failure when the target is unusable.
async fn ensure_server_info(d: &Arc<Dispatcher>, cancel: &CancellationToken) -> Result<()> {
    let mut server_info_rx = d.server_info_rx.lock().await;
    let m = tokio::select! {
        m = server_info_rx.recv() => m,
        _ = cancel.cancelled() => bail!("cancelled before server info"),
    };
    let Some(Msg::ServerInfo { agent, root }) = m else {
        bail!("server hung up before confirming the target");
    };
    tracing::info!("remote agent {agent}; confirmed target {root}");
    Ok(())
}

/// Read + encode one chunk (runs as a pipeline task).
async fn chunk_job(
    reader: Arc<Reader>,
    offset: u64,
    len: u64,
    codec: FileCodec,
    _permit: OwnedSemaphorePermit,
) -> std::io::Result<Pending> {
    let buf = reader.read_at(offset, len).await?;
    if buf.len() as u64 != len {
        return Err(std::io::Error::other(format!(
            "file shrank during transfer (read {} of {len} at offset {offset})",
            buf.len()
        )));
    }
    let orig_len = buf.len() as u64;
    let asked = codec.pick();
    if asked == Comp::None {
        return Ok(Pending { offset, orig_len, comp: Comp::None, data: buf, raw: None });
    }
    // zstd on 1 MiB takes milliseconds of CPU: keep it off the async
    // workers, which also drive every link reader and writer.
    let (comp, data, buf) = tokio::task::spawn_blocking(move || {
        chunk::encode(&buf, asked).map(|(comp, data)| (comp, data, buf))
    })
    .await
    .map_err(std::io::Error::other)??;
    codec.observe(asked, comp);
    Ok(match comp {
        Comp::None => Pending { offset, orig_len, comp, data: buf, raw: None },
        _ => Pending { offset, orig_len, comp, data, raw: Some(buf) },
    })
}

/// Send one parked chunk in order: hash the decoded bytes, then ship.
#[allow(clippy::too_many_arguments)]
async fn commit_pending(
    set: &Shared,
    id: u32,
    seq: u64,
    pending: Pending,
    hasher: &mut blake3::Hasher,
    stats: &Stats,
    pb: Option<&indicatif::ProgressBar>,
) -> Result<()> {
    let wire_len = pending.data.len() as u64;
    let orig_len = pending.orig_len;
    hasher.update(pending.raw.as_deref().unwrap_or(&pending.data));
    set.bus
        .send_data(Msg::Data {
            id,
            seq,
            offset: pending.offset,
            comp: pending.comp,
            orig_len,
            data: pending.data,
        })
        .await
        .map_err(|e| anyhow::anyhow!("send failed: {e}"))?;
    stats.bytes_literal.fetch_add(orig_len, Ordering::Relaxed);
    stats.bytes_wire.fetch_add(wire_len, Ordering::Relaxed);
    if let Some(pb) = pb {
        pb.inc(orig_len);
    }
    Ok(())
}

/// Whole-file streaming: reads ahead in a bounded pipeline, encodes, sends
/// in `seq` order, hashing content inline at the cursor.
#[allow(clippy::too_many_arguments)]
async fn send_whole(
    set: &Shared,
    ctx: &Arc<IoCtx>,
    ctl: &Arc<CompCtl>,
    chunk_size: u64,
    entry: &FileEntry,
    path: &Path,
    chunk_sem: &Arc<Semaphore>,
    stats: &Stats,
    pb: Option<&indicatif::ProgressBar>,
    cancel: &CancellationToken,
) -> Result<()> {
    // Empty file: no chunks at all.
    if entry.size == 0 {
        let empty_hash = *blake3::hash(&[]).as_bytes();
        set.bus
            .send_data(Msg::SendDone { id: entry.id, last_seq: 0, hash: empty_hash })
            .await?;
        return Ok(());
    }

    let reader = Arc::new(Reader::open(ctx, path).await.context("open source file")?);
    let codec = FileCodec::new(ctl);
    let mut js = JoinSet::<std::io::Result<(u64, Pending)>>::new();
    let mut hasher = blake3::Hasher::new();
    let mut parked: BTreeMap<u64, Pending> = BTreeMap::new();
    let mut offset = 0u64; // next file offset to read
    let mut next_seq = 0u64; // seq owned by that read
    let mut cursor = 0u64; // next seq to send

    let out: Result<()> = loop {
        // 1) flush everything already parked at the cursor (in order)
        while let Some(p) = parked.remove(&cursor) {
            commit_pending(set, entry.id, cursor, p, &mut hasher, stats, pb).await?;
            cursor += 1;
        }
        if offset >= entry.size && cursor == next_seq {
            break Ok(());
        }

        // 2) spawn more reads while the pipeline has room.
        while offset < entry.size {
            match chunk_sem.clone().try_acquire_owned() {
                Ok(p) => {
                    let len = chunk_size.min(entry.size - offset);
                    let seq = next_seq;
                    next_seq += 1;
                    let reader2 = reader.clone();
                    let off2 = offset;
                    let codec2 = codec.clone();
                    js.spawn(async move {
                        chunk_job(reader2, off2, len, codec2, p)
                            .await
                            .map(|s| (seq, s))
                    });
                    offset += len;
                }
                Err(_) => break,
            }
        }

        // 3) wait for progress: a completed chunk, or (when the file has
        // jobs to read but all pipeline permits are held by other streams)
        // a free permit to spawn the next read.
        let res = if js.is_empty() {
            let acquired = tokio::select! {
                p = chunk_sem.clone().acquire_owned() => Some(p),
                _ = cancel.cancelled() => None,
            };
            match acquired {
                Some(Ok(p)) => {
                    let len = chunk_size.min(entry.size - offset);
                    let seq = next_seq;
                    next_seq += 1;
                    let reader2 = reader.clone();
                    let off2 = offset;
                    let codec2 = codec.clone();
                    js.spawn(async move {
                        chunk_job(reader2, off2, len, codec2, p)
                            .await
                            .map(|s| (seq, s))
                    });
                    offset += len;
                    continue;
                }
                Some(Err(_)) => break Err(anyhow::anyhow!("chunk semaphore closed")),
                None => break Err(anyhow::anyhow!("cancelled")),
            }
        } else {
            tokio::select! {
                r = js.join_next() => r,
                _ = cancel.cancelled() => break Err(anyhow::anyhow!("cancelled")),
            }
        };
        match res {
            Some(Ok(Ok((seq, pending)))) => {
                parked.insert(seq, pending);
            }
            Some(Ok(Err(e))) => break Err(anyhow::anyhow!("read/encode failed: {e}")),
            Some(Err(e)) => break Err(anyhow::anyhow!("chunk task crashed: {e:#}")),
            None => unreachable!("join_next polled only while the set is non-empty"),
        }
    };

    match out {
        Ok(()) => {
            let hash = *hasher.finalize().as_bytes();
            set.bus
                .send_data(Msg::SendDone { id: entry.id, last_seq: cursor, hash })
                .await?;
            Ok(())
        }
        Err(e) => {
            stats.bytes_partial.fetch_add(hasher.count(), Ordering::Relaxed);
            Err(e)
        }
    }
}

/// Delta streaming: scan against the receiver's signature (rayon-parallel),
/// then stream `Copy` frames and chunk-sized literal `Data` frames in order,
/// with a concurrent whole-file hash for end-to-end integrity.
#[allow(clippy::too_many_arguments)]
async fn send_delta(
    set: &Shared,
    ctx: &Arc<IoCtx>,
    ctl: &Arc<CompCtl>,
    chunk_size: u64,
    entry: &FileEntry,
    path: &Path,
    block_len: u64,
    blocks: Vec<BlockSig>,
    chunk_sem: &Arc<Semaphore>,
    stats: &Stats,
    pb: Option<&indicatif::ProgressBar>,
    _net: NetEst,
    tuning: Tuning,
    cancel: &CancellationToken,
) -> Result<()> {
    // Full-file hash task (runs while the scan works). Delta coverage is
    // verified end to end this way too: the receiver hashes what it applies.
    // Aborted on drop, so every early return also stops the hashing.
    let hash_job = AbortOnDropHandle::new(tokio::spawn({
        let ctx = ctx.clone();
        let path = path.to_path_buf();
        let size = entry.size;
        let ck = chunk_size;
        async move { hash_exact(&ctx, &path, size, ck).await }
    }));

    // Parallel rolling scan on the rayon pool (blocking).
    let scan_job = tokio::task::spawn_blocking({
        let path = path.to_path_buf();
        let size = entry.size;
        let block = block_len as usize;
        let workers = tuning.workers.max(1) as usize;
        move || -> std::io::Result<Vec<crate::delta::Instr>> {
            let index = SigIndex::build(block, blocks);
            scan_for_delta(
                |off, len| {
                    use std::io::{Read, Seek, SeekFrom};
                    let mut f = std::fs::File::open(&path)?;
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
                },
                size,
                &index,
                workers,
            )
        }
    });
    let instrs = tokio::select! {
        r = scan_job => r.map_err(|e| anyhow::anyhow!("scan task crashed: {e}"))?
            .map_err(|e| anyhow::anyhow!("delta scan failed: {e}"))?,
        _ = cancel.cancelled() => bail!("cancelled during scan"),
    };

    // Instruction stream → item list (`seq` = item index, in file order).
    let mut items: Vec<Item> = Vec::new();
    for ins in instrs {
        match ins {
            crate::delta::Instr::Copy { new_off, old_off, len } => {
                items.push(Item::Copy { new_off, old_off, len });
            }
            crate::delta::Instr::Literal { new_off, len } => {
                let mut start = new_off;
                let end = new_off + len;
                while start < end {
                    let take = chunk_size.min(end - start);
                    items.push(Item::Lit { off: start, len: take });
                    start += take;
                }
            }
        }
    }
    let total = items.len() as u64;
    let copies: BTreeMap<u64, (u64, u64, u64)> = items
        .iter()
        .enumerate()
        .filter_map(|(i, it)| match it {
            Item::Copy { new_off, old_off, len } => Some((i as u64, (*new_off, *old_off, *len))),
            Item::Lit { .. } => None,
        })
        .collect();

    if entry.size > 0 && copies.is_empty() && total == 0 {
        // Defensive: scan produced nothing for a non-empty file.
        bail!("delta scan produced no instructions");
    }

    let reader = Arc::new(Reader::open(ctx, path).await.context("open source file")?);
    let codec = FileCodec::new(ctl);
    let mut js = JoinSet::<std::io::Result<(u64, Pending)>>::new();
    let mut hasher = blake3::Hasher::new(); // literal-hash is informational only
    let mut parked: BTreeMap<u64, Pending> = BTreeMap::new();
    let mut spawned = 0u64; // items handed off (either spawned or pre-sent copies)
    let mut cursor = 0u64;

    let out: Result<()> = loop {
        // 1) flush everything already ready at the cursor, in order
        while cursor < total {
            if let Some(&c) = copies.get(&cursor) {
                commit_copy(set, entry.id, cursor, c.0, c.1, c.2).await?;
                cursor += 1;
            } else if let Some(p) = parked.remove(&cursor) {
                commit_pending(set, entry.id, cursor, p, &mut hasher, stats, pb).await?;
                cursor += 1;
            } else {
                break;
            }
        }
        if cursor == total {
            break Ok(());
        }
        // 2) hand off more items while the pipeline has room (copies are
        //    zero-cost and count immediately)
        let mut spawned_pending = true;
        while spawned < total {
            match items[spawned as usize] {
                Item::Copy { .. } => spawned += 1,
                Item::Lit { off, len } => {
                    match chunk_sem.clone().try_acquire_owned() {
                        Ok(permit) => {
                            let seq = spawned;
                            let reader2 = reader.clone();
                            let codec2 = codec.clone();
                            js.spawn(async move {
                                chunk_job(reader2, off, len, codec2, permit)
                                    .await
                                    .map(|p| (seq, p))
                            });
                            spawned += 1;
                        }
                        Err(_) => {
                            spawned_pending = spawned < total;
                            break;
                        }
                    }
                }
            }
        }
        if spawned < total && spawned_pending && js.is_empty() {
            // All pipeline permits held by other file streams and nothing in
            // flight here: wait for a permit instead of spinning.
            let acquired = tokio::select! {
                p = chunk_sem.clone().acquire_owned() => Some(p),
                _ = cancel.cancelled() => None,
            };
            match acquired {
                Some(Ok(permit)) => {
                    match items[spawned as usize] {
                        Item::Copy { .. } => spawned += 1,
                        Item::Lit { off, len } => {
                            let seq = spawned;
                            let reader2 = reader.clone();
                            let codec2 = codec.clone();
                            js.spawn(async move {
                                chunk_job(reader2, off, len, codec2, permit)
                                    .await
                                    .map(|p| (seq, p))
                            });
                            spawned += 1;
                        }
                    }
                }
                Some(Err(_)) => break Err(anyhow::anyhow!("chunk semaphore closed")),
                None => break Err(anyhow::anyhow!("cancelled")),
            }
        }
        // 3) wait for the next chunk completion (or cancellation)
        let res = if js.is_empty() {
            // No literal jobs in flight; remaining items are copies (sent in
            // the loop above) — top up and re-check for completion.
            if spawned < total {
                continue;
            }
            // Everything handed off but nothing parked: final flush next round.
            None
        } else {
            tokio::select! {
                r = js.join_next() => r,
                _ = cancel.cancelled() => break Err(anyhow::anyhow!("cancelled")),
            }
        };
        match res {
            Some(Ok(Ok((seq, pending)))) => {
                parked.insert(seq, pending);
            }
            Some(Ok(Err(e))) => break Err(anyhow::anyhow!("read/encode failed: {e}")),
            Some(Err(e)) => break Err(anyhow::anyhow!("chunk task crashed: {e:#}")),
            None => {
                // With copies in flight the ordering flush in step 1 keeps
                // moving; a remaining future will surface there.
                if spawned < total {
                    continue;
                }
                break Err(anyhow::anyhow!("chunk pipeline stalled"));
            }
        }
    };

    out?;
    let hash = tokio::select! {
        h = hash_job => h.unwrap_or(Ok([0u8; 32])).unwrap_or([0u8; 32]),
        _ = cancel.cancelled() => [0u8; 32],
    };
    set.bus
        .send_data(Msg::SendDone { id: entry.id, last_seq: total, hash })
        .await?;
    Ok(())
}

/// Send one `Copy` instruction in order (content stays on the receiver).
async fn commit_copy(set: &Shared, id: u32, seq: u64, new_off: u64, old_off: u64, len: u64) -> Result<()> {
    set.bus
        .send_data(Msg::Copy { id, seq, new_off, old_off, len })
        .await
        .map_err(|e| anyhow::anyhow!("send failed: {e}"))
}

pub async fn run_sender(
    set: Shared,
    d: &Arc<Dispatcher>,
    params: &RoleParams,
    mut result_rx: mpsc::Receiver<Msg>,
    watch: &Arc<FatalWatch>,
    progress: &Arc<Progress>,
    cancel: &CancellationToken,
) -> Result<Summary> {
    // Network goes both ways only after all links attach (client enforces
    // that in the transport setup; the server does it in the greeting).
    let links = set.bus.count().max(1) as u16;

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
    tracing::info!(rtt_ms = net.rtt_s * 1000.0, ?tuning, "tuning decided");
    let comp_ctl = Arc::new(match tuning.comp {
        Comp::Zstd(level) if tuning.auto_comp => CompCtl::auto(level),
        comp => CompCtl::fixed(comp),
    });

    // The receiver validates the target before we push anything heavy.
    ensure_server_info(d, cancel).await?;

    let ctx = Arc::new(IoCtx::new(params.io, tuning.chunk));

    // Manifest: sources scanned, ids assigned, local paths kept for reads.
    let (entries, skipped) = {
        let sources = params.sources.clone();
        let filters = params.filters.clone();
        let follow = params.follow_symlinks;
        let job = tokio::task::spawn_blocking(move || {
            fsops::build_manifest(&sources, &filters, follow)
        });
        tokio::select! {
            r = job => r.map_err(|e| anyhow::anyhow!("manifest task died: {e}"))??,
            _ = cancel.cancelled() => bail!("cancelled while scanning sources"),
        }
    };
    for s in skipped.iter().take(8) {
        tracing::debug!("scan skip: {s}");
    }

    let stats = Arc::new(Stats::default());
    let total_files = entries.len() as u64;
    let total_bytes: u64 = entries.iter().map(|(e, _)| e.size).sum();
    progress.start(total_files, total_bytes, "send");

    set.bus
        .send_control(&Msg::Manifest { files: entries.iter().map(|(e, _)| e.clone()).collect() })
        .await?;
    tracing::debug!("manifest sent: {total_files} files, {total_bytes} bytes");
    // Runtime compression choice from delivered bytes; stops when dropped.
    let _comp_sampler = compctl::spawn_sampler(
        comp_ctl.clone(),
        {
            let stats = stats.clone();
            move || stats.bytes_literal.load(Ordering::Relaxed)
        },
        cancel.clone(),
    );

    let depth = pipeline_depth(links);
    let file_sem = Arc::new(Semaphore::new(tuning.workers.max(1) as usize));
    // A file's worker permit lives here from dispatch until its FileResult:
    // the receiver may write far slower than we send (SD cards), and
    // releasing on "sent" let thousands of files pile up there — each with
    // an open part file (EMFILE) and its frames in memory.
    let in_flight: Arc<std::sync::Mutex<HashMap<u32, OwnedSemaphorePermit>>> = Arc::default();
    let chunk_sem = Arc::new(Semaphore::new(depth));
    let mut book = PlanBook::default();
    let mut js = JoinSet::<Result<()>>::new();

    // Keep the overall bar alive between file completions: with few large
    // files the result-driven paints are minutes apart even though chunks
    // stream every millisecond. Repaint twice a second until the very end.
    let stop_paint = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let painter = tokio::spawn({
        let stats = stats.clone();
        let progress = progress.clone();
        let stop = stop_paint.clone();
        async move {
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(500)).await;
                stats.paint(&progress, total_files);
            }
        }
    });

    // ── results drainer ───────────────────────────────────────────────
    // Runs alongside the plan loop: the dispatcher routes every control
    // frame through one task, so an undrained result queue would block it
    // and starve the plan loop of the very plans it is waiting for.
    tokio::spawn({
        let stats = stats.clone();
        let progress = progress.clone();
        let cancel = cancel.clone();
        let in_flight = in_flight.clone();
        let total = total_files;
        async move {
            loop {
                let m = tokio::select! {
                    m = result_rx.recv() => m,
                    _ = cancel.cancelled() => None,
                };
                let Some(m) = m else { break };
                match m {
                    Msg::FileResult { id, outcome } => {
                        tracing::debug!("drained FileResult id={id} {outcome:?}");
                        in_flight.lock().unwrap().remove(&id); // frees a worker slot
                        match outcome {
                            Outcome::Ok => {
                                stats.files_done.fetch_add(1, Ordering::Relaxed);
                            }
                            Outcome::Failed(e) => {
                                stats.files_failed.fetch_add(1, Ordering::Relaxed);
                                tracing::debug!("receiver failed file {id}: {e}");
                            }
                        }
                    }
                    _ => {}
                }
                stats.pending_results.fetch_sub(1, Ordering::Relaxed);
                stats.results_notify.notify_waiters();
                stats.paint(&progress, total);
            }
        }
    });

    // ── plan loop ─────────────────────────────────────────────────────
    for (entry, path) in &entries {
        watch.check()?;
        let mut action = book.next_plan(d, entry.id, cancel).await?;
        let mut revisit = false;
        loop {
            match action {
                PlanAction::Skip => {
                    stats.files_skipped.fetch_add(1, Ordering::Relaxed);
                    stats.bytes_skipped.fetch_add(entry.size, Ordering::Relaxed);
                    stats.paint(progress, total_files);
                    break;
                }
                PlanAction::Verify { hash: theirs } => {
                    if revisit {
                        bail!("plan exchange looped on file {:?}", entry.rel);
                    }
                    revisit = true;
                    // An unreadable source must not end the session: the
                    // sentinel never matches, so the receiver plans a
                    // transfer and the file task fails it cleanly.
                    let hash = match hash_exact(&ctx, path, entry.size, tuning.chunk).await {
                        Ok(h) => h,
                        Err(e) => {
                            tracing::warn!("hashing {} failed: {e:#}", entry.rel);
                            ABORT_HASH
                        }
                    };
                    // The receiver always needs our hash: equal → it fixes
                    // the mtime, different → it sends the follow-up plan.
                    set.bus.send_control(&Msg::FileHash { id: entry.id, hash }).await?;
                    watch.check()?;
                    action = if hash == theirs {
                        PlanAction::Skip // no reply to wait for
                    } else {
                        book.next_plan(d, entry.id, cancel).await?
                    };
                }
                PlanAction::Whole | PlanAction::Delta => {
                    let (block_len, blocks) = if action == PlanAction::Delta {
                        book.take_sig(d, entry.id, cancel).await?
                    } else {
                        (tuning.block, Vec::new())
                    };
                    // The permit caps how many files are in flight end to end
                    // (sending or still being applied by the receiver).
                    let permit = tokio::select! {
                        p = file_sem.clone().acquire_owned() =>
                            p.map_err(|_| anyhow::anyhow!("worker pool closed"))?,
                        _ = cancel.cancelled() =>
                            bail!("cancelled before sending {:?}", entry.rel),
                    };
                    // Parked before the first frame goes out, so the result
                    // can never arrive ahead of it.
                    in_flight.lock().unwrap().insert(entry.id, permit);
                    stats.pending_results.fetch_add(1, Ordering::Relaxed);
                    spawn_file_task(
                        &mut js, set.clone(), ctx.clone(), chunk_sem.clone(), stats.clone(),
                        progress.clone(), cancel.clone(), entry.clone(),
                        path.clone(), action, block_len, blocks, tuning, net, comp_ctl.clone(),
                    );
                    break;
                }
            }
        }
    }

    // ── collect: in-flight transfers ──────────────────────────────────
    while let Some(res) = js.join_next().await {
        if let Err(e) = res {
            tracing::warn!("transfer task crashed: {e:#}");
            stats.files_failed.fetch_add(1, Ordering::Relaxed);
            // The receiver's hash-compare abort marker doubles as cleanup.
        }
        watch.check()?;
    }
    stats.paint(progress, total_files);
    stop_paint.store(true, Ordering::Relaxed);
    painter.abort();
    tracing::debug!(
        "sender: all file tasks joined; pending_results={}",
        stats.pending_results.load(Ordering::Relaxed)
    );

    let deadline = tokio::time::Instant::now() + SUMMARY_TIMEOUT;
    loop {
        // Enable FIRST (registration without a waker), then re-check the
        // counter: every notify_waiters() after this point is wake-guaranteed.
        let mut notified = std::pin::pin!(stats.results_notify.notified());
        let woke_early = notified.as_mut().enable();
        if woke_early {
            // A stale wakeup already fired this waiter out of the queue; only
            // leave when the counter confirms zero — else re-arm.
            if stats.pending_results.load(Ordering::Relaxed) == 0 {
                break;
            }
            watch.check()?;
            continue;
        }
        if stats.pending_results.load(Ordering::Relaxed) == 0 {
            break;
        }
        tokio::select! {
            _ = notified.as_mut() => {}
            _ = tokio::time::sleep_until(deadline) =>
                bail!("timed out waiting for receiver results"),
            _ = cancel.cancelled() => bail!("cancelled while awaiting results"),
        }
        watch.check()?;
    }
    tracing::debug!("sender: plan/results drained; sending Done");

    // Tell the receiver we are done; wait for its Done before teardown.
    // The summary must be snapshotted only after every FileResult drained —
    // building it earlier reported half the transfer as "ok" under races.
    let _ = set.bus.send_control(&Msg::Done { failed: stats.files_failed.load(Ordering::Relaxed) > 0 }).await;
    let done_got = tokio::time::timeout(SUMMARY_TIMEOUT, d.done_rx.lock().await.recv()).await;
    tracing::debug!("sender: peer Done awaited → {done_got:?}");

    let summary = Summary {
        files_done: stats.files_done.load(Ordering::Relaxed),
        files_failed: stats.files_failed.load(Ordering::Relaxed),
        files_skipped: stats.files_skipped.load(Ordering::Relaxed),
        bytes_literal: stats.bytes_literal.load(Ordering::Relaxed),
        bytes_wire: stats.bytes_wire.load(Ordering::Relaxed),
        bytes_skipped: stats.bytes_skipped.load(Ordering::Relaxed),
        net_bps: net.bps,
        deleted: Vec::new(),
        ..Summary::default()
    };
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
#[allow(unused_variables)]
fn spawn_file_task(
    js: &mut JoinSet<Result<()>>,
    set: Shared,
    ctx: Arc<IoCtx>,
    chunk_sem: Arc<Semaphore>,
    stats: Arc<Stats>,
    progress: Arc<Progress>,
    cancel: CancellationToken,
    entry: FileEntry,
    path: PathBuf,
    action: PlanAction,
    block_len: u64,
    blocks: Vec<BlockSig>,
    tuning: Tuning,
    net: NetEst,
    comp_ctl: Arc<CompCtl>,
) {
    js.spawn(async move {
        let pb = progress.file(&entry.rel, entry.size);
        tracing::debug!(id = entry.id, rel = %entry.rel, ?action, "file task start");
        let out = if action == PlanAction::Whole {
            send_whole(&set, &ctx, &comp_ctl, tuning.chunk, &entry, &path, &chunk_sem,
                       &stats, pb.as_ref(), &cancel).await
        } else {
            send_delta(&set, &ctx, &comp_ctl, tuning.chunk, &entry, &path, block_len,
                       blocks, &chunk_sem, &stats, pb.as_ref(), net, tuning, &cancel).await
        };
        if let Err(e) = &out {
            tracing::warn!("send of {} failed: {e:#}", entry.rel);
            // Every failure path ends here, including ones that bailed before
            // the first frame; the abort makes the receiver fail the file and
            // report it, which is where the failure gets counted.
            send_abort(&set, entry.id).await;
        }
        if let Some(pb) = pb {
            match &out {
                Ok(()) => {
                    // Finished bars linger forever in a MultiProgress; with
                    // thousands of files the redraw grows linearly, so the
                    // successful ones clear their line instead.
                    pb.finish_and_clear();
                }
                Err(e) => pb.finish_with_message(format!("failed: {e}")),
            }
        }
        out
    });
}

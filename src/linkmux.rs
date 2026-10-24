//! Frame codec + per-link worker tasks + the in-process bus that routes
//! frames between links, the session and per-file receiver tasks.
//!
//! Structure:
//!
//! * [`frame`] — framed message IO (2-way) over any `AsyncRead`/`AsyncWrite`.
//! * [`Duplex`] / [`Link`] — transport-agnostic links (TCP, SSH channel, pipe).
//! * [`WriterBus`] — all link writer queues; control on link 0, data
//!   round-robined over every link.
//! * [`Router`] — per-file ordered inbound channel map for the receiver side.
//! * [`LinkSet`] — the whole in-process bundle both ends build per session.
//!
//! Flow control is a chain of natural bounded buffers: each link writer queue
//! is [`LINK_QUEUE_DEPTH`] messages deep, which backpressures data producers
//! much like the TCP socket itself would, bounding pipeline memory by
//! `links × depth × chunk`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::protocol::{decode, encode_into, Msg, FRAME_LIMIT};

/// A link is anything we can duplex frames over.
pub trait Duplex: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> Duplex for T {}
pub type Link = Box<dyn Duplex + Send + Unpin>;

/// Writer queue depth per link — the local window that backpressures
/// producers exactly like the TCP socket itself would.
pub const LINK_QUEUE_DEPTH: usize = 48;

/// Per-file inbound channel depth (see [`Router`]). Bounds receiver memory
/// for files waiting on a slow disk: ≈ files in flight × depth × chunk
/// (32 × 16 × 1 MiB = 512 MiB worst case; 64 allowed 2 GiB on a small ARM board).
pub const ROUTER_DEPTH: usize = 16;

pub mod frame {
    use super::*;

    /// Encode `msg` into a framed buffer (4-byte big-endian length prefix).
    pub fn encode_payload(msg: &Msg) -> Vec<u8> {
        let mut out = Vec::with_capacity(16 + 512);
        out.extend_from_slice(&0u32.to_be_bytes());
        encode_into(msg, &mut out);
        let len = (out.len() - 4) as u32;
        out[..4].copy_from_slice(&len.to_be_bytes());
        out
    }

    pub async fn write_msg<W: AsyncWrite + Unpin>(w: &mut W, msg: &Msg) -> Result<()> {
        let bytes = encode_payload(msg);
        w.write_all(&bytes).await?;
        w.flush().await?;
        Ok(())
    }

    /// Read one raw frame; `Ok(None)` = clean close at a frame boundary.
    pub async fn read_raw<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<Vec<u8>>> {
        use std::io::ErrorKind;
        let mut len_buf = [0u8; 4];
        match r.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e)
                if e.kind() == ErrorKind::UnexpectedEof
                    || e.kind() == ErrorKind::BrokenPipe
                    || e.kind() == ErrorKind::ConnectionReset =>
            {
                return Ok(None)
            }
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > FRAME_LIMIT {
            bail!("frame of {len} bytes exceeds protocol limit");
        }
        let mut payload = vec![0u8; len];
        match r.read_exact(&mut payload).await {
            Ok(_) => {}
            Err(e)
                if e.kind() == ErrorKind::UnexpectedEof
                    || e.kind() == ErrorKind::BrokenPipe
                    || e.kind() == ErrorKind::ConnectionReset =>
            {
                bail!("link closed mid-frame")
            }
            Err(e) => return Err(e.into()),
        }
        Ok(Some(payload))
    }

    pub async fn read_msg<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<Msg>> {
        match read_raw(r).await? {
            None => Ok(None),
            Some(payload) => Ok(Some(decode(&payload).context("protocol decode")?)),
        }
    }

    /// First frame of a link must be `Msg::Hello`; returns its fields.
    pub async fn read_hello<R: AsyncRead + Unpin>(
        r: &mut R,
    ) -> Result<HelloSpec> {
        let msg = match read_msg(r).await? {
            Some(m) => m,
            None => anyhow::bail!("link closed before hello"),
        };
        match msg {
            Msg::Hello { link_id, instance } => Ok(HelloSpec { link_id, instance }),
            other => anyhow::bail!("expected hello, got {other:?} (is the peer rtransfer?)"),
        }
    }

    /// Echo a hello back.
    pub async fn write_hello<W: AsyncWrite + Unpin>(w: &mut W, h: &HelloSpec) -> Result<()> {
        write_msg(w, &Msg::Hello { link_id: h.link_id, instance: h.instance }).await
    }
}

/// `Msg::Hello` payload.
#[derive(Debug, Clone, Copy)]
pub struct HelloSpec {
    pub link_id: u32,
    pub instance: u128,
}

// ────────────────────────── link writer plumbing ───────────────────────────

type WriterQ = mpsc::Sender<Msg>;

/// All link writer queues plus round-robin over data sends.
#[derive(Default)]
pub struct WriterBus {
    links: Mutex<Vec<WriterQ>>,
    next: AtomicUsize,
    added: tokio::sync::Notify,
    /// Writer tasks, so [`Self::drain`] can wait for queued frames to hit
    /// the wire before the process exits.
    writers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl WriterBus {
    pub fn count(&self) -> usize {
        self.links.lock().unwrap().len()
    }

    /// Attach one writer queue and spawn its frame serializer on `w`.
    /// Control frames going out must be reserved for link 0 by convention.
    pub fn attach_writer<W>(&self, w: W)
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, mut rx) = mpsc::channel::<Msg>(LINK_QUEUE_DEPTH);
        self.links.lock().unwrap().push(tx);
        self.added.notify_waiters();
        let task = tokio::spawn(async move {
            let mut w = w;
            while let Some(msg) = rx.recv().await {
                if frame::write_msg(&mut w, &msg).await.is_err() {
                    return;
                }
            }
            // All senders gone and the queue empty: half-close so the peer
            // reads a clean EOF after the last frame.
            let _ = w.shutdown().await;
        });
        self.writers.lock().unwrap().push(task);
    }

    /// Close every queue and wait (bounded) until the frames already queued
    /// are written. `send_*` only enqueue, so without this a process exiting
    /// right after its role ends (the stdio serve) dropped its final Plans,
    /// FileResults or Done, and the peer waited forever.
    pub async fn drain(&self, timeout: Duration) {
        self.close_all();
        let writers: Vec<_> = std::mem::take(&mut *self.writers.lock().unwrap());
        let all = async {
            for w in writers {
                let _ = w.await;
            }
        };
        if tokio::time::timeout(timeout, all).await.is_err() {
            tracing::warn!("link writers did not drain within {timeout:?}; closing anyway");
        }
    }

    /// Wait until at least `target` links exist.
    pub async fn wait_links(&self, target: usize, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        while self.count() < target {
            if tokio::time::Instant::now() >= deadline {
                bail!("only {}/{} links connected before timeout", self.count(), target);
            }
            let _ = tokio::time::timeout_at(deadline, self.added.notified()).await;
        }
        Ok(())
    }

    /// Send on the control link (index 0 — always the first attached).
    pub async fn send_control(&self, msg: &Msg) -> Result<()> {
        let q = self
            .links
            .lock()
            .unwrap()
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("control link gone"))?;
        q.send(msg.clone()).await.context("control queue closed")
    }

    /// Send one data-ish frame over any link, preferring the round-robin
    /// next queue with room, falling back to blocking on one of them.
    /// Takes the message by value: a refused `try_send` hands it back, so a
    /// 1 MiB `Data` frame is never copied on its way to a queue.
    pub async fn send_data(&self, msg: Msg) -> Result<()> {
        let qs: Vec<WriterQ> = self.links.lock().unwrap().to_vec();
        if qs.is_empty() {
            bail!("no links to send on");
        }
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let mut msg = msg;
        for i in 0..qs.len() {
            match qs[(start + i) % qs.len()].try_send(msg) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(m) | mpsc::error::TrySendError::Closed(m)) => {
                    msg = m;
                }
            }
        }
        // Everything in flight: block on the chosen queue (still gives real
        // backpressure instead of growing memory).
        qs[start % qs.len()].send(msg).await.context("links closed")
    }

    pub fn close_all(&self) {
        self.links.lock().unwrap().clear();
        self.added.notify_waiters();
    }
}

// ────────────────────────── per-file inbound router ────────────────────────

/// Lazily creates one bounded channel per file id; the receiving side's
/// file task pulls its half via [`Router::take_rx`], which wakes when the
/// channel's *first* frame arrives or when the router shuts down.
pub struct Router {
    chan: Mutex<HashMap<u32, WriterQ>>,
    pending: Mutex<HashMap<u32, mpsc::Receiver<Msg>>>,
    depth: usize,
    wake: tokio::sync::Notify,
    cancelled: AtomicBool,
    /// Called when the first frame for a previously-unseen file id arrives;
    /// lets the receiver spawn apply tasks just-in-time.
    spawner: Mutex<Option<Box<dyn Fn(u32) + Send + Sync>>>,
}

impl Router {
    pub fn new(depth: usize) -> Self {
        Router {
            chan: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            depth,
            wake: tokio::sync::Notify::new(),
            cancelled: AtomicBool::new(false),
            spawner: Mutex::new(None),
        }
    }

    /// Register a callback fired when a file channel is created for an id no
    /// apply task has claimed yet. Only one callback is supported.
    pub fn on_new_file(&self, f: impl Fn(u32) + Send + Sync + 'static) {
        *self.spawner.lock().unwrap() = Some(Box::new(f));
    }

    /// Get or create the per-file sender. Returns also whether the channel
    /// was freshly created (i.e. this is the file's first frame).
    fn chan_for(&self, id: u32) -> (WriterQ, bool) {
        let mut chan = self.chan.lock().unwrap();
        match chan.entry(id) {
            std::collections::hash_map::Entry::Occupied(o) => (o.get().clone(), false),
            std::collections::hash_map::Entry::Vacant(v) => {
                let (tx, rx) = mpsc::channel(self.depth);
                v.insert(tx.clone());
                self.pending.lock().unwrap().insert(id, rx);
                self.wake.notify_one();
                (tx, true)
            }
        }
    }

    /// Push a frame to a file task. Late frames for an already-finished file
    /// are ignored (they can only be the tail of an in-flight window).
    pub async fn route(&self, msg: Msg, id: u32) -> Result<()> {
        let (q, fresh) = self.chan_for(id);
        if fresh {
            if let Some(spawn) = self.spawner.lock().unwrap().as_ref() {
                spawn(id);
            }
        }
        if q.send(msg).await.is_err() {
            tracing::trace!("frame for file {id} after its task ended; ignored");
        }
        Ok(())
    }

    /// Take the receiver half created for `id`; `None` once shutdown.
    ///
    /// The matching *sender* stays in `chan` for the process lifetime: the
    /// in-flight pipeline keeps routing frames into this very channel until
    /// the file's `SendDone`, and only once the reader drops the receiver do
    /// further `route()` pushes fail fast and get ignored.
    pub async fn take_rx(&self, id: u32) -> Option<mpsc::Receiver<Msg>> {
        loop {
            let rx = {
                let mut guard = self.pending.lock().unwrap();
                guard.remove(&id)
            };
            if let Some(rx) = rx {
                return Some(rx);
            }
            if self.cancelled.load(Ordering::SeqCst) {
                return None;
            }
            self.wake.notified().await;
        }
    }

    pub fn shutdown(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.chan.lock().unwrap().clear();
        self.pending.lock().unwrap().clear();
        self.wake.notify_waiters();
    }
}

/// Everything needed to route inbound frames + reply outbound, shared across
/// tasks via `Arc`.
pub struct LinkSet {
    pub instance: u128,
    pub bus: WriterBus,
    pub router: Router,
    /// Inbound control-plane messages (everything not routed per-file).
    ctrl_tx: mpsc::Sender<Msg>,
    /// Set once the first (control) reader is attached.
    control_attached: AtomicBool,
}

pub type Shared = std::sync::Arc<LinkSet>;
pub type CtrlRx = mpsc::Receiver<Msg>;

impl LinkSet {
    pub fn new(instance: u128, ctrl_buffer: usize, router_depth: usize) -> (Shared, CtrlRx) {
        let (tx, rx) = mpsc::channel(ctrl_buffer);
        let set = LinkSet {
            instance,
            bus: WriterBus::default(),
            router: Router::new(router_depth),
            ctrl_tx: tx,
            control_attached: AtomicBool::new(false),
        };
        (std::sync::Arc::new(set), rx)
    }

    /// Attach the read half of a link: routes file-scoped frames to the
    /// router, everything else to the session's control channel.
    ///
    /// The first reader attached is the control link. When it ends, a
    /// synthetic `Msg::Error` is queued: after a normal end the dispatcher
    /// has already stopped at `Done` and never sees it, but a peer that
    /// vanishes mid-session now fails the session instead of leaving the
    /// role engines waiting forever for plans or results.
    pub fn attach_reader(self: &Shared, read_half: impl AsyncRead + Unpin + Send + 'static) {
        let set = self.clone();
        let is_control = !self.control_attached.swap(true, Ordering::SeqCst);
        tokio::spawn(async move {
            let mut r = read_half;
            loop {
                match frame::read_msg(&mut r).await {
                    Ok(Some(msg)) => {
                        let routed = match &msg {
                            Msg::Data { id, .. } | Msg::Copy { id, .. } | Msg::SendDone { id, .. } => {
                                let id = *id;
                                set.router.route(msg, id).await
                            }
                            _ => set
                                .ctrl_tx
                                .send(msg)
                                .await
                                .map_err(|e| anyhow::Error::msg(e.to_string())),
                        };
                        if routed.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::warn!("data link lost (reader closed): {e:#}");
                        break;
                    }
                }
            }
            if is_control {
                let _ = set
                    .ctrl_tx
                    .send(Msg::error("peer closed the control link before the session ended"))
                    .await;
            }
        });
    }
}

/// Attach both halves of a link (writer + reader).
pub fn attach_duplex(set: &Shared, link: Link) {
    let (r, w) = tokio::io::split(link);
    set.bus.attach_writer(w);
    set.attach_reader(r);
}

/// Tear the whole session plumbing down (cancels reader/writer tasks and
/// unblocks waiters).
pub fn shutdown(set: &Shared) {
    set.bus.close_all();
    set.router.shutdown();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{BenchInfo, CopyMode};
    use std::io;

    #[tokio::test]
    async fn framing_roundtrip_via_memory_pipe() {
        let (a, b) = tokio::io::duplex(1024);
        let (_, mut wb) = tokio::io::split(a);
        let (mut rb, _) = tokio::io::split(b);
        let msg = Msg::Greeting {
            mode: CopyMode::Push,
            delete: true,
            allow_delta: false,
            target: "/x".into(),
            links: 2,
            bench: BenchInfo { cores: 1, hash_mb_s: 1.0, zstd_l3_mb_s: 1.0, zstd_l9_mb_s: 1.0 },
            include: Vec::new(),
            exclude: Vec::new(),
            merge: false,
            fsync: false,
        };
        tokio::spawn(async move { frame::write_msg(&mut wb, &msg).await.unwrap() });
        let got = frame::read_msg(&mut rb).await.unwrap().unwrap();
        assert!(matches!(got, Msg::Greeting { mode: CopyMode::Push, delete: true, links: 2, .. }));
    }

    struct NullW;
    impl tokio::io::AsyncWrite for NullW {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            std::task::Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn writer_bus_senders_count() {
        let (set, _ctrl) = LinkSet::new(999, 16, 8);
        for _ in 0..3 {
            set.bus.attach_writer(NullW);
        }
        assert_eq!(set.bus.count(), 3);
        // Non-blocking fast fill across links succeeds.
        for s in 0..6u64 {
            set.bus.send_data(Msg::Probe { seq: s, data: vec![2; 8] }).await.unwrap();
        }
        set.bus.send_control(&Msg::Probe { seq: 9, data: vec![1; 8] }).await.unwrap();
        set.bus.close_all();
        assert_eq!(set.bus.count(), 0);
    }

    #[tokio::test]
    async fn router_lazily_creates_channels() {
        let router = std::sync::Arc::new(Router::new(2));
        let h = {
            let router = router.clone();
            tokio::spawn(async move {
                router
                    .route(
                        Msg::Data {
                            id: 5,
                            seq: 0,
                            offset: 0,
                            comp: crate::protocol::Comp::None,
                            orig_len: 1,
                            data: vec![0],
                        },
                        5,
                    )
                    .await
            })
        };
        let rx = router.take_rx(5).await.expect("channel must exist by now");
        h.await.unwrap().unwrap();
        let mut rx = rx;
        assert!(matches!(rx.recv().await, Some(Msg::Data { seq: 0, .. })));
        // Before shutdown a second take just waits (the channel is live);
        // shutdown must end the wait with None.
        router.shutdown();
        assert!(router.take_rx(5).await.is_none(), "shutdown must end the wait");
    }

    /// `drain` must not return before every queued frame is on the wire —
    /// the stdio serve exits right after it (and used to drop its tail).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drain_delivers_every_queued_frame() {
        let (ours, theirs) = tokio::io::duplex(64 * 1024);
        let (set, _ctrl) = LinkSet::new(1, 16, 8);
        set.bus.attach_writer(ours);
        // A slow reader on the far end, so frames are still queued at drain.
        let reader = tokio::spawn(async move {
            let mut r = theirs;
            let mut n = 0u64;
            while let Some(Msg::Probe { .. }) = frame::read_msg(&mut r).await.unwrap() {
                n += 1;
                if n % 16 == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            n
        });
        for seq in 0..400u64 {
            set.bus.send_control(&Msg::Probe { seq, data: vec![0; 2048] }).await.unwrap();
        }
        set.bus.drain(Duration::from_secs(10)).await;
        assert_eq!(reader.await.unwrap(), 400, "frames lost at drain");
    }

    /// A closed control link must surface as a session error, not silence.
    #[tokio::test]
    async fn control_link_eof_reports_error() {
        let (ours, theirs) = tokio::io::duplex(1024);
        let (set, mut ctrl) = LinkSet::new(2, 16, 8);
        set.attach_reader(ours);
        drop(theirs);
        let got = tokio::time::timeout(Duration::from_secs(5), ctrl.recv()).await.unwrap();
        assert!(matches!(got, Some(Msg::Error { .. })), "got {got:?}");
    }
}

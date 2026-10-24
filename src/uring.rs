//! Linux `io_uring` file-IO worker.
//!
//! `tokio-uring`'s runtime is `!Send`, so the ring (and every file handle it
//! knows about) must live on one dedicated thread. Callers on the main tokio
//! multi-threaded runtime talk to it through a bounded command channel
//! (natural IO backpressure) and `oneshot` replies (runtime-agnostic wakers,
//! so they work across runtimes).
//!
//! File handles are registered by id and pinned to the worker thread so they
//! are never dropped from a foreign thread.

#[cfg(target_os = "linux")]
mod imp {
    use std::collections::HashMap;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::{mpsc, oneshot};

    const RING_ENTRIES: u32 = 512;
    /// Command-channel depth: also caps concurrent in-flight submissions.
    const CHANNEL_DEPTH: usize = 1024;

    pub struct UringWorker {
        tx: mpsc::Sender<Cmd>,
        next_id: AtomicU64,
        _thread: std::thread::JoinHandle<()>,
    }

    enum Cmd {
        OpenRead { path: PathBuf, reply: oneshot::Sender<io::Result<u64>> },
        OpenWrite { path: PathBuf, reply: oneshot::Sender<io::Result<u64>> },
        ReadAt { id: u64, off: u64, len: u64, reply: oneshot::Sender<io::Result<Vec<u8>>> },
        WriteAt { id: u64, off: u64, data: Vec<u8>, reply: oneshot::Sender<io::Result<()>> },
        Sync { id: u64, reply: oneshot::Sender<io::Result<()>> },
        /// `reply: None` = fire-and-forget release from a `Drop`; an unknown
        /// id is then a no-op (ids are never reused, so a release after an
        /// explicit close is harmless).
        Close { id: u64, reply: Option<oneshot::Sender<io::Result<()>>> },
    }

    type Files = HashMap<u64, (PathBuf, Arc<tokio_uring::fs::File>)>;

    impl UringWorker {
        /// Spawn the worker thread; `None` if io_uring itself is unavailable.
        pub fn spawn() -> Option<Arc<UringWorker>> {
            let (tx, rx) = mpsc::channel::<Cmd>(CHANNEL_DEPTH);
            let thread = std::thread::Builder::new()
                .name("rtransfer-uring".into())
                .spawn(move || run_worker(rx))
                .expect("spawn uring worker thread");
            Some(Arc::new(UringWorker {
                tx,
                next_id: AtomicU64::new(1),
                _thread: thread,
            }))
        }

        pub async fn open_read(&self, path: &Path) -> io::Result<u64> {
            let _id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let (rtx, rrx) = oneshot::channel();
            self.tx
                .send(Cmd::OpenRead { path: path.into(), reply: rtx })
                .await
                .map_err(|_| io::Error::other("uring worker dead"))?;
            let got = rrx.await.map_err(|_| io::Error::other("uring worker dead"))?;
            if got.is_err() {
                self.next_id.fetch_sub(1, Ordering::Relaxed);
            }
            got
        }

        pub async fn open_write(&self, path: &Path) -> io::Result<u64> {
            let _id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let (rtx, rrx) = oneshot::channel();
            self.tx
                .send(Cmd::OpenWrite { path: path.into(), reply: rtx })
                .await
                .map_err(|_| io::Error::other("uring worker dead"))?;
            let got = rrx.await.map_err(|_| io::Error::other("uring worker dead"))?;
            if got.is_err() {
                self.next_id.fetch_sub(1, Ordering::Relaxed);
            }
            got
        }

        pub async fn read_buf(&self, id: u64, off: u64, len: u64) -> io::Result<Vec<u8>> {
            let (rtx, rrx) = oneshot::channel();
            self.tx
                .send(Cmd::ReadAt { id, off, len, reply: rtx })
                .await
                .map_err(|_| io::Error::other("uring worker dead"))?;
            rrx.await.map_err(|_| io::Error::other("uring worker dead"))?
        }

        pub async fn write_buf(&self, id: u64, off: u64, data: Vec<u8>) -> io::Result<()> {
            let (rtx, rrx) = oneshot::channel();
            self.tx
                .send(Cmd::WriteAt { id, off, data, reply: rtx })
                .await
                .map_err(|_| io::Error::other("uring worker dead"))?;
            rrx.await.map_err(|_| io::Error::other("uring worker dead"))?
        }

        pub async fn sync(&self, id: u64) -> io::Result<()> {
            let (rtx, rrx) = oneshot::channel();
            self.tx
                .send(Cmd::Sync { id, reply: rtx })
                .await
                .map_err(|_| io::Error::other("uring worker dead"))?;
            rrx.await.map_err(|_| io::Error::other("uring worker dead"))?
        }

        pub async fn close(&self, id: u64) -> io::Result<()> {
            let (rtx, rrx) = oneshot::channel();
            let _ = self.tx.send(Cmd::Close { id, reply: Some(rtx) }).await;
            rrx.await.map_err(|_| io::Error::other("uring worker dead"))?
        }

        /// Close `id` without waiting (usable from `Drop`). Falls back to a
        /// spawned send when the command queue is momentarily full.
        pub fn release(&self, id: u64) {
            match self.tx.try_send(Cmd::Close { id, reply: None }) {
                Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
                Err(mpsc::error::TrySendError::Full(cmd)) => {
                    if let Ok(rt) = tokio::runtime::Handle::try_current() {
                        let tx = self.tx.clone();
                        rt.spawn(async move {
                            let _ = tx.send(cmd).await;
                        });
                    }
                }
            }
        }
    }

    fn run_worker(mut rx: mpsc::Receiver<Cmd>) {
        let mut builder = tokio_uring::builder();
        builder.entries(RING_ENTRIES);
        let rt = match tokio_uring::Runtime::new(&builder) {
            Ok(rt) => rt,
            Err(_) => return, // kernel too old for io_uring
        };
        rt.block_on(async move {
            let mut files: Files = HashMap::new();
            let mut next_id: u64 = 1;
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    Cmd::OpenRead { path, reply } => {
                        match tokio_uring::fs::File::open(&path).await {
                            Ok(f) => {
                                let id = next_id;
                                next_id += 1;
                                files.insert(id, (path, Arc::new(f)));
                                let _ = reply.send(Ok(id));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(e));
                            }
                        }
                    }
                    Cmd::OpenWrite { path, reply } => {
                        match tokio_uring::fs::File::create(&path).await {
                            Ok(f) => {
                                let id = next_id;
                                next_id += 1;
                                files.insert(id, (path, Arc::new(f)));
                                let _ = reply.send(Ok(id));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(e));
                            }
                        }
                    }
                    Cmd::ReadAt { id, off, len, reply } => match files.get(&id) {
                        None => {
                            let _ = reply.send(Err(io::Error::other("uring: unknown file id")));
                        }
                        Some((_, f)) => {
                            let f = f.clone();
                            tokio_uring::spawn(async move {
                                let buf = vec![0u8; len as usize];
                                let (res, buf) = f.read_at(buf, off).await;
                                let out = match res {
                                    Ok(n) if n == 0 => Err(io::Error::new(
                                        io::ErrorKind::UnexpectedEof,
                                        "uring read at EOF",
                                    )),
                                    Ok(n) => {
                                        let mut buf = buf;
                                        buf.truncate(n);
                                        Ok(buf)
                                    }
                                    Err(e) => Err(e),
                                };
                                let _ = reply.send(out);
                            });
                        }
                    },
                    Cmd::WriteAt { id, off, data, reply } => match files.get(&id) {
                        None => {
                            let _ = reply.send(Err(io::Error::other("uring: unknown file id")));
                        }
                        Some((_, f)) => {
                            let f = f.clone();
                            tokio_uring::spawn(async move {
                                let (res, _buf) = f.write_at(data, off).submit().await;
                                let _ = reply.send(res.map(|_n| ()));
                            });
                        }
                    },
                    Cmd::Sync { id, reply } => match files.get(&id) {
                        None => {
                            let _ = reply.send(Err(io::Error::other("uring: unknown file id")));
                        }
                        Some((_, f)) => {
                            let f = f.clone();
                            tokio_uring::spawn(async move {
                                let res = f.sync_all().await;
                                let _ = reply.send(res);
                            });
                        }
                    },
                    Cmd::Close { id, reply } => {
                        if let Some((_, f)) = files.remove(&id) {
                            tokio_uring::spawn(async move {
                                // In-flight ops drop their Arc clone when completed;
                                // wait until we own the handle, then close (frees FD).
                                let owned = {
                                    let mut f = f;
                                    loop {
                                        match Arc::try_unwrap(f) {
                                            Ok(file) => break file,
                                            Err(arc) => {
                                                f = arc;
                                                tokio::time::sleep(Duration::from_millis(5)).await;
                                            }
                                        }
                                    }
                                };
                                let res = owned.close().await;
                                if let Some(reply) = reply {
                                    let _ = reply.send(res);
                                }
                            });
                        } else if let Some(reply) = reply {
                            let _ = reply.send(Err(io::Error::other("uring: unknown file id")));
                        }
                    }
                }
            }
        });
    }
}

#[cfg(target_os = "linux")]
pub use imp::UringWorker;

#[cfg(target_os = "linux")]
pub fn global_worker() -> Option<std::sync::Arc<UringWorker>> {
    use std::sync::OnceLock;
    static WORKER: OnceLock<Option<std::sync::Arc<UringWorker>>> = OnceLock::new();
    WORKER.get_or_init(|| UringWorker::spawn()).clone()
}

// Non-Linux platforms: ioengine never requests the uring engine, but keep a
// stub type so `IoCtx`'s code paths typecheck everywhere.
#[cfg(not(target_os = "linux"))]
pub struct UringWorker {
    _never: (),
}

#[cfg(not(target_os = "linux"))]
impl UringWorker {
    pub fn release(&self, _id: u64) {}
}

#[cfg(not(target_os = "linux"))]
pub fn global_worker() -> Option<std::sync::Arc<UringWorker>> {
    None
}

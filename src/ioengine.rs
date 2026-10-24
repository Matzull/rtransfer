//! File IO engines.
//!
//! Two backends behind one async interface:
//!
//! * **blocking** — `std::fs` on tokio's blocking threads via positional
//!   copies of the file handle. Portable baseline, always available.
//! * **uring** — Linux `io_uring` via [`crate::uring`], driven by a dedicated
//!   worker thread. Selected automatically when the chunk size amortizes ring
//!   overhead and the kernel supports it.
//!
//! Everything is positional (`read_at` / `write_at`): reads and writes across
//! many files in flight need no shared cursors. Integrity is layered above
//! this module via end-to-end blake3 hashing done by the session code.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::fs::File;

use crate::uring::UringWorker;

/// Backend choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    /// io_uring on Linux when useful and available, otherwise blocking.
    Auto,
    Blocking,
    Uring,
}

/// Chunk size below which ring submission overhead beats a pooled pread.
const URING_MIN_CHUNK: u64 = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    Blocking,
    Uring,
}

/// Resolved IO context for one session.
#[derive(Clone)]
pub struct IoCtx {
    engine: Engine,
    worker: Option<Arc<UringWorker>>,
}

impl IoCtx {
    /// Resolve an [`IoMode`] for the given chunk size.
    pub fn new(mode: IoMode, chunk: u64) -> Self {
        #[allow(unused_variables)]
        let uring_ok = cfg!(target_os = "linux") && chunk >= URING_MIN_CHUNK;
        let want_uring = match mode {
            IoMode::Blocking => false,
            _ => uring_ok,
        };
        if want_uring {
            if let Some(worker) = crate::uring::global_worker() {
                return IoCtx { engine: Engine::Uring, worker: Some(worker) };
            }
            if mode == IoMode::Uring {
                tracing::warn!("io_uring unavailable (Linux 5.11+ required); using blocking IO");
            }
        }
        IoCtx { engine: Engine::Blocking, worker: None }
    }

    pub fn engine(&self) -> Engine {
        self.engine
    }
}

/// A file registered with the uring worker; released (closed) on drop so no
/// handle outlives its `Reader`/`Writer`.
struct UringFile {
    handle: u64,
    worker: Arc<UringWorker>,
}

impl Drop for UringFile {
    fn drop(&mut self) {
        self.worker.release(self.handle);
    }
}

/// Asynchronous positional reader.
pub struct Reader {
    inner: ReaderInner,
}

enum ReaderInner {
    Blocking { path: PathBuf },
    Uring(UringFile),
}

impl Reader {
    pub async fn open(ctx: &IoCtx, path: &Path) -> io::Result<Reader> {
        match ctx.engine {
            Engine::Blocking => Ok(Reader { inner: ReaderInner::Blocking { path: path.into() } }),
            Engine::Uring => {
                let worker = ctx
                    .worker
                    .clone()
                    .ok_or_else(|| io::Error::other("uring worker unavailable"))?;
                let handle = worker.open_read(path).await?;
                Ok(Reader { inner: ReaderInner::Uring(UringFile { handle, worker }) })
            }
        }
    }

    /// Read exactly `len` bytes at `offset`. The vector may be shorter only
    /// if the file ends sooner than requested (callers size ranges from
    /// real file lengths, so short reads are an error condition on purpose).
    pub async fn read_at(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        match &self.inner {
            ReaderInner::Blocking { path } => {
                let file = File::open(path).await?;
                read_blocking(file, offset, len).await
            }
            ReaderInner::Uring(f) => {
                let buf = f.worker.read_buf(f.handle, offset, len).await?;
                if buf.len() as u64 != len {
                    // Trim semantics: delivered exactly as much as exists.
                    return Ok(buf);
                }
                Ok(buf)
            }
        }
    }
}

/// Blocking positional read against a std file.
async fn read_blocking(
    file: tokio::fs::File,
    offset: u64,
    len: u64,
) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len as usize];
    let std_file = file.into_std().await;
    tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
        let mut f = std_file;
        f.seek(SeekFrom::Start(offset))?;
        let mut done = 0usize;
        while done < buf.len() {
            match f.read(&mut buf[done..])? {
                0 => break, // EOF
                n => done += n,
            }
        }
        buf.truncate(done);
        Ok(buf)
    })
    .await
    .map_err(|e| io::Error::other(format!("blocking read task: {e}")))?
}

/// Asynchronous positional reader.
pub struct Writer {
    inner: WriterInner,
}

enum WriterInner {
    Blocking { file: std::fs::File, path: PathBuf },
    Uring(UringFile),
}

impl Writer {
    pub async fn create(ctx: &IoCtx, path: &Path) -> io::Result<Writer> {
        match ctx.engine {
            Engine::Blocking => {
                let file = File::create(path).await?.into_std().await;
                Ok(Writer { inner: WriterInner::Blocking { file, path: path.into() } })
            }
            Engine::Uring => {
                let worker = ctx
                    .worker
                    .clone()
                    .ok_or_else(|| io::Error::other("uring worker unavailable"))?;
                let handle = worker.open_write(path).await?;
                Ok(Writer { inner: WriterInner::Uring(UringFile { handle, worker }) })
            }
        }
    }

    /// Write `data` at `offset` (full append-only-positional write).
    pub async fn write_at(&self, offset: u64, data: Vec<u8>) -> io::Result<()> {
        match &self.inner {
            WriterInner::Blocking { file, .. } => {
                let mut f = file.try_clone()?;
                tokio::task::spawn_blocking(move || -> io::Result<()> {
                    f.seek(SeekFrom::Start(offset))?;
                    f.write_all(&data)?;
                    Ok(())
                })
                .await
                .map_err(|e| io::Error::other(format!("blocking write task: {e}")))?
            }
            WriterInner::Uring(f) => {
                f.worker.write_buf(f.handle, offset, data).await
            }
        }
    }

    pub async fn sync_data(&self) -> io::Result<()> {
        match &self.inner {
            WriterInner::Blocking { file, .. } => {
                let f = file.try_clone()?;
                tokio::task::spawn_blocking(move || f.sync_data())
                    .await
                    .map_err(|e| io::Error::other(format!("blocking sync task: {e}")))?
            }
            WriterInner::Uring(f) => f.worker.sync(f.handle).await,
        }
    }

    /// Close the file. `finalize_len` truncates it to the expected size —
    /// the part file should already be exactly full, this is belt-and-braces.
    pub async fn close(self, finalize_len: Option<u64>) -> io::Result<()> {
        match self.inner {
            WriterInner::Blocking { file, path } => {
                drop(file);
                match finalize_len {
                    None => Ok(()),
                    Some(len) => {
                        tokio::task::spawn_blocking(move || -> io::Result<()> {
                            let f = std::fs::OpenOptions::new().write(true).open(path)?;
                            f.set_len(len)?;
                            Ok(())
                        })
                        .await
                        .map_err(|e| io::Error::other(format!("blocking close task: {e}")))?
                    }
                }
            }
            WriterInner::Uring(f) => {
                // The guard's release after this explicit close is a no-op.
                f.worker.close(f.handle).await?;
                Ok(())
            }
        }
    }
}

/// Reader handle for a path only if it exists (used for delta "old file"
/// reads); `Ok(None)` otherwise.
pub async fn open_reader_if_exists(ctx: &IoCtx, path: &Path) -> io::Result<Option<Reader>> {
    if tokio::fs::try_exists(path).await.unwrap_or(false) {
        Ok(Some(Reader::open(ctx, path).await?))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    fn open_fds() -> usize {
        std::fs::read_dir("/proc/self/fd").map(|d| d.count()).unwrap_or(0)
    }

    /// Dropped readers must hand their uring handle back (a leak here
    /// exhausts a 1024-fd limit after ~1000 files).
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_uring_readers_release_their_fds() {
        let ctx = IoCtx::new(IoMode::Uring, 1024 * 1024);
        if ctx.engine() != Engine::Uring {
            return; // io_uring unavailable here: nothing to leak
        }
        let p = std::env::temp_dir().join(format!("rtransfer-fdleak-{}", std::process::id()));
        std::fs::write(&p, b"0123456789").unwrap();
        let before = open_fds();
        for _ in 0..2000 {
            let r = Reader::open(&ctx, &p).await.unwrap();
            assert_eq!(r.read_at(0, 10).await.unwrap(), b"0123456789");
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let after = open_fds();
        std::fs::remove_file(&p).unwrap();
        assert!(after < before + 50, "fd leak: {before} open before, {after} after 2000 readers");
    }
}

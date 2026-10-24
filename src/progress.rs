//! Terminal progress display (single overall line + per-file bars).

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Mutex;
use std::time::{Duration, Instant};

fn bytes_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner} {msg:<34} {bar:30} {bytes:>10}/{total_bytes:<10} {bytes_per_sec:>10} eta {eta}",
    )
    .expect("progress template")
    .progress_chars("=>-")
}

fn counter_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner} {msg:<30} {bar:24} {bytes:>10}/{total_bytes:<10} {bytes_per_sec:>10} eta {eta}",
    )
    .expect("progress template")
    .progress_chars("=>-")
}

/// Hierarchical bars: one overall bar + per-file children. Skips all
/// terminal work when `quiet`.
pub struct Progress {
    quiet: bool,
    multi: MultiProgress,
    overall: Mutex<Option<ProgressBar>>,
    start: Instant,
}

impl Progress {
    pub fn new(quiet: bool) -> Self {
        let multi = MultiProgress::new();
        Progress { quiet, multi, overall: Mutex::new(None), start: Instant::now() }
    }

    /// Start the overall bar once totals are known (files, bytes).
    pub fn start(&self, files: u64, bytes: u64, label: &str) {
        if self.quiet {
            return;
        }
        let pb = self.multi.add(ProgressBar::new(bytes.max(1)));
        pb.set_style(counter_style());
        pb.set_message(format!("{label}: 0/{files} files, 0 bytes"));
        pb.set_position(0);
        pb.set_length((bytes.max(1)) as u64);
        pb.enable_steady_tick(Duration::from_millis(200));
        self.overall.lock().unwrap().replace(pb);
        let _ = files;
    }

    /// Overall bar only counts bytes; file count is patched into the message.
    pub fn set(&self, done_files: u64, files: u64, done_bytes: u64) {
        if self.quiet {
            return;
        }
        if let Some(pb) = &*self.overall.lock().unwrap() {
            pb.set_message(format!("{done_files}/{files} files"));
            pb.set_position(done_bytes);
        }
    }

    /// A child bar for one file transfer.
    pub fn file(&self, name: &str, size: u64) -> Option<ProgressBar> {
        if self.quiet {
            return None;
        }
        let guard = self.overall.lock().unwrap();
        let anchor = guard.as_ref()?;
        let pb = self.multi.insert_after(anchor, ProgressBar::new(size.max(1)));
        drop(guard);
        pb.set_style(bytes_style());
        pb.set_message(short_name(name));
        Some(pb)
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    pub fn finish(&self, summary: &str) {
        if self.quiet {
            return;
        }
        if let Some(pb) = &*self.overall.lock().unwrap() {
            pb.finish_with_message(summary.to_string());
        }
    }
}

fn short_name(name: &str) -> String {
    // Keep paths readable but bounded on busy screens.
    const MAX: usize = 32;
    if name.len() <= MAX {
        name.to_string()
    } else {
        let head: String = name.chars().rev().take(MAX - 1).collect();
        format!("…{}", head.chars().rev().collect::<String>())
    }
}

use std::sync::atomic::AtomicU64;

/// Mostly-wired counters shared by sessions and their file tasks.
#[derive(Debug, Default)]
pub struct Counters {
    pub files_done: AtomicU64,
    pub files_failed: AtomicU64,
    pub files_skipped: AtomicU64,
    pub bytes_done: AtomicU64,
    pub bytes_wire: AtomicU64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_no_panic_quiet_or_not() {
        // Indicatif works headless (draws to a null draw target when the
        // terminal is not a tty); exercise both paths for template errors.
        let p = Progress::new(false);
        p.start(3, 1500, "send");
        let f = p.file("a/very/long/path/that/exceeds/the/bound/of/the/line.bin", 1000);
        if let Some(pb) = &f {
            pb.inc(500);
        }
        p.set(1, 3, 500);
        p.finish("done");
        let q = Progress::new(true);
        q.start(1, 1, "x");
        q.set(1, 1, 1);
        q.finish("x done");
    }
}

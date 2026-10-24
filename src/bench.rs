//! CPU heuristics: measure what this machine can actually do, then let the
//! tuner pick transfer parameters materialized from those numbers.

use std::time::Instant;

use crate::delta::fill_random;
use crate::protocol::BenchInfo;

/// Raw CPU capability measurements (throughputs as bytes/s).
#[derive(Debug, Clone, Copy)]
pub struct CpuBench {
    pub cores: u32,
    /// Content hashing throughput (single core), bytes/s.
    pub hash_bps: f64,
    /// zstd level 3 throughput (single core), bytes/s.
    pub zstd_l3_bps: f64,
    /// zstd level 9 throughput (single core), bytes/s.
    pub zstd_l9_bps: f64,
}

impl CpuBench {
    pub fn to_info(&self) -> BenchInfo {
        BenchInfo {
            cores: self.cores,
            hash_mb_s: self.hash_bps as f32 / (1024.0 * 1024.0),
            zstd_l3_mb_s: self.zstd_l3_bps as f32 / (1024.0 * 1024.0),
            zstd_l9_mb_s: self.zstd_l9_bps as f32 / (1024.0 * 1024.0),
        }
    }
}

fn available_cores() -> u32 {
    std::thread::available_parallelism().map(|n| n.get() as u32).unwrap_or(1)
}

/// Median of `n` repetitions, with one warm-up run discarded.
fn median<F: FnMut() -> f64>(mut f: F, runs: usize) -> f64 {
    let mut v = Vec::with_capacity(runs);
    v.push(f()); // warm-up, not counted
    for _ in 0..runs {
        v.push(f());
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// Throughput of hashing `buf` once.
fn bench_hash(buf: &[u8]) -> f64 {
    let t0 = Instant::now();
    let _ = blake3::hash(buf);
    buf.len() as f64 / t0.elapsed().as_secs_f64()
}

/// Throughput of zstd-compressing `buf` at `level`.
fn bench_zstd(buf: &[u8], level: i32) -> f64 {
    let t0 = Instant::now();
    let out = zstd::bulk::compress(buf, level).expect("zstd bench");
    let elapsed = t0.elapsed().as_secs_f64();
    // Judge speed, not size; but keep an allocation-warm sanity check.
    std::hint::black_box(&out);
    buf.len() as f64 / elapsed
}

/// Core count only, for roles that never compress (the receiver): the full
/// suite costs a slow machine like a small ARM board a second or two of CPU per session.
pub fn cores_only() -> CpuBench {
    CpuBench { cores: available_cores(), hash_bps: 0.0, zstd_l3_bps: 0.0, zstd_l9_bps: 0.0 }
}

/// Run the CPU benchmark suite. `buf_size` is the working set per run.
pub fn bench_cpu(buf_size: Option<usize>) -> CpuBench {
    let cores = available_cores();
    let buf_size = buf_size.unwrap_or(16 * 1024 * 1024);
    let mut buf = vec![0u8; buf_size];
    fill_random(&mut buf, 0x57_10_6f);
    let runs = 3;
    CpuBench {
        cores,
        hash_bps: median(|| bench_hash(&buf), runs),
        zstd_l3_bps: median(|| bench_zstd(&buf, 3), runs),
        zstd_l9_bps: median(|| bench_zstd(&buf, 9), runs),
    }
}

/// Estimated single-core zstd throughput at an arbitrary level, interpolated.
pub fn zstd_speed_l3_l9(bench: &CpuBench, level: i32) -> f64 {
    let (_a, _b, t) = (3.0, 9.0, (level as f64 - 3.0) / 6.0);
    let linear = bench.zstd_l3_bps * (1.0 - t) + bench.zstd_l9_bps * t;
    match level {
        1 => bench.zstd_l3_bps * 1.8,
        0 | 2..=8 => linear,
        _ => bench.zstd_l9_bps / 1.5_f64.powi(level - 9),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_runs_and_zstd_speed_monotone() {
        let b = bench_cpu(Some(1 * 1024 * 1024));
        assert!(b.hash_bps > 0.0);
        assert!(b.zstd_l3_bps > 0.0);
        assert!(b.zstd_l9_bps > 0.0);
        assert!(b.cores >= 1);
        // Fillable info converts.
        let info = b.to_info();
        assert!(info.hash_mb_s > 0.0);
        assert_eq!(info.cores, b.cores);
    }

    #[test]
    fn zstd_interpolation() {
        let b = CpuBench { cores: 4, hash_bps: 1000.0, zstd_l3_bps: 300.0, zstd_l9_bps: 100.0 };
        assert!(zstd_speed_l3_l9(&b, 3) >= 300.0 - 1.0);
        assert!((zstd_speed_l3_l9(&b, 9) - 100.0).abs() < 1.0);
        assert!(zstd_speed_l3_l9(&b, 1) > zstd_speed_l3_l9(&b, 3));
        assert!(zstd_speed_l3_l9(&b, 6) < zstd_speed_l3_l9(&b, 3));
        assert!(zstd_speed_l3_l9(&b, 12) < zstd_speed_l3_l9(&b, 9));
    }
}

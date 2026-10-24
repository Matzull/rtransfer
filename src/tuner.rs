//! Heuristic tuning: turn measured CPU + network capability into concrete
//! transfer parameters (chunk size, delta block size, compression level,
//! file-worker count).
//!
//! Rules of thumb implemented here:
//!
//! * **Chunk size** targets ~20 ms of network per frame (compact enough for
//!   smooth flow control, big enough to amortize framing + syscalls).
//! * **Delta block size** is chosen per file, rsync-style: ≈ √(file size)
//!   rounded up to 4 KiB, raised only if the signature (~20 B per block) would
//!   cost more than ~1.5 s of the measured bandwidth. Small blocks find small
//!   edits; adjacent matched blocks are coalesced into one `Copy` anyway.
//! * **Compression** pays off only when the CPU can compress much faster than
//!   the network consumes: zstd 3 with ≥2× CPU headroom (9 on slow links),
//!   1 when tight, otherwise uncompressed (`Comp::None`).
//! * **Files in flight** scales with cores — multiple files overlap disk and
//!   network latency with each other.

use crate::bench::{CpuBench, zstd_speed_l3_l9};
use crate::protocol::Comp;

/// All sizes are bytes unless noted.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tuning {
    pub links: u16,
    /// Files transferred in parallel.
    pub workers: u32,
    /// Literal chunk size (unit of framing, compression and IO).
    pub chunk: u64,
    /// Delta block size override; 0 = auto per file (see [`block_for`]).
    pub block: u64,
    pub comp: Comp,
}

/// Result of the bandwidth probe.
#[derive(Debug, Clone, Copy, Default)]
pub struct NetEst {
    /// Measured aggregate bytes/s in the data direction; 0 = unknown.
    pub bps: f64,
    /// Round-trip estimate, seconds; 0 = unknown.
    pub rtt_s: f64,
}

pub const MIN_CHUNK: u64 = 16 * 1024;
pub const MAX_CHUNK: u64 = 1024 * 1024;
pub const MIN_BLOCK: u64 = 4 * 1024;
pub const MAX_BLOCK: u64 = 8 * 1024 * 1024;
/// Below this (16 MiB/s ≈ 134 Mbit/s) the auto-picker may spend more CPU on
/// compression (zstd 9) because every byte saved is worth more.
pub const SLOW_LINK_BPS: f64 = 16.0 * 1024.0 * 1024.0;

impl Tuning {
    /// Combine measurements and explicit user overrides (0 = auto).
    pub fn decide(
        bench: &CpuBench,
        net: NetEst,
        links: u16,
        workers: Option<u32>,
        chunk_override: u64,
        block_base_override: u64,
        comp_override: Option<Option<i8>>, // outer None = "auto"
    ) -> Tuning {
        // Chunk: ~20 ms of network per frame.
        let mut chunk = if net.bps > 0.0 { (0.02 * net.bps) as u64 } else { 256 * 1024 };
        chunk = chunk.clamp(MIN_CHUNK, MAX_CHUNK);
        if chunk_override > 0 {
            chunk = chunk_override.clamp(4 * 1024, 64 * MAX_CHUNK);
        }

        // Files in flight, end to end: a slot is held until the receiver
        // reports the file applied, so small files are latency-bound and need
        // many slots (6000 × 4-32 KiB to an ARM board: 8 slots 9.6 s, 32 slots 6.0 s).
        // Read/compress concurrency is bounded separately by the chunk
        // pipeline, so extra slots cost little CPU.
        let workers = workers.unwrap_or_else(|| (bench.cores * 2).clamp(8, 32));

        // Compression level (auto).
        let comp = match comp_override {
            Some(user) => user.map_or(Comp::None, |l| Comp::Zstd(l as i8)),
            None => {
                let par = (bench.cores.saturating_sub(1)).max(1);
                if net.bps == 0.0 {
                    // Unknown network (e.g. stdio pipe): zstd-3 is the safe,
                    // and widely-deployed, default.
                    Comp::Zstd(3)
                } else {
                    // Beyond level 3 zstd buys little and costs a lot: on a
                    // real build tree (1 MiB chunks) L3 = 3.01x @ 320 MB/s,
                    // L9 = 3.13x @ 84, L16 = 3.24x @ 14 per core. So: L3 when
                    // the CPU keeps 2x headroom; L9 only on slow links where
                    // ~4% fewer bytes still matter; L1 or nothing when the CPU
                    // cannot keep up. (Picking the *highest* affordable level
                    // ran L16 on a LAN: 22x the CPU for 7% less data.)
                    let fits = |l: i32, headroom: f64| zstd_speed_l3_l9(bench, l) * par as f64 >= headroom * net.bps;
                    let pick = if fits(3, 2.0) {
                        if net.bps < SLOW_LINK_BPS && fits(9, 2.0) { Some(9) } else { Some(3) }
                    } else if fits(1, 1.2) {
                        Some(1)
                    } else {
                        None
                    };
                    pick.map_or(Comp::None, |l| Comp::Zstd(l as i8))
                }
            }
        };

        // Delta block: explicit override, or 0 = auto per file (block_for).
        let block = if block_base_override > 0 {
            block_base_override.clamp(MIN_BLOCK, 64 * MAX_BLOCK)
        } else {
            0
        };

        Tuning {
            links: links.max(1),
            workers,
            chunk,
            block,
            comp,
        }
    }
}

/// Delta block size for one file. `override_block > 0` (the user's
/// `--block-bytes`) is used as is; otherwise ≈ √size rounded up to 4 KiB —
/// rsync's heuristic, which keeps both the signature (size/block × ~20 B) and
/// the literal cost of one edit (≈ one block) near √size. The signature is
/// further capped at ~1.5 s of measured bandwidth. Never exceeds the file.
///
/// (Earlier versions derived a single base from bandwidth alone, which on a
/// LAN saturated at 8 MiB: every file below that became one block, and any
/// edit resent the whole file.)
pub fn block_for(override_block: u64, file_size: u64, net: NetEst) -> u64 {
    let file = file_size.max(1);
    if override_block > 0 {
        return override_block.min(file);
    }
    let mut block = ((file as f64).sqrt() as u64).div_ceil(4096) * 4096;
    if net.bps > 0.0 {
        let needed = (20.0 * file as f64 / (1.5 * net.bps)) as u64;
        block = block.max(needed);
    }
    block.clamp(MIN_BLOCK, MAX_BLOCK).min(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bench::CpuBench;

    fn bench() -> CpuBench {
        CpuBench { cores: 8, hash_bps: 2e9, zstd_l3_bps: 400e6, zstd_l9_bps: 120e6 }
    }

    #[test]
    fn fast_network_skips_compression() {
        // 12 GB/s: even level 1 across 8 cores (5.76 GB/s) falls below the
        // 2× headroom rule — don't compress.
        let t = Tuning::decide(&bench(), NetEst { bps: 1.2e10, rtt_s: 0.0002 }, 8, None, 0, 0, None);
        assert_eq!(t.comp, Comp::None);
        assert_eq!(t.chunk, MAX_CHUNK);
        assert!(t.workers >= 2);
        assert_eq!(t.links, 8);
    }

    #[test]
    fn slow_network_gets_strong_compression() {
        // 100 Mbit/s ≈ 12 MB/s: level 9 at 120 MB/s × 7 cores passes 2×.
        let t = Tuning::decide(&bench(), NetEst { bps: 12e6, rtt_s: 0.02 }, 4, None, 0, 0, None);
        assert!(matches!(t.comp, Comp::Zstd(l) if l >= 9));
        // Chunk ~20ms×12MB/s = 240KB.
        assert!((64..=512).contains(&(t.chunk / 1024)));
        // Block size is auto (decided per file).
        assert_eq!(t.block, 0);
    }

    #[test]
    fn lan_gets_cheap_compression() {
        // A ~50 MB/s LAN (e.g. a small ARM board): plenty of CPU headroom, but levels above 3
        // would burn many times the CPU for a few percent — stay at 3.
        let t = Tuning::decide(&bench(), NetEst { bps: 50e6, rtt_s: 0.003 }, 4, None, 0, 0, None);
        assert_eq!(t.comp, Comp::Zstd(3));
    }

    #[test]
    fn unknown_network_defaults_zstd3() {
        let t = Tuning::decide(&bench(), NetEst::default(), 1, None, 0, 0, None);
        assert!(matches!(t.comp, Comp::Zstd(3)));
        assert_eq!(t.links, 1);
    }

    #[test]
    fn overrides_are_respected() {
        let t = Tuning::decide(
            &bench(),
            NetEst { bps: 10e6, rtt_s: 0.01 },
            2,
            Some(7),
            512 * 1024,
            2 * 1024 * 1024,
            Some(Some(5)),
        );
        assert_eq!(t.workers, 7);
        assert_eq!(t.chunk, 512 * 1024);
        assert!(matches!(t.comp, Comp::Zstd(5)));
        assert!(t.block >= 2 * 1024 * 1024);
    }

    #[test]
    fn block_for_scales_with_size() {
        let net = NetEst { bps: 12e6, rtt_s: 0.02 };
        let lan = NetEst { bps: 1e8, rtt_s: 0.001 };
        // A few-MiB build artifact gets small blocks, so one edit costs KiBs
        // (the old bandwidth-only base made it a single 8 MiB block).
        assert_eq!(block_for(0, 5 << 20, lan), 4096);
        // √size growth, 4 KiB granularity: 1 GiB → 32 KiB.
        assert_eq!(block_for(0, 1 << 30, lan), 32 * 1024);
        // 100 GiB over 12 MB/s: the budget needs ≈111 KB, so √size (320 KiB)
        // still wins; over 1 MB/s the budget (≈1.43 MB) dominates.
        let huge = 100 * 1024 * 1024 * 1024;
        assert_eq!(block_for(0, huge, net), 320 * 1024);
        let slow = block_for(0, huge, NetEst { bps: 1e6, rtt_s: 0.05 });
        assert!((1_400_000..=MAX_BLOCK).contains(&slow), "{slow}");
        // Never larger than the file; explicit override wins (also capped).
        assert_eq!(block_for(0, 512, net), 512);
        assert_eq!(block_for(256 * 1024, 10 << 20, net), 256 * 1024);
        assert_eq!(block_for(256 * 1024, 10_000, net), 10_000);
    }
}

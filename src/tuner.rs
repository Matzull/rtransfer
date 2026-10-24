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
//! * **Compression** is not decided here: unless the user forces a codec, the
//!   sender switches zstd 3 on and off at runtime by measured throughput
//!   (see [`crate::compctl`]).
//! * **Files in flight** scales with cores — multiple files overlap disk and
//!   network latency with each other.

use crate::bench::CpuBench;
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
    /// Codec to use: the forced one, or the level the runtime controller
    /// toggles when [`Self::auto_comp`] is set.
    pub comp: Comp,
    /// Compression left to the runtime controller (`--compression auto`).
    pub auto_comp: bool,
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
/// zstd level the runtime controller uses. Beyond 3 zstd buys little and
/// costs a lot: on a real build tree (1 MiB chunks) L3 = 3.01x @ 320 MB/s,
/// L9 = 3.13x @ 84, L16 = 3.24x @ 14 per core.
pub const AUTO_ZSTD_LEVEL: i8 = 3;

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
        // Chunk: ~20 ms of network per frame when the bandwidth is known;
        // otherwise the largest (cheapest per byte to frame and compress).
        let mut chunk = if net.bps > 0.0 { (0.02 * net.bps) as u64 } else { MAX_CHUNK };
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

        let (comp, auto_comp) = match comp_override {
            Some(user) => (user.map_or(Comp::None, Comp::Zstd), false),
            None => (Comp::Zstd(AUTO_ZSTD_LEVEL), true),
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
            auto_comp,
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
        CpuBench { cores: 8 }
    }

    #[test]
    fn auto_compression_is_left_to_the_runtime_controller() {
        for net in [NetEst { bps: 1.2e10, rtt_s: 0.0002 }, NetEst { bps: 12e6, rtt_s: 0.02 }, NetEst::default()] {
            let t = Tuning::decide(&bench(), net, 4, None, 0, 0, None);
            assert!(t.auto_comp);
            assert_eq!(t.comp, Comp::Zstd(AUTO_ZSTD_LEVEL));
            assert_eq!(t.block, 0, "block size is auto (decided per file)");
        }
    }

    #[test]
    fn chunk_follows_bandwidth_or_defaults_to_max() {
        // ~20 ms of a 12 MB/s link ≈ 240 KB.
        let t = Tuning::decide(&bench(), NetEst { bps: 12e6, rtt_s: 0.02 }, 4, None, 0, 0, None);
        assert!((64..=512).contains(&(t.chunk / 1024)));
        let t = Tuning::decide(&bench(), NetEst::default(), 1, None, 0, 0, None);
        assert_eq!(t.chunk, MAX_CHUNK);
        assert_eq!(t.links, 1);
        assert!(t.workers >= 8);
    }

    #[test]
    fn forced_none_disables_auto() {
        let t = Tuning::decide(&bench(), NetEst::default(), 4, None, 0, 0, Some(None));
        assert_eq!(t.comp, Comp::None);
        assert!(!t.auto_comp);
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
        assert!(!t.auto_comp);
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

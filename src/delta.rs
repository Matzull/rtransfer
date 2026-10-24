//! Rsync-style delta transfers, modernized.
//!
//! The receiver computes a *signature* of the destination file: for every
//! `block_len`-sized block it stores a weak rolling checksum (adler-family)
//! and a strong checksum (blake3, truncated to 16 bytes). The sender looks
//! that signature index up while *rolling* over its own file — the same
//! windowed scan rsync uses, so content that moved anywhere in the file still
//! matches — and emits `Copy` instructions for re-usable blocks and
//! `Literal` instructions for the rest.
//!
//! Scanning is parallelized over segments of the file. Each worker owns a
//! range of *offsets* and reads `[seg_start, min(file_len, seg_end + block))`,
//! so every rolling window is computed exactly once. Candidates are merged
//! left-to-right greedily, which reproduces sequential rsync behavior.

use std::collections::HashMap;

use rand::RngCore;
use rand::rngs::StdRng;
use rand::SeedableRng;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

/// One block signature: weak (rolling) + strong (blake3, truncated 16 bytes).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub struct BlockSig {
    pub weak: u32,
    pub strong: [u8; 16],
}

/// Weak rolling checksum (adler-32 family, mod 65521) as used by rsync.
/// [`Self::roll`] updates it one byte at a time.
#[derive(Debug, Clone, Copy)]
pub struct WeakSum {
    s1: u32,
    s2: u32,
}

const MOD: u32 = 65521;

impl WeakSum {
    /// Build the checksum of a full window (must be exactly the block length).
    fn init(buf: &[u8]) -> Self {
        let mut s1 = 0u32;
        let mut s2 = 0u32;
        for &b in buf {
            s1 = (s1 + b as u32) % MOD;
            s2 = (s2 + s1) % MOD;
        }
        WeakSum { s1, s2 }
    }

    #[inline]
    fn value(&self) -> u32 {
        (self.s2 << 16) | self.s1
    }

    /// Slide the window one byte: remove `old`, add `new`.
    #[inline]
    fn roll(&mut self, new: u8, old: u8, len: u32) {
        let lenm = len % MOD;
        let oldm = old as u32 % MOD;
        // s1' = s1 + new - old; s2' = s2 + s1' - len * old
        self.s1 = (self.s1 + new as u32 + MOD - oldm) % MOD;
        self.s2 = (self.s2 + self.s1 + MOD - lenm.wrapping_mul(oldm) % MOD) % MOD;
    }
}

/// Strong hash for one block: blake3 truncated to 16 bytes.
#[inline]
pub fn strong_sum(buf: &[u8]) -> [u8; 16] {
    let mut h = blake3::Hasher::new();
    h.update(buf);
    (*h.finalize().as_bytes())[..16].try_into().unwrap()
}

/// Lookup index over one signature: weak -> block indices, plus the blocks.
#[derive(Debug, Clone)]
pub struct SigIndex {
    pub block_len: usize,
    pub blocks: Vec<BlockSig>,
    map: HashMap<u32, Vec<u32>>,
}

impl SigIndex {
    pub fn build(block_len: usize, blocks: Vec<BlockSig>) -> Self {
        let mut map: HashMap<u32, Vec<u32>> = HashMap::with_capacity(blocks.len() * 2 + 16);
        for (i, b) in blocks.iter().enumerate() {
            map.entry(b.weak).or_default().push(i as u32);
        }
        SigIndex { block_len, blocks, map }
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// All old-file offsets whose full signature matches `window`.
    pub fn candidates(&self, window: &[u8]) -> Vec<u64> {
        if window.len() != self.block_len || self.blocks.is_empty() {
            return Vec::new();
        }
        let weak = WeakSum::init(window).value();
        let mut out = Vec::new();
        if let Some(idxs) = self.map.get(&weak) {
            let strong = strong_sum(window);
            for i in idxs {
                if self.blocks[*i as usize].strong == strong {
                    out.push(*i as u64 * self.block_len as u64);
                }
            }
        }
        out.sort_unstable();
        out
    }
}

/// A place where the receiver's old content reappears inside the new file.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Candidate {
    pub new_off: u64,
    pub old_off: u64,
}

/// One delta instruction. Literals are streamed as `Data` chunks and copies
/// as `Copy` frames (see `protocol.rs`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Instr {
    Copy { new_off: u64, old_off: u64, len: u64 },
    Literal { new_off: u64, len: u64 },
}

#[derive(Debug, Default)]
pub struct ScanOut {
    /// Signatures of aligned full blocks inside the segment, offset order.
    pub sig: Vec<(u64, BlockSig)>,
    pub cands: Vec<Candidate>,
}

/// Scan one segment. `buf` holds the bytes of
/// `[seg_start, min(file_len, seg_end + block_len))`; this segment owns
/// positions within `[seg_start, seg_end)`.
pub fn scan_segment(
    buf: &[u8],
    seg_start: u64,
    seg_end: u64,
    file_len: u64,
    block_len: usize,
    index: Option<&SigIndex>,
) -> ScanOut {
    let block = block_len as u64;
    let mut out = ScanOut::default();
    if file_len == 0 || block_len == 0 || block > file_len {
        return out;
    }
    // Valid positions p satisfy p + block <= file_len and belong to
    // [seg_start, seg_end).
    let end_pos = seg_end.min(file_len - block + 1).min(seg_start + buf.len() as u64 - block + 1);
    if end_pos <= seg_start {
        return out;
    }
    // `buf` starts at `seg_start`, so the first window is at index 0.
    let mut weak = WeakSum::init(&buf[..block_len]);
    let mut pos = seg_start;

    loop {
        if index.is_none() && pos % block == 0 {
            let w = &buf[(pos - seg_start) as usize..(pos - seg_start) as usize + block_len];
            out.sig.push((pos, BlockSig { weak: weak.value(), strong: strong_sum(w) }));
        }
        if let Some(index) = index {
            let weak_val = weak.value();
            if let Some(idxs) = index.map.get(&weak_val) {
                let w = &buf[(pos - seg_start) as usize..(pos - seg_start) as usize + block_len];
                let strong = strong_sum(w);
                if let Some(old_off) = idxs
                    .iter()
                    .filter(|&&i| index.blocks[i as usize].strong == strong)
                    .map(|&i| i as u64 * block)
                    .min()
                {
                    out.cands.push(Candidate { new_off: pos, old_off });
                }
            }
        }
        if pos + 1 >= end_pos {
            break;
        }
        let drop_off = (pos - seg_start) as usize;
        weak.roll(buf[drop_off + block_len], buf[drop_off], block_len as u32);
        pos += 1;
    }
    out
}

fn plan_segments(file_len: u64, block: u64, workers: usize) -> Vec<(u64, u64)> {
    if file_len < block {
        return Vec::new();
    }
    let span = file_len - block + 1; // scannable positions [0, span)
    // Enough segments to keep every core busy, each at least ~8 MiB of
    // scanning (bounds syscall overhead from per-segment reads).
    let min_seg = (8 * 1024 * 1024).max(block * 4);
    let n_segs = span.max(1) / min_seg + 1;
    let cap = (workers.max(1) as u64).min(span).min(65536);
    let n_segs = n_segs.min(cap).clamp(1, 4096);
    let seg_len = span / n_segs;
    let mut segs = Vec::with_capacity(n_segs as usize);
    let mut start = 0u64;
    for i in 0..n_segs {
        let end = if i + 1 == n_segs { span } else { start + seg_len };
        segs.push((start, end));
        start = end;
    }
    segs
}

/// Deterministic fast pseudo-random filler (probe payloads / tests).
pub fn fill_random(dst: &mut [u8], seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    rng.fill_bytes(dst);
}

/// Merge candidates (ascending `new_off`) into the instruction stream that
/// covers `[0, file_len)` exactly.
pub fn merge_candidates(cands: &[Candidate], file_len: u64, block_len: u64) -> Vec<Instr> {
    let mut out = Vec::new();
    let mut cursor = 0u64;
    for c in cands {
        if c.new_off < cursor {
            continue;
        }
        if c.new_off > cursor {
            out.push(Instr::Literal { new_off: cursor, len: c.new_off - cursor });
        }
        // Coalesce a block that continues the previous copy on both sides:
        // one Copy frame (and one sequential read/write on the receiver)
        // instead of one per block — matters with small √size blocks.
        match out.last_mut() {
            Some(Instr::Copy { new_off, old_off, len })
                if *new_off + *len == c.new_off && *old_off + *len == c.old_off =>
            {
                *len += block_len;
            }
            _ => out.push(Instr::Copy { new_off: c.new_off, old_off: c.old_off, len: block_len }),
        }
        cursor = c.new_off + block_len;
    }
    if cursor < file_len {
        out.push(Instr::Literal { new_off: cursor, len: file_len - cursor });
    }
    out
}

/// Signature-only pass over the whole file (receiver side). `read_range`
/// must return exactly the requested range; it runs on the rayon pool.
pub fn full_signature(
    read_range: impl Fn(u64, u64) -> std::io::Result<Vec<u8>> + Sync,
    file_len: u64,
    block_len: u64,
    workers: usize,
) -> std::io::Result<Vec<BlockSig>> {
    if file_len == 0 || file_len < block_len {
        return Ok(Vec::new());
    }
    let mut sig: Vec<(u64, BlockSig)> = plan_segments(file_len, block_len, workers)
        .into_par_iter()
        .map(|(s, e)| {
            let buf = read_range(s, (e + block_len).min(file_len) - s)?;
            Ok(scan_segment(&buf, s, e, file_len, block_len as usize, None).sig)
        })
        .collect::<Result<Vec<_>, std::io::Error>>()?
        .concat();
    sig.sort_unstable_by_key(|&(off, _)| off);
    Ok(sig.into_iter().map(|(_, s)| s).collect())
}

/// Sender-side scan of the new file against the receiver's signature index.
/// Returns the instruction stream covering the entire file.
pub fn scan_for_delta(
    read_range: impl Fn(u64, u64) -> std::io::Result<Vec<u8>> + Sync,
    file_len: u64,
    index: &SigIndex,
    workers: usize,
) -> std::io::Result<Vec<Instr>> {
    let block = index.block_len as u64;
    if file_len == 0 {
        return Ok(Vec::new());
    }
    if index.is_empty() {
        return Ok(vec![Instr::Literal { new_off: 0, len: file_len }]);
    }
    let mut cands: Vec<Candidate> = plan_segments(file_len, block, workers)
        .into_par_iter()
        .map(|(s, e)| {
            let buf = read_range(s, (e + block).min(file_len) - s)?;
            Ok(scan_segment(&buf, s, e, file_len, index.block_len, Some(index)).cands)
        })
        .collect::<Result<Vec<_>, std::io::Error>>()?
        .concat();
    cands.sort_unstable_by_key(|c| c.new_off);
    Ok(merge_candidates(&cands, file_len, block))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(seed: u64, len: usize) -> Vec<u8> {
        let mut v = vec![0u8; len];
        fill_random(&mut v, seed);
        v
    }

    fn index_of(buf: &[u8], block: u64, workers: usize) -> SigIndex {
        let blocks = full_signature(
            |off, len| Ok(buf[off as usize..(off + len) as usize].to_vec()),
            buf.len() as u64,
            block,
            workers,
        )
        .unwrap();
        SigIndex::build(block as usize, blocks)
    }

    fn apply(old: &[u8], new: &[u8], instrs: &[Instr]) -> Vec<u8> {
        let mut out = Vec::with_capacity(new.len());
        let mut cursor = 0u64;
        let mut copied = 0u64;
        for ins in instrs {
            match *ins {
                Instr::Copy { new_off, old_off, len } => {
                    assert_eq!(cursor, new_off);
                    out.extend_from_slice(&old[old_off as usize..(old_off + len) as usize]);
                    copied += len;
                    cursor = new_off + len;
                }
                Instr::Literal { new_off, len } => {
                    assert_eq!(cursor, new_off);
                    let n = new[new_off as usize..(new_off + len) as usize].to_vec();
                    out.extend_from_slice(&n);
                    cursor = new_off + len;
                }
            }
        }
        assert_eq!(cursor, new.len() as u64);
        // Test helper also reports coverage through a side channel.
        COVER.with(|c| c.set(copied));
        out
    }

    thread_local! {
        static COVER: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }

    fn copied_bytes_of(old: &[u8], new: &[u8], block: u64, workers: usize) -> Vec<u8> {
        let index = index_of(old, block, workers);
        let instrs = scan_for_delta(
            |off, len| Ok(new[off as usize..(off + len) as usize].to_vec()),
            new.len() as u64,
            &index,
            workers,
        )
        .unwrap();
        apply(old, new, &instrs)
    }

    fn assert_rebuild(old: &[u8], new: &[u8], block: u64, workers: usize, min_copy: u64) {
        COVER.with(|c| c.set(0));
        let out = copied_bytes_of(old, new, block, workers);
        assert_eq!(out, new, "reconstruction must reproduce the new file");
        let copied = COVER.with(|c| c.get());
        assert!(copied >= min_copy, "expected at least {min_copy} copied bytes, got {copied}");
    }

    #[test]
    fn weak_sum_matches_reference() {
        let buf = data(1, 4096);
        let w = WeakSum::init(&buf[..1024]).value();
        let mut s1 = 0u32;
        let mut s2 = 0u32;
        for &b in &buf[..1024] {
            s1 = (s1 + b as u32) % MOD;
            s2 = (s2 + s1) % MOD;
        }
        assert_eq!(w, s1 | (s2 << 16));
        // Rolling across a window equals re-initializing at that offset.
        let mut rw = WeakSum::init(&buf[..512]);
        for p in 1..(4096 - 512) {
            rw.roll(buf[p + 511], buf[p - 1], 512);
            assert_eq!(rw.value(), WeakSum::init(&buf[p..p + 512]).value(), "p = {p}");
        }
    }

    #[test]
    fn roundtrip_append() {
        let old = data(2, 4 * 1024 * 1024);
        let mut new = old.clone();
        new.extend_from_slice(&data(3, 256 * 1024));
        // ~98% should be re-usable.
        assert_rebuild(&old, &new, 65536, 4, new.len() as u64 - 1 * 1024 * 1024);
    }

    #[test]
    fn roundtrip_prepend_and_middle() {
        let old = data(4, 4 * 1024 * 1024);
        let mut new = data(5, 100_000);
        new.extend_from_slice(&old);
        let mid = old.len() / 2;
        let mut spliced = old[..mid].to_vec();
        spliced.extend_from_slice(&data(6, 50_000));
        spliced.extend_from_slice(&old[mid..]);
        assert_rebuild(&old, &new, 65536, 4, new.len() as u64 - 2 * 1024 * 1024);
        assert_rebuild(&old, &spliced, 65536, 4, new.len() as u64 / 3);
    }

    #[test]
    fn roundtrip_shrink_and_grow_block_sizes() {
        let old = data(7, 3 * 1024 * 1024);
        let mut new = old[..1024 * 1024].to_vec();
        new.extend_from_slice(&data(8, 1024 * 1024));
        for block in [4096u64, 65536, 262_144] {
            for workers in [1usize, 3, 8] {
                assert_rebuild(&old, &new, block, workers, new.len() as u64 / 2);
            }
        }
    }

    /// A mid-file insertion with small blocks must yield exactly
    /// copy · literal · copy — adjacent block matches coalesce.
    #[test]
    fn adjacent_copies_coalesce() {
        let old = data(13, 2 * 1024 * 1024);
        let mut new = old[..1024 * 1024].to_vec();
        new.extend_from_slice(&data(14, 4096));
        new.extend_from_slice(&old[1024 * 1024..]);
        let index = index_of(&old, 4096, 4);
        let instrs = scan_for_delta(
            |off, len| Ok(new[off as usize..(off + len) as usize].to_vec()),
            new.len() as u64,
            &index,
            4,
        )
        .unwrap();
        assert_eq!(apply(&old, &new, &instrs), new);
        let copies = instrs.iter().filter(|i| matches!(i, Instr::Copy { .. })).count();
        assert_eq!(copies, 2, "expected 2 coalesced copies, got {instrs:?}");
    }

    /// Files above the 8 MiB segment minimum are scanned in several
    /// segments; every segment's buffer starts at its own offset.
    #[test]
    fn multi_segment_files_roundtrip() {
        let old = data(15, 20 * 1024 * 1024 + 123);
        let mut new = old[..12 * 1024 * 1024].to_vec();
        new.extend_from_slice(&data(16, 65536));
        new.extend_from_slice(&old[12 * 1024 * 1024..]);
        assert!(plan_segments(old.len() as u64, 4096, 4).len() >= 2);
        assert_rebuild(&old, &new, 4096, 4, old.len() as u64 - 8192);
    }

    #[test]
    fn whole_file_only_literals_when_no_matching_blocks() {
        let old = data(9, 1024);
        let new = data(10, 2000);
        let out = copied_bytes_of(&old, &new, 256, 2);
        assert_eq!(out, new);
    }

    #[test]
    fn candidates_found_at_moved_positions() {
        let block = 65536u64;
        let payload = data(11, block as usize);
        let tail = data(12, 4 * 1024 * 1024);
        let old = {
            let mut o = tail.clone();
            o.extend_from_slice(&payload);
            o
        };
        let index = index_of(&old, block, 4);
        // Payload reappears somewhere inside; its offset is block-aligned.
        let c = index.candidates(&payload);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0], tail.len() as u64);
        // The first old block is a different match target from the payload.
        assert_ne!(index.blocks[0].strong, strong_sum(&payload));
    }

    #[test]
    fn segment_coverage_is_exact() {
        let file_len = 10 * 1024 * 1024 + 77u64;
        let block = 65536u64;
        let span = file_len - block + 1;
        let segs = plan_segments(file_len, block, 4);
        assert!(segs.len() >= 2);
        let mut last = 0u64;
        for &(s, e) in &segs {
            assert_eq!(s, last);
            assert!(e > s);
            last = e;
        }
        assert_eq!(last, span);
    }
}

//! Wire-codec shootout for rtransfer's message shapes.
//!
//! The types mirror `rtransfer::protocol::Msg` (only the variants that matter
//! for cost): bulk `Data` frames, tiny control `Plan` frames, and a large
//! `Manifest`. `MsgBytes` is the same type with `#[serde(with = "serde_bytes")]`
//! on the payload, the one-line fix for serde's per-element `Vec<u8>` path.
//! rkyv is measured twice: full deserialize into an owned `Msg` (comparable
//! to the others) and zero-copy `access` (reads the archive in place; its
//! MiB/s figures are not comparable, nothing is copied).
//!
//! Run: `cargo run --release` (or `make bench-serde` from the repo root).

use std::hint::black_box;
use std::time::{Duration, Instant};

use rand::RngCore;
use serde::{Deserialize, Serialize};

// ─── mirrored message shapes ─────────────────────────────────────────────────

#[derive(Serialize, Deserialize, bincode::Encode, bincode::Decode, bitcode::Encode, bitcode::Decode, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, PartialEq, Debug)]
enum Comp {
    None,
    Zstd(i8),
}

#[derive(Serialize, Deserialize, bincode::Encode, bincode::Decode, bitcode::Encode, bitcode::Decode, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, PartialEq, Debug)]
enum PlanAction {
    Skip,
    Verify { hash: [u8; 32] },
    Whole,
    Delta,
}

#[derive(Serialize, Deserialize, bincode::Encode, bincode::Decode, bitcode::Encode, bitcode::Decode, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, PartialEq, Debug)]
struct FileEntry {
    id: u32,
    rel: String,
    size: u64,
    mtime: Option<i64>,
}

/// As in the crate today: plain `Vec<u8>` payload.
#[derive(Serialize, Deserialize, bincode::Encode, bincode::Decode, bitcode::Encode, bitcode::Decode, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, PartialEq, Debug)]
enum Msg {
    Plan { id: u32, action: PlanAction },
    Data { id: u32, seq: u64, offset: u64, comp: Comp, orig_len: u64, data: Vec<u8> },
    Manifest { files: Vec<FileEntry> },
}

/// Same, with the payload routed through `serde_bytes`.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
enum MsgBytes {
    Plan { id: u32, action: PlanAction },
    Data {
        id: u32,
        seq: u64,
        offset: u64,
        comp: Comp,
        orig_len: u64,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    Manifest { files: Vec<FileEntry> },
}

impl From<&Msg> for MsgBytes {
    fn from(m: &Msg) -> Self {
        match m.clone() {
            Msg::Plan { id, action } => MsgBytes::Plan { id, action },
            Msg::Data { id, seq, offset, comp, orig_len, data } => {
                MsgBytes::Data { id, seq, offset, comp, orig_len, data }
            }
            Msg::Manifest { files } => MsgBytes::Manifest { files },
        }
    }
}

// ─── workloads ───────────────────────────────────────────────────────────────

fn data_msg() -> Msg {
    let mut data = vec![0u8; 1 << 20];
    rand::rng().fill_bytes(&mut data); // incompressible, like a zstd'd chunk
    Msg::Data { id: 7, seq: 42, offset: 42 << 20, comp: Comp::Zstd(3), orig_len: 1 << 20, data }
}

fn plan_msg() -> Msg {
    Msg::Plan { id: 12_345, action: PlanAction::Verify { hash: [0xAB; 32] } }
}

/// ~15k entries with rust-`target/`-like paths (a large build tree).
fn manifest_msg() -> Msg {
    let files = (0..15_000u32)
        .map(|i| FileEntry {
            id: i,
            rel: format!("target/release/deps/libcrate_{:04}-{:016x}.rmeta", i % 700, u64::from(i) * 0x9E37_79B9_7F4A_7C15),
            size: 1_000 + u64::from(i) * 4_099,
            mtime: Some(1_792_740_096_000 + i64::from(i)),
        })
        .collect();
    Msg::Manifest { files }
}

// ─── codecs ──────────────────────────────────────────────────────────────────

/// Encoder output: rkyv produces an `AlignedVec`, and converting it to a
/// `Vec<u8>` would charge it an extra copy the real code would not make.
enum Encoded {
    Vec(Vec<u8>),
    Aligned(rkyv::util::AlignedVec),
}

impl Encoded {
    fn as_slice(&self) -> &[u8] {
        match self {
            Encoded::Vec(v) => v,
            Encoded::Aligned(a) => a,
        }
    }
}

struct Codec {
    name: &'static str,
    enc: fn(&Msg) -> Encoded,
    /// Input is always 16-byte aligned (rkyv requires it; a receiver would
    /// read frames into an aligned buffer — free for the other codecs).
    dec: fn(&[u8]) -> Msg,
}

type RkyvErr = rkyv::rancor::Error;

fn bincfg() -> bincode::config::Configuration {
    bincode::config::standard()
}

fn bytes_back(m: MsgBytes) -> Msg {
    match m {
        MsgBytes::Plan { id, action } => Msg::Plan { id, action },
        MsgBytes::Data { id, seq, offset, comp, orig_len, data } => {
            Msg::Data { id, seq, offset, comp, orig_len, data }
        }
        MsgBytes::Manifest { files } => Msg::Manifest { files },
    }
}

// Encode-side conversion to `MsgBytes` clones the payload; it is timed
// separately below and subtracted so the number reflects the codec alone.
const CODECS: &[Codec] = &[
    Codec {
        name: "bincode2 serde (old)",
        enc: |m| Encoded::Vec(bincode::serde::encode_to_vec(m, bincfg()).unwrap()),
        dec: |b| bincode::serde::decode_from_slice(b, bincfg()).unwrap().0,
    },
    Codec {
        name: "bincode2 serde+serde_bytes",
        enc: |m| Encoded::Vec(bincode::serde::encode_to_vec(MsgBytes::from(m), bincfg()).unwrap()),
        dec: |b| bytes_back(bincode::serde::decode_from_slice(b, bincfg()).unwrap().0),
    },
    Codec {
        name: "bincode2 native derive",
        enc: |m| Encoded::Vec(bincode::encode_to_vec(m, bincfg()).unwrap()),
        dec: |b| bincode::decode_from_slice(b, bincfg()).unwrap().0,
    },
    Codec {
        name: "bitcode serde",
        enc: |m| Encoded::Vec(bitcode::serialize(m).unwrap()),
        dec: |b| bitcode::deserialize(b).unwrap(),
    },
    Codec {
        name: "bitcode native derive",
        enc: |m| Encoded::Vec(bitcode::encode(m)),
        dec: |b| bitcode::decode(b).unwrap(),
    },
    Codec {
        name: "postcard serde+serde_bytes",
        enc: |m| Encoded::Vec(postcard::to_allocvec(&MsgBytes::from(m)).unwrap()),
        dec: |b| bytes_back(postcard::from_bytes(b).unwrap()),
    },
    Codec {
        name: "rkyv (full deserialize)",
        enc: |m| Encoded::Aligned(rkyv::to_bytes::<RkyvErr>(m).unwrap()),
        dec: |b| rkyv::from_bytes::<Msg, RkyvErr>(b).unwrap(),
    },
];

/// rkyv's actual pitch: read the archive in place, no owned `Msg` built.
/// For `Data` that means the payload is a borrowed `&[u8]` into the frame.
fn rkyv_access_rows(bytes: &[u8], raw_bytes: usize, budget: Duration) {
    let rows: [(&str, f64); 2] = [
        (
            "rkyv access (validated)",
            time_ns(budget, || {
                black_box(rkyv::access::<ArchivedMsg, RkyvErr>(black_box(bytes)).unwrap());
            }),
        ),
        (
            "rkyv access_unchecked",
            time_ns(budget, || {
                // SAFETY: `bytes` is a valid archive produced by `rkyv::to_bytes`
                // just above (and validated by the row before). Untrusted wire
                // input would need the validated path.
                black_box(unsafe { rkyv::access_unchecked::<ArchivedMsg>(black_box(bytes)) });
            }),
        ),
    ];
    for (name, ns) in rows {
        println!(
            "{:<28} {:>10} {:>11} {:>11} {:>11} {:>11.0}",
            name,
            bytes.len(),
            "(same)",
            fmt_time(ns),
            "",
            mib_s(raw_bytes, ns)
        );
    }
}

fn mib_s(raw_bytes: usize, ns: f64) -> f64 {
    raw_bytes as f64 / (ns / 1e9) / (1024.0 * 1024.0)
}

/// Codecs whose encode path pays a `MsgBytes::from` clone.
fn converts(name: &str) -> bool {
    name.contains("serde_bytes")
}

// ─── harness ─────────────────────────────────────────────────────────────────

/// Best-of-5 nanoseconds per call, each sample running ≥ `budget`.
///
/// The clock is read once per batch, not per call: `Instant::now()` costs
/// ~20-25 ns, which would otherwise swamp the sub-100 ns rows. The batch is
/// sized from a calibration run to take roughly 1 ms.
fn time_ns(budget: Duration, mut f: impl FnMut()) -> f64 {
    f(); // warm-up
    let t0 = Instant::now();
    let mut calib = 0u64;
    while t0.elapsed() < Duration::from_millis(5) {
        f();
        calib += 1;
    }
    let batch = (calib / 5).max(1);
    let mut best = f64::INFINITY;
    for _ in 0..5 {
        let (mut n, t0) = (0u64, Instant::now());
        while t0.elapsed() < budget {
            for _ in 0..batch {
                f();
            }
            n += batch;
        }
        best = best.min(t0.elapsed().as_nanos() as f64 / n as f64);
    }
    best
}

fn fmt_time(ns: f64) -> String {
    match ns {
        n if n < 1e3 => format!("{n:.0} ns"),
        n if n < 1e6 => format!("{:.1} µs", n / 1e3),
        n => format!("{:.2} ms", n / 1e6),
    }
}

fn run(title: &str, msg: &Msg, raw_bytes: usize) {
    let budget = Duration::from_millis(120);
    let clone_ns = time_ns(budget, || {
        black_box(MsgBytes::from(black_box(msg)));
    });
    println!("\n## {title}");
    println!("{:<28} {:>10} {:>11} {:>11} {:>11} {:>11}", "codec", "size", "encode", "decode", "enc MiB/s", "dec MiB/s");
    let mut rkyv_bytes = rkyv::util::AlignedVec::<16>::new();
    for c in CODECS {
        let mut bytes = rkyv::util::AlignedVec::<16>::new();
        
        bytes.extend_from_slice((c.enc)(msg).as_slice());
        let bytes: &[u8] = &bytes;
        assert_eq!(&(c.dec)(bytes), msg, "{} does not round-trip", c.name);
        if c.name.starts_with("rkyv") {
            rkyv_bytes.extend_from_slice(bytes);
        }
        let mut enc = time_ns(budget, || {
            black_box((c.enc)(black_box(msg)));
        });
        if converts(c.name) {
            enc = (enc - clone_ns).max(1.0);
        }
        let dec = time_ns(budget, || {
            black_box((c.dec)(black_box(bytes)));
        });
        println!(
            "{:<28} {:>10} {:>11} {:>11} {:>11.0} {:>11.0}",
            c.name,
            bytes.len(),
            fmt_time(enc),
            fmt_time(dec),
            mib_s(raw_bytes, enc),
            mib_s(raw_bytes, dec)
        );
    }
    rkyv_access_rows(&rkyv_bytes, raw_bytes, budget);
}

fn main() {
    println!("rtransfer wire-codec benchmark (best of 5; MiB/s over the logical payload)");
    let data = data_msg();
    run("Data frame, 1 MiB payload (bulk path)", &data, 1 << 20);
    let plan = plan_msg();
    run("Plan frame (control path, per file)", &plan, 41);
    let manifest = manifest_msg();
    let raw: usize = match &manifest {
        Msg::Manifest { files } => files.iter().map(|f| f.rel.len() + 4 + 8 + 9).sum(),
        _ => unreachable!(),
    };
    run("Manifest, 15k entries", &manifest, raw);
}

//! Wire protocol (RTFP v1) for rtransfer.
//!
//! Framing is simple and binary: every frame is
//!
//! ```text
//! [u32 big-endian length][payload]
//! ```
//!
//! where `payload` is a bincode-2 (standard config, 16 MiB limit) encoding of a
//! single [`Msg`]. Frames travel over `AsyncRead + AsyncWrite` links (raw TCP,
//! SSH channels or stdio pipes alike).

use serde::{Deserialize, Serialize};

use crate::delta::BlockSig;

/// Protocol magic word, sent inside the first frame of every link.
pub const MAGIC: u32 = u32::from_le_bytes(*b"RTFP");
/// Wire protocol version understood by this build.
pub const VERSION: u32 = 1;

/// Hard cap for a single frame payload (signatures and probes stay far below).
pub const FRAME_LIMIT: usize = 16 * 1024 * 1024;

/// Sentinel content hash. In `SendDone` it aborts the file on the receiver
/// (whatever `last_seq` says); in `FileHash` it means "could not hash", which
/// never equals a real hash.
pub const ABORT_HASH: [u8; 32] = [0xFF; 32];

fn codec() -> impl bincode::config::Config {
    bincode::config::standard()
        .with_limit::<FRAME_LIMIT>()
}

/// Which direction files are copied in, from the *client's* point of view.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyMode {
    /// Client pushes local files to the remote receiver.
    Push,
    /// Client pulls remote files into a local receiver.
    Pull,
}

/// Which side produces the files that will be sent on the wire.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Sender,
    Receiver,
}

impl CopyMode {
    pub fn client_role(self) -> Role {
        match self {
            CopyMode::Push => Role::Sender,
            CopyMode::Pull => Role::Receiver,
        }
    }
    pub fn server_role(self) -> Role {
        match self {
            CopyMode::Push => Role::Receiver,
            CopyMode::Pull => Role::Sender,
        }
    }
}

/// Per-chunk codec marker.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comp {
    None,
    Zstd(i8),
}

/// One file as listed in the manifest.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct FileEntry {
    pub id: u32,
    /// Unix-style relative path using only `/` separators.
    pub rel: String,
    pub size: u64,
    /// Modification time, unix milliseconds. `None` when unavailable.
    pub mtime: Option<i64>,
}

/// Plan for one file decided by the receiver.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanAction {
    /// Nothing to do: destination already matches.
    Skip,
    /// Same size, mtime differs: `hash` is the receiver's content hash. The
    /// sender always replies with its own [`Msg::FileHash`]; when equal it
    /// skips the file without waiting, otherwise a follow-up plan comes.
    Verify { hash: [u8; 32] },
    /// Send the whole file in chunks (no delta).
    Whole,
    /// Destination exists; the signature in [`Msg::SigData`] can be used.
    Delta,
}

/// Outcome of one file reported back from the receiver.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Outcome {
    Ok,
    Failed(String),
}

/// Benchmarks as measured by whoever computes tuning (not sent for now;
/// kept in the protocol for future cross-side decisions).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub struct BenchInfo {
    pub cores: u32,
    pub hash_mb_s: f32,
    pub zstd_l3_mb_s: f32,
    pub zstd_l9_mb_s: f32,
}

/// Every wire message. `Data`/`Copy`/`SendDone`/`SigData` may travel on
/// *any* link; everything else is control-plane traffic on the first link.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Msg {
    /// First frame of every link. Echoed back by the peer.
    Hello { link_id: u32, instance: u128 },
    /// Client → server: session intent and flags.
    Greeting {
        mode: CopyMode,
        delete: bool,
        allow_delta: bool,
        /// Directory the session operates on relative to the server root.
        /// Absolute paths must start with `/`.
        target: String,
        /// How many data links the client intends to establish (including
        /// control). The server awaits this many before transferring.
        links: u16,
        bench: BenchInfo,
        /// Sender-side include/exclude globs (used by the server in pull mode).
        include: Vec<String>,
        exclude: Vec<String>,
        /// Single-source push: the source tree merges into the target
        /// directory (rels carry no leading name component); `--delete`
        /// then scopes to the whole target dir. False = named-forest mode.
        merge: bool,
        /// Receiver fsyncs the part file before rename. Off by default
        /// (rsync's default too): on flash media fsync per file costs
        /// far more than the write itself.
        fsync: bool,
    },
    /// Server → client: confirmation that the target is usable.
    ServerInfo { agent: String, root: String },
    /// Fatal session error; the sender closes links afterwards.
    Error { msg: String },
    /// Server → client (stdio bootstrap): TCP port announced for extra links.
    Announce { port: u16 },

    /// Bandwidth probe. Sent in the data direction over every link.
    Probe {
        seq: u64,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    /// Reply to a probe, always sent on the control link.
    ProbeAck { seq: u64 },

    /// Sender → receiver: full file list for the session.
    Manifest { files: Vec<FileEntry> },
    /// Receiver → sender: ratchet file-by-file decisions.
    Plan { id: u32, action: PlanAction },
    /// Receiver → sender: delta signature of the destination file.
    SigData { id: u32, block_len: u64, blocks: Vec<BlockSig> },
    /// Sender → receiver: content hash of one source file (Verify flow).
    FileHash { id: u32, hash: [u8; 32] },

    /// Sender → receiver: literal content chunk, optionally compressed.
    /// `data` goes through `serde_bytes`: a single length-prefixed byte copy
    /// (same bytes as a `Vec<u8>` sequence, without the per-element cost).
    Data {
        id: u32,
        seq: u64,
        offset: u64,
        comp: Comp,
        orig_len: u64,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    /// Sender → receiver: reuse `len` bytes from the receiver's old file.
    Copy { id: u32, seq: u64, new_off: u64, old_off: u64, len: u64 },
    /// Sender → receiver: no more chunks for a file (`last_seq` = count).
    SendDone { id: u32, last_seq: u64, hash: [u8; 32] },
    /// Receiver → sender: result of applying one file.
    FileResult { id: u32, outcome: Outcome },
    /// Receiver → sender: files removed locally via `--delete` (informational).
    Deleted { rels: Vec<String> },

    /// Final message; both sides then close their links.
    Done { failed: bool },
}

impl Msg {
    pub fn error(msg: impl Into<String>) -> Msg {
        Msg::Error { msg: msg.into() }
    }
}

pub fn encode(msg: &Msg) -> Vec<u8> {
    bincode::serde::encode_to_vec(msg, codec()).expect("bincode encode cannot fail")
}

pub fn encode_into(msg: &Msg, buf: &mut Vec<u8>) {
    bincode::serde::encode_into_std_write(msg, buf, codec())
        .expect("bincode encode cannot fail");
}

pub fn decode(payload: &[u8]) -> Result<Msg, anyhow::Error> {
    let (msg, used) = bincode::serde::decode_from_slice::<Msg, _>(payload, codec())?;
    if used != payload.len() {
        anyhow::bail!("protocol frame has {} trailing bytes", payload.len() - used);
    }
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all_variants() {
        let msgs = vec![
            Msg::Hello { link_id: 0, instance: 42 },
            Msg::Greeting {
                mode: CopyMode::Push,
                delete: false,
                allow_delta: true,
                target: "/dst x/?".into(),
                links: 4,
                bench: BenchInfo { cores: 8, hash_mb_s: 2000.0, zstd_l3_mb_s: 300.0, zstd_l9_mb_s: 90.0 },
                include: vec!["*.rs".into()],
                exclude: vec!["target/**".into()],
                merge: false,
                fsync: true,
            },
            Msg::ServerInfo { agent: "rtransfer 0.1".into(), root: "/srv".into() },
            Msg::Error { msg: "no".into() },
            Msg::Announce { port: 43111 },
            Msg::Probe { seq: 3, data: vec![7u8; 512] },
            Msg::ProbeAck { seq: 3 },
            Msg::Manifest {
                files: vec![FileEntry {
                    id: 0,
                    rel: "a/b.txt".into(),
                    size: 1000,
                    mtime: Some(1700000000),
                }],
            },
            Msg::Plan { id: 0, action: PlanAction::Skip },
            Msg::Plan { id: 0, action: PlanAction::Verify { hash: [5; 32] } },
            Msg::SigData { id: 1, block_len: 65536, blocks: vec![BlockSig { weak: 7, strong: [1; 16] }] },
            Msg::FileHash { id: 2, hash: [9; 32] },
            Msg::Data { id: 3, seq: 1, offset: 4096, comp: Comp::Zstd(3), orig_len: 65536, data: vec![2; 65536] },
            Msg::Copy { id: 3, seq: 2, new_off: 8192, old_off: 0, len: 4096 },
            Msg::SendDone { id: 3, last_seq: 3, hash: [4; 32] },
            Msg::FileResult { id: 3, outcome: Outcome::Ok },
            Msg::Deleted { rels: vec!["gone".into()] },
            Msg::Done { failed: true },
        ];
        for m in msgs {
            let e = encode(&m);
            assert!(e.len() <= FRAME_LIMIT);
            assert_eq!(decode(&e).unwrap(), m);
        }
    }

    /// `serde_bytes` must not change the wire format: a byte payload still
    /// encodes as a varint length followed by the raw bytes, exactly like a
    /// plain `Vec<u8>` sequence did (peers of either build interoperate).
    #[test]
    fn byte_payload_wire_format_unchanged() {
        #[derive(Serialize)]
        enum Legacy {
            _Hello,
            _Greeting,
            _ServerInfo,
            _Error,
            _Announce,
            Probe { seq: u64, data: Vec<u8> },
        }
        let data: Vec<u8> = (0..=255u8).cycle().take(70_000).collect();
        let legacy = bincode::serde::encode_to_vec(Legacy::Probe { seq: 9, data: data.clone() }, codec()).unwrap();
        assert_eq!(encode(&Msg::Probe { seq: 9, data }), legacy);
    }
}

//! # rtransfer
//!
//! Modern rsync-like file transfer: delta-sync, parallel links, SSH or raw
//! TCP transports, io_uring IO, adaptive tuning, end-to-end blake3 integrity.
//!
//! Entry points:
//! * [`cli`] — command line definition (used by the `main` binary)
//! * [`serve`] — transfer server (stdio + TCP listener modes)
//! * [`session`] — role engines' shared session plumbing
//! * [`send`] / [`recv`] — sender and receiver role implementations

pub mod bench;
pub mod chunk;
pub mod compctl;
pub mod cli;
pub mod client;
pub mod delta;
pub mod fsops;
pub mod ioengine;
pub mod linkmux;
pub mod progress;
pub mod protocol;
pub mod recv;
pub mod send;
pub mod serve;
pub mod session;
pub mod transport;
pub mod tuner;
pub mod uring;

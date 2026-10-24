//! Command line surface.
//!
//! ```text
//! rtransfer push [OPTS] <SRC>... <REMOTE>   # local → remote
//! rtransfer pull [OPTS] <REMOTE> <DEST>     # remote → local
//! rtransfer serve [OPTS]                    # server (TCP or --stdio)
//! ```

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "rtransfer",
    version,
    about = "Fast multithreaded file transfer tool with delta-sync.",
    long_about = "rtransfer: rsync/rclone-style transfers with delta-sync, parallel \
links, SSH and raw-socket backends, per-machine tuning and end-to-end blake3 \
integrity."
)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Copy local files/dirs to a remote destination.
    Push {
        #[command(flatten)]
        args: SyncArgs,
        /// One or more local sources; the last argument is the remote.
        #[arg(required = true)]
        src: Vec<String>,
        /// Remote destination: `tcp://host:port/dir`, `ssh://[user@]host[:p]/dir`,
        /// or rsync-style (`[user@]host:/abs/path`).
        #[arg(required = true)]
        remote: String,
    },
    /// Copy remote files/dirs into a local destination.
    Pull {
        #[command(flatten)]
        args: SyncArgs,
        /// Remote source (same forms as push's remote).
        #[arg(required = true)]
        remote: String,
        /// Local destination directory (created if missing).
        #[arg(required = true)]
        dest: PathBuf,
    },
    /// Run the transfer server.
    Serve {
        #[command(flatten)]
        args: ServeArgs,
    },
}

#[derive(clap::Args)]
pub struct SyncArgs {
    /// Parallel transfer links (1 = single connection).
    #[arg(long, default_value_t = 4)]
    pub links: u16,
    /// Files transferred in parallel (auto from cores when omitted).
    #[arg(long)]
    pub workers: Option<u32>,
    /// Literal chunk size in bytes (auto from bandwidth when 0/omitted).
    #[arg(long, default_value_t = 0)]
    pub chunk_bytes: u64,
    /// Delta block size in bytes (auto when 0/omitted).
    #[arg(long, default_value_t = 0)]
    pub block_bytes: u64,
    /// Compression: `auto`, `none` or `zstd:<level>`.
    #[arg(long, default_value = "auto")]
    pub compression: String,
    /// Send whole files, skipping the delta exchange entirely.
    #[arg(long)]
    pub no_delta: bool,
    /// fsync the staged part file before the atomic rename (sacrifices
    /// throughput for crash durability; off by default, like rsync).
    #[arg(long)]
    pub fsync: bool,
    /// Delete extraneous files on the receiving side (rsync-like scope).
    #[arg(long)]
    pub delete: bool,
    /// Include globs (evaluated first; `--include '*.rs'`).
    #[arg(long)]
    pub include: Vec<String>,
    /// Exclude globs (evaluated second; `--exclude '**/*.cache'`).
    #[arg(long)]
    pub exclude: Vec<String>,
    /// Follow symlinks when scanning sources.
    #[arg(long)]
    pub follow: bool,
    /// IO engine: `auto` (io_uring on Linux when useful), `uring`, `blocking`.
    #[arg(long, default_value = "auto")]
    pub io: IoChoice,
    /// Skip progress bars; print errors only.
    #[arg(long, short = 'q')]
    pub quiet: bool,
    /// SSH port (used by rsync-style remotes and ssh:// URLs without a port).
    #[arg(long, default_value_t = 22)]
    pub ssh_port: u16,
    /// Explicit SSH private key file.
    #[arg(long)]
    pub ssh_key: Option<PathBuf>,
    /// SSH password (otherwise: keys → agent → prompt).
    #[arg(long)]
    pub password: Option<String>,
    /// Accept any host key without prompting (automation mode).
    #[arg(long)]
    pub accept_host_key: bool,
    /// Serve binary to STAGE on an SSH remote that has no rtransfer of its
    /// own (defaults to this executable — so the staged copy always matches
    /// the local platform). Point this at a cross-compiled build (e.g.
    /// `--serve-binary target/aarch64-unknown-linux-gnu/release/rtransfer`)
    /// when the remote runs a different architecture.
    #[arg(long)]
    pub serve_binary: Option<PathBuf>,
    /// Remote directory under which the staged serve gets its private
    /// throwaway subdirectory (created if missing; removed again after the
    /// session). Use it when the remote /tmp is noexec or off-limits.
    #[arg(long, default_value = "/tmp")]
    pub stage_dir: String,
}

#[derive(clap::Args)]
pub struct ServeArgs {
    /// Serve root all relative targets resolve against.
    #[arg(long, default_value = ".")]
    pub root: PathBuf,
    /// Listen on TCP for raw-socket transfers (limit the bind address for
    /// LAN use; default `127.0.0.1` when --port requests a listener).
    #[arg(long)]
    pub bind: Option<String>,
    /// TCP port for raw-socket transfers.
    #[arg(long, default_value_t = 7822)]
    pub port: u16,
    /// Serve a single session over stdin/stdout (SSH bootstrap; implies
    /// an ephemeral loopback listener announced to the client for data).
    #[arg(long)]
    pub stdio: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum IoChoice {
    Auto,
    Uring,
    Blocking,
}

/// Parse `--compression` (`auto` | `none` | `zstd:<1..22>`).
/// Outer `None` = auto decision; `Some(None)` = disabled; `Some(Some(l))` =
/// forced zstd level.
pub fn parse_compression(s: &str) -> anyhow::Result<Option<Option<i8>>> {
    Ok(match s {
        "auto" => None,
        "none" => Some(None),
        _ => match s.strip_prefix("zstd:") {
            Some(level) => {
                let l: i32 = level
                    .parse()
                    .map_err(|_| anyhow::anyhow!("bad zstd level {level:?}"))?;
                if !(1..=22).contains(&l) {
                    anyhow::bail!("zstd level must be 1..=22, got {l}");
                }
                Some(Some(l as i8))
            }
            None => anyhow::bail!("--compression must be auto, none or zstd:<level>"),
        },
    })
}

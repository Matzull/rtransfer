//! CLI entry point: parses args, wires tracing/signals, prints the summary.

use std::net::IpAddr;
use std::path::PathBuf;

use clap::Parser;
use tokio_util::sync::CancellationToken;

use rtransfer::session::Summary;
use rtransfer::{cli, client, serve};
use rtransfer::protocol::CopyMode;

#[tokio::main]
async fn main() {
    #[cfg(all(feature = "profile", target_os = "linux"))]
    let prof = profiling::start();
    let cli = cli::Cli::parse();
    let code = match cli.cmd {
        cli::Cmd::Serve { args } => run_serve(args).await,
        cli::Cmd::Push { args, src, remote } => {
            run_dir(&args, CopyMode::Push, src, remote, None).await
        }
        cli::Cmd::Pull { args, remote, dest } => {
            run_dir(&args, CopyMode::Pull, Vec::new(), remote, Some(dest)).await
        }
    };
    #[cfg(all(feature = "profile", target_os = "linux"))]
    profiling::finish(prof);
    std::process::exit(code);
}

/// Opt-in CPU sampling (`--features profile`): with `RTRANSFER_PROFILE=<prefix>`
/// set, writes `<prefix>.svg` (flamegraph), `<prefix>.txt` (top functions by
/// self and inclusive samples) and `<prefix>.folded` (collapsed stacks, root
/// first, for grepping callers) when the process exits normally.
#[cfg(all(feature = "profile", target_os = "linux"))]
mod profiling {
    use std::collections::{HashMap, HashSet};
    use std::io::Write;

    pub struct Session {
        guard: pprof::ProfilerGuard<'static>,
        prefix: String,
    }

    pub fn start() -> Option<Session> {
        let prefix = std::env::var("RTRANSFER_PROFILE").ok()?;
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(499)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .map_err(|e| eprintln!("rtransfer: profiler failed to start: {e}"))
            .ok()?;
        Some(Session { guard, prefix })
    }

    pub fn finish(session: Option<Session>) {
        let Some(Session { guard, prefix }) = session else { return };
        let report = match guard.report().build() {
            Ok(r) => r,
            Err(e) => return eprintln!("rtransfer: profiler report failed: {e}"),
        };
        if let Ok(f) = std::fs::File::create(format!("{prefix}.svg")) {
            if let Err(e) = report.flamegraph(f) {
                eprintln!("rtransfer: flamegraph failed: {e}");
            }
        }
        // Self = leaf frame; inclusive = anywhere on the stack (once per stack).
        let (mut own, mut incl, mut total) = (HashMap::<String, isize>::new(), HashMap::<String, isize>::new(), 0isize);
        let mut folded = std::fs::File::create(format!("{prefix}.folded")).ok();
        for (frames, count) in &report.data {
            total += count;
            let names: Vec<String> = frames.frames.iter().flatten().map(|s| s.name()).collect();
            if let Some(f) = folded.as_mut() {
                let stack: Vec<&str> = names.iter().rev().map(String::as_str).collect();
                let _ = writeln!(f, "{};{} {count}", frames.thread_name, stack.join(";"));
            }
            if let Some(leaf) = names.first() {
                *own.entry(leaf.clone()).or_default() += count;
            }
            for n in names.into_iter().collect::<HashSet<_>>() {
                *incl.entry(n).or_default() += count;
            }
        }
        let Ok(mut out) = std::fs::File::create(format!("{prefix}.txt")) else { return };
        let _ = writeln!(out, "total samples: {total} (499 Hz)");
        for (title, map) in [("self", &own), ("inclusive", &incl)] {
            let mut v: Vec<_> = map.iter().collect();
            v.sort_by(|a, b| b.1.cmp(a.1));
            let _ = writeln!(out, "\n== top by {title} ==");
            for (name, c) in v.into_iter().take(45) {
                let _ = writeln!(out, "{:6.2}%  {:7}  {name}", 100.0 * *c as f64 / total.max(1) as f64, c);
            }
        }
    }
}

fn init_tracing(ansi: bool) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(ansi)
        .with_env_filter(filter)
        .try_init();
}

fn install_ctrl_c(cancel: CancellationToken) {
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("interrupted; aborting session");
        cancel.cancel();
    });
}

// ─── serve ────────────────────────────────────────────────────────────────

async fn run_serve(args: cli::ServeArgs) -> i32 {
    // Stdio bootstrap keeps stdout for the protocol; logs go to stderr, which
    // the SSH client forwards as `remote:` lines (plain text, no colors).
    init_tracing(!args.stdio);
    let cancel = CancellationToken::new();
    install_ctrl_c(cancel.clone());

    let bind = match (&args.bind, args.stdio) {
        (Some(b), _) => Some(b.parse::<IpAddr>().unwrap_or_else(|_| {
            eprintln!("rtransfer: error: bad --bind address (use an IP like 0.0.0.0)");
            std::process::exit(2);
        })),
        (None, false) => Some(IpAddr::from([127, 0, 0, 1])),
        (None, true) => None,
    };
    let opts = serve::ServeOptions {
        root: client::expand_tilde(&args.root),
        bind,
        port: if args.stdio { None } else { Some(args.port) },
        stdio: args.stdio,
    };
    match serve::serve(&opts, cancel).await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("rtransfer: error: {e:#}");
            1
        }
    }
}

// ─── client ───────────────────────────────────────────────────────────────

async fn run_dir(
    args: &cli::SyncArgs,
    mode: CopyMode,
    srcs: Vec<String>,
    remote: String,
    dest: Option<PathBuf>,
) -> i32 {
    init_tracing(true);
    let cancel = CancellationToken::new();
    install_ctrl_c(cancel.clone());

    let out = client::check_and_run(args, mode, &srcs, &remote, dest, &cancel).await;
    match out {
        Ok(summary) => {
            print_summary(&summary);
            if summary.files_failed > 0 {
                3
            } else {
                0
            }
        }
        Err(e) => {
            eprintln!("rtransfer: error: {e:#}");
            1
        }
    }
}

fn print_summary(s: &Summary) {
    let mb = |b: u64| b as f64 / (1024.0 * 1024.0);
    let mut parts = vec![format!("{} ok", s.files_done)];
    if s.files_skipped > 0 {
        parts.push(format!("{} skipped", s.files_skipped));
    }
    if s.files_failed > 0 {
        parts.push(format!("{} FAILED", s.files_failed));
    }
    parts.push(format!("{:.1} MiB literal", mb(s.bytes_literal)));
    if s.bytes_wire > 0 && s.bytes_wire < s.bytes_literal {
        parts.push(format!("{:.1} MiB on wire", mb(s.bytes_wire)));
    }
    if !s.deleted.is_empty() {
        parts.push(format!("{} deleted", s.deleted.len()));
    }
    println!("{} — {:.1}s", parts.join(", "), s.elapsed_secs);
}

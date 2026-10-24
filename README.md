# rtransfer

Fast, multithreaded file transfer in the spirit of `rsync` and `rclone`:
delta sync, parallel links, an SSH backend that needs nothing installed on
the remote, a raw TCP backend for trusted networks, and end-to-end BLAKE3
integrity checks.

- **Delta sync**: rsync-style rolling checksums, so only changed blocks of an
  existing file are sent (block size ≈ √file size, tuned per file).
- **Quick skip**: unchanged files (same size and mtime) are skipped without
  being read; same-size files are verified by hash.
- **Parallel**: many files in flight at once, spread over several TCP links,
  with chunks compressed by zstd on all cores.
- **Adaptive compression**: while transferring, the sender measures
  throughput with zstd on and off and keeps whichever is faster. It skips
  files that don't compress.
- **Zero-install SSH**: if the remote has no `rtransfer`, the binary is staged
  into a temporary directory for the session and removed afterwards.
- **Safe writes**: data goes to a part file and is checked against the
  sender's BLAKE3 hash before an atomic rename into place.
- **io_uring** file I/O on Linux, chosen automatically when available.

## Building

You need Rust **1.85 or newer** (edition 2024); install it with
[rustup](https://rustup.rs).

```sh
git clone https://github.com/Matzull/rtransfer.git
cd rtransfer
cargo build --release          # or: make
# → target/release/rtransfer
```

The `Makefile` wraps the common tasks:

| Command            | What it does                                                   |
|--------------------|----------------------------------------------------------------|
| `make`             | Release build for this machine                                 |
| `make aarch64`     | Release build for 64-bit ARM (e.g. Raspberry Pi) via [`cross`] |
| `make all`         | Both of the above                                              |
| `make test`        | Run the test suite                                             |
| `make check`       | Type-check everything, tests included                          |
| `make bench-serde` | Wire-codec benchmark (`benches/serialization/`)                |
| `make clean`       | Remove build artifacts                                         |

[`cross`]: https://github.com/cross-rs/cross

`make aarch64` needs `cross` (`cargo install cross`) and Docker or Podman.

Optional cargo features:

- `aws-lc-rs`: use AWS-LC instead of `ring` as the SSH crypto backend
  (`cargo build --release --no-default-features --features aws-lc-rs`).
- `profile` (Linux): built-in CPU profiler. Run with
  `RTRANSFER_PROFILE=<prefix>` to get `<prefix>.svg` (flamegraph) and
  `<prefix>.txt` when the process exits.

## Usage

```text
rtransfer push [OPTIONS] <SRC>... <REMOTE>   # local → remote
rtransfer pull [OPTIONS] <REMOTE> <DEST>     # remote → local
rtransfer serve [OPTIONS]                    # run a server for tcp:// transfers
```

### Remote forms

| Form                                | Example                                   |
|-------------------------------------|-------------------------------------------|
| `ssh://[user@]host[:port]/abs/path` | `ssh://user@server/srv/backup`           |
| rsync style, absolute path          | `user@server:/srv/backup`                 |
| rsync style, relative to `$HOME`    | `user@server:backup`                      |
| `tcp://host:port/dir`               | `tcp://192.0.2.10:7822/backup`            |

### Examples

```sh
# Copy a directory to a remote machine over SSH
rtransfer push ./photos user@server:/data/photos

# Mirror it, deleting files on the remote that no longer exist locally
rtransfer push --delete ./photos user@server:/data/photos

# Only Rust sources, skipping build output
rtransfer push --include '*.rs' --exclude '**/target/**' ./project user@server:src

# Fetch a remote directory
rtransfer pull user@server:/var/log/app ./logs

# Remote with a different CPU architecture: stage a cross-compiled build
make aarch64
rtransfer push ./data ssh://user@arm-server/srv/data \
    --serve-binary target/aarch64-unknown-linux-gnu/release/rtransfer
```

### SSH authentication

rtransfer has its own SSH client and does not run the system `ssh` binary.
It authenticates in this order:

1. the key given with `--ssh-key`;
2. your default keys;
3. the SSH agent;
4. a password prompt.

`--password` and `--accept-host-key` are there for automation. Use them
with care.

If `rtransfer` is not on the remote's `PATH`, a copy of the local binary, or
of `--serve-binary`, is uploaded to `<stage-dir>/rtransfer-stage-XXXXXX/` and
deleted when the session ends. `--stage-dir` defaults to `/tmp`; change it if
the remote's `/tmp` is mounted `noexec`.

### Raw TCP mode

For the highest throughput on a **trusted** network, run a server on the
receiving machine and use a `tcp://` remote:

```sh
# on the server
rtransfer serve --root /srv/incoming --bind 0.0.0.0 --port 7822

# on the client
rtransfer push ./data tcp://server:7822/data
```

> **Warning:** TCP mode is neither encrypted nor authenticated. Anyone who
> can reach the port can read and write under `--root`. Only use it on
> networks you trust; without `--bind` the server listens on `127.0.0.1`
> only. Prefer SSH everywhere else.

### Common options

| Option                              | Default | Meaning                                                  |
|-------------------------------------|---------|----------------------------------------------------------|
| `--links <n>`                       | 4       | Parallel TCP links per session                           |
| `--workers <n>`                     | auto    | Files in flight                                          |
| `--compression auto\|none\|zstd:<l>` | auto    | auto: zstd 3, on only while it is faster           |
| `--no-delta`                        | off     | Always send whole files                                  |
| `--delete`                          | off     | Delete extraneous files on the receiver                  |
| `--include` / `--exclude <glob>`    |         | Filters; can be repeated (includes are evaluated first)  |
| `--follow`                          | off     | Follow symlinks in the sources                           |
| `--fsync`                           | off     | fsync each file before the rename (durability over speed)|
| `--io auto\|uring\|blocking`         | auto    | File I/O engine                                          |
| `--chunk-bytes` / `--block-bytes`   | auto    | Override the chunk / delta block size                    |
| `--ssh-port <port>`                 | 22      | SSH port for remotes without one                         |
| `-q, --quiet`                       | off     | No progress bars, errors only                            |

Run `rtransfer <command> --help` for the full list. Set `RUST_LOG=debug`
for detailed logs; over SSH the remote side's log lines are shown with a
`remote:` prefix.

## Benchmarks

rtransfer compared with `rsync` (daemon mode) and with `tar` streamed over a
raw TCP socket (`socat`), with and without `zstd -1`. All tools use plain TCP
on `127.0.0.1`, without SSH, so the numbers compare transfer engines rather
than ciphers. The data is synthetic: half random bytes (incompressible) and
half text (compresses about 3x). Every time is the median of 3 runs and
includes a final `sync`. Every result was checked against the source with
`diff -r`.

Machine: AMD Ryzen 7 3700X (8 cores / 16 threads), NVMe SSD, Linux 7.2.
Sender and receiver share the same CPU.

**Many small files**: 10 000 files of 1–128 KiB (255 MiB), full-speed loopback.

| Tool                  | Initial copy        | Re-sync, no changes |
|-----------------------|--------------------:|--------------------:|
| rtransfer             | 1.77 s (145 MiB/s)  | 0.14 s              |
| rsync (daemon)        | 1.65 s (155 MiB/s)  | 0.14 s              |
| tar \| socat          | 1.57 s (163 MiB/s)  | —                   |
| tar \| zstd -1 \| socat | 1.63 s (156 MiB/s) | —                   |

**A few big files**: 4 × 512 MiB (2 GiB), full-speed loopback.

| Tool                  | Initial copy          | Re-sync, no changes |
|-----------------------|----------------------:|--------------------:|
| rtransfer             | 2.08 s (985 MiB/s)    | 0.01 s              |
| rsync (daemon)        | 1.80 s (1,136 MiB/s)  | 0.06 s              |
| tar \| socat          | 3.53 s (580 MiB/s)    | —                   |
| tar \| zstd -1 \| socat | 4.16 s (493 MiB/s)   | —                   |

**Slow link**: 2 000 small files + 2 × 64 MiB (178 MiB) through a
50 Mbit/s link with 40 ms RTT, emulated by
[`benches/transfer/shaper.py`](benches/transfer/shaper.py). The
bandwidth limit is shared by all connections. "After edits" means
1 MiB was overwritten in each big file and 1% of the small files grew.

| Tool                  | Initial copy (on the wire) | Re-sync, no changes | Re-sync after edits |
|-----------------------|---------------------------:|--------------------:|--------------------:|
| rtransfer             | 22.1 s (127.8 MiB)         | 0.30 s (0.1 MiB)    | 1.24 s (2.5 MiB)    |
| rsync (daemon)        | 30.8 s (179.1 MiB)         | 0.38 s (0.0 MiB)    | 0.96 s (2.2 MiB)    |
| rsync -z (daemon)     | 21.0 s (120.9 MiB)         | —                   | 0.95 s (2.2 MiB)    |
| tar \| socat          | 30.3 s (180.4 MiB)         | —                   | —                   |
| tar \| zstd -1 \| socat | 21.8 s (128.5 MiB)       | —                   | —                   |

What the numbers show:

- **Compression adapts by itself.** The sender measures throughput with
  compression on and off while it transfers, and keeps whichever is faster.
  - On the slow link it compresses, which makes it 28% faster than plain
    rsync and tar, and close to `rsync -z`.
  - At full speed it doesn't, and stays within 7–15% of rsync.
  - With the same defaults, `rsync -z` would slow down a fast link instead.
- **No fixed cost per session.** rtransfer doesn't benchmark the CPU or probe
  bandwidth before transferring, so an unchanged re-sync costs no more than
  rsync's, and sends 0.1 MiB on the slow link.
- **Still behind rsync:** the initial copy at full speed, by 7–15%, and
  re-syncing small edits on the slow link, 1.24 s against 0.96 s. Batching many
  small files per frame is the next step.

Reproduce with:

```sh
cargo build --release
benches/transfer/loopback.sh            # all scenarios; or: small | big | slow
REPS=5 MBIT=20 RTT_MS=80 benches/transfer/loopback.sh slow
```

The test data is generated into `benches/transfer/data/` (about 2.5 GiB).
Results, raw TSV plus a Markdown summary, go to `benches/transfer/results/`.
`HOST=user@server RDIR=/srv/scratch benches/transfer/run.sh <dir>` runs a
similar comparison over SSH against a real remote host.

## Contributing

Contributions are welcome: bug reports, ideas and pull requests.

1. **Open an issue first** for anything larger than a small fix, so the
   approach can be agreed before you write the code.
2. Fork the repository and create a branch from `main`.
3. Keep the build clean:
   ```sh
   cargo check --tests     # must produce zero warnings
   cargo test              # unit + integration tests
   ```
   Fix warnings rather than silencing them; do not add
   `#[allow(dead_code)]`.
4. Add a test for any bug you fix or feature you add. Unit tests live next
   to the code; end-to-end transfers are in `tests/integration.rs`.
5. If you change the wire protocol (`src/protocol.rs`), rebuild every binary
   you use as `--serve-binary`. Old and new builds cannot talk to each other.
6. Match the style of the surrounding code and run `cargo fmt`.
7. Open a pull request that describes what changed and why. For performance
   work, include before/after numbers. `benches/transfer/run.sh` compares
   rtransfer with rsync, tar over SSH and tar over netcat on a real remote.

By submitting a contribution you agree that it is licensed under the same
license as the project (GPL-3.0-or-later).

## License

rtransfer is free software, licensed under the
[GNU General Public License v3.0 or later](LICENSE).

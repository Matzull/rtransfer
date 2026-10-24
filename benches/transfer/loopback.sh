#!/usr/bin/env bash
# Reproducible loopback benchmark on synthetic data: rtransfer vs rsync
# (daemon) vs tar over raw TCP (socat), with and without zstd.
#
# usage: benches/transfer/loopback.sh [scenario...]     (default: small big slow)
#   small  10 000 files of 1-128 KiB, full-speed loopback
#   big    4 x 512 MiB, full-speed loopback
#   slow   2 000 small + 2 x 64 MiB through a shaped link ($MBIT, $RTT_MS),
#          plus an unchanged re-sync and a delta run after edits (gen.py mutate)
#
# env: REPS (3, median reported)  MBIT (50)  RTT_MS (40)
#      WORK (benches/transfer/data/loopback; synthetic trees are cached there)
#
# Everything runs on 127.0.0.1 with raw TCP (no SSH, no encryption) so the
# numbers compare transfer engines, not ciphers. Each timing includes a final
# `sync`. Results: benches/transfer/results/loopback-<date>/{raw.tsv,summary.md}
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
BIN=$REPO/target/release/rtransfer
WORK=${WORK:-$HERE/data/loopback}
REPS=${REPS:-3}
MBIT=${MBIT:-50}
RTT_MS=${RTT_MS:-40}
OUT=$HERE/results/loopback-$(date +%Y%m%d-%H%M%S)
SCENARIOS=("$@")
[ ${#SCENARIOS[@]} -eq 0 ] && SCENARIOS=(small big slow)

# Ports: servers, and shaper fronts for the slow scenario.
P_RT=7951 P_RSYNC=7952 P_TAR=7953 S_RT=7961 S_RSYNC=7962 S_TAR=7963

[ -x "$BIN" ] || { echo "build first: cargo build --release" >&2; exit 1; }
for t in rsync socat zstd python3; do command -v $t >/dev/null || { echo "missing: $t" >&2; exit 1; }; done
mkdir -p "$OUT" "$WORK"
RAW=$OUT/raw.tsv
printf 'scenario\ttool\tkind\trep\tseconds\twire_bytes\tok\n' > "$RAW"

PIDS=()
cleanup() { for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

now() { date +%s.%N; }
wait_listen() { for _ in $(seq 100); do ss -Hltn "sport = :$1" | grep -q . && return; sleep 0.05; done; echo "port $1 not listening" >&2; exit 1; }

# ── servers (shared by every scenario; all write under $WORK/dst) ─────────
DST=$WORK/dst
mkdir -p "$DST"
"$BIN" serve --root "$DST" --port $P_RT > "$OUT/rtransfer-serve.log" 2>&1 & PIDS+=($!)
cat > "$WORK/rsyncd.conf" <<EOF
use chroot = no
pid file = $WORK/rsyncd.pid
log file = $OUT/rsyncd.log
[bench]
    path = $DST
    read only = false
EOF
rm -f "$WORK/rsyncd.pid"
rsync --daemon --no-detach --address=127.0.0.1 --port=$P_RSYNC --config="$WORK/rsyncd.conf" & PIDS+=($!)
wait_listen $P_RT; wait_listen $P_RSYNC

# ── runners: $1 = source tree, $2 = destination name, $3 = port ────────────
run_rtransfer() { "$BIN" push -q "$1" "tcp://127.0.0.1:$3/$2"; }
run_rsync()     { rsync -a "$1/" "rsync://127.0.0.1:$3/bench/$2/"; }
run_rsync_z()   { rsync -az "$1/" "rsync://127.0.0.1:$3/bench/$2/"; }
tar_socat() {  # $4 = encoder ("" or "zstd"); listener on P_TAR, sender dials $3
    mkdir -p "$DST/$2"
    if [ -n "$4" ]; then
        (socat -u TCP-LISTEN:$P_TAR,reuseaddr,bind=127.0.0.1 STDOUT | zstd -d -q | tar -xf - -C "$DST/$2") &
    else
        (socat -u TCP-LISTEN:$P_TAR,reuseaddr,bind=127.0.0.1 STDOUT | tar -xf - -C "$DST/$2") &
    fi
    local rx=$!
    wait_listen $P_TAR
    if [ -n "$4" ]; then
        tar -C "$1" -cf - . | zstd -1 -T0 -q | socat -u STDIN "TCP:127.0.0.1:$3"
    else
        tar -C "$1" -cf - . | socat -u STDIN "TCP:127.0.0.1:$3"
    fi
    wait $rx
}
run_tar_socat()      { tar_socat "$1" "$2" "$3" ""; }
run_tar_zstd_socat() { tar_socat "$1" "$2" "$3" zstd; }

port_of() {  # tool → (server port, shaper port)
    case $1 in
        rtransfer) echo "$P_RT $S_RT" ;;
        rsync|rsync-z) echo "$P_RSYNC $S_RSYNC" ;;
        *) echo "$P_TAR $S_TAR" ;;
    esac
}

wire() { if [ -f "$1" ]; then awk '{ print $1 + $2 }' "$1"; else echo 0; fi; }

# One measured run: scenario tool kind rep source dest_name port stats_file
measure() {
    local scen=$1 tool=$2 kind=$3 rep=$4 src=$5 name=$6 port=$7 stats=$8
    if [ "$kind" = fresh ]; then rm -rf "${DST:?}/$name"; fi
    sleep 0.3  # let the shaper's stats file settle before sampling
    local w0 t0 t1 w1 ok=ok
    w0=$(wire "$stats"); t0=$(now)
    "run_${tool//-/_}" "$src" "$name" "$port" >> "$OUT/$scen-$tool.log" 2>&1 || ok=FAILED
    sync; t1=$(now)
    sleep 0.3; w1=$(wire "$stats")
    if [ "$ok" = ok ] && ! diff -rq "$src" "$DST/$name" > /dev/null; then ok=MISMATCH; fi
    printf '%s\t%s\t%s\t%s\t%.3f\t%s\t%s\n' "$scen" "$tool" "$kind" "$rep" \
        "$(awk -v a="$t0" -v b="$t1" 'BEGIN { print b - a }')" $((w1 - w0)) "$ok" >> "$RAW"
    echo "  $tool $kind #$rep: $(tail -1 "$RAW" | cut -f5)s $ok"
}

for scen in "${SCENARIOS[@]}"; do
    case $scen in small) data=small ;; big) data=big ;; slow) data=mixed ;; *) echo "unknown scenario $scen" >&2; exit 1 ;; esac
    src=$WORK/$data
    if [ ! -f "$src.done" ]; then
        echo "generating $data …"
        rm -rf "$src"; python3 "$HERE/gen.py" "$data" "$src"; touch "$src.done"
    fi
    tools=(rtransfer rsync tar-socat tar-zstd-socat)
    stats=""
    if [ "$scen" = slow ]; then
        tools=(rtransfer rsync rsync-z tar-socat tar-zstd-socat)
        if [ ! -f "$src-mut.done" ]; then
            rm -rf "$src-mut"; cp -a "$src" "$src-mut"; python3 "$HERE/gen.py" mutate "$src-mut"; touch "$src-mut.done"
        fi
        for pair in "$S_RT $P_RT" "$S_RSYNC $P_RSYNC" "$S_TAR $P_TAR"; do
            set -- $pair
            python3 "$HERE/shaper.py" "$1" "$2" "$MBIT" "$RTT_MS" "$OUT/shaper-$1.stats" & PIDS+=($!)
            wait_listen "$1"
        done
    fi
    tar -C "$src" -cf - . > /dev/null  # warm the page cache
    echo "== $scen: $(find "$src" -type f | wc -l) files, $(( $(du -sb "$src" | cut -f1) >> 20 )) MiB"
    for tool in "${tools[@]}"; do
        read -r sport shport <<< "$(port_of "$tool")"
        port=$sport
        if [ "$scen" = slow ]; then port=$shport; stats=$OUT/shaper-$shport.stats; fi
        for rep in $(seq "$REPS"); do
            measure "$scen" "$tool" fresh "$rep" "$src" "$tool" "$port" "$stats"
            case $tool in rtransfer|rsync|rsync-z) ;; *) continue ;; esac
            if [ "$tool" != rsync-z ]; then measure "$scen" "$tool" again "$rep" "$src" "$tool" "$port" "$stats"; fi
            if [ "$scen" = slow ]; then measure "$scen" "$tool" delta "$rep" "$src-mut" "$tool" "$port" "$stats"; fi
        done
        rm -rf "${DST:?}/$tool"
    done
done

# ── summary table ─────────────────────────────────────────────────────────
python3 - "$RAW" "$WORK" "$MBIT" "$RTT_MS" "$REPS" > "$OUT/summary.md" <<'PY'
import csv, os, statistics, subprocess, sys
from collections import defaultdict
raw, work, mbit, rtt, reps = sys.argv[1:6]
rows = list(csv.DictReader(open(raw), delimiter="\t"))
size = lambda d: sum(os.path.getsize(os.path.join(p, f)) for p, _, fs in os.walk(d) for f in fs)
names = {"rtransfer": "rtransfer", "rsync": "rsync (daemon)", "rsync-z": "rsync -z (daemon)",
         "tar-socat": "tar \\| socat", "tar-zstd-socat": "tar \\| zstd -1 \\| socat"}
kinds = {"fresh": "initial copy", "again": "re-sync, no changes", "delta": "re-sync after edits"}
data = {"small": "small", "big": "big", "slow": "mixed"}
cpu = subprocess.run("lscpu | sed -n 's/^Model name: *//p'", shell=True, capture_output=True, text=True).stdout.strip()
print(f"Machine: {cpu}, {os.cpu_count()} threads, Linux {os.uname().release}. "
      f"Median of {reps} runs; times include a final `sync`.\n")
groups = defaultdict(list)
for r in rows:
    groups[(r["scenario"], r["tool"], r["kind"])].append(r)
for scen in dict.fromkeys(r["scenario"] for r in rows):
    total = size(os.path.join(work, data[scen]))
    slow = scen == "slow"
    print(f"### {scen}\n")
    print("| Tool | Run | Time | Throughput |" + (" On the wire |" if slow else ""))
    print("|---|---|---:|---:|" + ("---:|" if slow else ""))
    for (s, tool, kind), rs in groups.items():
        if s != scen:
            continue
        bad = [r["ok"] for r in rs if r["ok"] != "ok"]
        t = statistics.median(float(r["seconds"]) for r in rs)
        tp = f"{total / 2**20 / t:,.0f} MiB/s" if kind == "fresh" else "—"
        line = f"| {names[tool]} | {kinds[kind]} | {t:.2f} s{' ⚠ ' + bad[0] if bad else ''} | {tp} |"
        if slow:
            line += f" {statistics.median(int(r['wire_bytes']) for r in rs) / 2**20:,.1f} MiB |"
        print(line)
    print()
PY
cat "$OUT/summary.md"
echo "results: $OUT"

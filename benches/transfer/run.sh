#!/usr/bin/env bash
# rtransfer vs rsync vs tar (over ssh / raw netcat): push a subset of a local
# directory to a remote host and time each tool end to end.
#
# usage: benches/transfer/run.sh <src_dir> [limit_gib=3] [tools...]
#   tools (default all, in this order):
#     rtransfer rtransfer-again rsync rsync-again tar-ssh tar-nc tar-zstd-nc
#   `*-again` re-pushes onto that tool's previous result (unchanged-tree
#   sync), so it must directly follow it: every fresh run empties $RDIR so
#   only one copy of the subset ever occupies the remote disk.
#
# env: HOST   (required) ssh destination, e.g. user@server
#      RDIR   (required) absolute remote scratch dir, e.g. /srv/rtransfer-bench;
#             it is DELETED before every fresh run and at the end
#      SERVE  serve binary to stage remotely (default: target/release/rtransfer;
#             use target/aarch64-unknown-linux-gnu/release/rtransfer for ARM)
#      PORT (9911)  SUBSET_SEED (42)  KEEP_DATA (0: delete the local subset copy)
#
# The subset is a deterministic random pick of entries of <src_dir>
# (directories are descended until pieces fit under the limit), copied once
# into benches/transfer/data/ so every tool sends the very same single tree
# from a warm page cache.
#
# Remote writes happen only under $RDIR (including rtransfer's stage dir).
# `wall` = until the tool exits; `+sync` = including a remote `sync` (on slow
# remote disks the page cache can hide most of the write time).
set -euo pipefail

SRC=$(realpath "${1:?usage: run.sh <src_dir> [limit_gib] [tools...]}")
LIMIT_GIB=${2:-3}
shift $(( $# >= 2 ? 2 : 1 ))
TOOLS=("$@")
[ ${#TOOLS[@]} -eq 0 ] && TOOLS=(rtransfer rtransfer-again rsync rsync-again tar-ssh tar-nc tar-zstd-nc)

HOST=${HOST:?set HOST=user@server}
RHOST=${HOST#*@}
RDIR=${RDIR:?set RDIR=/absolute/remote/scratch/dir (deleted by this script)}
STAGE=$RDIR/.stage
PORT=${PORT:-9911}
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
BIN=$REPO/target/release/rtransfer
SERVE=${SERVE:-$REPO/target/release/rtransfer}
DATA=$HERE/data
OUT=$HERE/results/$(date +%Y%m%d-%H%M%S)

# RDIR gets `rm -rf`: insist on an absolute path at least two levels deep.
case "$RDIR" in /?*/?*) ;; *) echo "refusing RDIR '$RDIR': use an absolute scratch dir like /srv/rtransfer-bench" >&2; exit 1 ;; esac
[ -x "$BIN" ] && [ -x "$SERVE" ] || { echo "build first: make (or make aarch64 for SERVE)" >&2; exit 1; }
mkdir -p "$OUT"

R() { ssh -o BatchMode=yes "$HOST" "$@"; }
now() { date +%s.%N; }

# ── subset → $DATA ────────────────────────────────────────────────────────
# $DATA and the results live under $HERE, which may itself be inside <src_dir>
# (e.g. the repo as source): never pick them, and descend through ancestors.
python3 - "$SRC" "$LIMIT_GIB" "${SUBSET_SEED:-42}" "$DATA" "$HERE/results" > "$OUT/sources.txt" <<'PY'
import os, random, sys
root, limit, seed = sys.argv[1], float(sys.argv[2]) * 2**30, int(sys.argv[3])
excluded = [os.path.realpath(x) for x in sys.argv[4:]]
def is_excluded(p):
    return any(os.path.realpath(p) == x or os.path.realpath(p).startswith(x + os.sep) for x in excluded)
def holds_excluded(p):
    return any(x.startswith(os.path.realpath(p) + os.sep) for x in excluded)
def size(p):
    if os.path.islink(p) or not os.path.isdir(p):
        return os.lstat(p).st_size
    return sum(os.lstat(os.path.join(d, f)).st_size for d, _, fs in os.walk(p) for f in fs)
rnd, picked, total = random.Random(seed), [], 0
queue = [os.path.join(root, e) for e in sorted(os.listdir(root))]
rnd.shuffle(queue)
while queue and total < limit:
    p = queue.pop()
    if is_excluded(p):
        continue
    s = None if holds_excluded(p) else size(p)
    if s is not None and total + s <= limit:
        picked.append(os.path.relpath(p, root)); total += s
    elif os.path.isdir(p) and not os.path.islink(p):
        kids = [os.path.join(p, e) for e in sorted(os.listdir(p))]
        rnd.shuffle(kids); queue.extend(kids)
print("\n".join(sorted(picked)))
PY
[ -s "$OUT/sources.txt" ] || { echo "empty subset" >&2; exit 1; }
rm -rf "$DATA"; mkdir -p "$DATA"
(cd "$SRC" && tr '\n' '\0' < "$OUT/sources.txt" | xargs -0 cp -a --parents -t "$DATA")
BYTES=$(du -sb "$DATA" | cut -f1)
FILES=$(find "$DATA" -type f | wc -l)
echo "subset of $SRC: $FILES files, $((BYTES >> 20)) MiB → $HOST:$RDIR" | tee "$OUT/summary.txt"
R "mkdir -p '$RDIR'"
tar -C "$DATA" -cf - . > /dev/null   # warm the page cache for every tool

# ── runners: each pushes $DATA into $1 ────────────────────────────────────
run_rtransfer() { "$BIN" push -q "$DATA" "ssh://$HOST$1" --serve-binary "$SERVE" --stage-dir "$STAGE"; }
run_rsync()     { rsync -a "$DATA/" "$HOST:$1/"; }
run_tar_ssh()   { tar -C "$DATA" -cf - . | R "mkdir -p '$1' && tar -xf - -C '$1'"; }
# Raw TCP: OpenBSD nc listens on the remote, bash's /dev/tcp sends (no local
# nc needed). `-d` (never read stdin) makes nc exit on network EOF; without
# it the listener sometimes lingers after all data arrived. It accepts once,
# so wait for it with a plain sleep rather than a probe connection.
nc_push() {  # $1 dst, $2 remote decoder prefix; payload on stdin
    local pid
    # The payload is our stdin: keep these ssh calls from swallowing its head.
    R "pkill -f '^nc -d -l $PORT\$'" < /dev/null || true   # stale listener from an aborted run
    pid=$(R "mkdir -p '$1' && { nohup sh -c \"nc -d -l $PORT | $2 tar -xf - -C '$1'\" >'$1.err' 2>&1 & echo \$!; }" < /dev/null)
    sleep 0.5
    cat > "/dev/tcp/$RHOST/$PORT"
    # Wait by pid: a `pgrep -f 'nc -l …'` loop matches its own command line.
    R "while kill -0 $pid 2>/dev/null; do sleep 0.05; done; cat '$1.err'; rm -f '$1.err'"
}
run_tar_nc()      { tar -C "$DATA" -cf - . | nc_push "$1" ""; }
run_tar_zstd_nc() { tar -C "$DATA" -cf - . | zstd -1 -T0 -q | nc_push "$1" "zstd -d |"; }

printf '%-16s %8s %8s %9s %8s %8s\n' tool wall +sync "MiB/s" files MiB | tee -a "$OUT/summary.txt"
for tool in "${TOOLS[@]}"; do
    base=${tool%-again}; dst=$RDIR/$base
    [ "$tool" = "$base" ] && R "rm -rf '$RDIR' && mkdir -p '$RDIR' && sync"
    t0=$(now)
    if ! "run_${base//-/_}" "$dst" > "$OUT/$tool.log" 2>&1; then
        echo "$tool FAILED (see $OUT/$tool.log)" | tee -a "$OUT/summary.txt"; continue
    fi
    t1=$(now); R sync; t2=$(now)
    read -r rf rb <<< "$(R "find '$dst' -type f | wc -l; du -sb '$dst' | cut -f1" | tr '\n' ' ')"
    awk -v t="$tool" -v a="$t0" -v b="$t1" -v c="$t2" -v n="$BYTES" -v f="$rf" -v m="$((rb >> 20))" \
        -v want="$FILES" 'BEGIN { printf "%-16s %7.2fs %7.2fs %9.1f %8s %8s%s\n", t, b - a, c - a,
            n / 1048576 / (c - a), f, m, f == want ? "" : "  INCOMPLETE" }' \
        | tee -a "$OUT/summary.txt"
done
R "rm -rf '$RDIR' '$STAGE'"
[ "${KEEP_DATA:-0}" = 1 ] || rm -rf "$DATA"
echo "results: $OUT"

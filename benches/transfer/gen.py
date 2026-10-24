#!/usr/bin/env python3
"""Deterministic synthetic trees for the transfer benchmarks.

usage: gen.py small|big|mixed <dir>     build a fresh tree
       gen.py mutate <dir>              edit it in place (for delta runs)

Half of the content is random (incompressible, like media or archives) and
half text-like (compresses ~3x with zstd, like source code or logs), so
compression neither dominates nor disappears.

  small  10 000 files of 1-128 KiB (log-uniform) in 100 directories
  big    4 files x 512 MiB
  mixed  2 000 small files + 2 x 64 MiB
  mutate overwrite 1 MiB in the middle of every file >= 1 MiB and append
         1 KiB to 1% of the small files
"""
import math, os, random, sys

KIB, MIB = 1024, 1024 * 1024


def corpus(rnd, size):
    """Text-like bytes: random words from a fixed vocabulary, with lines."""
    vocab = ["".join(rnd.choices("abcdefghijklmnopqrstuvwxyz", k=rnd.randint(2, 10))) for _ in range(4096)]
    out, n = [], 0
    while n < size:
        line = " ".join(rnd.choices(vocab, k=rnd.randint(4, 16))) + "\n"
        out.append(line)
        n += len(line)
    return "".join(out).encode()[:size]


class Source:
    def __init__(self, seed):
        self.rnd = random.Random(seed)
        self.text = corpus(self.rnd, 64 * MIB)

    def bytes(self, n, text):
        if not text:
            return self.rnd.randbytes(n)
        # Slices at random offsets of a 64 MiB corpus: far apart enough that
        # zstd's window cannot exploit repetition between files.
        out = bytearray()
        while len(out) < n:
            take = min(n - len(out), MIB)
            off = self.rnd.randrange(len(self.text) - take)
            out += self.text[off:off + take]
        return bytes(out)


def write(path, src, size, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        left = size
        while left:
            n = min(left, 8 * MIB)
            f.write(src.bytes(n, text))
            left -= n


def small_files(root, src, count):
    lo, hi = math.log(KIB), math.log(128 * KIB)
    for i in range(count):
        size = int(math.exp(src.rnd.uniform(lo, hi)))
        write(os.path.join(root, f"d{i % 100:03}", f"f{i:05}.dat"), src, size, i % 2 == 0)


def main():
    mode, root = sys.argv[1], sys.argv[2]
    src = Source(seed=1)
    if mode == "small":
        small_files(root, src, 10_000)
    elif mode == "big":
        for i in range(4):
            write(os.path.join(root, f"big{i}.bin"), src, 512 * MIB, i % 2 == 1)
    elif mode == "mixed":
        small_files(root, src, 2_000)
        for i in range(2):
            write(os.path.join(root, "big", f"big{i}.bin"), src, 64 * MIB, i % 2 == 1)
    elif mode == "mutate":
        rnd = random.Random(2)
        files = sorted(os.path.join(d, f) for d, _, fs in os.walk(root) for f in fs)
        small = [p for p in files if os.path.getsize(p) < MIB]
        for p in files:
            if os.path.getsize(p) >= MIB:
                with open(p, "r+b") as f:
                    f.seek(os.path.getsize(p) // 2)
                    f.write(rnd.randbytes(MIB))
        for p in rnd.sample(small, max(1, len(small) // 100)):
            with open(p, "ab") as f:
                f.write(rnd.randbytes(KIB))
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()

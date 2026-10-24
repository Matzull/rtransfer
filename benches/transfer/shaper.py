#!/usr/bin/env python3
"""Userspace link shaper: a TCP proxy that emulates a slow WAN link.

usage: shaper.py <listen_port> <target_port> <mbit> <rtt_ms> <stats_file>

Every connection to 127.0.0.1:<listen_port> is forwarded to <target_port>.
All connections share one bandwidth budget per direction (so 4 parallel
links do not get 4x the bandwidth), and every segment is delayed by half the
RTT each way. Bounded per-connection queues keep TCP backpressure intact.
Cumulative forwarded bytes are written to <stats_file> as "up down".

No root needed (unlike `tc netem`).
"""
import asyncio, sys, time

CHUNK = 64 * 1024


class Bucket:
    """Token bucket shared by every connection in one direction."""

    def __init__(self, rate):
        self.rate, self.tokens, self.last = rate, 0.0, time.monotonic()
        self.lock = asyncio.Lock()
        self.burst = max(rate * 0.005, 2 * CHUNK)  # ~5 ms of traffic

    async def take(self, n):
        async with self.lock:
            while True:
                now = time.monotonic()
                self.tokens = min(self.burst, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
                await asyncio.sleep((n - self.tokens) / self.rate)


async def pump(reader, writer, bucket, delay, counter, idx):
    queue = asyncio.Queue(maxsize=64)

    async def deliver():
        while (item := await queue.get()) is not None:
            due, data = item
            if (wait := due - time.monotonic()) > 0:
                await asyncio.sleep(wait)
            await bucket.take(len(data))
            writer.write(data)
            await writer.drain()
            counter[idx] += len(data)
        if writer.can_write_eof():
            writer.write_eof()  # half-close: the other direction may still be busy

    task = asyncio.create_task(deliver())
    try:
        while data := await reader.read(CHUNK):
            await queue.put((time.monotonic() + delay, data))
    except ConnectionError:
        pass
    await queue.put(None)
    try:
        await task
    except ConnectionError:
        pass


async def main():
    lport, tport, mbit, rtt_ms, stats = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4]), sys.argv[5]
    rate, delay = mbit * 1e6 / 8, rtt_ms / 2000
    up, down = Bucket(rate), Bucket(rate)
    counter = [0, 0]

    async def handle(cr, cw):
        try:
            tr, tw = await asyncio.open_connection("127.0.0.1", tport)
        except OSError:
            cw.close()
            return
        await asyncio.gather(pump(cr, tw, up, delay, counter, 0), pump(tr, cw, down, delay, counter, 1),
                             return_exceptions=True)
        cw.close()
        tw.close()

    async def report():
        while True:
            with open(stats, "w") as f:
                f.write(f"{counter[0]} {counter[1]}\n")
            await asyncio.sleep(0.2)

    server = await asyncio.start_server(handle, "127.0.0.1", lport)
    asyncio.create_task(report())
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

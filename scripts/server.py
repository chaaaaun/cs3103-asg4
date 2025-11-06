import asyncio
import json
import struct
import time
from collections import deque

from gamenetapi import HUDP, ChannelType
import socket

class UdpMetrics:
    def __init__(self, window_seconds=1.0, reorder_window=1024):
        # Timing/units
        self.window_seconds = window_seconds  # throughput and PDR windows [s]
        # Per-packet state
        self.last_transit_ms = None  # for RTP jitter transit delta [ms]
        self.jitter_ms = 0.0  # RTP smoothed interarrival jitter estimate [ms]
        self.last_delay_ms = None  # for IPDV if you need it later [ms]
        # Throughput window (receiver-side)
        self.bytes_window = deque()  # (recv_time_s, size_bytes)
        self.bytes_in_window = 0
        # Loss/PDR tracking with reordering tolerance
        self.reorder_window = reorder_window
        self.high_watermark = -1  # highest seq observed so far
        self.bitmap = 0  # bitset for [high_watermark - k, ..., high_watermark-1]
        self.received_total = 0
        self.expected_total = 0
        # Rolling PDR window
        self.packets_window = deque()  # (recv_time_s, seq)
        self.expected_window = 0
        self.received_window = 0

    def _advance_bitmap(self, steps):
        # Shift left by steps; keep only reorder_window bits
        if steps >= self.reorder_window:
            self.bitmap = 0
        else:
            self.bitmap = ((self.bitmap << steps) & ((1 << self.reorder_window) - 1))

    def _mark_seen(self, seq):
        if self.high_watermark < 0:
            # First packet initializes state
            self.high_watermark = seq
            # No gap counted for the very first packet
            self.received_total += 1
            self.expected_total += 1
            return

        if seq > self.high_watermark:
            # New high watermark; any gap implies missing packets (potential loss)
            gap = seq - self.high_watermark
            self._advance_bitmap(gap)
            # Mark the just-arrived seq as seen (bit 0)
            self.bitmap |= 1
            # All packets in the gap were expected; only 1 arrived now
            self.expected_total += gap
            self.received_total += 1
            self.high_watermark = seq
        else:
            # seq <= high_watermark: in-order duplicate, reorder, or very late
            distance = self.high_watermark - seq
            if distance >= self.reorder_window:
                # Too old; treat as late beyond window (don’t change loss counters)
                return
            # If not seen before within window, mark and count received
            mask = 1 << distance
            if (self.bitmap & mask) == 0:
                self.bitmap |= mask
                self.received_total += 1
                # expected_total doesn’t change here; it was accounted when advancing
            # else duplicate; ignore for counters

    def _evict_windows(self, now_s):
        # Evict bytes outside throughput window
        while self.bytes_window and (now_s - self.bytes_window[0][0]) > self.window_seconds:
            _, sz = self.bytes_window.popleft()
            self.bytes_in_window -= sz
        # Evict packets outside PDR window; recompute expected_window conservatively
        # Strategy: maintain expected_window as range [min_seq_in_window, high_watermark],
        # but adjust as packets fall out; we approximate by counting arrivals and estimating expected via gaps seen while inside window.
        # Simpler approach: track min/max seq seen in window and compute expected_window = (max_seq - min_seq + 1) minus holes outside reorder_window.
        # Here we track just arrivals and recompute min/max on eviction for simplicity.
        pass  # See note below for a more robust windowed PDR update.

    def update_from_packet(self, seq, ts_in_ms, payload: bytes):
        now_ms = time.time() * 1000.0  # receiver arrival time in ms
        now_s = now_ms / 1000.0
        size_bytes = len(payload)

        # One-way latency (assuming synchronized clocks)
        delay_ms = now_ms - ts_in_ms

        # RTP interarrival jitter (RFC 3550 Appendix A.8), using ms units
        transit = delay_ms  # arrival - send in same units
        if self.last_transit_ms is None:
            self.last_transit_ms = transit
        d = transit - self.last_transit_ms
        self.last_transit_ms = transit
        if d < 0:
            d = -d
        # Exponential filter: J += (|d| - J)/16
        self.jitter_ms += (d - self.jitter_ms) / 16.0

        # Throughput window: add bytes, evict old
        self.bytes_window.append((now_s, size_bytes))
        self.bytes_in_window += size_bytes
        # Evict outside window
        while self.bytes_window and (now_s - self.bytes_window[0][0]) > self.window_seconds:
            _, sz = self.bytes_window.popleft()
            self.bytes_in_window -= sz
        throughput_bps = (self.bytes_in_window * 8.0) / self.window_seconds

        # Loss/PDR with reordering tolerance
        self._mark_seen(seq)
        # For a simple rolling PDR, maintain a time window of packets and recompute expected in-window as max_seq - min_seq + 1
        self.packets_window.append((now_s, seq))
        while self.packets_window and (now_s - self.packets_window[0][0]) > self.window_seconds:
            self.packets_window.popleft()
        if self.packets_window:
            seqs = [s for _, s in self.packets_window]
            min_seq, max_seq = min(seqs), max(seqs)
            expected_window = max_seq - min_seq + 1
            received_window = len(set(seqs))
            pdr_window = (received_window / expected_window) if expected_window > 0 else 1.0
        else:
            pdr_window = 1.0

        # Cumulative PDR
        pdr_total = (self.received_total / self.expected_total) if self.expected_total > 0 else 1.0

        return {
            "one_way_latency_ms": delay_ms,
            "jitter_ms": self.jitter_ms,
            "throughput_bps": throughput_bps,
            "pdr_window": pdr_window,
            "pdr_total": pdr_total,
            "received_total": self.received_total,
            "expected_total": self.expected_total,
        }

async def run_udp_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 9999))

    metrics = UdpMetrics(window_seconds=1.0, reorder_window=1024)
    results = None
    print("UDP server started.")

    try:
        while True:
            data, addr = sock.recvfrom(65535)
            if len(data) < 11:
                raise ValueError(f"packet too short: need at least 11 bytes, got {len(data)}")

            # 3-byte big-endian unsigned sequence
            seq = int.from_bytes(data[0:3], "big", signed=False) & 0xFFFFFF
            # 8-byte big-endian unsigned timestamp
            ts, = struct.unpack_from(">Q", data, 3)
            payload = data[11:]

            print(f"seq={seq} ts={ts} payload size={len(payload)}")
            results = metrics.update_from_packet(seq, ts, payload)

    except KeyboardInterrupt:
        print(results)
        print("\n[CLIENT] Interrupted")
    finally:
        sock.close()

async def run_hudp_server():
    hudp = await HUDP().start(local_addr=("127.0.0.1", 9999))
    print("HUDP server started.")

    try:
        while True:
            msg = await hudp.recv_message()
            print(f"Server got ch={msg.channel} seq={msg.seq} ts={msg.ts} payload={msg.payload!r} from {msg.addr}")

            # echo back using same sequence number
            if msg.payload == b'':
                hudp.send_message(msg.channel, msg.seq, int(time.time()), msg.payload, addr=msg.addr)

    finally:
        hudp.close()

if __name__ == "__main__":
    asyncio.run(run_udp_server())
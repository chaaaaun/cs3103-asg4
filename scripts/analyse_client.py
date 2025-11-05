# scripts/analyze_client.py
import argparse, asyncio, csv, os, socket, struct, time, random
from typing import Optional

DELAY_MEAN = 0.05           
DELAY_JITTER = 0.02          # ±20 ms jitter
PACKET_LOSS_RATE = 0.1

# Try to import HUDP only if used
try:
    from gamenetapi import HUDP, ChannelType
except Exception:
    HUDP = None  # type: ignore

CSV_PATH = "metrics.csv"

def now_ms() -> int:
    return int(time.time() * 1000)

def ensure_csv(path=CSV_PATH):
    if not os.path.exists(path):
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["ts_ms","role","mode","event","seq","send_ms","recv_ms","rtt_ms","bytes","pdr","throughput_bps","jitter_ms"])

def write_row(**kw):
    ensure_csv()
    with open(CSV_PATH, "a", newline="") as f:
        csv.writer(f).writerow([
            kw.get("ts_ms", now_ms()),
            kw.get("role", "client"),
            kw.get("mode"),
            kw.get("event"),
            kw.get("seq"),
            kw.get("send_ms"),
            kw.get("recv_ms"),
            kw.get("rtt_ms"),
            kw.get("bytes"),
            kw.get("pdr"),
            kw.get("throughput_bps"),
            kw.get("jitter_ms"),
        ])

def pack_payload(seq: int, send_ms: int, user_bytes: bytes) -> bytes:
    # wire: 3-byte seq (big-endian) + 8-byte send_ms + user payload
    return seq.to_bytes(3, "big", signed=False) + struct.pack(">Q", send_ms) + user_bytes

def unpack_payload(b: bytes):
    seq = int.from_bytes(b[0:3], "big", signed=False)
    send_ms = struct.unpack(">Q", b[3:11])[0]
    user = b[11:]
    return seq, send_ms, user

async def delays_and_drop(udp, payload, seq, payload_len):
    # Simulate network delay (used to represent latency)
        network_delay = max(0, random.gauss(DELAY_MEAN, DELAY_JITTER))
        await asyncio.sleep(network_delay)

        # Simulating packet loss
        if random.random() < PACKET_LOSS_RATE:
            print(f"[DROP] message")
            # packet lost
        else:
            payload = pack_payload(seq & 0xFFFFFF, now_ms(), os.urandom(payload_len))
            udp.send_message(ChannelType.UNRELIABLE, seq & 0xFFFFFF, int(time.time()), payload)


async def run_hudp(addr: str, port: int, pps: int, duration_s: int, payload_len: int):
    assert HUDP is not None, "gamenetapi not available"
    udp = await HUDP().start(remote_addr=(addr, port))
    mode = "hudp-unreliable"  # flip to hudp-reliable later
    try:
        seq = 0
        interval = 1.0 / max(1, pps)
        t_end = time.time() + duration_s

        sent = 0
        got = 0
        bytes_recv = 0
        last_t = time.time()
        jitter = 0.0
        prev_rtt = None

        while time.time() < t_end:
            # send one packet
            payload = pack_payload(seq & 0xFFFFFF, now_ms(), os.urandom(payload_len))
            asyncio.create_task(delays_and_drop(udp, payload, seq, payload_len))
            sent += 1

            # try to receive one (echo)
            try:
                udp._transport.set_write_buffer_limits(0)  # keep it simple
                resp = await asyncio.wait_for(udp.recv_message(), timeout=interval)
                r_seq, s_ms, user = unpack_payload(resp.payload)
                rtt = now_ms() - s_ms
                got += 1
                bytes_recv += len(resp.payload)

                # PDR and throughput estimates
                pdr = got / sent if sent else 0.0
                elapsed = time.time() - last_t
                if elapsed >= 1.0:
                    throughput_bps = bytes_recv * 8 / elapsed
                    bytes_recv = 0
                    last_t = time.time()
                else:
                    throughput_bps = ""

                # RFC3550 jitter estimator on RTT deltas (good enough for comparison)
                if prev_rtt is not None:
                    d = abs(rtt - prev_rtt)
                    jitter += (d - jitter) / 16.0
                prev_rtt = rtt

                write_row(mode=mode, event="echo_ok", seq=r_seq, send_ms=s_ms, recv_ms=now_ms(),
                          rtt_ms=rtt, bytes=len(resp.payload), pdr=pdr, throughput_bps=throughput_bps, jitter_ms=round(jitter,2))
            except asyncio.TimeoutError:
                write_row(mode=mode, event="echo_timeout", seq=seq, send_ms=now_ms(),
                          recv_ms="", rtt_ms="", bytes="", pdr=(got/sent if sent else 0.0), throughput_bps="", jitter_ms="")

            seq += 1
            # rate control
            await asyncio.sleep(max(0.0, interval))
    finally:
        udp.close()

def run_raw(addr: str, port: int, pps: int, duration_s: int, payload_len: int):
    mode = "udp-baseline"
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(max(0.001, 1.0/pps))
    target = (addr, port)

    seq = 0
    interval = 1.0 / max(1, pps)
    t_end = time.time() + duration_s

    sent = 0
    got = 0
    bytes_recv = 0
    last_t = time.time()
    jitter = 0.0
    prev_rtt = None

    while time.time() < t_end:
        s_ms = now_ms()
        pkt = pack_payload(seq & 0xFFFFFF, s_ms, os.urandom(payload_len))
        sock.sendto(pkt, target)
        sent += 1

        # try recv echo
        try:
            data, _ = sock.recvfrom(65535)
            r_seq, s_ms, _ = unpack_payload(data)
            rtt = now_ms() - s_ms
            got += 1
            bytes_recv += len(data)

            pdr = got / sent if sent else 0.0
            elapsed = time.time() - last_t
            if elapsed >= 1.0:
                throughput_bps = bytes_recv * 8 / elapsed
                bytes_recv = 0
                last_t = time.time()
            else:
                throughput_bps = ""

            if prev_rtt is not None:
                d = abs(rtt - prev_rtt)
                jitter += (d - jitter) / 16.0
            prev_rtt = rtt

            write_row(mode=mode, event="echo_ok", seq=r_seq, send_ms=s_ms, recv_ms=now_ms(),
                      rtt_ms=rtt, bytes=len(data), pdr=pdr, throughput_bps=throughput_bps, jitter_ms=round(jitter,2))
        except socket.timeout:
            write_row(mode=mode, event="echo_timeout", seq=seq, send_ms=now_ms(),
                      recv_ms="", rtt_ms="", bytes="", pdr=(got/sent if sent else 0.0), throughput_bps="", jitter_ms="")
        seq += 1
        # rate control
        now_t = time.time()
        sleep_t = interval - (now_t - (s_ms/1000))
        if sleep_t > 0:
            time.sleep(sleep_t)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["udp", "hudp"], required=True, help="udp=baseline raw, hudp=current HUDP (unreliable)")
    parser.add_argument("--addr", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--pps", type=int, default=200, help="packets per second")
    parser.add_argument("--duration", type=int, default=10, help="seconds")
    parser.add_argument("--payload", type=int, default=32, help="bytes of user payload")
    args = parser.parse_args()

    ensure_csv()
    if args.mode == "udp":
        run_raw(args.addr, args.port, args.pps, args.duration, args.payload)
    else:
        asyncio.run(run_hudp(args.addr, args.port, args.pps, args.duration, args.payload))

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

async def delays_and_drop(udp, seq, payload_len):
    # Simulate network delay (used to represent latency)
    network_delay = max(0, random.gauss(DELAY_MEAN, DELAY_JITTER))
    await asyncio.sleep(network_delay)

    # Simulating packet loss
    if random.random() < PACKET_LOSS_RATE:
        print(f"[DROP] seq={seq} message")
        # packet lost
    else:
        payload = pack_payload(seq & 0xFFFFFF, now_ms(), os.urandom(payload_len))
        await udp.send_reliable_with_window(seq & 0xFFFFFF, payload)


async def run_hudp_u(addr: str, port: int, pps: int, duration_s: int, payload_len: int):
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
            udp.send_message(ChannelType.UNRELIABLE, False, seq & 0xFFFFFF, payload)
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

ACK_GRACE_S = 0.5

async def run_hudp_r(addr: str, port: int, pps: int, duration_s: int, payload_len: int):
    assert HUDP is not None, "gamenetapi not available"
    udp = await HUDP().start(remote_addr=(addr, port))
    mode = "hudp-reliable"

    try:
        interval = 1.0 / max(1, pps)
        t_end = time.time() + duration_s

        sent = 0                 # actually transmitted (not locally dropped)
        acked = 0
        bytes_acked = 0
        last_t = time.time()
        jitter = 0.0
        prev_rtt = None

        pending: set[int] = set()     # seqs we tried to send and expect ACKs for
        tasks: list[asyncio.Task] = [] # send tasks

        stop_acks = asyncio.Event()

        async def ack_listener():
            nonlocal acked, bytes_acked, last_t, jitter, prev_rtt
            while not stop_acks.is_set():
                try:
                    evt = await asyncio.wait_for(udp._ack_queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue

                seq = evt["seq"]
                rtt = evt["rtt_ms"]

                if seq in pending:
                    pending.remove(seq)
                acked += 1
                bytes_acked += payload_len

                # PDR & throughput every ~1s
                pdr = (acked / sent) if sent else 0.0
                elapsed = time.time() - last_t
                throughput_bps = ""
                if elapsed >= 1.0:
                    throughput_bps = bytes_acked * 8 / elapsed
                    bytes_acked = 0
                    last_t = time.time()

                # simple jitter estimator on ACK RTT deltas
                if prev_rtt is not None:
                    d = abs(rtt - prev_rtt)
                    jitter += (d - jitter) / 16.0
                prev_rtt = rtt

                write_row(mode=mode, event="echo_ok", seq=seq, send_ms="",
                        recv_ms=now_ms(), rtt_ms=round(rtt, 1), bytes=payload_len,
                        pdr=pdr, throughput_bps=throughput_bps, jitter_ms=round(jitter, 2))

        ack_task = asyncio.create_task(ack_listener())

        # schedule sends as fire-and-forget tasks
        seq = 0
        while time.time() < t_end:
            s = seq  # capture per-iteration
            async def one_send(s=s):
                # do delay/loss & maybe send
                actually_sent = await delays_and_drop(udp, s, payload_len)
                if actually_sent:
                    # mark pending & increment sent only when it really went out
                    pending.add(s)
                return actually_sent

            t = asyncio.create_task(one_send())
            tasks.append(t)

            # keep pacing
            await asyncio.sleep(interval)

            # after the task completes, update 'sent' if it actually transmitted
            try:
                if await t:
                    sent += 1
            except Exception:
                # defensive: log a failed send task as a local drop
                print("error")

            seq += 1

        # all send tasks are created; wait for any still running to finish
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # grace period for late ACKs
        try:
            await asyncio.wait_for(stop_acks.wait(), timeout=0.0)
        except Exception:
            pass
        await asyncio.sleep(ACK_GRACE_S)
        stop_acks.set()
        await asyncio.sleep(0)  # let ack_task notice the event
        ack_task.cancel()
        try:
            await ack_task
        except asyncio.CancelledError:
            pass

        # whatever is still pending now -> timeout
        for s in sorted(pending):
            write_row(mode=mode, event="echo_timeout", seq=s, send_ms=now_ms(),
                      recv_ms="", rtt_ms="", bytes="", pdr=(acked/sent if sent else 0.0),
                      throughput_bps="", jitter_ms="")

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
    parser.add_argument("--mode", choices=["udp", "hudp-u", "hudp-r"], required=True, help="udp=baseline raw, hudp=current HUDP (unreliable)")
    parser.add_argument("--addr", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--pps", type=int, default=200, help="packets per second")
    parser.add_argument("--duration", type=int, default=10, help="seconds")
    parser.add_argument("--payload", type=int, default=32, help="bytes of user payload")
    args = parser.parse_args()

    ensure_csv()
    if args.mode == "udp":
        run_raw(args.addr, args.port, args.pps, args.duration, args.payload)
    elif args.mode == "hudp-u":
        asyncio.run(run_hudp_u(args.addr, args.port, args.pps, args.duration, args.payload))
    else:  # "hudp-r"
        asyncio.run(run_hudp_r(args.addr, args.port, args.pps, args.duration, args.payload))

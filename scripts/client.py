import argparse
import asyncio
import os
import socket
import struct
import time
from typing import Tuple

from gamenetapi import HUDP


# Collect metrics on the server side
async def send_udp(target: Tuple[str, int], sock: socket.socket, pps: int, duration_s: int, payload_len: int):
    sock.settimeout(max(0.001, 1.0 / pps))

    seq = 0
    interval = 1.0 / max(1, pps)

    loop = asyncio.get_running_loop()
    next_time = loop.time()
    end_time = loop.time() + duration_s

    while loop.time() < end_time:
        next_time += interval
        pkt = (seq & 0xFFFFFF).to_bytes(3, "big", signed=False) + struct.pack(">Q",
                                                                              int(time.time() * 1000)) + os.urandom(
            payload_len)
        sock.sendto(pkt, target)

        seq += 1

        print("[UDP] Sent packet with seq", seq)
        await asyncio.sleep(max(0.0, next_time - loop.time()))


async def send_hudp_unreliable(target: Tuple[str, int], hudp: HUDP, pps: int, duration_s: int, payload_len: int):
    seq = 0
    interval = 1.0 / max(1, pps)

    loop = asyncio.get_running_loop()
    next_time = loop.time()
    end_time = loop.time() + duration_s

    while loop.time() < end_time:
        next_time += interval
        await hudp.send_unreliable(seq, os.urandom(payload_len), target)

        seq += 1
        await asyncio.sleep(max(0.0, next_time - loop.time()))


async def send_hudp_reliable(target: Tuple[str, int], hudp: HUDP, pps: int, duration_s: int, payload_len: int):
    seq = 0
    interval = 1.0 / max(1, pps)

    loop = asyncio.get_running_loop()
    next_time = loop.time()
    end_time = loop.time() + duration_s

    while loop.time() < end_time:
        next_time += interval
        await hudp.send_reliable_with_window(seq, os.urandom(payload_len), target)

        seq += 1
        await asyncio.sleep(max(0.0, next_time - loop.time()))


async def run_client(protocol: str, addr, pps: int, duration_s: int, payload_len: int):
    hudp = await HUDP().start(remote_addr=addr)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Client maintains its own sequence numbers
    reliable_seq = 0
    unreliable_seq = 0

    try:
        if protocol == "udp":
            await send_udp(addr, sock, pps, duration_s, payload_len)
        elif protocol == "reliable":
            await send_hudp_reliable(addr, hudp, pps, duration_s, payload_len)
        elif protocol == "unreliable":
            await send_hudp_unreliable(addr, hudp, pps, duration_s, payload_len)
    except KeyboardInterrupt:
        print("\n[CLIENT] Interrupted")
        return
    finally:
        hudp.close()
        sock.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocol", "-p", choices=["udp", "unreliable", "reliable"], required=True,
                        help="udp=baseline raw, unreliable=HUDP (unreliable) reliable=HUDP (reliable)")
    parser.add_argument("--addr", "-a", default="127.0.0.1")
    parser.add_argument("--port", "-P", type=int, default=9999)
    parser.add_argument("--pps", type=int, default=50, help="packets per second")
    parser.add_argument("--duration", "-t", type=int, default=30, help="seconds")
    parser.add_argument("--payload", type=int, default=128, help="bytes of user payload")
    args = parser.parse_args()

    asyncio.run(run_client(args.protocol, (args.addr, args.port), args.pps, args.duration, args.payload))

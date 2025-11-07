import argparse
import asyncio
import socket
import struct

import gamenetapi
from gamenetapi import HUDP, ChannelType


async def run_udp_server(target):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(target)

    metrics = gamenetapi.Metrics(window_seconds=1.0, reorder_window=1024)
    results = None
    print("UDP server started.")

    try:
        while True:
            data, addr = sock.recvfrom(65535)
            sock.sendto(data, addr)
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

async def run_hudp_server(target):
    hudp = await HUDP().start(local_addr=target)
    print("HUDP server started.")

    try:
        while True:
            msg = await hudp.recv_message()
            # Echo the payload back verbatim so the client can compute RTT/jitter/throughput
            is_reliable = (msg.channel == ChannelType.RELIABLE)
            # reuse the same seq so the client can match
            hudp.send_message(msg.channel, is_reliable, msg.seq, msg.payload, addr=msg.addr)
    except asyncio.CancelledError:
        pass
    finally:
        print("")
        print("=====")
        print("Sample metrics for last-sent unreliable packet")
        print("=====")
        rm = hudp.get_metrics(ChannelType.UNRELIABLE, -1)
        print("single-packet one way latency: ", rm["one_way_latency_ms"], "ms")
        print("jitter: ", rm["jitter_ms"], "ms")
        print("session throughput: ", rm["throughput_bps"], "bps")
        print("packet delivery ratio: ", rm["pdr_total"]*100, "%")

        print("")
        print("=====")
        print("Sample metrics for last-sent reliable packet")
        print("=====")
        rm = hudp.get_metrics(ChannelType.RELIABLE, -1)
        print("single-packet one way latency: ", rm["one_way_latency_ms"], "ms")
        print("jitter: ", rm["jitter_ms"], "ms")
        print("session throughput: ", rm["throughput_bps"], "bps")
        print("packet delivery ratio: ", rm["pdr_total"]*100, "%")

        hudp.close()

async def run_server(protocol, target, timeout):
    try:
        if protocol == "udp":
            await asyncio.wait_for(run_udp_server(target), timeout)
        elif protocol == "hudp":
            await asyncio.wait_for(run_hudp_server(target), timeout)
    except asyncio.TimeoutError:
        print("Testing ending automatically due to asyncio timeout error...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocol", "-p", choices=["udp", "hudp"], required=True)
    parser.add_argument("--addr", "-a", default="127.0.0.1")
    parser.add_argument("--port", "-P", type=int, default=9999)
    parser.add_argument("--timeout", "-t", type=int, default=65)
    args = parser.parse_args()

    asyncio.run(run_server(args.protocol, (args.addr, args.port), args.timeout))

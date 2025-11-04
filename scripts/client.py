import asyncio
import time
import random

from gamenetapi import HUDP, ChannelType

RELIABILITY_RATIO = 1.0

async def run_client():
    udp = await HUDP().start(remote_addr=("127.0.0.1", 9999))

    reliable_seq = 0
    unreliable_seq = 0

    try:
        while True:
            is_reliable = random.random() < RELIABILITY_RATIO
            ts = int(time.time())
            if is_reliable:
                udp.send_message(ChannelType.RELIABLE, reliable_seq, ts, payload="hello")
                reliable_seq += 1
            else:
                udp.send_message(ChannelType.UNRELIABLE, unreliable_seq, ts, payload="hello")
                unreliable_seq += 1

            msg = await asyncio.wait_for(udp.recv_message(), timeout=5)
            print(f"Client got ch={msg.channel} seq={msg.seq} ts={msg.ts} payload={msg.payload!r} from {msg.addr}")

            await asyncio.sleep(0.5)

    finally:
        udp.close()

if __name__ == "__main__":
    asyncio.run(run_client())
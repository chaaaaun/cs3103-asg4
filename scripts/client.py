import asyncio
import time

from gamenetapi import HUDP, ChannelType


async def run_client():
    udp = await HUDP().start(remote_addr=("127.0.0.1", 9999))
    try:
        udp.send_message(channel=ChannelType.UNRELIABLE, seq=1, ts=int(time.time()), payload="hello")
        resp = await udp.recv_message()
        print(f"client got ch={resp.channel} seq={resp.seq} ts={resp.ts} payload={resp.payload!r} from {resp.addr}")
    finally:
        udp.close()


if __name__ == "__main__":
    print("Starting client...")
    asyncio.run(run_client())

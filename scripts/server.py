import asyncio
import time
from gamenetapi import HUDP, ChannelType

async def run_server():
    udp = await HUDP().start(local_addr=("127.0.0.1", 9999))
    print("Server started.")

    try:
        while True:
            msg = await udp.recv_message()
            print(f"Server got ch={msg.channel} seq={msg.seq} ts={msg.ts} payload={msg.payload!r} from {msg.addr}")

    finally:
        udp.close()

if __name__ == "__main__":
    asyncio.run(run_server())
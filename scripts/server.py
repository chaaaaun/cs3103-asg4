import asyncio
import time

from gamenetapi import HUDP


async def run_server():
    udp = await HUDP().start(local_addr=("127.0.0.1", 9999))
    print("Server started.")
    try:
        while True:
            msg = await udp.recv_message()
            print(f"server got ch={msg.channel} seq={msg.seq} ts={msg.ts} payload={msg.payload!r} from {msg.addr}")
            # echo back, bump sequence as example
            udp.send_message(msg.channel, (msg.seq + 1) & 0xFFFFFF, int(time.time()), msg.payload, addr=msg.addr)
    finally:
        udp.close()

if __name__ == "__main__":
    print("Starting server...")
    asyncio.run(run_server())
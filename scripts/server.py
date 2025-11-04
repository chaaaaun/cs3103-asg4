import asyncio
import time
from gamenetapi import HUDP, ChannelType

async def run_server():
    udp = await HUDP(is_server=True).start(local_addr=("127.0.0.1", 9999))
    print("Server started.")

    try:
        while True:
            msg = await udp.recv_message()
            print(f"Server got ch={msg.channel} seq={msg.seq} ts={msg.ts} payload={msg.payload!r} from {msg.addr}")

            # echo back using same sequence number
            udp.send_message(msg.channel, msg.seq, int(time.time()), msg.payload, addr=msg.addr)

    finally:
        udp.close()

if __name__ == "__main__":
    asyncio.run(run_server())
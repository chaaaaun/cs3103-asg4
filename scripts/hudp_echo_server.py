# scripts/hudp_echo_server.py
import asyncio, time
from gamenetapi import HUDP, ChannelType

async def run(host="127.0.0.1", port=9999):
    udp = await HUDP().start(local_addr=(host, port))
    print(f"[hudp] echo server on {host}:{port}")
    try:
        while True:
            msg = await udp.recv_message()
            # echo back on same channel
            if msg.channel == ChannelType.UNRELIABLE:
                udp.send_message(ChannelType.UNRELIABLE, (msg.seq + 1) & 0xFFFFFF, int(time.time()), msg.payload, addr=msg.addr)
            else:
                # when reliable is added later, switch to udp.send_reliable(...)
                udp.send_message(msg.channel, (msg.seq + 1) & 0xFFFFFF, int(time.time()), msg.payload, addr=msg.addr)
    finally:
        udp.close()

if __name__ == "__main__":
    asyncio.run(run())

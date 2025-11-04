import asyncio
import time
import random

from gamenetapi import HUDP, ChannelType

RELIABILITY_RATIO = 0.5
WINDOW_SIZE = 5
SEND_INTERVAL = 0.1

async def run_client():
    addr = ("127.0.0.1", 9999)
    udp = await HUDP().start(remote_addr=addr)
    
    # Client maintains its own sequence numbers
    reliable_seq = 0
    unreliable_seq = 0
    try:
        # Create separate tasks for sending and receiving
        async def send_loop():
            nonlocal reliable_seq, unreliable_seq
            while True:
                is_reliable = random.random() < RELIABILITY_RATIO
                if is_reliable:
                    # Check window availability (only for reliable)
                    active_reliable = sum(1 for (a, _) in udp.send_window.keys() if a == addr)
                    
                    if active_reliable >= WINDOW_SIZE:
                        print(f"[CLIENT] Window full")
                        await asyncio.sleep(0.1)
                        continue

                    ts = int(time.time())
                    seq_used = udp.send_message(ChannelType.RELIABLE, reliable_seq, ts=ts, payload="hello", addr=addr)
                    reliable_seq += 1
                    print(f"[CLIENT SEND] {'REL'} seq={seq_used}")

                else:
                    ts = int(time.time())
                    seq_used = udp.send_message(ChannelType.UNRELIABLE, unreliable_seq, ts=ts, payload="hello", addr=addr)
                    unreliable_seq += 1
                    print(f"[CLIENT SEND] {'UNREL'} seq={seq_used}")
        
        async def recv_loop():
            while True:
                try:
                    msg = await asyncio.wait_for(udp.recv_message(), timeout=0.1)
                    print(f"Client got ch={msg.channel} seq={msg.seq} ts={msg.ts} payload={msg.payload!r} from {msg.addr}")
                except asyncio.TimeoutError:
                    continue

        # Run both loops concurrently
        send_task = asyncio.create_task(send_loop())
        recv_task = asyncio.create_task(recv_loop())
        
        # Run for 30 seconds as per spec
        await asyncio.sleep(30)
        
        send_task.cancel()
        recv_task.cancel()
        
    except KeyboardInterrupt:
        print("\n[CLIENT] Interrupted")
    finally:
        udp.close()

if __name__ == "__main__":
    asyncio.run(run_client())
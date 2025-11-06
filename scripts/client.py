import asyncio
import time
import random
from gamenetapi import HUDP, ChannelType

RELIABILITY_RATIO = 0.5
WINDOW_SIZE = 5
SEND_INTERVAL = 0.1
DELAY_MEAN = 0.05           
DELAY_JITTER = 0.02          # ±20 ms jitter
PACKET_LOSS_RATE = 0.1
RETRANSMIT_TIMEOUT = 1.0     # Retransmit after 1 second if no ACK

async def delayed_send(udp, addr, channel, seq, ts, payload):
    """Simulate network delay and packet loss"""
    # Simulate network delay
    network_delay = max(0, random.gauss(DELAY_MEAN, DELAY_JITTER))
    await asyncio.sleep(network_delay)
    
    # Simulate packet loss
    if random.random() < PACKET_LOSS_RATE:
        print(f"[DROP] {channel} seq={seq}")
        return False  # Packet dropped
    else:
        udp.send_message(channel, seq, ts, payload, addr)
        return True  # Packet sent

async def send_reliable_with_retry(udp, addr, seq, payload, max_retries=5):
    """Send reliable message with automatic retransmission on timeout"""
    retries = 0
    
    while retries < max_retries:
        ts = int(time.monotonic())
        
        # Send with delay and potential drop
        sent = await delayed_send(udp, addr, ChannelType.RELIABLE, seq, ts, payload)
        
        if sent:
            print(f"[CLIENT SEND] REL seq={seq} (attempt {retries + 1})")
        else:
            print(f"[CLIENT SEND] REL seq={seq} (attempt {retries + 1}) - DROPPED, will retry")
        
        # Wait for ACK with timeout
        try:
            # Check if we got an ACK within timeout period
            # This assumes the HUDP implementation handles ACKs internally
            # and removes from send_window when ACK received
            start_time = time.monotonic()
            while time.monotonic() - start_time < RETRANSMIT_TIMEOUT:
                # Check if message is still in send window (not ACKed)
                if (addr, seq) not in udp.send_window:
                    print(f"[CLIENT ACK] REL seq={seq} acknowledged")
                    return True  # Successfully delivered
                await asyncio.sleep(0.01)
            
            # Timeout - no ACK received
            print(f"[CLIENT TIMEOUT] REL seq={seq} - retransmitting")
            retries += 1
            
        except Exception as e:
            print(f"[CLIENT ERROR] seq={seq}: {e}")
            retries += 1
    
    print(f"[CLIENT FAILED] REL seq={seq} - max retries reached")
    return False

async def run_client():
    addr = ("127.0.0.1", 9999)
    udp = await HUDP().start(remote_addr=addr)
    
    # Client maintains its own sequence numbers
    reliable_seq = 0
    unreliable_seq = 0
    
    try:
        async def send_loop():
            nonlocal reliable_seq, unreliable_seq
            while True:
                is_reliable = random.random() < RELIABILITY_RATIO
                
                if is_reliable:
                    # BLOCK until window has room, then send with explicit seq
                    sent_seq = await udp.send_reliable_with_window(reliable_seq, "hello", addr=addr)
                    print(f"[CLIENT SEND] REL seq={sent_seq}")
                    reliable_seq += 1
                    
                else:
                    #continue # for testing of ONLY reliable
                    # Unreliable messages - send once with delay/drop, no retry
                    ts = int(time.monotonic())
                    sent = await delayed_send(udp, addr, ChannelType.UNRELIABLE, unreliable_seq, ts, "hello")
                    
                    if sent:
                        print(f"[CLIENT SEND] UNREL seq={unreliable_seq}")
                    else:
                        print(f"[CLIENT SEND] UNREL seq={unreliable_seq} - DROPPED (no retry)")
                    
                    unreliable_seq += 1
                
                await asyncio.sleep(SEND_INTERVAL)
        
        async def recv_loop():
            while True:
                try:
                    msg = await asyncio.wait_for(udp.recv_message(), timeout=0.1)
                    print(f"[CLIENT RECV] ch={msg.channel} seq={msg.seq} ts={msg.ts} payload={msg.payload!r} from {msg.addr}")
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

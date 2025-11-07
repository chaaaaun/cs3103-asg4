import socket
import time
import random
import statistics

SERVER_IP = "127.0.0.1"
SERVER_PORT = 9999

# Simulation parameters (change these for different network conditions)
PACKET_LOSS_RATE = 0.1       
DELAY_MEAN = 0.05           
DELAY_JITTER = 0.02          # ±20 ms jitter
TOTAL_PACKETS = 100          # Number of packets to send

# Metrics
sent_packets = 0
delivered_packets = 0
latencies = []   # simulated one-way delay per packet
start_time = time.time()

with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
    for i in range(1, TOTAL_PACKETS + 1):
        message = f"Packet {i}"
        sent_packets += 1

        # Simulate network delay (used to represent latency)
        network_delay = max(0, random.gauss(DELAY_MEAN, DELAY_JITTER))
        time.sleep(network_delay)

        # Simulating packet loss
        if random.random() < PACKET_LOSS_RATE:
            print(f"Drop: {message}")
            continue  # packet lost
        else:
            client_socket.sendto(message.encode(), (SERVER_IP, SERVER_PORT))
            delivered_packets += 1
            latencies.append(network_delay)
            print(f"Sent: {message} (simulated latency={network_delay*1000:.2f} ms)")

end_time = time.time()
duration = end_time - start_time

# --- Metric Calculations ---
avg_latency = (sum(latencies) / len(latencies)) * 1000 if latencies else 0
jitter_values = [abs(latencies[i+1] - latencies[i]) for i in range(len(latencies)-1)]
avg_jitter = (statistics.mean(jitter_values) * 1000) if jitter_values else 0
throughput = (delivered_packets * 50) / duration  # ASSUMING!! 50 bytes per packet
pdr = (delivered_packets / sent_packets) * 100 if sent_packets else 0 #packet delivery ratio

# --- Results ---
print("\n--------Performance Metrics--------")
print(f"Total duration: {duration:.2f} s")
print(f"Packets sent: {sent_packets}")
print(f"Packets delivered: {delivered_packets}")
print(f"Packet delivery ratio: {pdr:.2f}%")
print(f"Average latency: {avg_latency:.2f} ms (one-way, simulated)")
print(f"Average jitter (RFC 3550): {avg_jitter:.2f} ms")
print(f"Throughput: {throughput/1000:.2f} KB/s")
print("============================\n")

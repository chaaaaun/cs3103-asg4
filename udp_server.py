# server
import socket
import asyncio

# Define server address and port
HOST = "127.0.0.1"    #"0.0.0.0"  # localhost
PORT = 9999
async def run_server():
    # Create UDP socket
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
        server_socket.bind((HOST, PORT))
        print(f"UDP server listening on {HOST}:{PORT}...")

        while True:
            data, addr = server_socket.recvfrom(65535)  # buffer size is 1024 bytes
            # print(f"Received message from {addr}: {data.decode()}")
            print(f"Received {len(data)} bytes from {addr}")
            
            # Echo message back to client
            # server_socket.sendto(b"Message received: " + data, addr)
            server_socket.sendto(data, addr)  

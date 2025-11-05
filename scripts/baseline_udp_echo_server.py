# scripts/baseline_udp_echo_server.py
import socket

def main(host="127.0.0.1", port=9000, buf=65535):
    print(f"[baseline] UDP echo server on {host}:{port}")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    while True:
        data, addr = sock.recvfrom(buf)
        # echo back verbatim
        sock.sendto(data, addr)

if __name__ == "__main__":
    main()

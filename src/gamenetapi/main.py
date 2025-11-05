import asyncio
import socket
import time
from collections import defaultdict
from typing import Optional, Tuple, Union, Dict, Any

from gamenetapi.header import (
    _encode_header, _decode_header, HudpMessage, ChannelType, Addr, BytesLike
)

PACKET_TIMEOUT_MS = 50
GIVEUP_TIMEOUT_MS = 200.0
WINDOW_SIZE = 5
MAX_SEQ = 65536

class _UDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, queue: "asyncio.Queue[Tuple[bytes, Addr]]"):
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.queue = queue
        self._closed = asyncio.get_running_loop().create_future()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: Addr) -> None:
        self.queue.put_nowait((data, addr))

    def error_received(self, exc: Exception) -> None:
        pass

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if not self._closed.done():
            self._closed.set_result(True)


class HUDP:
    def __init__(self):
        self._recv_queue: asyncio.Queue = asyncio.Queue()
        self._app_queue: asyncio.Queue = asyncio.Queue()
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional[_UDPProtocol] = None
        self._recv_task: Optional[asyncio.Task] = None
        self._closed = False
        
        # Selective Repeat state
        self.send_window: Dict[Tuple[Addr, int], Dict[str, Any]] = {}
        self.recv_window: Dict[Addr, Dict[int, HudpMessage]] = defaultdict(dict)
        self.send_seq: Dict[Addr, int] = defaultdict(int)
        self.send_base: Dict[Addr, int] = defaultdict(int)
        self.recv_base: Dict[Addr, int] = defaultdict(int)

        self._remote_addr: Optional[Addr] = None

    async def start(self, local_addr: Optional[Addr] = None, remote_addr: Optional[Addr] = None) -> "HUDP":
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: _UDPProtocol(self._recv_queue),
            local_addr=local_addr,
            remote_addr=remote_addr,
            family=socket.AF_INET
        )
        self._transport = transport
        self._protocol = protocol
        self._remote_addr = remote_addr

        # Start the main receive loop
        self._recv_task = asyncio.create_task(self._recv_loop())
        return self

    def close(self) -> None:
        if self._transport is not None:
            self._transport.close()
        self._closed = True
        if self._recv_task:
            self._recv_task.cancel()
        # Cancel all retransmit timers
        for entry in self.send_window.values():
            if "timer" in entry:
                entry["timer"].cancel()
        self.send_window.clear()

    def _get_timestamp_ms(self) -> int:
        return int(time.monotonic() * 1000) % (2**32)

    def _in_window(self, seq: int, base: int, size: int) -> bool:
        return ((seq - base) % MAX_SEQ) < size

    def send_raw(self, data: BytesLike, addr: Optional[Addr] = None) -> None:
        if self._transport is None:
            raise RuntimeError("Transport not started")
        if addr is None:
            addr = self._remote_addr
        if addr is None:
            raise ValueError("No destination address specified")
        self._transport.sendto(bytes(data), addr)

    def send_message(self, channel: ChannelType, seq: Optional[int], ts: int,
                     payload: Union[str, bytes], addr: Optional[Addr] = None) -> int:
        if isinstance(payload, str):
            payload = payload.encode()

        if addr is None:
            addr = self._remote_addr
        if addr is None:
            raise ValueError("No destination address for send_message")

        if seq is None:
            seq = self.send_seq[addr]
            self.send_seq[addr] = (seq + 1) % MAX_SEQ

        ts = self._get_timestamp_ms()
        header = _encode_header(channel, seq, ts)
        packet = header + payload

        self.send_raw(packet, addr)

        if channel == ChannelType.UNRELIABLE:
            print(f"[SEND UNREL] seq={seq} to {addr}")
        elif channel == ChannelType.RELIABLE:
            key = (addr, seq)
            self.send_window[key] = {
                'packet': packet,
                'first_sent': self._get_timestamp_ms(),
                'last_sent': self._get_timestamp_ms(),
                'retries': 0,
                'timer': asyncio.create_task(self._retransmit_timer(addr, seq))
            }
            print(f"[SEND REL] seq={seq} to {addr}")
        elif channel == ChannelType.ACK:
            print(f"[SEND ACK] seq={seq} to {addr}")

        return seq

    # Per-packet retransmit timer
    async def _retransmit_timer(self, addr: Addr, seq: int):
        key = (addr, seq)

        # This means that (addr, seq) still waiting for ACK.
        while key in self.send_window:
            await asyncio.sleep(PACKET_TIMEOUT_MS / 1000)
            if key not in self.send_window:
                break
            entry = self.send_window[key]
            elapsed = ((time.monotonic() - entry['first_sent']) * 1000) % (2**32)

            # If elapsed time exceeds max time allowed, skip the packet.
            if elapsed >= GIVEUP_TIMEOUT_MS:
                print(f"[GIVEUP] seq={seq} to {addr} after {elapsed:.0f}ms")
                del self.send_window[key]
                break
            self.send_raw(entry['packet'], addr)
            entry['last_sent'] = self._get_timestamp_ms()
            entry['retries'] += 1
            print(f"[RETRANSMIT] seq={seq} to {addr} (retry #{entry['retries']})")

    async def recv_message(self) -> HudpMessage:
        return await self._app_queue.get()

    async def _recv_loop(self):
        try:
            while not self._closed:
                data, addr = await self._recv_queue.get()
                try:
                    channel, seq, ts, payload = _decode_header(data)
                except Exception as e:
                    print(f"[ERROR] Bad packet from {addr}: {e}")
                    continue

                # Deliver unreliable packets immediately, no need sequence number also actually.
                if channel == ChannelType.UNRELIABLE:
                    await self._app_queue.put(HudpMessage(channel, seq, ts, payload, addr))
                    print(f"[RECV UNREL] seq={seq} from {addr}")

                # ACK messages: cancel retransmit timers
                # Only client will run this elif.
                elif channel == ChannelType.ACK:
                    key = (addr, seq)
                    if key in self.send_window:
                        entry = self.send_window[key]
                        if entry['retries'] == 0:
                            rtt = ((time.monotonic() - entry['first_sent']) * 1000) % (2**32)
                            print(f"[RECV ACK] seq={seq} from {addr}, RTT={rtt:.1f}ms")
                        else:
                            print(f"[RECV ACK] seq={seq} from {addr} (retransmitted)")
                        entry['timer'].cancel()
                        # ACK packet received for seq number so remove from send_window
                        del self.send_window[key]

                        # Slide window base
                        # send_base is the oldest packet that has not yet been ACKed.
                        while (addr, self.send_base[addr]) not in self.send_window:
                            self.send_base[addr] = (self.send_base[addr] + 1) % MAX_SEQ
                            if self.send_base[addr] == self.send_seq[addr]:
                                break

                elif channel == ChannelType.RELIABLE:
                    self.send_message(ChannelType.ACK, seq, self._get_timestamp_ms(), b'', addr)

                    base = self.recv_base[addr]
                    if not self._in_window(seq, base, WINDOW_SIZE):
                        print(f"[RECV REL] seq={seq} from {addr} outside window [base={base}]")
                        continue

                    if seq == base:
                        await self._deliver_in_order(addr, seq, ts, payload)
                        await self._deliver_buffered(addr)
                    elif seq in self.recv_window[addr]:
                        print(f"[RECV REL] seq={seq} from {addr} (duplicate)")
                    else:
                        msg = HudpMessage(channel, seq, ts, payload, addr)
                        msg.arrival_time = self._get_timestamp_ms()
                        self.recv_window[addr][seq] = msg
                        print(f"[RECV REL] seq={seq} from {addr} (buffered, expecting {base})")
                        await self._check_timeout_gaps(addr)

        except asyncio.CancelledError:
            pass

    async def _deliver_in_order(self, addr: Addr, seq: int, ts: int, payload: bytes):
        msg = HudpMessage(ChannelType.RELIABLE, seq, ts, payload, addr)
        await self._app_queue.put(msg)
        print(f"[RECV REL] seq={seq} from {addr} (in-order)")
        self.recv_base[addr] = (self.recv_base[addr] + 1) % MAX_SEQ

    async def _deliver_buffered(self, addr: Addr):
        base = self.recv_base[addr]
        while base in self.recv_window[addr]:
            msg = self.recv_window[addr].pop(base)
            await self._app_queue.put(msg)
            print(f"[RECV REL] seq={msg.seq} from {addr} (from buffer)")
            base = (base + 1) % MAX_SEQ
            self.recv_base[addr] = base

    async def _check_timeout_gaps(self, addr: Addr):
        base = self.recv_base[addr]
        # recv_window used to keep track of all the packets that have not yet been delivered to the application
        # due to previous packets missing.
        window = self.recv_window[addr]
        if not window:
            return

        ahead = [s for s in window.keys() if s > base]
        if not ahead:
            return
        
        earliest = min(ahead)
        msg = window.get(earliest)
        if msg is None:
            return
        
        arrival = getattr(msg, "arrival_time", None)
        if arrival is None:
            return
        waited_ms = (time.monotonic() - arrival) * 1000.0

        if waited_ms >= GIVEUP_TIMEOUT_MS:
            print(f"[TIMEOUT] Skipping gap from seq={base} to seq={earliest} after {waited_ms:.0f}ms")
            # Advance base to the earliest buffered seq we decided to accept,
            # then deliver that and any contiguous buffered packets immediately.
            self.recv_base[addr] = earliest
            await self._deliver_buffered(addr)
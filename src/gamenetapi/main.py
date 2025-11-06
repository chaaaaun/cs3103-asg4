import asyncio
import socket
import time
from collections import defaultdict
from typing import Optional, Tuple, Union, Dict, Any, List

from gamenetapi.header import (
    encode_packet, decode_packet, HudpMessage, ChannelType, Addr, BytesLike
)

PACKET_TIMEOUT_MS = 50.0  # per-packet retransmit timer period
GIVEUP_TIMEOUT_MS = 200.0  # drop unacked packet (sender) / skip missing (receiver) after this
SEND_WINDOW_SIZE = 5
WINDOW_SIZE = 10
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

        # Gap-age timer (receiver)
        self.gap_start: Dict[Addr, Optional[float]] = defaultdict(lambda: None)

        # Sender window event per address (wake blocked senders)
        self._win_event: Dict[Addr, asyncio.Event] = {}

    def _get_win_event(self, addr: Addr) -> asyncio.Event:
        ev = self._win_event.get(addr)
        if ev is None:
            ev = asyncio.Event()
            ev.set()  # initially open
            self._win_event[addr] = ev
        return ev

    def _inflight_count(self, addr: Addr) -> int:
        return sum(1 for (a, _) in self.send_window.keys() if a == addr)

    async def wait_for_window(self, addr: Optional[Addr] = None, timeout: Optional[float] = None) -> None:
        if addr is None:
            addr = self._remote_addr
        if addr is None:
            raise ValueError("No destination address for wait_for_window")
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while self._inflight_count(addr) >= SEND_WINDOW_SIZE:
            ev = self._get_win_event(addr)
            ev.clear()
            if timeout is None:
                await ev.wait()
            else:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise asyncio.TimeoutError
                await asyncio.wait_for(ev.wait(), timeout=remaining)

    # Non-blocking SR send (explicit seq); returns None if window full
    def try_send_reliable(self, seq: int, payload: Union[str, bytes], addr: Optional[Addr] = None) -> Optional[int]:
        if addr is None:
            addr = self._remote_addr
        if addr is None:
            raise ValueError("No destination address for try_send_reliable")
        if self._inflight_count(addr) >= SEND_WINDOW_SIZE:
            return None
        return self.send_message(ChannelType.RELIABLE, False, seq, self._get_timestamp_ms(), payload, addr=addr)

    # Blocking SR send (explicit seq); waits for a sender slot
    async def send_reliable_with_window(self, seq: int, payload: Union[str, bytes], addr: Optional[Addr] = None,
                                        timeout: Optional[float] = None) -> int:
        await self.wait_for_window(addr=addr, timeout=timeout)
        return self.send_message(ChannelType.RELIABLE, False, seq, self._get_timestamp_ms(), payload, addr=addr)

    # Blocking SU send (explicit seq)
    async def send_unreliable(self, seq: int, payload: Union[str, bytes], addr: Optional[Addr] = None,
                                        timeout: Optional[float] = None) -> int:
        return self.send_message(ChannelType.UNRELIABLE, False, seq, self._get_timestamp_ms(), payload, addr=addr)

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
        return int(time.monotonic() * 1000) % (2 ** 32)

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

    def send_message(self, channel: ChannelType, ack: bool, seq: Optional[int], ts: int, payload: Union[str, bytes],
                     addr: Optional[Addr] = None) -> int:
        if isinstance(payload, str):
            payload = payload.encode()

        if addr is None:
            addr = self._remote_addr
        if addr is None:
            raise ValueError("No destination address for send_message")

        # --- seq handling (supports explicit seq from caller) ---
        if seq is None:
            seq = self.send_seq[addr]
            self.send_seq[addr] = (seq + 1) % MAX_SEQ
        else:
            ahead = (seq + 1 - self.send_seq[addr]) % MAX_SEQ
            if 0 < ahead < (MAX_SEQ // 2):
                self.send_seq[addr] = (seq + 1) % MAX_SEQ

        ts = self._get_timestamp_ms()
        packet = encode_packet(channel, ack, seq, ts, payload)

        self.send_raw(packet, addr)

        if channel == ChannelType.UNRELIABLE:
            print(f"[SEND UNREL] seq={seq} to {addr}")
        elif channel == ChannelType.RELIABLE and not ack:
            key = (addr, seq)
            self.send_window[key] = {
                'packet': packet,
                'first_sent': time.monotonic(),
                'last_sent': time.monotonic(),
                'retries': 0,
                'timer': asyncio.create_task(self._retransmit_timer(addr, seq))
            }
            print(f"[SEND REL] seq={seq} to {addr}")
        elif channel == ChannelType.RELIABLE and ack:
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
            elapsed = (time.monotonic() - entry['first_sent']) * 1000

            # If elapsed time exceeds max time allowed, skip the packet.
            if elapsed >= GIVEUP_TIMEOUT_MS:
                print(f"[GIVEUP] seq={seq} to {addr} after {elapsed:.0f}ms")
                del self.send_window[key]
                self._get_win_event(addr).set()  # free a slot
                break
            self.send_raw(entry['packet'], addr)
            entry['last_sent'] = time.monotonic()
            entry['retries'] += 1
            print(f"[RETRANSMIT] seq={seq} to {addr} (retry #{entry['retries']})")

    async def recv_message(self) -> HudpMessage:
        return await self._app_queue.get()

    async def _recv_loop(self):
        try:
            while not self._closed:
                data, addr = await self._recv_queue.get()
                try:
                    channel, ack, seq, ts, payload = decode_packet(data)
                except Exception as e:
                    print(f"[ERROR] Bad packet from {addr}: {e}")
                    continue

                # Deliver unreliable packets immediately, no need sequence number also actually.
                if channel == ChannelType.UNRELIABLE:
                    await self._app_queue.put(HudpMessage(channel, seq, ts, payload, addr))
                    print(f"[RECV UNREL] seq={seq} from {addr}")

                # ACK messages: cancel retransmit timers
                elif channel == ChannelType.RELIABLE and ack:
                    key = (addr, seq)
                    if key in self.send_window:
                        entry = self.send_window[key]
                        if entry['retries'] == 0:
                            rtt = (time.monotonic() - entry['first_sent']) * 1000
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

                        self._get_win_event(addr).set()  # sender slot freed

                elif channel == ChannelType.RELIABLE:
                    base = self.recv_base[addr]

                    # Duplicate older than base -> re-ACK and discard
                    if seq < base:
                        self.send_message(ChannelType.RELIABLE, True, seq, self._get_timestamp_ms(), b'', addr)
                        print(f"[RECV REL] seq={seq} from {addr} (duplicate < base), reACK")
                        continue

                    # Before processing, see if the current head-of-line gap has aged out
                    await self._maybe_skip_gap_on_timeout(addr, seq)
                    base = self.recv_base[addr]

                    if not self._in_window(seq, base, WINDOW_SIZE):
                        print(f"[RECV REL] seq={seq} from {addr} outside window [base={base}]")
                        continue

                    # SR: ACK on receipt (even if buffered)
                    self.send_message(ChannelType.RELIABLE, True, seq, self._get_timestamp_ms(), b'', addr)

                    if seq == base:
                        await self._deliver_in_order(addr, seq, ts, payload)
                        await self._deliver_buffered(addr)
                        self.gap_start[addr] = None
                    elif seq in self.recv_window[addr]:
                        print(f"[RECV REL] seq={seq} from {addr} (duplicate)")
                    else:
                        msg = HudpMessage(channel, seq, ts, payload, addr)
                        msg.arrival_time = time.monotonic()  # harmless with gap-age logic
                        self.recv_window[addr][seq] = msg
                        print(f"[RECV REL] seq={seq} from {addr} (buffered, expecting {base})")
                        if self.gap_start[addr] is None:
                            self.gap_start[addr] = time.monotonic()

        except asyncio.CancelledError:
            pass

    async def _maybe_skip_gap_on_timeout(self, addr: Addr, incoming_seq: Optional[int]) -> None:
        """
        If we've been waiting for recv_base[addr] longer than GIVEUP_TIMEOUT_MS,
        skip it and advance to the earliest available in-window candidate > base,
        then immediately drain buffered contiguous packets.
        """
        gs = self.gap_start[addr]
        if gs is None:
            return

        waited_ms = (time.monotonic() - gs) * 1000.0
        if waited_ms < GIVEUP_TIMEOUT_MS:
            return

        base = self.recv_base[addr]
        candidates: List[int] = []
        if incoming_seq is not None and self._in_window(incoming_seq, base, WINDOW_SIZE) and incoming_seq > base:
            candidates.append(incoming_seq)
        for s in self.recv_window[addr].keys():
            if s > base and self._in_window(s, base, WINDOW_SIZE):
                candidates.append(s)

        if candidates:
            jump_to = min(candidates)
        else:
            # nothing available; minimally move base by one to avoid permanent stall
            jump_to = (base + 1) % MAX_SEQ

        print(f"[TIMEOUT] Skipping missing seq={base} after {waited_ms:.0f}ms")
        self.recv_base[addr] = jump_to
        self.gap_start[addr] = None
        await self._deliver_buffered(addr)

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

        # If still have future packets buffered, (re)start gap timer
        if any(s > self.recv_base[addr] for s in self.recv_window[addr].keys()):
            if self.gap_start[addr] is None:
                self.gap_start[addr] = time.monotonic()

import asyncio
import socket
from typing import Optional, Tuple, Union

from gamenetapi.header import _encode_header, HudpMessage, _decode_header, ChannelType

Addr = Tuple[str, int]
BytesLike = Union[bytes, bytearray, memoryview]


class _UDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, queue: "asyncio.Queue[Tuple[bytes, Addr]]"):
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.queue = queue
        self._closed = asyncio.get_running_loop().create_future()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: Addr) -> None:
        # Push received datagrams into an async queue for awaitable recv()
        self.queue.put_nowait((data, addr))

    def error_received(self, exc: Exception) -> None:
        # Optionally handle per-datagram errors (often rare with UDP)
        # Could log or push an error sentinel into the queue if desired.
        pass

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if not self._closed.done():
            self._closed.set_result(True)


class HUDP:
    def __init__(self) -> None:
        self._queue: "asyncio.Queue[Tuple[bytes, Addr]]" = asyncio.Queue()
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional[_UDPProtocol] = None

    async def start(
            self,
            local_addr: Optional[Addr] = None,
            remote_addr: Optional[Addr] = None,
    ) -> "HUDP":
        """
        - local_addr for server-style bind (e.g. ("127.0.0.1", 9999))
        - remote_addr for client-style connect (e.g. ("127.0.0.1", 9999))
        """
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: _UDPProtocol(self._queue),
            local_addr=local_addr,
            remote_addr=remote_addr,
            family=socket.AF_INET,
            reuse_port=False,
        )
        self._transport = transport  # asyncio.DatagramTransport
        self._protocol = protocol  # _UDPProtocol
        return self

    def close(self) -> None:
        if self._transport is not None:
            self._transport.close()

    # Low-level raw send/recv without custom header
    def send_raw(self, data: BytesLike, addr: Optional[Addr] = None) -> None:
        if self._transport is None:
            raise RuntimeError("UDP transport not started")
        if isinstance(data, bytearray):
            data = bytes(data)
        self._transport.sendto(data, addr)

    async def recv_raw(self) -> Tuple[bytes, Addr]:
        return await self._queue.get()

    def send_message(
            self,
            channel: ChannelType,
            seq: int,
            ts: int,
            payload: Union[str, BytesLike],
            addr: Optional[Addr] = None,
    ) -> None:
        """
        Encode header + payload and send.
        If remote_addr was set at start(), addr can be omitted.
        """
        if isinstance(payload, str):
            payload = payload.encode()
        header = _encode_header(channel, seq, ts)
        self.send_raw(header + bytes(payload), addr)

    async def recv_message(self) -> HudpMessage:
        """
        Receive a datagram and return parsed UdpMessage(channel, seq, ts, payload, addr).
        Raises ValueError if header is malformed or too short.
        TODO: Implement reliable channel handling
        """
        data, addr = await self.recv_raw()
        ch, seq, ts, payload = _decode_header(data)
        return HudpMessage(ch, seq, ts, payload, addr)

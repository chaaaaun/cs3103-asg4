from dataclasses import dataclass
from enum import IntEnum
from typing import Tuple, Union

Addr = Tuple[str, int]
BytesLike = Union[bytes, bytearray, memoryview]

# Fixed header sizes (big-endian)
HEADER_SIZE = 1 + 3 + 4  # 8 bytes total

class ChannelType(IntEnum):
    UNRELIABLE = 0
    RELIABLE = 1

@dataclass
class HudpMessage:
    channel: ChannelType
    seq: int
    ts: int
    payload: bytes
    addr: Addr

def _pack_u24_be(value: int) -> bytes:
    if not (0 <= value <= 0xFFFFFF):
        raise ValueError("SeqNo must fit in 3 bytes (0..16777215)")
    return bytes([(value >> 16) & 0xFF, (value >> 8) & 0xFF, value & 0xFF])

def _unpack_u24_be(b: bytes) -> int:
    if len(b) != 3:
        raise ValueError("Need exactly 3 bytes for u24")
    return (b[0] << 16) | (b[1] << 8) | b[2]

def _encode_header(channel: ChannelType, seq: int, ts: int) -> bytes:
    if not (0 <= channel <= 0xFF):
        raise ValueError("ChannelType must fit in 1 byte (0..255)")
    if not (0 <= ts <= 0xFFFFFFFF):
        raise ValueError("Timestamp must fit in 4 bytes (0..4294967295)")
    ch = channel.to_bytes(1, "big", signed=False)
    u24 = _pack_u24_be(seq)
    u32 = ts.to_bytes(4, "big", signed=False)
    return ch + u24 + u32

def _decode_header(data: bytes) -> Tuple[ChannelType, int, int, bytes]:
    if len(data) < HEADER_SIZE:
        raise ValueError(f"Datagram too short for header: {len(data)} < {HEADER_SIZE}")
    channel = data[0]
    if channel != ChannelType.UNRELIABLE and channel != ChannelType.RELIABLE:
        raise ValueError(f"Invalid channel: {channel}")
    seq = _unpack_u24_be(data[1:4])
    ts = int.from_bytes(data[4:8], "big", signed=False)
    payload = data[8:]
    return channel, seq, ts, payload
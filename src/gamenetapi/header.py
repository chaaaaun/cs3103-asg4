import struct
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
    channel: ChannelType  # 1 bit
    ack: bool  # 1 bit
    seq: int  # 3 bytes
    ts: int  # 4 bytes
    payload: bytes
    addr: Addr


def _pack_u8(channel: ChannelType, ack: bool) -> bytes:
    assert isinstance(channel, ChannelType)
    assert isinstance(ack, bool)

    byte = (int(channel) << 7) | ((1 if ack else 0) << 6)
    return struct.pack('>B', byte)


def _unpack_u8(b: bytes) -> Tuple[ChannelType, bool]:
    (val,) = struct.unpack('>B', b)
    ch = ChannelType((val >> 7) & 0x1)
    ack = bool((val >> 6) & 0x1)
    return ch, ack


def _pack_u24_be(value: int) -> bytes:
    if not (0 <= value <= 0xFFFFFF):
        raise ValueError("SeqNo must fit in 3 bytes (0..16777215)")
    return bytes([(value >> 16) & 0xFF, (value >> 8) & 0xFF, value & 0xFF])


def _unpack_u24_be(b: bytes) -> int:
    if len(b) != 3:
        raise ValueError("Need exactly 3 bytes for u24")
    return (b[0] << 16) | (b[1] << 8) | b[2]


def encode_packet(channel: ChannelType, ack: bool, seq: int, ts: int, payload: str | bytes) -> bytes:
    if not (0 <= channel <= 0xFF):
        raise ValueError("ChannelType must fit in 1 byte (0..255)")
    if not (0 <= ts <= 0xFFFFFFFF):
        raise ValueError("Timestamp must fit in 4 bytes (0..4294967295)")
    u8 = _pack_u8(channel, ack)
    u24 = _pack_u24_be(seq)
    u32 = ts.to_bytes(4, "big", signed=False)
    return u8 + u24 + u32 + payload


def decode_packet(data: bytes) -> Tuple[ChannelType, bool, int, int, bytes]:
    if len(data) < HEADER_SIZE:
        raise ValueError(f"Datagram too short for header: {len(data)} < {HEADER_SIZE}")

    u8 = data[0:1]
    channel, ack = _unpack_u8(u8)
    seq = _unpack_u24_be(data[1:4])
    ts = int.from_bytes(data[4:8], "big", signed=False)
    payload = data[8:]
    return channel, ack, seq, ts, payload

"""AppMessage — the key/value dict protocol Pebble watchapps use.

AppMessage payload layout (on the APP_MESSAGE endpoint):
  u8  command   (0x01 PUSH, 0x02 ACK, 0x03 NACK)
  u8  transaction_id
  16  app uuid  (only for PUSH)
  u8  tuple_count           (only for PUSH)
  then `tuple_count` tuples:
      u32 key
      u8  type   (see TupleType)
      u16 length
      length bytes value

Tuple headers and integer values are little-endian (matching the watch's CPU);
note this is the opposite of the big-endian Pebble Protocol frame around it.
"""

from __future__ import annotations

import struct
import uuid as _uuid
from enum import IntEnum

from .protocol import uuid_to_bytes


class AppMessageCmd(IntEnum):
    PUSH = 0x01
    ACK = 0x02
    NACK = 0x03


class TupleType(IntEnum):
    BYTES = 0
    CSTRING = 1
    UINT = 2
    INT = 3


class Int:
    """Wrap an int to force an exact AppMessage byte width.

    Pebble watchapps read a specific union member (uint8/uint16/uint32, or the
    signed equivalents). The auto-encoder picks the smallest width that fits,
    which can mismatch what the app reads (e.g. app does t->value->uint16 but a
    small number encoded as 1 byte). Use these to pin the width:

        from pebble_le import u8, u16, u32, i8, i16, i32
        await pebble.send_app_message(uuid, {1: u16(150), 4: u32(5000)})
    """

    __slots__ = ("value", "width", "signed")

    def __init__(self, value: int, width: int, signed: bool):
        self.value = value
        self.width = width
        self.signed = signed


def u8(v: int) -> Int:
    return Int(v, 1, False)


def u16(v: int) -> Int:
    return Int(v, 2, False)


def u32(v: int) -> Int:
    return Int(v, 4, False)


def i8(v: int) -> Int:
    return Int(v, 1, True)


def i16(v: int) -> Int:
    return Int(v, 2, True)


def i32(v: int) -> Int:
    return Int(v, 4, True)


AppMessageValue = int | str | bytes | bytearray | Int


def _encode_tuple(key: int, value: AppMessageValue) -> bytes:
    if isinstance(value, Int):
        fmt = {
            (1, False): "<B",
            (2, False): "<H",
            (4, False): "<I",
            (1, True): "<b",
            (2, True): "<h",
            (4, True): "<i",
        }[(value.width, value.signed)]
        raw = struct.pack(fmt, value.value)
        ttype = TupleType.INT if value.signed else TupleType.UINT
        return struct.pack("<IBH", key, int(ttype), len(raw)) + raw
    if isinstance(value, bool):
        msg = "bool is ambiguous; pass int"
        raise TypeError(msg)
    if isinstance(value, str):
        raw = value.encode("utf-8") + b"\x00"
        ttype = TupleType.CSTRING
    elif isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
        ttype = TupleType.BYTES
    elif isinstance(value, int):
        # Pick the smallest signed/unsigned width that fits.
        if value < 0:
            for fmt in ("<b", "<h", "<i"):
                try:
                    raw = struct.pack(fmt, value)
                    break
                except struct.error:
                    continue
            else:
                msg = f"int {value} too large for AppMessage"
                raise OverflowError(msg)
            ttype = TupleType.INT
        else:
            for fmt in ("<B", "<H", "<I"):
                try:
                    raw = struct.pack(fmt, value)
                    break
                except struct.error:
                    continue
            else:
                msg_0 = f"int {value} too large for AppMessage"
                raise OverflowError(msg_0)
            ttype = TupleType.UINT
    else:
        msg_1 = f"unsupported AppMessage value type: {type(value)}"
        raise TypeError(msg_1)
    return struct.pack("<IBH", key, int(ttype), len(raw)) + raw


def _decode_tuples(payload: bytes) -> dict[int, int | str | bytes]:
    out: dict[int, int | str | bytes] = {}
    off = 0
    while off + 7 <= len(payload):
        key, ttype, length = struct.unpack_from("<IBH", payload, off)
        off += 7
        raw = payload[off : off + length]
        off += length
        if ttype == TupleType.CSTRING:
            out[key] = raw.rstrip(b"\x00").decode("utf-8", "replace")
        elif ttype == TupleType.UINT:
            out[key] = int.from_bytes(raw, "little", signed=False)
        elif ttype == TupleType.INT:
            out[key] = int.from_bytes(raw, "little", signed=True)
        else:
            out[key] = raw
    return out


def build_app_message_push(
    transaction_id: int,
    app_uuid: str,
    data: dict[int, AppMessageValue],
) -> bytes:
    body = struct.pack("<BB", int(AppMessageCmd.PUSH), transaction_id & 0xFF)
    body += uuid_to_bytes(app_uuid)
    body += struct.pack("<B", len(data))
    for key, value in data.items():
        body += _encode_tuple(key, value)
    return body


def build_app_message_ack(transaction_id: int) -> bytes:
    return struct.pack("<BB", int(AppMessageCmd.ACK), transaction_id & 0xFF)


def parse_app_message(payload: bytes):
    """Returns (cmd, transaction_id, app_uuid|None, dict|None).

    cmd is an AppMessageCmd when recognized, else the raw int. The watch sends
    control/command bytes (e.g. 0x7F) we don't model; an unknown one must not
    crash the reader. Callers act only on PUSH/ACK/NACK and ignore the rest.
    """
    if len(payload) < 2:
        return None, None, None, None
    try:
        cmd = AppMessageCmd(payload[0])
    except ValueError:
        return payload[0], payload[1], None, None
    txn = payload[1]
    if cmd == AppMessageCmd.PUSH and len(payload) >= 19:
        app_uuid = str(_uuid.UUID(bytes=payload[2:18]))
        # payload[18] = tuple count; tuples follow.
        tuples = _decode_tuples(payload[19:])
        return cmd, txn, app_uuid, tuples
    return cmd, txn, None, None

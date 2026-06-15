"""AppMessage — the key/value dict protocol Pebble watchapps use. (verbatim)"""

from __future__ import annotations

import struct
import uuid as _uuid
from enum import IntEnum

from .protocol import uuid_to_bytes


class AppMessageCmd(IntEnum):
    PUSH = 0x01
    ACK = 0x02
    NACK = 0x03


# Some Pebble firmware/transport paths encode the AppMessage ACK/NACK with the
# high-bit-set bytes instead of 0x02/0x03 (verified against Gadgetbridge's
# PebbleProtocol: APPLICATIONMESSAGE_ACK = 0xff, APPLICATIONMESSAGE_NACK = 0x7f).
# We normalize both encodings to the same AppMessageCmd so callers only ever
# see ACK/NACK regardless of which the watch sends.
APPMESSAGE_ACK_ALT = 0xFF
APPMESSAGE_NACK_ALT = 0x7F


class TupleType(IntEnum):
    BYTES = 0
    CSTRING = 1
    UINT = 2
    INT = 3


class Int:
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
        raise TypeError("bool is ambiguous; pass int")
    if isinstance(value, str):
        raw = value.encode("utf-8") + b"\x00"
        ttype = TupleType.CSTRING
    elif isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
        ttype = TupleType.BYTES
    elif isinstance(value, int):
        if value < 0:
            for fmt in ("<b", "<h", "<i"):
                try:
                    raw = struct.pack(fmt, value)
                    break
                except struct.error:
                    continue
            else:
                raise OverflowError(f"int {value} too large for AppMessage")
            ttype = TupleType.INT
        else:
            for fmt in ("<B", "<H", "<I"):
                try:
                    raw = struct.pack(fmt, value)
                    break
                except struct.error:
                    continue
            else:
                raise OverflowError(f"int {value} too large for AppMessage")
            ttype = TupleType.UINT
    else:
        raise TypeError(f"unsupported AppMessage value type: {type(value)}")
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


def build_app_message_push(transaction_id: int, app_uuid: str, data: dict) -> bytes:
    body = struct.pack("<BB", int(AppMessageCmd.PUSH), transaction_id & 0xFF)
    body += uuid_to_bytes(app_uuid)
    body += struct.pack("<B", len(data))
    for key, value in data.items():
        body += _encode_tuple(key, value)
    return body


def build_app_message_ack(transaction_id: int) -> bytes:
    return struct.pack("<BB", int(AppMessageCmd.ACK), transaction_id & 0xFF)


def parse_app_message(payload: bytes):
    if len(payload) < 2:
        return None, None, None, None
    raw_cmd = payload[0]
    # Normalize the alternate high-bit ACK/NACK encoding (0xff/0x7f) that some
    # firmware uses, to the same AppMessageCmd as the 0x02/0x03 form.
    if raw_cmd == APPMESSAGE_ACK_ALT:
        return AppMessageCmd.ACK, payload[1], None, None
    if raw_cmd == APPMESSAGE_NACK_ALT:
        return AppMessageCmd.NACK, payload[1], None, None
    try:
        cmd = AppMessageCmd(payload[0])
    except ValueError:
        return payload[0], payload[1], None, None
    txn = payload[1]
    if cmd == AppMessageCmd.PUSH and len(payload) >= 19:
        app_uuid = str(_uuid.UUID(bytes=payload[2:18]))
        tuples = _decode_tuples(payload[19:])
        return cmd, txn, app_uuid, tuples
    return cmd, txn, None, None

"""Codec: AppMessage value dict <-> D-Bus wire shape (a{i(sv)}).

Each value is encoded as a (tag, payload) pair so the integer width survives
the D-Bus hop. Tag is one of: u8 u16 u32 i8 i16 i32 uint int str bytes.
"""

from __future__ import annotations

from typing import Any

from ._types import Int, i8, i16, i32, u8, u16, u32

WireValue = tuple[str, Any]

_WIDTH_BUILDERS = {
    "u8": u8,
    "u16": u16,
    "u32": u32,
    "i8": i8,
    "i16": i16,
    "i32": i32,
}

_INT_TO_TAG = {
    (1, False): "u8",
    (2, False): "u16",
    (4, False): "u32",
    (1, True): "i8",
    (2, True): "i16",
    (4, True): "i32",
}


def encode_value(value: int | str | bytes | bytearray | Int) -> WireValue:
    """Turn one outbound AppMessage value into a (tag, payload) wire pair."""
    if isinstance(value, Int):
        tag = _INT_TO_TAG.get((value.width, value.signed))
        if tag is None:  # pragma: no cover
            msg = f"unsupported Int width/sign: {value.width}/{value.signed}"
            raise ValueError(msg)
        bits = value.width * 8
        lo = -(1 << (bits - 1)) if value.signed else 0
        hi = (1 << (bits - 1)) - 1 if value.signed else (1 << bits) - 1
        if not (lo <= value.value <= hi):
            msg = f"{tag} value {value.value!r} out of range [{lo}, {hi}]"
            raise ValueError(msg)
        return tag, int(value.value)
    if isinstance(value, bool):
        msg = "bool is ambiguous; pass int or a width wrapper (u8/i16/…)"
        raise TypeError(msg)
    if isinstance(value, str):
        return "str", value
    if isinstance(value, (bytes, bytearray)):
        return "bytes", bytes(value)
    if isinstance(value, int):
        # Pebble AppMessage caps plain integers at 32 bits on the wire.
        if value < 0:
            if value < -(1 << 31):
                msg = f"int value {value!r} out of i32 range [-(2**31), 2**31-1]"
                raise ValueError(msg)
            return "int", value
        if value > (1 << 32) - 1:
            msg = f"uint value {value!r} out of u32 range [0, 2**32-1]"
            raise ValueError(msg)
        return "uint", value
    msg = f"unsupported AppMessage value type: {type(value)!r}"
    raise TypeError(msg)


def decode_value(wire: WireValue) -> int | str | bytes | Int:
    """Turn a (tag, payload) wire pair back into an AppMessage value."""
    tag, payload = wire
    builder = _WIDTH_BUILDERS.get(tag)
    if builder is not None:
        return builder(int(payload))
    if tag in ("uint", "int"):
        return int(payload)
    if tag == "str":
        return str(payload)
    if tag == "bytes":
        return bytes(payload)
    msg = f"unknown wire value tag: {tag!r}"
    raise ValueError(msg)


def encode_data_dict(data: dict[int, Any]) -> dict[int, WireValue]:
    """Outbound: {appkey: value} -> {appkey: (tag, payload)}."""
    return {int(k): encode_value(v) for k, v in data.items()}


def decode_data_dict(wire: dict[int, WireValue]) -> dict[int, int | str | bytes | Int]:
    """Inbound: {appkey: (tag, payload)} -> {appkey: value}."""
    return {int(k): decode_value(v) for k, v in wire.items()}

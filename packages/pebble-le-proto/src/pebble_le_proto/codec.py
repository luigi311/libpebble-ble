"""Codec: AppMessage value dict <-> a D-Bus-marshallable wire shape.

The problem this solves
-----------------------
libpebble_ble's AppMessage values are richer than D-Bus's generic variant can
round-trip. A value can be:

    int                      -> auto-width (smallest that fits)
    str                      -> CSTRING
    bytes                    -> BYTES
    Int(value, width, signed) via u8/u16/u32/i8/i16/i32  -> EXACT width

That last case is the whole reason `appmessage.Int` exists: a watchapp reads a
specific union member (e.g. t->value->uint16), so the phone must send exactly
that width. If we shoved a Python int through a bare D-Bus variant, the width
pin would be lost — the variant only knows "it's a number", and the daemon
would re-guess the width with the auto-encoder, which can mismatch what the app
reads.

The wire shape
--------------
We encode each value as a (tag, payload) struct so the width survives:

    D-Bus signature for one value:  (s v)     ->  WireValue = tuple[str, Any]
    D-Bus signature for the dict:   a{i(sv)}  ->  {key: (tag, payload)}

tag is one of: "u8" "u16" "u32" "i8" "i16" "i32" "uint" "int" "str" "bytes".
  - the six width tags carry a plain int payload and rebuild the exact Int()
  - "uint"/"int" carry a plain int and let the library auto-width (no pin)
  - "str" carries a string, "bytes" carries a bytes/bytearray

Decoding inbound (watch -> client): the library hands us plain int/str/bytes
(it does not preserve which width the watch used, and callers don't need it),
so we tag those as "uint"/"int"/"str"/"bytes".
"""

from __future__ import annotations

from typing import Any

from libpebble_ble import Int, i8, i16, i32, u8, u16, u32

# One value on the wire: (tag, payload). Payload type depends on the tag.
WireValue = tuple[str, Any]

# tag -> constructor that rebuilds the exact-width Int wrapper.
_WIDTH_BUILDERS = {
    "u8": u8,
    "u16": u16,
    "u32": u32,
    "i8": i8,
    "i16": i16,
    "i32": i32,
}

# (width, signed) -> tag, for encoding an Int back to its tag.
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
        if tag is None:  # pragma: no cover - Int only built via the wrappers
            msg = f"unsupported Int width/sign: {value.width}/{value.signed}"
            raise ValueError(msg)
        return tag, int(value.value)
    if isinstance(value, bool):
        # Mirror the library: bool is ambiguous, force the caller to be explicit.
        msg = "bool is ambiguous; pass int or a width wrapper (u8/i16/...)"
        raise TypeError(msg)
    if isinstance(value, str):
        return "str", value
    if isinstance(value, (bytes, bytearray)):
        return "bytes", bytes(value)
    if isinstance(value, int):
        # No width pin requested -> let the library auto-width on send.
        return ("int" if value < 0 else "uint"), value
    msg = f"unsupported AppMessage value type: {type(value)!r}"
    raise TypeError(msg)


def decode_value(wire: WireValue) -> int | str | bytes | Int:
    """Turn a (tag, payload) wire pair back into an AppMessage value.

    For the six width tags this rebuilds the exact-width Int so the daemon
    sends precisely what the caller pinned. For "uint"/"int" it returns a plain
    int (auto-width). For inbound (watch->client) traffic the tags are always
    the plain ones, so the client sees plain int/str/bytes — exactly what
    libpebble_ble's inbound handler would have delivered.
    """
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
    """Outbound: {appkey: value} -> {appkey: (tag, payload)} for a{i(sv)}."""
    return {int(k): encode_value(v) for k, v in data.items()}


def decode_data_dict(wire: dict[int, WireValue]) -> dict[int, int | str | bytes | Int]:
    """Inbound: {appkey: (tag, payload)} -> {appkey: value}."""
    return {int(k): decode_value(v) for k, v in wire.items()}

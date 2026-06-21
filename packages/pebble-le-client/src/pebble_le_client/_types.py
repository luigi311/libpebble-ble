"""AppMessage value types: Int and the u8/u16/u32/i8/i16/i32 width-pinning wrappers."""

from __future__ import annotations


class Int:
    """An integer with an explicit wire width and signedness.

    Use the convenience wrappers (u8, u16, …) rather than constructing directly.
    The width is used by the daemon to pick the correct Pebble union member when
    encoding the AppMessage; without it, the encoder auto-sizes and the watchapp
    may read the wrong field.
    """

    __slots__ = ("value", "width", "signed")

    def __init__(self, value: int, width: int, signed: bool) -> None:
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

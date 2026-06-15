"""Codec round-trip tests — the load-bearing invariant of the whole split.

If these pass, a width-pinned value (u16/i8/...) survives the daemon<->client
D-Bus hop and reaches libpebble_ble's encoder as the exact width the caller
asked for. If they ever fail, watchapps start reading the wrong union member.
"""

from libpebble_ble import Int, i8, i16, i32, u8, u16, u32
from pebble_le_proto import decode_data_dict, decode_value, encode_data_dict, encode_value


def test_width_pins_survive_round_trip():
    src = {
        0: "hello",
        1: u16(150),
        2: u8(7),
        3: u32(70000),
        4: i8(-3),
        5: i16(-1000),
        6: i32(-5000),
        7: b"\xde\xad\xbe\xef",
        8: 42,
        9: -3,
    }
    back = decode_data_dict(encode_data_dict(src))

    assert back[0] == "hello"
    for key, width, signed in [(1, 2, False), (2, 1, False), (3, 4, False),
                               (4, 1, True), (5, 2, True), (6, 4, True)]:
        v = back[key]
        assert isinstance(v, Int), f"key {key} lost its Int wrapper"
        assert v.width == width and v.signed == signed, f"key {key} width/sign drifted"
    assert back[3].value == 70000
    assert back[6].value == -5000
    assert back[7] == b"\xde\xad\xbe\xef"
    # plain ints stay plain so the library auto-widths them
    assert back[8] == 42 and not isinstance(back[8], Int)
    assert back[9] == -3 and not isinstance(back[9], Int)


def test_inbound_plain_values():
    # Inbound (watch -> client) values are plain int/str/bytes; they tag as
    # uint/int/str/bytes and decode back to plain Python types.
    assert decode_value(encode_value("x")) == "x"
    assert decode_value(encode_value(5)) == 5
    assert decode_value(encode_value(-5)) == -5
    assert decode_value(encode_value(b"\x00\x01")) == b"\x00\x01"


def test_bool_rejected():
    import pytest

    with pytest.raises(TypeError):
        encode_value(True)

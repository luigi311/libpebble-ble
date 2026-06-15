"""pebble_le — talk to a Pebble smartwatch over Bluetooth Low Energy from Linux.

This package:
  1. Connects to the watch as a GATT *client* (bleak), drives the LE pairing
     handshake the watch requires, negotiates MTU, and reads the connectivity
     characteristic (pebble.py).
  2. Spins up a GATT *server* / peripheral (BlueZ via D-Bus) exposing the
     PPoGATT service the watch connects back to for actual data transfer
     (gatt_server.py, framing shared via ppogatt.py).
  3. Wraps the Pebble Protocol (protocol.py) + AppMessage (appmessage.py) and
     gives you a dict-push API keyed by watchapp UUID, while answering the
     watch's PHONE_VERSION/PING keepalives so the session stays up.

Platform: Linux only. The peripheral (GATT server) role is not available to
Python on macOS/Windows. Requires a running BlueZ >= 5.48.

Protocol references (reverse-engineered, no official spec exists):
  * Gadgetbridge - PebbleGATTClient.java / PebbleLESupport (UUIDs, handshake)
  * Rebble mobile-app (PPoGATT window negotiation, packet types)
  * libpebble2 (Pebble Protocol endpoints, AppMessage dict serialization,
    PhoneAppVersion / Ping packet layouts)

Quick start
-----------
    import asyncio
    from pebble_le import Pebble

    async def main():
        APP_UUID = "00000000-0000-0000-0000-000000000000"  # your watchapp uuid

        async with Pebble("AA:BB:CC:DD:EE:FF") as pebble:   # watch BT address
            @pebble.on_app_message
            def handler(app_uuid, data):
                print("from watch:", app_uuid, data)

            await pebble.launch_app(APP_UUID)               # optional
            await pebble.send_app_message(APP_UUID, {0: "hello", 1: 42})
            await asyncio.sleep(60)

    asyncio.run(main())
"""

import sys

if sys.platform != "linux":
    msg = (
        "pebble_le requires Linux. The GATT server (peripheral) role used to "
        "talk to a Pebble over BLE is only available to Python on Linux/BlueZ."
    )
    raise RuntimeError(msg)

from .appmessage import (
    AppMessageCmd,
    Int,
    TupleType,
    i8,
    i16,
    i32,
    u8,
    u16,
    u32,
)
from .exceptions import PebbleNackError
from .pebble import AckHandler, AppMessageHandler, NackHandler, Pebble
from .protocol import AppRunStateCmd, Endpoint, TimeCmd

__version__ = "0.1.0"

__all__ = [
    "AckHandler",
    "AppMessageCmd",
    "AppMessageHandler",
    "AppRunStateCmd",
    "Endpoint",
    "Int",
    "NackHandler",
    "Pebble",
    "PebbleNackError",
    "TimeCmd",
    "TupleType",
    "__version__",
    "i8",
    "i16",
    "i32",
    "u8",
    "u16",
    "u32",
]

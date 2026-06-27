"""cobble_client — talk to your Pebble through the daemon, in plain Python.

An app never opens a BLE connection or touches D-Bus directly. It does:

    import asyncio
    from cobble_client import CobbleClient, u16

    async def main():
        async with CobbleClient() as cobble:
            @cobble.on_app_message
            def handler(app_uuid, data):
                print("from watch:", app_uuid, data)

            await cobble.send_app_message(
                "00000000-0000-0000-0000-000000000000",
                {0: "hello", 1: u16(150)},
            )
            await asyncio.sleep(60)

    asyncio.run(main())

The `on_app_message` decorator and the `{0: "hello", 1: u16(150)}` dict are the
SAME API libpebble_ble exposes to the daemon — that symmetry is deliberate, so
moving code between "in the daemon" and "an app talking to the daemon" is a
no-op at the call site.

The width wrappers (u8/u16/u32/i8/i16/i32) are re-exported here so apps don't
need to depend on libpebble-ble directly just to pin an integer width.
"""

from ._types import Int, i8, i16, i32, u8, u16, u32
from .client import CobbleClient, DaemonNotRunningError, NotConnectedError

__all__ = [
    "CobbleClient",
    "DaemonNotRunningError",
    "Int",
    "NotConnectedError",
    "i8",
    "i16",
    "i32",
    "u8",
    "u16",
    "u32",
]

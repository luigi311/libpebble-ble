"""Example app: send an AppMessage and print anything the watch sends back.

Run the daemon first:

    cobbled E6:94:0A:D4:D5:DC

Then run this:

    python -m cobble_client.examples.echo <app-uuid>

This is what every consuming app looks like: no BLE, no D-Bus, no pairing —
just the daemon's address-book and the same dict API libpebble_ble uses.
"""

from __future__ import annotations

import asyncio
import sys

from cobble_client import CobbleClient, DaemonNotRunningError, u16


async def main(app_uuid: str) -> None:
    try:
        async with CobbleClient() as cobble:
            if not await cobble.is_connected():
                print("daemon is up but the watch isn't connected yet; waiting...")

            @cobble.on_app_message
            def show(uuid: str, data: dict) -> None:
                print(f"<< {uuid}: {data}")

            @cobble.on_connection_changed
            def conn(connected: bool) -> None:
                print(f"** watch {'connected' if connected else 'disconnected'}")

            txn = await cobble.send_app_message(app_uuid, {0: "hello from an app", 1: u16(150)})
            print(f">> sent txn={txn}; listening 60s (Ctrl-C to stop)")
            await asyncio.sleep(60)
    except DaemonNotRunningError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python -m cobble_client.examples.echo <app-uuid>", file=sys.stderr)
        sys.exit(2)
    asyncio.run(main(sys.argv[1]))

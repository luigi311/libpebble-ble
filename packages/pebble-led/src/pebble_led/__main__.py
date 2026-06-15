"""Daemon entry point: `pebble-led <WATCH_ADDRESS>`.

Acquires the session bus, exports the PebbleDaemon interface, requests the
well-known name (org.pebble_le.Daemon) so clients can find it and check
liveness, opens the watch connection, and runs until signalled.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys

from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType
from loguru import logger
from pebble_le_proto import BUS_NAME, OBJECT_PATH, USE_SESSION_BUS

from .service import PebbleDaemon


async def _run(address: str, adapter: str) -> None:
    bus_type = BusType.SESSION if USE_SESSION_BUS else BusType.SYSTEM
    bus = await MessageBus(bus_type=bus_type).connect()

    daemon = PebbleDaemon(address, adapter=adapter)
    bus.export(OBJECT_PATH, daemon)

    # Request the well-known name. If another daemon already owns it, bail —
    # two daemons would fight over the single watch link.
    reply = await bus.request_name(BUS_NAME)
    from dbus_fast.constants import RequestNameReply

    if reply not in (RequestNameReply.PRIMARY_OWNER, RequestNameReply.ALREADY_OWNER):
        logger.error(
            f"could not acquire bus name {BUS_NAME} (reply={reply!r}); "
            f"is another pebble-led already running?"
        )
        bus.disconnect()
        sys.exit(1)

    logger.success(f"owning {BUS_NAME} at {OBJECT_PATH}")

    await daemon.start()

    # Run until SIGINT/SIGTERM.
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        await stop_event.wait()
    finally:
        logger.info("shutting down ...")
        await daemon.stop()
        bus.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="pebble-led",
        description="Long-lived daemon owning the Pebble BLE connection.",
    )
    parser.add_argument("address", help="watch BT address, e.g. E6:94:0A:D4:D5:DC")
    parser.add_argument("--adapter", default="hci0", help="HCI adapter (default hci0)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="TRACE" if args.verbose else "INFO")

    asyncio.run(_run(args.address, args.adapter))


if __name__ == "__main__":
    main()

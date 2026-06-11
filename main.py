"""CLI smoke test: python -m pebble_le (or the `pebble-le` console script).

Examples:
    python -m pebble_le --scan
    python -m pebble_le E6:94:0A:D4:D5:DC --app-uuid <uuid> --launch \
        --data 1=i:42 0="s:hello from linux" 2=b:deadbeef
"""

import argparse
import asyncio
import sys

from loguru import logger
from src.libpebble_ble import Pebble


def _parse_kv(s: str):
    """Parse 'KEY=VALUE' into (int_key, typed_value).
    VALUE is int if it looks numeric, else string. Prefix with 'b:' for
    raw bytes (hex), 's:' to force string, 'i:' to force int.
    """
    key_s, _, val = s.partition("=")
    key = int(key_s, 0)
    if val.startswith("b:"):
        return key, bytes.fromhex(val[2:])
    if val.startswith("s:"):
        return key, val[2:]
    if val.startswith("i:"):
        return key, int(val[2:], 0)
    try:
        return key, int(val, 0)
    except ValueError:
        return key, val


def main():
    parser = argparse.ArgumentParser(
        prog="pebble-le",
        description="pebble_le — send an AppMessage to a Pebble watchapp",
    )
    parser.add_argument("address", nargs="?", help="watch BT address")
    parser.add_argument("--scan", action="store_true", help="list nearby Pebbles")
    parser.add_argument(
        "--app-uuid",
        default="00000000-0000-0000-0000-000000000000",
        help="target watchapp UUID",
    )
    parser.add_argument(
        "--launch",
        action="store_true",
        help="ask the watch to launch the app before sending",
    )
    parser.add_argument(
        "--data",
        nargs="*",
        default=["0=hello from linux", "1=1"],
        metavar="KEY=VALUE",
        help="AppMessage tuples, e.g. 0=hi 1=42 2=b:deadbeef",
    )
    parser.add_argument(
        "--listen",
        type=float,
        default=60.0,
        help="seconds to listen for inbound messages",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    configure_logger(args.verbose)

    async def _main():
        if args.scan:
            logger.debug("scanning...")
            for addr, name in await Pebble.scan():
                logger.debug(f"  {addr}  {name}")
            return
        if not args.address:
            parser.error("address required (or use --scan)")

        data = dict(_parse_kv(item) for item in args.data)

        async with Pebble(args.address) as pebble:

            @pebble.on_app_message
            def show(app_uuid, data):
                logger.debug(f"<< {app_uuid}: {data}")

            if args.launch:
                await pebble.launch_app(args.app_uuid)
                await asyncio.sleep(1.0)  # give the app a moment to open

            txn = await pebble.send_app_message(args.app_uuid, data)
            logger.debug(f"sent AppMessage txn={txn} to {args.app_uuid}: {data}")
            logger.debug(f"listening {args.listen:.0f}s for replies (Ctrl-C to stop)")
            try:
                await asyncio.sleep(args.listen)
            except KeyboardInterrupt:
                pass

    asyncio.run(_main())


def configure_logger(verbose: bool) -> None:
    # Remove default logger to configure our own
    logger.remove()

    # Add a sink for file logging and the console.
    logger.add(sys.stdout, level="TRACE" if verbose else "INFO")


if __name__ == "__main__":
    main()

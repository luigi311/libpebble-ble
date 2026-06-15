"""The daemon: a dbus-fast ServiceInterface wrapping one libpebble_ble.Pebble.

Interface (org.pebble_le.Daemon on /org/pebble_le/Daemon):

  Properties
    Connected     b    watch BLE link is up right now
    WatchAddress  s    configured watch address ("" if none)

  Methods
    SendAppMessage(s app_uuid, a{i(sv)} data) -> u txn
        Push a key/value dict to a watchapp. Returns the transaction id.
        Width pins (u8/u16/...) survive via the proto codec's (tag,payload).
    LaunchApp(s app_uuid)
    StopApp(s app_uuid)
    UpdateTime()
            Sync the watch's clock to the daemon host's current local time.
    Ping() -> b
        Cheap "are you really responding" probe (distinct from name-has-owner,
        which only proves the process exists).

  Signals
    AppMessageReceived(s app_uuid, a{i(sv)} data)   inbound PUSH from a watchapp
    AckReceived(u txn)                              watch ACKed one of our sends
    NackReceived(u txn)                             watch NACKed one of our sends
    ConnectionChanged(b connected)                  BLE link came up / went down

Liveness for clients is two independent questions, both answered here:
  * "is the daemon process alive?"  -> the bus name has an owner (client side).
  * "is the watch reachable?"       -> the Connected property / ConnectionChanged.
"""

from __future__ import annotations

import asyncio

from dbus_fast import Variant
from dbus_fast.service import (
    PropertyAccess,
    ServiceInterface,
    dbus_property,
    method,
    signal,
)
from libpebble_ble import Pebble
from loguru import logger
from pebble_le_proto import INTERFACE, decode_data_dict, encode_data_dict

from .notify_monitor import NotificationMonitor


class PebbleDaemon(ServiceInterface):
    """D-Bus front end over a single Pebble connection.

    The daemon owns reconnection: if the watch drops, it keeps trying to bring
    the link back up, and flips the Connected property (+ ConnectionChanged
    signal) so clients can react. Apps never touch BLE; they only ever see this
    interface.
    """

    def __init__(self, address: str, adapter: str = "hci0") -> None:
        super().__init__(INTERFACE)
        self._address = address
        self._adapter = adapter
        self._pebble: Pebble | None = None
        self._connected = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._reconnect_task: asyncio.Task | None = None
        self._stopping = False
        self._notify_monitor: NotificationMonitor | None = None
        # apps whose notifications are pure noise on a watch
        self._notify_blocklist = {""}

    # ------------------------------------------------------------------ #
    # Lifecycle (called by __main__, not over D-Bus)
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        """Open the BLE connection and start the reconnect supervisor."""
        self._loop = asyncio.get_running_loop()
        self._reconnect_task = self._loop.create_task(self._supervise())

        # Start the desktop-notification monitor. It runs independently of the
        # watch link; forwards are dropped while the watch is disconnected.
        self._notify_monitor = NotificationMonitor(self._on_desktop_notification)
        try:
            await self._notify_monitor.start()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"could not start notification monitor: {e!r}")
            self._notify_monitor = None

    async def stop(self) -> None:
        self._stopping = True
        if self._notify_monitor is not None:
            await self._notify_monitor.stop()
            self._notify_monitor = None
        if self._reconnect_task is not None:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
        if self._pebble is not None:
            try:
                await self._pebble.disconnect()
            except Exception as e:  # noqa: BLE001
                logger.debug(f"pebble disconnect during stop: {e!r}")
            self._pebble = None

    async def _supervise(self) -> None:
        """Keep a live connection to the watch, reconnecting on drop.

        The watch routinely drops the first attempt and reconnects on its own
        schedule; libpebble_ble.connect() already retries the transient
        failures internally, so here we just loop with a backoff around the
        whole connect+run, and re-attach our handlers each time.
        """
        backoff = 2.0
        while not self._stopping:
            try:
                logger.info(f"connecting to watch {self._address} ...")
                pebble = Pebble(self._address, adapter=self._adapter)
                await pebble.connect()
                self._pebble = pebble
                self._wire_handlers(pebble)
                self._set_connected(True)
                backoff = 2.0
                logger.success("watch connected; daemon ready")

                try:
                    await pebble.update_time()
                    logger.info("watch time synchronized")
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"time sync on connect failed: {e!r}")

                # Park here until the link drops. We detect drop by polling the
                # library's internal connected event; when it clears, we fall
                # through to reconnect. (A future libpebble_ble disconnect
                # callback would let us await instead of poll.)
                while not self._stopping and pebble._connected.is_set():
                    await asyncio.sleep(1.0)

                logger.warning("watch link went down")
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.warning(f"connection attempt failed: {e!r}")
            finally:
                if self._pebble is not None:
                    try:
                        await self._pebble.disconnect()
                    except Exception:  # noqa: BLE001, S110
                        pass
                    self._pebble = None
                self._set_connected(False)

            if self._stopping:
                break
            logger.debug(f"reconnecting in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    def _wire_handlers(self, pebble: Pebble) -> None:
        """Attach inbound dispatch from the watch to our D-Bus signals."""

        @pebble.on_app_message
        def _on_msg(app_uuid: str, data: dict) -> None:
            wire = encode_data_dict(data)
            # Signals must be emitted on the bus thread/loop; we're already on
            # the asyncio loop the bus runs on, so a direct call is fine.
            self.AppMessageReceived(app_uuid, wire)

        @pebble.on_ack
        def _on_ack(txn: int) -> None:
            self.AckReceived(txn & 0xFFFFFFFF)

        @pebble.on_nack
        def _on_nack(txn: int) -> None:
            self.NackReceived(txn & 0xFFFFFFFF)

    def _set_connected(self, value: bool) -> None:
        if value == self._connected:
            return
        self._connected = value
        # Property-changed notification for `Connected`, plus our explicit signal.
        self.emit_properties_changed({"Connected": value})
        self.ConnectionChanged(value)

    async def _on_desktop_notification(self, app_name: str, summary: str, body: str) -> None:
        if self._pebble is None or not self._connected:
            logger.debug(f"watch down; dropping notification from {app_name!r}")
            return
        if app_name.lower() in self._notify_blocklist:
            logger.debug(f"filtered notification from {app_name!r}")
            return
        if not summary and not body:
            return  # empty/progress-only notifications
        await self._pebble.send_notification(summary, body, subtitle=app_name)

    # ------------------------------------------------------------------ #
    # D-Bus properties
    # ------------------------------------------------------------------ #
    @dbus_property(access=PropertyAccess.READ)
    def Connected(self) -> "b":  # noqa: N802, F821
        return self._connected

    @dbus_property(access=PropertyAccess.READ)
    def WatchAddress(self) -> "s":  # noqa: N802, F821
        return self._address or ""

    # ------------------------------------------------------------------ #
    # D-Bus methods
    # ------------------------------------------------------------------ #
    @method()
    async def SendAppMessage(  # noqa: N802
        self,
        app_uuid: "s",
        data: "a{i(sv)}",
        wait_ack: "b",  # noqa: F821
    ) -> "u":  # noqa: F821
        pebble = self._require_connected()
        # data arrives as {int: (tag, Variant)}; unwrap variants then decode.
        unwrapped = {k: (tag, v.value) for k, (tag, v) in data.items()}
        decoded = decode_data_dict(unwrapped)
        # When wait_ack is set, this coroutine doesn't return until the watch
        # ACKs/NACKs — that's what lets a caller self-throttle to the watch's
        # real rate instead of outrunning it. raise_on_timeout stays False so a
        # missed ACK degrades to "returned anyway" rather than erroring.
        txn = await pebble.send_app_message(app_uuid, decoded, wait_ack=wait_ack)
        logger.debug(f"D-Bus SendAppMessage uuid={app_uuid} wait_ack={wait_ack} -> txn={txn}")
        return txn & 0xFFFFFFFF

    @method()
    async def LaunchApp(self, app_uuid: "s"):  # noqa: N802, F821
        await self._require_connected().launch_app(app_uuid)

    @method()
    async def StopApp(self, app_uuid: "s"):  # noqa: N802, F821
        await self._require_connected().stop_app(app_uuid)

    @method()
    async def UpdateTime(self):  # noqa: N802
        await self._require_connected().update_time()

    @method()
    async def Notify(self, title: "s", body: "s", subtitle: "s") -> "u":  # noqa: N802, F821
        return await self._require_connected().send_notification(title, body, subtitle)

    @method()
    def Ping(self) -> "b":  # noqa: N802, F821
        return True

    # ------------------------------------------------------------------ #
    # D-Bus signals
    # ------------------------------------------------------------------ #
    @signal()
    def AppMessageReceived(  # noqa: N802
        self, app_uuid: str, data: dict
    ) -> "sa{i(sv)}":  # noqa: F821
        # dbus-fast reads the return annotation as the signal signature and the
        # returned value as the body. Wrap each payload back into a Variant.
        wired = {k: (tag, _wrap(tag, payload)) for k, (tag, payload) in data.items()}
        return [app_uuid, wired]

    @signal()
    def AckReceived(self, txn: int) -> "u":  # noqa: N802, F821
        return txn & 0xFFFFFFFF

    @signal()
    def NackReceived(self, txn: int) -> "u":  # noqa: N802, F821
        return txn & 0xFFFFFFFF

    @signal()
    def ConnectionChanged(self, connected: bool) -> "b":  # noqa: N802, F821
        return connected

    # ------------------------------------------------------------------ #
    def _require_connected(self) -> Pebble:
        if self._pebble is None or not self._connected:
            from dbus_fast import DBusError

            msg = "watch is not connected"
            raise DBusError(f"{INTERFACE}.NotConnected", msg)
        return self._pebble


def _wrap(tag: str, payload) -> Variant:
    """Wrap a decoded payload into the D-Bus Variant the (s v) struct needs."""
    if tag in ("u8", "u16", "u32", "uint"):
        return Variant("u", int(payload) & 0xFFFFFFFF)
    if tag in ("i8", "i16", "i32", "int"):
        return Variant("i", int(payload))
    if tag == "str":
        return Variant("s", str(payload))
    if tag == "bytes":
        return Variant("ay", bytes(payload))
    msg = f"cannot wrap unknown tag {tag!r}"
    raise ValueError(msg)

"""PebbleClient — the D-Bus proxy wearing libpebble_ble's API.

Everything D-Bus (bus name, object path, proxy setup, variant marshalling,
the (tag,payload) wire encoding) is hidden behind this class. Apps see methods
that take the same dicts libpebble_ble takes and decorators with the same
shapes its handlers have.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from dbus_fast import DBusError, Variant
from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType

from ._codec import decode_data_dict, encode_data_dict

BUS_NAME = "org.pebble_le.Daemon"
OBJECT_PATH = "/org/pebble_le/Daemon"
INTERFACE = "org.pebble_le.Daemon"
USE_SESSION_BUS = True

# Same handler shapes libpebble_ble uses, so code reads identically either side.
AppMessageHandler = Callable[[str, dict], None]
AckHandler = Callable[[int], None]
NackHandler = Callable[[int], None]
ConnectionHandler = Callable[[bool], None]

_DBUS = "org.freedesktop.DBus"
_DBUS_PATH = "/org/freedesktop/DBus"


class DaemonNotRunningError(RuntimeError):
    """The pebble-led daemon is not running (its bus name has no owner)."""


class NotConnectedError(RuntimeError):
    """The daemon is running but its BLE link to the watch is down."""


class PebbleClient:
    """Async client for the pebble-led daemon.

    Usage as a context manager (recommended):

        async with PebbleClient() as pebble:
            await pebble.send_app_message(uuid, {0: "hi"})

    or manually:

        pebble = PebbleClient()
        await pebble.connect()
        ...
        await pebble.close()
    """

    def __init__(self) -> None:
        self._bus: MessageBus | None = None
        self._iface = None  # proxy interface for org.pebble_le.Daemon
        self._dbus_iface = None  # proxy for org.freedesktop.DBus (NameHasOwner)
        self._msg_handlers: list[AppMessageHandler] = []
        self._ack_handlers: list[AckHandler] = []
        self._nack_handlers: list[NackHandler] = []
        self._conn_handlers: list[ConnectionHandler] = []

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    async def __aenter__(self) -> "PebbleClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def connect(self, *, require_daemon: bool = True) -> None:
        """Connect to the session bus and bind to the daemon's interface.

        If require_daemon is True (default) and the daemon isn't running, raise
        DaemonNotRunningError immediately rather than blocking. Set it False if
        you want to bind anyway and rely on D-Bus activation to start the
        daemon on the first method call.
        """
        bus_type = BusType.SESSION if USE_SESSION_BUS else BusType.SYSTEM
        self._bus = await MessageBus(bus_type=bus_type).connect()

        # Bind the freedesktop bus driver so we can do liveness checks.
        introspect = await self._bus.introspect(_DBUS, _DBUS_PATH)
        obj = self._bus.get_proxy_object(_DBUS, _DBUS_PATH, introspect)
        self._dbus_iface = obj.get_interface(_DBUS)

        if require_daemon and not await self.is_daemon_running():
            await self.close()
            msg = (
                f"the pebble-led daemon ({BUS_NAME}) is not running. Start it "
                f"with `pebble-led <watch-address>` or enable its service."
            )
            raise DaemonNotRunningError(msg)

        introspect = await self._bus.introspect(BUS_NAME, OBJECT_PATH)
        proxy = self._bus.get_proxy_object(BUS_NAME, OBJECT_PATH, introspect)
        self._iface = proxy.get_interface(INTERFACE)

        # Bridge daemon signals -> our local handler lists.
        self._iface.on_app_message_received(self._dispatch_app_message)
        self._iface.on_ack_received(self._dispatch_ack)
        self._iface.on_nack_received(self._dispatch_nack)
        self._iface.on_connection_changed(self._dispatch_connection)

    async def close(self) -> None:
        bus, self._bus = self._bus, None
        self._iface = None
        self._dbus_iface = None
        if bus is not None:
            bus.disconnect()

    # ------------------------------------------------------------------ #
    # liveness
    # ------------------------------------------------------------------ #
    async def is_daemon_running(self) -> bool:
        """True if the daemon process is alive (its bus name has an owner).

        This is the cheap, race-free liveness check: no socket connect, no
        timeout, no stale pidfile. Distinct from `connected` below, which asks
        whether the *watch* is reachable.
        """
        if self._dbus_iface is None:
            msg = "call connect() first"
            raise RuntimeError(msg)
        return await self._dbus_iface.call_name_has_owner(BUS_NAME)

    @property
    def connected(self) -> bool:
        """Whether the daemon currently has a live BLE link to the watch.

        Reads the cached `Connected` property; dbus-fast keeps proxy properties
        updated from PropertiesChanged, so this reflects the latest signalled
        value without a round trip.
        """
        if self._iface is None:
            return False
        # dbus-fast exposes a cached getter; fall back to False if unavailable.
        getter = getattr(self._iface, "get_connected", None)
        if getter is None:
            return False
        try:
            return bool(getter())  # cached, synchronous
        except Exception:  # noqa: BLE001
            return False

    async def is_connected(self) -> bool:
        """Authoritative check of the watch link via a fresh property read."""
        self._require_iface()
        return bool(await self._iface.get_connected())

    # ------------------------------------------------------------------ #
    # methods
    # ------------------------------------------------------------------ #
    async def send_app_message(self, app_uuid: str, data: dict, *, wait_ack: bool = False) -> int:
        """Push a key/value dict to a watchapp. Returns the transaction id.

        Same signature and same value types as libpebble_ble.send_app_message:
        ints, str, bytes, or u8/u16/u32/i8/i16/i32 width wrappers.

        If wait_ack is True, the call does not return until the watch has
        ACKed/NACKed the message. Use this to self-throttle a stream of updates
        to the watch's real receive rate: with one send in flight at a time you
        can't outrun the watch and build a backlog.
        """
        self._require_iface()
        wire = encode_data_dict(data)
        # Wrap each (tag, payload) payload into the Variant the a{i(sv)} wants.
        marshalled = {k: [tag, _wrap(tag, payload)] for k, (tag, payload) in wire.items()}
        try:
            return await self._iface.call_send_app_message(app_uuid, marshalled, wait_ack)
        except DBusError as e:
            raise self._translate(e) from e

    async def launch_app(self, app_uuid: str) -> None:
        self._require_iface()
        try:
            await self._iface.call_launch_app(app_uuid)
        except DBusError as e:
            raise self._translate(e) from e

    async def stop_app(self, app_uuid: str) -> None:
        self._require_iface()
        try:
            await self._iface.call_stop_app(app_uuid)
        except DBusError as e:
            raise self._translate(e) from e

    async def ping_daemon(self) -> bool:
        """Round-trip probe that the daemon is actually servicing calls."""
        self._require_iface()
        return await self._iface.call_ping()

    async def push_weather(
        self,
        location_name: str,
        current_temp: int,
        current_weather: int,
        today_high: int,
        today_low: int,
        tomorrow_weather: int,
        tomorrow_high: int,
        tomorrow_low: int,
        forecast_short: str = "",
        *,
        is_current_location: bool = True,
        location_key: bytes | None = None,
    ) -> None:
        """Push weather data to the Pebble built-in weather app.

        location_key: 16-byte UUID identifying the location entry. Re-use the
            same bytes to update an existing entry. Defaults to a fixed UUID for
            the current-location slot.
        current_weather / tomorrow_weather: 0=PartlyCloudy, 1=CloudyDay,
            2=LightSnow, 3=LightRain, 4=HeavyRain, 5=HeavySnow, 6=Generic,
            7=Sun, 8=RainAndSnow, 255=Unknown.
        Temperatures are in Celsius.
        """
        self._require_iface()
        if location_key is None:
            # Fixed UUID for the single "current weather" slot so repeated calls
            # update the same entry rather than creating duplicates on the watch.
            location_key = bytes.fromhex("e4c75d6ae95f4b778c3251a1c2b1d5c4")
        if len(location_key) != 16:
            msg = f"location_key must be 16 bytes, got {len(location_key)}"
            raise ValueError(msg)
        try:
            await self._iface.call_push_weather(
                bytes(location_key),
                location_name,
                forecast_short,
                current_temp,
                current_weather,
                today_high,
                today_low,
                tomorrow_weather,
                tomorrow_high,
                tomorrow_low,
                is_current_location,
            )
        except DBusError as e:
            raise self._translate(e) from e

    # ------------------------------------------------------------------ #
    # handler registration (mirrors libpebble_ble's decorators)
    # ------------------------------------------------------------------ #
    def on_app_message(self, fn: AppMessageHandler) -> AppMessageHandler:
        self._msg_handlers.append(fn)
        return fn

    def on_ack(self, fn: AckHandler) -> AckHandler:
        self._ack_handlers.append(fn)
        return fn

    def on_nack(self, fn: NackHandler) -> NackHandler:
        self._nack_handlers.append(fn)
        return fn

    def on_connection_changed(self, fn: ConnectionHandler) -> ConnectionHandler:
        self._conn_handlers.append(fn)
        return fn

    # ------------------------------------------------------------------ #
    # signal dispatch (D-Bus -> local handlers)
    # ------------------------------------------------------------------ #
    def _dispatch_app_message(self, app_uuid: str, data: dict) -> None:
        # data is {int: [tag, Variant]}; unwrap variants, then decode to the
        # plain int/str/bytes an app expects.
        unwrapped = {k: (tag, v.value) for k, (tag, v) in data.items()}
        decoded = decode_data_dict(unwrapped)
        for h in self._msg_handlers:
            _safe(h, app_uuid, decoded)

    def _dispatch_ack(self, txn: int) -> None:
        for h in self._ack_handlers:
            _safe(h, txn)

    def _dispatch_nack(self, txn: int) -> None:
        for h in self._nack_handlers:
            _safe(h, txn)

    def _dispatch_connection(self, connected: bool) -> None:
        for h in self._conn_handlers:
            _safe(h, connected)

    # ------------------------------------------------------------------ #
    def _require_iface(self):
        if self._iface is None:
            msg = "not connected to the daemon; call connect() first"
            raise RuntimeError(msg)
        return self._iface

    @staticmethod
    def _translate(e: DBusError) -> Exception:
        if e.type and e.type.endswith(".NotConnected"):
            return NotConnectedError("watch is not connected")
        return e


def _wrap(tag: str, payload) -> Variant:
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


def _safe(fn, *args) -> None:
    """Run a user handler, swallowing exceptions so one bad handler can't kill
    the signal pump. (Mirrors libpebble_ble's try/except around handlers.)"""
    try:
        result = fn(*args)
        if asyncio.iscoroutine(result):
            asyncio.ensure_future(result)  # noqa: RUF006
    except Exception:  # noqa: BLE001
        import traceback

        traceback.print_exc()

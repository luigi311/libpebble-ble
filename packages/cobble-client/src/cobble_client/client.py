"""CobbleClient — the D-Bus proxy wearing libpebble_ble's API.

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

BUS_NAME = "org.cobble.Daemon"
OBJECT_PATH = "/org/cobble/Daemon"
INTERFACE = "org.cobble.Daemon"
USE_SESSION_BUS = True

# Same handler shapes libpebble_ble uses, so code reads identically either side.
AppMessageHandler = Callable[[str, dict], None]
AckHandler = Callable[[int], None]
NackHandler = Callable[[int], None]
ConnectionHandler = Callable[[bool], None]
# tag, app_uuid, session_timestamp, items_left, crc, item_type, item_size, data
HealthDataHandler = Callable[[int, bytes, int, int, int, int, int, bytes], None]
# the decoded health profile dict (see CobbleClient.HEALTH_PROFILE_FIELDS)
HealthProfileHandler = Callable[[dict], None]
# key, value (bool / int / str)
WatchSettingHandler = Callable[[str, object], None]

_DBUS = "org.freedesktop.DBus"
_DBUS_PATH = "/org/freedesktop/DBus"


class DaemonNotRunningError(RuntimeError):
    """The cobbled daemon is not running (its bus name has no owner)."""


class NotConnectedError(RuntimeError):
    """The daemon is running but its BLE link to the watch is down."""


class CobbleClient:
    """Async client for the cobbled daemon.

    Usage as a context manager (recommended):

        async with CobbleClient() as cobble:
            await cobble.send_app_message(uuid, {0: "hi"})

    or manually:

        cobble = CobbleClient()
        await cobble.connect()
        ...
        await cobble.close()
    """

    def __init__(self) -> None:
        self._bus: MessageBus | None = None
        self._iface = None  # proxy interface for org.cobble.Daemon
        self._dbus_iface = None  # proxy for org.freedesktop.DBus (NameHasOwner)
        self._msg_handlers: list[AppMessageHandler] = []
        self._ack_handlers: list[AckHandler] = []
        self._nack_handlers: list[NackHandler] = []
        self._conn_handlers: list[ConnectionHandler] = []
        self._health_handlers: list[HealthDataHandler] = []
        self._health_profile_handlers: list[HealthProfileHandler] = []
        self._watch_setting_handlers: list[WatchSettingHandler] = []

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    async def __aenter__(self) -> "CobbleClient":
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
                f"the cobbled daemon ({BUS_NAME}) is not running. Start it "
                f"with `cobbled` or enable its systemd user service."
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
        self._iface.on_health_data_received(self._dispatch_health_data)
        self._iface.on_health_profile_received(self._dispatch_health_profile)
        self._iface.on_watch_setting_received(self._dispatch_watch_setting)

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

    async def update_time(self) -> None:
        """Sync the watch clock to the current system time."""
        self._require_iface()
        try:
            await self._iface.call_update_time()
        except DBusError as e:
            raise self._translate(e) from e

    async def notify(self, title: str, body: str, subtitle: str = "") -> int:
        """Send a notification to the watch. Returns the BlobDB token.

        subtitle is shown as the sender/app name and is used for icon selection.
        """
        self._require_iface()
        try:
            return await self._iface.call_notify(title, body, subtitle)
        except DBusError as e:
            raise self._translate(e) from e

    async def scan(self, timeout_secs: float = 10.0) -> list[tuple[str, str]]:
        """Scan for nearby Pebble devices. Returns [(address, name)] pairs."""
        self._require_iface()
        try:
            return await self._iface.call_scan(timeout_secs)
        except DBusError as e:
            raise self._translate(e) from e

    async def activate_health(
        self,
        height_cm: int,
        weight_kg: int,
        age: int,
        gender: int,
        *,
        hrm_enabled: bool = False,
    ) -> None:
        """Write the health user profile to the watch and trigger a DataLog sync.

        gender: 0 = female, 1 = male, 2 = other (libpebble3 HealthGender).
        """
        self._require_iface()
        try:
            await self._iface.call_activate_health(height_cm, weight_kg, age, gender, hrm_enabled)
        except DBusError as e:
            raise self._translate(e) from e

    async def fetch_health_data(self) -> None:
        """Ask the watch to flush pending health records via DataLog sessions."""
        self._require_iface()
        try:
            await self._iface.call_fetch_health_data()
        except DBusError as e:
            raise self._translate(e) from e

    async def fetch_health_params(self) -> None:
        """Ask the watch to re-sync its health/watch settings (WatchPrefs)."""
        self._require_iface()
        try:
            await self._iface.call_fetch_health_params()
        except DBusError as e:
            raise self._translate(e) from e

    async def get_health_profile(self) -> dict:
        """Return the watch's health profile as a dict keyed by field name.

        Keys: height_cm, weight_kg, age, gender, tracking/insight flags, HRM
        settings, heart-rate zones, imperial_units. Raises if no profile has
        synced yet (call fetch_health_params first).
        """
        self._require_iface()
        try:
            raw = await self._iface.call_get_health_profile()
        except DBusError as e:
            raise self._translate(e) from e
        return {k: _unwrap(v) for k, v in raw.items()}

    async def get_watch_settings(self) -> dict:
        """Return all decoded general watch settings (db 12) as a key -> value dict.

        Values are plain bool / int / str. Empty until the watch syncs settings
        on connect; call fetch_health_params to force a re-sync.
        """
        self._require_iface()
        try:
            raw = await self._iface.call_get_watch_settings()
        except DBusError as e:
            raise self._translate(e) from e
        return {k: _unwrap(v) for k, v in raw.items()}

    async def get_watch_version(self) -> dict:
        """Return the watch's version info as a dict.

        Keys: firmware_version/major/minor/patch/suffix/git_hash, is_recovery,
        recovery_version (if present), board, serial, bt_address, bootloader/
        resource timestamps, language(+version), hardware_platform,
        platform_revision, watch_type, capabilities, is_unfaithful, and
        health_insights_version/javascript_version (if present).
        """
        self._require_iface()
        try:
            raw = await self._iface.call_get_watch_version()
        except DBusError as e:
            raise self._translate(e) from e
        return {k: _unwrap(v) for k, v in raw.items()}

    async def get_watch_color(self) -> dict:
        """Return the watch's color/variant as a dict.

        Keys: protocol_number, js_name, description, watch_type, supports_hrm.
        Raises if the watch reports an error or an unknown color.
        """
        self._require_iface()
        try:
            raw = await self._iface.call_get_watch_color()
        except DBusError as e:
            raise self._translate(e) from e
        return {k: _unwrap(v) for k, v in raw.items()}

    async def reboot_watch(self) -> None:
        """Reboot the watch. It drops the link and the daemon reconnects."""
        self._require_iface()
        try:
            await self._iface.call_reboot_watch()
        except DBusError as e:
            raise self._translate(e) from e

    async def reset_into_recovery(self) -> None:
        """Reboot the watch into its recovery (PRF) firmware."""
        self._require_iface()
        try:
            await self._iface.call_reset_into_recovery()
        except DBusError as e:
            raise self._translate(e) from e

    async def create_core_dump(self) -> None:
        """Trigger a core dump on the watch."""
        self._require_iface()
        try:
            await self._iface.call_create_core_dump()
        except DBusError as e:
            raise self._translate(e) from e

    async def factory_reset(self) -> None:
        """Factory-reset the watch. DESTRUCTIVE: wipes all data and unpairs."""
        self._require_iface()
        try:
            await self._iface.call_factory_reset()
        except DBusError as e:
            raise self._translate(e) from e

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

    def on_health_data(self, fn: HealthDataHandler) -> HealthDataHandler:
        self._health_handlers.append(fn)
        return fn

    def on_health_profile(self, fn: HealthProfileHandler) -> HealthProfileHandler:
        self._health_profile_handlers.append(fn)
        return fn

    def on_watch_setting(self, fn: WatchSettingHandler) -> WatchSettingHandler:
        self._watch_setting_handlers.append(fn)
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

    def _dispatch_health_data(
        self,
        tag: int,
        app_uuid: bytes,
        session_timestamp: int,
        items_left: int,
        crc: int,
        item_type: int,
        item_size: int,
        data: bytes,
    ) -> None:
        for h in self._health_handlers:
            _safe(h, tag, app_uuid, session_timestamp, items_left, crc, item_type, item_size, data)

    def _dispatch_health_profile(self, profile) -> None:
        decoded = {k: _unwrap(v) for k, v in profile.items()}
        for h in self._health_profile_handlers:
            _safe(h, decoded)

    def _dispatch_watch_setting(self, key: str, value) -> None:
        unwrapped = _unwrap(value)
        for h in self._watch_setting_handlers:
            _safe(h, key, unwrapped)

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


def _unwrap(value):
    """Unwrap a dbus_fast Variant to its plain Python value (recursively)."""
    if isinstance(value, Variant):
        return _unwrap(value.value)
    return value


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

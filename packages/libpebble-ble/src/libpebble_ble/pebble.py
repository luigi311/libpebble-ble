"""High-level Pebble connection: lifecycle, pairing, endpoint dispatch, AppMessage API.

This is the class users interact with. It owns:
  * the bleak GATT *client* connection used for the fed9 pairing/connectivity
    handshake, and
  * the phone-hosted PPoGATT GATT *server* (gatt_server.PebbleGattServer) the
    watch connects back to for actual data transfer,
and routes inbound Pebble Protocol messages by endpoint — answering the
watch's PHONE_VERSION and PING keepalives itself and fanning AppMessages out
to registered handlers.

Pairing: connect() handles first-time bonding itself. It registers a
temporary auto-accept BlueZ agent (agent.PairingAgent), pokes the watch's
pairing-trigger characteristic so the WATCH initiates bonding (the
Gadgetbridge flow — the human confirms on the watch screen), falls back to
host-initiated Pair() if the watch stays quiet, and on AuthenticationFailed
removes the (stale) BlueZ bond and retries once from scratch. After bonding
the device is marked Trusted so BlueZ lets the watch reconnect to our GATT
server unprompted.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timezone

from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from dbus_fast import Variant
from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType
from loguru import logger

from . import appmessage, protocol
from .agent import PairingAgent
from .appmessage import AppMessageCmd, AppMessageValue
from .exceptions import PebbleNackError
from .gatt_server import BLUEZ, DBUS_OM_IFACE, PebbleGattServer
from .protocol import AppRunStateCmd, Endpoint
from .uuids import (
    CONNECTION_PARAMS_CHARACTERISTIC,
    CONNECTIVITY_CHARACTERISTIC,
    MTU_CHARACTERISTIC,
    PAIRING_TRIGGER_CHARACTERISTIC,
)

PROPERTIES_IFACE = "org.freedesktop.DBus.Properties"
DEVICE_IFACE = "org.bluez.Device1"
ADAPTER_IFACE = "org.bluez.Adapter1"

# Handler signatures:
#   AppMessageHandler(app_uuid: str, data: dict)   - inbound PUSH from a watchapp
#   AckHandler(transaction_id: int)                 - watch ACKed one of our sends
#   NackHandler(transaction_id: int)                - watch NACKed one of our sends
AppMessageHandler = Callable[[str, dict[int, int | str | bytes]], None]
AckHandler = Callable[[int], None]
NackHandler = Callable[[int], None]


@dataclass
class Pebble:
    address: str
    adapter: str = "hci0"
    _client: BleakClient | None = field(default=None, init=False)
    _server: PebbleGattServer | None = field(default=None, init=False)
    _txn: int = field(default=0, init=False)
    _loop: asyncio.AbstractEventLoop | None = field(default=None, init=False)
    _handlers: list = field(default_factory=list, init=False)
    _ack_handlers: list = field(default_factory=list, init=False)
    _nack_handlers: list = field(default_factory=list, init=False)
    # transaction_id -> asyncio.Future resolved when the watch ACK/NACKs it
    _pending: dict = field(default_factory=dict, init=False)
    _connected: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    # characteristics we successfully subscribed to (for clean teardown)
    _subscribed: list = field(default_factory=list, init=False)

    # ---- discovery ----
    @staticmethod
    async def scan(timeout: float = 8.0):
        """Return [(address, name), ...] of nearby Pebbles."""
        found = []
        devices = await BleakScanner.discover(timeout=timeout)
        for d in devices:
            name = d.name or ""
            if "pebble" in name.lower():
                found.append((d.address, name))
        return found

    # ---- async context manager ----
    async def __aenter__(self) -> "Pebble":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.disconnect()

    # ---- BlueZ device helpers ----
    def _device_path(self) -> str:
        return f"/org/bluez/{self.adapter}/dev_" + self.address.upper().replace(":", "_")

    async def _read_device_props(self) -> dict:
        """Read org.bluez.Device1 properties for our watch ({} on failure)."""
        bus = None
        try:
            bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
            path = self._device_path()
            introspect = await bus.introspect(BLUEZ, path)
            obj = bus.get_proxy_object(BLUEZ, path, introspect)
            props = obj.get_interface(PROPERTIES_IFACE)
            raw = await props.call_get_all(DEVICE_IFACE)
            return {k: v.value for k, v in raw.items()}
        except Exception as e:
            logger.trace(f"device props read failed: {e!r}")
            return {}
        finally:
            if bus:
                bus.disconnect()

    async def _wait_paired(self, timeout: float) -> bool:
        """Poll BlueZ until the device reports Paired/Bonded, or timeout."""
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            props = await self._read_device_props()
            if props.get("Paired") or props.get("Bonded"):
                return True
            await asyncio.sleep(0.5)
        return False

    async def _set_trusted(self) -> None:
        """Mark the device Trusted so BlueZ lets the bonded watch reconnect
        to our GATT server without an authorization prompt."""
        bus = None
        try:
            bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
            path = self._device_path()
            introspect = await bus.introspect(BLUEZ, path)
            obj = bus.get_proxy_object(BLUEZ, path, introspect)
            props = obj.get_interface(PROPERTIES_IFACE)
            await props.call_set(DEVICE_IFACE, "Trusted", Variant("b", True))
            logger.debug("watch marked Trusted in BlueZ")
        except Exception as e:
            logger.debug(f"could not set Trusted: {e!r}")
        finally:
            if bus:
                bus.disconnect()

    async def _forget_device(self) -> None:
        """Remove the device from BlueZ entirely (clears any stale host-side
        bond/keys). The watch's own bond, if stale, must be forgotten on the
        watch (Settings -> Bluetooth)."""
        bus = None
        try:
            bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
            adapter_path = f"/org/bluez/{self.adapter}"
            introspect = await bus.introspect(BLUEZ, adapter_path)
            obj = bus.get_proxy_object(BLUEZ, adapter_path, introspect)
            adapter = obj.get_interface(ADAPTER_IFACE)
            await adapter.call_remove_device(self._device_path())
            logger.info(f"removed {self.address} from BlueZ (stale bond cleared)")
        except Exception as e:
            logger.debug(f"RemoveDevice failed (may not exist): {e!r}")
        finally:
            if bus:
                bus.disconnect()

    # ---- connection lifecycle ----
    async def _find_known_device(self):
        """If BlueZ already has this device cached, return a BLEDevice that
        carries its D-Bus object path. Passing such a device to BleakClient
        makes bleak skip its own internal discovery scan (it only scans when
        the device path is unknown). That's essential here: once the watch is
        bonded and connected it stops advertising, so bleak's scan would raise
        BleakDeviceNotFoundError even though the device is right there in BlueZ.
        Returns (BLEDevice, connected: bool) or None.
        """
        bus = None
        try:
            bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
            introspect = await bus.introspect(BLUEZ, "/")
            obj = bus.get_proxy_object(BLUEZ, "/", introspect)
            om = obj.get_interface(DBUS_OM_IFACE)
            objects = await om.call_get_managed_objects()
            target = self.address.upper()
            for path, ifaces in objects.items():
                dev = ifaces.get(DEVICE_IFACE)
                if not dev:
                    continue
                addr = dev.get("Address")
                if addr is None or addr.value.upper() != target:
                    continue
                # Unwrap the dbus_fast Variants into plain values for details.
                props = {k: v.value for k, v in dev.items()}
                connected = bool(props.get("Connected", False))
                name = props.get("Name") or props.get("Alias") or self.address
                # bleak's BlueZ backend reads details["path"] and details["props"];
                # providing them means connect() won't run find_device_by_address.
                ble_device = BLEDevice(
                    address=self.address,
                    name=name,
                    details={"path": path, "props": props},
                )
                return ble_device, connected
            return None
        except Exception as e:
            logger.debug(f"could not query known devices: {e}")
            return None
        finally:
            if bus:
                bus.disconnect()

    async def _resolve_device(self, timeout: float) -> tuple[BLEDevice, dict]:
        """Find the watch via the BlueZ cache or a scan. Returns (device, props)."""
        known = await self._find_known_device()
        if known is not None:
            device, connected = known
            logger.debug(f"watch already known to BlueZ (connected={connected})")
            return device, device.details.get("props", {})
        device = await BleakScanner.find_device_by_address(self.address, timeout=timeout)
        if device is None:
            msg = (
                f"{self.address} not found in {timeout}s. Make sure the "
                f"watch is advertising (not already connected elsewhere) "
                f"and not half-bonded in the OS — if you ever paired it via "
                f"system Bluetooth settings, 'forget' it on both ends first."
            )
            raise RuntimeError(msg)
        return device, {}

    async def _teardown_client(self) -> None:
        client, self._client = self._client, None
        self._subscribed.clear()
        if client is not None:
            try:
                if client.is_connected:
                    await client.disconnect()
            except Exception as e:
                logger.debug(f"client teardown error: {e!r}")

    async def _dbus_device_disconnect(self) -> None:
        """Force BlueZ to drop any half-open link to the watch.

        When a connect attempt dies mid service-discovery, bleak's client
        never reaches is_connected=True, but BlueZ can still be holding the
        ACL link (or believe it is). A raw Device1.Disconnect resets that
        state so the next attempt starts clean.
        """
        bus = None
        try:
            bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
            path = self._device_path()
            introspect = await bus.introspect(BLUEZ, path)
            obj = bus.get_proxy_object(BLUEZ, path, introspect)
            dev = obj.get_interface(DEVICE_IFACE)
            await dev.call_disconnect()
            logger.debug("forced BlueZ Device1.Disconnect")
        except Exception as e:
            logger.trace(f"Device1.Disconnect failed (probably already down): {e!r}")
        finally:
            if bus:
                bus.disconnect()

    async def _connect_client(self, timeout: float, attempts: int, retry_delay: float) -> dict:
        """Resolve the watch and establish the GATT client link, retrying
        transient failures. Returns the device's BlueZ properties snapshot.

        Pebbles routinely drop the first connection attempt ("failed to
        discover services, device disconnected") when they're still tearing
        down a previous link or mid auto-reconnect; Gadgetbridge retries, and
        so do we — with a BlueZ-level link reset and a growing pause between
        attempts so the watch has time to settle.
        """
        last_error: Exception | None = None
        for i in range(1, attempts + 1):
            try:
                device, props = await self._resolve_device(timeout)
                self._client = BleakClient(
                    device,
                    timeout=timeout,
                    disconnected_callback=self._on_bleak_disconnect,
                )
                try:
                    await self._client.connect()
                except Exception as e:
                    if "already connected" not in str(e).lower():
                        raise
                    logger.debug("device was already connected; attaching")
                return props
            except Exception as e:
                last_error = e
                logger.warning(f"connect attempt {i}/{attempts} to {self.address} failed: {e!r}")
                await self._teardown_client()
                await self._dbus_device_disconnect()
                if i < attempts:
                    delay = retry_delay * i
                    logger.debug(f"retrying in {delay:.1f}s ...")
                    await asyncio.sleep(delay)
        msg = (
            f"could not establish a GATT connection to {self.address} after "
            f"{attempts} attempts (last error: {last_error!r}). The watch may "
            f"still be settling from a previous session — try again in a few "
            f"seconds, or toggle Bluetooth/Airplane mode on the watch."
        )
        raise RuntimeError(msg) from last_error

    def _on_bleak_disconnect(self, _client: BleakClient) -> None:
        """Fired by bleak when the GATT client link drops (range, airplane
        mode, watch reboot). Clears the connected event so the supervisor's
        wait wakes and reconnects. Runs in bleak's callback context, so we
        only flip state here and let teardown happen on the loop."""
        logger.warning("bleak reported watch disconnect")
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._connected.clear)
        else:
            self._connected.clear()

    async def _do_pairing(self, watch_initiated_wait: float = 10.0) -> bool:
        """Bond with the watch. Returns True once BlueZ reports Paired.

        Gadgetbridge's working order, mirrored here:
          1. Poke the pairing-trigger characteristic (a read first — the
             >=4.0-FW path — then the 0x09 write, which both requests pairing
             and announces the phone-hosted GATT server). The WATCH then shows
             its confirm screen and initiates bonding; our default agent
             accepts the host side.
          2. Only if the watch stays quiet, initiate Pair() from the host.
        """
        try:
            await self._client.read_gatt_char(PAIRING_TRIGGER_CHARACTERISTIC)
        except Exception as e:
            logger.debug(f"pairing trigger read failed (fine on some FW): {e!r}")
        try:
            await self._client.write_gatt_char(
                PAIRING_TRIGGER_CHARACTERISTIC, bytes([0x09]), response=True
            )
            logger.debug("wrote 0x09 to pairing trigger (pair + server mode)")
        except Exception as e:
            logger.debug(f"pairing trigger write failed: {e!r}")

        logger.info("waiting for the watch to initiate bonding ...")
        if await self._wait_paired(watch_initiated_wait):
            logger.debug("bonded (watch-initiated)")
            return True

        # Watch didn't start security; initiate from our side instead.
        logger.debug("watch did not initiate bonding; calling Pair() from the host")
        try:
            paired = await self._client.pair()
            if paired:
                logger.debug("bonded (host-initiated)")
                return True
        except Exception as e:
            logger.warning(f"host-initiated Pair() failed: {e!r}")
        # A failed/raced Pair() can still land the bond a beat later.
        return await self._wait_paired(3.0)

    async def connect(
        self,
        pairing: bool = True,
        timeout: float = 30.0,
        connect_attempts: int = 3,
        retry_delay: float = 2.0,
    ):
        self._loop = asyncio.get_running_loop()

        # 1. Bring up OUR GATT server FIRST (the 10000000 service). This is the
        #    working Gadgetbridge architecture: the phone hosts a server and the
        #    watch connects back to it as a client to carry PPoGATT data. The
        #    server must exist before the watch is told to connect back.
        self._server = PebbleGattServer(
            self.adapter, on_data=self._on_pebble_message, loop=self._loop
        )
        await self._server.start()

        agent: PairingAgent | None = None
        try:
            # 2. Resolve + connect (with transient-failure retries) + bond if
            #    needed. Two pairing attempts: a failed first pairing
            #    (typically AuthenticationFailed from a stale host-side bond)
            #    clears the BlueZ device and retries fresh.
            for attempt in (1, 2):
                known_props = await self._connect_client(timeout, connect_attempts, retry_delay)
                logger.success(f"connected to {self.address}")

                already_bonded = bool(known_props.get("Paired") or known_props.get("Bonded"))

                # Subscribe to connectivity BEFORE pairing: it works unbonded
                # and carries the watch's pairing-status updates.
                self._subscribed.clear()
                try:
                    await self._client.start_notify(
                        CONNECTIVITY_CHARACTERISTIC, self._on_connectivity
                    )
                    self._subscribed.append(CONNECTIVITY_CHARACTERISTIC)
                    logger.debug("subscribed to connectivity")
                except Exception as e:
                    logger.warning(f"connectivity subscribe failed: {e}")

                if not pairing or already_bonded:
                    if already_bonded:
                        logger.debug("already bonded; skipping pairing")
                    break

                # First-time bonding. Register a temporary default agent that
                # auto-accepts OUR watch's requests (headless hosts have no
                # agent, which is exactly what makes Pair() die with
                # AuthenticationFailed).
                if agent is None:
                    agent = PairingAgent(self.address)
                    try:
                        await agent.register()
                    except Exception as e:
                        agent = None
                        logger.warning(
                            f"could not register pairing agent: {e!r}; "
                            f"relying on a system agent being present"
                        )
                logger.info(
                    "watch is not bonded — pairing now. "
                    "CONFIRM THE PAIRING ON THE WATCH when it prompts."
                )
                if await self._do_pairing():
                    logger.success("bonded with watch")
                    await self._set_trusted()
                    break

                if attempt == 1:
                    logger.warning(
                        "pairing failed — clearing the (possibly stale) BlueZ "
                        "bond and retrying once from scratch"
                    )
                    await self._teardown_client()
                    await self._forget_device()
                    await asyncio.sleep(2.0)  # let the watch resume advertising
                    continue

                msg = (
                    f"pairing with {self.address} failed twice. If the watch "
                    f"lists this computer under Settings -> Bluetooth, FORGET "
                    f"it there (its own bond is stale), then try again."
                )
                raise RuntimeError(msg)

            # 3. Remaining fed9 handshake: MTU + connection-params (these can
            #    require an encrypted/bonded link, hence after pairing).
            for char_uuid, cb, label in (
                (MTU_CHARACTERISTIC, self._on_mtu, "MTU"),
                (CONNECTION_PARAMS_CHARACTERISTIC, self._on_conn_params, "connection-params"),
            ):
                try:
                    await self._client.start_notify(char_uuid, cb)
                    self._subscribed.append(char_uuid)
                    logger.debug(f"subscribed to {label}")
                except Exception as e:
                    logger.warning(f"{label} subscribe failed: {e}")

            # The watch publishes its preferred MTU on the MTU characteristic;
            # read the current value too in case the notification already fired.
            try:
                mtu_val = await self._client.read_gatt_char(MTU_CHARACTERISTIC)
                self._on_mtu(None, bytearray(mtu_val))
            except Exception as e:
                logger.debug(f"MTU characteristic read failed: {e}")

            # Pairing trigger (connect-back nudge). In the server
            # (non-clientOnly) path Gadgetbridge writes 0x09; clientOnly writes
            # 0x11. Idempotent if _do_pairing already wrote it.
            try:
                await self._client.write_gatt_char(
                    PAIRING_TRIGGER_CHARACTERISTIC,
                    bytes([0x09]),
                    response=True,
                )
                logger.debug("wrote 0x09 to pairing trigger (server mode)")
            except Exception as e:
                logger.warning(f"pairing trigger write failed: {e}")

            # 4. Wait for the watch to connect back to our GATT server and
            #    subscribe to the write characteristic. That subscription is
            #    the signal the PPoGATT data channel is live.
            logger.debug("waiting for watch to connect back to our GATT server ...")
            ok = await self._server.wait_connected(timeout=timeout)
            if not ok:
                logger.warning(
                    f"watch did not connect back to our GATT server within {timeout}. "
                    "The PPoGATT data channel is not established; sends may not "
                    "reach the watch.",
                )
            else:
                logger.debug("PPoGATT data channel established")
                # The watch still needs to complete the PPoGATT RESET handshake
                # before it will accept Pebble Protocol messages. Wait for it.
                ready = await self._server.wait_session_ready(timeout=10.0)
                if not ready:
                    logger.warning(
                        "PPoGATT session not confirmed ready; early sends "
                        "(time sync, launches) may be dropped"
                    )
                else:
                    logger.debug("PPoGATT session ready")

            mtu = getattr(self._client, "mtu_size", 0) or 23
            if mtu >= 23 and self._server.mtu == 23:
                # Only fall back to the link MTU if the watch never told us its
                # preferred MTU via the MTU characteristic.
                self._server.set_mtu(mtu)
            logger.debug(f"ATT MTU = {self._server.mtu}")

            self._connected.set()
        except BaseException:
            # Don't leak the GATT server registration or a half-open client.
            await self._teardown_client()
            if self._server:
                await self._server.stop()
                self._server = None
            raise
        finally:
            if agent is not None:
                await agent.unregister()

    async def disconnect(self):
        self._connected.clear()
        if self._client and self._client.is_connected:
            for char_uuid in self._subscribed:
                try:
                    await self._client.stop_notify(char_uuid)
                except Exception:
                    pass
            self._subscribed.clear()
        if self._server:
            await self._server.stop()
            self._server = None
        if self._client and self._client.is_connected:
            await self._client.disconnect()

    async def wait_disconnected(self) -> None:
        """Block until the watch link drops (the connected event clears)."""
        # Event has no 'wait for clear'; poll the flag with a short sleep.
        while self._connected.is_set():
            await asyncio.sleep(0.5)

    async def forget(self) -> None:
        """Remove this watch from BlueZ (clears the host-side bond).

        Use when bonding state is wedged. Remember the watch keeps its own
        bond table: if pairing still fails afterwards, forget this host on
        the watch too (Settings -> Bluetooth)."""
        await self._forget_device()

    # ---- fed9 characteristic callbacks ----
    def _on_connectivity(self, _char, data: bytearray):
        raw = bytes(data)
        if raw:
            flags = raw[0]
            # Best-effort decode of the status bits Gadgetbridge reports.
            logger.debug(
                f"connectivity update: {raw.hex()} "
                f"(connected={bool(flags & 1)}, paired={bool(flags & 2)}, "
                f"encrypted={bool(flags & 4)})"
            )

    def _on_conn_params(self, _char, data: bytearray):
        logger.debug(f"connection-params update: {bytes(data).hex()}")

    def _on_mtu(self, _char, data: bytearray):
        # The watch reports its preferred MTU here as a little-endian u16.
        if len(data) >= 2:
            watch_mtu = int.from_bytes(bytes(data[:2]), "little")
            logger.debug(f"watch requested MTU: {watch_mtu}")
            if self._server and watch_mtu >= 23:
                self._server.set_mtu(watch_mtu)

    # ---- inbound Pebble Protocol dispatch ----
    def _on_pebble_message(self, message: bytes):
        endpoint, payload = protocol.pebble_unpack(message)
        logger.trace(f"rx endpoint={endpoint} len={len(payload)}")

        if endpoint == Endpoint.PHONE_VERSION:
            self._on_phone_version(payload)
        elif endpoint == Endpoint.PING:
            self._on_ping(payload)
        elif endpoint == Endpoint.APP_MESSAGE:
            self._on_app_message_payload(payload)
        elif endpoint == Endpoint.BLOB_DB:
            self._on_blobdb(payload)
        # Other endpoints (system messages, app run state notifications, etc.)
        # flow here too; we don't model them. Ignore quietly rather than crash.

    def _on_phone_version(self, payload: bytes):
        # The watch is asking who we are. If we don't answer it will conclude
        # the phone app is absent and drop the session after a timeout.
        logger.debug("watch requested phone version; replying")
        self._send_pebble(Endpoint.PHONE_VERSION, protocol.build_phone_version_response())

    def _on_ping(self, payload: bytes):
        cookie = protocol.parse_ping(payload)
        if cookie is not None:
            logger.debug(f"ping cookie={cookie}; replying pong")
            self._send_pebble(Endpoint.PING, protocol.build_pong(cookie))

    def _on_app_message_payload(self, payload: bytes):
        # Log the raw AppMessage bytes — invaluable for reconciling the watch's
        # actual command/transaction values against what we sent.
        logger.trace(f"inbound APP_MESSAGE raw: {payload.hex()}")
        cmd, txn, app_uuid, data = appmessage.parse_app_message(payload)

        if cmd == AppMessageCmd.PUSH:
            # The watchapp pushed data to us. ACK it so it doesn't retransmit,
            # then fan out to handlers.
            self._send_pebble(Endpoint.APP_MESSAGE, appmessage.build_app_message_ack(txn))
            logger.debug(f"inbound PUSH txn={txn} uuid={app_uuid} data={data}")
            for h in self._handlers:
                try:
                    h(app_uuid, data)
                except Exception:
                    logger.exception("app message handler raised")

        elif cmd == AppMessageCmd.ACK:
            # The watch confirmed one of our sends.
            logger.debug(f"inbound ACK txn={txn}")
            self._resolve_pending(txn, True)
            for h in self._ack_handlers:
                try:
                    h(txn)
                except Exception:
                    logger.exception("ack handler raised")

        elif cmd == AppMessageCmd.NACK:
            # The watch rejected one of our sends (e.g. app not listening, or
            # the app's inbox was too small for the message).
            logger.debug(f"inbound NACK txn={txn}")
            self._resolve_pending(txn, False)
            for h in self._nack_handlers:
                try:
                    h(txn)
                except Exception:
                    logger.exception("nack handler raised")
        # any other (unknown) command byte is ignored

    def _on_blobdb(self, payload: bytes):
        parsed = protocol.parse_blobdb_response(payload)
        if parsed is None:
            logger.debug(f"unparseable BlobDB response: {payload.hex()}")
            return
        token, status = parsed
        try:
            status_name = protocol.BlobDBStatus(status).name
        except ValueError:
            status_name = f"unknown({status})"
        if status == protocol.BlobDBStatus.SUCCESS:
            logger.debug(f"BlobDB token={token} -> {status_name}")
        else:
            logger.warning(f"BlobDB token={token} -> {status_name}")

    def _resolve_pending(self, txn: int, acked: bool):
        fut = self._pending.pop(txn, None)
        if fut is None and self._pending:
            # The watch's ACK transaction id didn't match any we sent. Pebble's
            # AppMessage ACK echoes the transaction id of the PUSH it answers,
            # but firmware/stack quirks can renumber it. With sends outstanding,
            # resolve the oldest one — ACKs are ordered, so this stays correct
            # for the common one-in-flight case.
            oldest = next(iter(self._pending))
            logger.debug(f"ACK txn={txn} had no exact match; resolving oldest pending txn={oldest}")
            fut = self._pending.pop(oldest, None)
        if fut is not None and not fut.done():
            fut.set_result(acked)

    # ---- public API ----
    def on_app_message(self, fn: AppMessageHandler) -> AppMessageHandler:
        """Register a handler for inbound AppMessages pushed BY a watchapp.
        Called as fn(app_uuid: str, data: dict). Usable as a decorator.
        """
        self._handlers.append(fn)
        return fn

    def on_ack(self, fn: AckHandler) -> AckHandler:
        """Register a handler called as fn(transaction_id) when the watch ACKs
        one of our sends. Usable as a decorator.
        """
        self._ack_handlers.append(fn)
        return fn

    def on_nack(self, fn: NackHandler) -> NackHandler:
        """Register a handler called as fn(transaction_id) when the watch NACKs
        one of our sends. Usable as a decorator.
        """
        self._nack_handlers.append(fn)
        return fn

    async def update_time(self) -> None:
        """Sync the watch's clock to this machine's current local time.

        Sends a TIME SET_UTC message with the host's current UTC timestamp,
        the host's local UTC offset (in minutes, DST-aware), and the local
        timezone name. The watch stores UTC and applies the offset for display.
        """
        if not self._connected.is_set():
            msg = "not connected"
            raise RuntimeError(msg)

        now = datetime.now(UTC)
        utc_ts = int(now.timestamp())

        local = now.astimezone()  # host's configured local zone
        offset = local.utcoffset()
        offset_minutes = int(offset.total_seconds() // 60) if offset else 0
        tz_name = local.tzname() or ""

        logger.debug(f"setting watch time: utc={utc_ts} offset={offset_minutes}min tz={tz_name!r}")
        self._send_pebble(
            Endpoint.TIME,
            protocol.build_set_utc(utc_ts, offset_minutes, tz_name),
        )

    async def launch_app(self, app_uuid: str):
        """Ask the watch to launch the watchapp identified by app_uuid.

        Sending an AppMessage to an app that isn't running just gets NACKed,
        so launching it first is usually what you want. (APP_RUN_STATE
        endpoint, command 0x01 = start.)
        """
        if not self._connected.is_set():
            msg = "not connected"
            raise RuntimeError(msg)
        self._send_pebble(
            Endpoint.APP_RUN_STATE,
            protocol.build_app_run_state(AppRunStateCmd.START, app_uuid),
        )

    async def stop_app(self, app_uuid: str):
        """Ask the watch to close the watchapp identified by app_uuid."""
        if not self._connected.is_set():
            msg = "not connected"
            raise RuntimeError(msg)
        self._send_pebble(
            Endpoint.APP_RUN_STATE,
            protocol.build_app_run_state(AppRunStateCmd.STOP, app_uuid),
        )

    async def send_app_message(
        self,
        app_uuid: str,
        data: dict[int, AppMessageValue],
        wait_ack: bool = False,
        ack_timeout: float = 5.0,
        raise_on_timeout: bool = False,
    ) -> int:
        """Push a key/value dict to the watchapp identified by app_uuid.

        Keys are ints (your appKeys), values may be int/str/bytes, or one of the
        explicit-width wrappers (u8/u16/u32/i8/i16/i32) to match exactly what the
        watchapp reads. Returns the transaction id used.

        If wait_ack is True, awaits the watch's ACK/NACK for this transaction.
        On NACK it raises PebbleNackError. On timeout it logs a warning and
        returns normally (set raise_on_timeout=True to raise TimeoutError
        instead) — a missed ACK doesn't necessarily mean the message didn't
        arrive, so by default a stream keeps going.
        """
        if not self._connected.is_set():
            msg = "not connected"
            raise RuntimeError(msg)
        self._txn = (self._txn + 1) & 0xFF
        txn = self._txn
        body = appmessage.build_app_message_push(txn, app_uuid, data)

        fut = None
        if wait_ack:
            fut = self._loop.create_future()
            self._pending[txn] = fut

        self._send_pebble(Endpoint.APP_MESSAGE, body)

        if wait_ack:
            try:
                acked = await asyncio.wait_for(fut, ack_timeout)
            except TimeoutError:
                self._pending.pop(txn, None)
                if raise_on_timeout:
                    msg_0 = f"no ACK/NACK for transaction {txn} within {ack_timeout}s"
                    raise TimeoutError(msg_0)
                logger.warning(
                    f"no ACK for transaction {txn} within {ack_timeout} (message may still have arrived)"
                )
                return txn
            if not acked:
                msg_1 = f"watch NACKed transaction {txn}"
                raise PebbleNackError(msg_1)
        return txn

    async def send_notification(
        self,
        title: str,
        body: str,
        subtitle: str = "",
        icon: int | None = None,
    ) -> int:
        """Push a notification to the watch's notification center (BlobDB).

        Returns the BlobDB token used, so a caller can correlate the watch's
        async BlobDB response (logged in _on_blobdb) if it wants confirmation.
        """
        if not self._connected.is_set():
            msg = "not connected"
            raise RuntimeError(msg)

        token = int.from_bytes(__import__("os").urandom(2), "little")
        kwargs = {} if icon is None else {"icon": icon}
        payload = protocol.build_notification(title, body, subtitle, token=token, **kwargs)
        logger.debug(f"sending notification token={token} title={title!r}")
        self._send_pebble(Endpoint.BLOB_DB, payload)
        return token

    def _send_pebble(self, endpoint: Endpoint, payload: bytes):
        message = protocol.pebble_pack(endpoint, payload)
        if not self._server:
            msg = "server not started"
            raise RuntimeError(msg)
        self._server.send(message)

"""High-level Pebble connection: lifecycle, endpoint dispatch, AppMessage API.

This is the class users interact with. It owns:
  * the bleak GATT *client* connection used for the fed9 pairing/connectivity
    handshake, and
  * the phone-hosted PPoGATT GATT *server* (gatt_server.PebbleGattServer) the
    watch connects back to for actual data transfer,
and routes inbound Pebble Protocol messages by endpoint — answering the
watch's PHONE_VERSION and PING keepalives itself and fanning AppMessages out
to registered handlers.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType
from loguru import logger

from . import appmessage, protocol
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
                dev = ifaces.get("org.bluez.Device1")
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

    async def connect(self, pairing: bool = True, timeout: float = 30.0):
        self._loop = asyncio.get_running_loop()

        # 1. Bring up OUR GATT server FIRST (the 10000000 service). This is the
        #    working Gadgetbridge architecture: the phone hosts a server and the
        #    watch connects back to it as a client to carry PPoGATT data. The
        #    server must exist before the watch is told to connect back.
        self._server = PebbleGattServer(
            self.adapter, on_data=self._on_pebble_message, loop=self._loop
        )
        await self._server.start()

        # 2. Resolve and connect to the watch as a central (for the fed9
        #    connectivity/pairing-trigger handshake).
        logger.info(f"locating {self.address} ...")
        known = await self._find_known_device()
        if known is not None:
            device, connected = known
            logger.info(f"watch already known to BlueZ (connected={connected})")
        else:
            device = await BleakScanner.find_device_by_address(self.address, timeout=timeout)
            if device is None:
                await self._server.stop()
                msg = (
                    f"{self.address} not found in {timeout}s. Make sure the "
                    f"watch is advertising (not already connected elsewhere) "
                    f"and not half-bonded in the OS — if you ever paired it via "
                    f"system Bluetooth settings, 'forget' it on both ends first."
                )
                raise RuntimeError(msg)

        self._client = BleakClient(device, timeout=timeout)
        try:
            await self._client.connect()
        except Exception as e:
            if "already connected" not in str(e).lower():
                await self._server.stop()
                raise
            logger.info("device was already connected; attaching")
        logger.info(f"connected to {self.address}")

        already_bonded = bool(known and known[0].details.get("props", {}).get("Bonded"))
        if pairing and not already_bonded:
            try:
                paired = await self._client.pair()
                logger.info(f"bonding result: {paired}")
            except Exception as e:
                logger.warning(f"explicit pair() failed (may already be bonded): {e}")
        elif already_bonded:
            logger.info("already bonded; skipping pair()")

        # 3. fed9 handshake. Subscribe to connectivity, MTU and connection-
        #    params updates, then write the pairing trigger. With our GATT
        #    server already advertised, the trigger write is what prompts the
        #    watch to connect back to our 10000000 service.
        for char_uuid, cb, label in (
            (CONNECTIVITY_CHARACTERISTIC, self._on_connectivity, "connectivity"),
            (MTU_CHARACTERISTIC, self._on_mtu, "MTU"),
            (CONNECTION_PARAMS_CHARACTERISTIC, self._on_conn_params, "connection-params"),
        ):
            try:
                await self._client.start_notify(char_uuid, cb)
                self._subscribed.append(char_uuid)
                logger.info(f"subscribed to {label}")
            except Exception as e:
                logger.warning(f"{label} subscribe failed: {e}")

        # The watch publishes its preferred MTU on the MTU characteristic; read
        # the current value too in case the notification already fired.
        try:
            mtu_val = await self._client.read_gatt_char(MTU_CHARACTERISTIC)
            self._on_mtu(None, bytearray(mtu_val))
        except Exception as e:
            logger.debug(f"MTU characteristic read failed: {e}")

        # Pairing trigger. In the server (non-clientOnly) path Gadgetbridge
        # writes 0x09; clientOnly writes 0x11. We use the server path here.
        try:
            await self._client.write_gatt_char(
                PAIRING_TRIGGER_CHARACTERISTIC,
                bytes([0x09]),
                response=True,
            )
            logger.info("wrote 0x09 to pairing trigger (server mode)")
        except Exception as e:
            logger.warning(f"pairing trigger write failed: {e}")

        # 4. Wait for the watch to connect back to our GATT server and subscribe
        #    to the write characteristic. That subscription is the signal the
        #    PPoGATT data channel is live.
        logger.info("waiting for watch to connect back to our GATT server ...")
        ok = await self._server.wait_connected(timeout=timeout)
        if not ok:
            logger.warning(
                f"watch did not connect back to our GATT server within {timeout}. "
                "The PPoGATT data channel is not established; sends may not "
                "reach the watch.",
            )
        else:
            logger.info("PPoGATT data channel established")

        mtu = getattr(self._client, "mtu_size", 0) or 23
        if mtu >= 23 and self._server.mtu == 23:
            # Only fall back to the link MTU if the watch never told us its
            # preferred MTU via the MTU characteristic.
            self._server.set_mtu(mtu)
        logger.info(f"ATT MTU = {self._server.mtu}")

        self._connected.set()

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

    # ---- fed9 characteristic callbacks ----
    def _on_connectivity(self, _char, data: bytearray):
        logger.info(f"connectivity update from watch: {bytes(data).hex()}")

    def _on_conn_params(self, _char, data: bytearray):
        logger.debug(f"connection-params update: {bytes(data).hex()}")

    def _on_mtu(self, _char, data: bytearray):
        # The watch reports its preferred MTU here as a little-endian u16.
        if len(data) >= 2:
            watch_mtu = int.from_bytes(bytes(data[:2]), "little")
            logger.info(f"watch requested MTU: {watch_mtu}")
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
        # Other endpoints (system messages, app run state notifications, etc.)
        # flow here too; we don't model them. Ignore quietly rather than crash.

    def _on_phone_version(self, payload: bytes):
        # The watch is asking who we are. If we don't answer it will conclude
        # the phone app is absent and drop the session after a timeout.
        logger.info("watch requested phone version; replying")
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

    def _send_pebble(self, endpoint: Endpoint, payload: bytes):
        message = protocol.pebble_pack(endpoint, payload)
        if not self._server:
            msg = "server not started"
            raise RuntimeError(msg)
        self._server.send(message)

"""Phone-hosted PPoGATT GATT server (BlueZ peripheral via D-Bus / dbus-fast).

We register an org.bluez.GattApplication1 exporting the PPoGATT service.
The watch, acting as GATT client, writes Pebble data to our WRITE char and
receives our notifications on the same char. This is the working Gadgetbridge
(non-clientOnly) architecture.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import TYPE_CHECKING

from dbus_fast import Variant
from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType
from dbus_fast.service import PropertyAccess, ServiceInterface, dbus_property, method
from loguru import logger

from .ppogatt import (
    PPOGATT_WINDOW,
    PPoGATTSession,
    PPoGATTType,
    parse_ppogatt_header,
    ppogatt_header,
)
from .uuids import (
    PPOGATT_BADBAD_SERVICE,
    PPOGATT_SERVER_READ_CHARACTERISTIC,
    PPOGATT_SERVER_SERVICE,
    PPOGATT_SERVER_WRITE_CHARACTERISTIC,
)

if TYPE_CHECKING:
    from collections.abc import Callable

BLUEZ = "org.bluez"
GATT_MANAGER_IFACE = "org.bluez.GattManager1"
DBUS_OM_IFACE = "org.freedesktop.DBus.ObjectManager"

APP_PATH = "/org/pebble_le/app"


class _Characteristic(ServiceInterface):
    def __init__(
        self,
        path,
        uuid,
        flags,
        service_path,
        on_write=None,
        read_value=None,
        on_subscribe=None,
    ):
        super().__init__("org.bluez.GattCharacteristic1")
        self.path = path
        self._uuid = uuid
        self._flags = flags
        self._service_path = service_path
        self._value = bytearray(read_value or b"")
        self._notifying = False
        self._on_write = on_write
        self._on_subscribe = on_subscribe

    # --- properties BlueZ reads ---
    @dbus_property(access=PropertyAccess.READ)
    def UUID(self) -> "s":
        return self._uuid

    @dbus_property(access=PropertyAccess.READ)
    def Service(self) -> "o":
        return self._service_path

    @dbus_property(access=PropertyAccess.READ)
    def Flags(self) -> "as":
        return self._flags

    @dbus_property(access=PropertyAccess.READ)
    def Notifying(self) -> "b":
        return self._notifying

    @dbus_property(access=PropertyAccess.READ)
    def Value(self) -> "ay":
        return bytes(self._value)

    # --- methods the watch (via BlueZ) calls ---
    @method()
    def ReadValue(self, options: "a{sv}") -> "ay":
        logger.trace(f"GATT-server ReadValue on {self._uuid}")
        return bytes(self._value)

    @method()
    def WriteValue(self, value: "ay", options: "a{sv}"):
        logger.trace(f"GATT-server WriteValue on {self._uuid}: {bytes(value).hex()}")
        if self._on_write:
            self._on_write(bytes(value))

    @method()
    def StartNotify(self):
        logger.info(f"GATT-server StartNotify on {self._uuid} (watch subscribed)")
        self._notifying = True
        if self._on_subscribe:
            self._on_subscribe()

    @method()
    def StopNotify(self):
        self._notifying = False

    def notify(self, data: bytes):
        """Push a notification to the watch by emitting PropertiesChanged."""
        self._value = bytearray(data)
        self.emit_properties_changed({"Value": bytes(data)})


class _Service(ServiceInterface):
    def __init__(self, path, uuid, primary=True):
        super().__init__("org.bluez.GattService1")
        self.path = path
        self._uuid = uuid
        self._primary = primary

    @dbus_property(access=PropertyAccess.READ)
    def UUID(self) -> "s":
        return self._uuid

    @dbus_property(access=PropertyAccess.READ)
    def Primary(self) -> "b":
        return self._primary


class _Application(ServiceInterface):
    """Implements org.freedesktop.DBus.ObjectManager for BlueZ."""

    def __init__(self):
        super().__init__(DBUS_OM_IFACE)
        self.services: list = []
        self.characteristics: list = []

    @method()
    def GetManagedObjects(self) -> "a{oa{sa{sv}}}":
        resp = {}
        for svc in self.services:
            resp[svc.path] = {
                "org.bluez.GattService1": {
                    "UUID": Variant("s", svc._uuid),
                    "Primary": Variant("b", svc._primary),
                }
            }
        for ch in self.characteristics:
            resp[ch.path] = {
                "org.bluez.GattCharacteristic1": {
                    "UUID": Variant("s", ch._uuid),
                    "Service": Variant("o", ch._service_path),
                    "Flags": Variant("as", ch._flags),
                }
            }
        return resp


class PebbleGattServer:
    """Hosts the PPoGATT server the WATCH connects back to.

    (Gadgetbridge's PebbleGATTServer).
    We are the BlueZ peripheral:

      service 10000000
        char 10000002  READ           -> fixed 19-byte blob on read
        char 10000001  WRITE_NO_RESP + NOTIFY
                       watch writes PPoGATT packets here (-> _handle_ppogatt_in)
                       we notify PPoGATT packets back on the same char
      service badbadba-...  (added after the first service is registered)

    on_data(payload: bytes) is called with each reassembled Pebble Protocol
    message coming up from the watch. Use .send(payload) to push one down.

    Flow control and RX dedup live in the shared PPoGATTSession: DATA chunks
    queue here and at most PPOGATT_WINDOW are in flight un-ACKed at a time;
    each watch ACK releases the next chunk.
    """

    # The exact 19-byte response Gadgetbridge returns for a read of 10000002.
    _READ_BLOB = bytes([0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])

    def __init__(
        self,
        adapter: str = "hci0",
        on_data: Callable[[bytes], None] | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        self.adapter = adapter
        self.on_data = on_data
        self._loop = loop
        self._bus: MessageBus | None = None
        self._app: _Application | None = None
        self._write_char = None  # 10000001: watch->us write, us->watch notify
        self._read_char = None  # 10000002: read
        self._mtu = 23
        self._session = PPoGATTSession()
        self._tx_queue: deque[bytes] = deque()  # chunk payloads awaiting a window slot
        self._connected_evt = asyncio.Event()

    async def start(self):
        self._loop = self._loop or asyncio.get_running_loop()
        self._bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
        self._app = _Application()

        # Primary PPoGATT server service.
        svc = _Service(APP_PATH + "/service0", PPOGATT_SERVER_SERVICE)
        self._app.services.append(svc)

        # READ characteristic (10000002): the watch reads our fixed blob.
        self._read_char = _Characteristic(
            APP_PATH + "/service0/char0",
            PPOGATT_SERVER_READ_CHARACTERISTIC,
            ["read"],
            svc.path,
            read_value=self._READ_BLOB,
        )
        # WRITE characteristic (10000001): the watch writes PPoGATT packets to
        # us; we also push notifications back on it. write-without-response so
        # the watch streams without per-write ACK round-trips.
        self._write_char = _Characteristic(
            APP_PATH + "/service0/char1",
            PPOGATT_SERVER_WRITE_CHARACTERISTIC,
            ["write-without-response", "write", "notify"],
            svc.path,
            on_write=self._handle_ppogatt_in,
            on_subscribe=self._on_watch_subscribed,
        )
        self._app.characteristics += [self._read_char, self._write_char]

        # Second "BADBAD" service. Gadgetbridge adds this right after the first
        # service registers; the watch apparently expects it to be present.
        badbad = _Service(APP_PATH + "/service1", PPOGATT_BADBAD_SERVICE)
        self._app.services.append(badbad)
        badbad_char = _Characteristic(
            APP_PATH + "/service1/char0",
            PPOGATT_BADBAD_SERVICE,
            ["read"],
            badbad.path,
            read_value=b"\x00",
        )
        self._app.characteristics.append(badbad_char)

        # Export everything onto the bus.
        self._bus.export(APP_PATH, self._app)
        self._bus.export(svc.path, svc)
        self._bus.export(self._read_char.path, self._read_char)
        self._bus.export(self._write_char.path, self._write_char)
        self._bus.export(badbad.path, badbad)
        self._bus.export(badbad_char.path, badbad_char)

        introspect = await self._bus.introspect(BLUEZ, f"/org/bluez/{self.adapter}")
        adapter_obj = self._bus.get_proxy_object(BLUEZ, f"/org/bluez/{self.adapter}", introspect)
        gatt_mgr = adapter_obj.get_interface(GATT_MANAGER_IFACE)
        await gatt_mgr.call_register_application(APP_PATH, {})
        logger.info(
            f"GATT server registered: hosting {PPOGATT_SERVER_SERVICE} (+BADBAD) on {self.adapter}",
        )

    def set_mtu(self, mtu: int):
        self._mtu = mtu

    @property
    def mtu(self) -> int:
        return self._mtu

    async def wait_connected(self, timeout: float):
        """Wait until the watch subscribes to our write characteristic."""
        try:
            await asyncio.wait_for(self._connected_evt.wait(), timeout)
            return True
        except TimeoutError:
            return False

    def _on_watch_subscribed(self):
        logger.info("watch subscribed to PPoGATT server characteristic")
        if not self._connected_evt.is_set():
            self._connected_evt.set()

    # ---- PPoGATT transport (mirrors PebbleLESupport.handlePPoGATTPacket) ----
    def _handle_ppogatt_in(self, packet: bytes):
        if not packet:
            return
        command, serial = parse_ppogatt_header(packet[0])
        body = packet[1:]
        logger.trace(f"PPoGATT rx cmd={command} serial={serial} len={len(body)}")

        if command == PPoGATTType.RESET_REQUEST:
            # Gadgetbridge replies {0x03,0x19,0x19} if a payload was present,
            # else {0x03}. The payload bytes advertise our window sizes. Reset
            # all sequence/window state.
            self._session.reset()
            self._tx_queue.clear()
            if len(packet) > 1:
                self._send_raw(bytes([0x03, PPOGATT_WINDOW, PPOGATT_WINDOW]))
            else:
                self._send_raw(bytes([0x03]))
            return
        if command == PPoGATTType.RESET_COMPLETE:
            logger.debug("PPoGATT reset complete")
            return
        if command == PPoGATTType.ACK:
            logger.trace(f"PPoGATT ack serial={serial}")
            self._session.on_ack()
            self._pump_tx()
            return
        if command == PPoGATTType.DATA:
            # Always ACK (a retransmit means our previous ACK was lost); the
            # session drops duplicates so they can't desync framing.
            self._send_raw(bytes([ppogatt_header(PPoGATTType.ACK, serial)]))
            messages = self._session.on_data(serial, body)
            if messages and self.on_data:
                for message in messages:
                    self.on_data(message)
            return
        logger.debug(f"PPoGATT unknown command {command} ignored")

    def _send_raw(self, packet: bytes):
        """Notify a raw PPoGATT packet to the watch on the write characteristic."""
        if self._write_char:
            self._write_char.notify(packet)

    def send(self, pebble_message: bytes):
        """Send one whole Pebble Protocol message, chunked to the MTU, as a
        sequence of DATA packets with incrementing 5-bit serials. Chunks beyond
        the PPoGATT send window are queued and released as ACKs come back.
        """
        max_body = max(self._mtu - 3 - 1, 20)  # ATT(3) + ppogatt header(1)
        for i in range(0, len(pebble_message), max_body):
            self._tx_queue.append(pebble_message[i : i + max_body])
        self._pump_tx()

    def _pump_tx(self):
        while self._tx_queue and self._session.can_send():
            chunk = self._tx_queue.popleft()
            header = ppogatt_header(PPoGATTType.DATA, self._session.next_tx_seq())
            self._send_raw(bytes([header]) + chunk)

    async def stop(self):
        if self._bus:
            try:
                introspect = await self._bus.introspect(BLUEZ, f"/org/bluez/{self.adapter}")
                adapter_obj = self._bus.get_proxy_object(
                    BLUEZ, f"/org/bluez/{self.adapter}", introspect
                )
                gatt_mgr = adapter_obj.get_interface(GATT_MANAGER_IFACE)
                await gatt_mgr.call_unregister_application(APP_PATH)
            except Exception as e:
                logger.debug(f"unregister application failed: {e}")
            self._bus.disconnect()
            self._bus = None

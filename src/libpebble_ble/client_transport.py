"""PPoGATT client transport — watch-hosted server model (Pebble 2 only).

Alternative transport for firmwares where the *watch* hosts the PPoGATT
service 30000003 and we are the GATT client (Gadgetbridge's experimental
clientOnly mode). See uuids.py for the characteristic mapping.

NOTE: the high-level Pebble class uses the phone-hosted SERVER transport
(gatt_server.PebbleGattServer), which is the path that works on classic
Pebbles. This client transport is kept for Pebble 2 experimentation; wire it
up yourself if you need it.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from .ppogatt import (
    PPOGATT_WINDOW,
    PPoGATTSession,
    PPoGATTType,
    parse_ppogatt_header,
    ppogatt_header,
)
from .uuids import PPOGATT_WATCH_NOTIFY, PPOGATT_WATCH_WRITE

if TYPE_CHECKING:
    from collections.abc import Callable

    from bleak import BleakClient

log = logging.getLogger("pebble_le.client_transport")


class PebblePPoGATTClient:
    """PPoGATT transport over a bleak GATT *client* connection.

    Same framing/reassembly/flow-control as the server variant (shared via
    PPoGATTSession), but I/O goes through the watch's characteristics.
    on_data(payload) fires for each whole Pebble Protocol message reassembled
    from the watch.
    """

    def __init__(self, client: BleakClient, on_data: Callable[[bytes], None] | None = None):
        self._client = client
        self.on_data = on_data
        self._mtu = 23
        self._max_write = 20
        self._session = PPoGATTSession()
        self._tx_space = asyncio.Event()
        self._tx_space.set()

    async def start(self, wait_timeout: float = 20.0):
        # The PPoGATT service (30000003) is sometimes not present in the GATT
        # table immediately after a fresh connect — on Pebble it can appear only
        # after the connectivity handshake. Wait for the notify characteristic
        # to show up rather than crashing on a missing-characteristic error.
        char = await self._await_characteristic(PPOGATT_WATCH_NOTIFY, wait_timeout)
        if char is None:
            present = []
            try:
                for svc in self._client.services:
                    for c in svc.characteristics:
                        present.append(c.uuid)
            except Exception:
                pass
            msg = (
                f"PPoGATT characteristic {PPOGATT_WATCH_NOTIFY} not found after "
                f"{wait_timeout}s. The watch hasn't exposed the 30000003 "
                f"service on this connection yet. Characteristics seen: "
                f"{present}"
            )
            raise RuntimeError(msg)

        # Subscribe to the watch's notify characteristic, then send a PPoGATT
        # RESET_REQUEST to (re)initialize the sequence windows. The watch should
        # answer with RESET_COMPLETE; from then on DATA packets carry Pebble
        # Protocol messages.
        await self._client.start_notify(PPOGATT_WATCH_NOTIFY, self._on_notify)
        log.info("subscribed to PPoGATT notify %s", PPOGATT_WATCH_NOTIFY)
        await self._write_ppogatt(PPoGATTType.RESET_REQUEST, b"")

    async def _await_characteristic(self, uuid: str, timeout: float):
        """Poll bleak's service cache for a characteristic, re-discovering if
        needed, until it appears or timeout elapses.
        """
        uuid = uuid.lower()
        deadline = asyncio.get_event_loop().time() + timeout
        attempt = 0
        while asyncio.get_event_loop().time() < deadline:
            try:
                for svc in self._client.services:
                    for c in svc.characteristics:
                        if c.uuid.lower() == uuid:
                            return c
            except Exception as e:
                log.debug("service scan error: %s", e)
            attempt += 1
            if attempt == 1:
                log.info(
                    "PPoGATT service not present yet; waiting for the "
                    "watch to expose it (the connectivity handshake may "
                    "still be in progress) ..."
                )
            # Ask bleak/BlueZ to refresh the GATT table. Newer bleak caches
            # services from connect; get_services(force) re-reads them.
            try:
                getter = getattr(self._client, "get_services", None)
                if getter is not None:
                    await getter()
            except Exception as e:
                log.debug("get_services refresh failed: %s", e)
            await asyncio.sleep(1.0)
        return None

    def set_mtu(self, mtu: int):
        self._mtu = mtu
        self._max_write = max(mtu - 3 - 1, 20)  # ATT(3) + ppogatt header(1)

    def _on_notify(self, _char, data: bytearray):
        packet = bytes(data)
        if not packet:
            return
        ptype, seq = parse_ppogatt_header(packet[0])
        body = packet[1:]
        log.debug("PPoGATT rx type=%s seq=%d len=%d", ptype, seq, len(body))
        if ptype == PPoGATTType.RESET_REQUEST:
            self._session.reset()
            self._tx_space.set()
            asyncio.create_task(self._write_ppogatt(PPoGATTType.RESET_COMPLETE, b""))
        elif ptype == PPoGATTType.RESET_COMPLETE:
            self._session.reset()
            self._tx_space.set()
            log.info("PPoGATT reset complete; transport ready")
        elif ptype == PPoGATTType.ACK:
            self._session.on_ack()
            if self._session.can_send():
                self._tx_space.set()
        elif ptype == PPoGATTType.DATA:
            # Always ACK; the session drops duplicates (see PPoGATTSession).
            asyncio.create_task(self._write_ppogatt(PPoGATTType.ACK, b"", seq=seq))
            messages = self._session.on_data(seq, body)
            if messages and self.on_data:
                for message in messages:
                    self.on_data(message)
        else:
            log.debug("PPoGATT unknown command %s ignored", ptype)

    async def _write_ppogatt(self, ptype: PPoGATTType, body: bytes, seq: int | None = None):
        if seq is None:
            # next_tx_seq also counts the packet against the send window; only
            # DATA packets are flow-controlled, but reset/complete are rare and
            # always followed by a session.reset(), so this stays correct.
            seq = self._session.next_tx_seq()
            if not self._session.can_send():
                self._tx_space.clear()
        packet = bytes([ppogatt_header(ptype, seq)]) + body
        try:
            await self._client.write_gatt_char(PPOGATT_WATCH_WRITE, packet, response=False)
            log.debug("PPoGATT tx type=%s seq=%d len=%d", ptype, seq, len(body))
        except Exception as e:
            log.warning("PPoGATT write failed: %s", e)

    async def send(self, pebble_message: bytes):
        """Send one whole Pebble Protocol message, chunked to the MTU and
        honoring the PPoGATT send window.
        """
        for i in range(0, len(pebble_message), self._max_write):
            await self._tx_space.wait()
            await self._write_ppogatt(PPoGATTType.DATA, pebble_message[i : i + self._max_write])

    async def stop(self):
        try:
            await self._client.stop_notify(PPOGATT_WATCH_NOTIFY)
        except Exception:
            pass

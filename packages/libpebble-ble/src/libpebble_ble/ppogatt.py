"""PPoGATT — "Pebble Protocol over GATT" transport framing.

Each PPoGATT packet is a 1-byte header followed by an optional payload.
Per Gadgetbridge's PebbleLESupport.handlePPoGATTPacket the header is:

  bits 0-2 : command  (3-bit, see PPoGATTType)
  bits 3-7 : serial   (5-bit sequence number, wraps at 32)

DATA packets (command 0) carry one Pebble Protocol message, possibly split
across several DATA packets if it exceeds the negotiated ATT MTU.

Reset handshake observed in Gadgetbridge:
  - watch sends command 0x02 (RESET_REQUEST). We reply:
      {0x03, 0x19, 0x19} if the request carried a payload, else {0x03}.
    The 0x19/0x19 bytes advertise our rx/tx window sizes (25 packets each).
  - command 0x03 is RESET_COMPLETE.
  - command 0x01 is ACK, where the ACK header is ((serial << 3) | 1).

This module also provides :class:`PPoGATTSession`, the sequence/window/
reassembly state machine shared by both transports (the phone-hosted GATT
server in ``gatt_server`` and the GATT client in ``client_transport``), so
the dedup and framing rules live in exactly one place.
"""

from __future__ import annotations

import logging
import struct
from enum import IntEnum

from loguru import logger

# Window size we advertise in the reset reply and honor on our TX side.
PPOGATT_WINDOW = 0x19  # 25 packets

# If the reassembly buffer ever exceeds this, the length-prefixed framing has
# desynced (e.g. corrupt length); we drop the buffer instead of growing forever.
MAX_REASSEMBLY = 16 * 1024


class PPoGATTType(IntEnum):
    DATA = 0
    ACK = 1
    RESET_REQUEST = 2
    RESET_COMPLETE = 3


def ppogatt_header(packet_type: PPoGATTType, seq: int) -> int:
    return (int(packet_type) & 0x07) | ((seq & 0x1F) << 3)


def parse_ppogatt_header(byte: int) -> tuple[PPoGATTType | int, int]:
    """Parse a PPoGATT header byte. Returns (command, serial).

    The command is returned as a PPoGATTType when recognized, or as a plain
    int otherwise (the 3-bit field can hold 4..7, which we don't model); an
    unknown command must not crash the reader.
    """
    cmd: PPoGATTType | int = byte & 0x07
    try:
        cmd = PPoGATTType(cmd)
    except ValueError:
        pass
    return cmd, (byte >> 3) & 0x1F


class PPoGATTSession:
    """Sequence, window, and reassembly state for one PPoGATT link.

    TX flow control: PPoGATT has a send window (we advertise PPOGATT_WINDOW
    packets in the reset reply). Callers ask :meth:`can_send` before emitting
    a DATA packet, take a serial from :meth:`next_tx_seq`, and feed every
    inbound ACK to :meth:`on_ack`.

    RX dedup: we track the expected inbound 5-bit serial. Out-of-sequence
    DATA (a retransmit whose ACK we already sent, or a packet after loss) is
    reported back as a drop — the transport must still ACK it, but must NOT
    append it, so a lost ACK can't desync the length-prefixed framing.
    """

    def __init__(self) -> None:
        self.tx_seq = 0
        self.tx_inflight = 0
        self.rx_seq = 0
        self.reassembly = bytearray()

    def reset(self) -> None:
        self.tx_seq = 0
        self.tx_inflight = 0
        self.rx_seq = 0
        self.reassembly.clear()

    # ---- TX side ----
    def can_send(self) -> bool:
        return self.tx_inflight < PPOGATT_WINDOW

    def next_tx_seq(self) -> int:
        """Reserve the next 5-bit DATA serial and count it as in flight."""
        seq = self.tx_seq
        self.tx_seq = (self.tx_seq + 1) & 0x1F
        self.tx_inflight += 1
        return seq

    def on_ack(self) -> None:
        if self.tx_inflight > 0:
            self.tx_inflight -= 1

    # ---- RX side ----
    def on_data(self, serial: int, body: bytes) -> list[bytes] | None:
        """Feed one inbound DATA packet.

        Returns None if the packet was a duplicate/out-of-order and must be
        dropped (after ACKing), otherwise the list of complete Pebble Protocol
        messages that became available (possibly empty while a message is
        still being reassembled).
        """
        if serial != self.rx_seq:
            logger.warning(
                f"PPoGATT DATA serial={serial}, expected {self.rx_seq} — dropping (duplicate or out-of-order)",
            )
            return None
        self.rx_seq = (self.rx_seq + 1) & 0x1F
        self.reassembly += body
        return self._drain()

    def _drain(self) -> list[bytes]:
        out: list[bytes] = []
        while len(self.reassembly) >= 4:
            length = struct.unpack(">H", self.reassembly[:2])[0]
            total = 4 + length
            if total > MAX_REASSEMBLY:
                logger.error(
                    f"PPoGATT framing desync (claimed length {length}); dropping reassembly buffer",
                )
                self.reassembly.clear()
                return out
            if len(self.reassembly) < total:
                break
            out.append(bytes(self.reassembly[:total]))
            del self.reassembly[:total]
        if len(self.reassembly) > MAX_REASSEMBLY:
            logger.error("PPoGATT reassembly buffer overflow; dropping buffer")
            self.reassembly.clear()
        return out

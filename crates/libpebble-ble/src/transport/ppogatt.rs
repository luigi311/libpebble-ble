//! PPoGATT — "Pebble Protocol over GATT" transport framing.
//!
//! Each PPoGATT packet: 1-byte header followed by an optional payload.
//!
//!   bits 0-2 : command  (3-bit, see PPoGATTType)
//!   bits 3-7 : serial   (5-bit sequence number, wraps at 32)
//!
//! DATA packets carry one Pebble Protocol message, possibly split across
//! several DATA packets if it exceeds the negotiated ATT MTU.
//!
//! Reset handshake:
//!   watch sends RESET_REQUEST (0x02). We reply {0x03, 0x19, 0x19} if the
//!   request carried a payload, else {0x03}. The 0x19 bytes advertise our
//!   rx/tx window sizes (25 packets each).
//!   ACK header: (serial << 3) | 1.
//!
//! RX windowing:
//!   The watch sends up to PPOGATT_WINDOW packets without waiting for ACKs.
//!   We buffer ahead-of-sequence packets (serial within the window but beyond
//!   rx_seq) so that when the gap fills we can deliver the whole run and send
//!   one cumulative ACK rather than forcing per-packet retransmits.

use std::collections::HashMap;

/// Window size we advertise in the reset reply and honor on both TX and RX.
pub const PPOGATT_WINDOW: u8 = 0x19;

/// Drop the reassembly buffer if it grows beyond this (framing desync).
pub const MAX_REASSEMBLY: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PPoGATTType {
    Data = 0,
    Ack = 1,
    ResetRequest = 2,
    ResetComplete = 3,
}

impl PPoGATTType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Data),
            1 => Some(Self::Ack),
            2 => Some(Self::ResetRequest),
            3 => Some(Self::ResetComplete),
            _ => None,
        }
    }
}

pub fn ppogatt_header(packet_type: PPoGATTType, seq: u8) -> u8 {
    (packet_type as u8 & 0x07) | ((seq & 0x1F) << 3)
}

/// Returns (command_byte, serial). Command is the raw 3-bit field; the
/// caller must convert to PPoGATTType and handle unknowns gracefully.
pub fn parse_ppogatt_header(byte: u8) -> (u8, u8) {
    (byte & 0x07, (byte >> 3) & 0x1F)
}

/// Sequence, window, and reassembly state for one PPoGATT link.
///
/// TX flow control: callers check `can_send()` before emitting a DATA
/// packet, take a serial from `next_tx_seq()`, and feed every inbound ACK
/// to `on_ack()`.
///
/// RX windowing: we buffer ahead-of-sequence packets (within the window)
/// so that when the missing serial arrives we can deliver the whole run
/// and ACK the highest consecutive serial in one go.
pub struct PPoGATTSession {
    pub tx_seq: u8,
    pub tx_ack_seq: u8,
    pub tx_inflight: u8,
    pub rx_seq: u8,
    reassembly: Vec<u8>,
    /// Packets received out-of-order but within the window, keyed by serial.
    rx_buffer: HashMap<u8, Vec<u8>>,
}

impl PPoGATTSession {
    pub fn new() -> Self {
        Self {
            tx_seq: 0,
            tx_ack_seq: 0,
            tx_inflight: 0,
            rx_seq: 0,
            reassembly: Vec::new(),
            rx_buffer: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.tx_seq = 0;
        self.tx_ack_seq = 0;
        self.tx_inflight = 0;
        self.rx_seq = 0;
        self.reassembly.clear();
        self.rx_buffer.clear();
    }

    // ---- TX side ----
    pub fn can_send(&self) -> bool {
        self.tx_inflight < PPOGATT_WINDOW
    }

    pub fn next_tx_seq(&mut self) -> u8 {
        let seq = self.tx_seq;
        self.tx_seq = (self.tx_seq + 1) & 0x1F;
        self.tx_inflight += 1;
        seq
    }

    /// Cumulative ACK: the watch ACKs the highest serial it has received,
    /// confirming every in-flight packet up to and including it.
    pub fn on_ack(&mut self, serial: u8) {
        let covered = ((serial.wrapping_sub(self.tx_ack_seq)) & 0x1F) + 1;
        if covered > self.tx_inflight {
            // Serial is behind tx_ack_seq (stale duplicate) or beyond the
            // in-flight window (watch ACKed something we never sent).
            tracing::warn!(
                "PPoGATT ACK serial={serial} out-of-window \
                 (covered={covered} inflight={}); ignoring",
                self.tx_inflight
            );
            return;
        }
        self.tx_inflight -= covered;
        self.tx_ack_seq = (serial + 1) & 0x1F;
    }

    // ---- RX side ----
    /// Feed one inbound DATA packet.
    ///
    /// Returns `Some(messages)` when one or more complete Pebble Protocol
    /// messages are now available (in-order delivery or a gap just filled).
    /// The caller must ACK `rx_seq - 1` (the highest consecutive serial).
    ///
    /// Returns `None` when the packet was buffered (ahead-of-sequence within
    /// the window) or is a duplicate (behind rx_seq). No ACK should be sent
    /// for buffered packets; duplicates can be ignored since we already ACKed.
    pub fn on_data(&mut self, serial: u8, body: &[u8]) -> Option<Vec<Vec<u8>>> {
        // 5-bit distance from expected: 0 = in-order, 1..window-1 = ahead, window.. = behind.
        let diff = serial.wrapping_sub(self.rx_seq) & 0x1F;

        if diff == 0 {
            // In-order: consume this packet then drain any buffered followers.
            self.rx_seq = (self.rx_seq + 1) & 0x1F;
            self.reassembly.extend_from_slice(body);
            while let Some(buffered) = self.rx_buffer.remove(&self.rx_seq) {
                self.rx_seq = (self.rx_seq + 1) & 0x1F;
                self.reassembly.extend_from_slice(&buffered);
            }
            Some(self.drain())
        } else if diff < PPOGATT_WINDOW {
            // Ahead-of-sequence but within the window: buffer for later.
            tracing::trace!(
                "PPoGATT DATA serial={serial} buffered (expected {}, diff={diff})",
                self.rx_seq
            );
            self.rx_buffer.insert(serial, body.to_vec());
            None
        } else {
            // Behind rx_seq: duplicate we already ACKed (normal retransmit after
            // a lost ACK). Safe to ignore; debug-level since it isn't an error.
            tracing::debug!(
                "PPoGATT DATA serial={serial} duplicate (expected {}), ignoring",
                self.rx_seq
            );
            None
        }
    }

    fn drain(&mut self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            if self.reassembly.len() < 4 {
                break;
            }
            let length = u16::from_be_bytes([self.reassembly[0], self.reassembly[1]]) as usize;
            let total = 4 + length;
            if total > MAX_REASSEMBLY {
                tracing::error!(
                    "PPoGATT framing desync (claimed length {length}); dropping reassembly buffer"
                );
                self.reassembly.clear();
                return out;
            }
            if self.reassembly.len() < total {
                break;
            }
            out.push(self.reassembly[..total].to_vec());
            self.reassembly.drain(..total);
        }
        if self.reassembly.len() > MAX_REASSEMBLY {
            tracing::error!("PPoGATT reassembly buffer overflow; dropping buffer");
            self.reassembly.clear();
        }
        out
    }
}

impl Default for PPoGATTSession {
    fn default() -> Self {
        Self::new()
    }
}

//! DataLog endpoint (6778 decimal / 0x1A7A) — watch-initiated logging sessions for health data.
//!
//! Protocol flow (phone initiates a sync):
//!   phone  → HealthSync endpoint 911 (or REPORTSESSIONS 0x84)
//!   watch  → OPENSESSION(id, uuid, timestamp, tag, item_type, item_size)
//!   phone  → ACK(id)
//!   watch  → SENDDATA(id, items_left, crc, <record bytes>)  [repeated]
//!   phone  → ACK(id)
//!   watch  → CLOSE(id)
//!   phone  → ACK(id)
//!
//! Health sessions have app_uuid = all-zeros. The tag field identifies the
//! data type: 81 = per-minute steps, 83 = sleep, 84 = activity sessions, 85 = HR.

// Commands: watch → phone
pub const DATALOG_OPENSESSION: u8 = 0x01;
pub const DATALOG_SENDDATA: u8 = 0x02;
pub const DATALOG_CLOSE: u8 = 0x03;
pub const DATALOG_TIMEOUT: u8 = 0x07;

// Commands: phone → watch
pub const DATALOG_REPORTSESSIONS: u8 = 0x84;
pub const DATALOG_ACK: u8 = 0x85;
pub const DATALOG_NACK: u8 = 0x86;

/// DataLog tag values for Pebble Health (all sessions have app_uuid = all-zeros).
pub mod tag {
    /// Per-minute step / activity data.
    pub const ACTIVITY_STEPS: u32 = 81;
    /// Per-minute sleep-state data.
    pub const SLEEP: u32 = 83;
    /// Aggregated activity-session records (workouts, open sessions).
    pub const ACTIVITY_SESSIONS: u32 = 84;
    /// Per-minute heart-rate data (Pebble Time Round and later).
    pub const HEART_RATE: u32 = 85;
}

/// Metadata about one open DataLog session, keyed by its 1-byte handle.
#[derive(Debug, Clone)]
pub struct DatalogSession {
    pub id: u8,
    /// 16-byte UUID; all-zeros for health sessions.
    pub app_uuid: [u8; 16],
    /// Unix timestamp when the session was opened on the watch.
    pub opened_at: u32,
    pub tag: u32,
    /// Item type: 0 = byte array, 1 = uint, 2 = int.
    pub item_type: u8,
    /// Bytes per record.
    pub item_size: u16,
}

/// A batch of records delivered in one SENDDATA message.
#[derive(Debug, Clone)]
pub struct DatalogData {
    pub tag: u32,
    /// 16-byte UUID; all-zeros for health sessions.
    pub app_uuid: [u8; 16],
    /// Unix timestamp when the session was opened on the watch.
    pub session_timestamp: u32,
    /// Records still queued on the watch after this batch (0 = this is the last).
    pub items_left: u32,
    pub item_type: u8,
    pub item_size: u16,
    /// Raw record bytes. Contains `data.len() / item_size as usize` complete records.
    pub data: Vec<u8>,
}

/// Parse the command byte and session handle from a DataLog payload.
/// Returns `(cmd, handle, rest)`.
pub fn parse_header(payload: &[u8]) -> Option<(u8, u8, &[u8])> {
    if payload.len() < 2 {
        return None;
    }
    Some((payload[0], payload[1], &payload[2..]))
}

/// Parse an OPENSESSION body (the bytes after command + handle).
pub fn parse_opensession(id: u8, data: &[u8]) -> Option<DatalogSession> {
    // uuid(16) + opened_at(4) + tag(4) + item_type(1) + item_size(2) = 27 bytes minimum
    if data.len() < 27 {
        return None;
    }
    let mut app_uuid = [0u8; 16];
    app_uuid.copy_from_slice(&data[..16]);
    let opened_at = u32::from_le_bytes([data[16], data[17], data[18], data[19]]);
    let tag = u32::from_le_bytes([data[20], data[21], data[22], data[23]]);
    let item_type = data[24];
    let item_size = u16::from_le_bytes([data[25], data[26]]);
    Some(DatalogSession { id, app_uuid, opened_at, tag, item_type, item_size })
}

/// Parse a SENDDATA body (the bytes after command + handle).
/// Returns `(items_left, crc, record_bytes)`.
pub fn parse_senddata(data: &[u8]) -> Option<(u32, u32, &[u8])> {
    if data.len() < 8 {
        return None;
    }
    let items_left = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let crc = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    Some((items_left, crc, &data[8..]))
}

/// Build an ACK or NACK reply for a session handle.
pub fn build_reply(handle: u8, ack: bool) -> Vec<u8> {
    vec![if ack { DATALOG_ACK } else { DATALOG_NACK }, handle]
}

/// Build a REPORTSESSIONS request. Sending this to the watch prompts it to
/// open DataLog sessions for any pending data (useful after reconnection).
pub fn build_report_sessions() -> Vec<u8> {
    vec![DATALOG_REPORTSESSIONS]
}

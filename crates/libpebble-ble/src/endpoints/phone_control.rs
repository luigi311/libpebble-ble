//! Phone call control (endpoint 33).
//!
//! Phone → Watch commands (builders):
//!   IncomingCall  (0x04)  — show incoming call screen
//!   MissedCall    (0x06)  — missed call notification
//!   Ring          (0x07)  — start ringing
//!   Start         (0x08)  — call answered / in progress
//!   End           (0x09)  — call ended / hung up
//!
//! Watch → Phone commands (parsed):
//!   Answer        (0x01)  — user tapped answer on watch
//!   Hangup        (0x02)  — user tapped decline/hangup on watch

/// Action the watch sent back to the phone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhoneAction {
    /// User tapped Answer on the watch.
    Answer { cookie: u32 },
    /// User tapped Decline / Hang Up on the watch.
    Hangup { cookie: u32 },
}

// ── Wire constants ──────────────────────────────────────────────────────

const CMD_ANSWER: u8 = 0x01;
const CMD_HANGUP: u8 = 0x02;
const CMD_INCOMING_CALL: u8 = 0x04;
const CMD_MISSED_CALL: u8 = 0x06;
const CMD_RING: u8 = 0x07;
const CMD_START: u8 = 0x08;
const CMD_END: u8 = 0x09;

/// Parse a watch → phone payload (1-byte command + 4-byte cookie).
pub fn parse_phone_action(payload: &[u8]) -> Option<PhoneAction> {
    if payload.len() < 5 {
        return None;
    }
    let cookie = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
    match payload[0] {
        CMD_ANSWER => Some(PhoneAction::Answer { cookie }),
        CMD_HANGUP => Some(PhoneAction::Hangup { cookie }),
        _ => None,
    }
}

// ── Builders (phone → watch) ────────────────────────────────────────────

/// `cookie` is an arbitrary u32 that the watch echoes back in Answer/Hangup
/// so the phone can match the action to the right call.
pub fn build_incoming_call(cookie: u32, caller_number: &str, caller_name: &str) -> Vec<u8> {
    build_call_string(CMD_INCOMING_CALL, cookie, caller_number, caller_name)
}

pub fn build_missed_call(cookie: u32, caller_number: &str, caller_name: &str) -> Vec<u8> {
    build_call_string(CMD_MISSED_CALL, cookie, caller_number, caller_name)
}

pub fn build_ring(cookie: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    out.push(CMD_RING);
    out.extend_from_slice(&cookie.to_le_bytes());
    out
}

pub fn build_call_start(cookie: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    out.push(CMD_START);
    out.extend_from_slice(&cookie.to_le_bytes());
    out
}

pub fn build_call_end(cookie: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    out.push(CMD_END);
    out.extend_from_slice(&cookie.to_le_bytes());
    out
}

fn build_call_string(cmd: u8, cookie: u32, caller_number: &str, caller_name: &str) -> Vec<u8> {
    let num = truncate_bytes(caller_number, 31);
    let name = truncate_bytes(caller_name, 31);
    let mut out = Vec::with_capacity(5 + 1 + num.len() + 1 + name.len());
    out.push(cmd);
    out.extend_from_slice(&cookie.to_le_bytes());
    // SString: 1-byte length prefix, no null terminator.
    out.push(num.len() as u8);
    out.extend_from_slice(num.as_bytes());
    out.push(name.len() as u8);
    out.extend_from_slice(name.as_bytes());
    out
}

/// Truncate to `max` bytes (not chars), rounding down to a valid UTF-8
/// boundary, and strip interior nulls.
fn truncate_bytes(s: &str, max: usize) -> &str {
    let s = if s.len() > max {
        let mut end = max;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        &s[..end]
    } else {
        s
    };
    if let Some(pos) = s.bytes().position(|b| b == 0) {
        &s[..pos]
    } else {
        s
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_answer() {
        let pkt = [CMD_ANSWER, 0x2A, 0x00, 0x00, 0x00];
        assert_eq!(parse_phone_action(&pkt), Some(PhoneAction::Answer { cookie: 42 }));
    }

    #[test]
    fn parse_hangup() {
        let pkt = [CMD_HANGUP, 0xFF, 0xFF, 0xFF, 0x7F];
        assert_eq!(parse_phone_action(&pkt), Some(PhoneAction::Hangup { cookie: i32::MAX as u32 }));
    }

    #[test]
    fn parse_rejects_short() {
        assert_eq!(parse_phone_action(&[CMD_ANSWER]), None);
        assert_eq!(parse_phone_action(&[]), None);
    }

    #[test]
    fn parse_rejects_unknown_cmd() {
        let pkt = [0xFF, 0, 0, 0, 0];
        assert_eq!(parse_phone_action(&pkt), None);
    }

    #[test]
    fn build_ring_and_start() {
        assert_eq!(build_ring(0x12345678), vec![CMD_RING, 0x78, 0x56, 0x34, 0x12]);
        assert_eq!(build_call_start(1), vec![CMD_START, 1, 0, 0, 0]);
        assert_eq!(build_call_end(0), vec![CMD_END, 0, 0, 0, 0]);
    }

    #[test]
    fn incoming_call_round_trips_strings() {
        let pkt = build_incoming_call(7, "555-1234", "Alice");
        assert_eq!(&pkt[0..1], &[CMD_INCOMING_CALL]);
        assert_eq!(u32::from_le_bytes([pkt[1], pkt[2], pkt[3], pkt[4]]), 7);
        // SString format: [len u8][bytes]
        let num_len = pkt[5] as usize;
        assert_eq!(num_len, 8);
        assert_eq!(&pkt[6..6 + num_len], b"555-1234");
        let name_len = pkt[6 + num_len] as usize;
        assert_eq!(name_len, 5);
        assert_eq!(&pkt[7 + num_len..7 + num_len + name_len], b"Alice");
        // Total: 1 cmd + 4 cookie + 1 num_len + 8 num + 1 name_len + 5 name = 20
        assert_eq!(pkt.len(), 20);
    }

    #[test]
    fn truncates_long_strings() {
        let long = "a".repeat(40);
        let pkt = build_incoming_call(0, &long, &long);
        // 1 cmd + 4 cookie + 1 len + 31 num + 1 len + 31 name = 69
        assert_eq!(pkt.len(), 69);
        assert_eq!(pkt[5], 31); // num len
        assert_eq!(pkt[6 + 31], 31); // name len
    }
}

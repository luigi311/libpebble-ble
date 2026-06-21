//! Ping/Pong endpoint (2001).

pub fn parse_ping(payload: &[u8]) -> Option<u32> {
    if payload.len() >= 5 && payload[0] == 0x00 {
        Some(u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]))
    } else {
        None
    }
}

pub fn build_pong(cookie: u32) -> Vec<u8> {
    let mut out = vec![0x01u8];
    out.extend_from_slice(&cookie.to_be_bytes());
    out
}

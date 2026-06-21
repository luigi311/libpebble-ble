//! Time endpoint (11) — UTC clock synchronisation.

pub fn build_set_utc(utc_timestamp: u32, utc_offset_minutes: i16, tz_name: &str) -> Vec<u8> {
    let name = tz_name.as_bytes();
    let name = &name[..name.len().min(0xFF)];
    let mut out = vec![0x03u8]; // TIME_SETTIME_UTC
    out.extend_from_slice(&utc_timestamp.to_be_bytes());
    out.extend_from_slice(&utc_offset_minutes.to_be_bytes());
    out.push(name.len() as u8);
    out.extend_from_slice(name);
    out
}

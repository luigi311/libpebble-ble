//! Pebble Protocol endpoints.
//!
//! Each sub-module owns one endpoint: its wire types, builders, and parsers.
//! The framing layer (Endpoint enum, pebble_pack, pebble_unpack) lives here.
//!
//! Every Pebble Protocol message: [u16 length BE][u16 endpoint BE][payload].

pub mod app_message;
pub mod app_run_state;
pub mod blob_db;
pub mod phone_version;
pub mod ping;
pub mod time;

pub use app_message::AppMessageValue;
pub use app_run_state::AppRunStateCmd;

use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Endpoint {
    Time = 11,
    PhoneVersion = 17,
    SystemMessage = 18,
    AppMessage = 48,
    AppRunState = 52,
    BlobDb = 0xB1DB,
    Ping = 2001,
    AppFetch = 6001,
}

impl Endpoint {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            11 => Some(Self::Time),
            17 => Some(Self::PhoneVersion),
            18 => Some(Self::SystemMessage),
            48 => Some(Self::AppMessage),
            52 => Some(Self::AppRunState),
            0xB1DB => Some(Self::BlobDb),
            2001 => Some(Self::Ping),
            6001 => Some(Self::AppFetch),
            _ => None,
        }
    }
}

pub fn pebble_pack(endpoint: Endpoint, payload: &[u8]) -> Option<Vec<u8>> {
    let len = u16::try_from(payload.len()).ok()?;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&(endpoint as u16).to_be_bytes());
    out.extend_from_slice(payload);
    Some(out)
}

/// Returns `(endpoint_raw, payload)`. `endpoint_raw` is `u16` — callers map
/// to `Endpoint` themselves so unknown endpoints don't crash the reader.
pub fn pebble_unpack(data: &[u8]) -> Option<(u16, &[u8])> {
    if data.len() < 4 {
        return None;
    }
    let length = u16::from_be_bytes([data[0], data[1]]) as usize;
    let endpoint = u16::from_be_bytes([data[2], data[3]]);
    let end = 4 + length;
    if data.len() < end {
        return None;
    }
    Some((endpoint, &data[4..end]))
}

pub(crate) fn uuid_to_bytes(uuid: &str) -> Option<[u8; 16]> {
    Uuid::parse_str(uuid).ok().map(|u| *u.as_bytes())
}

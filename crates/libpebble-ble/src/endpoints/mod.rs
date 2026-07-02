//! Pebble Protocol endpoints.
//!
//! Each sub-module owns one endpoint: its wire types, builders, and parsers.
//! The framing layer (Endpoint enum, pebble_pack, pebble_unpack) lives here.
//!
//! Every Pebble Protocol message: [u16 length BE][u16 endpoint BE][payload].

pub mod app_message;
pub mod app_run_state;
pub mod blob_db;
pub mod datalog;
pub mod health;
pub mod music;
pub mod phone_control;
pub mod phone_version;
pub mod ping;
pub mod reset;
pub mod screenshot;
pub mod system;
pub mod time;
pub mod watch_pref;

pub use app_message::AppMessageValue;
pub use app_run_state::AppRunStateCmd;
pub use phone_control::PhoneAction;

use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Endpoint {
    Recovery = 0,
    Time = 11,
    WatchVersion = 16,
    PhoneVersion = 17,
    SystemMessage = 18,
    MusicControl = 32,
    PhoneControl = 33,
    AppMessage = 48,
    LegacyAppLaunch = 49,
    AppCustomize = 50,
    BleControl = 51,
    AppRunState = 52,
    HealthSync = 911,
    Logs = 2000,
    Ping = 2001,
    LogDump = 2002,
    Reset = 2003,
    AppLogs = 2006,
    SystemRegistration = 5000,
    FactoryRegistry = 5001,
    AppFetch = 6001,
    DataLog = 6778,
    Screenshot = 8000,
    FileInstallManager = 8181,
    GetBytes = 9000,
    AudioStreaming = 10000,
    VoiceControl = 11000,
    TimelineActions = 11440,
    AppReorder = 43981,
    BlobDb = 45531,
    BlobDbV2 = 45787,
    PutBytes = 0xBEEF,
}

impl Endpoint {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            11 => Some(Self::Time),
            16 => Some(Self::WatchVersion),
            17 => Some(Self::PhoneVersion),
            18 => Some(Self::SystemMessage),
            32 => Some(Self::MusicControl),
            33 => Some(Self::PhoneControl),
            48 => Some(Self::AppMessage),
            52 => Some(Self::AppRunState),
            911 => Some(Self::HealthSync),
            45531 => Some(Self::BlobDb),
            45787 => Some(Self::BlobDbV2),
            2001 => Some(Self::Ping),
            2003 => Some(Self::Reset),
            5001 => Some(Self::FactoryRegistry),
            6001 => Some(Self::AppFetch),
            6778 => Some(Self::DataLog),
            8000 => Some(Self::Screenshot),
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

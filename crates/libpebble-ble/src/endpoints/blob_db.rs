//! BlobDB endpoint (0xb1db) — key/value database writes, including notifications.

use uuid::Uuid;

use crate::uuids::NOTIFICATIONS_APP_UUID;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlobDBCommand {
    Insert = 0x01,
    Delete = 0x04,
    Clear = 0x05,
    /// Like Insert but carries a timestamp; requires BlobDB2 v1 (weather, pins with time).
    InsertWithTimestamp = 0x0D,
}

/// Pebble weather condition icons shown in the built-in weather app.
/// Matches libpebble3's WeatherManager.WeatherType enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WeatherType {
    PartlyCloudy = 0,
    CloudyDay    = 1,
    LightSnow    = 2,
    LightRain    = 3,
    HeavyRain    = 4,
    HeavySnow    = 5,
    Generic      = 6,
    Sun          = 7,
    RainAndSnow  = 8,
    Unknown      = 255,
}

impl WeatherType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::PartlyCloudy,
            1 => Self::CloudyDay,
            2 => Self::LightSnow,
            3 => Self::LightRain,
            4 => Self::HeavyRain,
            5 => Self::HeavySnow,
            6 => Self::Generic,
            7 => Self::Sun,
            8 => Self::RainAndSnow,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlobDBId {
    Pin = 1,
    App = 2,
    Reminder = 3,
    Notification = 4,
    Weather = 5,
    CannedResponses = 6,
    HealthParams = 7,
    Contacts = 8,
    /// App configuration/preferences (e.g. "weatherApp" location list).
    AppConfigs = 9,
    HealthStats = 10,
    AppGlance = 11,
    /// Watch-side preferences DB (BlobDB2 only — used in MarkAllDirty to trigger sync).
    WatchPrefs = 12,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlobDBStatus {
    Success = 1,
    GeneralFailure = 2,
    InvalidOperation = 3,
    InvalidDatabaseId = 4,
    InvalidData = 5,
    KeyDoesNotExist = 6,
    DatabaseFull = 7,
    DataStale = 8,
    NotSupported = 9,
    Locked = 10,
    TryLater = 11,
}

impl BlobDBStatus {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Success),
            2 => Some(Self::GeneralFailure),
            3 => Some(Self::InvalidOperation),
            4 => Some(Self::InvalidDatabaseId),
            5 => Some(Self::InvalidData),
            6 => Some(Self::KeyDoesNotExist),
            7 => Some(Self::DatabaseFull),
            8 => Some(Self::DataStale),
            9 => Some(Self::NotSupported),
            10 => Some(Self::Locked),
            11 => Some(Self::TryLater),
            _ => None,
        }
    }
}

pub fn build_blobdb_insert(db: BlobDBId, key: &[u8; 16], blob: &[u8], token: u16) -> Result<Vec<u8>, &'static str> {
    let blob_len = u16::try_from(blob.len()).map_err(|_| "BlobDB blob exceeds 65535 bytes")?;
    let mut out = Vec::new();
    out.push(BlobDBCommand::Insert as u8);
    out.extend_from_slice(&token.to_le_bytes());
    out.push(db as u8);
    out.push(16u8); // key length
    out.extend_from_slice(key);
    out.extend_from_slice(&blob_len.to_le_bytes());
    out.extend_from_slice(blob);
    Ok(out)
}

/// Like `build_blobdb_insert` but with an arbitrary byte-string key (e.g. "activityPreferences").
pub fn build_blobdb_str_insert(db: BlobDBId, key: &str, blob: &[u8], token: u16) -> Result<Vec<u8>, &'static str> {
    let key_bytes = key.as_bytes();
    let key_len = u8::try_from(key_bytes.len()).map_err(|_| "BlobDB string key exceeds 255 bytes")?;
    let blob_len = u16::try_from(blob.len()).map_err(|_| "BlobDB blob exceeds 65535 bytes")?;
    let mut out = Vec::new();
    out.push(BlobDBCommand::Insert as u8);
    out.extend_from_slice(&token.to_le_bytes());
    out.push(db as u8);
    out.push(key_len);
    out.extend_from_slice(key_bytes);
    out.extend_from_slice(&blob_len.to_le_bytes());
    out.extend_from_slice(blob);
    Ok(out)
}

/// Like `build_blobdb_insert` but with an explicit unix timestamp (u32).
/// Required for BlobDB records that carry their own timestamp (weather, pins).
/// Wire: `[0x0D][token 2B LE][db 1B][timestamp 4B LE][keyLen 1B][key N][valueLen 2B LE][value M]`
pub fn build_blobdb_insert_with_timestamp(
    db: BlobDBId,
    key: &[u8; 16],
    blob: &[u8],
    timestamp: u32,
    token: u16,
) -> Result<Vec<u8>, &'static str> {
    let blob_len = u16::try_from(blob.len()).map_err(|_| "BlobDB blob exceeds 65535 bytes")?;
    let mut out = Vec::new();
    out.push(BlobDBCommand::InsertWithTimestamp as u8);
    out.extend_from_slice(&token.to_le_bytes());
    out.push(db as u8);
    out.extend_from_slice(&timestamp.to_le_bytes());
    out.push(16u8);
    out.extend_from_slice(key);
    out.extend_from_slice(&blob_len.to_le_bytes());
    out.extend_from_slice(blob);
    Ok(out)
}

/// Build a `WeatherAppBlobRecord` matching the format expected by the Pebble weather app.
///
/// Binary layout (all little-endian):
///   version u8=3 | currentTemp i16 | currentWeatherType u8 | todayHigh i16 | todayLow i16
///   tomorrowWeatherType u8 | tomorrowHigh i16 | tomorrowLow i16 | lastUpdateTimeUtc u32
///   isCurrentLocation u8 | allStringsLength u16 | locationName SLongString | forecastShort SLongString
///
/// SLongString = [u16 LE length][utf-8 bytes]
#[allow(clippy::too_many_arguments)]
pub fn build_weather_blob(
    location_name: &str,
    forecast_short: &str,
    current_temp: i16,
    current_weather: WeatherType,
    today_high: i16,
    today_low: i16,
    tomorrow_weather: WeatherType,
    tomorrow_high: i16,
    tomorrow_low: i16,
    timestamp: u32,
    is_current_location: bool,
) -> Vec<u8> {
    let loc = location_name.as_bytes();
    let fc = forecast_short.as_bytes();
    let all_strings_len = (loc.len() + 2 + fc.len() + 2) as u16;

    let mut out = Vec::new();
    out.push(3u8); // version
    out.extend_from_slice(&current_temp.to_le_bytes());
    out.push(current_weather as u8);
    out.extend_from_slice(&today_high.to_le_bytes());
    out.extend_from_slice(&today_low.to_le_bytes());
    out.push(tomorrow_weather as u8);
    out.extend_from_slice(&tomorrow_high.to_le_bytes());
    out.extend_from_slice(&tomorrow_low.to_le_bytes());
    out.extend_from_slice(&timestamp.to_le_bytes());
    out.push(is_current_location as u8);
    out.extend_from_slice(&all_strings_len.to_le_bytes());
    out.extend_from_slice(&(loc.len() as u16).to_le_bytes());
    out.extend_from_slice(loc);
    out.extend_from_slice(&(fc.len() as u16).to_le_bytes());
    out.extend_from_slice(fc);
    out
}

/// Build the `WeatherPrefsBlobItem` value for the `AppConfigs` `"weatherApp"` key.
///
/// Wire format: `[numLocations: u8][uuid1: 16 bytes][uuid2: 16 bytes]...`
///
/// This tells the watch which location UUIDs are active in the Weather BlobDB.
/// Without this entry the watch weather app shows "no location information".
pub fn build_weather_prefs_blob(location_keys: &[[u8; 16]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + location_keys.len() * 16);
    out.push(location_keys.len() as u8);
    for key in location_keys {
        out.extend_from_slice(key);
    }
    out
}

pub fn parse_blobdb_response(payload: &[u8]) -> Option<(u16, u8)> {
    if payload.len() < 3 {
        return None;
    }
    let token = u16::from_le_bytes([payload[0], payload[1]]);
    let status = payload[2];
    Some((token, status))
}

/// Pebble system icon resource IDs (0x80000000 flag = system resource).
/// Values from Gadgetbridge's PebbleIconID enum.
mod icon_id {
    pub const GENERIC: u32 = 0x80000037;
    pub const EMAIL: u32 = 0x8000003C;
    pub const SMS: u32 = 0x80000035;
    pub const MISSED_CALL: u32 = 0x80000005;
    pub const TWITTER: u32 = 0x8000000E;
    pub const FACEBOOK: u32 = 0x80000007;
    pub const FACEBOOK_MESSENGER: u32 = 0x8000006F;
    pub const INSTAGRAM: u32 = 0x80000029;
    pub const GOOGLE_HANGOUTS: u32 = 0x80000022;
    pub const WHATSAPP: u32 = 0x80000057;
}

/// Pebble 8-bit ARGB color constants (alpha always 0b11 = opaque).
/// Format: bits [7:6]=alpha, [5:4]=Red, [3:2]=Green, [1:0]=Blue (2 bits each).
mod pebble_color {
    pub const BLUE: u8 = 0xC3;           // 11_00_00_11
    pub const GREEN: u8 = 0xCC;          // 11_00_11_00
    pub const RED: u8 = 0xF0;            // 11_11_00_00
    pub const VIVID_CERULEAN: u8 = 0xCB; // 11_00_10_11 — Twitter blue
    pub const JAZZBERRY_JAM: u8 = 0xD5;  // 11_01_01_01 — Facebook pink/red
    pub const ORANGE: u8 = 0xF4;         // 11_11_01_00 — Instagram orange
}

/// Notification category, used to select the correct Pebble icon and background
/// color so the notification renders with the right visual treatment on the watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NotificationCategory {
    #[default]
    Generic,
    Email,
    /// SMS and general instant-messaging apps.
    Messaging,
    MissedCall,
    Twitter,
    Facebook,
    FacebookMessenger,
    Instagram,
    Hangouts,
    WhatsApp,
}

impl NotificationCategory {
    fn icon(self) -> u32 {
        match self {
            Self::Generic => icon_id::GENERIC,
            Self::Email => icon_id::EMAIL,
            Self::Messaging => icon_id::SMS,
            Self::MissedCall => icon_id::MISSED_CALL,
            Self::Twitter => icon_id::TWITTER,
            Self::Facebook => icon_id::FACEBOOK,
            Self::FacebookMessenger => icon_id::FACEBOOK_MESSENGER,
            Self::Instagram => icon_id::INSTAGRAM,
            Self::Hangouts => icon_id::GOOGLE_HANGOUTS,
            Self::WhatsApp => icon_id::WHATSAPP,
        }
    }

    fn background_color(self) -> u8 {
        match self {
            Self::Generic => pebble_color::BLUE,
            Self::Email => pebble_color::BLUE,
            Self::Messaging => pebble_color::GREEN,
            Self::MissedCall => pebble_color::RED,
            Self::Twitter => pebble_color::VIVID_CERULEAN,
            Self::Facebook => pebble_color::JAZZBERRY_JAM,
            Self::FacebookMessenger => pebble_color::JAZZBERRY_JAM,
            Self::Instagram => pebble_color::ORANGE,
            Self::Hangouts => pebble_color::GREEN,
            Self::WhatsApp => pebble_color::GREEN,
        }
    }
}

fn build_notification_blob(
    title: &str,
    body: &str,
    subtitle: &str,
    timestamp: u32,
    category: NotificationCategory,
) -> Result<Vec<u8>, &'static str> {
    let parent_uuid = Uuid::parse_str(NOTIFICATIONS_APP_UUID).unwrap().into_bytes();
    let item_uuid = Uuid::new_v4().into_bytes();

    let mut attrs = Vec::new();
    let mut attr_count = 0u8;
    for (attr_id, value) in [(1u8, title), (2u8, subtitle), (3u8, body)] {
        if !value.is_empty() {
            let raw = value.as_bytes();
            let raw_len = u16::try_from(raw.len()).map_err(|_| "notification text attribute exceeds 65535 bytes")?;
            attrs.push(attr_id);
            attrs.extend_from_slice(&raw_len.to_le_bytes());
            attrs.extend_from_slice(raw);
            attr_count += 1;
        }
    }
    // Attribute 4: icon (u32)
    attrs.push(4u8);
    attrs.extend_from_slice(&4u16.to_le_bytes());
    attrs.extend_from_slice(&category.icon().to_le_bytes());
    attr_count += 1;
    // Attribute 28: background color (u8)
    attrs.push(28u8);
    attrs.extend_from_slice(&1u16.to_le_bytes());
    attrs.push(category.background_color());
    attr_count += 1;

    let attrs_len = u16::try_from(attrs.len()).map_err(|_| "notification attributes exceed 65535 bytes")?;
    let mut out = Vec::new();
    out.extend_from_slice(&item_uuid);
    out.extend_from_slice(&parent_uuid);
    out.extend_from_slice(&timestamp.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // duration
    out.push(0x01); // type = notification
    out.extend_from_slice(&0x0001u16.to_le_bytes()); // flags
    out.push(0x04); // layout = notification
    out.extend_from_slice(&attrs_len.to_le_bytes());
    out.push(attr_count);
    out.push(0); // action count
    out.extend_from_slice(&attrs);
    Ok(out)
}

pub fn build_notification(
    title: &str,
    body: &str,
    subtitle: &str,
    timestamp: u32,
    token: u16,
    category: NotificationCategory,
) -> Result<Vec<u8>, &'static str> {
    let blob = build_notification_blob(title, body, subtitle, timestamp, category)?;
    let key: [u8; 16] = Uuid::new_v4().into_bytes();
    build_blobdb_insert(BlobDBId::Notification, &key, &blob, token)
}

// ---------------------------------------------------------------------------
// BlobDB V2 — bidirectional sync protocol (endpoint 0xB2DB)
// ---------------------------------------------------------------------------

// Phone → Watch command bytes
const BLOBDB2_CMD_VERSION: u8        = 0x0B;
const BLOBDB2_CMD_MARK_ALL_DIRTY: u8 = 0x0C;

// Watch → Phone command bytes (watch-initiated)
const BLOBDB2_CMD_WRITE: u8          = 0x08;
const BLOBDB2_CMD_WRITEBACK: u8      = 0x09;
const BLOBDB2_CMD_SYNCDONE: u8       = 0x0A;

// Response bytes (0x80 | request_cmd)
const BLOBDB2_RESP_DIRTY_DATABASE: u8  = 0x86; // watch → phone
const BLOBDB2_RESP_START_SYNC: u8      = 0x87; // watch → phone
const BLOBDB2_RESP_WRITE: u8           = 0x88; // phone → watch
const BLOBDB2_RESP_WRITEBACK: u8       = 0x89; // phone → watch
const BLOBDB2_RESP_SYNCDONE: u8        = 0x8A; // phone → watch
const BLOBDB2_RESP_VERSION: u8         = 0x8B; // watch → phone
const BLOBDB2_RESP_MARK_ALL_DIRTY: u8  = 0x8C; // watch → phone

/// Raw DB-ID constant for matching incoming BlobDB2 Write messages that carry WatchPrefs records.
/// Matches `BlobDBId::WatchPrefs as u8`; kept as a plain constant so it can be used in match arms.
pub const BLOBDB2_DB_WATCH_PREFS: u8 = 12;

/// A `Write` or `WriteBack` record sent by the watch to the phone.
///
/// Wire layout: `[cmd][token 2B LE][db 1B][timestamp 4B LE][keySize 1B][key N][valueSize 2B LE][value M]`
#[derive(Debug, Clone)]
pub struct BlobDB2Write {
    pub token: u16,
    pub is_writeback: bool,
    pub db: u8,
    pub timestamp: u32,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Decoded incoming BlobDB2 message (watch → phone direction plus responses to phone-initiated requests).
#[derive(Debug)]
pub enum BlobDB2Incoming {
    /// Watch pushes a record to us (Write or WriteBack).
    Write(BlobDB2Write),
    /// Watch signals sync is complete for a database.
    SyncDone { token: u16, db: u8 },
    /// Watch replies to our `Version` query.
    VersionResponse { token: u16, status: u8, version: u8 },
    /// Watch replies to our `MarkAllDirty` command.
    MarkAllDirtyResponse { token: u16, status: u8 },
    /// Watch replies to our `DirtyDatabase` command.
    DirtyDatabaseResponse { token: u16, status: u8, dirty_dbs: Vec<u8> },
    /// Watch replies to our `StartSync` command.
    StartSyncResponse { token: u16, status: u8 },
}

impl BlobDB2Incoming {
    /// Returns the token for response variants (None for watch-initiated messages).
    pub fn response_token(&self) -> Option<u16> {
        match self {
            Self::VersionResponse { token, .. }      => Some(*token),
            Self::MarkAllDirtyResponse { token, .. } => Some(*token),
            Self::DirtyDatabaseResponse { token, .. } => Some(*token),
            Self::StartSyncResponse { token, .. }    => Some(*token),
            _ => None,
        }
    }
}

/// Build a `Version` query packet (phone → watch, cmd=0x0B).
pub fn build_blobdb2_version(token: u16) -> Vec<u8> {
    let mut out = vec![BLOBDB2_CMD_VERSION];
    out.extend_from_slice(&token.to_le_bytes());
    out
}

/// Build a `MarkAllDirty` command (phone → watch, cmd=0x0C).
pub fn build_blobdb2_mark_all_dirty(token: u16, db: BlobDBId) -> Vec<u8> {
    let mut out = vec![BLOBDB2_CMD_MARK_ALL_DIRTY];
    out.extend_from_slice(&token.to_le_bytes());
    out.push(db as u8);
    out
}

/// ACK a `Write` from the watch (phone → watch, cmd=0x88).
pub fn build_blobdb2_write_response(token: u16, status: BlobDBStatus) -> Vec<u8> {
    blobdb2_status_response(BLOBDB2_RESP_WRITE, token, status)
}

/// ACK a `WriteBack` from the watch (phone → watch, cmd=0x89).
pub fn build_blobdb2_writeback_response(token: u16, status: BlobDBStatus) -> Vec<u8> {
    blobdb2_status_response(BLOBDB2_RESP_WRITEBACK, token, status)
}

/// ACK a `SyncDone` from the watch (phone → watch, cmd=0x8A).
pub fn build_blobdb2_syncdone_response(token: u16, status: BlobDBStatus) -> Vec<u8> {
    blobdb2_status_response(BLOBDB2_RESP_SYNCDONE, token, status)
}

fn blobdb2_status_response(cmd: u8, token: u16, status: BlobDBStatus) -> Vec<u8> {
    let mut out = vec![cmd];
    out.extend_from_slice(&token.to_le_bytes());
    out.push(status as u8);
    out
}

/// Parse an incoming BlobDB2 message from the watch.
///
/// All messages share a 3-byte header: `[cmd 1B][token 2B LE]`.
pub fn parse_blobdb2_incoming(payload: &[u8]) -> Option<BlobDB2Incoming> {
    if payload.len() < 3 {
        return None;
    }
    let cmd = payload[0];
    let token = u16::from_le_bytes([payload[1], payload[2]]);
    let rest = &payload[3..];

    match cmd {
        BLOBDB2_CMD_WRITE | BLOBDB2_CMD_WRITEBACK => {
            // [db 1B][timestamp 4B LE][keySize 1B][key keySize B][valueSize 2B LE][value valueSize B]
            if rest.len() < 7 {
                return None;
            }
            let db = rest[0];
            let timestamp = u32::from_le_bytes([rest[1], rest[2], rest[3], rest[4]]);
            let key_size = rest[5] as usize;
            let off = 6;
            if rest.len() < off + key_size + 2 {
                return None;
            }
            let key = rest[off..off + key_size].to_vec();
            let off = off + key_size;
            let val_size = u16::from_le_bytes([rest[off], rest[off + 1]]) as usize;
            let off = off + 2;
            if rest.len() < off + val_size {
                return None;
            }
            let value = rest[off..off + val_size].to_vec();
            Some(BlobDB2Incoming::Write(BlobDB2Write {
                token,
                is_writeback: cmd == BLOBDB2_CMD_WRITEBACK,
                db,
                timestamp,
                key,
                value,
            }))
        }
        BLOBDB2_CMD_SYNCDONE => {
            if rest.is_empty() {
                return None;
            }
            Some(BlobDB2Incoming::SyncDone { token, db: rest[0] })
        }
        BLOBDB2_RESP_VERSION => {
            if rest.len() < 2 {
                return None;
            }
            Some(BlobDB2Incoming::VersionResponse { token, status: rest[0], version: rest[1] })
        }
        BLOBDB2_RESP_MARK_ALL_DIRTY => {
            if rest.is_empty() {
                return None;
            }
            Some(BlobDB2Incoming::MarkAllDirtyResponse { token, status: rest[0] })
        }
        BLOBDB2_RESP_DIRTY_DATABASE => {
            if rest.len() < 2 {
                return None;
            }
            let status = rest[0];
            let count = rest[1] as usize;
            if rest.len() < 2 + count {
                return None;
            }
            let dirty_dbs = rest[2..2 + count].to_vec();
            Some(BlobDB2Incoming::DirtyDatabaseResponse { token, status, dirty_dbs })
        }
        BLOBDB2_RESP_START_SYNC => {
            if rest.is_empty() {
                return None;
            }
            Some(BlobDB2Incoming::StartSyncResponse { token, status: rest[0] })
        }
        _ => None,
    }
}

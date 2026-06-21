//! BlobDB endpoint (0xb1db) — key/value database writes, including notifications.

use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlobDBCommand {
    Insert = 0x01,
    Delete = 0x04,
    Clear = 0x05,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlobDBId {
    Pin = 1,
    App = 2,
    Reminder = 3,
    Notification = 4,
    Weather = 5,
    CannedMessages = 6,
    Preferences = 7,
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

pub fn parse_blobdb_response(payload: &[u8]) -> Option<(u16, u8)> {
    if payload.len() < 3 {
        return None;
    }
    let token = u16::from_le_bytes([payload[0], payload[1]]);
    let status = payload[2];
    Some((token, status))
}

// ---------------------------------------------------------------------------
// Notifications (built on top of BlobDB inserts)
// ---------------------------------------------------------------------------

const NOTIFICATIONS_APP_UUID: &str = "b2cae818-10f8-46df-ad2b-98ad2254a3c1";

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

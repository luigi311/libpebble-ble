//! System endpoints: WatchVersion (16) and SystemMessage (18).
//!
//! Mirrors libpebble3's `packets/System.kt` + `services/SystemService.kt`.
//!
//! `WatchVersion` (endpoint 16): phone sends a 1-byte request; the watch replies
//! with running + recovery firmware versions, board/serial/BT address, resource
//! info, language, and a protocol-capabilities bitfield.
//!
//! `SystemMessage` (endpoint 18): firmware-update lifecycle + reconnect control.
//! Wire layout is `[command=0x00][messageType][body]`.

use std::cmp::Ordering;

// ---------------------------------------------------------------------------
// WatchVersion (endpoint 16)
// ---------------------------------------------------------------------------

/// `WatchVersionRequest` command byte (phone → watch).
pub const WATCH_VERSION_REQUEST: u8 = 0x00;
/// `WatchVersionResponse` command byte (watch → phone).
pub const WATCH_VERSION_RESPONSE: u8 = 0x01;

/// Build a `WatchVersionRequest` (endpoint 16). Just the command byte.
pub fn build_watch_version_request() -> Vec<u8> {
    vec![WATCH_VERSION_REQUEST]
}

/// Firmware-property flag bits (libpebble3 `FirmwareProperty`).
mod fw_flag {
    pub const IS_RECOVERY: u8 = 1 << 0;
    pub const IS_BLE: u8 = 1 << 1;
    pub const IS_DUAL_SLOT: u8 = 1 << 2;
    pub const IS_SLOT0: u8 = 1 << 3;
}

/// Raw per-slot firmware version block (libpebble3 `WatchFirmwareVersion`): 47 bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchFirmwareVersion {
    pub timestamp: u32,
    /// e.g. "v4.0.1-tag" (null-padded 32-byte field on the wire).
    pub version_tag: String,
    pub git_hash: String,
    pub flags: u8,
    /// Hardware platform code (see libpebble3 `WatchHardwarePlatform`; not mapped here).
    pub hardware_platform: u8,
    pub metadata_version: u8,
}

impl WatchFirmwareVersion {
    pub fn is_recovery(&self) -> bool {
        self.flags & fw_flag::IS_RECOVERY != 0
    }
    pub fn is_ble(&self) -> bool {
        self.flags & fw_flag::IS_BLE != 0
    }
    pub fn is_dual_slot(&self) -> bool {
        self.flags & fw_flag::IS_DUAL_SLOT != 0
    }
    pub fn is_slot0(&self) -> bool {
        self.flags & fw_flag::IS_SLOT0 != 0
    }

    /// Parse this raw block into a semantic [`FirmwareVersion`]. Returns `None`
    /// if the version tag doesn't parse (libpebble3 `firmwareVersion()`).
    pub fn parsed(&self) -> Option<FirmwareVersion> {
        let (major, minor, patch, suffix) = parse_version_tag(&self.version_tag)?;
        Some(FirmwareVersion {
            string_version: self.version_tag.clone(),
            timestamp: self.timestamp,
            major,
            minor,
            patch,
            suffix,
            git_hash: self.git_hash.clone(),
            is_recovery: self.is_recovery(),
            is_dual_slot: self.is_dual_slot(),
            is_slot0: self.is_slot0(),
        })
    }
}

/// A parsed firmware version (libpebble3 `FirmwareVersion`).
///
/// Ordering compares `(major, minor, patch)` then `timestamp`; the suffix and
/// git hash are ignored, matching libpebble3's comparator.
#[derive(Debug, Clone)]
pub struct FirmwareVersion {
    pub string_version: String,
    pub timestamp: u32,
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    /// Pre-release tag after the first `-` ("" when absent).
    pub suffix: String,
    pub git_hash: String,
    pub is_recovery: bool,
    pub is_dual_slot: bool,
    pub is_slot0: bool,
}

impl FirmwareVersion {
    fn code(&self) -> u64 {
        self.patch as u64 + self.minor as u64 * 1_000 + self.major as u64 * 1_000_000
    }

    /// Active slot for dual-slot firmware (0 or 1), or `None` if not dual-slot.
    pub fn slot(&self) -> Option<u8> {
        match (self.is_dual_slot, self.is_slot0) {
            (true, true) => Some(0),
            (true, false) => Some(1),
            _ => None,
        }
    }
}

impl PartialEq for FirmwareVersion {
    fn eq(&self, other: &Self) -> bool {
        self.code() == other.code() && self.timestamp == other.timestamp
    }
}
impl Eq for FirmwareVersion {}
impl PartialOrd for FirmwareVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for FirmwareVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        self.code()
            .cmp(&other.code())
            .then_with(|| self.timestamp.cmp(&other.timestamp))
    }
}

/// Parse a firmware version tag like "v4.0.1-tag" into `(major, minor, patch, suffix)`.
/// `suffix` is everything after the first `-` ("" if none). Returns `None` if the
/// major/minor components are missing or non-numeric.
fn parse_version_tag(tag: &str) -> Option<(u32, u32, u32, String)> {
    let stripped = tag.strip_prefix('v').unwrap_or(tag);
    let (version, suffix) = match stripped.split_once('-') {
        Some((v, s)) => (v, s.to_string()),
        None => (stripped, String::new()),
    };
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    Some((major, minor, patch, suffix))
}

/// Decoded `WatchVersionResponse` (libpebble3 `WatchInfo`, minus watch color which
/// comes from a separate factory-registry endpoint).
#[derive(Debug, Clone)]
pub struct WatchVersionInfo {
    pub running: FirmwareVersion,
    pub recovery: Option<FirmwareVersion>,
    pub bootloader_timestamp: u32,
    pub board: String,
    pub serial: String,
    /// Bluetooth MAC, big-endian-formatted (e.g. "AA:BB:CC:DD:EE:FF").
    pub bt_address: String,
    pub resource_crc: u32,
    pub resource_timestamp: u32,
    pub language: String,
    pub language_version: u16,
    pub hardware_platform: u8,
    /// Protocol-capabilities bitfield: bit N corresponds to `ProtocolCapsFlag` value N.
    pub capabilities: u64,
    pub is_unfaithful: bool,
    pub health_insights_version: Option<u16>,
    pub javascript_version: Option<u16>,
}

impl WatchVersionInfo {
    /// Watch display family for this watch's hardware platform.
    pub fn watch_type(&self) -> WatchType {
        hardware_platform(self.hardware_platform).0
    }
    /// Board revision string (e.g. "snowy_dvt") for this watch's hardware platform.
    pub fn platform_revision(&self) -> &'static str {
        hardware_platform(self.hardware_platform).1
    }
}

/// Watch display/hardware family (libpebble3 `WatchType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchType {
    Aplite,
    Basalt,
    Chalk,
    Diorite,
    Emery,
    Flint,
    Gabbro,
    Unknown,
}

impl WatchType {
    /// Codename string used by app variants ("aplite", "basalt", …). `Unknown` → "unknown".
    pub fn codename(self) -> &'static str {
        match self {
            Self::Aplite => "aplite",
            Self::Basalt => "basalt",
            Self::Chalk => "chalk",
            Self::Diorite => "diorite",
            Self::Emery => "emery",
            Self::Flint => "flint",
            Self::Gabbro => "gabbro",
            Self::Unknown => "unknown",
        }
    }

    /// Whether this watch has a color display (libpebble3 `WatchType.isColor`).
    pub fn is_color(self) -> bool {
        matches!(self, Self::Basalt | Self::Chalk | Self::Emery | Self::Gabbro)
    }

    /// Whether this watch supports Bluetooth Classic (libpebble3 `supportsBtClassic`).
    pub fn supports_bt_classic(self) -> bool {
        matches!(self, Self::Aplite | Self::Basalt | Self::Chalk)
    }
}

/// Map a hardware-platform protocol number to `(watch type, board revision)`.
/// Mirrors libpebble3 `WatchHardwarePlatform`.
pub fn hardware_platform(code: u8) -> (WatchType, &'static str) {
    use WatchType::*;
    match code {
        1 => (Aplite, "ev1"),
        2 => (Aplite, "ev2"),
        3 => (Aplite, "ev2_3"),
        4 => (Aplite, "ev2_4"),
        5 => (Aplite, "v1_5"),
        6 => (Aplite, "v2_0"),
        7 => (Basalt, "snowy_evt2"),
        8 => (Basalt, "snowy_dvt"),
        9 => (Chalk, "spalding_evt"),
        10 => (Basalt, "snowy_s3"),
        11 => (Chalk, "spalding"),
        12 => (Diorite, "silk_evt"),
        13 => (Emery, "robert_evt"),
        14 => (Diorite, "silk"),
        15 => (Flint, "asterix"),
        16 => (Emery, "obelix_evt"),
        17 => (Emery, "obelix_dvt"),
        18 => (Emery, "obelix_pvt"),
        19 => (Gabbro, "getafix_evt"),
        20 => (Gabbro, "getafix_dvt"),
        21 => (Gabbro, "getafix_dvt2"),
        243 => (Emery, "obelix_bb2"),
        244 => (Emery, "obelix_bb"),
        247 => (Emery, "robert_bb2"),
        248 => (Diorite, "silk_bb2"),
        249 => (Emery, "robert_bb"),
        250 => (Diorite, "silk_bb"),
        251 => (Chalk, "spalding_bb2"),
        252 => (Basalt, "unk"),
        253 => (Basalt, "snowy_bb2"),
        254 => (Aplite, "bb2"),
        255 => (Aplite, "bigboard"),
        _ => (Unknown, "unknown"),
    }
}

/// A forward byte cursor for parsing fixed-layout watch replies.
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.data.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u16_be(&mut self) -> Option<u16> {
        let b = self.take(2)?;
        Some(u16::from_be_bytes([b[0], b[1]]))
    }
    fn u32_be(&mut self) -> Option<u32> {
        let b = self.take(4)?;
        Some(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    /// A fixed-width, null-padded string field (C-string within `n` bytes).
    fn fixed_string(&mut self, n: usize) -> Option<String> {
        let bytes = self.take(n)?;
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        Some(String::from_utf8_lossy(&bytes[..end]).into_owned())
    }
}

fn parse_firmware_block(c: &mut Cursor) -> Option<WatchFirmwareVersion> {
    Some(WatchFirmwareVersion {
        timestamp: c.u32_be()?,
        version_tag: c.fixed_string(32)?,
        git_hash: c.fixed_string(8)?,
        flags: c.u8()?,
        hardware_platform: c.u8()?,
        metadata_version: c.u8()?,
    })
}

/// Format 6 big-endian MAC bytes as "AA:BB:CC:DD:EE:FF" (libpebble3 reverses the
/// wire bytes, which are little-endian on the link).
fn format_mac(bytes: &[u8]) -> String {
    bytes
        .iter()
        .rev()
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// Parse a `WatchVersionResponse` payload (endpoint 16, including the leading
/// command byte). Returns `None` if it's not a response, is truncated in a fixed
/// field, or the running firmware tag doesn't parse.
pub fn parse_watch_version_response(payload: &[u8]) -> Option<WatchVersionInfo> {
    let mut c = Cursor::new(payload);
    if c.u8()? != WATCH_VERSION_RESPONSE {
        return None;
    }
    let running_raw = parse_firmware_block(&mut c)?;
    let recovery_raw = parse_firmware_block(&mut c)?;
    let bootloader_timestamp = c.u32_be()?;
    let board = c.fixed_string(9)?;
    let serial = c.fixed_string(12)?;
    let bt_address = format_mac(c.take(6)?);
    let resource_crc = c.u32_be()?;
    let resource_timestamp = c.u32_be()?;
    let language = c.fixed_string(6)?;
    let language_version = c.u16_be()?;
    let capabilities = u64::from_le_bytes(c.take(8)?.try_into().ok()?);

    // Trailing optionals: present only when the watch sent enough bytes.
    let is_unfaithful = c.u8().map(|b| b != 0).unwrap_or(true);
    let health_insights_version = c.u16_be();
    let javascript_version = c.u16_be();

    Some(WatchVersionInfo {
        running: running_raw.parsed()?,
        recovery: recovery_raw.parsed(),
        bootloader_timestamp,
        board,
        serial,
        bt_address,
        resource_crc,
        resource_timestamp,
        language,
        language_version,
        hardware_platform: running_raw.hardware_platform,
        capabilities,
        is_unfaithful,
        health_insights_version,
        javascript_version,
    })
}

// ---------------------------------------------------------------------------
// SystemMessage (endpoint 18)
// ---------------------------------------------------------------------------

/// `SystemMessage` message-type bytes (libpebble3 `SystemMessage.Message`).
pub mod system_message {
    pub const NEW_FIRMWARE_AVAILABLE: u8 = 0x00;
    pub const FIRMWARE_UPDATE_START: u8 = 0x01;
    pub const FIRMWARE_UPDATE_COMPLETE: u8 = 0x02;
    pub const FIRMWARE_UPDATE_FAILED: u8 = 0x03;
    pub const FIRMWARE_UP_TO_DATE: u8 = 0x04;
    pub const STOP_RECONNECTING: u8 = 0x06;
    pub const START_RECONNECTING: u8 = 0x07;
    pub const MAP_DISABLED: u8 = 0x08;
    pub const MAP_ENABLED: u8 = 0x09;
    pub const FIRMWARE_UPDATE_START_RESPONSE: u8 = 0x0a;
}

/// Status in a `FirmwareUpdateStartResponse` (libpebble3 `FirmwareUpdateStartStatus`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirmwareUpdateStartStatus {
    Stopped,
    Started,
    Cancelled,
    Unknown(u8),
}

impl FirmwareUpdateStartStatus {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Stopped,
            1 => Self::Started,
            2 => Self::Cancelled,
            other => Self::Unknown(other),
        }
    }
}

/// Build a body-less `SystemMessage` (e.g. `STOP_RECONNECTING`). Wire: `[0x00][type]`.
pub fn build_system_message(message_type: u8) -> Vec<u8> {
    vec![0x00, message_type]
}

/// Build a `FirmwareUpdateStart` message (endpoint 18).
/// Wire: `[0x00][0x01][bytesAlreadyTransferred u32 LE][bytesToSend u32 LE]`.
pub fn build_firmware_update_start(bytes_already: u32, bytes_to_send: u32) -> Vec<u8> {
    let mut out = vec![0x00, system_message::FIRMWARE_UPDATE_START];
    out.extend_from_slice(&bytes_already.to_le_bytes());
    out.extend_from_slice(&bytes_to_send.to_le_bytes());
    out
}

/// The message-type byte of an inbound `SystemMessage` (`payload[1]`).
pub fn system_message_type(payload: &[u8]) -> Option<u8> {
    payload.get(1).copied()
}

/// Parse a `FirmwareUpdateStartResponse` (`[0x00][0x0a][status]`). The leading
/// SystemMessage command byte is always 0x00.
pub fn parse_firmware_update_start_response(payload: &[u8]) -> Option<FirmwareUpdateStartStatus> {
    if payload.len() < 3
        || payload[0] != 0x00
        || payload[1] != system_message::FIRMWARE_UPDATE_START_RESPONSE
    {
        return None;
    }
    Some(FirmwareUpdateStartStatus::from_u8(payload[2]))
}

// ---------------------------------------------------------------------------
// WatchFactoryData / factory registry (endpoint 5001)
// ---------------------------------------------------------------------------

/// `WatchFactoryData` command bytes (libpebble3 `WatchFactoryData.Message`).
pub const FACTORY_DATA_REQUEST: u8 = 0x00;
pub const FACTORY_DATA_RESPONSE: u8 = 0x01;
pub const FACTORY_DATA_ERROR: u8 = 0xff;

/// Build a factory-registry read request for `key` (endpoint 5001).
/// Wire: `[0x00][keyLen u8][key bytes]` (the key is an `SString`).
/// Returns `None` if `key` is longer than 255 bytes (won't fit the length byte).
pub fn build_factory_data_request(key: &str) -> Option<Vec<u8>> {
    let key_bytes = key.as_bytes();
    let key_len = u8::try_from(key_bytes.len()).ok()?;
    let mut out = Vec::with_capacity(2 + key_bytes.len());
    out.push(FACTORY_DATA_REQUEST);
    out.push(key_len);
    out.extend_from_slice(key_bytes);
    Some(out)
}

/// Build a request for the watch's manufacturing color ("mfg_color").
pub fn build_watch_color_request() -> Vec<u8> {
    build_factory_data_request("mfg_color").expect("mfg_color fits the length byte")
}

/// Parse a `WatchFactoryDataResponse` (`[0x01][len u8][value...]`) into the raw
/// value bytes. Returns `None` for a non-response, an error reply, or truncation.
pub fn parse_factory_data_response(payload: &[u8]) -> Option<Vec<u8>> {
    if payload.first() != Some(&FACTORY_DATA_RESPONSE) {
        return None;
    }
    let len = *payload.get(1)? as usize;
    Some(payload.get(2..2 + len)?.to_vec())
}

/// Decode an "mfg_color" factory value (a 4-byte big-endian int) into the watch
/// color protocol number, then look it up. `None` if the value is short or the
/// color is unknown.
pub fn parse_watch_color(value: &[u8]) -> Option<&'static WatchColorInfo> {
    let n = i32::from_be_bytes(value.get(..4)?.try_into().ok()?);
    watch_color(n)
}

/// A watch color/variant (libpebble3 `WatchColor`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchColorInfo {
    pub protocol_number: i32,
    /// Canonical id used by JS apps, e.g. "pebble_2_hr_charcoal_red".
    pub js_name: &'static str,
    /// Human description, e.g. "Pebble 2 HR - Charcoal Red".
    pub description: &'static str,
    pub watch_type: WatchType,
    pub supports_hrm: bool,
}

/// Look up a watch color by its protocol number. `None` if unknown.
pub fn watch_color(protocol_number: i32) -> Option<&'static WatchColorInfo> {
    WATCH_COLORS
        .iter()
        .find(|c| c.protocol_number == protocol_number)
}

macro_rules! watch_colors {
    ($( ($num:expr, $js:expr, $desc:expr, $ty:expr, $hrm:expr) ),* $(,)?) => {
        &[ $( WatchColorInfo { protocol_number: $num, js_name: $js, description: $desc, watch_type: $ty, supports_hrm: $hrm } ),* ]
    };
}

/// All known watch colors (libpebble3 `WatchColor`). The two polished-PTR remap
/// targets (protocol -999) are omitted since they're never reported by the watch.
const WATCH_COLORS: &[WatchColorInfo] = watch_colors![
    (1, "pebble_black", "Pebble Classic - Black", WatchType::Aplite, false),
    (2, "pebble_white", "Pebble Classic - White", WatchType::Aplite, false),
    (3, "pebble_red", "Pebble Classic - Red", WatchType::Aplite, false),
    (4, "pebble_orange", "Pebble Classic - Orange", WatchType::Aplite, false),
    (5, "pebble_pink", "Pebble Classic - Pink", WatchType::Aplite, false),
    (6, "pebble_steel_silver", "Pebble Steel - Silver", WatchType::Aplite, false),
    (7, "pebble_steel_gunmetal", "Pebble Steel - Gunmetal", WatchType::Aplite, false),
    (8, "pebble_fly_blue", "Pebble Classic - Fly Blue", WatchType::Aplite, false),
    (9, "pebble_fresh_green", "Pebble Classic - Fresh Green", WatchType::Aplite, false),
    (10, "pebble_hot_pink", "Pebble Classic - Hot Pink", WatchType::Aplite, false),
    (11, "pebble_time_white", "Pebble Time - White", WatchType::Basalt, false),
    (12, "pebble_time_black", "Pebble Time - Black", WatchType::Basalt, false),
    (13, "pebble_time_red", "Pebble Time - Red", WatchType::Basalt, false),
    (14, "pebble_time_steel_silver", "Pebble Time Steel - Silver", WatchType::Basalt, false),
    (15, "pebble_time_steel_black", "Pebble Time Steel - Black", WatchType::Basalt, false),
    (16, "pebble_time_steel_gold", "Pebble Time Steel - Gold", WatchType::Basalt, false),
    (17, "pebble_time_round_silver", "Pebble Time Round - Silver", WatchType::Chalk, false),
    (18, "pebble_time_round_black", "Pebble Time Round - Black", WatchType::Chalk, false),
    (19, "pebble_time_round_silver_20", "Pebble Time Round - Silver", WatchType::Chalk, false),
    (20, "pebble_time_round_black_20", "Pebble Time Round - Black", WatchType::Chalk, false),
    (21, "pebble_time_round_rose_gold", "Pebble Time Round - Rose Gold", WatchType::Chalk, false),
    (22, "pebble_time_round_silver_rainbow", "Pebble Time Round - Silver Rainbow", WatchType::Chalk, false),
    (23, "pebble_time_round_black_rainbow", "Pebble Time Round - Black Rainbow", WatchType::Chalk, false),
    (24, "pebble_2_se_black_charcoal", "Pebble 2 SE - Black Charcoal", WatchType::Diorite, false),
    (25, "pebble_2_hr_black_charcoal", "Pebble 2 HR - Black Charcoal", WatchType::Diorite, true),
    (26, "pebble_2_se_white_gray", "Pebble 2 SE - White/Gray", WatchType::Diorite, false),
    (27, "pebble_2_hr_charcoal_sorbet_green", "Pebble 2 HR - Sorbet Green", WatchType::Diorite, true),
    (28, "pebble_2_hr_charcoal_red", "Pebble 2 HR - Charcoal Red", WatchType::Diorite, true),
    (29, "pebble_2_hr_white_gray", "Pebble 2 HR - White Gray", WatchType::Diorite, true),
    (30, "pebble_2_hr_white_turquoise", "Pebble 2 HR - White Turquoise", WatchType::Diorite, true),
    (31, "pebble_time_2_black", "Pebble Time 2 - Black", WatchType::Emery, true),
    (32, "pebble_time_2_silver", "Pebble Time 2 - Silver", WatchType::Emery, true),
    (33, "pebble_time_2_gold", "Pebble Time 2 - Gold", WatchType::Emery, true),
    (34, "pebble_2_duo_black", "Pebble 2 Duo - Black", WatchType::Flint, false),
    (35, "pebble_2_duo_white", "Pebble 2 Duo - White", WatchType::Flint, false),
    (36, "pebble_time_2_black_gray", "Pebble Time 2 - Black/Gray", WatchType::Emery, true),
    (37, "pebble_time_2_black_red", "Pebble Time 2 - Black/Red", WatchType::Emery, true),
    (38, "pebble_time_2_silver_blue", "Pebble Time 2 - Silver/Blue", WatchType::Emery, true),
    (39, "pebble_time_2_silver_gray", "Pebble Time 2 - Silver/Gray", WatchType::Emery, true),
    (40, "pebble_round_2_black", "Pebble Round 2 - Black", WatchType::Gabbro, false),
    (41, "pebble_round_2_silver", "Pebble Round 2 - Silver", WatchType::Gabbro, false),
    (42, "pebble_round_2_gold", "Pebble Round 2 - Gold", WatchType::Gabbro, false),
];

#[cfg(test)]
mod tests {
    use super::*;

    // --- firmware version tag parsing (mirrors libpebble3 FirmwareVersionTest) ---

    fn wfv(tag: &str) -> WatchFirmwareVersion {
        WatchFirmwareVersion {
            timestamp: 123456789,
            version_tag: tag.to_string(),
            git_hash: "ABCDEFG".to_string(),
            flags: 1, // IsRecovery bit set in the reference test
            hardware_platform: 0,
            metadata_version: 0,
        }
    }

    #[test]
    fn parse_fw_version_with_tag() {
        let v = wfv("v4.0.1-tag").parsed().expect("parses");
        assert_eq!((v.major, v.minor, v.patch), (4, 0, 1));
        assert_eq!(v.suffix, "tag");
        assert_eq!(v.git_hash, "ABCDEFG");
        assert!(v.is_recovery);
    }

    #[test]
    fn parse_fw_version_no_tag() {
        let v = wfv("v4.0.2").parsed().expect("parses");
        assert_eq!((v.major, v.minor, v.patch), (4, 0, 2));
        assert_eq!(v.suffix, "");
    }

    #[test]
    fn parse_fw_version_no_patch() {
        let v = wfv("v4.0-prf4").parsed().expect("parses");
        assert_eq!((v.major, v.minor, v.patch), (4, 0, 0));
        assert_eq!(v.suffix, "prf4");
    }

    #[test]
    fn parse_fw_version_rejects_garbage() {
        assert!(wfv("not-a-version").parsed().is_none());
    }

    // --- ordering (mirrors libpebble3 SystemServiceTest.firmwareVersionComparator) ---

    #[test]
    fn firmware_version_ordering() {
        let mk = |major, minor, patch, ts: u32| FirmwareVersion {
            string_version: String::new(),
            timestamp: ts,
            major,
            minor,
            patch,
            suffix: String::new(),
            git_hash: String::new(),
            is_recovery: false,
            is_dual_slot: false,
            is_slot0: false,
        };
        let v3_0_0 = mk(3, 0, 0, 0);
        let v3_1_0 = mk(3, 1, 0, 0);
        let v3_1_111 = mk(3, 1, 111, 0);
        let v3_2_0 = mk(3, 2, 0, 0);
        let v4_0_0 = mk(4, 0, 0, 0);
        let v4_0_0_later = mk(4, 0, 0, 1);

        assert!(v3_1_0 > v3_0_0);
        assert!(v3_1_111 > v3_1_0);
        assert!(v3_2_0 > v3_1_111);
        assert!(v4_0_0 > v3_2_0);
        assert!(v4_0_0_later > v4_0_0);
        // Same code + timestamp compare equal regardless of suffix.
        assert_eq!(v4_0_0, mk(4, 0, 0, 0));
        assert_ne!(v4_0_0, v4_0_0_later);
    }

    // --- WatchVersionResponse parsing ---

    fn pad(s: &str, n: usize) -> Vec<u8> {
        let mut b = s.as_bytes().to_vec();
        b.truncate(n);
        b.resize(n, 0);
        b
    }

    fn encode_block(ts: u32, tag: &str, git: &str, flags: u8, hw: u8, meta: u8) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&ts.to_be_bytes());
        v.extend_from_slice(&pad(tag, 32));
        v.extend_from_slice(&pad(git, 8));
        v.extend_from_slice(&[flags, hw, meta]);
        v
    }

    #[test]
    fn parse_watch_version_response_full() {
        let mut p = vec![WATCH_VERSION_RESPONSE];
        p.extend(encode_block(100, "v4.0.1", "deadbeef", 0b1100, 12, 1)); // running: dual-slot, slot0
        p.extend(encode_block(50, "v3.0-prf", "cafef00d", 0b0001, 12, 1)); // recovery
        p.extend_from_slice(&7u32.to_be_bytes()); // bootloader timestamp
        p.extend_from_slice(&pad("snowy_dvt", 9));
        p.extend_from_slice(&pad("Q402450001AB", 12));
        p.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06]); // bt addr (LE on wire)
        p.extend_from_slice(&0xCAFEu32.to_be_bytes()); // resource crc
        p.extend_from_slice(&200u32.to_be_bytes()); // resource timestamp
        p.extend_from_slice(&pad("en_US", 6));
        p.extend_from_slice(&5u16.to_be_bytes()); // language version
        // capabilities: bit 6 (HealthInsights) set -> byte0 = 0x40
        p.extend_from_slice(&[0x40, 0, 0, 0, 0, 0, 0, 0]);
        // optionals
        p.push(0x01); // is_unfaithful = true
        p.extend_from_slice(&3u16.to_be_bytes()); // health insights version
        p.extend_from_slice(&9u16.to_be_bytes()); // javascript version

        let info = parse_watch_version_response(&p).expect("parses");
        assert_eq!(info.running.string_version, "v4.0.1");
        assert_eq!(info.running.slot(), Some(0));
        assert_eq!(info.recovery.as_ref().unwrap().major, 3);
        assert_eq!(info.bootloader_timestamp, 7);
        assert_eq!(info.board, "snowy_dvt");
        assert_eq!(info.serial, "Q402450001AB");
        assert_eq!(info.bt_address, "06:05:04:03:02:01");
        assert_eq!(info.resource_crc, 0xCAFE);
        assert_eq!(info.language, "en_US");
        assert_eq!(info.language_version, 5);
        assert_eq!(info.capabilities & (1 << 6), 1 << 6); // HealthInsights
        assert!(info.is_unfaithful);
        assert_eq!(info.health_insights_version, Some(3));
        assert_eq!(info.javascript_version, Some(9));
    }

    #[test]
    fn parse_watch_version_response_without_optionals() {
        let mut p = vec![WATCH_VERSION_RESPONSE];
        p.extend(encode_block(100, "v4.0.1", "deadbeef", 0, 12, 1));
        p.extend(encode_block(50, "v3.0", "cafef00d", 1, 12, 1));
        p.extend_from_slice(&0u32.to_be_bytes());
        p.extend_from_slice(&pad("snowy", 9));
        p.extend_from_slice(&pad("SERIAL", 12));
        p.extend_from_slice(&[0; 6]);
        p.extend_from_slice(&0u32.to_be_bytes());
        p.extend_from_slice(&0u32.to_be_bytes());
        p.extend_from_slice(&pad("en", 6));
        p.extend_from_slice(&0u16.to_be_bytes());
        p.extend_from_slice(&[0; 8]);
        // no optional trailer

        let info = parse_watch_version_response(&p).expect("parses");
        assert!(info.is_unfaithful); // defaults true when absent
        assert_eq!(info.health_insights_version, None);
        assert_eq!(info.javascript_version, None);
    }

    #[test]
    fn watch_type_capabilities() {
        assert_eq!(WatchType::Basalt.codename(), "basalt");
        assert!(WatchType::Basalt.is_color());
        assert!(!WatchType::Aplite.is_color());
        assert!(WatchType::Chalk.supports_bt_classic());
        assert!(!WatchType::Diorite.supports_bt_classic());
    }

    #[test]
    fn watch_color_lookup_and_request() {
        assert_eq!(build_watch_color_request(), {
            let mut v = vec![0x00, 9];
            v.extend_from_slice(b"mfg_color");
            v
        });

        // mfg_color response: [0x01][len=4][BE i32 = 28]
        let response = [0x01, 4, 0, 0, 0, 28];
        let value = parse_factory_data_response(&response).expect("parses");
        let color = parse_watch_color(&value).expect("known color");
        assert_eq!(color.js_name, "pebble_2_hr_charcoal_red");
        assert_eq!(color.watch_type, WatchType::Diorite);
        assert!(color.supports_hrm);

        assert!(watch_color(24).is_some_and(|c| !c.supports_hrm)); // Pebble 2 SE
        assert!(watch_color(999).is_none());
        assert!(parse_factory_data_response(&[FACTORY_DATA_ERROR]).is_none());
    }

    #[test]
    fn hardware_platform_mapping() {
        assert_eq!(hardware_platform(8), (WatchType::Basalt, "snowy_dvt"));
        assert_eq!(hardware_platform(15), (WatchType::Flint, "asterix"));
        assert_eq!(hardware_platform(18), (WatchType::Emery, "obelix_pvt"));
        assert_eq!(hardware_platform(255), (WatchType::Aplite, "bigboard"));
        assert_eq!(hardware_platform(0), (WatchType::Unknown, "unknown"));
        assert_eq!(hardware_platform(99), (WatchType::Unknown, "unknown"));
    }

    #[test]
    fn parse_watch_version_response_rejects_short() {
        assert!(parse_watch_version_response(&[WATCH_VERSION_RESPONSE, 0, 0]).is_none());
        assert!(parse_watch_version_response(&[WATCH_VERSION_REQUEST]).is_none());
    }

    // --- SystemMessage ---

    #[test]
    fn system_message_builders() {
        assert_eq!(
            build_system_message(system_message::STOP_RECONNECTING),
            vec![0x00, 0x06]
        );
        assert_eq!(
            build_firmware_update_start(16, 1024),
            vec![0x00, 0x01, 16, 0, 0, 0, 0, 4, 0, 0],
        );
    }

    #[test]
    fn firmware_update_start_response_parse() {
        let payload = [0x00, system_message::FIRMWARE_UPDATE_START_RESPONSE, 0x01];
        assert_eq!(
            parse_firmware_update_start_response(&payload),
            Some(FirmwareUpdateStartStatus::Started),
        );
        assert_eq!(system_message_type(&payload), Some(0x0a));
        // Wrong message type, and wrong leading command byte, are both rejected.
        assert!(parse_firmware_update_start_response(&[0x00, 0x02, 0x01]).is_none());
        assert!(parse_firmware_update_start_response(&[0x05, 0x0a, 0x01]).is_none());
    }
}

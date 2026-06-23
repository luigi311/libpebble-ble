//! PhoneVersion endpoint (17) — capability advertisement to the watch.

// OS type occupies the lower nibble (bits 0–3) of the platform_flags field.
// Values match libpebble3's PhoneAppVersion.OSType enum.
#[allow(dead_code)]
mod remote_os {
    pub const UNKNOWN: u32 = 0;
    pub const IOS:     u32 = 1;
    pub const ANDROID: u32 = 2;
    pub const MACOS:   u32 = 3;
    pub const LINUX:   u32 = 4;
    pub const WINDOWS: u32 = 5;
}

// Platform capability flags occupy bits 4+ of the platform_flags field.
// Values match libpebble3's PhoneAppVersion.PlatformFlag enum.
#[allow(dead_code)]
mod platform_caps {
    pub const TELEPHONY:     u32 = 1 << 4;  // 16
    pub const SMS:           u32 = 1 << 5;  // 32
    pub const GPS:           u32 = 1 << 6;  // 64
    pub const BTLE:          u32 = 1 << 7;  // 128
    pub const CAMERA_REAR:   u32 = 1 << 8;  // 256
    pub const ACCELEROMETER: u32 = 1 << 9;  // 512
    pub const GYROSCOPE:     u32 = 1 << 10; // 1024
    pub const COMPASS:       u32 = 1 << 11; // 2048
}

// Protocol capability bits — 64-bit value sent little-endian (per Pebble protocol spec).
// Bit positions match libpebble3's ProtocolCapsFlag enum (value field).
#[allow(dead_code)]
mod protocol_caps {
    pub const APP_RUN_STATE: u64              = 1 << 0;  // SupportsAppRunStateProtocol
    pub const INFINITE_LOG_DUMPING: u64       = 1 << 1;  // SupportsInfiniteLogDump
    pub const UPDATED_MUSIC_PROTOCOL: u64     = 1 << 2;  // SupportsExtendedMusicProtocol
    pub const TWO_WAY_DISMISSAL: u64          = 1 << 3;  // SupportsTwoWayDismissal
    pub const LOCALIZATION: u64               = 1 << 4;  // SupportsLocalization
    pub const APP_MESSAGES_8K: u64            = 1 << 5;  // Supports8kAppMessage
    pub const HEALTH_INSIGHTS: u64            = 1 << 6;  // SupportsHealthInsights
    pub const APP_DICTATION: u64              = 1 << 7;  // SupportsAppDictation
    pub const SEND_TEXT_APP: u64              = 1 << 8;  // SupportsSendTextApp
    pub const NOTIFICATION_FILTERING: u64     = 1 << 9;  // SupportsNotificationFiltering
    pub const UNREAD_CORE_DUMP: u64           = 1 << 10; // SupportsUnreadCoreDump
    pub const WEATHER: u64                    = 1 << 11; // SupportsWeatherApp
    pub const REMINDERS_APP: u64              = 1 << 12; // SupportsRemindersApp
    pub const WORKOUT_APP: u64                = 1 << 13; // SupportsWorkoutApp
    pub const SMOOTH_FW_INSTALL_PROGRESS: u64 = 1 << 14; // SupportsSmoothFwInstallProgress
    pub const CUSTOM_VIBE_PATTERNS: u64       = 1 << 15; // SupportsCustomVibePatterns
    pub const JS_BYTECODE_VERSION: u64        = 1 << 16; // JavascriptBytecodeVersionAppended
    pub const FW_UPDATE_ACROSS_DISCONNECT: u64 = 1 << 21; // SupportsFwUpdateAcrossDisconnection
    pub const BLOB_DB_VERSION: u64            = 1 << 22; // SupportsBlobDbVersion
    pub const SETTINGS_SYNC: u64              = 1 << 23; // SupportsSettingsSync
}

pub fn build_phone_version_response() -> Vec<u8> {
    let platform_flags = remote_os::LINUX
        | platform_caps::TELEPHONY
        | platform_caps::SMS
        | platform_caps::GPS
        | platform_caps::BTLE;

    let protocol_caps = protocol_caps::APP_RUN_STATE
        | protocol_caps::INFINITE_LOG_DUMPING
        | protocol_caps::UPDATED_MUSIC_PROTOCOL
        | protocol_caps::TWO_WAY_DISMISSAL
        | protocol_caps::LOCALIZATION
        | protocol_caps::APP_MESSAGES_8K
        | protocol_caps::HEALTH_INSIGHTS
        | protocol_caps::NOTIFICATION_FILTERING
        | protocol_caps::WEATHER
        | protocol_caps::BLOB_DB_VERSION
        | protocol_caps::SETTINGS_SYNC;

    let mut out = vec![0x01u8];
    out.extend_from_slice(&0xFFFF_FFFFu32.to_be_bytes());
    out.extend_from_slice(&0x0000_0000u32.to_be_bytes()); // sessionCaps: unused as of v3.0, Pebble app sends 0
    out.extend_from_slice(&platform_flags.to_be_bytes());
    out.extend_from_slice(&[2u8, 4, 4, 2]); // response_version=2, major=4, minor=4, bugfix=2
    out.extend_from_slice(&protocol_caps.to_le_bytes()); // little-endian per Pebble protocol spec
    out
}

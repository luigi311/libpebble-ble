//! Pebble Health — activation preferences and sync trigger.
//!
//! Activation (once, after first connect):
//!   Write user profile to BlobDB PREFERENCES key "activityPreferences".
//!   Optionally write "hrmPreferences" to enable heart-rate monitoring.
//!
//! Sync trigger (on demand):
//!   Send a HealthSync request (endpoint 911). The watch ACKs it and then
//!   streams pending records via the DataLog endpoint (0x6778).

/// Build the 9-byte blob for the "activityPreferences" BlobDB PREFERENCES key.
///
/// The watch uses this to configure its health tracking and step calibration.
/// `height_cm`  user height in centimetres.
/// `weight_kg`  user weight in kilograms.
/// `age`        user age in years.
/// `gender`     0 = female, 1 = male, 2 = other (matches libpebble3 `HealthGender`;
///              used for step-length calibration).
pub fn build_activate_health_blob(height_cm: u16, weight_kg: u16, age: u8, gender: u8) -> Vec<u8> {
    let mut blob = Vec::with_capacity(9);
    blob.extend_from_slice(&height_cm.saturating_mul(10).to_le_bytes()); // height in mm (LE u16)
    blob.extend_from_slice(&weight_kg.saturating_mul(100).to_le_bytes()); // weight in dag (LE u16)
    blob.push(0x01); // tracking enabled
    blob.push(0x00); // activity insights disabled
    blob.push(0x00); // sleep insights disabled
    blob.push(age);
    blob.push(gender);
    blob
}

/// Build the 9-byte blob to deactivate health tracking (all zeros).
pub fn build_deactivate_health_blob() -> Vec<u8> {
    vec![0u8; 9]
}

/// Decoded "activityPreferences" health profile, read back from the watch.
///
/// This is the inverse of [`build_activate_health_blob`]: the watch stores the
/// 9-byte blob we wrote and (on a BlobDB2 sync) hands it back unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActivityPreferences {
    pub height_cm: u16,
    pub weight_kg: u16,
    pub tracking_enabled: bool,
    pub activity_insights_enabled: bool,
    pub sleep_insights_enabled: bool,
    pub age: u8,
    /// 0 = female, 1 = male, 2 = other (matches libpebble3 `HealthGender`).
    pub gender: u8,
}

/// Decode a 9-byte "activityPreferences" blob into an [`ActivityPreferences`].
///
/// Returns `None` if the blob is shorter than 9 bytes. Trailing bytes beyond
/// the 9th are ignored so a longer firmware blob still parses.
pub fn parse_activity_preferences(blob: &[u8]) -> Option<ActivityPreferences> {
    if blob.len() < 9 {
        return None;
    }
    let height_mm = u16::from_le_bytes([blob[0], blob[1]]);
    let weight_dag = u16::from_le_bytes([blob[2], blob[3]]);
    Some(ActivityPreferences {
        height_cm: height_mm / 10,  // stored in mm
        weight_kg: weight_dag / 100, // stored in decagrams
        tracking_enabled: blob[4] != 0,
        activity_insights_enabled: blob[5] != 0,
        sleep_insights_enabled: blob[6] != 0,
        age: blob[7],
        gender: blob[8],
    })
}

/// Decode a "unitsDistance" blob (libpebble3 `DistanceUnitsBlobItem`): one byte.
/// Returns `Some(true)` for imperial units (mi/lb), `Some(false)` for metric
/// (km/kg), or `None` if empty.
pub fn parse_units_distance(blob: &[u8]) -> Option<bool> {
    blob.first().map(|&b| b != 0)
}

/// Heart-rate monitoring interval (libpebble3 `HRMonitoringInterval`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HrMonitoringInterval {
    TenMin,
    ThirtyMin,
    OneHour,
    Disabled,
    /// A value the firmware reported that we don't have a name for.
    Unknown(u8),
}

impl HrMonitoringInterval {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::TenMin,
            1 => Self::ThirtyMin,
            2 => Self::OneHour,
            3 => Self::Disabled,
            other => Self::Unknown(other),
        }
    }

    /// The on-wire byte value (round-trips `from_u8`).
    pub fn code(self) -> u8 {
        match self {
            Self::TenMin => 0,
            Self::ThirtyMin => 1,
            Self::OneHour => 2,
            Self::Disabled => 3,
            Self::Unknown(v) => v,
        }
    }
}

/// Decoded "hrmPreferences" blob (libpebble3 `ActivityHRMSettings`).
///
/// The struct grew across firmware revisions, so the optional fields are only
/// present when the watch sent a long-enough blob:
///   1 byte  → `enabled` only (legacy hardware)
///   2 bytes → `+ measurement_interval` (fw ≥ v4.9.146)
///   3 bytes → `+ activity_tracking_enabled` (fw ≥ v4.9.150)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HrmPreferences {
    pub enabled: bool,
    pub measurement_interval: Option<HrMonitoringInterval>,
    pub activity_tracking_enabled: Option<bool>,
}

/// Decode an "hrmPreferences" blob. Returns `None` if empty.
pub fn parse_hrm_preferences(blob: &[u8]) -> Option<HrmPreferences> {
    let enabled = *blob.first()? != 0;
    Some(HrmPreferences {
        enabled,
        measurement_interval: blob.get(1).map(|&b| HrMonitoringInterval::from_u8(b)),
        activity_tracking_enabled: blob.get(2).map(|&b| b != 0),
    })
}

/// Build the 1-byte blob for the "hrmPreferences" BlobDB PREFERENCES key.
pub fn build_hrm_blob(enabled: bool) -> Vec<u8> {
    vec![if enabled { 0x01 } else { 0x00 }]
}

/// Decoded "heartRatePreferences" blob (libpebble3 `HeartRatePreferencesBlobItem`):
/// six little-endian `u8` BPM/threshold values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartRatePreferences {
    pub resting_hr: u8,
    pub elevated_hr: u8,
    pub max_hr: u8,
    pub zone1_threshold: u8,
    pub zone2_threshold: u8,
    pub zone3_threshold: u8,
}

/// Decode a 6-byte "heartRatePreferences" blob. Returns `None` if too short.
pub fn parse_heart_rate_preferences(blob: &[u8]) -> Option<HeartRatePreferences> {
    if blob.len() < 6 {
        return None;
    }
    Some(HeartRatePreferences {
        resting_hr: blob[0],
        elevated_hr: blob[1],
        max_hr: blob[2],
        zone1_threshold: blob[3],
        zone2_threshold: blob[4],
        zone3_threshold: blob[5],
    })
}

/// Health sync request command (phone → watch, endpoint 911).
pub const HEALTH_SYNC_CMD_SYNC: u8 = 0x01;
/// Health sync ACK command (watch → phone, endpoint 911).
pub const HEALTH_SYNC_CMD_ACK: u8 = 0x11;

/// Build the 5-byte payload for a HealthSync request (endpoint 911).
///
/// `seconds_since_sync = 0` asks the watch to flush everything in its queue.
pub fn build_health_sync_request() -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    out.push(HEALTH_SYNC_CMD_SYNC);
    out.extend_from_slice(&0u32.to_le_bytes()); // seconds_since_sync = 0
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_preferences_round_trips() {
        let blob = build_activate_health_blob(180, 75, 30, 1);
        let prefs = parse_activity_preferences(&blob).expect("decodes");
        assert_eq!(prefs.height_cm, 180);
        assert_eq!(prefs.weight_kg, 75);
        assert_eq!(prefs.age, 30);
        assert_eq!(prefs.gender, 1);
        assert!(prefs.tracking_enabled);
        assert!(!prefs.activity_insights_enabled);
        assert!(!prefs.sleep_insights_enabled);
    }

    #[test]
    fn activity_preferences_rejects_short_blob() {
        assert!(parse_activity_preferences(&[0u8; 8]).is_none());
    }

    #[test]
    fn hrm_preferences_decodes() {
        // 1-byte legacy blob: only `enabled`.
        let one = parse_hrm_preferences(&build_hrm_blob(true)).expect("decodes");
        assert!(one.enabled);
        assert_eq!(one.measurement_interval, None);
        assert_eq!(one.activity_tracking_enabled, None);

        // 3-byte blob observed from the watch: [enabled, interval=TenMin, activity=true].
        let three = parse_hrm_preferences(&[1, 0, 1]).expect("decodes");
        assert!(three.enabled);
        assert_eq!(three.measurement_interval, Some(HrMonitoringInterval::TenMin));
        assert_eq!(three.activity_tracking_enabled, Some(true));

        assert_eq!(parse_hrm_preferences(&[]), None);
    }

    #[test]
    fn heart_rate_preferences_decodes() {
        // Real value read from the watch (libpebble3 defaults).
        let hr = parse_heart_rate_preferences(&[70, 100, 190, 130, 154, 172]).expect("decodes");
        assert_eq!(hr.resting_hr, 70);
        assert_eq!(hr.elevated_hr, 100);
        assert_eq!(hr.max_hr, 190);
        assert_eq!(hr.zone1_threshold, 130);
        assert_eq!(hr.zone2_threshold, 154);
        assert_eq!(hr.zone3_threshold, 172);
        assert!(parse_heart_rate_preferences(&[1, 2, 3, 4, 5]).is_none());
    }
}

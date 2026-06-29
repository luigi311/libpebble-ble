//! WatchPrefs (BlobDB 12) general device-settings decoding.
//!
//! Mirrors libpebble3's typed `WatchPref` registry (`WatchPrefEntity.kt`): each
//! known key maps to a wire type, and [`decode_watch_pref`] turns the raw blob
//! the watch syncs into a typed [`WatchPrefValue`].
//!
//! Health keys (activityPreferences, hrmPreferences, heartRatePreferences,
//! unitsDistance) are NOT here — they live in [`crate::endpoints::health`].
//! Keys libpebble3 itself leaves raw (dndWeekday/WeekendSchedule, workerId,
//! *AppOpened markers, watchface, automaticTimezoneID) are intentionally absent
//! so [`watch_pref_type`] returns `None` and the caller leaves them untouched.

use uuid::Uuid;

/// Wire encoding of a watch preference value (libpebble3 `WatchPrefType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchPrefType {
    Bool,
    /// One-byte value, including the various single-byte enum settings.
    U8,
    U16,
    U32,
    Str,
    Uuid,
    /// `[enabled: u8][uuid: 16B]` quick-launch binding.
    QuickLaunch,
    /// One-byte Pebble color code.
    Color,
}

/// A decoded watch preference value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchPrefValue {
    Bool(bool),
    /// Unsigned integer (u8/u16/u32 widths and single-byte enum codes).
    Number(u32),
    /// String, UUID, quick-launch, or color rendered as text.
    Text(String),
}

/// The "no binding" sentinel UUID used by quick-launch settings.
const NULL_UUID: Uuid = Uuid::from_bytes([0xff; 16]);

/// Look up the wire type for a known watch-pref key. Returns `None` for keys not
/// in the registry — the caller should leave those untouched.
pub fn watch_pref_type(key: &str) -> Option<WatchPrefType> {
    use WatchPrefType::*;
    Some(match key {
        // Booleans (libpebble3 BoolWatchPref)
        "timezoneSource" | "clock24h" | "stationaryMode" | "displayOrientationLeftHanded"
        | "lightEnabled" | "lightAmbientSensorEnabled" | "lightMotion" | "lightDynamicIntensity"
        | "langEnglish" | "timelineQuickViewEnabled" | "dndManuallyEnabled" | "dndSmartEnabled"
        | "notifDesignStyle" | "notifVibeDelay" | "notifBacklight" | "menuScrollWrapAround"
        | "dndMotionBacklight" | "musicShowVolumeControls" | "musicShowProgressBar" => Bool,
        // Single-byte enums (libpebble3 EnumWatchPref). Value is the raw enum code;
        // see WatchPrefEntity.kt for the per-key enum tables.
        "textStyle" | "mask" | "dndInterruptionsMask" | "dndShowNotifications" | "vibeIntensity"
        | "vibeScoreNotifications" | "vibeScoreIncomingCalls" | "vibeScoreAlarms"
        | "menuScrollVibeBehavior" | "motionSensitivity" | "lightIntensity" | "lightTouch" => U8,
        // u16 (libpebble3 NumberWatchPref / TypeUInt16)
        "timelineQuickViewBeforeTimeMin" => U16,
        // u32 (libpebble3 NumberWatchPref / RgbColorWatchPref, all TypeUInt32)
        "lightTimeoutMs" | "lightAmbientThreshold" | "dynBacklightMinThreshold"
        | "notifWindowTimeout" | "lightColor" => U32,
        // Quick launch (libpebble3 QuicklaunchWatchPref)
        "qlUp" | "qlDown" | "qlSelect" | "qlBack" | "qlComboBackUp" | "qlComboUpDown"
        | "qlSingleClickUp" | "qlSingleClickDown" => QuickLaunch,
        _ => return None,
    })
}

/// Decode a watch-pref blob for a known key. Returns `None` for unknown keys or
/// blobs too short for their type.
///
/// Note: the `textStyle` enum has a per-watch-model offset in libpebble3
/// (`applyOffsetForReceiveFromWatch`); we return the raw on-wire code without
/// that adjustment.
pub fn decode_watch_pref(key: &str, raw: &[u8]) -> Option<WatchPrefValue> {
    match watch_pref_type(key)? {
        WatchPrefType::Bool => Some(WatchPrefValue::Bool(*raw.first()? != 0)),
        WatchPrefType::U8 | WatchPrefType::Color => {
            Some(WatchPrefValue::Number(*raw.first()? as u32))
        }
        WatchPrefType::U16 => {
            let b = raw.get(..2)?;
            Some(WatchPrefValue::Number(u16::from_le_bytes([b[0], b[1]]) as u32))
        }
        WatchPrefType::U32 => {
            let b = raw.get(..4)?;
            Some(WatchPrefValue::Number(u32::from_le_bytes([b[0], b[1], b[2], b[3]])))
        }
        WatchPrefType::Str => Some(WatchPrefValue::Text(
            String::from_utf8_lossy(raw).trim_end_matches('\0').to_owned(),
        )),
        WatchPrefType::Uuid => {
            let b: [u8; 16] = raw.get(..16)?.try_into().ok()?;
            Some(WatchPrefValue::Text(Uuid::from_bytes(b).to_string()))
        }
        WatchPrefType::QuickLaunch => {
            let enabled = *raw.first()? != 0;
            let b: [u8; 16] = raw.get(1..17)?.try_into().ok()?;
            let uuid = Uuid::from_bytes(b);
            let text = if !enabled || uuid == NULL_UUID {
                "off".to_string()
            } else {
                uuid.to_string()
            };
            Some(WatchPrefValue::Text(text))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_bool() {
        assert_eq!(decode_watch_pref("clock24h", &[1]), Some(WatchPrefValue::Bool(true)));
        assert_eq!(decode_watch_pref("lightEnabled", &[0]), Some(WatchPrefValue::Bool(false)));
    }

    #[test]
    fn decodes_u32_le() {
        // lightTimeoutMs observed from the watch: [136,19,0,0] = 5000.
        assert_eq!(
            decode_watch_pref("lightTimeoutMs", &[136, 19, 0, 0]),
            Some(WatchPrefValue::Number(5000)),
        );
    }

    #[test]
    fn decodes_u8_enum() {
        assert_eq!(
            decode_watch_pref("vibeScoreNotifications", &[9]),
            Some(WatchPrefValue::Number(9)),
        );
    }

    #[test]
    fn quick_launch_off_when_disabled() {
        let mut blob = vec![0u8]; // disabled
        blob.extend_from_slice(&[0xff; 16]);
        assert_eq!(decode_watch_pref("qlUp", &blob), Some(WatchPrefValue::Text("off".into())));
    }

    #[test]
    fn unknown_and_health_keys_are_none() {
        assert_eq!(decode_watch_pref("automaticTimezoneID", &[0, 0]), None);
        assert_eq!(decode_watch_pref("activityPreferences", &[0; 9]), None);
        assert_eq!(decode_watch_pref("dndWeekdaySchedule", &[0; 4]), None);
    }

    #[test]
    fn short_blob_is_none() {
        assert_eq!(decode_watch_pref("lightTimeoutMs", &[1, 2]), None);
    }
}

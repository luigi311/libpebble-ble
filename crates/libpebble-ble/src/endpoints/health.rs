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
/// `gender`     0 = male, 1 = female (used for step-length calibration).
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

/// Build the 1-byte blob for the "hrmPreferences" BlobDB PREFERENCES key.
pub fn build_hrm_blob(enabled: bool) -> Vec<u8> {
    vec![if enabled { 0x01 } else { 0x00 }]
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

//! Daemon state: events, health profile, music, and session data.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use libpebble_ble::{
    ActivityPreferences, AppMessageValue, DatalogData, HeartRatePreferences, HrmPreferences,
    Pebble, WatchPrefValue,
};

use crate::db::AppDb;
use tokio::sync::mpsc;
use zbus::zvariant::{OwnedValue, Value};

pub const BUS_NAME: &str = "org.cobble.Daemon";
pub const OBJECT_PATH: &str = "/org/cobble/Daemon";
pub(crate) const MUSIC_APP_UUID: &str = "1f03293d-47af-4f28-b960-f2b02a6dd757";

#[derive(Debug, zbus::DBusError)]
#[zbus(prefix = "org.cobble.Daemon")]
pub(crate) enum DaemonError {
    NotConnected(String),
    Failed(String),
}

#[derive(Debug)]
pub enum DaemonEvent {
    ConnectionChanged(bool),
    AppMessageReceived { uuid: String, data: HashMap<u32, AppMessageValue> },
    AckReceived(u8),
    NackReceived(u8),
    HealthData(DatalogData),
    BatteryChanged(u8),
    AppRunState { uuid: String, running: bool },
    MusicAction(String),
    PhoneAction(libpebble_ble::PhoneAction),
    HealthProfile(ActivityPreferences),
    HealthHrm(HrmPreferences),
    HealthHeartRate(HeartRatePreferences),
    HealthUnits(bool),
    WatchSetting { key: String, value: WatchPrefValue },
}

#[derive(Debug, Clone, Copy)]
pub struct HealthProfile {
    pub height_cm: u16, pub weight_kg: u16, pub age: u16, pub gender: u16,
    pub tracking_enabled: bool, pub activity_insights_enabled: bool, pub sleep_insights_enabled: bool,
    pub hrm_enabled: bool, pub hrm_measurement_interval: u8, pub hrm_activity_tracking: bool,
    pub resting_hr: u16, pub elevated_hr: u16, pub max_hr: u16,
    pub hr_zone1_threshold: u16, pub hr_zone2_threshold: u16, pub hr_zone3_threshold: u16,
    pub imperial_units: bool,
}

impl HealthProfile {
    pub(crate) fn to_dbus_map(self) -> HashMap<String, OwnedValue> {
        fn val(v: impl Into<Value<'static>>) -> OwnedValue {
            OwnedValue::try_from(v.into()).expect("primitive converts to OwnedValue")
        }
        HashMap::from([
            ("height_cm".into(), val(self.height_cm)),
            ("weight_kg".into(), val(self.weight_kg)),
            ("age".into(), val(self.age)),
            ("gender".into(), val(self.gender)),
            ("tracking_enabled".into(), val(self.tracking_enabled)),
            ("activity_insights_enabled".into(), val(self.activity_insights_enabled)),
            ("sleep_insights_enabled".into(), val(self.sleep_insights_enabled)),
            ("hrm_enabled".into(), val(self.hrm_enabled)),
            ("hrm_measurement_interval".into(), val(self.hrm_measurement_interval)),
            ("hrm_activity_tracking".into(), val(self.hrm_activity_tracking)),
            ("resting_hr".into(), val(self.resting_hr)),
            ("elevated_hr".into(), val(self.elevated_hr)),
            ("max_hr".into(), val(self.max_hr)),
            ("hr_zone1_threshold".into(), val(self.hr_zone1_threshold)),
            ("hr_zone2_threshold".into(), val(self.hr_zone2_threshold)),
            ("hr_zone3_threshold".into(), val(self.hr_zone3_threshold)),
            ("imperial_units".into(), val(self.imperial_units)),
        ])
    }
}

#[derive(Default, Clone)]
pub(crate) struct MusicState {
    pub(crate) player: Option<(String, String)>,
    pub(crate) track: Option<(String, String, String, u32, u32, u32)>,
    pub(crate) play_state: Option<(u8, u32, u32, u8, u8)>,
    pub(crate) volume: Option<u8>,
}

pub(crate) fn watch_pref_owned_value(v: &WatchPrefValue) -> OwnedValue {
    let value = match v {
        WatchPrefValue::Bool(b) => Value::from(*b),
        WatchPrefValue::Number(n) => Value::from(*n),
        WatchPrefValue::Text(s) => Value::from(s.clone()),
    };
    OwnedValue::try_from(value).expect("primitive value converts to OwnedValue")
}

pub(crate) fn dbus_val(v: impl Into<Value<'static>>) -> OwnedValue {
    OwnedValue::try_from(v.into()).expect("primitive converts to OwnedValue")
}

pub(crate) struct DaemonState {
    pub(crate) address: String,
    pub(crate) adapter: String,
    pub(crate) config_path: PathBuf,
    pub(crate) pebble: Option<Arc<Pebble>>,
    pub(crate) connected: bool,
    pub(crate) stopping: bool,
    pub(crate) notify_blocklist: Vec<String>,
    pub(crate) event_tx: mpsc::UnboundedSender<DaemonEvent>,
    pub(crate) db: Option<Arc<Mutex<AppDb>>>,
    pub(crate) health_profile: Option<ActivityPreferences>,
    pub(crate) hrm_prefs: Option<HrmPreferences>,
    pub(crate) heart_rate_prefs: Option<HeartRatePreferences>,
    pub(crate) imperial_units: Option<bool>,
    pub(crate) watch_settings: HashMap<String, WatchPrefValue>,
    pub(crate) battery_level: Option<u8>,
    pub(crate) music: MusicState,
}

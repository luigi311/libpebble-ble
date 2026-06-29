//! libpebble-ble — talk to a Pebble smartwatch over BLE from Linux.
//!
//! Platform: Linux only. Requires a running BlueZ >= 5.48.

pub mod endpoints;
pub mod error;
pub mod transport;
pub mod uuids;

mod pebble;

pub use endpoints::app_message::AppMessageValue;
pub use endpoints::app_run_state::AppRunStateCmd;
pub use endpoints::blob_db::{NotificationCategory, WeatherType};
pub use endpoints::datalog::DatalogData;
pub use endpoints::health::{
    parse_activity_preferences, parse_heart_rate_preferences, parse_hrm_preferences,
    parse_units_distance, ActivityPreferences, HeartRatePreferences, HrMonitoringInterval,
    HrmPreferences,
};
pub use endpoints::Endpoint;
pub use error::PebbleError;
pub use pebble::{AckHandler, AppMessageHandler, HealthDataHandler, NackHandler, Pebble, WatchPrefHandler};

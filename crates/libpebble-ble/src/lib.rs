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
pub use endpoints::blob_db::NotificationCategory;
pub use endpoints::Endpoint;
pub use error::PebbleError;
pub use pebble::{AckHandler, AppMessageHandler, NackHandler, Pebble};

//! D-Bus service interface (org.cobble.Daemon).
//!
//! Interface (org.cobble.Daemon on /org/cobble/Daemon):
//!
//!   Properties
//!     Connected     b    watch BLE link is up right now
//!     WatchAddress  s    configured watch address
//!
//!   Methods
//!     SendAppMessage(s uuid, a{i(sv)} data, b wait_ack) -> u txn
//!     LaunchApp(s uuid)
//!     StopApp(s uuid)
//!     UpdateTime()
//!     Notify(s title, s body, s subtitle) -> u token
//!     Ping() -> b
//!     Scan(d timeout_secs) -> a(ss)
//!     ActivateHealth(q height_cm, q weight_kg, y age, y gender, b hrm_enabled)
//!     FetchHealthData()
//!     PushWeather(ay location_key, s location_name, s forecast_short, n current_temp, y current_weather, n today_high, n today_low, y tomorrow_weather, n tomorrow_high, n tomorrow_low, b is_current_location)
//!     ReprocessHealthData()
//!
//!   Signals
//!     AppMessageReceived(s uuid, a{i(sv)} data)
//!     AckReceived(u txn)
//!     NackReceived(u txn)
//!     ConnectionChanged(b connected)
//!     HealthDataReceived(u tag, ay app_uuid, u session_timestamp, u items_left, u crc, y item_type, q item_size, ay data)
//!
//! AppMessage values cross the D-Bus hop as (tag, payload) pairs; see codec.rs.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use libpebble_ble::{AppMessageValue, DatalogData, Pebble, WeatherType};

use crate::db::HealthDb;
use tokio::sync::mpsc;
use tracing::{debug, warn};
use zbus::{
    interface,
    object_server::SignalEmitter,
    Connection,
};

use crate::codec::{decode_wire_dict, encode_wire_dict, WireDict};
use crate::notification::app_name_to_category;

/// Custom D-Bus errors under the `org.cobble.Daemon` prefix.
/// `NotConnected` lets the Python client's `_translate()` raise `NotConnectedError`
/// instead of a generic `DBusError`.
#[derive(Debug, zbus::DBusError)]
#[zbus(prefix = "org.cobble.Daemon")]
enum DaemonError {
    NotConnected(String),
    Failed(String),
}

pub const BUS_NAME: &str = "org.cobble.Daemon";
pub const OBJECT_PATH: &str = "/org/cobble/Daemon";

// ---------------------------------------------------------------------------
// Events (supervisor → signal emitter)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum DaemonEvent {
    ConnectionChanged(bool),
    AppMessageReceived { uuid: String, data: HashMap<u32, AppMessageValue> },
    AckReceived(u8),
    NackReceived(u8),
    HealthData(DatalogData),
}

// ---------------------------------------------------------------------------
// CobbleDaemon
// ---------------------------------------------------------------------------

struct DaemonState {
    address: String,
    adapter: String,
    config_path: PathBuf,
    pebble: Option<Arc<Pebble>>,
    connected: bool,
    stopping: bool,
    // Block unnamed senders (empty app_name) — system daemons and
    // desktop-environment internals that don't set an app_name should
    // not be forwarded to the watch.
    notify_blocklist: Vec<String>,
    event_tx: mpsc::UnboundedSender<DaemonEvent>,
    db: Option<Arc<Mutex<HealthDb>>>,
}

#[derive(Clone)]
pub struct CobbleDaemon {
    state: Arc<Mutex<DaemonState>>,
}

impl CobbleDaemon {

    pub fn new(
        address: String,
        adapter: String,
        config_path: PathBuf,
        event_tx: mpsc::UnboundedSender<DaemonEvent>,
        db: Option<Arc<Mutex<HealthDb>>>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(DaemonState {
                address,
                adapter,
                config_path,
                pebble: None,
                connected: false,
                stopping: false,
                notify_blocklist: vec!["".to_string()],
                event_tx,
                db,
            })),
        }
    }

    /// Returns the current (address, adapter) used by the supervisor on each reconnect.
    pub fn current_connection_params(&self) -> (String, String) {
        let s = self.state.lock().unwrap();
        (s.address.clone(), s.adapter.clone())
    }

    pub(crate) fn event_tx(&self) -> mpsc::UnboundedSender<DaemonEvent> {
        self.state.lock().unwrap().event_tx.clone()
    }

    fn require_pebble(&self) -> Result<Arc<Pebble>, DaemonError> {
        let state = self.state.lock().unwrap();
        if !state.connected {
            return Err(DaemonError::NotConnected("watch is not connected".into()));
        }
        state.pebble.clone().ok_or_else(|| DaemonError::NotConnected("watch is not connected".into()))
    }

    /// Called by the supervisor when the watch connects.
    pub fn set_connected(&self, pebble: Arc<Pebble>) {
        let mut state = self.state.lock().unwrap();
        state.pebble = Some(pebble);
        state.connected = true;
        let _ = state.event_tx.send(DaemonEvent::ConnectionChanged(true));
    }

    /// Called by the supervisor when the watch disconnects.
    pub fn set_disconnected(&self) {
        let mut state = self.state.lock().unwrap();
        state.connected = false;
        state.pebble = None;
        let _ = state.event_tx.send(DaemonEvent::ConnectionChanged(false));
    }

    pub fn is_stopping(&self) -> bool {
        self.state.lock().unwrap().stopping
    }

    pub fn set_stopping(&self) {
        self.state.lock().unwrap().stopping = true;
    }

    /// Forward a desktop notification to the watch (called by NotificationMonitor).
    pub fn on_desktop_notification(&self, app_name: String, summary: String, body: String) {
        let state = self.state.lock().unwrap();
        if !state.connected {
            debug!("watch down; dropping notification from {app_name:?}");
            return;
        }
        if state.notify_blocklist.iter().any(|b| b.eq_ignore_ascii_case(&app_name)) {
            debug!("filtered notification from {app_name:?}");
            return;
        }
        if summary.is_empty() && body.is_empty() {
            return;
        }
        if let Some(pebble) = state.pebble.clone() {
            drop(state);
            let category = app_name_to_category(&app_name);
            debug!("notification from {app_name:?} -> category {category:?}");
            tokio::spawn(async move {
                if let Err(e) = pebble.send_notification(&summary, &body, &app_name, category).await {
                    warn!("send notification failed: {e}");
                }
            });
        }
    }
}

// ---------------------------------------------------------------------------
// zbus interface
// ---------------------------------------------------------------------------

#[interface(name = "org.cobble.Daemon")]
impl CobbleDaemon {
    // ---- Properties ----

    #[zbus(property)]
    fn connected(&self) -> bool {
        self.state.lock().unwrap().connected
    }

    #[zbus(property)]
    fn watch_address(&self) -> String {
        self.state.lock().unwrap().address.clone()
    }

    // ---- Methods ----

    async fn send_app_message(
        &self,
        app_uuid: String,
        data: WireDict,
        wait_ack: bool,
    ) -> Result<u32, DaemonError> {
        let pebble = self.require_pebble()?;
        let decoded = decode_wire_dict(data);
        let txn = pebble
            .send_app_message(&app_uuid, decoded, wait_ack, 5.0)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        debug!("D-Bus SendAppMessage uuid={app_uuid} wait_ack={wait_ack} -> txn={txn}");
        Ok(txn as u32)
    }

    async fn launch_app(&self, app_uuid: String) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.launch_app(&app_uuid).await.map_err(|e| DaemonError::Failed(e.to_string()))
    }

    async fn stop_app(&self, app_uuid: String) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.stop_app(&app_uuid).await.map_err(|e| DaemonError::Failed(e.to_string()))
    }

    async fn update_time(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.update_time().await.map_err(|e| DaemonError::Failed(e.to_string()))
    }

    async fn notify(&self, title: String, body: String, subtitle: String) -> Result<u32, DaemonError> {
        let pebble = self.require_pebble()?;
        // subtitle is conventionally the app_name; use it for category detection.
        let category = app_name_to_category(&subtitle);
        let token = pebble
            .send_notification(&title, &body, &subtitle, category)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        Ok(token as u32)
    }

    fn ping(&self) -> bool {
        true
    }

    async fn scan(&self, timeout_secs: f64) -> Result<Vec<(String, String)>, DaemonError> {
        let adapter = self.state.lock().unwrap().adapter.clone();
        Pebble::scan(&adapter, timeout_secs)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Write health user profile to the watch and trigger a DataLog sync.
    /// gender: 0 = male, 1 = female.
    async fn activate_health(
        &self,
        height_cm: u16,
        weight_kg: u16,
        age: u8,
        gender: u8,
        hrm_enabled: bool,
    ) -> Result<(), DaemonError> {
        if gender > 1 {
            return Err(DaemonError::Failed(format!("invalid gender={gender}; must be 0 (male) or 1 (female)")));
        }
        let pebble = self.require_pebble()?;
        pebble
            .activate_health(height_cm, weight_kg, age, gender, hrm_enabled)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Ask the watch to flush pending health records via DataLog sessions.
    fn fetch_health_data(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.fetch_health_data().map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Push weather data to the Pebble built-in weather app.
    ///
    /// `location_key` must be exactly 16 bytes (a UUID); re-use the same bytes to update
    /// an existing location entry rather than creating a new one.
    ///
    /// `current_weather` / `tomorrow_weather`: 0=PartlyCloudy, 1=CloudyDay, 2=LightSnow,
    ///   3=LightRain, 4=HeavyRain, 5=HeavySnow, 6=Generic, 7=Sun, 8=RainAndSnow, 255=Unknown
    async fn push_weather(
        &self,
        location_key: Vec<u8>,
        location_name: String,
        forecast_short: String,
        current_temp: i16,
        current_weather: u8,
        today_high: i16,
        today_low: i16,
        tomorrow_weather: u8,
        tomorrow_high: i16,
        tomorrow_low: i16,
        is_current_location: bool,
    ) -> Result<(), DaemonError> {
        if location_key.len() != 16 {
            return Err(DaemonError::Failed(format!(
                "location_key must be 16 bytes, got {}",
                location_key.len()
            )));
        }
        let key: [u8; 16] = location_key.try_into().unwrap();
        let pebble = self.require_pebble()?;
        pebble
            .push_weather(
                &key,
                &location_name,
                &forecast_short,
                current_temp,
                WeatherType::from_u8(current_weather),
                today_high,
                today_low,
                WeatherType::from_u8(tomorrow_weather),
                tomorrow_high,
                tomorrow_low,
                is_current_location,
            )
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Rebuild health_activity_minutes and health_activity_sessions from the raw
    /// blobs in health_records. Call this after a schema change or to backfill
    /// utc_offset for rows that were stored before the column existed.
    async fn reprocess_health_data(&self) -> Result<(), DaemonError> {
        let db = self.state.lock().unwrap().db.clone();
        let db = db.ok_or_else(|| DaemonError::Failed("health database not available".into()))?;
        tokio::task::spawn_blocking(move || db.lock().unwrap().reprocess())
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Re-read the config file from disk and apply changes.
    /// If address or adapter changed, disconnects the current session so the
    /// supervisor reconnects with the new parameters on the next cycle.
    async fn reload_config(&self) -> Result<(), DaemonError> {
        let config_path = self.state.lock().unwrap().config_path.clone();

        let new_cfg = crate::config::load(&config_path)
            .map_err(|e| DaemonError::Failed(e.to_string()))?;

        // Read state.pebble in the same lock scope as the config update so
        // we always disconnect the handle that was live when the new params
        // were applied — no window for the supervisor to slip in a new
        // connection that we'd then miss.
        let pebble_to_disconnect = {
            let mut state = self.state.lock().unwrap();
            let changed =
                state.address != new_cfg.address || state.adapter != new_cfg.adapter;
            state.address = new_cfg.address;
            state.adapter = new_cfg.adapter;
            if changed { state.pebble.clone() } else { None }
        };

        if let Some(pebble) = pebble_to_disconnect {
            let _ = pebble.disconnect().await;
        }

        Ok(())
    }

    // ---- Signals ----

    #[zbus(signal)]
    pub async fn app_message_received(
        signal_emitter: &SignalEmitter<'_>,
        app_uuid: &str,
        data: WireDict,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn ack_received(signal_emitter: &SignalEmitter<'_>, txn: u32) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn nack_received(signal_emitter: &SignalEmitter<'_>, txn: u32) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn connection_changed(
        signal_emitter: &SignalEmitter<'_>,
        connected: bool,
    ) -> zbus::Result<()>;

    /// Emitted for each batch of health records received from the watch.
    /// tag: data type (81=steps, 83=sleep, 84=activity sessions, 85=HR).
    /// app_uuid: 16 bytes (all-zeros for health sessions).
    /// item_size: bytes per record in `data`.
    /// items_left: records still queued on the watch after this batch.
    /// crc: CRC-32 of `data` as computed by the watch; use for deduplication on reconnect.
    #[zbus(signal)]
    pub async fn health_data_received(
        signal_emitter: &SignalEmitter<'_>,
        tag: u32,
        app_uuid: Vec<u8>,
        session_timestamp: u32,
        items_left: u32,
        crc: u32,
        item_type: u8,
        item_size: u16,
        data: Vec<u8>,
    ) -> zbus::Result<()>;
}

// ---------------------------------------------------------------------------
// Signal emission task
// ---------------------------------------------------------------------------

/// Processes `DaemonEvent`s from the reconnect supervisor and emits the
/// corresponding D-Bus signals. Keeps the `Connected` property in sync.
pub async fn run_signal_emitter(
    conn: Connection,
    _daemon: CobbleDaemon,
    mut event_rx: mpsc::UnboundedReceiver<DaemonEvent>,
    health_db: Option<Arc<Mutex<HealthDb>>>,
) {
    while let Some(event) = event_rx.recv().await {
        let iface_result = conn
            .object_server()
            .interface::<_, CobbleDaemon>(OBJECT_PATH)
            .await;
        let iface = match iface_result {
            Ok(i) => i,
            Err(e) => {
                warn!("could not get interface for signal emission: {e}");
                continue;
            }
        };
        let emitter = iface.signal_emitter();
        match event {
            DaemonEvent::ConnectionChanged(c) => {
                let _ = CobbleDaemon::connection_changed(emitter, c).await;
                let _ = iface.get().await.connected_changed(iface.signal_emitter()).await;
            }
            DaemonEvent::AppMessageReceived { uuid, data } => {
                let wire = encode_wire_dict(&data);
                let _ = CobbleDaemon::app_message_received(emitter, &uuid, wire).await;
            }
            DaemonEvent::AckReceived(txn) => {
                let _ = CobbleDaemon::ack_received(emitter, txn as u32).await;
            }
            DaemonEvent::NackReceived(txn) => {
                let _ = CobbleDaemon::nack_received(emitter, txn as u32).await;
            }
            DaemonEvent::HealthData(batch) => {
                if let Some(db) = &health_db {
                    let db = db.clone();
                    let batch_for_db = batch.clone();
                    match tokio::task::spawn_blocking(move || {
                        db.lock().unwrap().insert_batch(&batch_for_db)
                    })
                    .await
                    {
                        Ok(Err(e)) => warn!("health DB insert failed: {e}"),
                        Err(e) => warn!("health DB task panicked: {e}"),
                        Ok(Ok(())) => {}
                    }
                }
                let _ = CobbleDaemon::health_data_received(
                    emitter,
                    batch.tag,
                    batch.app_uuid.to_vec(),
                    batch.session_timestamp,
                    batch.items_left,
                    batch.crc,
                    batch.item_type,
                    batch.item_size,
                    batch.data,
                )
                .await;
            }
        }
    }
}

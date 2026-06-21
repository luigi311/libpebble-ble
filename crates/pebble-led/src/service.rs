//! D-Bus service interface (org.pebble_le.Daemon).
//!
//! Interface (org.pebble_le.Daemon on /org/pebble_le/Daemon):
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
    sync::{Arc, Mutex},
};

use libpebble_ble::{AppMessageValue, DatalogData, Pebble};
use tokio::sync::mpsc;
use tracing::{debug, warn};
use zbus::{
    interface,
    object_server::SignalEmitter,
    Connection,
};

use crate::codec::{decode_wire_dict, encode_wire_dict, WireDict};
use crate::notification::app_name_to_category;

/// Custom D-Bus errors under the `org.pebble_le.Daemon` prefix.
/// `NotConnected` lets the Python client's `_translate()` raise `NotConnectedError`
/// instead of a generic `DBusError`.
#[derive(Debug, zbus::DBusError)]
#[zbus(prefix = "org.pebble_le.Daemon")]
enum DaemonError {
    NotConnected(String),
    Failed(String),
}

pub const BUS_NAME: &str = "org.pebble_le.Daemon";
pub const OBJECT_PATH: &str = "/org/pebble_le/Daemon";

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
// PebbleDaemon
// ---------------------------------------------------------------------------

struct DaemonState {
    address: String,
    adapter: String,
    pebble: Option<Arc<Pebble>>,
    connected: bool,
    stopping: bool,
    // Block unnamed senders (empty app_name) — system daemons and
    // desktop-environment internals that don't set an app_name should
    // not be forwarded to the watch.
    notify_blocklist: Vec<String>,
    event_tx: mpsc::UnboundedSender<DaemonEvent>,
}

#[derive(Clone)]
pub struct PebbleDaemon {
    state: Arc<Mutex<DaemonState>>,
}

impl PebbleDaemon {
    pub fn new(address: String, adapter: String, event_tx: mpsc::UnboundedSender<DaemonEvent>) -> Self {
        Self {
            state: Arc::new(Mutex::new(DaemonState {
                address,
                adapter,
                pebble: None,
                connected: false,
                stopping: false,
                notify_blocklist: vec!["".to_string()],
                event_tx,
            })),
        }
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

#[interface(name = "org.pebble_le.Daemon")]
impl PebbleDaemon {
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
    _daemon: PebbleDaemon,
    mut event_rx: mpsc::UnboundedReceiver<DaemonEvent>,
) {
    while let Some(event) = event_rx.recv().await {
        let iface_result = conn
            .object_server()
            .interface::<_, PebbleDaemon>(OBJECT_PATH)
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
                let _ = PebbleDaemon::connection_changed(emitter, c).await;
                let _ = iface.get().await.connected_changed(iface.signal_emitter()).await;
            }
            DaemonEvent::AppMessageReceived { uuid, data } => {
                let wire = encode_wire_dict(&data);
                let _ = PebbleDaemon::app_message_received(emitter, &uuid, wire).await;
            }
            DaemonEvent::AckReceived(txn) => {
                let _ = PebbleDaemon::ack_received(emitter, txn as u32).await;
            }
            DaemonEvent::NackReceived(txn) => {
                let _ = PebbleDaemon::nack_received(emitter, txn as u32).await;
            }
            DaemonEvent::HealthData(batch) => {
                let _ = PebbleDaemon::health_data_received(
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

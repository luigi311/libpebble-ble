//! The daemon: a zbus ServiceInterface wrapping one libpebble_ble::Pebble.
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
//!
//!   Signals
//!     AppMessageReceived(s uuid, a{i(sv)} data)
//!     AckReceived(u txn)
//!     NackReceived(u txn)
//!     ConnectionChanged(b connected)
//!
//! AppMessage values cross the D-Bus hop as (tag: string, payload: variant).
//! Tag is one of: u8 u16 u32 i8 i16 i32 uint int str bytes.
//! This matches the pebble-le-proto Python package's codec so the Python
//! client can talk to this Rust daemon without any changes.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use libpebble_ble::{AppMessageValue, NotificationCategory, Pebble};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use zbus::{
    interface,
    object_server::SignalEmitter,
    zvariant::{Array, OwnedValue, Str},
    Connection,
};

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
// Desktop app-name → notification category mapping
// ---------------------------------------------------------------------------

fn app_name_to_category(app_name: &str) -> NotificationCategory {
    let lower = app_name.to_lowercase();
    let lower = lower.trim();

    if matches!(lower, "thunderbird" | "evolution" | "kmail" | "geary"
        | "mutt" | "neomutt" | "protonmail" | "gmail" | "outlook"
        | "apple mail" | "mail" | "fastmail" | "tutanota")
    {
        return NotificationCategory::Email;
    }
    if lower == "whatsapp" {
        return NotificationCategory::WhatsApp;
    }
    if lower.contains("facebook messenger") || lower == "messenger" {
        return NotificationCategory::FacebookMessenger;
    }
    if lower == "facebook" {
        return NotificationCategory::Facebook;
    }
    if matches!(lower, "twitter" | "tweetbot" | "tweetdeck" | "birdsite") {
        return NotificationCategory::Twitter;
    }
    if lower == "instagram" {
        return NotificationCategory::Instagram;
    }
    if matches!(lower, "hangouts" | "google hangouts") {
        return NotificationCategory::Hangouts;
    }
    if matches!(lower, "signal" | "telegram" | "discord" | "slack"
        | "element" | "fractal" | "nheko" | "fluffychat" | "mattermost"
        | "rocketchat" | "zulip" | "wire" | "viber" | "line"
        | "skype" | "teams" | "microsoft teams" | "google chat"
        | "messages" | "sms" | "kde connect" | "kdeconnect")
    {
        return NotificationCategory::Messaging;
    }
    NotificationCategory::Generic
}

// ---------------------------------------------------------------------------
// Wire codec (matches Python pebble-le-proto codec.py)
// ---------------------------------------------------------------------------

/// (tag, payload) pair as it appears on the wire.
type WireValue = (String, OwnedValue);
type WireDict = HashMap<i32, WireValue>;

fn decode_wire_dict(wire: WireDict) -> HashMap<u32, AppMessageValue> {
    wire.into_iter().filter_map(|(k, (tag, val))| {
        let k = u32::try_from(k).ok()?; // reject negative keys
        let v = decode_wire_value(&tag, val)?;
        Some((k, v))
    }).collect()
}

fn decode_wire_value(tag: &str, val: OwnedValue) -> Option<AppMessageValue> {
    match tag {
        "u8"   => Some(AppMessageValue::U8(u32::try_from(val).ok()? as u8)),
        "u16"  => Some(AppMessageValue::U16(u32::try_from(val).ok()? as u16)),
        "u32"  => Some(AppMessageValue::U32(u32::try_from(val).ok()?)),
        "i8"   => Some(AppMessageValue::I8(i32::try_from(val).ok()? as i8)),
        "i16"  => Some(AppMessageValue::I16(i32::try_from(val).ok()? as i16)),
        "i32"  => Some(AppMessageValue::I32(i32::try_from(val).ok()?)),
        // Pebble AppMessage caps integers at 32 bits; uint/int widen to u64/i64
        // on the Rust side but the wire value is always a 32-bit D-Bus integer.
        "uint" => Some(AppMessageValue::Uint(u32::try_from(val).ok()? as u64)),
        "int"  => Some(AppMessageValue::Int(i32::try_from(val).ok()? as i64)),
        "str"  => Some(AppMessageValue::Str(String::try_from(val).ok()?)),
        "bytes" => {
            let arr = Array::try_from(val).ok()?;
            let b: Vec<u8> = Vec::try_from(arr).ok()?;
            Some(AppMessageValue::Bytes(b))
        }
        _ => None,
    }
}

fn encode_wire_dict(data: &HashMap<u32, AppMessageValue>) -> WireDict {
    data.iter().filter_map(|(k, v)| {
        let k = i32::try_from(*k).ok()?; // reject keys >= 2^31
        let (tag, owned) = encode_wire_value(v);
        Some((k, (tag, owned)))
    }).collect()
}

fn encode_wire_value(value: &AppMessageValue) -> (String, OwnedValue) {
    match value {
        AppMessageValue::U8(v)    => ("u8".into(),    OwnedValue::from(*v as u32)),
        AppMessageValue::U16(v)   => ("u16".into(),   OwnedValue::from(*v as u32)),
        AppMessageValue::U32(v)   => ("u32".into(),   OwnedValue::from(*v)),
        AppMessageValue::I8(v)    => ("i8".into(),    OwnedValue::from(*v as i32)),
        AppMessageValue::I16(v)   => ("i16".into(),   OwnedValue::from(*v as i32)),
        AppMessageValue::I32(v)   => ("i32".into(),   OwnedValue::from(*v)),
        // Pebble AppMessage caps integers at 32 bits; upper bits are dropped here.
        AppMessageValue::Uint(v)  => ("uint".into(),  OwnedValue::from(*v as u32)),
        AppMessageValue::Int(v)   => ("int".into(),   OwnedValue::from(*v as i32)),
        AppMessageValue::Str(s)   => ("str".into(),   OwnedValue::from(Str::from(s.as_str()))),
        AppMessageValue::Bytes(b) => {
            let arr = Array::from(b.as_slice());
            ("bytes".into(), OwnedValue::try_from(arr).expect("bytes encode"))
        }
    }
}

// ---------------------------------------------------------------------------
// Daemon event → D-Bus signal bridge
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum DaemonEvent {
    ConnectionChanged(bool),
    AppMessageReceived { uuid: String, data: HashMap<u32, AppMessageValue> },
    AckReceived(u8),
    NackReceived(u8),
}

// ---------------------------------------------------------------------------
// zbus interface
// ---------------------------------------------------------------------------

struct DaemonState {
    address: String,
    adapter: String,
    pebble: Option<Arc<Pebble>>,
    connected: bool,
    stopping: bool,
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
                // Block unnamed senders (empty app_name) — system daemons and
                // desktop-environment internals that don't set an app_name should
                // not be forwarded to the watch.
                notify_blocklist: vec!["".to_string()],
                event_tx,
            })),
        }
    }

    fn require_pebble(&self) -> Result<Arc<Pebble>, DaemonError> {
        let state = self.state.lock().unwrap();
        if !state.connected {
            return Err(DaemonError::NotConnected("watch is not connected".into()));
        }
        state.pebble.clone().ok_or_else(|| DaemonError::NotConnected("watch is not connected".into()))
    }

    /// Called by the supervisor task when the watch connects.
    pub fn set_connected(&self, pebble: Arc<Pebble>) {
        let mut state = self.state.lock().unwrap();
        state.pebble = Some(pebble);
        state.connected = true;
        let _ = state.event_tx.send(DaemonEvent::ConnectionChanged(true));
    }

    /// Called by the supervisor task when the watch disconnects.
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

    #[allow(dead_code)]
    pub fn address(&self) -> String {
        self.state.lock().unwrap().address.clone()
    }

    #[allow(dead_code)]
    pub fn is_connected(&self) -> bool {
        self.state.lock().unwrap().connected
    }
}

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

    /// Scan for nearby Pebble watches. Returns a list of (address, name) pairs.
    /// `timeout_secs` controls how long to scan; 10.0 is a reasonable default.
    async fn scan(&self, timeout_secs: f64) -> Result<Vec<(String, String)>, DaemonError> {
        let adapter = self.state.lock().unwrap().adapter.clone();
        Pebble::scan(&adapter, timeout_secs)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
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
}

// ---------------------------------------------------------------------------
// Signal emission task
// ---------------------------------------------------------------------------

/// Processes `DaemonEvent`s from the reconnect supervisor and emits the
/// corresponding D-Bus signals. Keeps the property `Connected` in sync.
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
                // Also emit property change notification.
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
        }
    }
}

// ---------------------------------------------------------------------------
// Reconnect supervisor
// ---------------------------------------------------------------------------

/// Keeps a live connection to the watch, reconnecting on drop with exponential
/// backoff. Wires up libpebble-ble handlers to emit D-Bus signals.
pub async fn run_supervisor(
    daemon: PebbleDaemon,
    address: String,
    adapter: String,
) {
    let mut backoff = 2.0f64;

    while !daemon.is_stopping() {
        info!("connecting to watch {address} ...");
        let pebble = Arc::new(Pebble::new(&address, &adapter));

        // Wire handlers before connect so we catch any early events.
        {
            let event_tx = daemon.state.lock().unwrap().event_tx.clone();
            let tx = event_tx.clone();
            pebble.on_app_message(Arc::new(move |uuid, data| {
                let _ = tx.send(DaemonEvent::AppMessageReceived { uuid, data });
            }));
            let tx = event_tx.clone();
            pebble.on_ack(Arc::new(move |txn| {
                let _ = tx.send(DaemonEvent::AckReceived(txn));
            }));
            let tx = event_tx.clone();
            pebble.on_nack(Arc::new(move |txn| {
                let _ = tx.send(DaemonEvent::NackReceived(txn));
            }));
        }

        match pebble.connect().await {
            Ok(()) => {
                backoff = 2.0;
                daemon.set_connected(Arc::clone(&pebble));
                info!("watch connected; daemon ready");

                if let Err(e) = pebble.update_time().await {
                    warn!("time sync on connect failed: {e}");
                }

                pebble.wait_disconnected().await;
                warn!("watch link went down");
            }
            Err(e) => {
                warn!("connection attempt failed: {e}");
            }
        }

        daemon.set_disconnected();
        let _ = pebble.disconnect().await;

        if daemon.is_stopping() {
            break;
        }
        debug!("reconnecting in {backoff:.0}s");
        tokio::time::sleep(std::time::Duration::from_secs_f64(backoff)).await;
        backoff = (backoff * 2.0).min(30.0);
    }
}

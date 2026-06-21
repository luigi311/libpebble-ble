//! High-level Pebble connection: lifecycle, pairing, endpoint dispatch, AppMessage API.
//!
//! `Pebble` owns:
//!   * a bluer GATT *client* connection used for the fed9 pairing/connectivity
//!     handshake, and
//!   * the phone-hosted PPoGATT GATT *server* the watch connects back to.
//!
//! Pairing: `connect()` handles first-time bonding. It registers a temporary
//! auto-accept bluer agent, writes 0x09 to the pairing-trigger characteristic
//! so the WATCH initiates bonding (confirm on the watch screen), falls back to
//! host-initiated pair() if the watch stays quiet, and on failure removes the
//! stale BlueZ bond and retries once from scratch.

use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use bluer::{AdapterEvent, Address, Device};
use chrono::Local;
use futures::StreamExt;
use tokio::{
    sync::{oneshot, watch, Notify},
    time::timeout,
};
use tracing::{debug, info, trace, warn};

use crate::{
    endpoints::{
        app_message::{
            build_app_message_ack, build_app_message_push, parse_app_message, AppMessageCmd,
            AppMessageValue,
        },
        app_run_state::{build_app_run_state, AppRunStateCmd},
        blob_db::{
            build_blobdb_str_insert, build_notification, parse_blobdb_response, BlobDBId,
            BlobDBStatus, NotificationCategory,
        },
        datalog::{
            self, build_reply, build_report_sessions, DatalogData, DatalogSession,
            DATALOG_CLOSE, DATALOG_OPENSESSION, DATALOG_SENDDATA, DATALOG_TIMEOUT,
        },
        health::{build_activate_health_blob, build_health_sync_request, build_hrm_blob},
        phone_version::build_phone_version_response,
        ping::{build_pong, parse_ping},
        time::build_set_utc,
        pebble_pack, pebble_unpack, Endpoint,
    },
    error::PebbleError,
    transport::{
        agent::build_pairing_agent,
        gatt_server::{start_gatt_server, PebbleGattServerHandle},
    },
    uuids::{
        CONNECTION_PARAMS_CHARACTERISTIC, CONNECTIVITY_CHARACTERISTIC, MTU_CHARACTERISTIC,
        PAIRING_TRIGGER_CHARACTERISTIC,
    },
};

pub type AppMessageHandler =
    Arc<dyn Fn(String, HashMap<u32, AppMessageValue>) + Send + Sync + 'static>;
pub type AckHandler = Arc<dyn Fn(u8) + Send + Sync + 'static>;
pub type NackHandler = Arc<dyn Fn(u8) + Send + Sync + 'static>;
pub type HealthDataHandler = Arc<dyn Fn(DatalogData) + Send + Sync + 'static>;

struct PebbleInner {
    app_message_handlers: Vec<AppMessageHandler>,
    ack_handlers: Vec<AckHandler>,
    nack_handlers: Vec<NackHandler>,
    health_handlers: Vec<HealthDataHandler>,
    /// transaction_id → future resolved when watch ACK/NACKs it
    pending: HashMap<u8, oneshot::Sender<bool>>,
    txn: u8,
    /// Handle to the GATT server send channel (set once server is started).
    gatt_server: Option<PebbleGattServerHandle>,
    /// Open DataLog sessions keyed by the 1-byte handle from the watch.
    datalog_sessions: HashMap<u8, DatalogSession>,
}

impl PebbleInner {
    fn new() -> Self {
        Self {
            app_message_handlers: Vec::new(),
            ack_handlers: Vec::new(),
            nack_handlers: Vec::new(),
            health_handlers: Vec::new(),
            pending: HashMap::new(),
            txn: 0,
            gatt_server: None,
            datalog_sessions: HashMap::new(),
        }
    }
}

pub struct Pebble {
    pub address: String,
    pub adapter_name: String,
    inner: Arc<Mutex<PebbleInner>>,
    connected_tx: Arc<watch::Sender<bool>>,
    connected_rx: watch::Receiver<bool>,
}

impl Pebble {
    pub fn new(address: &str, adapter: &str) -> Self {
        let (tx, rx) = watch::channel(false);
        Self {
            address: address.to_string(),
            adapter_name: adapter.to_string(),
            inner: Arc::new(Mutex::new(PebbleInner::new())),
            connected_tx: Arc::new(tx),
            connected_rx: rx,
        }
    }

    // ---- BLE discovery ----

    /// Scan for nearby Pebble devices for `timeout_secs` seconds.
    /// Returns a list of `(address, name)` pairs for every device whose
    /// Bluetooth name contains "pebble" (case-insensitive).
    pub async fn scan(adapter_name: &str, timeout_secs: f64) -> Result<Vec<(String, String)>, PebbleError> {
        let session = bluer::Session::new().await?;
        let adapter = session
            .adapter(adapter_name)
            .map_err(|e| PebbleError::Other(format!("adapter {adapter_name}: {e}")))?;

        let mut stream = adapter.discover_devices().await?;
        let mut found: Vec<(String, String)> = Vec::new();

        let _ = tokio::time::timeout(Duration::from_secs_f64(timeout_secs), async {
            while let Some(event) = stream.next().await {
                if let AdapterEvent::DeviceAdded(addr) = event {
                    if let Ok(device) = adapter.device(addr) {
                        let name = device.name().await.ok().flatten().unwrap_or_default();
                        if name.to_lowercase().contains("pebble") {
                            info!("found Pebble: {addr} \"{name}\"");
                            found.push((addr.to_string(), name));
                        }
                    }
                }
            }
        })
        .await;

        Ok(found)
    }

    // ---- handler registration ----

    pub fn on_app_message(&self, handler: AppMessageHandler) {
        self.inner.lock().unwrap().app_message_handlers.push(handler);
    }

    pub fn on_ack(&self, handler: AckHandler) {
        self.inner.lock().unwrap().ack_handlers.push(handler);
    }

    pub fn on_nack(&self, handler: NackHandler) {
        self.inner.lock().unwrap().nack_handlers.push(handler);
    }

    pub fn on_health(&self, handler: HealthDataHandler) {
        self.inner.lock().unwrap().health_handlers.push(handler);
    }

    // ---- liveness ----

    pub fn is_connected(&self) -> bool {
        *self.connected_rx.borrow()
    }

    pub async fn wait_disconnected(&self) {
        let mut rx = self.connected_rx.clone();
        loop {
            if !*rx.borrow() {
                return;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    // ---- connect / disconnect ----

    pub async fn connect(&self) -> Result<(), PebbleError> {
        self.connect_with_options(true, 30.0, 3, 2.0).await
    }

    pub async fn connect_with_options(
        &self,
        pairing: bool,
        conn_timeout: f64,
        connect_attempts: u32,
        retry_delay: f64,
    ) -> Result<(), PebbleError> {
        let address = Address::from_str(&self.address)
            .map_err(|e| PebbleError::Other(format!("invalid address: {e}")))?;

        let session = bluer::Session::new().await?;
        let adapter = session
            .adapter(&self.adapter_name)
            .map_err(|e| PebbleError::Other(format!("adapter {}: {e}", self.adapter_name)))?;

        // 1. Start the phone-hosted PPoGATT GATT server FIRST (watch connects back to this).
        let connected_notify = Arc::new(Notify::new());
        let session_ready_notify = Arc::new(Notify::new());
        let inner_for_data = Arc::clone(&self.inner);
        let inner_for_disc = Arc::clone(&self.inner);
        let connected_tx_for_disc = Arc::clone(&self.connected_tx);
        let connected_notify_for_task = Arc::clone(&connected_notify);
        let session_ready_for_task = Arc::clone(&session_ready_notify);

        let gatt_server = start_gatt_server(
            &adapter,
            Arc::new(move |msg| {
                let inner = Arc::clone(&inner_for_data);
                tokio::spawn(async move {
                    on_pebble_message(msg, &inner);
                });
            }),
            Arc::new(move || {
                warn!("watch disconnected from PPoGATT server");
                let _ = connected_tx_for_disc.send(false);
                // Drain pending and snapshot handlers while holding the lock,
                // then drop it before invoking handlers to avoid re-entrant deadlock.
                let (pending, nack_handlers) = {
                    let mut inner = inner_for_disc.lock().unwrap();
                    let pending: Vec<(u8, oneshot::Sender<bool>)> =
                        inner.pending.drain().collect();
                    let nack_handlers = inner.nack_handlers.clone();
                    inner.datalog_sessions.clear();
                    (pending, nack_handlers)
                };
                for (txn, sender) in pending {
                    let _ = sender.send(false);
                    for h in &nack_handlers {
                        h(txn);
                    }
                }
            }),
            connected_notify_for_task,
            session_ready_for_task,
        )
        .await?;

        self.inner.lock().unwrap().gatt_server = Some(gatt_server);

        // 2. Register pairing agent.
        let agent = build_pairing_agent(address);
        let _agent_handle = session.register_agent(agent).await.map_err(|e| {
            warn!("could not register pairing agent: {e}; relying on a system agent");
            PebbleError::Other(e.to_string())
        });

        // 3. Connect to the watch (with retries).
        let device = self
            .connect_client(&adapter, address, conn_timeout, connect_attempts, retry_delay)
            .await?;

        // 4. Pairing / bonding.
        let already_paired = device.is_paired().await.unwrap_or(false);

        if !already_paired && pairing {
            for attempt in 1u32..=2 {
                match self.do_pairing(&device).await {
                    Ok(()) => {
                        // Mark Trusted so the watch can reconnect to our server unprompted.
                        if let Err(e) = device.set_trusted(true).await {
                            debug!("could not set Trusted: {e}");
                        }
                        break;
                    }
                    Err(e) if attempt == 1 => {
                        warn!("pairing failed ({e}); clearing stale bond and retrying");
                        if let Err(e2) = adapter.remove_device(address).await {
                            debug!("RemoveDevice failed: {e2}");
                        }
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                    Err(e) => {
                        return Err(PebbleError::PairingFailed(format!(
                            "pairing with {} failed twice: {e}",
                            self.address
                        )));
                    }
                }
            }
        }

        // 5. Subscribe to fed9 characteristics.
        self.subscribe_fed9(&device).await;

        // 6. Write 0x09 to pairing trigger (announce server mode).
        self.write_pairing_trigger(&device).await;

        // 7. Wait for watch to connect back to our GATT server.
        let timeout_dur = Duration::from_secs_f64(conn_timeout);
        match timeout(timeout_dur, connected_notify.notified()).await {
            Ok(()) => {
                debug!("PPoGATT data channel established");
                // Wait for PPoGATT reset handshake.
                match timeout(Duration::from_secs(10), session_ready_notify.notified()).await {
                    Ok(()) => debug!("PPoGATT session ready"),
                    Err(_) => warn!("PPoGATT session not confirmed ready; early sends may be dropped"),
                }
            }
            Err(_) => {
                warn!("watch did not connect back within {conn_timeout}s; sends may not reach watch")
            }
        }

        // 8. Monitor device events for disconnect.
        let connected_tx = Arc::clone(&self.connected_tx);
        let addr = address;
        tokio::spawn(async move {
            if let Ok(mut events) = device.events().await {
                while let Some(event) = events.next().await {
                    if let bluer::DeviceEvent::PropertyChanged(
                        bluer::DeviceProperty::Connected(false),
                    ) = event
                    {
                        warn!("BlueZ reports device Connected=False; watch dropped");
                        let _ = connected_tx.send(false);
                        break;
                    }
                }
            }
            // Keep device alive for duration of monitoring.
            let _ = addr;
        });

        let _ = self.connected_tx.send(true);
        info!("Pebble connected and ready");
        Ok(())
    }

    async fn connect_client(
        &self,
        adapter: &bluer::Adapter,
        address: Address,
        conn_timeout: f64,
        attempts: u32,
        retry_delay: f64,
    ) -> Result<Device, PebbleError> {
        let mut last_err: Option<PebbleError> = None;
        for i in 1..=attempts {
            let device = adapter.device(address)?;
            let result = timeout(
                Duration::from_secs_f64(conn_timeout),
                device.connect(),
            )
            .await;
            match result {
                Ok(Ok(())) => {
                    return Ok(device);
                }
                Ok(Err(e)) => {
                    let msg = e.to_string();
                    if msg.to_lowercase().contains("already connected") {
                        debug!("device was already connected; attaching");
                        return Ok(device);
                    }
                    warn!("connect attempt {i}/{attempts} failed: {e}");
                    last_err = Some(PebbleError::Ble(e));
                }
                Err(_) => {
                    warn!("connect attempt {i}/{attempts} timed out");
                    last_err = Some(PebbleError::Timeout(format!(
                        "connect to {} timed out after {conn_timeout}s",
                        self.address
                    )));
                }
            }
            // Force-disconnect stale BlueZ state before retry.
            if i < attempts {
                let _ = adapter.device(address).map(|d| {
                    tokio::spawn(async move { let _ = d.disconnect().await; });
                });
                tokio::time::sleep(Duration::from_secs_f64(retry_delay * i as f64)).await;
            }
        }
        Err(last_err.unwrap_or_else(|| PebbleError::Other("connect failed".into())))
    }

    async fn do_pairing(&self, device: &Device) -> Result<(), PebbleError> {
        // Poke pairing-trigger (read then write 0x09) — the watch shows its
        // confirm screen and initiates bonding.
        if let Some(c) = find_char(device, PAIRING_TRIGGER_CHARACTERISTIC).await {
            let _ = c.read().await;
            let _ = c.write(&[0x09]).await;
        }
        info!("waiting for the watch to initiate bonding; CONFIRM ON THE WATCH when prompted");

        // Poll for paired state for up to 10s (watch-initiated bonding).
        for _ in 0..20 {
            if device.is_paired().await.unwrap_or(false) {
                debug!("bonded (watch-initiated)");
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Watch didn't start security; try from host side.
        debug!("watch did not initiate bonding; calling pair() from the host");
        if let Err(e) = device.pair().await {
            warn!("host-initiated pair() failed: {e}");
        }
        // Wait a further 3s for the bond to settle.
        for _ in 0..6 {
            if device.is_paired().await.unwrap_or(false) {
                debug!("bonded (host-initiated)");
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(PebbleError::PairingFailed("pairing timed out".into()))
    }

    async fn subscribe_fed9(&self, device: &Device) {
        for (char_uuid, label) in [
            (CONNECTIVITY_CHARACTERISTIC, "connectivity"),
            (MTU_CHARACTERISTIC, "MTU"),
            (CONNECTION_PARAMS_CHARACTERISTIC, "connection-params"),
        ] {
            if let Some(c) = find_char(device, char_uuid).await {
                // Subscribe in a background task; we process the stream to log updates.
                let label = label.to_string();
                tokio::spawn(async move {
                    match c.notify().await {
                        Ok(stream) => {
                            debug!("subscribed to {label}");
                            tokio::pin!(stream);
                            while let Some(data) = stream.next().await {
                                trace!("{label} update: {} bytes", data.len());
                            }
                        }
                        Err(e) => warn!("subscribe to {label} failed: {e}"),
                    }
                });
            }
        }

        // Also read the current MTU characteristic value.
        if let Some(c) = find_char(device, MTU_CHARACTERISTIC).await {
            if let Ok(data) = c.read().await {
                if data.len() >= 2 {
                    let watch_mtu = u16::from_le_bytes([data[0], data[1]]) as usize;
                    debug!("watch requested MTU: {watch_mtu}");
                    if let Some(srv) = &self.inner.lock().unwrap().gatt_server {
                        if watch_mtu >= 23 {
                            srv.set_mtu(watch_mtu);
                        }
                    }
                }
            }
        }
    }

    async fn write_pairing_trigger(&self, device: &Device) {
        if let Some(c) = find_char(device, PAIRING_TRIGGER_CHARACTERISTIC).await {
            if let Err(e) = c.write(&[0x09]).await {
                warn!("pairing trigger write failed: {e}");
            } else {
                debug!("wrote 0x09 to pairing trigger (server mode)");
            }
        }
    }

    pub async fn disconnect(&self) {
        let _ = self.connected_tx.send(false);
        let mut inner = self.inner.lock().unwrap();
        inner.gatt_server = None;
    }

    pub async fn forget(&self) -> Result<(), PebbleError> {
        let address = Address::from_str(&self.address)
            .map_err(|e| PebbleError::Other(format!("invalid address: {e}")))?;
        let session = bluer::Session::new().await?;
        let adapter = session.adapter(&self.adapter_name)?;
        adapter.remove_device(address).await?;
        info!("removed {} from BlueZ (stale bond cleared)", self.address);
        Ok(())
    }

    // ---- public API ----

    pub async fn update_time(&self) -> Result<(), PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        let now = Local::now();
        let utc_ts = now.timestamp() as u32;
        let offset_minutes = (now.offset().local_minus_utc() / 60) as i16;
        let tz_name = now.format("%Z").to_string();
        debug!("setting watch time: utc={utc_ts} offset={offset_minutes}min tz={tz_name:?}");
        self.send_pebble(Endpoint::Time, &build_set_utc(utc_ts, offset_minutes, &tz_name))
    }

    pub async fn launch_app(&self, app_uuid: &str) -> Result<(), PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        let payload = build_app_run_state(AppRunStateCmd::Start, app_uuid)
            .ok_or_else(|| PebbleError::Other(format!("invalid UUID: {app_uuid}")))?;
        self.send_pebble(Endpoint::AppRunState, &payload)
    }

    pub async fn stop_app(&self, app_uuid: &str) -> Result<(), PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        let payload = build_app_run_state(AppRunStateCmd::Stop, app_uuid)
            .ok_or_else(|| PebbleError::Other(format!("invalid UUID: {app_uuid}")))?;
        self.send_pebble(Endpoint::AppRunState, &payload)
    }

    pub async fn send_app_message(
        &self,
        app_uuid: &str,
        data: HashMap<u32, AppMessageValue>,
        wait_ack: bool,
        ack_timeout_secs: f64,
    ) -> Result<u8, PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        let txn = {
            let mut inner = self.inner.lock().unwrap();
            inner.txn = inner.txn.wrapping_add(1);
            inner.txn
        };
        let body = build_app_message_push(txn, app_uuid, &data)
            .ok_or_else(|| PebbleError::Other(format!("invalid UUID: {app_uuid}")))?;

        let rx = if wait_ack {
            let (tx, rx) = oneshot::channel::<bool>();
            self.inner.lock().unwrap().pending.insert(txn, tx);
            Some(rx)
        } else {
            None
        };

        self.send_pebble(Endpoint::AppMessage, &body)?;

        if let Some(rx) = rx {
            match timeout(Duration::from_secs_f64(ack_timeout_secs), rx).await {
                Ok(Ok(true)) => {}
                Ok(Ok(false)) => return Err(PebbleError::Nack(txn)),
                Ok(Err(_)) | Err(_) => {
                    self.inner.lock().unwrap().pending.remove(&txn);
                    warn!(
                        "no ACK for transaction {txn} within {ack_timeout_secs}s \
                         (message may still have arrived)"
                    );
                }
            }
        }
        Ok(txn)
    }

    pub async fn send_notification(
        &self,
        title: &str,
        body: &str,
        subtitle: &str,
        category: NotificationCategory,
    ) -> Result<u16, PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        let token = rand_u16();
        let now = Local::now().timestamp() as u32;
        let payload = build_notification(title, body, subtitle, now, token, category)
            .map_err(|e| PebbleError::Other(e.to_string()))?;
        debug!("sending notification token={token} title={title:?} category={category:?}");
        self.send_pebble(Endpoint::BlobDb, &payload)?;
        Ok(token)
    }


    /// Write "activityPreferences" (and optionally "hrmPreferences") to the
    /// BlobDB PREFERENCES store, then trigger a DataLog sync from the watch.
    pub async fn activate_health(
        &self,
        height_cm: u16,
        weight_kg: u16,
        age: u8,
        gender: u8,
        hrm_enabled: bool,
    ) -> Result<(), PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        let token = rand_u16();
        let blob = build_activate_health_blob(height_cm, weight_kg, age, gender);
        let payload = build_blobdb_str_insert(BlobDBId::Preferences, "activityPreferences", &blob, token)
            .map_err(|e| PebbleError::Other(e.to_string()))?;
        self.send_pebble(Endpoint::BlobDb, &payload)?;

        let hrm_token = rand_u16();
        let hrm_blob = build_hrm_blob(hrm_enabled);
        let hrm_payload = build_blobdb_str_insert(BlobDBId::Preferences, "hrmPreferences", &hrm_blob, hrm_token)
            .map_err(|e| PebbleError::Other(e.to_string()))?;
        self.send_pebble(Endpoint::BlobDb, &hrm_payload)?;

        debug!("health preferences written; triggering sync");
        self.fetch_health_data()
    }

    /// Ask the watch to flush pending health records via DataLog sessions.
    pub fn fetch_health_data(&self) -> Result<(), PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        // REPORTSESSIONS prompts the watch to open DataLog sessions for pending data.
        self.send_pebble(Endpoint::DataLog, &build_report_sessions())?;
        // HealthSync request additionally triggers a full flush.
        self.send_pebble(Endpoint::HealthSync, &build_health_sync_request())
    }

    fn send_pebble(&self, endpoint: Endpoint, payload: &[u8]) -> Result<(), PebbleError> {
        let message = pebble_pack(endpoint, payload)
            .ok_or_else(|| PebbleError::Other("payload too large for Pebble Protocol".into()))?;
        let inner = self.inner.lock().unwrap();
        if let Some(srv) = &inner.gatt_server {
            srv.send(message);
            Ok(())
        } else {
            Err(PebbleError::NotConnected)
        }
    }
}

// ---- Pebble Protocol dispatch (called from gatt_server on_data callback) ----

fn on_pebble_message(message: Vec<u8>, inner: &Arc<Mutex<PebbleInner>>) {
    let Some((endpoint_raw, payload)) = pebble_unpack(&message) else {
        return;
    };
    trace!("rx endpoint={endpoint_raw} len={}", payload.len());

    match Endpoint::from_u16(endpoint_raw) {
        Some(Endpoint::PhoneVersion) => {
            if let Some(reply) = pebble_pack(Endpoint::PhoneVersion, &build_phone_version_response()) {
                if let Some(srv) = &inner.lock().unwrap().gatt_server {
                    srv.send(reply);
                }
            }
            debug!("watch requested phone version; replied");
        }
        Some(Endpoint::Ping) => {
            if let Some(cookie) = parse_ping(payload) {
                debug!("ping cookie={cookie}; replying pong");
                if let Some(reply) = pebble_pack(Endpoint::Ping, &build_pong(cookie)) {
                    if let Some(srv) = &inner.lock().unwrap().gatt_server {
                        srv.send(reply);
                    }
                }
            }
        }
        Some(Endpoint::AppMessage) => {
            on_app_message(payload.to_vec(), inner);
        }
        Some(Endpoint::DataLog) => {
            on_datalog_message(payload.to_vec(), inner);
        }
        Some(Endpoint::HealthSync) => {
            debug!("health sync ACK from watch");
        }
        Some(Endpoint::BlobDb) => {
            if let Some((token, status)) = parse_blobdb_response(payload) {
                match BlobDBStatus::from_u8(status) {
                    Some(BlobDBStatus::Success) => debug!("BlobDB token={token} -> Success"),
                    Some(s) => warn!("BlobDB token={token} -> {s:?}"),
                    None => debug!("BlobDB token={token} -> unknown status {status}"),
                }
            }
        }
        _ => {} // unknown endpoint; ignore quietly
    }
}

fn on_app_message(payload: Vec<u8>, inner: &Arc<Mutex<PebbleInner>>) {
    trace!("inbound APP_MESSAGE raw: {} bytes", payload.len());
    let Some(parsed) = parse_app_message(&payload) else {
        return;
    };
    match parsed.cmd {
        AppMessageCmd::Push => {
            if let (Some(uuid), Some(data)) = (parsed.app_uuid, parsed.data) {
                // ACK the push.
                if let Some(ack) = pebble_pack(Endpoint::AppMessage, &build_app_message_ack(parsed.txn)) {
                    let inner_g = inner.lock().unwrap();
                    if let Some(srv) = &inner_g.gatt_server {
                        srv.send(ack);
                    }
                }
                debug!("inbound PUSH txn={} uuid={uuid} data={data:?}", parsed.txn);
                let handlers: Vec<_> = inner.lock().unwrap().app_message_handlers.clone();
                for h in handlers {
                    h(uuid.clone(), data.clone());
                }
            }
        }
        AppMessageCmd::Ack => {
            debug!("inbound ACK txn={}", parsed.txn);
            resolve_pending(parsed.txn, true, inner);
            let handlers: Vec<_> = inner.lock().unwrap().ack_handlers.clone();
            for h in handlers {
                h(parsed.txn);
            }
        }
        AppMessageCmd::Nack => {
            debug!("inbound NACK txn={}", parsed.txn);
            resolve_pending(parsed.txn, false, inner);
            let handlers: Vec<_> = inner.lock().unwrap().nack_handlers.clone();
            for h in handlers {
                h(parsed.txn);
            }
        }
    }
}

fn on_datalog_message(payload: Vec<u8>, inner: &Arc<Mutex<PebbleInner>>) {
    let Some((cmd, handle, rest)) = datalog::parse_header(&payload) else {
        return;
    };

    match cmd {
        DATALOG_OPENSESSION => {
            let Some(session) = datalog::parse_opensession(handle, rest) else {
                warn!("DataLog OPENSESSION: failed to parse (handle={handle})");
                return;
            };
            debug!(
                "DataLog OPENSESSION handle={handle} tag={} item_size={}",
                session.tag, session.item_size
            );
            inner.lock().unwrap().datalog_sessions.insert(handle, session);
            let ack = pebble_pack(Endpoint::DataLog, &build_reply(handle, true));
            if let Some(pkt) = ack {
                if let Some(srv) = &inner.lock().unwrap().gatt_server {
                    srv.send(pkt);
                }
            }
        }
        DATALOG_SENDDATA => {
            let Some((items_left, crc, record_bytes)) = datalog::parse_senddata(rest) else {
                warn!("DataLog SENDDATA: failed to parse (handle={handle})");
                return;
            };
            let batch = {
                let guard = inner.lock().unwrap();
                guard.datalog_sessions.get(&handle).map(|s| DatalogData {
                    tag: s.tag,
                    app_uuid: s.app_uuid,
                    session_timestamp: s.opened_at,
                    items_left,
                    crc,
                    item_type: s.item_type,
                    item_size: s.item_size,
                    data: record_bytes.to_vec(),
                })
            };
            if let Some(batch) = batch {
                debug!(
                    "DataLog SENDDATA handle={handle} tag={} bytes={} items_left={items_left}",
                    batch.tag,
                    batch.data.len()
                );
                // ACK before dispatching to handlers so protocol latency is not
                // gated on callback processing time.
                let ack = pebble_pack(Endpoint::DataLog, &build_reply(handle, true));
                if let Some(pkt) = ack {
                    if let Some(srv) = &inner.lock().unwrap().gatt_server {
                        srv.send(pkt);
                    }
                }
                let handlers: Vec<_> = inner.lock().unwrap().health_handlers.clone();
                for h in handlers {
                    h(batch.clone());
                }
            } else {
                warn!("DataLog SENDDATA for unknown session handle={handle}; sending NACK");
                let nack = pebble_pack(Endpoint::DataLog, &build_reply(handle, false));
                if let Some(pkt) = nack {
                    if let Some(srv) = &inner.lock().unwrap().gatt_server {
                        srv.send(pkt);
                    }
                }
            }
        }
        DATALOG_CLOSE => {
            debug!("DataLog CLOSE handle={handle}");
            inner.lock().unwrap().datalog_sessions.remove(&handle);
            let ack = pebble_pack(Endpoint::DataLog, &build_reply(handle, true));
            if let Some(pkt) = ack {
                if let Some(srv) = &inner.lock().unwrap().gatt_server {
                    srv.send(pkt);
                }
            }
        }
        DATALOG_TIMEOUT => {
            debug!("DataLog TIMEOUT handle={handle}");
            inner.lock().unwrap().datalog_sessions.remove(&handle);
        }
        _ => {
            debug!("DataLog unknown cmd={cmd:#04x} handle={handle}");
        }
    }
}

fn resolve_pending(txn: u8, acked: bool, inner: &Arc<Mutex<PebbleInner>>) {
    let mut guard = inner.lock().unwrap();
    if let Some(sender) = guard.pending.remove(&txn) {
        let _ = sender.send(acked);
    } else if !guard.pending.is_empty() {
        // Firmware quirk: ACK txn doesn't match what we sent. Resolve oldest.
        if let Some(oldest) = guard.pending.keys().copied().next() {
            debug!("ACK txn={txn} had no match; resolving oldest pending txn={oldest}");
            if let Some(sender) = guard.pending.remove(&oldest) {
                let _ = sender.send(acked);
            }
        }
    }
}

async fn find_char(device: &Device, uuid: bluer::Uuid) -> Option<bluer::gatt::remote::Characteristic> {
    for service in device.services().await.ok()? {
        for c in service.characteristics().await.ok()? {
            if c.uuid().await.map(|u| u == uuid).unwrap_or(false) {
                return Some(c);
            }
        }
    }
    None
}

fn rand_u16() -> u16 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    (t.subsec_nanos() & 0xFFFF) as u16
}

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
        app_run_state::{build_app_run_state, parse_app_run_state, AppRunStateCmd},
        blob_db::{
            build_blobdb2_mark_all_dirty, build_blobdb2_syncdone_response,
            build_blobdb2_version, build_blobdb2_write_response, build_blobdb2_writeback_response,
            build_blobdb_insert, build_blobdb_insert_with_timestamp, build_blobdb_str_insert, build_notification,
            build_weather_blob, build_weather_prefs_blob, parse_blobdb2_incoming, parse_blobdb_response,
            BlobDB2Incoming, BlobDBId, BlobDBStatus, NotificationCategory, WeatherType,
        },
        datalog::{
            self, build_reply, build_report_sessions, DatalogData, DatalogSession,
            DATALOG_CLOSE, DATALOG_OPENSESSION, DATALOG_SENDDATA, DATALOG_TIMEOUT,
        },
        health::{build_activate_health_blob, build_health_sync_request, build_hrm_blob},
        phone_version::build_phone_version_response,
        ping::{build_pong, parse_ping},
        reset::{build_reset, ResetCommand},
        screenshot::{
            build_screenshot_request, decode_to_rgba, parse_screenshot_header,
            ScreenshotResponseCode, ScreenshotVersion,
        },
        system::{
            build_watch_color_request, build_watch_version_request, parse_factory_data_response,
            parse_watch_color, parse_watch_version_response, system_message_type, WatchColorInfo,
            WatchVersionInfo, WATCH_VERSION_RESPONSE,
        },
        time::build_set_utc,
        pebble_pack, pebble_unpack, Endpoint,
    },
    error::PebbleError,
    transport::{
        agent::build_pairing_agent,
        gatt_server::{start_gatt_server, PebbleGattServerHandle},
    },
    uuids::{
        BATTERY_LEVEL_CHARACTERISTIC, CONNECTION_PARAMS_CHARACTERISTIC,
        CONNECTIVITY_CHARACTERISTIC, MTU_CHARACTERISTIC, PAIRING_TRIGGER_CHARACTERISTIC,
    },
};

pub type AppMessageHandler =
    Arc<dyn Fn(String, HashMap<u32, AppMessageValue>) + Send + Sync + 'static>;
pub type AckHandler = Arc<dyn Fn(u8) + Send + Sync + 'static>;
pub type NackHandler = Arc<dyn Fn(u8) + Send + Sync + 'static>;
pub type HealthDataHandler = Arc<dyn Fn(DatalogData) + Send + Sync + 'static>;
/// Handler for records the watch pushes back over BlobDB2 (Write/WriteBack).
/// Arguments: `(db_id, key, value)` — `db_id` matches `BlobDBId` (e.g. 7 =
/// HealthParams, 12 = WatchPrefs) so a single handler can route by database.
pub type WatchPrefHandler = Arc<dyn Fn(u8, String, Vec<u8>) + Send + Sync + 'static>;
/// Handler called with the watch battery percentage (0–100) when it changes.
pub type BatteryHandler = Arc<dyn Fn(u8) + Send + Sync + 'static>;
/// Handler called when an app opens/closes on the watch: `(app_uuid, running)`.
pub type AppRunStateHandler = Arc<dyn Fn(String, bool) + Send + Sync + 'static>;

/// A decoded watch screenshot: RGBA8888 pixels, row-major (`width*height*4` bytes).
#[derive(Debug, Clone)]
pub struct Screenshot {
    pub width: u32,
    pub height: u32,
    pub pixels: Vec<u8>,
}

/// Raw framebuffer handed from the dispatch to the awaiting `take_screenshot`.
struct RawScreenshot {
    version: ScreenshotVersion,
    width: u32,
    height: u32,
    data: Vec<u8>,
}

/// In-flight screenshot reassembly state (header, then accumulating data).
struct ScreenshotAccumulator {
    /// Identifies the originating `take_screenshot` so its cleanup can't clobber
    /// a different request that started after this one finished.
    request_id: u64,
    version: Option<ScreenshotVersion>,
    width: u32,
    height: u32,
    expected: usize,
    buffer: Vec<u8>,
    done: oneshot::Sender<Result<RawScreenshot, String>>,
}

struct PebbleInner {
    app_message_handlers: Vec<AppMessageHandler>,
    ack_handlers: Vec<AckHandler>,
    nack_handlers: Vec<NackHandler>,
    health_handlers: Vec<HealthDataHandler>,
    watch_pref_handlers: Vec<WatchPrefHandler>,
    battery_handlers: Vec<BatteryHandler>,
    /// Latest watch battery percentage (0–100); `None` until first read.
    battery_level: Option<u8>,
    app_run_state_handlers: Vec<AppRunStateHandler>,
    /// In-flight screenshot reassembly, if a `take_screenshot` is awaiting.
    screenshot: Option<ScreenshotAccumulator>,
    /// Monotonic id assigned to each screenshot request.
    screenshot_seq: u64,
    /// transaction_id → future resolved when watch ACK/NACKs it
    pending: HashMap<u8, oneshot::Sender<bool>>,
    /// BlobDB2 token → future resolved when watch sends the matching response
    blobdb2_pending: HashMap<u16, oneshot::Sender<BlobDB2Incoming>>,
    /// Futures awaiting a WatchVersionResponse (endpoint 16). All are resolved
    /// when the next response arrives.
    watch_version_pending: Vec<oneshot::Sender<WatchVersionInfo>>,
    /// Futures awaiting a factory-registry watch-color response (endpoint 5001).
    /// `None` is sent on an error reply or unknown color.
    watch_color_pending: Vec<oneshot::Sender<Option<&'static WatchColorInfo>>>,
    txn: u8,
    /// Handle to the GATT server send channel (set once server is started).
    gatt_server: Option<PebbleGattServerHandle>,
    /// Open DataLog sessions keyed by the 1-byte handle from the watch.
    datalog_sessions: HashMap<u8, DatalogSession>,
    /// BlobDB2 protocol version negotiated at connect time (0 = v0/unknown, 1+ = InsertWithTimestamp capable).
    blob_db_version: u8,
}

impl PebbleInner {
    fn new() -> Self {
        Self {
            app_message_handlers: Vec::new(),
            ack_handlers: Vec::new(),
            nack_handlers: Vec::new(),
            health_handlers: Vec::new(),
            watch_pref_handlers: Vec::new(),
            battery_handlers: Vec::new(),
            battery_level: None,
            app_run_state_handlers: Vec::new(),
            screenshot: None,
            screenshot_seq: 0,
            pending: HashMap::new(),
            blobdb2_pending: HashMap::new(),
            watch_version_pending: Vec::new(),
            watch_color_pending: Vec::new(),
            txn: 0,
            gatt_server: None,
            datalog_sessions: HashMap::new(),
            blob_db_version: 0,
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

    /// Register a handler called with the watch battery percentage (0–100)
    /// whenever it changes (and once with the initial value on connect).
    pub fn on_battery(&self, handler: BatteryHandler) {
        self.inner.lock().unwrap().battery_handlers.push(handler);
    }

    /// The latest known watch battery percentage (0–100), or `None` if not yet read.
    pub fn battery_level(&self) -> Option<u8> {
        self.inner.lock().unwrap().battery_level
    }

    /// Register a handler called when an app opens/closes on the watch:
    /// `(app_uuid, running)` where `running` is true on launch, false on exit.
    pub fn on_app_run_state(&self, handler: AppRunStateHandler) {
        self.inner.lock().unwrap().app_run_state_handlers.push(handler);
    }

    pub fn on_watch_pref(&self, handler: WatchPrefHandler) {
        self.inner.lock().unwrap().watch_pref_handlers.push(handler);
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
                debug!("watch disconnected from PPoGATT server");
                let _ = connected_tx_for_disc.send(false);
                // Drain pending and snapshot handlers while holding the lock,
                // then drop it before invoking handlers to avoid re-entrant deadlock.
                let (pending, nack_handlers) = {
                    let mut inner = inner_for_disc.lock().unwrap();
                    let pending: Vec<(u8, oneshot::Sender<bool>)> =
                        inner.pending.drain().collect();
                    let nack_handlers = inner.nack_handlers.clone();
                    inner.datalog_sessions.clear();
                    inner.blobdb2_pending.clear();
                    if let Some(acc) = inner.screenshot.take() {
                        let _ = acc.done.send(Err("watch disconnected".into()));
                    }
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

        // 5b. Read + subscribe to the standard BLE battery level.
        self.subscribe_battery(&device).await;

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

        // 8. BlobDB2 version handshake — determines whether to use InsertWithTimestamp
        //    and triggers the watch to sync its preferences back to us.
        let blob_db_version = self.negotiate_blobdb2_version().await;
        self.inner.lock().unwrap().blob_db_version = blob_db_version;
        if blob_db_version >= 1 {
            let _ = self.send_pebble(
                Endpoint::BlobDbV2,
                &build_blobdb2_mark_all_dirty(rand_u16(), BlobDBId::WatchPrefs),
            );
            debug!("BlobDB2 v{blob_db_version}: MarkAllDirty sent for WatchPrefs");
        }

        // 9. Monitor device events for disconnect, with a keepalive poll fallback.
        let connected_tx = Arc::clone(&self.connected_tx);
        tokio::spawn(async move {
            match device.events().await {
                Err(e) => {
                    warn!("could not subscribe to device events: {e}; falling back to keepalive polling");
                    let mut poll = tokio::time::interval(Duration::from_secs(60));
                    poll.tick().await; // skip the immediate first tick
                    loop {
                        poll.tick().await;
                        match device.is_connected().await {
                            Ok(false) | Err(_) => {
                                debug!("keepalive: device not connected; triggering disconnect");
                                let _ = connected_tx.send(false);
                                return;
                            }
                            Ok(true) => {}
                        }
                    }
                }
                Ok(mut events) => {
                    let mut poll = tokio::time::interval(Duration::from_secs(60));
                    poll.tick().await; // skip the immediate first tick
                    loop {
                        tokio::select! {
                            event = events.next() => match event {
                                Some(bluer::DeviceEvent::PropertyChanged(
                                    bluer::DeviceProperty::Connected(false),
                                )) => {
                                    debug!("BlueZ reports device Connected=False; watch dropped");
                                    let _ = connected_tx.send(false);
                                    return;
                                }
                                None => {
                                    // BlueZ removed the device from its cache rather than
                                    // emitting Connected=False — treat as disconnect.
                                    debug!("device event stream ended; triggering disconnect");
                                    let _ = connected_tx.send(false);
                                    return;
                                }
                                _ => {}
                            },
                            _ = poll.tick() => {
                                match device.is_connected().await {
                                    Ok(false) | Err(_) => {
                                        debug!("keepalive: device not connected; triggering disconnect");
                                        let _ = connected_tx.send(false);
                                        return;
                                    }
                                    Ok(true) => {}
                                }
                            }
                        }
                    }
                }
            }
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

    /// Read the watch battery level once and subscribe to change notifications
    /// (standard BLE Battery Service). Updates fire registered `on_battery`
    /// handlers. Best-effort — a watch without the service is simply silent.
    async fn subscribe_battery(&self, device: &Device) {
        // Scope the cache to this session: if a Pebble is reused across
        // reconnects, the first reading must not be deduped against a stale value.
        self.inner.lock().unwrap().battery_level = None;
        let Some(c) = find_char(device, BATTERY_LEVEL_CHARACTERISTIC).await else {
            debug!("no battery characteristic; battery level unavailable");
            return;
        };
        // Initial read.
        match c.read().await {
            Ok(data) => {
                if let Some(&level) = data.first() {
                    update_battery(&self.inner, level);
                }
            }
            Err(e) => debug!("battery read failed: {e}"),
        }
        // Subscribe to notifications in the background. A short delay avoids a
        // GATT auth error seen immediately after bonding (per libpebble3).
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            match c.notify().await {
                Ok(stream) => {
                    debug!("subscribed to battery level");
                    tokio::pin!(stream);
                    while let Some(data) = stream.next().await {
                        if let Some(&level) = data.first() {
                            update_battery(&inner, level);
                        }
                    }
                }
                Err(e) => warn!("subscribe to battery failed: {e}"),
            }
        });
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

    async fn negotiate_blobdb2_version(&self) -> u8 {
        let token = rand_u16();
        let (tx, rx) = oneshot::channel::<BlobDB2Incoming>();
        self.inner.lock().unwrap().blobdb2_pending.insert(token, tx);
        if self.send_pebble(Endpoint::BlobDbV2, &build_blobdb2_version(token)).is_err() {
            self.inner.lock().unwrap().blobdb2_pending.remove(&token);
            return 0;
        }
        match timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(BlobDB2Incoming::VersionResponse { status, version, .. })) if status == BlobDBStatus::Success as u8 => {
                debug!("BlobDB2 version: {version}");
                version
            }
            _ => {
                self.inner.lock().unwrap().blobdb2_pending.remove(&token);
                debug!("BlobDB2 version query timed out; assuming v0");
                0
            }
        }
    }

    pub async fn disconnect(&self) {
        let _ = self.connected_tx.send(false);
        let mut inner = self.inner.lock().unwrap();
        inner.gatt_server = None;
        // Fail any in-flight watch-info requests immediately instead of leaving
        // their callers pending until the per-request timeout fires.
        inner.watch_version_pending.clear();
        inner.watch_color_pending.clear();
        if let Some(acc) = inner.screenshot.take() {
            let _ = acc.done.send(Err("watch disconnected".into()));
        }
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

    /// Query the watch's version info (endpoint 16): firmware versions, board,
    /// serial, BT address, language, and protocol capabilities. Times out after
    /// 10s if the watch doesn't reply.
    pub async fn get_watch_version(&self) -> Result<WatchVersionInfo, PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        let (tx, rx) = oneshot::channel::<WatchVersionInfo>();
        self.inner.lock().unwrap().watch_version_pending.push(tx);
        let result = match self.send_pebble(Endpoint::WatchVersion, &build_watch_version_request()) {
            Err(e) => {
                drop(rx); // cancel our waiter
                Err(e)
            }
            Ok(()) => match timeout(Duration::from_secs(10), rx).await {
                Ok(Ok(info)) => Ok(info),
                _ => Err(PebbleError::Other("watch version request timed out".into())),
            },
        };
        // Drop our now-cancelled waiter (and any other dead ones); live waiters
        // from concurrent requests are kept.
        self.inner.lock().unwrap().watch_version_pending.retain(|s| !s.is_closed());
        result
    }

    /// Query the watch's manufacturing color/variant (factory registry, endpoint
    /// 5001). Returns `None` if the watch reports an error or an unknown color.
    /// Times out after 10s. (libpebble3 bundles this into the version request;
    /// here it's a separate call.)
    pub async fn get_watch_color(&self) -> Result<Option<&'static WatchColorInfo>, PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        let (tx, rx) = oneshot::channel::<Option<&'static WatchColorInfo>>();
        self.inner.lock().unwrap().watch_color_pending.push(tx);
        let result = match self.send_pebble(Endpoint::FactoryRegistry, &build_watch_color_request()) {
            Err(e) => {
                drop(rx); // cancel our waiter
                Err(e)
            }
            Ok(()) => match timeout(Duration::from_secs(10), rx).await {
                Ok(Ok(color)) => Ok(color),
                _ => Err(PebbleError::Other("watch color request timed out".into())),
            },
        };
        self.inner.lock().unwrap().watch_color_pending.retain(|s| !s.is_closed());
        result
    }

    /// Capture the watch screen (endpoint 8000). Returns a decoded RGBA image.
    /// Times out after 30s; errors if a capture is already in progress or the
    /// watch reports a non-OK response code.
    pub async fn take_screenshot(&self) -> Result<Screenshot, PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        let (tx, rx) = oneshot::channel::<Result<RawScreenshot, String>>();
        let request_id = {
            let mut guard = self.inner.lock().unwrap();
            if guard.screenshot.is_some() {
                return Err(PebbleError::Other("a screenshot is already in progress".into()));
            }
            guard.screenshot_seq += 1;
            let request_id = guard.screenshot_seq;
            guard.screenshot = Some(ScreenshotAccumulator {
                request_id,
                version: None,
                width: 0,
                height: 0,
                expected: 0,
                buffer: Vec::new(),
                done: tx,
            });
            request_id
        };
        if let Err(e) = self.send_pebble(Endpoint::Screenshot, &build_screenshot_request()) {
            self.clear_screenshot(request_id);
            return Err(e);
        }
        let result = timeout(Duration::from_secs(30), rx).await;
        // Drop the accumulator only if it's still ours and pending (timeout).
        self.clear_screenshot(request_id);
        match result {
            Ok(Ok(Ok(raw))) => Ok(Screenshot {
                width: raw.width,
                height: raw.height,
                pixels: decode_to_rgba(raw.version, raw.width, raw.height, &raw.data),
            }),
            Ok(Ok(Err(e))) => Err(PebbleError::Other(e)),
            _ => Err(PebbleError::Other("screenshot timed out".into())),
        }
    }

    /// Drop the in-flight screenshot accumulator only if it still belongs to
    /// `request_id` — avoids clobbering a request that started after this one
    /// already completed (and the dispatch took the accumulator).
    fn clear_screenshot(&self, request_id: u64) {
        let mut guard = self.inner.lock().unwrap();
        if guard.screenshot.as_ref().map(|a| a.request_id) == Some(request_id) {
            guard.screenshot = None;
        }
    }

    /// Reboot the watch (endpoint 2003). The watch drops the BLE link; the
    /// supervisor will reconnect. Fire-and-forget (no reply).
    pub fn reboot_watch(&self) -> Result<(), PebbleError> {
        self.send_reset(ResetCommand::Reset)
    }

    /// Reboot the watch into its recovery (PRF) firmware (endpoint 2003).
    pub fn reset_into_recovery(&self) -> Result<(), PebbleError> {
        self.send_reset(ResetCommand::ResetIntoPrf)
    }

    /// Trigger a core dump on the watch (endpoint 2003).
    pub fn create_core_dump(&self) -> Result<(), PebbleError> {
        self.send_reset(ResetCommand::CoreDump)
    }

    /// Factory-reset the watch (endpoint 2003). **Destructive** — wipes all
    /// watch data and unpairs. Fire-and-forget.
    pub fn factory_reset(&self) -> Result<(), PebbleError> {
        warn!("sending FACTORY RESET to the watch");
        self.send_reset(ResetCommand::FactoryReset)
    }

    fn send_reset(&self, command: ResetCommand) -> Result<(), PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        debug!("sending reset command {command:?}");
        self.send_pebble(Endpoint::Reset, &build_reset(command))
    }

    pub async fn update_time(&self) -> Result<(), PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        let now = Local::now();
        let utc_ts = now.timestamp() as u32;
        let offset_minutes = (now.offset().local_minus_utc() / 60) as i16;
        let tz_name = iana_time_zone::get_timezone()
            .unwrap_or_else(|_| now.format("%Z").to_string());
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


    /// Push weather data to the Pebble built-in weather app via BlobDB.
    ///
    /// Uses `InsertWithTimestamp` (cmd=0x0D) when BlobDB2 v1 was negotiated at
    /// connect time, and falls back to plain `Insert` otherwise. Temperatures
    /// are in Celsius.
    ///
    /// `location_key` is a 16-byte UUID that identifies the weather location.
    /// Re-using the same UUID on subsequent calls updates the existing entry.
    pub async fn push_weather(
        &self,
        location_key: &[u8; 16],
        location_name: &str,
        forecast_short: &str,
        current_temp: i16,
        current_weather: WeatherType,
        today_high: i16,
        today_low: i16,
        tomorrow_weather: WeatherType,
        tomorrow_high: i16,
        tomorrow_low: i16,
        is_current_location: bool,
    ) -> Result<(), PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        let now = chrono::Local::now().timestamp() as u32;
        let blob = build_weather_blob(
            location_name,
            forecast_short,
            current_temp,
            current_weather,
            today_high,
            today_low,
            tomorrow_weather,
            tomorrow_high,
            tomorrow_low,
            now,
            is_current_location,
        );
        let token = rand_u16();
        let blob_db_version = self.inner.lock().unwrap().blob_db_version;
        let payload = if blob_db_version >= 1 {
            build_blobdb_insert_with_timestamp(BlobDBId::Weather, location_key, &blob, now, token)
        } else {
            build_blobdb_insert(BlobDBId::Weather, location_key, &blob, token)
        }
        .map_err(|e| PebbleError::Other(e.to_string()))?;
        debug!(
            "push_weather token={token} location={location_name:?} \
             temp={current_temp}°C blobdb_version={blob_db_version}"
        );
        self.send_pebble(Endpoint::BlobDb, &payload)?;

        // Write the "weatherApp" AppConfigs entry so the watch knows which
        // location UUIDs are active. Without this the weather app shows
        // "no location information" even though the Weather BlobDB insert succeeds.
        let prefs_token = rand_u16();
        let prefs_blob = build_weather_prefs_blob(&[*location_key]);
        let prefs_payload =
            build_blobdb_str_insert(BlobDBId::AppConfigs, "weatherApp", &prefs_blob, prefs_token)
                .map_err(|e| PebbleError::Other(e.to_string()))?;
        debug!("push_weather prefs token={prefs_token}");
        self.send_pebble(Endpoint::BlobDb, &prefs_payload)
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
        let payload = build_blobdb_str_insert(BlobDBId::HealthParams, "activityPreferences", &blob, token)
            .map_err(|e| PebbleError::Other(e.to_string()))?;
        self.send_pebble(Endpoint::BlobDb, &payload)?;

        let hrm_token = rand_u16();
        let hrm_blob = build_hrm_blob(hrm_enabled);
        let hrm_payload = build_blobdb_str_insert(BlobDBId::HealthParams, "hrmPreferences", &hrm_blob, hrm_token)
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

    /// Ask the watch to re-push its watch-side preferences via BlobDB2.
    ///
    /// The health profile ("activityPreferences", "hrmPreferences",
    /// "heartRatePreferences") lives in the WatchPrefs DB (id 12), not the
    /// HealthParams DB (id 7) — the latter returns NotSupported for MarkAllDirty.
    /// Records arrive asynchronously through any handler registered with
    /// [`Pebble::on_watch_pref`].
    ///
    /// Requires BlobDB2 v1+; returns an error on v0 watches.
    pub async fn fetch_health_params(&self) -> Result<(), PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        let version = self.inner.lock().unwrap().blob_db_version;
        if version < 1 {
            return Err(PebbleError::Other(
                "watch does not support BlobDB2 sync (v0); cannot fetch health params".into(),
            ));
        }
        debug!("requesting WatchPrefs BlobDB2 re-sync (MarkAllDirty)");
        self.send_pebble(
            Endpoint::BlobDbV2,
            &build_blobdb2_mark_all_dirty(rand_u16(), BlobDBId::WatchPrefs),
        )
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
        Some(Endpoint::WatchVersion) => {
            if payload.first() == Some(&WATCH_VERSION_RESPONSE) {
                match parse_watch_version_response(payload) {
                    Some(info) => {
                        debug!(
                            "watch version: fw={} board={} serial={}",
                            info.running.string_version, info.board, info.serial
                        );
                        let waiters =
                            std::mem::take(&mut inner.lock().unwrap().watch_version_pending);
                        for w in waiters {
                            let _ = w.send(info.clone());
                        }
                    }
                    None => {
                        warn!("WatchVersion: failed to parse response ({} bytes)", payload.len());
                        // Fail waiters now rather than letting them time out on a
                        // response we already know is malformed.
                        inner.lock().unwrap().watch_version_pending.clear();
                    }
                }
            }
        }
        Some(Endpoint::SystemMessage) => {
            // Firmware-update lifecycle / reconnect control. We don't drive
            // firmware updates yet; surface the message type for diagnostics.
            debug!("system message type={:?}", system_message_type(payload));
        }
        Some(Endpoint::FactoryRegistry) => {
            // Factory-registry reply (currently only used for watch color).
            let color = parse_factory_data_response(payload)
                .as_deref()
                .and_then(parse_watch_color);
            debug!("factory registry: color={:?}", color.map(|c| c.js_name));
            let waiters = std::mem::take(&mut inner.lock().unwrap().watch_color_pending);
            for w in waiters {
                let _ = w.send(color);
            }
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
        Some(Endpoint::AppRunState) => {
            // Watch reports an app opening (Start) or closing (Stop).
            if let Some((cmd, uuid)) = parse_app_run_state(payload) {
                let running = match cmd {
                    AppRunStateCmd::Start => true,
                    AppRunStateCmd::Stop => false,
                    AppRunStateCmd::Request => return, // phone→watch only
                };
                debug!("app run state: uuid={uuid} running={running}");
                let handlers: Vec<_> = inner.lock().unwrap().app_run_state_handlers.clone();
                for h in handlers {
                    h(uuid.clone(), running);
                }
            }
        }
        Some(Endpoint::DataLog) => {
            on_datalog_message(payload.to_vec(), inner);
        }
        Some(Endpoint::Screenshot) => {
            on_screenshot_message(payload, inner);
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
        Some(Endpoint::BlobDbV2) => {
            on_blobdb2_message(payload.to_vec(), inner);
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

/// Accumulate one inbound screenshot message: the first carries the header,
/// the rest are raw framebuffer bytes. Resolves the awaiting `take_screenshot`
/// once the full buffer arrives or on an error response.
fn on_screenshot_message(payload: &[u8], inner: &Arc<Mutex<PebbleInner>>) {
    enum Step {
        Continue,
        Error(String),
        Done,
    }
    let mut guard = inner.lock().unwrap();
    let Some(acc) = guard.screenshot.as_mut() else {
        return; // no screenshot in progress; ignore
    };
    let step = if acc.version.is_none() && acc.expected == 0 && acc.buffer.is_empty() {
        // First message: parse the header.
        match parse_screenshot_header(payload) {
            Some((header, data)) => {
                if header.response_code != ScreenshotResponseCode::Ok {
                    Step::Error(format!("watch returned {:?}", header.response_code))
                } else if let Some(version) = header.version {
                    match crate::endpoints::screenshot::expected_size(
                        version,
                        header.width,
                        header.height,
                    ) {
                        Some(expected) => {
                            acc.version = Some(version);
                            acc.width = header.width;
                            acc.height = header.height;
                            acc.expected = expected;
                            acc.buffer.extend_from_slice(data);
                            if acc.expected > 0 && acc.buffer.len() >= acc.expected {
                                Step::Done
                            } else {
                                Step::Continue
                            }
                        }
                        None => Step::Error("invalid screenshot dimensions".into()),
                    }
                } else {
                    Step::Error("unknown screenshot version".into())
                }
            }
            None => Step::Error("malformed screenshot header".into()),
        }
    } else {
        acc.buffer.extend_from_slice(payload);
        if acc.expected > 0 && acc.buffer.len() >= acc.expected {
            Step::Done
        } else {
            Step::Continue
        }
    };
    match step {
        Step::Continue => {}
        Step::Error(e) => {
            if let Some(acc) = guard.screenshot.take() {
                let _ = acc.done.send(Err(e));
            }
        }
        Step::Done => {
            if let Some(acc) = guard.screenshot.take() {
                let raw = RawScreenshot {
                    version: acc.version.expect("version set when done"),
                    width: acc.width,
                    height: acc.height,
                    data: acc.buffer,
                };
                let _ = acc.done.send(Ok(raw));
            }
        }
    }
}

/// Store a new battery level and fire `on_battery` handlers if it changed.
fn update_battery(inner: &Arc<Mutex<PebbleInner>>, level: u8) {
    let handlers = {
        let mut guard = inner.lock().unwrap();
        if guard.battery_level == Some(level) {
            return; // unchanged — don't re-fire
        }
        guard.battery_level = Some(level);
        guard.battery_handlers.clone()
    };
    debug!("battery level: {level}%");
    for h in handlers {
        h(level);
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

fn on_blobdb2_message(payload: Vec<u8>, inner: &Arc<Mutex<PebbleInner>>) {
    let Some(msg) = parse_blobdb2_incoming(&payload) else {
        warn!("BlobDB2: failed to parse {} bytes", payload.len());
        return;
    };

    match msg {
        BlobDB2Incoming::Write(w) => {
            let key = String::from_utf8_lossy(&w.key).trim_end_matches('\0').to_owned();
            debug!(
                "BlobDB2 {}: db={} key={key:?} val_len={}",
                if w.is_writeback { "WriteBack" } else { "Write" },
                w.db, w.value.len()
            );
            let resp = if w.is_writeback {
                build_blobdb2_writeback_response(w.token, BlobDBStatus::Success)
            } else {
                build_blobdb2_write_response(w.token, BlobDBStatus::Success)
            };
            blobdb2_send(inner, resp);
            let handlers: Vec<_> = inner.lock().unwrap().watch_pref_handlers.clone();
            for h in handlers {
                h(w.db, key.clone(), w.value.clone());
            }
        }
        BlobDB2Incoming::SyncDone { token, db } => {
            debug!("BlobDB2 SyncDone db={db}");
            blobdb2_send(inner, build_blobdb2_syncdone_response(token, BlobDBStatus::Success));
        }
        other => {
            if let Some(token) = other.response_token() {
                let sender = inner.lock().unwrap().blobdb2_pending.remove(&token);
                match sender {
                    Some(sender) => {
                        let _ = sender.send(other);
                    }
                    // No one is awaiting this token (e.g. a fire-and-forget
                    // MarkAllDirty). Log the status so the response isn't lost.
                    None => debug!("BlobDB2 unsolicited response: {other:?}"),
                }
            }
        }
    }
}

fn blobdb2_send(inner: &Arc<Mutex<PebbleInner>>, payload: Vec<u8>) {
    if let Some(pkt) = pebble_pack(Endpoint::BlobDbV2, &payload) {
        if let Some(srv) = &inner.lock().unwrap().gatt_server {
            srv.send(pkt);
        }
    }
}

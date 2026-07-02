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
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Local;
use tokio::{
    sync::{oneshot, watch},
    time::timeout,
};
use tracing::{debug, warn};

use crate::{
    endpoints::{
        app_message::{
            build_app_message_push,
            AppMessageValue,
        },
        app_run_state::{build_app_run_state, AppRunStateCmd},
        blob_db::{
            build_blobdb2_mark_all_dirty,
            build_blobdb_insert, build_blobdb_insert_with_timestamp, build_blobdb_str_insert, build_notification,
            build_weather_blob, build_weather_prefs_blob, BlobDBId, NotificationCategory, WeatherType,
        },
        datalog::build_report_sessions,
        health::{build_activate_health_blob, build_health_sync_request, build_hrm_blob},
        music::{
            build_update_current_track, build_update_play_state, build_update_player_info,
            build_update_volume, MusicPlaybackState,
            MusicRepeat, MusicShuffle,
        },
        phone_control::{build_incoming_call, build_missed_call, build_call_start, build_call_end},
        reset::{build_reset, ResetCommand},
        screenshot::{
            build_screenshot_request, decode_to_rgba,
        },
        system::{
            build_watch_color_request, build_watch_version_request, WatchColorInfo,
            WatchVersionInfo,
        },
        time::build_set_utc,
        pebble_pack, Endpoint,
    },
    error::PebbleError,
};

mod inner;
mod connection;
mod dispatch;

use dispatch::rand_u16;
pub(crate) use inner::{
    PebbleInner, RawScreenshot, ScreenshotAccumulator,
};
pub use inner::{
    AckHandler, AppMessageHandler, AppRunStateHandler, BatteryHandler,
    HealthDataHandler, MusicActionHandler, NackHandler, PhoneActionHandler,
    Screenshot, WatchPrefHandler,
};

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

    /// Register a handler called with each media-control action the watch sends
    /// (play/pause/next/volume/get-current-track).
    pub fn on_music_action(&self, handler: MusicActionHandler) {
        self.inner.lock().unwrap().music_action_handlers.push(handler);
    }

    /// Register a handler called when the watch sends a phone control action
    /// (answer / hangup).
    pub fn on_phone_action(&self, handler: PhoneActionHandler) {
        self.inner.lock().unwrap().phone_action_handlers.push(handler);
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
            Ok(Ok(Ok(raw))) => {
                debug!("screenshot captured: {}x{}", raw.width, raw.height);
                Ok(Screenshot {
                    width: raw.width,
                    height: raw.height,
                    pixels: decode_to_rgba(raw.version, raw.width, raw.height, &raw.data),
                })
            }
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

    // ---- Music (push now-playing to the watch) ----

    /// Tell the watch which media app is playing.
    pub async fn update_music_player_info(&self, pkg: &str, name: &str) -> Result<(), PebbleError> {
        debug!("music: updating player info");
        self.send_pebble(Endpoint::MusicControl, &build_update_player_info(pkg, name))
    }

    /// Push the current track. `track_length_ms`/`track_count`/`current_track`
    /// are optional but should be all-or-nothing (provide a contiguous prefix).
    pub async fn update_music_track(
        &self,
        artist: &str,
        album: &str,
        title: &str,
        track_length_ms: Option<u32>,
        track_count: Option<u32>,
        current_track: Option<u32>,
    ) -> Result<(), PebbleError> {
        debug!("music: updating current track");
        self.send_pebble(
            Endpoint::MusicControl,
            &build_update_current_track(
                artist,
                album,
                title,
                track_length_ms,
                track_count,
                current_track,
            ),
        )
    }

    /// Push playback state (state, position, play-rate %, shuffle, repeat).
    pub async fn update_music_play_state(
        &self,
        state: MusicPlaybackState,
        track_position_ms: u32,
        play_rate_pct: u32,
        shuffle: MusicShuffle,
        repeat: MusicRepeat,
    ) -> Result<(), PebbleError> {
        debug!("music: play state={state:?} pos={track_position_ms}ms rate={play_rate_pct}");
        self.send_pebble(
            Endpoint::MusicControl,
            &build_update_play_state(state, track_position_ms, play_rate_pct, shuffle, repeat),
        )
    }

    /// Push the current volume (0–100).
    pub async fn update_music_volume(&self, volume_percent: u8) -> Result<(), PebbleError> {
        if volume_percent > 100 {
            return Err(PebbleError::Other(format!(
                "music volume {volume_percent} out of range (0-100)"
            )));
        }
        debug!("music: volume={volume_percent}");
        self.send_pebble(Endpoint::MusicControl, &build_update_volume(volume_percent))
    }

    // ── Phone calls ────────────────────────────────────────────────────

    /// Push an incoming call to the watch (shows caller screen).
    pub async fn push_incoming_call(
        &self,
        cookie: u32,
        caller_number: &str,
        caller_name: &str,
    ) -> Result<(), PebbleError> {
        debug!("phone: incoming call cookie={cookie} from {caller_name}");
        self.send_pebble(
            Endpoint::PhoneControl,
            &build_incoming_call(cookie, caller_number, caller_name),
        )
    }

    /// Push a missed call notification to the watch.
    pub async fn push_missed_call(
        &self,
        cookie: u32,
        caller_number: &str,
        caller_name: &str,
    ) -> Result<(), PebbleError> {
        debug!("phone: missed call cookie={cookie} from {caller_name}");
        self.send_pebble(
            Endpoint::PhoneControl,
            &build_missed_call(cookie, caller_number, caller_name),
        )
    }

    /// Notify the watch that the call is now active (answered).
    pub async fn push_call_start(&self, cookie: u32) -> Result<(), PebbleError> {
        debug!("phone: call {cookie} started");
        self.send_pebble(Endpoint::PhoneControl, &build_call_start(cookie))
    }

    /// Notify the watch that the call has ended.
    pub async fn push_call_end(&self, cookie: u32) -> Result<(), PebbleError> {
        debug!("phone: call {cookie} ended");
        self.send_pebble(Endpoint::PhoneControl, &build_call_end(cookie))
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
        debug!("launching app uuid={app_uuid}");
        let payload = build_app_run_state(AppRunStateCmd::Start, app_uuid)
            .ok_or_else(|| PebbleError::Other(format!("invalid UUID: {app_uuid}")))?;
        self.send_pebble(Endpoint::AppRunState, &payload)
    }

    pub async fn stop_app(&self, app_uuid: &str) -> Result<(), PebbleError> {
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        debug!("stopping app uuid={app_uuid}");
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
            {
                let mut inner = self.inner.lock().unwrap();
                inner.pending.insert(txn, tx);
                inner.pending_order.push_back(txn);
            }
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
                    let mut inner = self.inner.lock().unwrap();
                    inner.pending.remove(&txn);
                    inner.pending_order.retain(|&k| k != txn);
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
    #[allow(clippy::too_many_arguments)]
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
        debug!("triggering health data sync");
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

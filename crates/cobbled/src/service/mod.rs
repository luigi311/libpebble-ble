//! D-Bus service interface (org.cobble.Daemon).
//!
//! Interface (org.cobble.Daemon on /org/cobble/Daemon):
//!
//!   Properties
//!     Connected     b    watch BLE link is up right now
//!     WatchAddress  s    configured watch address
//!     BatteryLevel  n    watch battery percentage (0–100), or -1 if unknown
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
//!     FetchHealthParams()
//!     GetHealthProfile() -> a{sv}  health profile keyed by field name (height_cm, weight_kg, age, gender, …, imperial_units)
//!     GetWatchSettings() -> a{sv}  general watch settings (db 12), key -> bool/uint32/string
//!     GetWatchVersion() -> a{sv}  firmware/board/serial/BT/language/capabilities/platform
//!     GetWatchColor() -> a{sv}  watch color/variant (protocol_number, js_name, description, watch_type, supports_hrm)
//!     Screenshot() -> ay  capture the watch screen as PNG bytes
//!     SetMusicPlayerInfo(s pkg, s name)
//!     SetMusicTrack(s artist, s album, s title, u track_length_ms, u track_count, u track_number)
//!     SetMusicPlaybackState(y state, u track_position_ms, u play_rate_pct, y shuffle, y repeat)
//!     SetMusicVolume(y volume_percent)
//!     RebootWatch()
//!     ResetIntoRecovery()
//!     CreateCoreDump()
//!     FactoryReset(b confirm)  (DESTRUCTIVE; requires confirm=true)
//!     Forget()  remove the Bluetooth bond (unpair); re-pairs on next reconnect
//!     PushWeather(ay location_key, s location_name, s forecast_short, n current_temp, y current_weather, n today_high, n today_low, y tomorrow_weather, n tomorrow_high, n tomorrow_low, b is_current_location)
//!     ReprocessHealthData()
//!
//!   Signals
//!     AppMessageReceived(s uuid, a{i(sv)} data)
//!     AckReceived(u txn)
//!     NackReceived(u txn)
//!     ConnectionChanged(b connected)
//!     HealthDataReceived(u tag, ay app_uuid, u session_timestamp, u items_left, u crc, y item_type, q item_size, ay data)
//!     HealthProfileReceived(a{sv} profile)
//!     WatchSettingReceived(s key, v value)
//!     BatteryChanged(n level)  watch battery percentage (-1 = unknown)
//!     AppRunStateChanged(s uuid, b running)  app opened/closed on the watch
//!     MusicActionReceived(s action)  media-control action from the watch
//!
//! AppMessage values cross the D-Bus hop as (tag, payload) pairs; see codec.rs.
#![allow(clippy::too_many_arguments)]

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use libpebble_ble::{
    ActivityPreferences, HeartRatePreferences, HrmPreferences,
    MusicPlaybackState, MusicRepeat, MusicShuffle, Pebble, WatchColorInfo, WatchPrefValue,
    WatchVersionInfo, WeatherType,
};

use crate::db::AppDb;
use tokio::sync::{mpsc, watch};
use tracing::{debug, warn};
use zbus::{
    interface,
    object_server::SignalEmitter,
    zvariant::OwnedValue,
    Connection,
};

use crate::codec::{decode_wire_dict, encode_wire_dict, WireDict};
use crate::notification::app_name_to_category;

mod state;
pub(crate) use state::{
    DaemonError, DaemonEvent, DaemonState, HealthProfile, MusicState,
    BUS_NAME, MUSIC_APP_UUID, OBJECT_PATH,
    dbus_val, watch_pref_owned_value,
};

/// Render watch version info as a self-describing `a{sv}` map. Optional fields
/// (recovery firmware, health/JS versions) are omitted when absent.
fn watch_version_to_map(info: &WatchVersionInfo) -> HashMap<String, OwnedValue> {
    let r = &info.running;
    let mut m: HashMap<String, OwnedValue> = HashMap::from([
        ("firmware_version".into(), dbus_val(r.string_version.clone())),
        ("firmware_major".into(), dbus_val(r.major)),
        ("firmware_minor".into(), dbus_val(r.minor)),
        ("firmware_patch".into(), dbus_val(r.patch)),
        ("firmware_suffix".into(), dbus_val(r.suffix.clone())),
        ("firmware_git_hash".into(), dbus_val(r.git_hash.clone())),
        ("is_recovery".into(), dbus_val(r.is_recovery)),
        ("bootloader_timestamp".into(), dbus_val(info.bootloader_timestamp)),
        ("board".into(), dbus_val(info.board.clone())),
        ("serial".into(), dbus_val(info.serial.clone())),
        ("bt_address".into(), dbus_val(info.bt_address.clone())),
        ("resource_crc".into(), dbus_val(info.resource_crc)),
        ("resource_timestamp".into(), dbus_val(info.resource_timestamp)),
        ("language".into(), dbus_val(info.language.clone())),
        ("language_version".into(), dbus_val(info.language_version)),
        ("hardware_platform".into(), dbus_val(info.hardware_platform)),
        ("platform_revision".into(), dbus_val(info.platform_revision())),
        ("watch_type".into(), dbus_val(info.watch_type().codename())),
        ("capabilities".into(), dbus_val(info.capabilities)),
        ("is_unfaithful".into(), dbus_val(info.is_unfaithful)),
    ]);
    if let Some(recovery) = &info.recovery {
        m.insert("recovery_version".into(), dbus_val(recovery.string_version.clone()));
    }
    if let Some(v) = info.health_insights_version {
        m.insert("health_insights_version".into(), dbus_val(v));
    }
    if let Some(v) = info.javascript_version {
        m.insert("javascript_version".into(), dbus_val(v));
    }
    m
}

/// Encode RGBA8888 pixels as a PNG.
fn encode_png(width: u32, height: u32, rgba: &[u8]) -> Result<Vec<u8>, DaemonError> {
    let mut out = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut out, width, height);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder
            .write_header()
            .map_err(|e| DaemonError::Failed(format!("png header: {e}")))?;
        writer
            .write_image_data(rgba)
            .map_err(|e| DaemonError::Failed(format!("png data: {e}")))?;
    }
    Ok(out)
}

/// Render watch color info as a self-describing `a{sv}` map.
fn watch_color_to_map(c: &WatchColorInfo) -> HashMap<String, OwnedValue> {
    HashMap::from([
        ("protocol_number".into(), dbus_val(c.protocol_number)),
        ("js_name".into(), dbus_val(c.js_name)),
        ("description".into(), dbus_val(c.description)),
        ("watch_type".into(), dbus_val(c.watch_type.codename())),
        ("supports_hrm".into(), dbus_val(c.supports_hrm)),
    ])
}

// ---------------------------------------------------------------------------
// CobbleDaemon
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct CobbleDaemon {
    state: Arc<Mutex<DaemonState>>,
    /// Bumped by reload_config so the supervisor can wait event-driven
    /// when no address is configured.
    config_revision: watch::Sender<u64>,
    /// Notifies subscribers when the watch connects or disconnects.
    connection_tx: watch::Sender<bool>,
    /// Forwards watch music-control actions to the MPRIS monitor.
    music_action_tx: mpsc::UnboundedSender<String>,
    /// Forwards watch phone actions to the call monitor.
    phone_action_tx: mpsc::UnboundedSender<(String, u32)>,
}

impl CobbleDaemon {

    pub fn new(
        address: String,
        adapter: String,
        config_path: PathBuf,
        event_tx: mpsc::UnboundedSender<DaemonEvent>,
        db: Option<Arc<Mutex<AppDb>>>,
        music_action_tx: mpsc::UnboundedSender<String>,
        phone_action_tx: mpsc::UnboundedSender<(String, u32)>,
    ) -> Self {
        let (config_revision, _) = watch::channel(0);
        let (connection_tx, _) = watch::channel(false);
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
                health_profile: None,
                hrm_prefs: None,
                heart_rate_prefs: None,
                imperial_units: None,
                watch_settings: HashMap::new(),
                battery_level: None,
                music: MusicState::default(),
            })),
            config_revision,
            music_action_tx,
            phone_action_tx,
            connection_tx,
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

    /// Returns a receiver that fires when [`reload_config`] is called.
    /// Used by the supervisor to wait event-driven when no address is set.
    pub fn config_changed(&self) -> watch::Receiver<u64> {
        self.config_revision.subscribe()
    }

    /// Returns a receiver that fires when the watch connects or disconnects.
    pub fn watch_connection(&self) -> watch::Receiver<bool> {
        self.connection_tx.subscribe()
    }

    /// Returns the shared app database handle, if available.
    pub fn db(&self) -> Option<Arc<Mutex<AppDb>>> {
        self.state.lock().unwrap().db.clone()
    }

    /// Returns a clone of the music-action sender; used by the signal
    /// emitter to forward watch control actions to the MPRIS monitor.
    pub(crate) fn music_action_tx(&self) -> mpsc::UnboundedSender<String> {
        self.music_action_tx.clone()
    }

    /// Returns a clone of the phone-action sender.
    pub(crate) fn phone_action_tx(&self) -> mpsc::UnboundedSender<(String, u32)> {
        self.phone_action_tx.clone()
    }

    pub(crate) fn require_pebble(&self) -> Result<Arc<Pebble>, DaemonError> {
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
        self.connection_tx.send_replace(true);
    }

    /// Cache the demographic health profile synced from the watch and return the
    /// merged snapshot (profile + last-known HRM flag) for signal emission.
    pub(crate) fn cache_health_profile(&self, prefs: ActivityPreferences) -> HealthProfile {
        let mut s = self.state.lock().unwrap();
        s.health_profile = Some(prefs);
        Self::merged_profile(&s).expect("profile just set")
    }

    /// Cache the HRM record. Returns the merged snapshot only if the demographic
    /// profile is already known (otherwise there is nothing useful to signal yet).
    pub(crate) fn cache_hrm(&self, hrm: HrmPreferences) -> Option<HealthProfile> {
        let mut s = self.state.lock().unwrap();
        s.hrm_prefs = Some(hrm);
        Self::merged_profile(&s)
    }

    /// Cache the heart-rate record. Returns the merged snapshot only if the
    /// demographic profile is already known.
    pub(crate) fn cache_heart_rate(&self, hr: HeartRatePreferences) -> Option<HealthProfile> {
        let mut s = self.state.lock().unwrap();
        s.heart_rate_prefs = Some(hr);
        Self::merged_profile(&s)
    }

    /// Cache the distance-units flag (true = imperial). Returns the merged
    /// snapshot only if the demographic profile is already known.
    pub(crate) fn cache_units(&self, imperial: bool) -> Option<HealthProfile> {
        let mut s = self.state.lock().unwrap();
        s.imperial_units = Some(imperial);
        Self::merged_profile(&s)
    }

    /// Cache a decoded general watch setting (db 12).
    pub(crate) fn cache_watch_setting(&self, key: String, value: WatchPrefValue) {
        self.state.lock().unwrap().watch_settings.insert(key, value);
    }

    /// Re-send the last pushed music state to the watch — used to answer the
    /// watch's GetCurrentTrack request (e.g. when its music app opens).
    pub(crate) async fn replay_music_state(&self) {
        let (pebble, music) = {
            let s = self.state.lock().unwrap();
            (s.pebble.clone(), s.music.clone())
        };
        let Some(pebble) = pebble else { return };
        debug!(
            "replaying music to watch: player={} track={} state={}",
            music.player.is_some(),
            music.track.is_some(),
            music.play_state.is_some(),
        );
        if let Some((pkg, name)) = music.player {
            let _ = pebble.update_music_player_info(&pkg, &name).await;
        }
        if let Some((artist, album, title, len, count, num)) = music.track {
            let _ = pebble
                .update_music_track(&artist, &album, &title, Some(len), Some(count), Some(num))
                .await;
        }
        if let Some((state, pos, rate, shuffle, repeat)) = music.play_state {
            let _ = pebble
                .update_music_play_state(
                    MusicPlaybackState::from_u8(state),
                    pos,
                    rate,
                    MusicShuffle::from_u8(shuffle),
                    MusicRepeat::from_u8(repeat),
                )
                .await;
        }
        if let Some(volume) = music.volume {
            let _ = pebble.update_music_volume(volume).await;
        }
    }

    /// Cache a battery level, but only while connected and only if it changed.
    /// Returns true when the cache was updated (caller should emit a signal).
    /// Dropping events while disconnected preserves the "-1 = unknown" contract
    /// against late notifications from a torn-down session.
    pub(crate) fn set_battery_level(&self, level: u8) -> bool {
        let mut state = self.state.lock().unwrap();
        if !state.connected || state.battery_level == Some(level) {
            return false;
        }
        state.battery_level = Some(level);
        true
    }

    /// The battery level held by the live watch session, if any.
    pub(crate) fn session_battery_level(&self) -> Option<u8> {
        let pebble = self.state.lock().unwrap().pebble.clone();
        pebble.and_then(|p| p.battery_level())
    }

    fn merged_profile(s: &DaemonState) -> Option<HealthProfile> {
        let p = s.health_profile?;
        let hrm = s.hrm_prefs;
        let hr = s.heart_rate_prefs;
        Some(HealthProfile {
            height_cm: p.height_cm,
            weight_kg: p.weight_kg,
            age: p.age as u16,
            gender: p.gender as u16,
            tracking_enabled: p.tracking_enabled,
            activity_insights_enabled: p.activity_insights_enabled,
            sleep_insights_enabled: p.sleep_insights_enabled,
            hrm_enabled: hrm.map(|h| h.enabled).unwrap_or(false),
            hrm_measurement_interval: hrm
                .and_then(|h| h.measurement_interval)
                .map(|i| i.code())
                .unwrap_or(255),
            hrm_activity_tracking: hrm.and_then(|h| h.activity_tracking_enabled).unwrap_or(false),
            resting_hr: hr.map(|h| h.resting_hr as u16).unwrap_or(0),
            elevated_hr: hr.map(|h| h.elevated_hr as u16).unwrap_or(0),
            max_hr: hr.map(|h| h.max_hr as u16).unwrap_or(0),
            hr_zone1_threshold: hr.map(|h| h.zone1_threshold as u16).unwrap_or(0),
            hr_zone2_threshold: hr.map(|h| h.zone2_threshold as u16).unwrap_or(0),
            hr_zone3_threshold: hr.map(|h| h.zone3_threshold as u16).unwrap_or(0),
            imperial_units: s.imperial_units.unwrap_or(false),
        })
    }

    /// Called by the supervisor when the watch disconnects.
    pub fn set_disconnected(&self) {
        let mut state = self.state.lock().unwrap();
        state.connected = false;
        state.pebble = None;
        // Drop watch-scoped session state so a different watch reconnecting
        // doesn't serve the previous watch's stale profile/settings until it
        // re-syncs. The cache_* handlers rebuild these from the new session.
        state.health_profile = None;
        state.hrm_prefs = None;
        state.heart_rate_prefs = None;
        state.imperial_units = None;
        state.watch_settings.clear();
        state.battery_level = None;
        state.music = MusicState::default();
        let _ = state.event_tx.send(DaemonEvent::ConnectionChanged(false));
        self.connection_tx.send_replace(false);
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

#[allow(clippy::too_many_arguments)]
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

    /// Watch battery percentage (0–100), or -1 if unknown/disconnected.
    #[zbus(property)]
    fn battery_level(&self) -> i16 {
        self.state.lock().unwrap().battery_level.map(i16::from).unwrap_or(-1)
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
    /// gender: 0 = female, 1 = male, 2 = other (libpebble3 `HealthGender`).
    async fn activate_health(
        &self,
        height_cm: u16,
        weight_kg: u16,
        age: u8,
        gender: u8,
        hrm_enabled: bool,
    ) -> Result<(), DaemonError> {
        if gender > 2 {
            return Err(DaemonError::Failed(format!(
                "invalid gender={gender}; must be 0 (female), 1 (male), or 2 (other)"
            )));
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

    /// PROTOTYPE: ask the watch to re-sync its HealthParams BlobDB (height,
    /// weight, age, gender, HRM). Decoded records are logged by the daemon; this
    /// call only triggers the request and returns once it has been sent.
    async fn fetch_health_params(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .fetch_health_params()
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Return the last health profile (height/weight/age/gender/HRM) the watch
    /// synced. Fails if no profile has been received yet this session — call
    /// `FetchHealthParams` (or wait for the on-connect sync) first.
    fn get_health_profile(&self) -> Result<HashMap<String, OwnedValue>, DaemonError> {
        Self::merged_profile(&self.state.lock().unwrap())
            .map(HealthProfile::to_dbus_map)
            .ok_or_else(|| DaemonError::Failed("no health profile synced yet".into()))
    }

    /// Return all general watch settings (BlobDB WatchPrefs, db 12) decoded so
    /// far, as a map of key -> variant (bool / uint32 / string). Empty until the
    /// watch syncs settings on connect. See `WatchSettingReceived` for updates.
    fn get_watch_settings(&self) -> HashMap<String, OwnedValue> {
        self.state
            .lock()
            .unwrap()
            .watch_settings
            .iter()
            .map(|(k, v)| (k.clone(), watch_pref_owned_value(v)))
            .collect()
    }

    /// Query the watch's version info (firmware, board, serial, BT address,
    /// language, capabilities, platform) as a key -> variant map.
    async fn get_watch_version(&self) -> Result<HashMap<String, OwnedValue>, DaemonError> {
        let pebble = self.require_pebble()?;
        let info = pebble
            .get_watch_version()
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        Ok(watch_version_to_map(&info))
    }

    /// Query the watch's manufacturing color/variant as a key -> variant map.
    /// Fails if the watch reports an error or an unknown color.
    async fn get_watch_color(&self) -> Result<HashMap<String, OwnedValue>, DaemonError> {
        let pebble = self.require_pebble()?;
        match pebble
            .get_watch_color()
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?
        {
            Some(color) => Ok(watch_color_to_map(color)),
            None => Err(DaemonError::Failed("watch reported an unknown color".into())),
        }
    }

    /// Capture the watch screen and return it as PNG bytes.
    async fn screenshot(&self) -> Result<Vec<u8>, DaemonError> {
        let pebble = self.require_pebble()?;
        let shot = pebble
            .take_screenshot()
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        encode_png(shot.width, shot.height, &shot.pixels)
    }

    /// Tell the watch which media app is playing (now-playing source).
    pub(crate) async fn set_music_player_info(&self, pkg: String, name: String) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .update_music_player_info(&pkg, &name)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        self.state.lock().unwrap().music.player = Some((pkg, name));
        Ok(())
    }

    /// Push the current track metadata. `track_length_ms`/`track_count`/
    /// `track_number` are sent as-is (0 is a valid "unknown" value).
    pub(crate) async fn set_music_track(
        &self,
        artist: String,
        album: String,
        title: String,
        track_length_ms: u32,
        track_count: u32,
        track_number: u32,
    ) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .update_music_track(
                &artist,
                &album,
                &title,
                Some(track_length_ms),
                Some(track_count),
                Some(track_number),
            )
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        self.state.lock().unwrap().music.track =
            Some((artist, album, title, track_length_ms, track_count, track_number));
        Ok(())
    }

    /// Push playback state. `state`: 0=paused 1=playing 2=rewinding
    /// 3=fast-forwarding 4=unknown. `shuffle`: 0=unknown 1=off 2=on.
    /// `repeat`: 0=unknown 1=off 2=one 3=all.
    pub(crate) async fn set_music_playback_state(
        &self,
        state: u8,
        track_position_ms: u32,
        play_rate_pct: u32,
        shuffle: u8,
        repeat: u8,
    ) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .update_music_play_state(
                MusicPlaybackState::from_u8(state),
                track_position_ms,
                play_rate_pct,
                MusicShuffle::from_u8(shuffle),
                MusicRepeat::from_u8(repeat),
            )
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        self.state.lock().unwrap().music.play_state =
            Some((state, track_position_ms, play_rate_pct, shuffle, repeat));
        Ok(())
    }

    /// Push the current volume (0–100).
    pub(crate) async fn set_music_volume(&self, volume_percent: u8) -> Result<(), DaemonError> {
        if volume_percent > 100 {
            return Err(DaemonError::Failed(format!(
                "volume {volume_percent} out of range (0-100)"
            )));
        }
        let pebble = self.require_pebble()?;
        pebble
            .update_music_volume(volume_percent)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        self.state.lock().unwrap().music.volume = Some(volume_percent);
        Ok(())
    }

    /// Reboot the watch. It drops the link and the daemon reconnects.
    async fn reboot_watch(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.reboot_watch().map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Reboot the watch into its recovery (PRF) firmware.
    async fn reset_into_recovery(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.reset_into_recovery().map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Trigger a core dump on the watch.
    async fn create_core_dump(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.create_core_dump().map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Factory-reset the watch. DESTRUCTIVE: wipes all watch data and unpairs.
    /// Requires `confirm = true` so an accidental/no-arg call can't wipe the watch.
    async fn factory_reset(&self, confirm: bool) -> Result<(), DaemonError> {
        if !confirm {
            return Err(DaemonError::Failed(
                "factory_reset is destructive (wipes the watch and unpairs it); \
                 call with confirm=true to proceed"
                    .into(),
            ));
        }
        let pebble = self.require_pebble()?;
        pebble.factory_reset().map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Remove the watch's Bluetooth bond (unpair). The watch re-pairs on the
    /// next reconnect.
    async fn forget(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.forget().await.map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Push weather data to the Pebble built-in weather app.
    ///
    /// `location_key` must be exactly 16 bytes (a UUID); re-use the same bytes to update
    /// an existing location entry rather than creating a new one.
    ///
    /// `current_weather` / `tomorrow_weather`: 0=PartlyCloudy, 1=CloudyDay, 2=LightSnow,
    ///   3=LightRain, 4=HeavyRain, 5=HeavySnow, 6=Generic, 7=Sun, 8=RainAndSnow, 255=Unknown
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn push_weather(
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

    // ── Phone calls ───────────────────────────────────────────────────

    /// Push an incoming call to the watch (shows caller screen).
    /// `cookie` is an arbitrary u32 echoed back in answer/hangup actions.
    pub(crate) async fn push_incoming_call(
        &self,
        cookie: u32,
        caller_number: String,
        caller_name: String,
    ) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .push_incoming_call(cookie, &caller_number, &caller_name)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Push a missed call notification to the watch.
    async fn push_missed_call(
        &self,
        cookie: u32,
        caller_number: String,
        caller_name: String,
    ) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .push_missed_call(cookie, &caller_number, &caller_name)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Notify the watch that the call is now active (answered).
    pub(crate) async fn push_call_start(&self, cookie: u32) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .push_call_start(cookie)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Notify the watch that the call has ended.
    pub(crate) async fn push_call_end(&self, cookie: u32) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .push_call_end(cookie)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Rebuild health_activity_minutes and health_activity_sessions from the raw
    /// blobs in health_records. Call this after a schema change or to backfill
    /// utc_offset for rows that were stored before the column existed.
    async fn reprocess_health_data(&self) -> Result<(), DaemonError> {
        let db = self.state.lock().unwrap().db.clone();
        let db = db.ok_or_else(|| DaemonError::Failed("app database not available".into()))?;
        tokio::task::spawn_blocking(move || db.lock().unwrap().reprocess())
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Re-read the config file from disk and apply changes.
    /// If address or adapter changed, disconnects the current session so the
    /// supervisor reconnects with the new parameters on the next cycle.
    pub(crate) async fn reload_config(&self) -> Result<(), DaemonError> {
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

        // Bump the revision so any waiting supervisor wakes up.
        self.config_revision.send_modify(|r| *r += 1);

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

    /// Emitted when the watch syncs its health profile (height/weight/age/gender/HRM).
    /// Fires on connect and on any subsequent change, including HRM updates.
    #[zbus(signal)]
    pub async fn health_profile_received(
        signal_emitter: &SignalEmitter<'_>,
        profile: HashMap<String, OwnedValue>,
    ) -> zbus::Result<()>;

    /// Emitted for each general watch setting (db 12) as the watch syncs it.
    /// `value` is a variant: bool, uint32, or string depending on the key.
    #[zbus(signal)]
    pub async fn watch_setting_received(
        signal_emitter: &SignalEmitter<'_>,
        key: &str,
        value: OwnedValue,
    ) -> zbus::Result<()>;

    /// Emitted when the watch battery percentage changes. -1 means unknown.
    #[zbus(signal)]
    pub async fn battery_changed(
        signal_emitter: &SignalEmitter<'_>,
        level: i16,
    ) -> zbus::Result<()>;

    /// Emitted when an app opens (running=true) or closes (running=false) on the watch.
    #[zbus(signal)]
    pub async fn app_run_state_changed(
        signal_emitter: &SignalEmitter<'_>,
        uuid: &str,
        running: bool,
    ) -> zbus::Result<()>;

    /// Emitted when the watch sends a media-control action. `action` is one of
    /// play, pause, play_pause, next_track, previous_track, volume_up,
    /// volume_down, get_current_track. The transport actions (play/pause/next/…)
    /// are surfaced but not acted on yet; `get_current_track` is handled by
    /// replaying the cached music state to the watch.
    #[zbus(signal)]
    pub async fn music_action_received(
        signal_emitter: &SignalEmitter<'_>,
        action: &str,
    ) -> zbus::Result<()>;

    /// Emitted when the watch sends a phone control action.
    /// `action` is "answer" or "hangup". `cookie` is the u32 that was sent
    /// with the original incoming/missed call so the client can match it.
    #[zbus(signal)]
    pub async fn phone_action_received(
        signal_emitter: &SignalEmitter<'_>,
        action: &str,
        cookie: u32,
    ) -> zbus::Result<()>;
}

// ---------------------------------------------------------------------------
// Signal emission task
// ---------------------------------------------------------------------------

/// Processes `DaemonEvent`s from the reconnect supervisor and emits the
/// corresponding D-Bus signals. Keeps the `Connected` property in sync.
pub async fn run_signal_emitter(
    conn: Connection,
    daemon: CobbleDaemon,
    mut event_rx: mpsc::UnboundedReceiver<DaemonEvent>,
    app_db: Option<Arc<Mutex<AppDb>>>,
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
                if c {
                    // The connect-time battery read can be queued before this
                    // event and dropped by the disconnected gate; deliver it now.
                    if let Some(level) = daemon.session_battery_level() {
                        if daemon.set_battery_level(level) {
                            let _ =
                                iface.get().await.battery_level_changed(iface.signal_emitter()).await;
                            let _ = CobbleDaemon::battery_changed(emitter, i16::from(level)).await;
                        }
                    }
                } else {
                    // Battery is unknown while disconnected (state was cleared).
                    let _ = iface.get().await.battery_level_changed(iface.signal_emitter()).await;
                    let _ = CobbleDaemon::battery_changed(emitter, -1).await;
                }
            }
            DaemonEvent::BatteryChanged(level) => {
                // Gated on connected so a late event after disconnect can't
                // resurrect a stale level past the -1 contract.
                if daemon.set_battery_level(level) {
                    let _ = iface.get().await.battery_level_changed(iface.signal_emitter()).await;
                    let _ = CobbleDaemon::battery_changed(emitter, i16::from(level)).await;
                }
            }
            DaemonEvent::AppRunState { uuid, running } => {
                let _ = CobbleDaemon::app_run_state_changed(emitter, &uuid, running).await;
                // This firmware doesn't send GetCurrentTrack, but it does launch
                // the Music app — replay the cached now-playing so it displays.
                if running && uuid == MUSIC_APP_UUID {
                    daemon.replay_music_state().await;
                }
            }
            DaemonEvent::MusicAction(action) => {
                let _ = CobbleDaemon::music_action_received(emitter, &action).await;
                // The watch asks for the now-playing when its music app opens;
                // replay the last pushed state so it actually displays something.
                if action == "get_current_track" {
                    daemon.replay_music_state().await;
                }
                // Forward the action to the MPRIS monitor so it can control
                // the desktop media player (play/pause/next/volume/…).
                let _ = daemon.music_action_tx().send(action);
            }
            DaemonEvent::PhoneAction(action) => {
                let (name, cookie) = match action {
                    libpebble_ble::PhoneAction::Answer { cookie } => ("answer", cookie),
                    libpebble_ble::PhoneAction::Hangup { cookie } => ("hangup", cookie),
                };
                let _ = CobbleDaemon::phone_action_received(emitter, name, cookie).await;
                // Forward to the call monitor — it will call push_call_start
                // only after the modem confirms the answer.
                let _ = daemon.phone_action_tx().send((name.to_string(), cookie));
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
                if let Some(db) = &app_db {
                    let db = db.clone();
                    let batch_for_db = batch.clone();
                    match tokio::task::spawn_blocking(move || {
                        db.lock().unwrap().insert_batch(&batch_for_db)
                    })
                    .await
                    {
                        Ok(Err(e)) => warn!("app DB insert failed: {e}"),
                        Err(e) => warn!("app DB task panicked: {e}"),
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
            DaemonEvent::HealthProfile(prefs) => {
                let profile = daemon.cache_health_profile(prefs);
                let _ = CobbleDaemon::health_profile_received(emitter, profile.to_dbus_map()).await;
            }
            DaemonEvent::HealthHrm(hrm) => {
                if let Some(profile) = daemon.cache_hrm(hrm) {
                    let _ = CobbleDaemon::health_profile_received(emitter, profile.to_dbus_map()).await;
                }
            }
            DaemonEvent::HealthHeartRate(hr) => {
                if let Some(profile) = daemon.cache_heart_rate(hr) {
                    let _ = CobbleDaemon::health_profile_received(emitter, profile.to_dbus_map()).await;
                }
            }
            DaemonEvent::HealthUnits(imperial) => {
                if let Some(profile) = daemon.cache_units(imperial) {
                    let _ = CobbleDaemon::health_profile_received(emitter, profile.to_dbus_map()).await;
                }
            }
            DaemonEvent::WatchSetting { key, value } => {
                let variant = watch_pref_owned_value(&value);
                daemon.cache_watch_setting(key.clone(), value);
                let _ = CobbleDaemon::watch_setting_received(emitter, &key, variant).await;
            }
        }
    }
}

//! Rust D-Bus client for the cobbled Pebble BLE daemon.
//!
//! Mirrors the `org.cobble.Daemon` interface 1:1 (see `cobbled/src/service.rs`).
//!
//! # Quick start
//!
//! ```no_run
//! # async fn example() -> cobble_client::Result<()> {
//! use cobble_client::CobbleClient;
//!
//! let client = CobbleClient::new().await?;
//! if client.is_running().await {
//!     client.reload_config().await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! For signal subscriptions, obtain a [`CobbleDaemonProxy`] via
//! [`CobbleClient::proxy`] and call the generated `receive_*()` methods.

use std::collections::HashMap;

use zbus::{proxy, Connection};
pub use zbus::{Error, Result};
pub use zvariant::OwnedValue;

/// AppMessage wire type matching the D-Bus signature `a{i(sv)}`.
///
/// Each entry maps an integer key to a `(tag, value)` pair where `tag` is one
/// of `"u8"`, `"u16"`, `"u32"`, `"i8"`, `"i16"`, `"i32"`, `"str"`, `"bytes"`.
pub type WireDict = HashMap<i32, (String, OwnedValue)>;

/// Self-describing `a{sv}` map (watch version/color/health-profile/settings).
pub type VarDict = HashMap<String, OwnedValue>;

/// Extract a string field from an `a{sv}` map, or `""` if absent/not a string.
fn var_str(map: &VarDict, key: &str) -> String {
    map.get(key)
        .and_then(|v| <&str>::try_from(v).ok())
        .unwrap_or_default()
        .to_string()
}

/// Watch identity snapshot for display (subset of `GetWatchVersion`/`GetWatchColor`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchInfo {
    pub firmware_version: String,
    pub recovery_version: String,
    /// Watch model codename (from `watch_type`).
    pub model: String,
    pub board: String,
    pub serial: String,
    pub bt_address: String,
    pub language: String,
    /// Human-readable color/variant description.
    pub color: String,
}

/// A daemon/watch status change delivered to [`CobbleClient::watch_status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusEvent {
    /// The daemon (bus-name owner) appeared (`true`) or vanished (`false`).
    DaemonRunning(bool),
    /// The watch BLE link came up (`true`) or went down (`false`).
    Connected(bool),
    /// Battery percentage 0–100, or `-1` if unknown.
    Battery(i16),
    /// Fresh watch identity info (emitted after the link comes up).
    WatchInfo(WatchInfo),
}

/// Typed zbus proxy for `org.cobble.Daemon`.
///
/// All methods mirror the daemon's D-Bus interface exactly.  For one-shot
/// calls prefer the higher-level [`CobbleClient`] methods.  Use this proxy
/// directly when you need to subscribe to signals via the generated
/// `receive_<signal_name>()` methods.
#[proxy(
    interface = "org.cobble.Daemon",
    default_service = "org.cobble.Daemon",
    default_path = "/org/cobble/Daemon"
)]
pub trait CobbleDaemon {
    // ---- Properties ----

    /// `true` when the BLE link to the watch is up.
    #[zbus(property)]
    fn connected(&self) -> Result<bool>;

    /// Configured watch Bluetooth address.
    #[zbus(property)]
    fn watch_address(&self) -> Result<String>;

    /// Watch battery percentage (0–100), or `-1` if unknown/disconnected.
    #[zbus(property)]
    fn battery_level(&self) -> Result<i16>;

    // ---- Methods ----

    async fn send_app_message(
        &self,
        app_uuid: &str,
        data: WireDict,
        wait_ack: bool,
    ) -> Result<u32>;

    async fn launch_app(&self, app_uuid: &str) -> Result<()>;
    async fn stop_app(&self, app_uuid: &str) -> Result<()>;
    async fn update_time(&self) -> Result<()>;
    async fn notify(&self, title: &str, body: &str, subtitle: &str) -> Result<u32>;
    async fn ping(&self) -> Result<bool>;
    async fn scan(&self, timeout_secs: f64) -> Result<Vec<(String, String)>>;

    // ---- Health ----

    async fn activate_health(
        &self,
        height_cm: u16,
        weight_kg: u16,
        age: u8,
        gender: u8,
        hrm_enabled: bool,
    ) -> Result<()>;

    async fn fetch_health_data(&self) -> Result<()>;
    async fn fetch_health_params(&self) -> Result<()>;
    async fn get_health_profile(&self) -> Result<VarDict>;
    async fn reprocess_health_data(&self) -> Result<()>;

    // ---- Watch info / settings ----

    async fn get_watch_settings(&self) -> Result<VarDict>;
    async fn get_watch_version(&self) -> Result<VarDict>;
    async fn get_watch_color(&self) -> Result<VarDict>;

    /// Capture the watch screen, returned as PNG bytes.
    async fn screenshot(&self) -> Result<Vec<u8>>;

    // ---- Music (push now-playing to the watch) ----

    async fn set_music_player_info(&self, pkg: &str, name: &str) -> Result<()>;

    async fn set_music_track(
        &self,
        artist: &str,
        album: &str,
        title: &str,
        track_length_ms: u32,
        track_count: u32,
        track_number: u32,
    ) -> Result<()>;

    /// `state`: 0=paused 1=playing 2=rewinding 3=fast-forwarding 4=unknown.
    /// `shuffle`: 0=unknown 1=off 2=on. `repeat`: 0=unknown 1=off 2=one 3=all.
    async fn set_music_playback_state(
        &self,
        state: u8,
        track_position_ms: u32,
        play_rate_pct: u32,
        shuffle: u8,
        repeat: u8,
    ) -> Result<()>;

    /// `volume_percent` is 0–100.
    async fn set_music_volume(&self, volume_percent: u8) -> Result<()>;

    // ---- Device management ----

    async fn reboot_watch(&self) -> Result<()>;
    async fn reset_into_recovery(&self) -> Result<()>;
    async fn create_core_dump(&self) -> Result<()>;
    /// DESTRUCTIVE — wipes the watch; requires `confirm = true`.
    async fn factory_reset(&self, confirm: bool) -> Result<()>;
    /// Remove the Bluetooth bond (unpair); re-pairs on next reconnect.
    async fn forget(&self) -> Result<()>;

    // ---- Weather ----

    #[allow(clippy::too_many_arguments)]
    async fn push_weather(
        &self,
        location_key: Vec<u8>,
        location_name: &str,
        forecast_short: &str,
        current_temp: i16,
        current_weather: u8,
        today_high: i16,
        today_low: i16,
        tomorrow_weather: u8,
        tomorrow_high: i16,
        tomorrow_low: i16,
        is_current_location: bool,
    ) -> Result<()>;

    // ---- Daemon control ----

    async fn reload_config(&self) -> Result<()>;

    // ---- Signals ----

    #[zbus(signal)]
    fn app_message_received(&self, app_uuid: &str, data: WireDict) -> Result<()>;

    #[zbus(signal)]
    fn ack_received(&self, txn: u32) -> Result<()>;

    #[zbus(signal)]
    fn nack_received(&self, txn: u32) -> Result<()>;

    #[zbus(signal)]
    fn connection_changed(&self, connected: bool) -> Result<()>;

    #[zbus(signal)]
    #[allow(clippy::too_many_arguments)]
    fn health_data_received(
        &self,
        tag: u32,
        app_uuid: Vec<u8>,
        session_timestamp: u32,
        items_left: u32,
        crc: u32,
        item_type: u8,
        item_size: u16,
        data: Vec<u8>,
    ) -> Result<()>;

    #[zbus(signal)]
    fn health_profile_received(&self, profile: VarDict) -> Result<()>;

    #[zbus(signal)]
    fn watch_setting_received(&self, key: &str, value: OwnedValue) -> Result<()>;

    #[zbus(signal)]
    fn battery_changed(&self, level: i16) -> Result<()>;

    #[zbus(signal)]
    fn app_run_state_changed(&self, uuid: &str, running: bool) -> Result<()>;

    #[zbus(signal)]
    fn music_action_received(&self, action: &str) -> Result<()>;
}

const BUS_NAME: &str = "org.cobble.Daemon";

/// High-level client for the cobbled daemon.
///
/// Wraps a session D-Bus connection and exposes all daemon methods directly.
/// The underlying [`Connection`] is cheap to clone — pass clones into async
/// tasks rather than creating a new [`CobbleClient`] per call.
#[derive(Clone)]
pub struct CobbleClient {
    conn: Connection,
}

impl CobbleClient {
    /// Connect to the session bus.  Does **not** check whether the daemon is
    /// running; use [`is_running`](Self::is_running) for that.
    pub async fn new() -> Result<Self> {
        let conn = Connection::session().await?;
        Ok(Self { conn })
    }

    /// Returns `true` if the cobbled daemon currently owns its bus name.
    pub async fn is_running(&self) -> bool {
        self.conn
            .call_method(
                Some("org.freedesktop.DBus"),
                "/org/freedesktop/DBus",
                Some("org.freedesktop.DBus"),
                "NameHasOwner",
                &BUS_NAME,
            )
            .await
            .ok()
            .and_then(|reply| reply.body().deserialize::<bool>().ok())
            .unwrap_or(false)
    }

    /// Returns `true` if the daemon is running and the watch BLE link is up.
    pub async fn connected(&self) -> bool {
        let Ok(proxy) = self.proxy().await else { return false };
        proxy.connected().await.unwrap_or(false)
    }

    /// Build a typed proxy for signal subscriptions or less-common calls.
    /// The proxy borrows `self`'s connection; for owned/`'static` use cases
    /// clone the client and call `proxy()` on the clone.
    pub async fn proxy(&self) -> Result<CobbleDaemonProxy<'_>> {
        CobbleDaemonProxy::new(&self.conn).await
    }

    // ---- Properties ----

    pub async fn watch_address(&self) -> Result<String> {
        self.proxy().await?.watch_address().await
    }

    /// Watch battery percentage (0–100), or `-1` if unknown/disconnected.
    pub async fn battery_level(&self) -> Result<i16> {
        self.proxy().await?.battery_level().await
    }

    // ---- Apps / messaging ----

    pub async fn send_app_message(
        &self,
        app_uuid: &str,
        data: WireDict,
        wait_ack: bool,
    ) -> Result<u32> {
        self.proxy().await?.send_app_message(app_uuid, data, wait_ack).await
    }

    pub async fn launch_app(&self, app_uuid: &str) -> Result<()> {
        self.proxy().await?.launch_app(app_uuid).await
    }

    pub async fn stop_app(&self, app_uuid: &str) -> Result<()> {
        self.proxy().await?.stop_app(app_uuid).await
    }

    pub async fn update_time(&self) -> Result<()> {
        self.proxy().await?.update_time().await
    }

    pub async fn notify(&self, title: &str, body: &str, subtitle: &str) -> Result<u32> {
        self.proxy().await?.notify(title, body, subtitle).await
    }

    pub async fn ping(&self) -> Result<bool> {
        self.proxy().await?.ping().await
    }

    pub async fn scan(&self, timeout_secs: f64) -> Result<Vec<(String, String)>> {
        self.proxy().await?.scan(timeout_secs).await
    }

    // ---- Health ----

    pub async fn activate_health(
        &self,
        height_cm: u16,
        weight_kg: u16,
        age: u8,
        gender: u8,
        hrm_enabled: bool,
    ) -> Result<()> {
        self.proxy()
            .await?
            .activate_health(height_cm, weight_kg, age, gender, hrm_enabled)
            .await
    }

    pub async fn fetch_health_data(&self) -> Result<()> {
        self.proxy().await?.fetch_health_data().await
    }

    pub async fn fetch_health_params(&self) -> Result<()> {
        self.proxy().await?.fetch_health_params().await
    }

    /// Health profile keyed by field name (height_cm, weight_kg, age, gender, …).
    pub async fn get_health_profile(&self) -> Result<VarDict> {
        self.proxy().await?.get_health_profile().await
    }

    pub async fn reprocess_health_data(&self) -> Result<()> {
        self.proxy().await?.reprocess_health_data().await
    }

    // ---- Watch info / settings ----

    pub async fn get_watch_settings(&self) -> Result<VarDict> {
        self.proxy().await?.get_watch_settings().await
    }

    pub async fn get_watch_version(&self) -> Result<VarDict> {
        self.proxy().await?.get_watch_version().await
    }

    pub async fn get_watch_color(&self) -> Result<VarDict> {
        self.proxy().await?.get_watch_color().await
    }

    /// Capture the watch screen, returned as PNG bytes.
    pub async fn screenshot(&self) -> Result<Vec<u8>> {
        self.proxy().await?.screenshot().await
    }

    // ---- Music ----

    pub async fn set_music_player_info(&self, pkg: &str, name: &str) -> Result<()> {
        self.proxy().await?.set_music_player_info(pkg, name).await
    }

    pub async fn set_music_track(
        &self,
        artist: &str,
        album: &str,
        title: &str,
        track_length_ms: u32,
        track_count: u32,
        track_number: u32,
    ) -> Result<()> {
        self.proxy()
            .await?
            .set_music_track(artist, album, title, track_length_ms, track_count, track_number)
            .await
    }

    pub async fn set_music_playback_state(
        &self,
        state: u8,
        track_position_ms: u32,
        play_rate_pct: u32,
        shuffle: u8,
        repeat: u8,
    ) -> Result<()> {
        self.proxy()
            .await?
            .set_music_playback_state(state, track_position_ms, play_rate_pct, shuffle, repeat)
            .await
    }

    pub async fn set_music_volume(&self, volume_percent: u8) -> Result<()> {
        self.proxy().await?.set_music_volume(volume_percent).await
    }

    // ---- Device management ----

    pub async fn reboot_watch(&self) -> Result<()> {
        self.proxy().await?.reboot_watch().await
    }

    pub async fn reset_into_recovery(&self) -> Result<()> {
        self.proxy().await?.reset_into_recovery().await
    }

    pub async fn create_core_dump(&self) -> Result<()> {
        self.proxy().await?.create_core_dump().await
    }

    /// DESTRUCTIVE — wipes the watch; requires `confirm = true`.
    pub async fn factory_reset(&self, confirm: bool) -> Result<()> {
        self.proxy().await?.factory_reset(confirm).await
    }

    /// Remove the Bluetooth bond (unpair); re-pairs on next reconnect.
    pub async fn forget(&self) -> Result<()> {
        self.proxy().await?.forget().await
    }

    // ---- Weather ----

    #[allow(clippy::too_many_arguments)]
    pub async fn push_weather(
        &self,
        location_key: Vec<u8>,
        location_name: &str,
        forecast_short: &str,
        current_temp: i16,
        current_weather: u8,
        today_high: i16,
        today_low: i16,
        tomorrow_weather: u8,
        tomorrow_high: i16,
        tomorrow_low: i16,
        is_current_location: bool,
    ) -> Result<()> {
        self.proxy()
            .await?
            .push_weather(
                location_key,
                location_name,
                forecast_short,
                current_temp,
                current_weather,
                today_high,
                today_low,
                tomorrow_weather,
                tomorrow_high,
                tomorrow_low,
                is_current_location,
            )
            .await
    }

    // ---- Daemon control ----

    pub async fn reload_config(&self) -> Result<()> {
        self.proxy().await?.reload_config().await
    }

    // ---- High-level watch status ----

    /// Fetch a watch identity snapshot (firmware/model/board/serial/BT/color).
    /// Only meaningful while the watch is connected.
    pub async fn get_watch_info(&self) -> Result<WatchInfo> {
        let proxy = self.proxy().await?;
        let v = proxy.get_watch_version().await?;
        let mut info = WatchInfo {
            firmware_version: var_str(&v, "firmware_version"),
            recovery_version: var_str(&v, "recovery_version"),
            model: var_str(&v, "watch_type"),
            board: var_str(&v, "board"),
            serial: var_str(&v, "serial"),
            bt_address: var_str(&v, "bt_address"),
            language: var_str(&v, "language"),
            color: String::new(),
        };
        // Color is a separate call; tolerate failure (unknown color / older fw).
        if let Ok(c) = proxy.get_watch_color().await {
            info.color = var_str(&c, "description");
        }
        Ok(info)
    }

    /// Watch daemon/watch status via D-Bus signals (no polling), invoking
    /// `on_event` for every change. Emits the current state up front, then runs
    /// until the bus connection drops. Survives daemon restarts (tracked via the
    /// bus-name owner). Watch info is fetched and emitted whenever the link comes
    /// up.
    pub async fn watch_status<F>(&self, mut on_event: F) -> Result<()>
    where
        F: FnMut(StatusEvent) + Send,
    {
        use futures_util::stream::{select_all, StreamExt};

        let proxy = self.proxy().await?;

        // Initial snapshot — streams only deliver *changes*.
        let running = self.is_running().await;
        on_event(StatusEvent::DaemonRunning(running));
        if running {
            let connected = proxy.connected().await.unwrap_or(false);
            on_event(StatusEvent::Connected(connected));
            on_event(StatusEvent::Battery(proxy.battery_level().await.unwrap_or(-1)));
            if connected {
                if let Ok(info) = self.get_watch_info().await {
                    on_event(StatusEvent::WatchInfo(info));
                }
            }
        }

        // Merge the daemon-owner, connection, and battery signals into one stream.
        let owner = proxy
            .inner()
            .receive_owner_changed()
            .await?
            .map(|o| StatusEvent::DaemonRunning(o.is_some()))
            .boxed();
        let conn = proxy
            .receive_connection_changed()
            .await?
            .filter_map(|s| async move { s.args().ok().map(|a| StatusEvent::Connected(a.connected)) })
            .boxed();
        let batt = proxy
            .receive_battery_changed()
            .await?
            .filter_map(|s| async move { s.args().ok().map(|a| StatusEvent::Battery(a.level)) })
            .boxed();
        let mut events = select_all([owner, conn, batt]);

        while let Some(ev) = events.next().await {
            on_event(ev.clone());
            match ev {
                // Daemon (re)appeared: re-read the live state streams can't replay.
                StatusEvent::DaemonRunning(true) => {
                    let connected = proxy.connected().await.unwrap_or(false);
                    on_event(StatusEvent::Connected(connected));
                    on_event(StatusEvent::Battery(proxy.battery_level().await.unwrap_or(-1)));
                    if connected {
                        if let Ok(info) = self.get_watch_info().await {
                            on_event(StatusEvent::WatchInfo(info));
                        }
                    }
                }
                StatusEvent::DaemonRunning(false) => {
                    on_event(StatusEvent::Connected(false));
                    on_event(StatusEvent::Battery(-1));
                }
                // Link came up: pull fresh watch identity.
                StatusEvent::Connected(true) => {
                    if let Ok(info) = self.get_watch_info().await {
                        on_event(StatusEvent::WatchInfo(info));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

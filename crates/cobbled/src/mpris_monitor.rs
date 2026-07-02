//! MPRIS media-player monitor.
//!
//! Discovers `org.mpris.MediaPlayer2.*` players on the session bus and:
//!  1. Pushes metadata/playback/volume changes to the watch (via the daemon).
//!  2. Forwards watch media-control actions (play/pause/next/…) to the active
//!     MPRIS player.
//!
//! Player selection: prefers a player whose `PlaybackStatus` is `Playing`;
//! falls back to the most-recently-seen player.  Switches immediately when
//! another player starts playing.

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::Mutex;
use tracing::{debug, info, trace, warn};
use zbus::{zvariant::OwnedValue, Connection, MessageStream};

use crate::service::CobbleDaemon;

#[derive(Debug, Clone)]
struct PlayerState {
    bus_name: String,
    identity: String,
    playing: bool,
}

pub struct MprisMonitor {
    daemon: CobbleDaemon,
    conn: Connection,
    active: Arc<Mutex<Option<PlayerState>>>,
}

impl MprisMonitor {
    pub async fn new(daemon: CobbleDaemon) -> anyhow::Result<Self> {
        let conn = Connection::session().await?;
        Ok(Self { daemon, conn, active: Arc::new(Mutex::new(None)) })
    }

    /// Start watching for MPRIS players.  Blocks forever — spawn in a
    /// background task.
    pub async fn run(self: Arc<Self>) {
        if let Ok(existing) = self.list_players().await {
            for name in existing {
                self.track_player(name).await;
            }
        }
        let rule = "type='signal',sender='org.freedesktop.DBus',interface='org.freedesktop.DBus',member='NameOwnerChanged'";
        if let Err(e) = add_match(&self.conn, rule).await {
            warn!("mpris: cannot add NameOwnerChanged match: {e}");
            return;
        }
        let mut stream = MessageStream::from(&self.conn);
        while let Some(msg) = stream.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => { debug!("mpris: stream error: {e}"); continue; }
            };
            let hdr = msg.header();
            if hdr.interface().map(|i| i.as_str()) != Some("org.freedesktop.DBus") { continue; }
            if hdr.member().map(|m| m.as_str()) != Some("NameOwnerChanged") { continue; }
            let body = msg.body();
            let Ok((name, old_owner, new_owner)) = body.deserialize::<(String, String, String)>() else { continue; };
            if !name.starts_with("org.mpris.MediaPlayer2.") { continue; }

            if !old_owner.is_empty() {
                debug!("mpris: player {name} disappeared");
                let mut active = self.active.lock().await;
                if active.as_ref().map(|a| a.bus_name.as_str()) == Some(name.as_str()) {
                    *active = None;
                    if let Ok(players) = self.list_players().await {
                        for p in players {
                            if p != name {
                                if let Some(s) = self.read_player_state(&p).await {
                                    *active = Some(s);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            if !new_owner.is_empty() {
                info!("mpris: player {name} appeared");
                self.track_player(name).await;
            }
        }
    }

    /// Forward a media-control action from the watch to the active MPRIS player.
    pub async fn handle_action(&self, action: &str) {
        trace!("mpris: handle_action {action}");

        // Volume controls the system sink — no active player needed.
        match action {
            "volume_up" => { adjust_system_volume(5).await; return; }
            "volume_down" => { adjust_system_volume(-5).await; return; }
            _ => {}
        }

        let player = { self.active.lock().await.clone() };
        let Some(player) = player else {
            debug!("mpris: no active player to handle action {action}");
            return;
        };
        let bus = &player.bus_name;
        let path = "/org/mpris/MediaPlayer2";
        let iface = "org.mpris.MediaPlayer2.Player";

        match action {
            "play" => call_method(&self.conn, bus, path, iface, "Play").await,
            "pause" => call_method(&self.conn, bus, path, iface, "Pause").await,
            "play_pause" => call_method(&self.conn, bus, path, iface, "PlayPause").await,
            "next_track" => call_method(&self.conn, bus, path, iface, "Next").await,
            "previous_track" => call_method(&self.conn, bus, path, iface, "Previous").await,
            "get_current_track" => {
                if let Some(state) = self.read_player_state(bus).await {
                    self.push_to_watch(&state).await;
                }
            }
            // volume_up/volume_down handled above.
            "volume_up" | "volume_down" => unreachable!(),
            other => debug!("mpris: unhandled action '{other}'"),
        }
    }

    // ------------------------------------------------------------------
    // D-Bus property helpers
    // ------------------------------------------------------------------

    async fn get_prop<T>(&self, bus: &str, iface: &str, prop: &str) -> Option<T>
    where
        T: TryFrom<OwnedValue, Error = zbus::zvariant::Error>,
    {
        let reply = self
            .conn
            .call_method(Some(bus), "/org/mpris/MediaPlayer2", Some("org.freedesktop.DBus.Properties"), "Get", &(iface, prop))
            .await
            .ok()?;
        let body = reply.body();
        let v: zbus::zvariant::Value<'_> = body.deserialize().ok()?;
        OwnedValue::try_from(v).ok().and_then(|ov| T::try_from(ov).ok())
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    async fn list_players(&self) -> zbus::Result<Vec<String>> {
        let reply = self
            .conn
            .call_method(Some("org.freedesktop.DBus"), "/org/freedesktop/DBus", Some("org.freedesktop.DBus"), "ListNames", &())
            .await?;
        let names: Vec<String> = reply.body().deserialize()?;
        Ok(names.into_iter().filter(|n| n.starts_with("org.mpris.MediaPlayer2.")).collect())
    }

    async fn read_identity(&self, bus_name: &str) -> Option<String> {
        self.get_prop::<String>(bus_name, "org.mpris.MediaPlayer2", "Identity").await
    }

    async fn read_player_state(&self, bus_name: &str) -> Option<PlayerState> {
        let playing = self
            .get_prop::<String>(bus_name, "org.mpris.MediaPlayer2.Player", "PlaybackStatus")
            .await
            .map(|s| s == "Playing")
            .unwrap_or(false);
        let identity = self.read_identity(bus_name).await.unwrap_or_default();
        Some(PlayerState { bus_name: bus_name.to_string(), identity, playing })
    }

    async fn track_player(self: &Arc<Self>, bus_name: String) {
        let state = match self.read_player_state(&bus_name).await {
            Some(s) => s,
            None => return,
        };
        let is_active = {
            let mut active = self.active.lock().await;
            match active.as_ref() {
                Some(current) if state.playing && !current.playing => {
                    *active = Some(state.clone());
                    true
                }
                None => {
                    *active = Some(state.clone());
                    true
                }
                _ => active.as_ref().map(|a| a.bus_name.as_str()) == Some(bus_name.as_str()),
            }
        };
        if is_active {
            self.push_to_watch(&state).await;
        }

        let self2 = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = self2.listen_properties_changed(&bus_name).await {
                warn!("mpris: properties listener for {bus_name} ended: {e}");
            }
        });
    }

    async fn push_to_watch(&self, state: &PlayerState) {
        // Push player info best-effort; don't bail on failure — the watch may
        // already have the identity cached from a previous push.
        let _ = self
            .daemon
            .set_music_player_info(state.bus_name.clone(), state.identity.clone())
            .await;

        let meta = self.read_metadata(state).await;
        let artist = owned_str(meta.get("xesam:artist"));
        let album = owned_str(meta.get("xesam:album"));
        let title = owned_str(meta.get("xesam:title"));
        let length_us: i64 = meta.get("mpris:length").and_then(|v| v.downcast_ref::<i64>().ok()).unwrap_or(0).max(0);
        let track_length_ms = (length_us / 1000).min(u32::MAX as i64) as u32;
        let track_number: i32 = meta.get("xesam:trackNumber").and_then(|v| v.downcast_ref::<i32>().ok()).unwrap_or(0);
        let track_number = track_number.max(0) as u32;

        if !artist.is_empty() || !title.is_empty() {
            let _ = self.daemon.set_music_track(artist, album, title, track_length_ms, 0, track_number).await;
        }

        let status: String = self.get_prop(&state.bus_name, "org.mpris.MediaPlayer2.Player", "PlaybackStatus").await.unwrap_or_default();
        // Daemon contract: 0=paused 1=playing 2=rewinding 3=ffwd 4=unknown
        let play_state = match status.as_str() { "Playing" => 1u8, "Paused" => 0u8, _ => 4u8 };
        let position_us: i64 = self.get_prop(&state.bus_name, "org.mpris.MediaPlayer2.Player", "Position").await.unwrap_or(0).max(0);
        let position_ms = (position_us / 1000).min(u32::MAX as i64) as u32;
        let rate: f64 = self.get_prop(&state.bus_name, "org.mpris.MediaPlayer2.Player", "Rate").await.unwrap_or(1.0);
        let shuffle: bool = self.get_prop(&state.bus_name, "org.mpris.MediaPlayer2.Player", "Shuffle").await.unwrap_or(false);
        let shuffle_u8 = if shuffle { 2u8 } else { 1u8 };
        let loop_status: String = self.get_prop(&state.bus_name, "org.mpris.MediaPlayer2.Player", "LoopStatus").await.unwrap_or_default();
        let repeat = match loop_status.as_str() { "None" => 1u8, "Track" => 2u8, "Playlist" => 3u8, _ => 0u8 };
        let _ = self.daemon.set_music_playback_state(play_state, position_ms, (rate * 100.0) as u32, shuffle_u8, repeat).await;

        if let Some(volume) = get_system_volume().await {
            let _ = self.daemon.set_music_volume(volume).await;
        }
    }

    async fn read_metadata(&self, state: &PlayerState) -> HashMap<String, OwnedValue> {
        let reply = match self.conn.call_method(
            Some(state.bus_name.as_str()), "/org/mpris/MediaPlayer2",
            Some("org.freedesktop.DBus.Properties"), "Get",
            &("org.mpris.MediaPlayer2.Player", "Metadata"),
        ).await {
            Ok(r) => r,
            Err(_) => return HashMap::new(),
        };
        let body = reply.body();
        let v: zbus::zvariant::Value<'_> = match body.deserialize() { Ok(v) => v, Err(_) => return HashMap::new() };
        let ov = match OwnedValue::try_from(v) { Ok(ov) => ov, Err(_) => return HashMap::new() };
        HashMap::<String, OwnedValue>::try_from(ov).unwrap_or_default()
    }

    async fn listen_properties_changed(&self, bus_name: &str) -> zbus::Result<()> {
        // Resolve well-known name → unique name so we can verify the sender.
        let unique = match resolve_name(&self.conn, bus_name).await {
            Some(u) => u,
            None => return Ok(()),
        };

        let rule = format!(
            "type='signal',sender='{bus_name}',interface='org.freedesktop.DBus.Properties',member='PropertiesChanged',path='/org/mpris/MediaPlayer2'"
        );
        add_match(&self.conn, &rule).await?;
        debug!("mpris: listening for PropertiesChanged from {bus_name} ({unique})");
        let mut stream = MessageStream::from(&self.conn);
        while let Some(msg) = stream.next().await {
            let msg = match msg { Ok(m) => m, Err(_) => continue };
            let hdr = msg.header();
            if hdr.interface().map(|i| i.as_str()) != Some("org.freedesktop.DBus.Properties") { continue; }
            if hdr.member().map(|m| m.as_str()) != Some("PropertiesChanged") { continue; }
            if hdr.path().map(|p| p.as_str()) != Some("/org/mpris/MediaPlayer2") { continue; }
            // Verify the sender against the resolved unique name so we don't
            // process another player's signals in this listener.
            if hdr.sender().map(|s| s.as_str()) != Some(&unique) { continue; }

            debug!("mpris: PropertiesChanged from {bus_name}");
            if let Some(state) = self.read_player_state(bus_name).await {
                let is_active = {
                    let mut active = self.active.lock().await;
                    if state.playing {
                        match active.as_ref() {
                            Some(current) if current.bus_name != state.bus_name => {
                                *active = Some(state.clone());
                                true
                            }
                            None => {
                                *active = Some(state.clone());
                                true
                            }
                            _ => active.as_ref().map(|a| a.bus_name.as_str()) == Some(bus_name),
                        }
                    } else {
                        if active.as_ref().map(|a| a.bus_name.as_str()) == Some(bus_name) {
                            active.as_mut().unwrap().playing = false;
                        }
                        active.as_ref().map(|a| a.bus_name.as_str()) == Some(bus_name)
                    }
                };
                if is_active {
                    self.push_to_watch(&state).await;
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve a well-known bus name to its unique name.
async fn resolve_name(conn: &Connection, well_known: &str) -> Option<String> {
    let reply = conn
        .call_method(
            Some("org.freedesktop.DBus"),
            "/org/freedesktop/DBus",
            Some("org.freedesktop.DBus"),
            "GetNameOwner",
            &(well_known,),
        )
        .await
        .ok()?;
    reply.body().deserialize::<String>().ok()
}

async fn add_match(conn: &Connection, rule: &str) -> zbus::Result<()> {
    conn.call_method(Some("org.freedesktop.DBus"), "/org/freedesktop/DBus", Some("org.freedesktop.DBus"), "AddMatch", &(rule,)).await.map(|_| ())
}

async fn call_method(conn: &Connection, bus: &str, path: &str, iface: &str, method: &str) {
    let _ = conn.call_method(Some(bus), path, Some(iface), method, &()).await;
}

fn owned_str(v_opt: Option<&OwnedValue>) -> String {
    let v = match v_opt { Some(v) => v, None => return String::new() };
    if let Ok(s) = v.downcast_ref::<String>() { return s; }
    let arr: Vec<String> = match v.clone().try_into() { Ok(a) => a, Err(_) => return String::new() };
    arr.into_iter().next().unwrap_or_default()
}

// ---------------------------------------------------------------------------
// System volume (pactl — works on PulseAudio and PipeWire-Pulse)
// ---------------------------------------------------------------------------

/// Adjust the default sink volume by `delta_pct` percentage points.
/// Tries `pactl` first (PulseAudio / PipeWire-Pulse), then `wpctl` (PipeWire).
async fn adjust_system_volume(delta_pct: i32) {
    let step = if delta_pct >= 0 {
        format!("+{delta_pct}%")
    } else {
        format!("{delta_pct}%")
    };

    trace!("mpris: trying pactl set-sink-volume {step}");
    if try_pactl_set_volume(&step).await.is_ok() {
        return;
    }
    let frac = delta_pct as f64 / 100.0;
    trace!("mpris: pactl failed; trying wpctl set-volume {frac:.2}");
    if try_wpctl_set_volume(frac).await.is_ok() {
        return;
    }
    debug!("mpris: no volume tool found (tried pactl, wpctl)");
}

async fn try_pactl_set_volume(step: &str) -> std::io::Result<()> {
    let output = tokio::process::Command::new("pactl")
        .args(["set-sink-volume", "@DEFAULT_SINK@", "--", step])
        .output()
        .await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        debug!("mpris: pactl set-sink-volume {step} failed: {}", stderr.trim());
        return Err(std::io::Error::new(std::io::ErrorKind::Other, stderr.into_owned()));
    }
    trace!("mpris: pactl set-sink-volume {step} ok");
    Ok(())
}

async fn try_wpctl_set_volume(delta: f64) -> std::io::Result<()> {
    // wpctl set-volume uses VALUE[+|-] suffix: "0.05+" or "0.05-".
    let sign = if delta >= 0.0 { "+" } else { "-" };
    let arg = format!("{:.2}{sign}", delta.abs());
    let output = tokio::process::Command::new("wpctl")
        .args(["set-volume", "@DEFAULT_AUDIO_SINK@", &arg])
        .output()
        .await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        debug!("mpris: wpctl set-volume {arg} failed: {}", stderr.trim());
        return Err(std::io::Error::new(std::io::ErrorKind::Other, stderr.into_owned()));
    }
    trace!("mpris: wpctl set-volume {arg} ok");
    Ok(())
}

/// Read the current default sink volume as a 0–100 percentage.
/// Tries `pactl` first, then `wpctl`.
async fn get_system_volume() -> Option<u8> {
    if let Some(pct) = try_pactl_get_volume().await {
        return Some(pct);
    }
    if let Some(pct) = try_wpctl_get_volume().await {
        return Some(pct);
    }
    None
}

async fn try_pactl_get_volume() -> Option<u8> {
    let output = tokio::process::Command::new("pactl")
        .args(["get-sink-volume", "@DEFAULT_SINK@"])
        .output()
        .await
        .ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    // "Volume: front-left: 32768 /  50% / -6.02 dB,   front-right: ..."
    let pct = stdout.split('/').nth(1)?.trim().strip_suffix('%')?;
    pct.parse::<u8>().ok()
}

async fn try_wpctl_get_volume() -> Option<u8> {
    let output = tokio::process::Command::new("wpctl")
        .args(["get-volume", "@DEFAULT_AUDIO_SINK@"])
        .output()
        .await
        .ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    // "Volume: 0.50"
    let vol_str = stdout.strip_prefix("Volume: ")?.trim();
    let vol: f64 = vol_str.parse().ok()?;
    Some((vol * 100.0).round() as u8)
}

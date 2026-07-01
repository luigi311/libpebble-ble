//! Reconnect supervisor: keeps a live watch connection with exponential backoff.
//!
//! Wires libpebble-ble event callbacks to `DaemonEvent` so the D-Bus signal
//! emitter can forward them to clients. Reconnect delay: 2s → 30s cap.

use std::sync::Arc;

use libpebble_ble::{
    decode_watch_pref, parse_activity_preferences, parse_heart_rate_preferences,
    parse_hrm_preferences, parse_units_distance, AppRunStateHandler, BatteryHandler,
    HealthDataHandler, MusicAction, MusicActionHandler, Pebble, WatchPrefHandler,
};
use tracing::{debug, info, warn};

use crate::service::{CobbleDaemon, DaemonEvent};

/// BlobDB id of the WatchPrefs database (matches `BlobDBId::WatchPrefs`); the
/// only DB whose writebacks carry health/settings keys we decode.
const WATCH_PREFS_DB: u8 = 12;

pub async fn run_supervisor(daemon: CobbleDaemon) {
    let mut backoff = 2.0f64;

    while !daemon.is_stopping() {
        let (address, adapter) = daemon.current_connection_params();

        // If no watch address is configured yet (e.g. fresh install or first
        // run before the GUI has scanned and saved one), wait event-driven.
        // reload_config bumps a revision counter that we .await here — zero
        // wakeups, zero battery impact.
        if address.is_empty() {
            debug!("no watch address configured; waiting for config update ...");
            let mut rx = daemon.config_changed();
            // Re-check under lock to avoid a race: reload_config could have
            // run between the initial check and subscribe().
            let (addr, _) = daemon.current_connection_params();
            if addr.is_empty() {
                let _ = rx.changed().await;
            }
            continue;
        }

        info!("connecting to watch {address} ...");
        let pebble = Arc::new(Pebble::new(&address, &adapter));

        // Wire handlers before connect so we catch any early events.
        {
            let event_tx = daemon.event_tx();
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
            let tx = event_tx.clone();
            pebble.on_health(Arc::new(move |batch| {
                let _ = tx.send(DaemonEvent::HealthData(batch));
            }) as HealthDataHandler);
            let tx = event_tx.clone();
            pebble.on_battery(Arc::new(move |level| {
                let _ = tx.send(DaemonEvent::BatteryChanged(level));
            }) as BatteryHandler);
            let tx = event_tx.clone();
            pebble.on_app_run_state(Arc::new(move |uuid, running| {
                let _ = tx.send(DaemonEvent::AppRunState { uuid, running });
            }) as AppRunStateHandler);
            let tx = event_tx.clone();
            pebble.on_music_action(Arc::new(move |action: MusicAction| {
                let _ = tx.send(DaemonEvent::MusicAction(action.as_str().to_string()));
            }) as MusicActionHandler);

            // The watch syncs the health profile through the WatchPrefs DB
            // (db 12), keyed by name — HealthParams (db 7) is NotSupported for
            // MarkAllDirty. Decode the health-related keys into events; log the rest.
            let tx = event_tx.clone();
            pebble.on_watch_pref(Arc::new(move |db: u8, key: String, value: Vec<u8>| {
                // Health/settings keys live in the WatchPrefs DB (12). Ignore
                // writebacks from any other database so we never decode a
                // colliding key from the wrong DB.
                if db != WATCH_PREFS_DB {
                    return;
                }
                // Value-level details (PII, raw blobs) are logged only at debug
                // (i.e. under --verbose); default logs stay value-free.
                match key.as_str() {
                    "activityPreferences" => match parse_activity_preferences(&value) {
                        Some(p) => {
                            debug!(
                                "activityPreferences: height={}cm weight={}kg age={} gender={} \
                                 tracking={} activity_insights={} sleep_insights={}",
                                p.height_cm, p.weight_kg, p.age, p.gender,
                                p.tracking_enabled, p.activity_insights_enabled, p.sleep_insights_enabled,
                            );
                            let _ = tx.send(DaemonEvent::HealthProfile(p));
                        }
                        None => warn!("activityPreferences blob malformed ({} bytes)", value.len()),
                    },
                    "hrmPreferences" => match parse_hrm_preferences(&value) {
                        Some(hrm) => {
                            debug!(
                                "hrmPreferences: enabled={} interval={:?} activity_tracking={:?}",
                                hrm.enabled, hrm.measurement_interval, hrm.activity_tracking_enabled,
                            );
                            let _ = tx.send(DaemonEvent::HealthHrm(hrm));
                        }
                        None => warn!("hrmPreferences blob malformed ({} bytes)", value.len()),
                    },
                    "heartRatePreferences" => match parse_heart_rate_preferences(&value) {
                        Some(hr) => {
                            debug!(
                                "heartRatePreferences: resting={} elevated={} max={} zones={}/{}/{}",
                                hr.resting_hr, hr.elevated_hr, hr.max_hr,
                                hr.zone1_threshold, hr.zone2_threshold, hr.zone3_threshold,
                            );
                            let _ = tx.send(DaemonEvent::HealthHeartRate(hr));
                        }
                        None => warn!("heartRatePreferences blob malformed ({} bytes)", value.len()),
                    },
                    "unitsDistance" => match parse_units_distance(&value) {
                        Some(imperial) => {
                            debug!("unitsDistance: imperial={imperial}");
                            let _ = tx.send(DaemonEvent::HealthUnits(imperial));
                        }
                        None => warn!("unitsDistance blob malformed ({} bytes)", value.len()),
                    },
                    // General watch settings (backlight, clock, vibration, quiet time, …).
                    other => match decode_watch_pref(other, &value) {
                        Some(decoded) => {
                            debug!("watch setting {other:?} = {decoded:?}");
                            let _ = tx.send(DaemonEvent::WatchSetting {
                                key: other.to_string(),
                                value: decoded,
                            });
                        }
                        None => debug!(
                            "watch pref push db={db} key={other:?} ({} bytes) — no decoder",
                            value.len()
                        ),
                    },
                }
            }) as WatchPrefHandler);
        }

        match pebble.connect().await {
            Ok(()) => {
                // Verify the config hasn't changed while connect() was in flight.
                // If it has, discard this connection and retry immediately with the
                // new params rather than calling set_connected with a stale handle.
                let (cur_addr, cur_adapter) = daemon.current_connection_params();
                if cur_addr != address || cur_adapter != adapter {
                    let _ = pebble.disconnect().await;
                    backoff = 2.0;
                    continue;
                }

                backoff = 2.0;
                daemon.set_connected(Arc::clone(&pebble));
                info!("watch connected; daemon ready");

                if let Err(e) = pebble.update_time().await {
                    warn!("time sync on connect failed: {e}");
                }

                if let Err(e) = pebble.fetch_health_data() {
                    warn!("health sync on connect failed: {e}");
                }

                // Periodic health sync while connected.
                let pebble_sync = Arc::clone(&pebble);
                let sync_task = tokio::spawn(async move {
                    let mut interval =
                        tokio::time::interval(std::time::Duration::from_secs(15 * 60));
                    interval.tick().await; // skip the immediate first tick
                    loop {
                        interval.tick().await;
                        if let Err(e) = pebble_sync.fetch_health_data() {
                            warn!("periodic health sync failed: {e}");
                        }
                        debug!("periodic health sync triggered");
                    }
                });

                pebble.wait_disconnected().await;
                sync_task.abort();
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

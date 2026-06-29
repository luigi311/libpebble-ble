//! Reconnect supervisor: keeps a live watch connection with exponential backoff.
//!
//! Wires libpebble-ble event callbacks to `DaemonEvent` so the D-Bus signal
//! emitter can forward them to clients. Reconnect delay: 2s → 30s cap.

use std::sync::Arc;

use libpebble_ble::{
    parse_activity_preferences, parse_heart_rate_preferences, parse_hrm_preferences,
    parse_units_distance, HealthDataHandler, Pebble, WatchPrefHandler,
};
use tracing::{debug, info, warn};

use crate::service::{CobbleDaemon, DaemonEvent};

pub async fn run_supervisor(daemon: CobbleDaemon) {
    let mut backoff = 2.0f64;

    while !daemon.is_stopping() {
        let (address, adapter) = daemon.current_connection_params();
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

            // The watch syncs the health profile through the WatchPrefs DB
            // (db 12), keyed by name — HealthParams (db 7) is NotSupported for
            // MarkAllDirty. Decode the health-related keys into events; log the rest.
            let tx = event_tx.clone();
            pebble.on_watch_pref(Arc::new(move |db: u8, key: String, value: Vec<u8>| {
                let _ = db; // keep for parity with the handler signature
                match key.as_str() {
                    "activityPreferences" => match parse_activity_preferences(&value) {
                        Some(p) => {
                            info!(
                                "watch health profile: height={}cm weight={}kg age={} gender={} \
                                 tracking={} activity_insights={} sleep_insights={}",
                                p.height_cm, p.weight_kg, p.age, p.gender,
                                p.tracking_enabled, p.activity_insights_enabled, p.sleep_insights_enabled,
                            );
                            let _ = tx.send(DaemonEvent::HealthProfile(p));
                        }
                        None => warn!("activityPreferences blob malformed: {value:02x?}"),
                    },
                    "hrmPreferences" => {
                        if let Some(hrm) = parse_hrm_preferences(&value) {
                            info!(
                                "watch health profile: hrm_enabled={} interval={:?} activity_tracking={:?}",
                                hrm.enabled, hrm.measurement_interval, hrm.activity_tracking_enabled,
                            );
                            let _ = tx.send(DaemonEvent::HealthHrm(hrm));
                        }
                    }
                    "heartRatePreferences" => match parse_heart_rate_preferences(&value) {
                        Some(hr) => {
                            info!(
                                "watch HR prefs: resting={} elevated={} max={} zones={}/{}/{}",
                                hr.resting_hr, hr.elevated_hr, hr.max_hr,
                                hr.zone1_threshold, hr.zone2_threshold, hr.zone3_threshold,
                            );
                            let _ = tx.send(DaemonEvent::HealthHeartRate(hr));
                        }
                        None => warn!("heartRatePreferences blob malformed: {value:02x?}"),
                    },
                    "unitsDistance" => {
                        if let Some(imperial) = parse_units_distance(&value) {
                            info!("watch health profile: imperial_units={imperial}");
                            let _ = tx.send(DaemonEvent::HealthUnits(imperial));
                        }
                    }
                    other => debug!("watch pref push db={db} key={other:?} ({} bytes)", value.len()),
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

//! Reconnect supervisor: keeps a live watch connection with exponential backoff.
//!
//! Wires libpebble-ble event callbacks to `DaemonEvent` so the D-Bus signal
//! emitter can forward them to clients. Reconnect delay: 2s → 30s cap.

use std::sync::Arc;

use libpebble_ble::{HealthDataHandler, Pebble};
use tracing::{debug, info, warn};

use crate::service::{DaemonEvent, PebbleDaemon};

pub async fn run_supervisor(daemon: PebbleDaemon, address: String, adapter: String) {
    let mut backoff = 2.0f64;

    while !daemon.is_stopping() {
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

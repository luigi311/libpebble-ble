//! BLE connection lifecycle: scanning, connecting, pairing, and teardown.
//!
//! All async `Pebble::connect*` methods, GATT subscription helpers,
//! and the bonding handshake live here.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bluer::{AdapterEvent, Address, Device};
use futures::StreamExt;
use tokio::sync::Notify;
use tokio::time::timeout;
use tracing::{debug, info, trace, warn};

use super::{dispatch, Pebble};
use crate::endpoints::blob_db::{
    build_blobdb2_mark_all_dirty, build_blobdb2_version,
    BlobDB2Incoming, BlobDBId, BlobDBStatus,
};
use crate::endpoints::Endpoint;
use crate::error::PebbleError;
use crate::transport::agent::build_pairing_agent;
use crate::transport::gatt_server::start_gatt_server;
use crate::uuids::{
    BATTERY_LEVEL_CHARACTERISTIC, CONNECTION_PARAMS_CHARACTERISTIC,
    CONNECTIVITY_CHARACTERISTIC, MTU_CHARACTERISTIC, PAIRING_TRIGGER_CHARACTERISTIC,
};

impl Pebble {
    // ── scanning ───────────────────────────────────────────────────────

    /// Scan for nearby Pebble devices for `timeout_secs` seconds.
    /// Returns a list of `(address, name)` pairs for every device whose
    /// Bluetooth name contains "pebble" (case-insensitive).
    pub async fn scan(adapter_name: &str, timeout_secs: f64) -> Result<Vec<(String, String)>, PebbleError> {
        let session = bluer::Session::new().await?;
        let adapter = session
            .adapter(adapter_name)
            .map_err(|e| PebbleError::Other(format!("adapter {adapter_name}: {e}")))?;

        adapter.set_discoverable_timeout(timeout_secs as u32).await?;
        let mut stream = adapter.discover_devices().await?;
        let mut found: Vec<(String, String)> = Vec::new();

        let _ = tokio::time::timeout(Duration::from_secs_f64(timeout_secs), async {
            while let Some(event) = stream.next().await {
                if let AdapterEvent::DeviceAdded(addr) = event
                    && let Ok(device) = adapter.device(addr)
                {
                    let name = device.name().await.ok().flatten().unwrap_or_default();
                    if name.to_lowercase().contains("pebble") {
                        debug!("found Pebble: {addr} \"{name}\"");
                        found.push((addr.to_string(), name));
                    }
                }
            }
        })
        .await;

        Ok(found)
    }

    // ── connect ────────────────────────────────────────────────────────

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
                    super::dispatch::on_pebble_message(msg, &inner);
                });
            }),
            Arc::new(move || {
                debug!("watch disconnected from PPoGATT server");
                let _ = connected_tx_for_disc.send(false);
                let (pending, nack_handlers) = {
                    let mut inner = inner_for_disc.lock().unwrap();
                    let pending: Vec<(u8, tokio::sync::oneshot::Sender<bool>)> =
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
                match timeout(Duration::from_secs(10), session_ready_notify.notified()).await {
                    Ok(()) => debug!("PPoGATT session ready"),
                    Err(_) => warn!("PPoGATT session not confirmed ready; early sends may be dropped"),
                }
            }
            Err(_) => {
                warn!("watch did not connect back within {conn_timeout}s; sends may not reach watch")
            }
        }

        // 8. BlobDB2 version handshake.
        let blob_db_version = self.negotiate_blobdb2_version().await;
        self.inner.lock().unwrap().blob_db_version = blob_db_version;
        if blob_db_version >= 1 {
            let _ = self.send_pebble(
                Endpoint::BlobDbV2,
                &build_blobdb2_mark_all_dirty(dispatch::rand_u16(), BlobDBId::WatchPrefs),
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
                    poll.tick().await;
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
                    poll.tick().await;
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

    // ── private connection helpers ─────────────────────────────────────

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
            if i > 1 {
                let scan_duration: f64 = 3.0 * i as f64;
                let _ = Pebble::scan(adapter.name(), scan_duration).await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            };
            let device = adapter.device(address)?;
            let result = timeout(Duration::from_secs_f64(conn_timeout), device.connect()).await;
            match result {
                Ok(Ok(())) => return Ok(device),
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
        if let Some(c) = dispatch::find_char(device, PAIRING_TRIGGER_CHARACTERISTIC).await {
            let _ = c.read().await;
            let _ = c.write(&[0x09]).await;
        }
        info!("waiting for the watch to initiate bonding; CONFIRM ON THE WATCH when prompted");

        for _ in 0..20 {
            if device.is_paired().await.unwrap_or(false) {
                debug!("bonded (watch-initiated)");
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        debug!("watch did not initiate bonding; calling pair() from the host");
        if let Err(e) = device.pair().await {
            warn!("host-initiated pair() failed: {e}");
        }
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
            if let Some(c) = dispatch::find_char(device, char_uuid).await {
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

        if let Some(c) = dispatch::find_char(device, MTU_CHARACTERISTIC).await
            && let Ok(data) = c.read().await
            && data.len() >= 2
        {
            let watch_mtu = u16::from_le_bytes([data[0], data[1]]) as usize;
            debug!("watch requested MTU: {watch_mtu}");
            if let Some(srv) = &self.inner.lock().unwrap().gatt_server
                && watch_mtu >= 23
            {
                srv.set_mtu(watch_mtu);
            }
        }
    }

    /// Read the watch battery level once and subscribe to change notifications.
    async fn subscribe_battery(&self, device: &Device) {
        self.inner.lock().unwrap().battery_level = None;
        let Some(c) = dispatch::find_char(device, BATTERY_LEVEL_CHARACTERISTIC).await else {
            debug!("no battery characteristic; battery level unavailable");
            return;
        };
        match c.read().await {
            Ok(data) => {
                if let Some(&level) = data.first() {
                    super::dispatch::update_battery(&self.inner, level);
                }
            }
            Err(e) => debug!("battery read failed: {e}"),
        }
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            match c.notify().await {
                Ok(stream) => {
                    debug!("subscribed to battery level");
                    tokio::pin!(stream);
                    while let Some(data) = stream.next().await {
                        if let Some(&level) = data.first() {
                            super::dispatch::update_battery(&inner, level);
                        }
                    }
                }
                Err(e) => warn!("subscribe to battery failed: {e}"),
            }
        });
    }

    async fn write_pairing_trigger(&self, device: &Device) {
        if let Some(c) = dispatch::find_char(device, PAIRING_TRIGGER_CHARACTERISTIC).await {
            if let Err(e) = c.write(&[0x09]).await {
                warn!("pairing trigger write failed: {e}");
            } else {
                debug!("wrote 0x09 to pairing trigger (server mode)");
            }
        }
    }

    async fn negotiate_blobdb2_version(&self) -> u8 {
        let token = dispatch::rand_u16();
        let (tx, rx) = tokio::sync::oneshot::channel::<BlobDB2Incoming>();
        self.inner.lock().unwrap().blobdb2_pending.insert(token, tx);
        if self.send_pebble(Endpoint::BlobDbV2, &build_blobdb2_version(token)).is_err() {
            self.inner.lock().unwrap().blobdb2_pending.remove(&token);
            return 0;
        }
        match timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(BlobDB2Incoming::VersionResponse { status, version, .. }))
                if status == BlobDBStatus::Success as u8 =>
            {
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

    // ── teardown ───────────────────────────────────────────────────────

    /// Remove the Bluetooth bond (unpair). The supervisor re-pairs on the
    /// next reconnect.
    pub async fn forget(&self) -> Result<(), PebbleError> {
        let address = Address::from_str(&self.address)
            .map_err(|e| PebbleError::Other(format!("invalid address: {e}")))?;
        let session = bluer::Session::new().await?;
        let adapter = session.adapter(&self.adapter_name)?;
        adapter.remove_device(address).await?;
        info!("removed {} from BlueZ (stale bond cleared)", self.address);
        Ok(())
    }
}

//! Inbound Pebble Protocol message dispatch.
//!
//! Called from the GATT server data callback (via `connection.rs`).
//! Routes endpoint payloads to the correct decoder and fires registered
//! handlers. Also contains utility helpers used by other pebble modules.

use std::sync::{Arc, Mutex};

use tracing::{debug, trace, warn};

use super::PebbleInner;
use crate::endpoints::app_message::{
    build_app_message_ack, parse_app_message, AppMessageCmd,
};
use crate::endpoints::app_run_state::{parse_app_run_state, AppRunStateCmd};
use crate::endpoints::blob_db::{
    build_blobdb2_syncdone_response, build_blobdb2_write_response,
    build_blobdb2_writeback_response, parse_blobdb2_incoming, parse_blobdb_response,
    BlobDB2Incoming, BlobDBStatus,
};
use crate::endpoints::datalog::{self, build_reply, DatalogData, DATALOG_CLOSE, DATALOG_OPENSESSION, DATALOG_SENDDATA, DATALOG_TIMEOUT};
use crate::endpoints::music::parse_music_command;
use crate::endpoints::phone_control::parse_phone_action;
use crate::endpoints::phone_version::build_phone_version_response;
use crate::endpoints::ping::{build_pong, parse_ping};
use crate::endpoints::screenshot::{
    parse_screenshot_header, ScreenshotResponseCode,
};
use crate::endpoints::system::{
    parse_factory_data_response, parse_watch_color, parse_watch_version_response,
    system_message_type, WATCH_VERSION_RESPONSE,
};
use crate::endpoints::{pebble_pack, pebble_unpack, Endpoint};

use super::inner::RawScreenshot;

// Re-export helpers used by connection.rs and methods in mod.rs.
pub(crate) use helpers::{find_char, rand_u16};

/// Feed one assembled Pebble Protocol message to the dispatch state machine.
/// Called from the GATT server data callback.
pub(crate) fn on_pebble_message(message: Vec<u8>, inner: &Arc<Mutex<PebbleInner>>) {
    let Some((endpoint_raw, payload)) = pebble_unpack(&message) else {
        return;
    };
    trace!("rx endpoint={endpoint_raw} len={}", payload.len());

    match Endpoint::from_u16(endpoint_raw) {
        Some(Endpoint::PhoneVersion) => {
            if let Some(reply) = pebble_pack(Endpoint::PhoneVersion, &build_phone_version_response())
                && let Some(srv) = &inner.lock().unwrap().gatt_server
            {
                srv.send(reply);
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
                        inner.lock().unwrap().watch_version_pending.clear();
                    }
                }
            }
        }
        Some(Endpoint::SystemMessage) => {
            debug!("system message type={:?}", system_message_type(payload));
        }
        Some(Endpoint::FactoryRegistry) => {
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
                if let Some(reply) = pebble_pack(Endpoint::Ping, &build_pong(cookie))
                    && let Some(srv) = &inner.lock().unwrap().gatt_server
                {
                    srv.send(reply);
                }
            }
        }
        Some(Endpoint::AppMessage) => {
            on_app_message(payload.to_vec(), inner);
        }
        Some(Endpoint::AppRunState) => {
            if let Some((cmd, uuid)) = parse_app_run_state(payload) {
                let running = match cmd {
                    AppRunStateCmd::Start => true,
                    AppRunStateCmd::Stop => false,
                    AppRunStateCmd::Request => return,
                };
                debug!("app run state: uuid={uuid} running={running}");
                let handlers: Vec<_> = inner.lock().unwrap().app_run_state_handlers.clone();
                for h in handlers {
                    h(uuid.clone(), running);
                }
            }
        }
        Some(Endpoint::MusicControl) => {
            if let Some(action) = parse_music_command(payload) {
                debug!("music action from watch: {}", action.as_str());
                let handlers: Vec<_> = inner.lock().unwrap().music_action_handlers.clone();
                for h in handlers {
                    h(action);
                }
            }
        }
        Some(Endpoint::PhoneControl) => {
            debug!("phone packet from watch: {payload:02x?}");
            if let Some(action) = parse_phone_action(payload) {
                debug!("phone action from watch: {action:?}");
                let handlers: Vec<_> = inner.lock().unwrap().phone_action_handlers.clone();
                for h in handlers {
                    h(action);
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
        _ => {
            trace!("rx unknown endpoint={endpoint_raw} len={}", payload.len());
        } // unknown endpoint; log at trace for diagnostics
    }
}

fn on_app_message(payload: Vec<u8>, inner: &Arc<Mutex<PebbleInner>>) {
    trace!("inbound APP_MESSAGE raw: {} bytes", payload.len());
    let Some(parsed) = parse_app_message(&payload) else {
        debug!("APP_MESSAGE: failed to parse {} bytes", payload.len());
        return;
    };
    match parsed.cmd {
        AppMessageCmd::Push => {
            if let (Some(uuid), Some(data)) = (parsed.app_uuid, parsed.data) {
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
        debug!("DataLog: failed to parse header from {} bytes", payload.len());
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
            if let Some(pkt) = ack
                && let Some(srv) = &inner.lock().unwrap().gatt_server
            {
                srv.send(pkt);
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
                let ack = pebble_pack(Endpoint::DataLog, &build_reply(handle, true));
                if let Some(pkt) = ack
                    && let Some(srv) = &inner.lock().unwrap().gatt_server
                {
                    srv.send(pkt);
                }
                let handlers: Vec<_> = inner.lock().unwrap().health_handlers.clone();
                for h in handlers {
                    h(batch.clone());
                }
            } else {
                warn!("DataLog SENDDATA for unknown session handle={handle}; sending NACK");
                let nack = pebble_pack(Endpoint::DataLog, &build_reply(handle, false));
                if let Some(pkt) = nack
                    && let Some(srv) = &inner.lock().unwrap().gatt_server
                {
                    srv.send(pkt);
                }
            }
        }
        DATALOG_CLOSE => {
            debug!("DataLog CLOSE handle={handle}");
            inner.lock().unwrap().datalog_sessions.remove(&handle);
            let ack = pebble_pack(Endpoint::DataLog, &build_reply(handle, true));
            if let Some(pkt) = ack
                && let Some(srv) = &inner.lock().unwrap().gatt_server
            {
                srv.send(pkt);
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
    } else if !guard.pending.is_empty()
        && let Some(oldest) = guard.pending.keys().copied().next()
    {
        debug!("ACK txn={txn} had no match; resolving oldest pending txn={oldest}");
        if let Some(sender) = guard.pending.remove(&oldest) {
            let _ = sender.send(acked);
        }
    }
}

/// Accumulate one inbound screenshot message.
fn on_screenshot_message(payload: &[u8], inner: &Arc<Mutex<PebbleInner>>) {
    enum Step {
        Continue,
        Error(String),
        Done,
    }
    let mut guard = inner.lock().unwrap();
    let Some(acc) = guard.screenshot.as_mut() else {
        return;
    };
    let step = if acc.version.is_none() && acc.expected == 0 && acc.buffer.is_empty() {
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
pub(crate) fn update_battery(inner: &Arc<Mutex<PebbleInner>>, level: u8) {
    let handlers = {
        let mut guard = inner.lock().unwrap();
        if guard.battery_level == Some(level) {
            return;
        }
        guard.battery_level = Some(level);
        guard.battery_handlers.clone()
    };
    debug!("battery level: {level}%");
    for h in handlers {
        h(level);
    }
}

// ── BlobDB2 dispatch ───────────────────────────────────────────────────

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
                    None => debug!("BlobDB2 unsolicited response: {other:?}"),
                }
            }
        }
    }
}

fn blobdb2_send(inner: &Arc<Mutex<PebbleInner>>, payload: Vec<u8>) {
    if let Some(pkt) = pebble_pack(Endpoint::BlobDbV2, &payload)
        && let Some(srv) = &inner.lock().unwrap().gatt_server
    {
        srv.send(pkt);
    }
}

// ── Cross-module helpers ───────────────────────────────────────────────

mod helpers {
    use bluer::Device;

    pub(crate) async fn find_char(device: &Device, uuid: bluer::Uuid) -> Option<bluer::gatt::remote::Characteristic> {
        for service in device.services().await.ok()? {
            for c in service.characteristics().await.ok()? {
                if c.uuid().await.map(|u| u == uuid).unwrap_or(false) {
                    return Some(c);
                }
            }
        }
        None
    }

    pub(crate) fn rand_u16() -> u16 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        (t.subsec_nanos() & 0xFFFF) as u16
    }
}

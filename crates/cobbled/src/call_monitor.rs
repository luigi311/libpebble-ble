//! Real phone-call bridge: ModemManager / oFono ↔ Pebble watch.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, trace, warn};
use zbus::{zvariant::OwnedValue, Connection, MessageStream};

use crate::service::CobbleDaemon;

static NEXT_COOKIE: AtomicU32 = AtomicU32::new(1);

struct CallMap {
    inner: Arc<Mutex<HashMap<u32, ModemCall>>>,
}

#[derive(Clone)]
struct ModemCall {
    bus_name: String,
    call_path: String,
}

impl CallMap {
    fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(HashMap::new())) }
    }

    async fn insert(&self, cookie: u32, call: ModemCall) {
        self.inner.lock().await.insert(cookie, call);
    }

    async fn take(&self, cookie: u32) -> Option<ModemCall> {
        self.inner.lock().await.remove(&cookie)
    }

    async fn get(&self, cookie: u32) -> Option<ModemCall> {
        self.inner.lock().await.get(&cookie).cloned()
    }

    async fn remove_by_path(&self, call_path: &str) -> Option<u32> {
        let mut map = self.inner.lock().await;
        let cookie = map.iter().find(|(_, c)| c.call_path == call_path).map(|(k, _)| *k);
        if let Some(c) = cookie { map.remove(&c); }
        cookie
    }

    async fn find_by_path(&self, call_path: &str) -> Option<u32> {
        let map = self.inner.lock().await;
        map.iter().find(|(_, c)| c.call_path == call_path).map(|(k, _)| *k)
    }
}

// ── Public entry point ──────────────────────────────────────────────────

pub async fn run_call_monitor(daemon: CobbleDaemon, mut action_rx: mpsc::UnboundedReceiver<(String, u32)>) {
    // ModemManager and oFono are system services — use the system bus.
    let conn = match Connection::system().await {
        Ok(c) => {
            trace!("call-monitor: system bus connected");
            c
        }
        Err(e) => { warn!("call-monitor: cannot connect to system bus: {e}"); return; }
    };
    let map = CallMap::new();

    let conn2 = conn.clone();
    let daemon2 = daemon.clone();
    let map2 = CallMap { inner: map.inner.clone() };
    tokio::spawn(async move { watch_modem_manager(&conn2, &daemon2, &map2).await });

    let conn3 = conn.clone();
    let daemon3 = daemon.clone();
    let map3 = CallMap { inner: map.inner.clone() };
    tokio::spawn(async move { watch_ofono(&conn3, &daemon3, &map3).await });

    while let Some((action, cookie)) = action_rx.recv().await {
        handle_watch_action(&conn, &daemon, &map, &action, cookie).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// ModemManager
// ═══════════════════════════════════════════════════════════════════════

async fn watch_modem_manager(conn: &Connection, daemon: &CobbleDaemon, map: &CallMap) {
    trace!("call-monitor: starting ModemManager watcher");
    let _ = add_match(conn, "type='signal',sender='org.freedesktop.ModemManager1',interface='org.freedesktop.ModemManager1.Modem.Voice',member='CallAdded'").await;
    let _ = add_match(conn, "type='signal',sender='org.freedesktop.ModemManager1',interface='org.freedesktop.DBus.Properties',member='PropertiesChanged'").await;

    match list_mm_objects(conn, "org.freedesktop.ModemManager1", "/org/freedesktop/ModemManager1/Modem").await {
        Ok(modems) => {
            trace!("call-monitor: found {} MM modems: {modems:?}", modems.len());
            for modem in modems {
                scan_mm_calls(conn, daemon, map, &modem).await;
            }
        }
        Err(e) => trace!("call-monitor: MM introspection failed: {e}"),
    }

    let mut stream = MessageStream::from(conn);
    while let Some(msg) = stream.next().await {
        let msg = match msg { Ok(m) => m, Err(_) => continue };
        let hdr = msg.header();

        if hdr.member().map(|m| m.as_str()) == Some("CallAdded")
            && hdr.interface().map(|i| i.as_str()) == Some("org.freedesktop.ModemManager1.Modem.Voice")
        {
            let body = msg.body();
            if let Ok(call_path) = body.deserialize::<zbus::zvariant::ObjectPath<'_>>() {
                let call_path = call_path.to_string();
                debug!("call-monitor: MM call added {call_path}");
                handle_mm_call(conn, daemon, map, &call_path).await;
            }
        }

        if hdr.member().map(|m| m.as_str()) == Some("PropertiesChanged") {
            handle_mm_property_change(conn, daemon, map, &msg).await;
        }
    }
}

async fn scan_mm_calls(conn: &Connection, daemon: &CobbleDaemon, map: &CallMap, modem: &str) {
    let voice_path = format!("{modem}/Voice");
    if let Ok(calls) = list_mm_objects(conn, "org.freedesktop.ModemManager1", &voice_path).await {
        for call_path in calls {
            handle_mm_call(conn, daemon, map, &call_path).await;
        }
    }
}

async fn handle_mm_call(conn: &Connection, daemon: &CobbleDaemon, map: &CallMap, call_path: &str) {
    trace!("call-monitor: inspecting MM call {call_path}");
    let state: i32 = get_mm_prop(conn, call_path, "org.freedesktop.ModemManager1.Call", "State").await.unwrap_or(0);
    let direction: i32 = get_mm_prop(conn, call_path, "org.freedesktop.ModemManager1.Call", "Direction").await.unwrap_or(0);
    trace!("call-monitor: MM call {call_path} state={state} direction={direction}");
    if direction != 1 || state != 1 { return; }

    let number: String = get_mm_prop(conn, call_path, "org.freedesktop.ModemManager1.Call", "Number").await.unwrap_or_default();
    if number.is_empty() { return; }

    let cookie = NEXT_COOKIE.fetch_add(1, Ordering::SeqCst);
    map.insert(cookie, ModemCall { bus_name: "org.freedesktop.ModemManager1".into(), call_path: call_path.to_string() }).await;
    debug!("call-monitor: incoming MM call (cookie={cookie})");
    let _ = daemon.push_incoming_call(cookie, number.clone(), number).await;
}

async fn handle_mm_property_change(_conn: &Connection, daemon: &CobbleDaemon, map: &CallMap, msg: &zbus::Message) {
    let call_path = match msg.header().path() {
        Some(p) => p.to_string(),
        None => return,
    };
    let body = msg.body();
    let Ok((iface, changed, _)) = body.deserialize::<(String, HashMap<String, OwnedValue>, Vec<String>)>() else { return };
    if iface != "org.freedesktop.ModemManager1.Call" { return; }

    if let Some(state) = changed.get("State")
        && let Ok(s) = state.downcast_ref::<i32>()
    {
        if s == 9 {
            if let Some(cookie) = map.remove_by_path(&call_path).await {
                debug!("call-monitor: MM call {cookie} terminated");
                let _ = daemon.push_call_end(cookie).await;
            }
        } else if s == 3 {
            // Call became active — notify watch, keep cookie for hangup.
            if let Some(cookie) = map.find_by_path(&call_path).await {
                debug!("call-monitor: MM call {cookie} active");
                let _ = daemon.push_call_start(cookie).await;
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// oFono
// ═══════════════════════════════════════════════════════════════════════

async fn watch_ofono(conn: &Connection, daemon: &CobbleDaemon, map: &CallMap) {
    trace!("call-monitor: starting oFono watcher");
    let _ = add_match(conn, "type='signal',sender='org.ofono',interface='org.ofono.VoiceCallManager',member='CallAdded'").await;
    let _ = add_match(conn, "type='signal',sender='org.ofono',interface='org.ofono.VoiceCall',member='PropertyChanged'").await;

    match list_mm_objects(conn, "org.ofono", "/org/ofono").await {
        Ok(modems) => {
            trace!("call-monitor: found {} oFono modems: {modems:?}", modems.len());
            for modem in modems {
                scan_ofono_calls(conn, daemon, map, &modem).await;
            }
        }
        Err(e) => trace!("call-monitor: oFono introspection failed: {e}"),
    }

    let mut stream = MessageStream::from(conn);
    while let Some(msg) = stream.next().await {
        let msg = match msg { Ok(m) => m, Err(_) => continue };
        let hdr = msg.header();

        if hdr.member().map(|m| m.as_str()) == Some("CallAdded")
            && hdr.interface().map(|i| i.as_str()) == Some("org.ofono.VoiceCallManager")
        {
            let body = msg.body();
            if let Ok((call_path, props)) = body.deserialize::<(zbus::zvariant::ObjectPath<'_>, HashMap<String, OwnedValue>)>() {
                let call_path_str = call_path.to_string();
                debug!("call-monitor: oFono call added {call_path_str}");
                handle_ofono_call(daemon, map, &call_path_str, &props).await;
            }
        }

        if hdr.member().map(|m| m.as_str()) == Some("PropertyChanged") {
            handle_ofono_property_change(daemon, map, &msg).await;
        }
    }
}

async fn scan_ofono_calls(conn: &Connection, daemon: &CobbleDaemon, map: &CallMap, modem: &str) {
    let calls: Option<Vec<zbus::zvariant::ObjectPath<'_>>> = get_ofono_prop(conn, modem, "org.ofono.VoiceCallManager", "Calls").await;
    if let Some(calls) = calls {
        for call_path in calls {
            let path = call_path.to_string();
            let state: Option<String> = get_ofono_prop(conn, &path, "org.ofono.VoiceCall", "State").await;
            if let Some(ref s) = state
                && (s == "incoming" || s == "waiting")
            {
                let number: Option<String> = get_ofono_prop(conn, &path, "org.ofono.VoiceCall", "LineIdentification").await;
                if let Some(num) = number
                    && !num.is_empty()
                {
                    let cookie = NEXT_COOKIE.fetch_add(1, Ordering::SeqCst);
                    map.insert(cookie, ModemCall { bus_name: "org.ofono".into(), call_path: path.clone() }).await;
                    debug!("call-monitor: incoming oFono call (cookie={cookie})");
                    let _ = daemon.push_incoming_call(cookie, num.clone(), num).await;
                }
            }
        }
    }
}

async fn handle_ofono_call(daemon: &CobbleDaemon, map: &CallMap, call_path: &str, props: &HashMap<String, OwnedValue>) {
    let state = props.get("State").and_then(|v| v.downcast_ref::<String>().ok()).map(|s| s.to_string()).unwrap_or_default();
    if state != "incoming" && state != "waiting" { return; }
    let number = props.get("LineIdentification").and_then(|v| v.downcast_ref::<String>().ok()).map(|s| s.to_string()).unwrap_or_default();
    if number.is_empty() { return; }

    let cookie = NEXT_COOKIE.fetch_add(1, Ordering::SeqCst);
    map.insert(cookie, ModemCall { bus_name: "org.ofono".into(), call_path: call_path.to_string() }).await;
    debug!("call-monitor: incoming oFono call (cookie={cookie})");
    let _ = daemon.push_incoming_call(cookie, number.clone(), number).await;
}

async fn handle_ofono_property_change(daemon: &CobbleDaemon, map: &CallMap, msg: &zbus::Message) {
    let call_path = match msg.header().path() {
        Some(p) => p.to_string(),
        None => return,
    };
    let body = msg.body();
    let Ok((prop_name, value)) = body.deserialize::<(String, OwnedValue)>() else { return };
    if prop_name != "State" { return; }
    let state = value.downcast_ref::<String>().ok().map(|s| s.to_string()).unwrap_or_default();

    if state == "disconnected" {
        if let Some(cookie) = map.remove_by_path(&call_path).await {
            debug!("call-monitor: oFono call {cookie} disconnected");
            let _ = daemon.push_call_end(cookie).await;
        }
    } else if state == "active" {
        // Call answered — notify watch, keep cookie for hangup.
        if let Some(cookie) = map.find_by_path(&call_path).await {
            debug!("call-monitor: oFono call {cookie} active");
            let _ = daemon.push_call_start(cookie).await;
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Watch → modem forwarding
// ═══════════════════════════════════════════════════════════════════════

async fn handle_watch_action(conn: &Connection, daemon: &CobbleDaemon, map: &CallMap, action: &str, cookie: u32) {
    let call = match action {
        "answer" => map.get(cookie).await,
        "hangup" => map.take(cookie).await,
        _ => None,
    };
    let Some(call) = call else {
        debug!("call-monitor: no call mapped for cookie={cookie}");
        return;
    };
    let is_ofono = call.bus_name == "org.ofono";
    let (method, iface) = match (action, is_ofono) {
        ("answer", true) => ("Answer", "org.ofono.VoiceCall"),
        ("answer", false) => ("Accept", "org.freedesktop.ModemManager1.Call"),
        ("hangup", true) => ("Hangup", "org.ofono.VoiceCall"),
        ("hangup", false) => ("Hangup", "org.freedesktop.ModemManager1.Call"),
        _ => return,
    };

    debug!("call-monitor: {action} → {}/{}", call.bus_name, call.call_path);
    let _ = conn.call_method(Some(call.bus_name.as_str()), call.call_path.as_str(), Some(iface), method, &()).await;

    // After a successful answer, transition the watch to the in-call screen.
    if action == "answer" {
        let _ = daemon.push_call_start(cookie).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// D-Bus helpers
// ═══════════════════════════════════════════════════════════════════════

async fn add_match(conn: &Connection, rule: &str) -> zbus::Result<()> {
    conn.call_method(Some("org.freedesktop.DBus"), "/org/freedesktop/DBus", Some("org.freedesktop.DBus"), "AddMatch", &(rule,)).await.map(|_| ())
}

async fn list_mm_objects(conn: &Connection, bus: &str, base_path: &str) -> zbus::Result<Vec<String>> {
    let reply = conn.call_method(Some(bus), base_path, Some("org.freedesktop.DBus.Introspectable"), "Introspect", &()).await?;
    let xml: String = reply.body().deserialize()?;
    let mut paths = Vec::new();
    #[allow(clippy::collapsible_if)]
    for line in xml.lines() {
        if let Some(name) = line.trim().strip_prefix("<node name=\"") {
            if let Some(end) = name.find('"') {
                let child = &name[..end];
                if child != "Voice" { paths.push(format!("{base_path}/{child}")); }
            }
        }
    }
    Ok(paths)
}

async fn get_mm_prop<T>(conn: &Connection, path: &str, iface: &str, prop: &str) -> Option<T>
where
    T: TryFrom<OwnedValue, Error = zbus::zvariant::Error>,
{
    let reply = conn.call_method(
        Some("org.freedesktop.ModemManager1"), path,
        Some("org.freedesktop.DBus.Properties"), "Get", &(iface, prop),
    ).await.ok()?;
    let body = reply.body();
    let v: zbus::zvariant::Value<'_> = body.deserialize().ok()?;
    OwnedValue::try_from(v).ok().and_then(|ov| T::try_from(ov).ok())
}

async fn get_ofono_prop<T>(conn: &Connection, path: &str, iface: &str, prop: &str) -> Option<T>
where
    T: TryFrom<OwnedValue, Error = zbus::zvariant::Error>,
{
    let reply = conn.call_method(Some("org.ofono"), path, Some(iface), "GetProperties", &()).await.ok()?;
    let props: HashMap<String, OwnedValue> = reply.body().deserialize().ok()?;
    props.get(prop).cloned().and_then(|v| T::try_from(v).ok())
}

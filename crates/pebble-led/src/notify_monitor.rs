//! Session-bus notification monitor → forwards desktop notifications to the watch.
//!
//! Desktop notifications are method calls to org.freedesktop.Notifications.Notify
//! on the *session* bus. We observe them (without intercepting) and forward
//! (app_name, summary, body) to the provided callback.
//!
//! Two strategies are tried in order:
//!   1. BecomeMonitor (org.freedesktop.DBus.Monitoring) — works with dbus-broker
//!      (NixOS, Arch, modern Fedora/Ubuntu) and dbus-daemon >= 1.9.10.
//!   2. eavesdrop=true AddMatch — legacy fallback for older dbus-daemon installs;
//!      rejected by dbus-broker.

use std::{collections::HashMap, sync::Arc};

use futures::StreamExt;
use tracing::{debug, info, trace, warn};
use zbus::{zvariant::OwnedValue, Connection, Message, MessageStream};

const NOTIFICATIONS_IFACE: &str = "org.freedesktop.Notifications";

pub struct NotificationMonitor {
    conn: Option<Connection>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl NotificationMonitor {
    pub fn new() -> Self {
        Self { conn: None, task: None }
    }

    pub async fn start(
        &mut self,
        on_notification: Arc<dyn Fn(String, String, String) + Send + Sync + 'static>,
    ) -> anyhow::Result<()> {
        let conn = zbus::connection::Builder::session()?.build().await?;

        let rule = format!(
            "type='method_call',interface='{NOTIFICATIONS_IFACE}',member='Notify'"
        );

        // BecomeMonitor is the modern approach: works with dbus-broker and avoids
        // the need for eavesdrop=true which dbus-broker rejects.
        let became_monitor = conn
            .call_method(
                Some("org.freedesktop.DBus"),
                "/org/freedesktop/DBus",
                Some("org.freedesktop.DBus.Monitoring"),
                "BecomeMonitor",
                &(vec![rule.clone()], 0u32),
            )
            .await
            .is_ok();

        if became_monitor {
            info!("notification monitor active (BecomeMonitor)");
        } else {
            // Fall back to eavesdrop AddMatch for older dbus-daemon installs.
            conn.call_method(
                Some("org.freedesktop.DBus"),
                "/org/freedesktop/DBus",
                Some("org.freedesktop.DBus"),
                "AddMatch",
                &format!("eavesdrop=true,{rule}"),
            )
            .await?;
            info!("notification monitor active (eavesdrop AddMatch)");
        }

        let conn_clone = conn.clone();

        let handle = tokio::spawn(async move {
            let mut stream = MessageStream::from(&conn_clone);
            while let Some(msg) = stream.next().await {
                let msg = match msg {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("notification monitor stream error: {e}");
                        continue;
                    }
                };
                handle_message(&msg, &on_notification);
            }
        });

        self.task = Some(handle);
        self.conn = Some(conn);
        Ok(())
    }

    pub async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.conn = None;
    }
}

impl Default for NotificationMonitor {
    fn default() -> Self {
        Self::new()
    }
}

fn handle_message(msg: &Message, cb: &Arc<dyn Fn(String, String, String) + Send + Sync>) {
    // Only handle Notify method calls.
    let header = msg.header();
    if header.message_type() != zbus::message::Type::MethodCall {
        return;
    }
    if header.interface().map(|i| i.as_str()) != Some(NOTIFICATIONS_IFACE) {
        return;
    }
    if header.member().map(|m| m.as_str()) != Some("Notify") {
        return;
    }

    // Notify signature: susssasa{sv}i
    // 0:app_name 1:replaces_id 2:app_icon 3:summary 4:body 5:actions 6:hints 7:expire_timeout
    let body = match msg.body().deserialize::<(
        String,                         // app_name
        u32,                            // replaces_id
        String,                         // app_icon
        String,                         // summary
        String,                         // body
        Vec<String>,                    // actions
        HashMap<String, OwnedValue>,    // hints
        i32,                            // expire_timeout
    )>() {
        Ok(b) => b,
        Err(e) => {
            trace!("could not parse Notify body: {e}");
            return;
        }
    };

    let (app_name, _, _, summary, notif_body, _, _, _) = body;
    if summary.is_empty() && notif_body.is_empty() {
        return; // progress-only / empty
    }
    debug!("captured notification: app={app_name:?} summary={summary:?}");
    cb(app_name, summary, notif_body);
}

//! Daemon entry point: `pebble-led <WATCH_ADDRESS>`.
//!
//! Acquires the session bus, exports the PebbleDaemon interface, requests the
//! well-known name (org.pebble_le.Daemon) so clients can find it and check
//! liveness, opens the watch connection, and runs until signalled.

use std::sync::Arc;

use clap::Parser;
use tokio::{signal, signal::unix::SignalKind, sync::mpsc};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

mod codec;
mod notification;
mod notify_monitor;
mod service;
mod supervisor;

use notify_monitor::NotificationMonitor;
use service::{run_signal_emitter, BUS_NAME, OBJECT_PATH, PebbleDaemon};
use supervisor::run_supervisor;

#[derive(Parser)]
#[command(name = "pebble-led", about = "Long-lived daemon owning the Pebble BLE connection.")]
struct Cli {
    /// Watch Bluetooth address, e.g. E6:94:0A:D4:D5:DC
    address: String,
    /// HCI adapter name
    #[arg(long, default_value = "hci0")]
    adapter: String,
    /// Enable verbose (TRACE-level) logging
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let filter = if cli.verbose {
        EnvFilter::new("trace")
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let (event_tx, event_rx) = mpsc::unbounded_channel();

    let daemon = PebbleDaemon::new(cli.address.clone(), cli.adapter.clone(), event_tx);

    // Build the session D-Bus connection.
    let conn = zbus::connection::Builder::session()?
        .name(BUS_NAME)?
        .serve_at(OBJECT_PATH, daemon.clone())?
        .build()
        .await?;

    info!("owning {BUS_NAME} at {OBJECT_PATH}");

    // Start the signal emission task.
    let conn_for_signals = conn.clone();
    let daemon_for_signals = daemon.clone();
    tokio::spawn(async move {
        run_signal_emitter(conn_for_signals, daemon_for_signals, event_rx).await;
    });

    // Start the desktop notification monitor.
    let mut notify_monitor = NotificationMonitor::new();
    let daemon_for_notif = daemon.clone();
    let notif_cb = Arc::new(move |app: String, summary: String, body: String| {
        daemon_for_notif.on_desktop_notification(app, summary, body);
    });
    if let Err(e) = notify_monitor.start(notif_cb).await {
        warn!("could not start notification monitor: {e}");
    }

    // Start the reconnect supervisor in the background.
    let daemon_for_super = daemon.clone();
    let address = cli.address.clone();
    let adapter = cli.adapter.clone();
    tokio::spawn(async move {
        run_supervisor(daemon_for_super, address, adapter).await;
    });

    // Run until SIGINT or SIGTERM.
    let mut sigterm = signal::unix::signal(SignalKind::terminate())?;
    tokio::select! {
        _ = signal::ctrl_c() => {},
        _ = sigterm.recv() => {},
    }

    info!("shutting down ...");
    daemon.set_stopping();
    notify_monitor.stop().await;

    Ok(())
}

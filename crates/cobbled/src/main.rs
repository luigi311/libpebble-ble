//! Daemon entry point.
//!
//! Reads `$XDG_CONFIG_HOME/cobbled/config.toml` (or the path given by
//! `--config`), acquires the session D-Bus, exports the CobbleDaemon interface,
//! requests the well-known name (org.cobble.Daemon), opens the watch
//! connection, and runs until signalled.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clap::Parser;
use tokio::{signal, signal::unix::SignalKind, sync::mpsc};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

mod codec;
mod config;
mod config_watcher;
mod db;
mod notification;
mod notify_monitor;
mod service;
mod supervisor;

use db::HealthDb;
use notify_monitor::NotificationMonitor;
use service::{run_signal_emitter, BUS_NAME, OBJECT_PATH, CobbleDaemon};
use supervisor::run_supervisor;

#[derive(Parser)]
#[command(name = "cobbled", about = "Long-lived daemon owning the Pebble BLE connection.")]
struct Cli {
    /// Path to config file (default: $XDG_CONFIG_HOME/cobbled/config.toml)
    #[arg(long)]
    config: Option<PathBuf>,
    /// Increase log verbosity: -v = debug, -vv = trace. Overrides config/RUST_LOG.
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

fn default_db_path() -> anyhow::Result<PathBuf> {
    let base = if let Some(p) = std::env::var_os("XDG_DATA_HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p)
    } else if let Some(p) = std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p).join(".local/share")
    } else {
        anyhow::bail!(
            "neither XDG_DATA_HOME nor HOME is set; \
             set db in config to specify the health database path explicitly"
        );
    };
    Ok(base.join("cobbled/health.db"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config_path = match cli.config {
        Some(p) => p,
        None => config::default_config_path()?,
    };
    let cfg = config::load(&config_path)?;

    // Verbosity: CLI -v count wins; legacy `verbose = true` in config maps to
    // the deepest level (trace). Our crates follow the chosen level while noisy
    // dependencies (zbus, bluer) are kept one notch quieter so shared logs stay
    // readable. Level 0 still honours RUST_LOG for surgical control.
    let level = if cli.verbose > 0 {
        cli.verbose
    } else if cfg.verbose {
        2
    } else {
        0
    };
    let filter = match level {
        0 => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        1 => EnvFilter::new("info,cobbled=debug,libpebble_ble=debug"),
        _ => EnvFilter::new("debug,cobbled=trace,libpebble_ble=trace"),
    };
    tracing_subscriber::fmt().with_env_filter(filter).init();

    info!("loaded config from {}", config_path.display());

    let db_path = match cfg.db {
        Some(p) => p,
        None => default_db_path()?,
    };
    let health_db: Option<Arc<Mutex<HealthDb>>> = match HealthDb::open(&db_path) {
        Ok(db) => {
            info!("health DB opened at {}", db_path.display());
            Some(Arc::new(Mutex::new(db)))
        }
        Err(e) => {
            warn!("could not open health DB at {}: {e}", db_path.display());
            None
        }
    };

    let (event_tx, event_rx) = mpsc::unbounded_channel();

    let daemon = CobbleDaemon::new(cfg.address.clone(), cfg.adapter.clone(), config_path.clone(), event_tx, health_db.clone());

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
        run_signal_emitter(conn_for_signals, daemon_for_signals, event_rx, health_db).await;
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
    tokio::spawn(async move {
        run_supervisor(daemon_for_super).await;
    });

    // Watch the config file for external changes (manual edits, GUI saves)
    // and auto-reload whenever it is written.
    config_watcher::watch_config(config_path.clone(), daemon.clone());

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

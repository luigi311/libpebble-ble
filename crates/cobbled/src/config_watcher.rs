//! Watches the config TOML file for changes and auto-reloads.
//!
//! Uses the `notify` crate for efficient filesystem event monitoring (inotify
//! on Linux).  Events are debounced so that an atomic-write (create + rename)
//! or multi-write editor sequence triggers only one reload.

use std::path::PathBuf;
use std::time::Duration;

use notify::{EventKind, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::service::CobbleDaemon;

/// Spawn a file-system watcher on `config_path` and call
/// [`CobbleDaemon::reload_config`] whenever the file changes.
///
/// Runs until the channel is dropped (the returned handle is the only strong
/// reference to the watcher thread).
pub fn watch_config(config_path: PathBuf, daemon: CobbleDaemon) {
    let (tx, mut rx) = mpsc::unbounded_channel::<notify::Result<notify::Event>>();

    // The notify watcher must live on a non-async thread because its internal
    // event loop uses blocking I/O (inotify reads).
    let cfg = config_path.clone();
    std::thread::spawn(move || {
        let mut watcher = match notify::recommended_watcher(move |res| {
            let _ = tx.send(res);
        }) {
            Ok(w) => w,
            Err(e) => {
                warn!("cannot create config file watcher: {e}");
                return;
            }
        };

        if cfg.exists() {
            if let Err(e) = watcher.watch(&cfg, RecursiveMode::NonRecursive) {
                warn!("cannot watch config file {}: {e}", cfg.display());
            }
        } else if let Some(parent) = cfg.parent() {
            // File doesn't exist yet — watch the parent directory so we
            // notice when it is created (e.g. by the GUI's first save).
            if let Err(e) = watcher.watch(parent, RecursiveMode::NonRecursive) {
                warn!("cannot watch config dir {}: {e}", parent.display());
            }
        }
        debug!("config file watcher started for {}", cfg.display());

        // Park the thread — the watcher runs its own event loop internally.
        std::thread::park();
    });

    // Process events on the tokio runtime with debouncing.
    tokio::spawn(async move {
        loop {
            // Wait for the first relevant event.
            let event = loop {
                match rx.recv().await {
                    Some(Ok(e))
                        if matches!(e.kind, EventKind::Modify(_) | EventKind::Create(_)) =>
                    {
                        break e;
                    }
                    Some(Err(e)) => {
                        debug!("config watcher error: {e}");
                    }
                    None => return, // channel closed
                    _ => {} // ignore access / remove / other events
                }
            };

            debug!(
                "config file change detected ({:?}); debouncing ...",
                event.kind
            );

            // Debounce: keep draining events for 200ms.  If another relevant
            // event arrives, reset the timer so we only reload once per burst.
            loop {
                match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
                {
                    // Another relevant event → reset debounce timer.
                    Ok(Some(Ok(e)))
                        if matches!(
                            e.kind,
                            EventKind::Modify(_) | EventKind::Create(_)
                        ) =>
                    {
                        debug!("additional config event ({:?}); resetting debounce", e.kind);
                        continue;
                    }
                    // Irrelevant event (access, remove, error, …) → ignore and keep waiting.
                    Ok(Some(_)) => continue,
                    // Timeout or channel closed → time to reload.
                    _ => break,
                }
            }

            debug!("debounce complete; reloading config");
            if let Err(e) = daemon.reload_config().await {
                warn!("auto-reload config failed: {e}");
            }
        }
    });
}

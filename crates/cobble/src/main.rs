mod config;
mod db;

use std::cell::Cell;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use cobble_client::{CobbleClient, StatusEvent};
use slint::{ModelRc, VecModel};
use tracing::warn;

slint::include_modules!();

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config_path = config::default_config_path().unwrap_or_else(|_| {
        PathBuf::from(std::env::var("HOME").unwrap_or_default())
            .join(".config/cobbled/config.toml")
    });
    let db_path = config::default_db_path().unwrap_or_else(|_| {
        PathBuf::from(std::env::var("HOME").unwrap_or_default())
            .join(".local/share/cobbled/cobbled.db")
    });

    let window = AppWindow::new()?;

    let cfg = config::load(&config_path).unwrap_or_default();
    window.set_cfg_address(cfg.address.clone().into());
    window.set_cfg_adapter(cfg.adapter.clone().into());
    window.set_cfg_verbose(cfg.verbose);
    window.set_cfg_db(cfg.db.clone().unwrap_or_default().into());

    let effective_db_path = cfg
        .db
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| db_path.clone());

    // Derive the watch timezone offset from synced data so all times/labels
    // render in the watch's local zone, independent of the host's system tz.
    if let Ok(conn) = db::open(&effective_db_path) {
        db::set_watch_offset(db::watch_tz_offset(&conn));
    }

    // ── Shared filter state (main-thread only) ───────────────────────────────
    let period_workout  = Rc::new(Cell::new(1i32));
    let offset_workout  = Rc::new(Cell::new(0i32));
    let bar_range_w     = Rc::new(Cell::new((-1i64, -1i64)));
    let period_sleep    = Rc::new(Cell::new(1i32));
    let offset_sleep    = Rc::new(Cell::new(0i32));

    // ── Set initial period labels ────────────────────────────────────────────
    window.set_workout_period_label(db::period_label(1, 0).into());
    window.set_workout_can_forward(false);
    window.set_sleep_period_label(db::period_label(1, 0).into());
    window.set_sleep_can_forward(false);

    // ── Initial data load ────────────────────────────────────────────────────
    reload_workout_chart(&window, &effective_db_path, 1, 0);
    reload_workout_sessions(&window, &effective_db_path, 1, 0, (-1, -1));
    reload_sleep_chart(&window, &effective_db_path, 1, 0);
    reload_sleep_strip(&window, &effective_db_path, 1, 0);

    // ── Background tokio runtime ─────────────────────────────────────────────
    // Enter the runtime context on the main thread. zbus (via cobble-client)
    // needs an ambient Tokio runtime when it creates its connection/executor
    // tasks; without this guard those code paths panic with "there is no reactor
    // running". Load-bearing — do not remove.
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let _rt_guard = rt.enter();

    {
        let weak = window.as_weak();
        rt.spawn(async move {
            loop {
                // Stream daemon/watch status via D-Bus signals (no polling).
                if let Ok(client) = CobbleClient::new().await {
                    let weak2 = weak.clone();
                    let _ = client
                        .watch_status(move |ev| {
                            let weak3 = weak2.clone();
                            slint::invoke_from_event_loop(move || {
                                if let Some(w) = weak3.upgrade() {
                                    apply_status(&w, ev);
                                }
                            })
                            .ok();
                        })
                        .await;
                }
                // watch_status only returns if the bus connection drops; retry.
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }

    // ── Refresh ──────────────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2  = effective_db_path.clone();
        let pw = period_workout.clone(); let ow = offset_workout.clone(); let brw = bar_range_w.clone();
        let ps = period_sleep.clone();  let os = offset_sleep.clone();
        window.on_refresh_data(move || {
            if let Ok(conn) = db::open(&db2) {
                db::set_watch_offset(db::watch_tz_offset(&conn));
            }
            if let Some(w) = weak.upgrade() {
                reload_workout_chart(&w, &db2, pw.get(), ow.get());
                reload_workout_sessions(&w, &db2, pw.get(), ow.get(), brw.get());
                reload_sleep_chart(&w, &db2, ps.get(), os.get());
                reload_sleep_strip(&w, &db2, ps.get(), os.get());
            }
        });
    }

    // ── Workout: period changed ───────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let pw = period_workout.clone(); let ow = offset_workout.clone(); let brw = bar_range_w.clone();
        window.on_workout_period_changed(move |p| {
            pw.set(p); ow.set(0); brw.set((-1, -1));
            if let Some(w) = weak.upgrade() {
                update_workout_nav(&w, p, 0);
                reload_workout_chart(&w, &db2, p, 0);
                reload_workout_sessions(&w, &db2, p, 0, (-1, -1));
            }
        });
    }

    // ── Workout: go back ────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let pw = period_workout.clone(); let ow = offset_workout.clone(); let brw = bar_range_w.clone();
        window.on_workout_go_back(move || {
            let new_off = ow.get() + 1;
            ow.set(new_off); brw.set((-1, -1));
            let p = pw.get();
            if let Some(w) = weak.upgrade() {
                update_workout_nav(&w, p, new_off);
                reload_workout_chart(&w, &db2, p, new_off);
                reload_workout_sessions(&w, &db2, p, new_off, (-1, -1));
            }
        });
    }

    // ── Workout: go forward ──────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let pw = period_workout.clone(); let ow = offset_workout.clone(); let brw = bar_range_w.clone();
        window.on_workout_go_forward(move || {
            let new_off = (ow.get() - 1).max(0);
            ow.set(new_off); brw.set((-1, -1));
            let p = pw.get();
            if let Some(w) = weak.upgrade() {
                update_workout_nav(&w, p, new_off);
                reload_workout_chart(&w, &db2, p, new_off);
                reload_workout_sessions(&w, &db2, p, new_off, (-1, -1));
            }
        });
    }

    // ── Workout: bar tapped ──────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let pw = period_workout.clone(); let ow = offset_workout.clone(); let brw = bar_range_w.clone();
        window.on_bar_tapped(move |s, e| {
            let range = if s < 0 { (-1i64, -1i64) } else { (s as i64, e as i64) };
            brw.set(range);
            if let Some(w) = weak.upgrade() {
                reload_workout_sessions(&w, &db2, pw.get(), ow.get(), range);
            }
        });
    }

    // ── Sleep: period changed ────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let ps = period_sleep.clone(); let os = offset_sleep.clone();
        window.on_sleep_period_changed(move |p| {
            ps.set(p); os.set(0);
            if let Some(w) = weak.upgrade() {
                update_sleep_nav(&w, p, 0);
                reload_sleep_chart(&w, &db2, p, 0);
                reload_sleep_strip(&w, &db2, p, 0);
            }
        });
    }

    // ── Sleep: go back ───────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let ps = period_sleep.clone(); let os = offset_sleep.clone();
        window.on_sleep_go_back(move || {
            let new_off = os.get() + 1;
            os.set(new_off);
            let p = ps.get();
            if let Some(w) = weak.upgrade() {
                update_sleep_nav(&w, p, new_off);
                reload_sleep_chart(&w, &db2, p, new_off);
                reload_sleep_strip(&w, &db2, p, new_off);
            }
        });
    }

    // ── Sleep: go forward ────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let ps = period_sleep.clone(); let os = offset_sleep.clone();
        window.on_sleep_go_forward(move || {
            let new_off = (os.get() - 1).max(0);
            os.set(new_off);
            let p = ps.get();
            if let Some(w) = weak.upgrade() {
                update_sleep_nav(&w, p, new_off);
                reload_sleep_chart(&w, &db2, p, new_off);
                reload_sleep_strip(&w, &db2, p, new_off);
            }
        });
    }

    // ── Save config ──────────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let cfg_path2 = config_path.clone();
        let rt_handle = rt.handle().clone();
        window.on_save_config(move || {
            let Some(w) = weak.upgrade() else { return };
            let new_cfg = config::Config {
                address: w.get_cfg_address().to_string(),
                adapter: w.get_cfg_adapter().to_string(),
                verbose: w.get_cfg_verbose(),
                db: {
                    let s = w.get_cfg_db().to_string();
                    if s.is_empty() { None } else { Some(s) }
                },
            };
            match config::save(&cfg_path2, &new_cfg) {
                Err(e) => { w.set_save_status(format!("Error: {e}").into()); }
                Ok(()) => {
                    w.set_save_status("Saved.".into());
                    let weak2 = weak.clone();
                    rt_handle.spawn(async move {
                        match CobbleClient::new().await {
                            Err(e) => warn!("ReloadConfig: {e}"),
                            Ok(client) => {
                                if !client.is_running().await {
                                    warn!("ReloadConfig: daemon is not running");
                                } else if let Err(e) = client.reload_config().await {
                                    warn!("ReloadConfig: {e}");
                                }
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        slint::invoke_from_event_loop(move || {
                            if let Some(ww) = weak2.upgrade() { ww.set_save_status("".into()); }
                        }).ok();
                    });
                }
            }
        });
    }

    // ── Scan for Pebble watches ─────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let rt_handle = rt.handle().clone();
        window.on_scan_watches(move || {
            let weak_for_scan = weak.clone();
            let weak_for_ui = weak.clone();
            let rt = rt_handle.clone();
            // Show scanning state immediately on the UI thread.
            slint::invoke_from_event_loop(move || {
                if let Some(w) = weak_for_ui.upgrade() {
                    w.set_scan_in_progress(true);
                }
            }).ok();
            rt.spawn(async move {
                let results = match CobbleClient::new().await {
                    Err(e) => {
                        warn!("Scan: {e}");
                        Vec::new()
                    }
                    Ok(client) => match client.scan(5.0).await {
                        Err(e) => {
                            warn!("Scan: {e}");
                            Vec::new()
                        }
                        Ok(results) => results,
                    },
                };
                slint::invoke_from_event_loop(move || {
                    if let Some(w) = weak_for_scan.upgrade() {
                        let model: VecModel<WatchDevice> = VecModel::default();
                        for (addr, name) in results {
                            model.push(WatchDevice {
                                address: addr.into(),
                                name: name.into(),
                            });
                        }
                        w.set_scan_results(ModelRc::new(model));
                        w.set_scan_in_progress(false);
                    }
                }).ok();
            });
        });
    }

    // ── Device: manual refresh ───────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let rt_handle = rt.handle().clone();
        window.on_refresh_device(move || {
            let weak2 = weak.clone();
            rt_handle.spawn(async move {
                let Ok(client) = CobbleClient::new().await else { return };
                if !client.connected().await {
                    return;
                }
                let info = client.get_watch_info().await.ok();
                let battery = client.battery_level().await.unwrap_or(-1);
                slint::invoke_from_event_loop(move || {
                    if let Some(w) = weak2.upgrade() {
                        w.set_battery_level(battery as i32);
                        if let Some(info) = info {
                            apply_status(&w, StatusEvent::WatchInfo(info));
                        }
                    }
                })
                .ok();
            });
        });
    }

    // ── Device actions (Settings ▸ Device Actions) ───────────────────────────
    {
        let rt_handle = rt.handle().clone();
        let w = window.as_weak();
        window.on_reboot_watch({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || spawn_action(&rt, w.clone(), |c| async move { c.reboot_watch().await })
        });
        window.on_reset_into_recovery({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || spawn_action(&rt, w.clone(), |c| async move { c.reset_into_recovery().await })
        });
        window.on_create_core_dump({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || spawn_action(&rt, w.clone(), |c| async move { c.create_core_dump().await })
        });
        window.on_forget_watch({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || spawn_action(&rt, w.clone(), |c| async move { c.forget().await })
        });
        window.on_factory_reset({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || spawn_action(&rt, w.clone(), |c| async move { c.factory_reset(true).await })
        });
    }

    window.run()?;
    drop(rt);
    Ok(())
}

/// Run a device action on the runtime; the UI sets an optimistic status before
/// calling, so only failures are reported back.
fn spawn_action<F, Fut>(rt: &tokio::runtime::Handle, weak: slint::Weak<AppWindow>, f: F)
where
    F: FnOnce(CobbleClient) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = cobble_client::Result<()>> + Send + 'static,
{
    rt.spawn(async move {
        if let Err(e) = async { f(CobbleClient::new().await?).await }.await {
            let msg = format!("Error: {e}");
            slint::invoke_from_event_loop(move || {
                if let Some(w) = weak.upgrade() {
                    w.set_action_error(true);
                    w.set_action_status(msg.into());
                }
            })
            .ok();
        }
    });
}

/// Apply a status event to the window's properties (runs on the UI thread).
fn apply_status(w: &AppWindow, ev: StatusEvent) {
    match ev {
        StatusEvent::DaemonRunning(r) => w.set_daemon_running(r),
        StatusEvent::Connected(c) => {
            w.set_watch_connected(c);
            if !c {
                w.set_battery_level(-1);
                clear_watch_info(w);
            }
        }
        StatusEvent::Battery(b) => w.set_battery_level(b as i32),
        StatusEvent::WatchInfo(info) => {
            w.set_wi_model(info.model.into());
            w.set_wi_firmware(info.firmware_version.into());
            w.set_wi_color(info.color.into());
            w.set_wi_board(info.board.into());
            w.set_wi_serial(info.serial.into());
            w.set_wi_bt(info.bt_address.into());
            w.set_wi_language(info.language.into());
        }
    }
}

fn clear_watch_info(w: &AppWindow) {
    w.set_wi_model("".into());
    w.set_wi_firmware("".into());
    w.set_wi_color("".into());
    w.set_wi_board("".into());
    w.set_wi_serial("".into());
    w.set_wi_bt("".into());
    w.set_wi_language("".into());
}

// ─── Navigation label helpers ─────────────────────────────────────────────────

fn update_workout_nav(w: &AppWindow, period: i32, offset: i32) {
    w.set_workout_period_label(db::period_label(period, offset).into());
    w.set_workout_can_forward(offset > 0);
}

fn update_sleep_nav(w: &AppWindow, period: i32, offset: i32) {
    w.set_sleep_period_label(db::period_label(period, offset).into());
    w.set_sleep_can_forward(offset > 0);
}

// ─── Workout helpers ──────────────────────────────────────────────────────────

fn reload_workout_chart(window: &AppWindow, db_path: &PathBuf, period: i32, offset: i32) {
    match db::open(db_path) {
        Err(e) => warn!("cannot open DB: {e}"),
        Ok(conn) => match db::load_daily_steps(&conn, period, offset) {
            Err(e) => warn!("load daily steps failed: {e}"),
            Ok(steps) => {
                window.set_today_steps_label(db::compute_steps_summary(&steps, period).into());
                let slint_steps: Vec<DaySteps> = steps.into_iter().map(|s| DaySteps {
                    label: s.label.into(),
                    steps_label: s.steps_label.into(),
                    fraction: s.fraction,
                    bar_start: s.bar_start as i32,
                    bar_end: s.bar_end as i32,
                }).collect();
                window.set_daily_steps(ModelRc::new(VecModel::from(slint_steps)));
            }
        },
    }
}

fn reload_workout_sessions(
    window: &AppWindow,
    db_path: &PathBuf,
    period: i32,
    offset: i32,
    bar_range: (i64, i64),
) {
    let (start, end) = if bar_range.0 < 0 {
        db::period_range_offset(period, offset)
    } else {
        bar_range
    };
    match db::open(db_path) {
        Err(e) => warn!("cannot open DB: {e}"),
        Ok(conn) => match db::load_sessions_filtered(&conn, 1, start, end) {
            Err(e) => warn!("load workout sessions failed: {e}"),
            Ok(sessions) => {
                window.set_sessions(ModelRc::new(VecModel::from(to_slint_sessions(sessions))));
            }
        },
    }
}

// ─── Sleep helpers ────────────────────────────────────────────────────────────

fn reload_sleep_chart(window: &AppWindow, db_path: &PathBuf, period: i32, offset: i32) {
    match db::open(db_path) {
        Err(e) => warn!("cannot open DB: {e}"),
        Ok(conn) => match db::load_sleep_bars(&conn, period, offset) {
            Err(e) => warn!("load sleep bars failed: {e}"),
            Ok(bars) => {
                window.set_sleep_label(db::compute_sleep_summary(&bars, period).into());
                let slint_bars: Vec<SleepBar> = bars.into_iter().map(|b| SleepBar {
                    label: b.label.into(),
                    bar_start: b.bar_start as i32,
                    bar_end: b.bar_end as i32,
                    light_fraction: b.light_fraction,
                    deep_fraction: b.deep_fraction,
                    total_label: b.total_label.into(),
                    deep_label: b.deep_label.into(),
                }).collect();
                window.set_sleep_bars(ModelRc::new(VecModel::from(slint_bars)));
            }
        },
    }
}

fn reload_sleep_strip(window: &AppWindow, db_path: &PathBuf, period: i32, offset: i32) {
    match db::open(db_path) {
        Err(e) => warn!("cannot open DB: {e}"),
        Ok(conn) => match db::load_sleep_nights(&conn, period, offset) {
            Err(e) => warn!("load sleep nights failed: {e}"),
            Ok(nights) => {
                window.set_sleep_nights(ModelRc::new(VecModel::from(to_slint_nights(nights))));
            }
        },
    }
}

// ─── Conversion ───────────────────────────────────────────────────────────────

fn to_slint_sessions(sessions: Vec<db::HealthSessionData>) -> Vec<HealthSession> {
    sessions.into_iter().map(|s| HealthSession {
        type_name: s.type_name.into(),
        start_label: s.start_label.into(),
        duration_label: s.duration_label.into(),
        has_metrics: s.has_metrics,
        metrics_label: s.metrics_label.into(),
    }).collect()
}

fn to_slint_nights(nights: Vec<db::SleepNightData>) -> Vec<SleepNight> {
    nights.into_iter().map(|n| {
        let segs: Vec<SleepSegment> = n.segments.into_iter().map(|s| SleepSegment {
            start_frac: s.start_frac,
            width_frac: s.width_frac,
            is_deep: s.is_deep,
        }).collect();
        SleepNight {
            label: n.label.into(),
            duration_label: n.duration_label.into(),
            bar_start: n.bar_start as i32,
            segments: ModelRc::new(VecModel::from(segs)),
        }
    }).collect()
}

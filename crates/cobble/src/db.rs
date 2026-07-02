use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::Context;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use rusqlite::{params, Connection};

/// Watch UTC offset in seconds (local = UTC + offset), set from synced data so
/// the GUI renders the watch's local time regardless of the host system tz.
static WATCH_OFFSET_SECS: AtomicI64 = AtomicI64::new(0);

/// Set the watch timezone offset used for all local-time grouping and labels.
pub fn set_watch_offset(secs: i64) {
    WATCH_OFFSET_SECS.store(secs, Ordering::Relaxed);
}

fn watch_offset() -> i64 {
    WATCH_OFFSET_SECS.load(Ordering::Relaxed)
}

/// Read the watch's current UTC offset from the most recent health record across
/// both tables (whichever has the newer `start_ts`), falling back to UTC (0)
/// when there's no data yet.
pub fn watch_tz_offset(conn: &Connection) -> i64 {
    let latest = |table: &str| -> Option<(i64, i64)> {
        conn.query_row(
            &format!("SELECT start_ts, utc_offset FROM {table} ORDER BY start_ts DESC LIMIT 1"),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .ok()
    };
    match (
        latest("health_activity_minutes"),
        latest("health_activity_sessions"),
    ) {
        (Some((mt, mo)), Some((st, so))) => {
            if mt >= st {
                mo
            } else {
                so
            }
        }
        (Some((_, mo)), None) => mo,
        (None, Some((_, so))) => so,
        (None, None) => 0,
    }
}

/// Current date in the watch's timezone.
fn watch_today() -> NaiveDate {
    DateTime::from_timestamp(Utc::now().timestamp() + watch_offset(), 0)
        .map(|dt| dt.date_naive())
        .unwrap_or_default()
}

pub struct DayStepsData {
    pub label: String,
    pub steps_label: String,
    pub steps_raw: i64,   // raw count for summary computation
    pub fraction: f32,
    pub bar_start: i64,
    pub bar_end: i64,
}

pub struct SleepBarData {
    pub label: String,
    pub bar_start: i64,
    pub bar_end: i64,
    pub light_fraction: f32,
    pub deep_fraction: f32,
    pub light_secs: i64,  // raw durations for summary computation
    pub deep_secs: i64,
    pub total_label: String,
    pub deep_label: String,
}

pub struct HealthSessionData {
    pub type_name: String,
    pub start_label: String,
    pub duration_label: String,
    pub has_metrics: bool,
    pub metrics_label: String,
}

pub struct SleepSegmentData {
    pub start_frac: f32,
    pub width_frac: f32,
    pub is_deep: bool,
}

pub struct SleepNightData {
    pub label: String,
    pub duration_label: String,
    pub bar_start: i64,
    pub segments: Vec<SleepSegmentData>,
}


pub fn open(path: &Path) -> anyhow::Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("open app DB at {}", path.display()))?;
    // The daemon writes this DB in WAL mode; without a busy_timeout our reads
    // would fail instantly with SQLITE_BUSY during its writes / WAL checkpoints.
    conn.execute_batch(
        "PRAGMA busy_timeout=5000;
         PRAGMA foreign_keys=ON;",
    )?;
    Ok(conn)
}

// ─── Time helpers ─────────────────────────────────────────────────────────────

/// Real UTC timestamp of a watch-local wall-clock time (local = UTC + offset).
fn local_ts(date: NaiveDate, h: u32, m: u32, s: u32) -> i64 {
    date.and_hms_opt(h, m, s)
        .map(|naive| naive.and_utc().timestamp() - watch_offset())
        .unwrap_or(0)
}

/// Convert (year, month) + a backward month offset into the target (year, month).
fn offset_ym(base_year: i32, base_month: u32, offset: i32) -> (i32, u32) {
    let total = base_year * 12 + base_month as i32 - 1 - offset;
    (total.div_euclid(12), (total.rem_euclid(12) + 1) as u32)
}

// ─── Period range + label ─────────────────────────────────────────────────────

/// Compute [start, end] Unix timestamps for `period` shifted back by `offset` units.
/// period 0=Day (offset in days), 1=Week (offset in weeks), 2=Month (offset in months).
pub fn period_range_offset(period: i32, offset: i32) -> (i64, i64) {
    let today = watch_today();
    match period {
        0 => {
            let date = today - chrono::Duration::days(offset as i64);
            (local_ts(date, 0, 0, 0), local_ts(date, 23, 59, 59))
        }
        2 => {
            let (year, month) = offset_ym(today.year(), today.month(), offset);
            let first = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
            let last = if month == 12 {
                NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
            } else {
                NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
            } - chrono::Duration::days(1);
            (local_ts(first, 0, 0, 0), local_ts(last, 23, 59, 59))
        }
        _ => {
            let end = today - chrono::Duration::days(offset as i64 * 7);
            let start = end - chrono::Duration::days(6);
            (local_ts(start, 0, 0, 0), local_ts(end, 23, 59, 59))
        }
    }
}

/// Human-readable label for the navigated period (shown between the arrows).
pub fn period_label(period: i32, offset: i32) -> String {
    let today = watch_today();
    match period {
        0 => match offset {
            0 => "Today".to_string(),
            1 => "Yesterday".to_string(),
            n => (today - chrono::Duration::days(n as i64))
                .format("%a, %b %-d")
                .to_string(),
        },
        2 => {
            if offset == 0 {
                "This Month".to_string()
            } else {
                let (year, month) = offset_ym(today.year(), today.month(), offset);
                NaiveDate::from_ymd_opt(year, month, 1)
                    .unwrap()
                    .format("%B %Y")
                    .to_string()
            }
        }
        _ => match offset {
            0 => "This Week".to_string(),
            1 => "Last Week".to_string(),
            n => {
                let end = today - chrono::Duration::days(n as i64 * 7);
                let start = end - chrono::Duration::days(6);
                if start.year() == end.year() {
                    format!("{} \u{2013} {}", start.format("%b %-d"), end.format("%b %-d"))
                } else {
                    format!(
                        "{} \u{2013} {}",
                        start.format("%b %-d, %Y"),
                        end.format("%b %-d, %Y")
                    )
                }
            }
        },
    }
}

// ─── Step chart data ─────────────────────────────────────────────────────────

pub fn load_daily_steps(
    conn: &Connection,
    period: i32,
    offset: i32,
) -> anyhow::Result<Vec<DayStepsData>> {
    let (range_start, range_end) = period_range_offset(period, offset);
    match period {
        0 => load_steps_day(conn, range_start, range_end),
        2 => load_steps_by_date(conn, range_start, range_end, true),
        _ => load_steps_by_date(conn, range_start, range_end, false),
    }
}

fn load_steps_day(conn: &Connection, start: i64, end: i64) -> anyhow::Result<Vec<DayStepsData>> {
    let mut stmt = conn.prepare(
        // start_ts is UTC; local wall-clock = start_ts + utc_offset. Group by
        // local hour, but filter on the raw UTC start_ts against the UTC range
        // bounds (local_ts already returns real timestamps).
        "SELECT strftime('%H', start_ts + utc_offset, 'unixepoch') AS hour, SUM(steps) AS total
         FROM health_activity_minutes
         WHERE start_ts >= ?1 AND start_ts <= ?2
         GROUP BY hour ORDER BY hour ASC",
    )?;
    let rows: Vec<(String, i64)> = stmt
        .query_map(params![start, end], |r| Ok((r.get(0)?, r.get(1)?)))?
        .filter_map(|r| r.ok())
        .collect();

    let max_steps = rows.iter().map(|r| r.1).max().unwrap_or(1).max(1);
    let day_date = DateTime::from_timestamp(start + watch_offset(), 0)
        .map(|dt| dt.date_naive())
        .unwrap_or_else(watch_today);

    Ok(rows.into_iter().map(|(hour_str, total)| {
        let h: u32 = hour_str.parse().unwrap_or(0);
        DayStepsData {
            label: format!("{}", h),
            steps_label: format_number(total),
            steps_raw: total,
            fraction: total as f32 / max_steps as f32,
            bar_start: local_ts(day_date, h, 0, 0),
            bar_end: local_ts(day_date, h, 59, 59),
        }
    }).collect())
}

fn load_steps_by_date(
    conn: &Connection,
    start: i64,
    end: i64,
    month_fmt: bool,
) -> anyhow::Result<Vec<DayStepsData>> {
    let mut stmt = conn.prepare(
        "SELECT date(start_ts + utc_offset, 'unixepoch') AS day, SUM(steps) AS total
         FROM health_activity_minutes
         WHERE start_ts >= ?1 AND start_ts <= ?2
         GROUP BY day ORDER BY day ASC",
    )?;
    let totals: std::collections::HashMap<String, i64> = stmt
        .query_map(params![start, end], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })?
        .filter_map(|r| r.ok())
        .collect();

    // Backfill every day in the range so inactive days show as zero bars and
    // the avg/day in compute_steps_summary divides by the full period length.
    let start_date = DateTime::from_timestamp(start + watch_offset(), 0).map(|dt| dt.date_naive());
    let end_date = DateTime::from_timestamp(end + watch_offset(), 0).map(|dt| dt.date_naive());
    let mut days: Vec<(NaiveDate, i64)> = Vec::new();
    if let (Some(start_date), Some(end_date)) = (start_date, end_date) {
        let mut d = start_date;
        loop {
            let key = d.format("%Y-%m-%d").to_string();
            days.push((d, totals.get(&key).copied().unwrap_or(0)));
            if d >= end_date {
                break;
            }
            match d.succ_opt() {
                Some(next) => d = next,
                None => break,
            }
        }
    }

    let max_steps = days.iter().map(|(_, t)| *t).max().unwrap_or(1).max(1);

    Ok(days.into_iter().map(|(d, total)| {
        let label = if month_fmt { d.format("%-d").to_string() } else { d.format("%a").to_string() };
        DayStepsData {
            label,
            steps_label: format_number(total),
            steps_raw: total,
            fraction: total as f32 / max_steps as f32,
            bar_start: local_ts(d, 0, 0, 0),
            bar_end: local_ts(d, 23, 59, 59),
        }
    }).collect())
}

/// Summary label for the steps chart header.
/// Day: total steps. Week/Month: average steps per day.
pub fn compute_steps_summary(bars: &[DayStepsData], period: i32) -> String {
    if bars.is_empty() {
        return "0 steps".to_string();
    }
    let total: i64 = bars.iter().map(|b| b.steps_raw).sum();
    if period == 0 {
        format!("{} steps", format_number(total))
    } else {
        let avg = total / bars.len() as i64;
        format!("avg {} / day", format_number(avg))
    }
}

// ─── Sleep chart data ─────────────────────────────────────────────────────────

pub fn load_sleep_bars(
    conn: &Connection,
    period: i32,
    offset: i32,
) -> anyhow::Result<Vec<SleepBarData>> {
    let (range_start, range_end) = period_range_offset(period, offset);
    let label_fmt = period == 2;

    // Night key uses local wall-clock (start_ts + utc_offset), shifted -12h so a
    // post-midnight stretch counts toward the previous evening. Deep-sleep rows
    // (types 2/4) are sub-sessions *inside* the sleep/nap containers (types 1/3),
    // so the container total already includes deep — light = total - deep.
    let mut stmt = conn.prepare(
        "SELECT date(start_ts + utc_offset - 43200, 'unixepoch') AS night,
                SUM(CASE WHEN session_type IN (1, 3) THEN duration_secs ELSE 0 END) AS total_secs,
                SUM(CASE WHEN session_type IN (2, 4) THEN duration_secs ELSE 0 END) AS deep_secs
         FROM health_activity_sessions
         -- Bucketing shifts by -12h (date(start_ts+utc_offset-43200)); shift the
         -- UTC window by +12h to match, so a night's post-midnight sessions at a
         -- range boundary aren't dropped.
         WHERE start_ts >= ?1 + 43200 AND start_ts <= ?2 + 43200
           AND session_type <= 4
         GROUP BY night
         ORDER BY night ASC",
    )?;

    struct Row { night: String, total: i64, deep: i64 }
    let rows: Vec<Row> = stmt
        .query_map(params![range_start, range_end], |r| {
            Ok(Row { night: r.get(0)?, total: r.get(1)?, deep: r.get(2)? })
        })?
        .filter_map(|r| r.ok())
        .collect();

    let max_total = rows.iter().map(|r| r.total).max().unwrap_or(1).max(1);

    Ok(rows.into_iter().map(|r| {
        let nd = r.night.parse::<NaiveDate>().ok();
        let label = nd.map(|d| {
            if label_fmt { d.format("%-d").to_string() } else { d.format("%a").to_string() }
        }).unwrap_or_else(|| r.night.clone());
        let (bar_start, bar_end) = nd
            .map(|d| (local_ts(d, 0, 0, 0), local_ts(d, 23, 59, 59)))
            .unwrap_or((0, 0));
        let deep = r.deep.min(r.total);
        let light = (r.total - deep).max(0);
        SleepBarData {
            label,
            bar_start,
            bar_end,
            light_fraction: light as f32 / max_total as f32,
            deep_fraction: deep as f32 / max_total as f32,
            light_secs: light,
            deep_secs: deep,
            total_label: format_duration(r.total),
            deep_label: if deep > 0 { format!("{} deep", format_duration(deep)) } else { String::new() },
        }
    }).collect())
}

/// Summary label for the sleep chart header.
/// Day: total sleep + deep sleep for that night.
/// Week/Month: average per night.
pub fn compute_sleep_summary(bars: &[SleepBarData], period: i32) -> String {
    if bars.is_empty() {
        return "No sleep data".to_string();
    }
    let n = bars.len() as i64;
    let total_light: i64 = bars.iter().map(|b| b.light_secs).sum();
    let total_deep: i64 = bars.iter().map(|b| b.deep_secs).sum();

    if period == 0 {
        let sleep = total_light + total_deep;
        if total_deep > 0 {
            format!("{} · {} deep", format_duration(sleep), format_duration(total_deep))
        } else {
            format_duration(sleep)
        }
    } else {
        let avg_sleep = (total_light + total_deep) / n;
        let avg_deep = total_deep / n;
        if avg_deep > 0 {
            format!("avg {} · {} deep", format_duration(avg_sleep), format_duration(avg_deep))
        } else {
            format!("avg {}", format_duration(avg_sleep))
        }
    }
}

// ─── Activity sessions ───────────────────────────────────────────────────────

pub fn load_sessions_filtered(
    conn: &Connection,
    session_filter: i32,
    range_start: i64,
    range_end: i64,
) -> anyhow::Result<Vec<HealthSessionData>> {
    let sql = "SELECT s.start_ts, s.utc_offset, s.duration_secs, t.name,
                      s.has_metrics, s.steps, s.active_kcal, s.distance_m
               FROM (
                   SELECT start_ts, utc_offset, duration_secs, session_type,
                          (steps IS NOT NULL)       AS has_metrics,
                          COALESCE(steps, 0)        AS steps,
                          COALESCE(active_kcal, 0)  AS active_kcal,
                          COALESCE(distance_m, 0)   AS distance_m
                   FROM health_activity_sessions
               ) s
               JOIN session_types t ON s.session_type = t.id
               WHERE (?1 < 0 OR s.start_ts >= ?1)
                 AND (?2 < 0 OR s.start_ts <= ?2)
                 AND (?3 = 0
                      OR (?3 = 1 AND s.session_type >= 5)
                      OR (?3 = 2 AND s.session_type <= 4))
               ORDER BY s.start_ts DESC
               LIMIT 100";

    let mut stmt = conn.prepare(sql)?;
    let sessions = stmt
        .query_map(params![range_start, range_end, session_filter], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, bool>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, i64>(7)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .map(|(ts, utc_offset, dur, type_name, has_metrics, steps, active_kcal, distance_m)| {
            // Render in the watch's local zone via its stored offset.
            let true_ts = ts + utc_offset;
            let metrics_label = if has_metrics {
                format!(
                    "{} steps · {} kcal · {}",
                    format_number(steps), active_kcal, format_distance(distance_m),
                )
            } else {
                String::new()
            };
            HealthSessionData {
                type_name: capitalize(&type_name),
                start_label: format_ts(true_ts),
                duration_label: format_duration(dur),
                has_metrics,
                metrics_label,
            }
        })
        .collect();

    Ok(sessions)
}

// ─── Sleep timing strip data ──────────────────────────────────────────────────

pub fn load_sleep_nights(
    conn: &Connection,
    period: i32,
    offset: i32,
) -> anyhow::Result<Vec<SleepNightData>> {
    let (range_start, range_end) = period_range_offset(period, offset);
    let label_fmt = period == 2;

    let mut stmt = conn.prepare(
        "SELECT date(start_ts + utc_offset - 43200, 'unixepoch') AS night,
                start_ts, utc_offset, duration_secs, session_type
         FROM health_activity_sessions
         -- Bucketing shifts by -12h (date(start_ts+utc_offset-43200)); shift the
         -- UTC window by +12h to match, so a night's post-midnight sessions at a
         -- range boundary aren't dropped.
         WHERE start_ts >= ?1 + 43200 AND start_ts <= ?2 + 43200
           AND session_type <= 4
         ORDER BY night ASC, start_ts ASC",
    )?;

    struct Row { night: String, start_ts: i64, utc_offset: i64, duration_secs: i64, session_type: i32 }
    let rows: Vec<Row> = stmt
        .query_map(params![range_start, range_end], |r| {
            Ok(Row { night: r.get(0)?, start_ts: r.get(1)?, utc_offset: r.get(2)?, duration_secs: r.get(3)?, session_type: r.get(4)? })
        })?
        .filter_map(|r| r.ok())
        .collect();

    // Pass 1: collect raw phase data grouped by night key.
    struct RawPhase { true_start: i64, true_end: i64, is_deep: bool }
    struct RawNight {
        night_key: String,
        nd: Option<NaiveDate>,
        first_true_start: i64,
        bar_start: i64,
        phases: Vec<RawPhase>,
    }

    let mut raw: Vec<RawNight> = Vec::new();

    for row in rows {
        let true_start = row.start_ts + row.utc_offset; // watch-local epoch
        let true_end   = true_start + row.duration_secs;
        let is_deep    = matches!(row.session_type, 2 | 4);

        if raw.last().map(|n: &RawNight| n.night_key.as_str()) != Some(&row.night) {
            let nd = row.night.parse::<NaiveDate>().ok();
            let bar_start = nd.map(|d| local_ts(d, 0, 0, 0)).unwrap_or(0);
            raw.push(RawNight {
                night_key: row.night.clone(),
                nd,
                first_true_start: true_start,
                bar_start,
                phases: Vec::new(),
            });
        }
        raw.last_mut().unwrap().phases.push(RawPhase { true_start, true_end, is_deep });
    }

    // Pass 2: compute duration-proportional fractions and labels.
    let mut nights: Vec<SleepNightData> = Vec::new();
    for n in raw {
        let group_start = n.phases.iter().map(|p| p.true_start).min().unwrap_or(0);
        let group_end   = n.phases.iter().map(|p| p.true_end).max().unwrap_or(group_start);
        let span        = (group_end - group_start).max(1) as f32;
        // Deep phases overlay the containers — count only containers (non-deep)
        // so the duration isn't double-counted.
        let total_dur: i64 = n
            .phases
            .iter()
            .filter(|p| !p.is_deep)
            .map(|p| p.true_end - p.true_start)
            .sum();

        let day_str = n.nd.map(|d| {
            if label_fmt { d.format("%-d").to_string() } else { d.format("%a").to_string() }
        }).unwrap_or_else(|| n.night_key.clone());
        let time_str = DateTime::from_timestamp(n.first_true_start, 0)
            .map(|dt| dt.format("%-I:%M%P").to_string())
            .unwrap_or_else(|| "?".to_string());
        let label = format!("{} {}", day_str, time_str);

        let segments = n.phases.iter().map(|p| {
            let start_frac = ((p.true_start - group_start) as f32 / span).clamp(0.0, 1.0);
            let end_frac   = ((p.true_end   - group_start) as f32 / span).clamp(0.0, 1.0);
            SleepSegmentData {
                start_frac,
                width_frac: (end_frac - start_frac).max(0.0),
                is_deep: p.is_deep,
            }
        }).collect();

        nights.push(SleepNightData {
            label,
            duration_label: format_duration(total_dur),
            bar_start: n.bar_start,
            segments,
        });
    }

    Ok(nights)
}

// ─── Formatting helpers ───────────────────────────────────────────────────────

/// Format a watch-local-epoch (start_ts + utc_offset) as a date/time string.
fn format_ts(local_epoch: i64) -> String {
    DateTime::from_timestamp(local_epoch, 0)
        .map(|dt| dt.format("%b %d, %H:%M").to_string())
        .unwrap_or_else(|| "?".to_string())
}

pub fn format_duration(secs: i64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    if h > 0 { format!("{}h {}m", h, m) } else { format!("{}m", m) }
}

fn format_distance(meters: i64) -> String {
    if meters >= 1000 {
        format!("{:.1} km", meters as f64 / 1000.0)
    } else {
        format!("{} m", meters)
    }
}

pub fn format_number(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { result.push(','); }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

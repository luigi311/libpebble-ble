use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use anyhow::Context;
use libpebble_ble::endpoints::datalog::tag as datalog_tag;
use libpebble_ble::DatalogData;
use rusqlite::{Connection, params};
use tracing::warn;

// Pebble firmware version constants (from RecordVersion enum in dataloggingendpoint.cpp).
const VERSION_FW_3_10_AND_BELOW: u16 = 5;
const VERSION_FW_3_11: u16 = 6;
const VERSION_FW_4_0: u16 = 7;
const VERSION_FW_4_1: u16 = 8;
const VERSION_FW_4_3: u16 = 13;

pub struct HealthDb {
    conn: Connection,
}

impl HealthDb {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create DB directory {}", parent.display()))?;
            #[cfg(unix)]
            if let Err(e) = std::fs::set_permissions(
                parent,
                std::fs::Permissions::from_mode(0o700),
            ) {
                warn!("could not set permissions on {}: {e}", parent.display());
            }
        }
        let conn = Connection::open(path)
            .with_context(|| format!("open health DB at {}", path.display()))?;
        #[cfg(unix)]
        if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
            warn!("could not set permissions on {}: {e}", path.display());
        }

        // Drop views before recreating so definition changes take effect on every open.
        conn.execute_batch(
            "DROP VIEW IF EXISTS v_sleep;
             DROP VIEW IF EXISTS v_workouts;",
        )?;

        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA foreign_keys=ON;

             -- Raw DataLog batches (one row per SENDDATA message).
             -- data + item_size allow reprocessing if a parser needs fixing.
             CREATE TABLE IF NOT EXISTS health_records (
                 id          INTEGER PRIMARY KEY,
                 tag         INTEGER NOT NULL,
                 app_uuid    BLOB    NOT NULL,
                 session_ts  INTEGER NOT NULL,
                 item_type   INTEGER NOT NULL,
                 item_size   INTEGER NOT NULL,
                 crc         INTEGER NOT NULL,
                 data        BLOB    NOT NULL,
                 received_at INTEGER NOT NULL,
                 UNIQUE(tag, app_uuid, session_ts, crc)
             );
             CREATE INDEX IF NOT EXISTS idx_health_tag        ON health_records(tag);
             CREATE INDEX IF NOT EXISTS idx_health_session_ts ON health_records(session_ts);

             -- Per-minute activity data (tag 81).
             --
             -- Wire format: 9-byte chunk header + record_num × record_length sub-records.
             --   [chunk header, 9 bytes]
             --     u16 record_version
             --     u32 timestamp         unix ts of first minute in chunk
             --     i8  utc_offset_15min  15-min segments (skipped)
             --     u8  record_length     bytes per sub-record
             --     u8  record_num        count of sub-records
             --   [sub-record, record_length bytes each]
             --     u8  steps
             --     u8  orientation
             --     u16 vmc               (intensity / vector magnitude count)
             --     u8  light
             --     u8  flags             (version >= 5)
             --     u16 resting_gram_cal  (version >= 6)
             --     u16 active_gram_cal   (version >= 6)
             --     u16 distance_cm       (version >= 6)
             --     u8  heart_rate_bpm    (version >= 7)
             --     u16 heart_rate_weight (version >= 8)
             --     u8  heart_rate_zone   (version >= 13)
             CREATE TABLE IF NOT EXISTS health_activity_minutes (
                 id                    INTEGER PRIMARY KEY,
                 health_record_id      INTEGER NOT NULL REFERENCES health_records(id),
                 record_version        INTEGER NOT NULL,
                 record_ts             INTEGER NOT NULL,
                 steps                 INTEGER NOT NULL,
                 orientation           INTEGER NOT NULL,
                 vmc                   INTEGER NOT NULL,
                 light                 INTEGER NOT NULL,
                 flags                 INTEGER,
                 resting_gram_calories INTEGER,
                 active_gram_calories  INTEGER,
                 distance_cm           INTEGER,
                 heart_rate_bpm        INTEGER,
                 heart_rate_weight     INTEGER,
                 heart_rate_zone       INTEGER,
                 raw                   BLOB    NOT NULL,
                 UNIQUE(record_ts)
             );
             CREATE INDEX IF NOT EXISTS idx_activity_min_ts ON health_activity_minutes(record_ts);

             -- Overlay session records (tags 83 and 84).
             --
             -- Tag 83 (SLEEP) and tag 84 (ACTIVITY_SESSIONS) both use this format.
             -- Tag 83 contains only sleep-type sessions; tag 84 contains all types.
             -- Duplicates across tags are silently ignored via UNIQUE(start_ts, session_type).
             --
             -- Wire format (per item, base 18 bytes):
             --   u16 version
             --   u16 (unused)
             --   u16 session_type   1=sleep 2=deep_sleep 3=nap 4=deep_nap 5=walk 6=run
             --   u32 utc_offset     seconds east of UTC
             --   u32 start_ts       unix timestamp
             --   u32 duration_secs
             --   [walk/run extension, version >= 3, session_type 5 or 6, 8 extra bytes]
             --   u16 steps
             --   u16 active_kcal
             --   u16 resting_kcal
             --   u16 distance_m
             CREATE TABLE IF NOT EXISTS health_activity_sessions (
                 id               INTEGER PRIMARY KEY,
                 health_record_id INTEGER NOT NULL REFERENCES health_records(id),
                 record_version   INTEGER NOT NULL,
                 session_type     INTEGER NOT NULL,
                 utc_offset       INTEGER NOT NULL,
                 start_ts         INTEGER NOT NULL,
                 duration_secs    INTEGER NOT NULL,
                 steps            INTEGER,
                 active_kcal      INTEGER,
                 resting_kcal     INTEGER,
                 distance_m       INTEGER,
                 raw              BLOB    NOT NULL,
                 UNIQUE(start_ts, session_type)
             );
             CREATE INDEX IF NOT EXISTS idx_sessions_start ON health_activity_sessions(start_ts);

             -- Lookup table for overlay session types.
             -- Join with health_activity_sessions on session_type = id.
             CREATE TABLE IF NOT EXISTS session_types (
                 id   INTEGER PRIMARY KEY,
                 name TEXT    NOT NULL
             );
             INSERT OR IGNORE INTO session_types VALUES
                 (1, 'sleep'),
                 (2, 'deep_sleep'),
                 (3, 'nap'),
                 (4, 'deep_nap'),
                 (5, 'walk'),
                 (6, 'run');",
        )?;

        conn.execute_batch(
            "CREATE VIEW v_sleep AS
             SELECT s.id, s.start_ts, s.duration_secs, t.name AS type
             FROM health_activity_sessions s
             JOIN session_types t ON s.session_type = t.id
             WHERE s.session_type <= 4;

             CREATE VIEW v_workouts AS
             SELECT s.id, s.start_ts, s.duration_secs, t.name AS type,
                    s.steps, s.active_kcal, s.resting_kcal, s.distance_m
             FROM health_activity_sessions s
             JOIN session_types t ON s.session_type = t.id
             WHERE s.session_type >= 5;",
        )?;

        Ok(Self { conn })
    }

    /// Insert a raw batch into health_records and parse individual records into the
    /// per-tag tables. Silently skips duplicate batches (same CRC).
    pub fn insert_batch(&self, batch: &DatalogData) -> anyhow::Result<()> {
        let received_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let rows_changed = self.conn.execute(
            "INSERT OR IGNORE INTO health_records
                 (tag, app_uuid, session_ts, item_type, item_size, crc, data, received_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                batch.tag as i64,
                batch.app_uuid.as_slice(),
                batch.session_timestamp as i64,
                batch.item_type as i64,
                batch.item_size as i64,
                batch.crc as i64,
                &batch.data,
                received_at,
            ],
        )?;

        if rows_changed == 0 {
            // Duplicate batch; child records already stored on the first receipt.
            return Ok(());
        }

        let record_id = self.conn.last_insert_rowid();
        let item_size = batch.item_size as usize;

        match batch.tag {
            datalog_tag::ACTIVITY_STEPS => {
                self.insert_activity_minutes(record_id, &batch.data, item_size)
            }
            // Tags 83 (SLEEP) and 84 (ACTIVITY_SESSIONS) both use overlay format.
            // Tag 83 carries only sleep-type sessions; duplicates are silently ignored.
            datalog_tag::SLEEP | datalog_tag::ACTIVITY_SESSIONS => {
                self.insert_activity_sessions(record_id, &batch.data, item_size)
            }
            // tag 85 (HR) is protobuf — skip until schema is known.
            // tag 87 is device/firmware summary — not health data.
            _ => Ok(()),
        }
    }

    /// Parse tag 81 per-minute activity chunks.
    fn insert_activity_minutes(
        &self,
        record_id: i64,
        data: &[u8],
        item_size: usize,
    ) -> anyhow::Result<()> {
        const CHUNK_HEADER: usize = 9; // u16 ver + u32 ts + i8 utc_off + u8 rec_len + u8 rec_num

        if item_size < CHUNK_HEADER {
            warn!("activity item_size={item_size} too small; skipping");
            return Ok(());
        }
        if data.is_empty() || data.len() % item_size != 0 {
            return Ok(());
        }

        let mut stmt = self.conn.prepare_cached(
            "INSERT OR IGNORE INTO health_activity_minutes
                 (health_record_id, record_version, record_ts, steps, orientation, vmc, light,
                  flags, resting_gram_calories, active_gram_calories, distance_cm,
                  heart_rate_bpm, heart_rate_weight, heart_rate_zone, raw)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        )?;

        for item in data.chunks_exact(item_size) {
            let record_version = u16::from_le_bytes([item[0], item[1]]);
            let mut ts = u32::from_le_bytes([item[2], item[3], item[4], item[5]]) as i64;
            // item[6]: utc_offset in 15-min segments — not stored
            let record_length = item[7] as usize;
            let record_num = item[8] as usize;

            if record_length == 0 {
                continue;
            }

            let sub_data = &item[CHUNK_HEADER..];
            let count = (sub_data.len() / record_length).min(record_num);

            for i in 0..count {
                let rec = &sub_data[i * record_length..(i + 1) * record_length];
                let record_ts = ts;
                ts += 60;

                // Minimum: steps(1) + orientation(1) + vmc(2) + light(1) = 5 bytes
                if rec.len() < 5 {
                    continue;
                }

                let steps = rec[0] as i64;
                let orientation = rec[1] as i64;
                let vmc = u16::from_le_bytes([rec[2], rec[3]]) as i64;
                let light = rec[4] as i64;
                let mut off = 5usize;

                let flags: Option<i64> =
                    if record_version >= VERSION_FW_3_10_AND_BELOW && off < rec.len() {
                        let v = rec[off] as i64;
                        off += 1;
                        Some(v)
                    } else {
                        None
                    };

                let (resting_gram_cal, active_gram_cal, distance_cm) =
                    if record_version >= VERSION_FW_3_11 && off + 5 < rec.len() {
                        let r = u16::from_le_bytes([rec[off], rec[off + 1]]) as i64;
                        off += 2;
                        let a = u16::from_le_bytes([rec[off], rec[off + 1]]) as i64;
                        off += 2;
                        let d = u16::from_le_bytes([rec[off], rec[off + 1]]) as i64;
                        off += 2;
                        (Some(r), Some(a), Some(d))
                    } else {
                        (None, None, None)
                    };

                let heart_rate: Option<i64> =
                    if record_version >= VERSION_FW_4_0 && off < rec.len() {
                        let v = rec[off] as i64;
                        off += 1;
                        Some(v)
                    } else {
                        None
                    };

                let heart_rate_weight: Option<i64> =
                    if record_version >= VERSION_FW_4_1 && off + 1 < rec.len() {
                        let v = u16::from_le_bytes([rec[off], rec[off + 1]]) as i64;
                        off += 2;
                        Some(v)
                    } else {
                        None
                    };

                let heart_rate_zone: Option<i64> =
                    if record_version >= VERSION_FW_4_3 && off < rec.len() {
                        Some(rec[off] as i64)
                    } else {
                        None
                    };

                stmt.execute(params![
                    record_id,
                    record_version as i64,
                    record_ts,
                    steps,
                    orientation,
                    vmc,
                    light,
                    flags,
                    resting_gram_cal,
                    active_gram_cal,
                    distance_cm,
                    heart_rate,
                    heart_rate_weight,
                    heart_rate_zone,
                    rec,
                ])?;
            }
        }
        Ok(())
    }

    fn insert_activity_sessions(
        &self,
        record_id: i64,
        data: &[u8],
        item_size: usize,
    ) -> anyhow::Result<()> {
        Self::do_insert_activity_sessions(&self.conn, record_id, data, item_size)
    }

    /// Parse overlay session records (tags 83 and 84).
    ///
    /// Base (18 bytes): u16 version, u16 skip, u16 session_type, u32 utc_offset,
    ///   u32 start_ts, u32 duration_secs.
    ///
    /// Walk/run extension (version >= 3, session_type 5 or 6, 8 extra bytes):
    ///   u16 steps, u16 active_kcal, u16 resting_kcal, u16 distance_m.
    ///   Note: for version == 3 non-walk/run sessions the 8 bytes are present
    ///   in the payload but contain no useful data and are skipped.
    fn do_insert_activity_sessions(
        conn: &Connection,
        record_id: i64,
        data: &[u8],
        item_size: usize,
    ) -> anyhow::Result<()> {
        const MIN_ITEM: usize = 18;
        const WALK_RUN_EXT: usize = 26; // 18 base + 8 walk/run fields

        if item_size < MIN_ITEM {
            warn!(
                "session item_size={item_size} (expected >={MIN_ITEM}); \
                 raw bytes stored in health_records for reprocessing"
            );
            return Ok(());
        }
        if data.is_empty() || data.len() % item_size != 0 {
            return Ok(());
        }

        let mut stmt = conn.prepare_cached(
            "INSERT OR IGNORE INTO health_activity_sessions
                 (health_record_id, record_version, session_type, utc_offset, start_ts,
                  duration_secs, steps, active_kcal, resting_kcal, distance_m, raw)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        )?;

        for item in data.chunks_exact(item_size) {
            let version = u16::from_le_bytes([item[0], item[1]]);
            // item[2..4]: skip (u16)
            let session_type = u16::from_le_bytes([item[4], item[5]]);
            let utc_offset = u32::from_le_bytes([item[6], item[7], item[8], item[9]]) as i64;
            let start_ts = u32::from_le_bytes([item[10], item[11], item[12], item[13]]) as i64;
            let duration = u32::from_le_bytes([item[14], item[15], item[16], item[17]]) as i64;

            let is_walk_run = session_type == 5 || session_type == 6;
            let (steps, active_kcal, resting_kcal, distance_m) =
                if version >= 3 && is_walk_run && item_size >= WALK_RUN_EXT {
                    // Wire order: steps, active_kcal, resting_kcal, distance_m
                    let s = u16::from_le_bytes([item[18], item[19]]) as i64;
                    let a = u16::from_le_bytes([item[20], item[21]]) as i64;
                    let r = u16::from_le_bytes([item[22], item[23]]) as i64;
                    let d = u16::from_le_bytes([item[24], item[25]]) as i64;
                    (Some(s), Some(a), Some(r), Some(d))
                } else {
                    (None, None, None, None)
                };

            stmt.execute(params![
                record_id,
                version as i64,
                session_type as i64,
                utc_offset,
                start_ts,
                duration,
                steps,
                active_kcal,
                resting_kcal,
                distance_m,
                &item[..item_size],
            ])?;
        }
        Ok(())
    }
}

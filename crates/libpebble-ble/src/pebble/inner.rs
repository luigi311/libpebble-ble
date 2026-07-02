//! Core state and handler types for a Pebble BLE session.
//!
//! `PebbleInner` holds handler registrations, pending transactions,
//! screenshot reassembly, and the GATT server handle — everything that
//! lives inside the `Arc<Mutex<…>>` of a `Pebble`.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::endpoints::app_message::AppMessageValue;
use crate::endpoints::blob_db::BlobDB2Incoming;
use crate::endpoints::datalog::{DatalogData, DatalogSession};
use crate::endpoints::music::MusicAction;
use crate::endpoints::phone_control::PhoneAction;
use crate::endpoints::screenshot::ScreenshotVersion;
use crate::endpoints::system::{WatchColorInfo, WatchVersionInfo};
use crate::transport::gatt_server::PebbleGattServerHandle;

// ── Handler type aliases ───────────────────────────────────────────────

pub type AppMessageHandler =
    Arc<dyn Fn(String, HashMap<u32, AppMessageValue>) + Send + Sync + 'static>;
pub type AckHandler = Arc<dyn Fn(u8) + Send + Sync + 'static>;
pub type NackHandler = Arc<dyn Fn(u8) + Send + Sync + 'static>;
pub type HealthDataHandler = Arc<dyn Fn(DatalogData) + Send + Sync + 'static>;
/// Handler for records the watch pushes back over BlobDB2 (Write/WriteBack).
/// Arguments: `(db_id, key, value)` — `db_id` matches `BlobDBId` (e.g. 7 =
/// HealthParams, 12 = WatchPrefs) so a single handler can route by database.
pub type WatchPrefHandler = Arc<dyn Fn(u8, String, Vec<u8>) + Send + Sync + 'static>;
/// Handler called with the watch battery percentage (0–100) when it changes.
pub type BatteryHandler = Arc<dyn Fn(u8) + Send + Sync + 'static>;
/// Handler called when an app opens/closes on the watch: `(app_uuid, running)`.
pub type AppRunStateHandler = Arc<dyn Fn(String, bool) + Send + Sync + 'static>;
/// Handler called with a media-control action the watch sent (play/pause/next/…).
pub type MusicActionHandler = Arc<dyn Fn(MusicAction) + Send + Sync + 'static>;
/// Handler called when the watch sends a phone control action (answer/hangup).
pub type PhoneActionHandler = Arc<dyn Fn(PhoneAction) + Send + Sync + 'static>;

// ── Screenshot types ───────────────────────────────────────────────────

/// A decoded watch screenshot: RGBA8888 pixels, row-major (`width*height*4` bytes).
#[derive(Debug, Clone)]
pub struct Screenshot {
    pub width: u32,
    pub height: u32,
    pub pixels: Vec<u8>,
}

/// Raw framebuffer handed from the dispatch to the awaiting `take_screenshot`.
pub(crate) struct RawScreenshot {
    pub(crate) version: ScreenshotVersion,
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) data: Vec<u8>,
}

/// In-flight screenshot reassembly state (header, then accumulating data).
pub(crate) struct ScreenshotAccumulator {
    /// Identifies the originating `take_screenshot` so its cleanup can't clobber
    /// a different request that started after this one finished.
    pub(crate) request_id: u64,
    pub(crate) version: Option<ScreenshotVersion>,
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) expected: usize,
    pub(crate) buffer: Vec<u8>,
    pub(crate) done: oneshot::Sender<Result<RawScreenshot, String>>,
}

// ── Core state ─────────────────────────────────────────────────────────

pub(crate) struct PebbleInner {
    pub(crate) app_message_handlers: Vec<AppMessageHandler>,
    pub(crate) ack_handlers: Vec<AckHandler>,
    pub(crate) nack_handlers: Vec<NackHandler>,
    pub(crate) health_handlers: Vec<HealthDataHandler>,
    pub(crate) watch_pref_handlers: Vec<WatchPrefHandler>,
    pub(crate) battery_handlers: Vec<BatteryHandler>,
    /// Latest watch battery percentage (0–100); `None` until first read.
    pub(crate) battery_level: Option<u8>,
    pub(crate) app_run_state_handlers: Vec<AppRunStateHandler>,
    pub(crate) music_action_handlers: Vec<MusicActionHandler>,
    pub(crate) phone_action_handlers: Vec<PhoneActionHandler>,
    /// In-flight screenshot reassembly, if a `take_screenshot` is awaiting.
    pub(crate) screenshot: Option<ScreenshotAccumulator>,
    /// Monotonic id assigned to each screenshot request.
    pub(crate) screenshot_seq: u64,
    /// transaction_id → future resolved when watch ACK/NACKs it
    pub(crate) pending: HashMap<u8, oneshot::Sender<bool>>,
    /// Insertion-order queue for pending txns so resolve_pending can pick
    /// the true oldest when the watch ACKs a non-matching txn.
    pub(crate) pending_order: VecDeque<u8>,
    /// BlobDB2 token → future resolved when watch sends the matching response
    pub(crate) blobdb2_pending: HashMap<u16, oneshot::Sender<BlobDB2Incoming>>,
    /// Futures awaiting a WatchVersionResponse (endpoint 16). All are resolved
    /// when the next response arrives.
    pub(crate) watch_version_pending: Vec<oneshot::Sender<WatchVersionInfo>>,
    /// Futures awaiting a factory-registry watch-color response (endpoint 5001).
    /// `None` is sent on an error reply or unknown color.
    pub(crate) watch_color_pending: Vec<oneshot::Sender<Option<&'static WatchColorInfo>>>,
    pub(crate) txn: u8,
    /// Handle to the GATT server send channel (set once server is started).
    pub(crate) gatt_server: Option<PebbleGattServerHandle>,
    /// Open DataLog sessions keyed by the 1-byte handle from the watch.
    pub(crate) datalog_sessions: HashMap<u8, DatalogSession>,
    /// BlobDB2 protocol version negotiated at connect time (0 = v0/unknown, 1+ = InsertWithTimestamp capable).
    pub(crate) blob_db_version: u8,
}

impl PebbleInner {
    pub(crate) fn new() -> Self {
        Self {
            app_message_handlers: Vec::new(),
            ack_handlers: Vec::new(),
            nack_handlers: Vec::new(),
            health_handlers: Vec::new(),
            watch_pref_handlers: Vec::new(),
            battery_handlers: Vec::new(),
            battery_level: None,
            app_run_state_handlers: Vec::new(),
            music_action_handlers: Vec::new(),
            phone_action_handlers: Vec::new(),
            screenshot: None,
            screenshot_seq: 0,
            pending: HashMap::new(),
            pending_order: VecDeque::new(),
            blobdb2_pending: HashMap::new(),
            watch_version_pending: Vec::new(),
            watch_color_pending: Vec::new(),
            txn: 0,
            gatt_server: None,
            datalog_sessions: HashMap::new(),
            blob_db_version: 0,
        }
    }
}

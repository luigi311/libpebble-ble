use uuid::Uuid;

// fed9 pairing/connectivity service — the watch advertises this.
pub const PAIR_SERVICE_UUID: Uuid = uuid::uuid!("0000fed9-0000-1000-8000-00805f9b34fb");
pub const CONNECTIVITY_CHARACTERISTIC: Uuid =
    uuid::uuid!("00000001-328e-0fbb-c642-1aa6699bdada");
pub const PAIRING_TRIGGER_CHARACTERISTIC: Uuid =
    uuid::uuid!("00000002-328e-0fbb-c642-1aa6699bdada");
pub const MTU_CHARACTERISTIC: Uuid = uuid::uuid!("00000003-328e-0fbb-c642-1aa6699bdada");
pub const CONNECTION_PARAMS_CHARACTERISTIC: Uuid =
    uuid::uuid!("00000005-328e-0fbb-c642-1aa6699bdada");

// PPoGATT transport, phone-hosted server model (the working Gadgetbridge path):
// the phone hosts a GATT server and the watch connects back to it as a client.
// service 10000000:
//   READ_CHARACTERISTIC  10000002  PROPERTY_READ
//   WRITE_CHARACTERISTIC 10000001  PROPERTY_WRITE_NO_RESPONSE | NOTIFY
pub const PPOGATT_SERVER_SERVICE: Uuid = uuid::uuid!("10000000-328e-0fbb-c642-1aa6699bdada");
pub const PPOGATT_SERVER_WRITE_CHARACTERISTIC: Uuid =
    uuid::uuid!("10000001-328e-0fbb-c642-1aa6699bdada");
pub const PPOGATT_SERVER_READ_CHARACTERISTIC: Uuid =
    uuid::uuid!("10000002-328e-0fbb-c642-1aa6699bdada");
pub const PPOGATT_BADBAD_SERVICE: Uuid = uuid::uuid!("badbadba-dbad-badb-adba-badbadbadbad");

// PPoGATT transport, watch-hosted server model (Pebble 2 / clientOnly mode).
pub const PPOGATT_WATCH_NOTIFY: Uuid = uuid::uuid!("30000004-328e-0fbb-c642-1aa6699bdada");
pub const PPOGATT_WATCH_WRITE: Uuid = uuid::uuid!("30000006-328e-0fbb-c642-1aa6699bdada");

// Pebble system app UUIDs (from libpebblecommon SystemAppIDs).
// Used with send_app_message, launch_app, stop_app, etc.
pub const SYSTEM_APP_UUID: &str                = "00000000-0000-0000-0000-000000000000";
pub const SETTINGS_APP_UUID: &str              = "07e0d9cb-8957-4bf7-9d42-35bf47caadfe";
pub const CALENDAR_APP_UUID: &str              = "6c6c6fc2-1912-4d25-8396-3547d1dfac5b";
pub const WEATHER_APP_UUID: &str               = "61b22bc8-1e29-460d-a236-3fe409a439ff";
pub const HEALTH_APP_UUID: &str                = "36d8c6ed-4c83-4fa1-a9e2-8f12dc941f8c";
pub const MUSIC_APP_UUID: &str                 = "1f03293d-47af-4f28-b960-f2b02a6dd757";
pub const NOTIFICATIONS_APP_UUID: &str         = "b2cae818-10f8-46df-ad2b-98ad2254a3c1";
pub const ALARMS_APP_UUID: &str                = "67a32d95-ef69-46d4-a0b9-854cc62f97f9";
pub const SMS_APP_UUID: &str                   = "0863fc6a-66c5-4f62-ab8a-82ed00a98b5d";
pub const REMINDERS_APP_UUID: &str             = "42a07217-5491-4267-904a-d02a156752b6";
pub const WORKOUT_APP_UUID: &str               = "fef82c82-7176-4e22-88de-35a3fc18d43f";
pub const WATCHFACES_APP_UUID: &str            = "18e443ce-38fd-47c8-84d5-6d0c775fbe55";
pub const TICTOC_APP_UUID: &str                = "8f3c8686-31a1-4f5f-91f5-01600c9bdc59";
pub const KICKSTART_APP_UUID: &str             = "3af858c3-16cb-4561-91e7-f1ad2df8725f";
pub const MISSED_CALLS_APP_UUID: &str          = "af760190-bfc0-11e4-bb52-0800200c9a66";
pub const ANDROID_NOTIFICATIONS_UUID: &str     = "ed429c16-f674-4220-95da-454f303f15e2";
pub const TIMELINE_FUTURE_UUID: &str           = "79c76b48-6111-4e80-8deb-3119eebef33e";
pub const TIMELINE_PAST_UUID: &str             = "daae3686-bff6-4ba5-921b-262f847bb6e8";
pub const TIMELINE_MENU_ENTRY_UUID: &str       = "426ccd53-b380-4d83-8d06-9893de3477ce";
pub const QUIET_TIME_TOGGLE_UUID: &str         = "2220d805-cf9a-4e12-92b9-5ca778aff6bb";
pub const BACKLIGHT_UUID: &str                 = "d0f12e6c-97eb-2287-a2f5-115dfaa1d168";
pub const MOTION_BACKLIGHT_UUID: &str          = "d4f7be63-97e6-4952-b265-dd4bce11c155";
pub const AIRPLANE_MODE_UUID: &str             = "88c28c12-7f81-42db-aaa6-14ccef6f27e5";

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

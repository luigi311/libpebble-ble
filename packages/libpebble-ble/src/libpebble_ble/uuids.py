"""All Bluetooth UUIDs used to talk to a Pebble.

Verified against Gadgetbridge's PebbleGATTClient.java / PebbleGATTServer.java.
Kept in one module so the (reverse-engineered, magic-looking) identifiers have
a single home and every transport imports from here.
"""

# ---------------------------------------------------------------------------
# fed9 pairing/connectivity service — the watch advertises this; the central
# (us) reads/subscribes to it to set up the link before bulk data flows.
# ---------------------------------------------------------------------------
PAIR_SERVICE_UUID = "0000fed9-0000-1000-8000-00805f9b34fb"

CONNECTIVITY_CHARACTERISTIC = "00000001-328e-0fbb-c642-1aa6699bdada"
PAIRING_TRIGGER_CHARACTERISTIC = "00000002-328e-0fbb-c642-1aa6699bdada"
MTU_CHARACTERISTIC = "00000003-328e-0fbb-c642-1aa6699bdada"
CONNECTION_PARAMS_CHARACTERISTIC = "00000005-328e-0fbb-c642-1aa6699bdada"

# ---------------------------------------------------------------------------
# PPoGATT transport, phone-hosted server model (the WORKING Gadgetbridge
# path): the PHONE hosts a GATT server and the WATCH connects back to it as a
# client. The watch writes PPoGATT packets into our WRITE characteristic and
# we notify it back on the same characteristic.
#
#   SERVER_SERVICE (10000000) hosted by us (the phone/Linux box)
#     READ_CHARACTERISTIC  10000002  PROPERTY_READ
#     WRITE_CHARACTERISTIC 10000001  PROPERTY_WRITE_NO_RESPONSE | NOTIFY
#   plus a second "BADBAD" service added after the first, which Gadgetbridge
#   registers and the watch apparently expects to see.
# ---------------------------------------------------------------------------
PPOGATT_SERVER_SERVICE = "10000000-328e-0fbb-c642-1aa6699bdada"
PPOGATT_SERVER_WRITE_CHARACTERISTIC = (
    "10000001-328e-0fbb-c642-1aa6699bdada"  # watch -> us (also NOTIFY us -> watch)
)
PPOGATT_SERVER_READ_CHARACTERISTIC = "10000002-328e-0fbb-c642-1aa6699bdada"  # watch reads
PPOGATT_BADBAD_SERVICE = "badbadba-dbad-badb-adba-badbadbadbad"

# ---------------------------------------------------------------------------
# PPoGATT transport, watch-hosted server model (Gadgetbridge's experimental
# clientOnly mode, Pebble 2 only): the watch hosts service 30000003 and we are
# the GATT client. Per Gadgetbridge's PebbleGATTClient:
#   PPOGATT_CHARACTERISTIC_READ  = 30000004 -> we SUBSCRIBE (notify) to this
#   PPOGATT_CHARACTERISTIC_WRITE = 30000006 -> we WRITE PPoGATT packets to this
# (This is the opposite of the property flags one might infer from a raw GATT
# dump; the reference implementation is authoritative.)
# ---------------------------------------------------------------------------
PPOGATT_WATCH_NOTIFY = "30000004-328e-0fbb-c642-1aa6699bdada"  # watch -> us
PPOGATT_WATCH_WRITE = "30000006-328e-0fbb-c642-1aa6699bdada"  # us -> watch

CCCD_UUID = "00002902-0000-1000-8000-00805f9b34fb"

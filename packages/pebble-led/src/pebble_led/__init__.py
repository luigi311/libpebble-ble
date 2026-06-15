"""pebble_led — the long-lived daemon that owns the Pebble BLE connection.

One process holds the bleak GATT client, the phone-hosted PPoGATT GATT server,
and the single PPoGATT session with the watch. It answers the watch's
PHONE_VERSION/PING keepalives (handled inside libpebble_ble.Pebble) and exposes
a small D-Bus interface so any number of apps can send/receive AppMessages
without each one needing its own BLE link (which the hardware can't give them).
"""

from .service import PebbleDaemon

__all__ = ["PebbleDaemon"]

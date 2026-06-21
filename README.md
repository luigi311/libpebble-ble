# pebble-le

Talk to a Pebble smartwatch over Bluetooth Low Energy from Linux — as a Rust
library, a long-lived Rust daemon, and a Python client other apps use.

The watch gives you exactly **one** BLE link, so exactly one process can own
it. That process is the daemon; everything else talks to the daemon over D-Bus.

## Components

```
crates/libpebble-ble   Rust BLE/protocol library. Owns BlueZ (via bluer),
                       the PPoGATT GATT server, pairing, AppMessage, and all
                       endpoint codecs. Knows nothing about D-Bus or the daemon.
          ↑
crates/pebble-led      Rust daemon. Wraps one libpebble-ble Pebble instance,
                       exports org.pebble_le.Daemon on the session bus, handles
                       reconnection, forwards desktop notifications to the watch.
          ↑
packages/              Python client. pebble-le-client is the only Python
pebble-le-client       package; it wraps the D-Bus proxy behind the same API
                       libpebble-ble exposes (same decorators, same AppMessage
                       dict, same u8/u16/u32/i8/i16/i32 width wrappers).
```

The library never learns the daemon exists. The client never opens a BLE link.

## Rust library structure

```
crates/libpebble-ble/src/
  pebble.rs            High-level Pebble struct: connect lifecycle, endpoint
                       dispatch, AppMessage API, scan.

  transport/           BLE transport layer.
    agent.rs           BlueZ auto-accept pairing agent (registered during
                       first-time bonding; only accepts the configured address).
    gatt_server.rs     Phone-hosted PPoGATT GATT server (BlueZ peripheral).
                       The watch connects back to this as a GATT client.
    ppogatt.rs         PPoGATT framing, windowed sequence numbers, reassembly.

  endpoints/           One file per Pebble Protocol endpoint.
    mod.rs             Endpoint enum, pebble_pack/pebble_unpack framing.
    app_message.rs     AppMessage PUSH/ACK/NACK encode and decode (endpoint 48).
    app_run_state.rs   Launch/stop watchapps (endpoint 52).
    blob_db.rs         BlobDB inserts and notification builder (endpoint 0xb1db).
    phone_version.rs   Phone capability advertisement (endpoint 17).
    ping.rs            Ping/Pong (endpoint 2001).
    time.rs            UTC clock sync (endpoint 11).

  error.rs             PebbleError.
  uuids.rs             All Pebble and PPoGATT GATT UUIDs.
```

Adding a new endpoint: create `endpoints/<name>.rs`, add it to
`endpoints/mod.rs`, and add a match arm in `pebble.rs::on_pebble_message`.

## Liveness — two independent questions

* **Is the daemon process alive?** Its well-known bus name (`org.pebble_le.Daemon`)
  has an owner. `PebbleClient.is_daemon_running()` checks this with
  `NameHasOwner` — no socket connect, no timeout, no stale pidfile.
* **Is the watch reachable?** The daemon's `Connected` property +
  `ConnectionChanged` signal. `PebbleClient.connected` / `is_connected()`.

A daemon can be alive while the watch is out of range; apps need to check both.

## Quick start

Run the daemon (owns the BLE link, syncs time, forwards desktop notifications):

```sh
pebble-led E6:94:0A:D4:D5:DC
pebble-led --verbose E6:94:0A:D4:D5:DC   # TRACE-level logging
pebble-led --adapter hci1 E6:94:0A:D4:D5:DC  # non-default adapter
```

Any Python app talks to it without touching BLE or D-Bus:

```python
import asyncio
from pebble_le_client import PebbleClient, u16

async def main():
    async with PebbleClient() as pebble:
        @pebble.on_app_message
        def handler(app_uuid, data):
            print("from watch:", app_uuid, data)

        await pebble.send_app_message(
            "00000000-0000-0000-0000-000000000000",
            {0: "hello", 1: u16(150)},
        )
        await asyncio.sleep(60)

asyncio.run(main())
```

## Building

```sh
# Build everything (Rust)
cargo build --release

# Run tests
cargo test

# Build and run the daemon directly
cargo run --bin pebble-led -- E6:94:0A:D4:D5:DC

# Python client: install dependencies and run tests
uv sync --all-packages
uv run pytest
```

## Installing the daemon

```sh
# Build the release binary
cargo build --release

# Copy the binary somewhere on your PATH
sudo install -m755 target/release/pebble-led /usr/local/bin/

# Or build the .deb (requires cargo-deb or debhelper setup)
dpkg-buildpackage -us -uc -b
sudo apt install ./pebble-led_*_*.deb
```

### Configure the daemon

The systemd unit reads the watch address from a per-user env file:

```sh
mkdir -p ~/.config/pebble-led
echo 'PEBBLE_ADDRESS=E6:94:0A:D4:D5:DC' > ~/.config/pebble-led/env
```

Start as a user service (must be a user service — the notification monitor
connects to your session D-Bus, which only exists inside your login):

```sh
systemctl --user daemon-reload
systemctl --user enable --now pebble-led.service
```

### Platform notes

* **dbus-broker systems**: The notification monitor uses `BecomeMonitor` 
  (the dbus-broker-compatible API) and falls back to `eavesdrop=true`
  AddMatch on older `dbus-daemon` installs.

* **BlueZ `AccessDenied`**: add yourself to the `bluetooth` group and start a
  fresh session: `sudo usermod -aG bluetooth "$USER"`, then log out and back in.

## D-Bus interface (`org.pebble_le.Daemon`)

Object path: `/org/pebble_le/Daemon` — session bus.

| Kind | Name | Signature | Notes |
|------|------|-----------|-------|
| Property | `Connected` | `b` | watch BLE link is up |
| Property | `WatchAddress` | `s` | configured watch address |
| Method | `SendAppMessage` | `(s, a{i(sv)}, b) → u` | uuid, data, wait_ack → txn |
| Method | `LaunchApp` | `(s)` | uuid |
| Method | `StopApp` | `(s)` | uuid |
| Method | `UpdateTime` | `()` | sync watch clock to system time |
| Method | `Notify` | `(s, s, s) → u` | title, body, subtitle → token |
| Method | `Ping` | `() → b` | daemon liveness probe |
| Method | `Scan` | `(d) → a(ss)` | timeout\_secs → [(address, name)] |
| Signal | `AppMessageReceived` | `(s, a{i(sv)})` | uuid, data |
| Signal | `AckReceived` | `(u)` | txn |
| Signal | `NackReceived` | `(u)` | txn |
| Signal | `ConnectionChanged` | `(b)` | connected |

AppMessage values cross D-Bus as `(tag, variant)` pairs where tag is one of
`u8 u16 u32 i8 i16 i32 uint int str bytes`. The Python client handles all
marshalling transparently.

## Supported features

### libpebble-ble
- [x] Connect via ble
- [x] Pings
- [x] App Launch
- [x] AppMessage
- [x] Time sync
- [ ] Notifications
  - [x] Send
  - [ ] Actions 
  - [x] Categorization (Text/Call/Other)
- [ ] Weather
- [x] Health
  - [x] Steps
  - [x] Sleep
  - [x] Heartrate
- [ ] Music
  - [ ] Playing status
  - [ ] Controls
- [ ] PBW install

### pebble-led (Daemon)
- [x] Pings
- [x] Reconnects
- [x] Time Sync
- [ ] Notificiations
  - [x] Forwarding
  - [ ] Actions (Dismiss)
  - [x] Categorizations
- [x] AppMessages
  - [x] External applications
- [ ] Music
- [x] Health
- [ ] Weather


## Why one repo

The daemon and Python client must agree on the D-Bus wire contract (bus name,
object path, interface name, AppMessage value encoding). A monorepo makes a
contract change one atomic commit that covers both ends at once.

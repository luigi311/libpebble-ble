# cobble

Talk to a Pebble smartwatch over Bluetooth Low Energy from Linux — as a Rust
library, a long-lived Rust daemon, and a Python client other apps use.

The watch gives you exactly **one** BLE link, so exactly one process can own
it. That process is the daemon (`cobbled`); everything else talks to the daemon over D-Bus.

## Components

```
crates/libpebble-ble   Rust BLE/protocol library. Owns BlueZ (via bluer),
                       the PPoGATT GATT server, pairing, AppMessage, and all
                       endpoint codecs. Knows nothing about D-Bus or the daemon.
          ↑
crates/cobbled         Rust daemon. Wraps one libpebble-ble Pebble instance,
                       exports org.cobble.Daemon on the session bus, handles
                       reconnection, forwards desktop notifications to the watch.
          ↑
packages/              Python client. cobble-client is the only Python
cobble-client          package; it wraps the D-Bus proxy behind the same API
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
    blob_db.rs         BlobDB inserts, notification builder, weather blobs,
                       and BlobDB2 bidirectional sync protocol (endpoints
                       0xb1db / 0xb2db).
    datalog.rs         DataLog session protocol — used by health data sync
                       (endpoint 0x11).
    health.rs          Health/settings blob decode + encode: activityPreferences
                       (height/weight/age/gender), hrmPreferences, heartRate
                       zones, unitsDistance, and the HealthSync request.
    watch_pref.rs      General watch-settings (WatchPrefs) typed registry —
                       decode db-12 keys (backlight, clock, vibration, …).
    phone_version.rs   Phone capability advertisement (endpoint 17).
    ping.rs            Ping/Pong (endpoint 2001).
    system.rs          WatchVersion (16), SystemMessage (18), and factory
                       registry / watch color (5001): firmware version, board,
                       serial, platform, capabilities, color.
    reset.rs           Reboot / recovery / factory reset / core dump (2003).
    time.rs            UTC clock sync (endpoint 11).

  error.rs             PebbleError.
  uuids.rs             All Pebble and PPoGATT GATT UUIDs, plus system app
                       UUIDs (weather, health, notifications, etc.).
```

Adding a new endpoint: create `endpoints/<name>.rs`, add it to
`endpoints/mod.rs`, and add a match arm in `pebble.rs::on_pebble_message`.

## Liveness — two independent questions

* **Is the daemon process alive?** Its well-known bus name (`org.cobble.Daemon`)
  has an owner. `CobbleClient.is_daemon_running()` checks this with
  `NameHasOwner` — no socket connect, no timeout, no stale pidfile.
* **Is the watch reachable?** The daemon's `Connected` property +
  `ConnectionChanged` signal. `CobbleClient.connected` / `is_connected()`.

A daemon can be alive while the watch is out of range; apps need to check both.

## Quick start

Create the config file first (required — the daemon will not start without it).
The path follows the XDG Base Directory spec: `$XDG_CONFIG_HOME/cobbled/config.toml`,
which defaults to `~/.config/cobbled/config.toml` when `XDG_CONFIG_HOME` is not set.

```sh
mkdir -p "${XDG_CONFIG_HOME:-$HOME/.config}/cobbled"
cat > "${XDG_CONFIG_HOME:-$HOME/.config}/cobbled/config.toml" << 'EOF'
address = "E6:94:0A:D4:D5:DC"   # your watch Bluetooth address
# adapter = "hci0"               # optional, default is hci0
# verbose = false                 # optional, or use -v at runtime
# db = "/custom/path/health.db"  # optional, default is XDG_DATA_HOME/cobbled/health.db
EOF
```

Run the daemon (owns the BLE link, syncs time, forwards desktop notifications):

```sh
cobbled                          # reads ~/.config/cobbled/config.toml
cobbled --verbose                # TRACE-level logging (overrides config)
cobbled --config /other/path.toml  # use a different config file
```

Any Python app talks to it without touching BLE or D-Bus:

```python
import asyncio
from cobble_client import CobbleClient, u16

async def main():
    async with CobbleClient() as cobble:
        @cobble.on_app_message
        def handler(app_uuid, data):
            print("from watch:", app_uuid, data)

        await cobble.send_app_message(
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

# Build and run the daemon directly (config file must exist first)
cargo run --bin cobbled

# Python client: install dependencies and run tests
uv sync --all-packages
uv run pytest
```

## Installing the daemon

```sh
# Build the release binary
cargo build --release

# Copy the binary somewhere on your PATH
sudo install -m755 target/release/cobbled /usr/local/bin/

# Or build the .deb (requires cargo-deb or debhelper setup)
dpkg-buildpackage -us -uc -b
sudo apt install ./cobbled_*_*.deb
```

### Configure the daemon

Create `$XDG_CONFIG_HOME/cobbled/config.toml` before enabling the service
(defaults to `~/.config/cobbled/config.toml` when `XDG_CONFIG_HOME` is not
set). The systemd unit checks for this file and will not start (or enter a
restart loop) if it is absent.

```toml
# $XDG_CONFIG_HOME/cobbled/config.toml
address = "E6:94:0A:D4:D5:DC"   # required — your watch Bluetooth address
# adapter = "hci0"               # optional, default hci0
# verbose = false                 # optional
# db = "/custom/path/health.db"  # optional, default XDG_DATA_HOME/cobbled/health.db
```

Start as a user service (must be a user service — the notification monitor
connects to your session D-Bus, which only exists inside your login):

```sh
systemctl --user daemon-reload
systemctl --user enable --now cobbled.service
```

### Platform notes

* **dbus-broker systems**: The notification monitor uses `BecomeMonitor`
  (the dbus-broker-compatible API) and falls back to `eavesdrop=true`
  AddMatch on older `dbus-daemon` installs.

* **BlueZ `AccessDenied`**: add yourself to the `bluetooth` group and start a
  fresh session: `sudo usermod -aG bluetooth "$USER"`, then log out and back in.

## D-Bus interface (`org.cobble.Daemon`)

Object path: `/org/cobble/Daemon` — session bus.

| Kind | Name | Signature | Notes |
|------|------|-----------|-------|
| Property | `Connected` | `b` | watch BLE link is up |
| Property | `WatchAddress` | `s` | configured watch address |
| Property | `BatteryLevel` | `n` | watch battery percentage (0–100), or -1 if unknown |
| Method | `SendAppMessage` | `(s, a{i(sv)}, b) → u` | uuid, data, wait_ack → txn |
| Method | `LaunchApp` | `(s)` | uuid |
| Method | `StopApp` | `(s)` | uuid |
| Method | `UpdateTime` | `()` | sync watch clock to system time |
| Method | `Notify` | `(s, s, s) → u` | title, body, subtitle → token |
| Method | `Ping` | `() → b` | daemon liveness probe |
| Method | `Scan` | `(d) → a(ss)` | timeout\_secs → [(address, name)] |
| Method | `ActivateHealth` | `(q, q, y, y, b)` | height\_cm, weight\_kg, age, gender (0=female 1=male 2=other), hrm\_enabled |
| Method | `FetchHealthData` | `()` | flush pending health records from watch |
| Method | `FetchHealthParams` | `()` | re-sync watch settings (health + general) from watch |
| Method | `GetHealthProfile` | `() → a{sv}` | watch health profile: height/weight/age/gender, HRM, HR zones, units |
| Method | `GetWatchSettings` | `() → a{sv}` | general watch settings (backlight, clock, vibration, quiet time, …) |
| Method | `GetWatchVersion` | `() → a{sv}` | firmware version, board, serial, BT address, language, capabilities, platform |
| Method | `GetWatchColor` | `() → a{sv}` | watch color/variant (protocol\_number, js\_name, description, watch\_type, supports\_hrm) |
| Method | `RebootWatch` | `()` | reboot the watch |
| Method | `ResetIntoRecovery` | `()` | reboot into recovery (PRF) firmware |
| Method | `CreateCoreDump` | `()` | trigger a watch core dump |
| Method | `FactoryReset` | `(b)` | DESTRUCTIVE — wipe + unpair; requires `confirm = true` |
| Method | `Forget` | `()` | remove the Bluetooth bond (unpair); re-pairs on next reconnect |
| Method | `ReprocessHealthData` | `()` | rebuild derived health tables from raw blobs |
| Method | `PushWeather` | `(ay, s, s, n, y, n, n, y, n, n, b)` | location\_key (16 bytes), location\_name, forecast\_short, current\_temp\_c, current\_weather, today\_high\_c, today\_low\_c, tomorrow\_weather, tomorrow\_high\_c, tomorrow\_low\_c, is\_current\_location. Weather types: 0=PartlyCloudy 1=CloudyDay 2=LightSnow 3=LightRain 4=HeavyRain 5=HeavySnow 6=Generic 7=Sun 8=RainAndSnow |
| Method | `ReloadConfig` | `()` | re-read config; disconnects if address/adapter changed |
| Signal | `AppMessageReceived` | `(s, a{i(sv)})` | uuid, data |
| Signal | `AckReceived` | `(u)` | txn |
| Signal | `NackReceived` | `(u)` | txn |
| Signal | `ConnectionChanged` | `(b)` | connected |
| Signal | `HealthDataReceived` | `(u, ay, u, u, u, y, q, ay)` | tag, app\_uuid, session\_timestamp, items\_left, crc, item\_type, item\_size, data |
| Signal | `HealthProfileReceived` | `(a{sv})` | watch health profile, emitted on connect and on change |
| Signal | `WatchSettingReceived` | `(s, v)` | key, value — emitted per general watch setting as it syncs |
| Signal | `BatteryChanged` | `(n)` | watch battery percentage (-1 = unknown) |
| Signal | `AppRunStateChanged` | `(s, b)` | app uuid, running — emitted when an app opens/closes on the watch |

AppMessage values cross D-Bus as `(tag, variant)` pairs where tag is one of
`u8 u16 u32 i8 i16 i32 uint int str bytes`. The Python client handles all
marshalling transparently.

Health data is stored automatically in SQLite at
`$XDG_DATA_HOME/cobbled/health.db` (or the path set in `config.toml`).
The `HealthDataReceived` signal fires for each batch so external tools can
consume raw records without reading the database directly.

## Supported features

### libpebble-ble
- [x] Connect via BLE (pairing, reconnect, MTU/connectivity handshake)
- [x] Pings
- [x] App launch / stop (+ inbound run-state events)
- [x] AppMessage
- [x] Time sync
- [ ] Notifications
  - [x] Send
  - [ ] Actions
  - [x] Categorization (Text/Call/Other)
- [x] Weather
- [ ] Health
  - [x] Steps
  - [ ] Sleep
  - [x] Heartrate
- [x] Watch settings
  - [x] Health profile read (height/weight/age/gender/HRM/HR zones/units)
  - [x] General settings read (backlight, clock, vibration, quiet time, …)
- [x] Watch info (firmware version, board, serial, BT address, capabilities, platform, color)
- [x] Battery level (read + change notifications)
- [x] Device management (reboot, recovery, factory reset, core dump, forget/unpair)
- [ ] Music
  - [ ] Playing status
  - [ ] Controls
- [ ] PBW install

### cobbled (Daemon)
- [x] Pings
- [x] Reconnects
- [x] Time Sync
- [ ] Notifications
  - [x] Forwarding
  - [ ] Actions (Dismiss)
  - [x] Categorizations
- [x] AppMessages
  - [x] External applications
- [x] Health (data sync + profile/settings read)
- [x] Watch info + device management (version, color, battery, reboot/reset/forget)
- [ ] Music
- [x] Weather

Every libpebble-ble capability is exposed over D-Bus and supported by the
Python client — see the [D-Bus interface](#d-bus-interface-orgcobbledaemon) table.


## Why one repo

The daemon and Python client must agree on the D-Bus wire contract (bus name,
object path, interface name, AppMessage value encoding). A monorepo makes a
contract change one atomic commit that covers both ends at once.

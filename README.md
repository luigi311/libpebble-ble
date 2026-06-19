# pebble-le

Talk to a Pebble smartwatch over Bluetooth Low Energy from Linux — as a library,
a long-lived daemon, and a client other apps use.

This is a `uv` workspace of four packages. The split exists because the watch
hardware gives you exactly **one** BLE link, so exactly one process can own it.
That process is the daemon; everything else talks to the daemon.

## The four packages

```
libpebble-ble        the BLE/protocol library. Owns bleak, the PPoGATT GATT
                     server, pairing, AppMessage. Knows nothing about D-Bus-as-
                     IPC, the daemon, or clients. Usable standalone.
        ↑
pebble-le-proto      the daemon<->client CONTRACT: bus name, object path,
                     interface name, and the value codec that lets a width-
                     pinned int (u16/i8/...) survive the D-Bus hop. One copy,
                     imported by both ends, so it can't drift.
        ↑                              ↑
pebble-led           the daemon.       pebble-le-client   the client.
imports proto +      Owns the single   imports proto.     Re-exposes
libpebble-ble.       Pebble link,      libpebble_ble's API (the on_app_message
Exports the D-Bus    answers pings,    decorator, the {0:"hi", 1:u16(150)} dict)
interface.           reconnects.       over D-Bus, hiding all of it.
```

The dependency arrows only point up. The library never learns the daemon
exists; the client never opens a BLE link. The package boundaries are the walls;
the repo is just where the walls live.

## Liveness — two independent questions

* **Is the daemon process alive?** Its well-known bus name (`org.pebble_le.Daemon`)
  has an owner. The client checks this with `NameHasOwner` — no socket connect,
  no timeout, no stale pidfile. `PebbleClient.is_daemon_running()`.
* **Is the watch reachable?** The daemon's `Connected` property +
  `ConnectionChanged` signal. `PebbleClient.connected` / `is_connected()`.

A daemon can be running fine while the watch is out of range, so apps need both.

## Quick start

Run the daemon (owns the link, syncs time, forwards notifications — all
independent of any app):

```sh
pebble-led E6:94:0A:D4:D5:DC
```

Any app then talks to it without touching BLE or D-Bus:

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

Note the call site is identical to using `libpebble_ble.Pebble` directly — same
decorator, same dict, same width wrappers. That symmetry is deliberate.


## Installing the daemon

Grab the latest `.deb` from [Releases](../../releases) and install it. Its
dependencies (bleak, dbus-fast, loguru, bluez) come from your distro — the
package bundles only the three Python modules:

```sh
sudo apt install ./pebble-led_*_all.deb
```

### Configure the daemon

Tell it your watch's address. The unit reads this from a per-user env file
rather than baking it in, so the package is the same for everyone:

```sh
mkdir -p ~/.config/pebble-led
echo 'PEBBLE_ADDRESS=E6:94:0A:D4:D5:DC' > ~/.config/pebble-led/env
```

Start it. It runs as a **user** service, not a system one — the notification
monitor eavesdrops on your *session* bus, which only exists inside your login,
so a root system service would never see your notifications:

```sh
systemctl --user daemon-reload
systemctl --user start pebble-led.service
```

Enable it to start at login and keep running after logout / across reboots:

```sh
systemctl --user enable pebble-led.service
```

### Troubleshooting

* **BlueZ calls fail with `AccessDenied`** — add yourself to the `bluetooth`
  group and start a fresh session: `sudo usermod -aG bluetooth "$USER"`, then
  log out and back in.
* **Notification monitor warns about the session bus at startup** — the daemon
  started before your desktop session bus was ready. Harmless if started by
  hand (your session's already up); the `graphical-session.target` ordering in
  the unit handles it for login/boot starts.


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
  - [ ] Categorization (Text/Call/Other)
- [ ] Weather
- [ ] Health
  - [ ] Steps
  - [ ] Sleep
  - [ ] Heartrate
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
  - [ ] Categorizations
- [x] AppMessages
  - [x] External applications
- [ ] Music
- [ ] Health
- [ ] Weather


## Why one repo

The client and daemon **must** agree on the wire contract. A monorepo makes a
contract change one atomic commit that CI runs across both ends — there's never
a window where they disagree.

"""pebble_le_proto — the single source of truth for the daemon<->client contract.

Both the daemon (which *exports* the D-Bus interface) and the client (which
*calls* it) import from here. Nothing about the wire shape — bus name, object
path, method/signal signatures, or how an AppMessage value dict is marshalled —
is written down anywhere else. That is the entire point of this package: if the
encoding has to match on both ends, it should physically exist in only one
place so it cannot drift.

This module deliberately has NO dbus-fast import. It is constants plus pure
codec functions. The daemon and client each open their own bus; this package
just tells them what to say on it.
"""

from __future__ import annotations

from .codec import (
    WireValue,
    decode_data_dict,
    decode_value,
    encode_data_dict,
    encode_value,
)

# ---------------------------------------------------------------------------
# D-Bus identity. Change these in ONE place and both ends follow.
# ---------------------------------------------------------------------------

# Well-known bus name the daemon owns. An app checks daemon liveness by asking
# the bus whether this name has an owner (see client.is_daemon_running()).
BUS_NAME = "org.pebble_le.Daemon"

# Object path the daemon exports the interface on.
OBJECT_PATH = "/org/pebble_le/Daemon"

# Interface name. Methods/signals/properties below all live on this interface.
INTERFACE = "org.pebble_le.Daemon"

# The daemon talks to BlueZ on the SYSTEM bus, but the daemon<->client API is
# better placed on the SESSION bus: it is per-user, needs no polkit rules for a
# normal desktop app to call it, and matches "apps the logged-in user runs".
# If you later want a system-wide daemon serving all users, flip this and add
# a D-Bus policy file. Kept here so both ends agree.
USE_SESSION_BUS = True


__all__ = [
    "BUS_NAME",
    "INTERFACE",
    "OBJECT_PATH",
    "USE_SESSION_BUS",
    "WireValue",
    "decode_data_dict",
    "decode_value",
    "encode_data_dict",
    "encode_value",
]

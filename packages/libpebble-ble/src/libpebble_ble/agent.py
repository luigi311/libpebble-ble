"""BlueZ pairing agent that auto-accepts the Pebble's bonding requests.

Why this exists: BLE bonding (SMP) needs an org.bluez.Agent1 on the host side
to answer the pairing request. Desktop environments register one (the
"Pair device?" dialog); a headless Python process has none, so BlueZ fails
the request with org.bluez.Error.AuthenticationFailed the moment either side
initiates security.

The Pebble flow is: the human confirms the pairing ON THE WATCH (the ✓/✗
screen), and the phone side just says yes. So this agent auto-accepts
confirmations/authorizations — but only for the one device address it was
created for; requests from any other device are rejected.

It registers as the *default* agent for the duration of pairing, because the
canonical Pebble sequence is watch-initiated bonding (triggered by a write to
the pairing-trigger characteristic), and BlueZ routes incoming security
requests to the default agent.
"""

from __future__ import annotations

from dbus_fast import DBusError
from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType
from dbus_fast.service import ServiceInterface, method
from loguru import logger

BLUEZ = "org.bluez"
AGENT_MANAGER_IFACE = "org.bluez.AgentManager1"
AGENT_PATH = "/org/pebble_le/agent"
# KeyboardDisplay lets BlueZ negotiate either Just-Works or numeric
# comparison; we auto-confirm both. (The human's consent lives on the watch
# screen, not here.)
AGENT_CAPABILITY = "KeyboardDisplay"


def addr_to_path_fragment(address: str) -> str:
    """BlueZ encodes a device address into its object path as dev_AA_BB_..."""
    return "dev_" + address.upper().replace(":", "_")


class _Agent(ServiceInterface):
    """org.bluez.Agent1 implementation that accepts pairing for one device."""

    def __init__(self, address: str | None):
        super().__init__("org.bluez.Agent1")
        self._fragment = addr_to_path_fragment(address) if address else None

    def _check(self, device: str) -> None:
        """Reject anything that isn't our watch."""
        if self._fragment and self._fragment not in device:
            msg = f"pebble_le agent only pairs its own watch, not {device}"
            raise DBusError("org.bluez.Error.Rejected", msg)

    @method()
    def Release(self):
        logger.debug("pairing agent released by BlueZ")

    @method()
    def RequestPinCode(self, device: "o") -> "s":  # noqa: F821
        self._check(device)
        logger.debug(f"agent RequestPinCode for {device} -> '0000'")
        return "0000"

    @method()
    def DisplayPinCode(self, device: "o", pincode: "s"):  # noqa: F821
        logger.info(f"pairing PIN for {device}: {pincode}")

    @method()
    def RequestPasskey(self, device: "o") -> "u":  # noqa: F821
        self._check(device)
        logger.debug(f"agent RequestPasskey for {device} -> 0")
        return 0

    @method()
    def DisplayPasskey(self, device: "o", passkey: "u", entered: "q"):  # noqa: F821
        logger.info(f"pairing passkey for {device}: {passkey:06d}")

    @method()
    def RequestConfirmation(self, device: "o", passkey: "u"):  # noqa: F821
        self._check(device)
        logger.debug(f"agent auto-confirming passkey {passkey:06d} for {device}")
        # Returning normally = confirmed.

    @method()
    def RequestAuthorization(self, device: "o"):  # noqa: F821
        self._check(device)
        logger.debug(f"agent auto-authorizing {device}")

    @method()
    def AuthorizeService(self, device: "o", uuid: "s"):  # noqa: F821
        self._check(device)
        logger.debug(f"agent auto-authorizing service {uuid} for {device}")

    @method()
    def Cancel(self):
        logger.debug("pairing agent: pending request cancelled by BlueZ")


class PairingAgent:
    """Temporarily registers an auto-accept agent as the BlueZ default.

    Usage:
        agent = PairingAgent("E6:94:0A:D4:D5:DC")
        await agent.register()
        ...   # pair here (watch- or host-initiated)
        await agent.unregister()

    Note: while registered as default this agent fields pairing requests
    system-wide; it rejects every device except the configured address, but
    a desktop pairing dialog for OTHER devices may not appear during that
    window. Registration is therefore kept as short as possible.
    """

    def __init__(self, address: str | None = None):
        self._address = address
        self._bus: MessageBus | None = None
        self._mgr = None

    async def __aenter__(self) -> "PairingAgent":
        await self.register()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.unregister()

    async def register(self) -> None:
        self._bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
        self._bus.export(AGENT_PATH, _Agent(self._address))
        introspect = await self._bus.introspect(BLUEZ, "/org/bluez")
        obj = self._bus.get_proxy_object(BLUEZ, "/org/bluez", introspect)
        self._mgr = obj.get_interface(AGENT_MANAGER_IFACE)
        await self._mgr.call_register_agent(AGENT_PATH, AGENT_CAPABILITY)
        # Default-agent status is what routes WATCH-initiated bonding to us.
        try:
            await self._mgr.call_request_default_agent(AGENT_PATH)
            logger.debug("pairing agent registered as system default")
        except Exception as e:
            logger.warning(
                f"agent registered but could not become default ({e!r}); "
                f"watch-initiated pairing may be answered by another agent"
            )

    async def unregister(self) -> None:
        bus, self._bus = self._bus, None
        if bus is None:
            return
        if self._mgr is not None:
            try:
                await self._mgr.call_unregister_agent(AGENT_PATH)
            except Exception as e:
                logger.debug(f"agent unregister failed: {e!r}")
            self._mgr = None
        bus.disconnect()

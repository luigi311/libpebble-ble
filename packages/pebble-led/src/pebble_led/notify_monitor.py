"""Session-bus notification monitor → forwards desktop notifications to the watch.

Desktop notifications are method calls to org.freedesktop.Notifications.Notify
on the *session* bus. We become a passive monitor (BecomeMonitor) and copy each
Notify call out to a callback, which the daemon turns into a watch notification.

Monitoring is receive-only: we see the calls the real notification daemon
handles, we don't intercept or replace them, so notifications still appear on
screen normally.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from dbus_fast import Message, MessageFlag, MessageType
from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType
from loguru import logger

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

NOTIFICATIONS_IFACE = "org.freedesktop.Notifications"
NOTIFICATIONS_PATH = "/org/freedesktop/Notifications"

# Match rule: Notify method calls on the notifications interface.
_MATCH_RULE = f"type='method_call',interface='{NOTIFICATIONS_IFACE}',member='Notify'"


class NotificationMonitor:
    """Eavesdrops on the session bus for Notify calls.

    on_notification(app_name, summary, body) is invoked (as a coroutine) for
    each captured notification. The monitor owns its own session-bus
    connection, independent of whichever bus the daemon runs on.
    """

    def __init__(
        self,
        on_notification: Callable[[str, str, str], Awaitable[None]],
    ) -> None:
        self._on_notification = on_notification
        self._bus: MessageBus | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._bus = await MessageBus(bus_type=BusType.SESSION).connect()

        # Eavesdrop via AddMatch (not BecomeMonitor). On dbus-fast a
        # BecomeMonitor connection stops routing messages to add_message_handler
        # the way we need; the classic eavesdrop match keeps normal delivery
        # while still copying us directed method_calls to the notification daemon.
        await self._bus.call(
            Message(
                destination="org.freedesktop.DBus",
                path="/org/freedesktop/DBus",
                interface="org.freedesktop.DBus",
                member="AddMatch",
                signature="s",
                body=[
                    "eavesdrop=true,type='method_call',"
                    f"interface='{NOTIFICATIONS_IFACE}',member='Notify'"
                ],
            )
        )
        self._bus.add_message_handler(self._handle_message)
        logger.success("notification monitor active (eavesdrop match)")

    def _handle_message(self, message: Message) -> bool:
        try:
            logger.info(
                f"MON: type={message.message_type} "
                f"path={message.path} iface={message.interface} member={message.member}"
            )
            # We only asked for Notify method calls, but be defensive.
            if (
                message.message_type != MessageType.METHOD_CALL
                or message.interface != NOTIFICATIONS_IFACE
                or message.member != "Notify"
            ):
                return False

            # This is an EAVESDROPPED copy of a call addressed to the real
            # notifications daemon — NOT a call to us. Mark it no-reply so
            # dbus-fast doesn't send an UnknownMethod error back to the sender
            # (which would break the real notification).
            message.flags |= MessageFlag.NO_REPLY_EXPECTED

            body = message.body
            # Notify signature: susssasa{sv}i
            # 0:app_name 1:replaces_id 2:app_icon 3:summary 4:body 5:actions
            # 6:hints 7:expire_timeout
            app_name = body[0] or ""
            summary = body[3] or ""
            notif_body = body[4] or ""

            logger.debug(f"captured notification: app={app_name!r} summary={summary!r}")
            self._loop.call_soon_threadsafe(
                lambda: self._loop.create_task(self._forward(app_name, summary, notif_body))
            )
            return True
        except Exception as e:
            # never let the handler die
            logger.warning(f"monitor handler error (continuing): {e!r}")
        return False  # monitor never consumes

    async def _forward(self, app_name: str, summary: str, body: str) -> None:
        try:
            await self._on_notification(app_name, summary, body)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"notification forward failed: {e!r}")

    async def stop(self) -> None:
        bus, self._bus = self._bus, None
        if bus is not None:
            try:
                bus.remove_message_handler(self._handle_message)
            except Exception:  # noqa: BLE001, S110
                pass
            bus.disconnect()

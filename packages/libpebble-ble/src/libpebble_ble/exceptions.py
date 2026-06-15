"""Exceptions raised by pebble_le."""


class PebbleNackError(Exception):
    """Raised when the watch NACKs an AppMessage sent with wait_ack=True."""

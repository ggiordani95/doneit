"""Identifier generation.

Task and run IDs must be globally unique and time-ordered so that database
indexes stay dense and logs sort naturally. Python 3.12 has no uuid7 in the
standard library, so we generate one per RFC 9562 on top of ``os.urandom``.
"""

from __future__ import annotations

import os
import time
import uuid

__all__ = ["correlation_id", "new_id", "run_id", "task_id", "uuid7"]


def uuid7() -> uuid.UUID:
    """A time-ordered UUID (RFC 9562 version 7).

    Layout: 48 bits of Unix time in milliseconds, 4 version bits, 12 random
    bits, 2 variant bits, 62 random bits.
    """
    ms = int(time.time() * 1000) & 0xFFFF_FFFF_FFFF
    rand_a = int.from_bytes(os.urandom(2), "big") & 0x0FFF
    rand_b = int.from_bytes(os.urandom(8), "big") & 0x3FFF_FFFF_FFFF_FFFF

    value = ms << 80
    value |= 0x7 << 76
    value |= rand_a << 64
    value |= 0b10 << 62
    value |= rand_b
    return uuid.UUID(int=value)


def new_id(prefix: str = "") -> str:
    """A prefixed, time-ordered identifier, e.g. ``task-018f...``."""
    value = str(uuid7())
    return f"{prefix}-{value}" if prefix else value


def task_id() -> str:
    return new_id("task")


def run_id() -> str:
    return new_id("run")


def correlation_id(external_key: str | None = None) -> str:
    """Correlation ID for a run.

    When a run is triggered by an external object (a Jira issue key, for
    example) the key is embedded so traces are searchable by ticket.
    """
    base = new_id("corr")
    return f"{external_key}:{base}" if external_key else base

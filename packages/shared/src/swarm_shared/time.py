"""Time helpers. Everything in the swarm is timezone-aware UTC."""

from datetime import UTC, datetime


def utcnow() -> datetime:
    """Current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


def isoformat(dt: datetime) -> str:
    """RFC 3339 / ISO 8601 string with a trailing Z, as used on the wire."""
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")

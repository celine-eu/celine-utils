from datetime import datetime, timezone


def utc_now() -> datetime:
    """Return current timezone-aware UTC datetime"""
    return datetime.now(timezone.utc)


def parse_unix_timestamp(unix_timestamp: int) -> datetime:
    """Convert Unix timestamp to UTC datetime."""
    return datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)

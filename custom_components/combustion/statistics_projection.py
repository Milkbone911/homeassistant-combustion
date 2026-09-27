"""S5 cloud-history projection into Recorder statistics."""
from __future__ import annotations

from datetime import datetime

from homeassistant.util import dt as dt_util


def datetime_from_us(value: int) -> datetime:
    """Convert archive microseconds to an aware UTC datetime."""
    return datetime.fromtimestamp(value / 1_000_000, tz=dt_util.UTC)

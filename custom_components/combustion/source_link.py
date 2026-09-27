"""S5 source-link helpers."""
from __future__ import annotations


def canonical_probe_serial(value: str) -> str | None:
    """Normalize a 32-bit Combustion probe serial."""
    value = value.strip()
    if not value or len(value) > 8:
        return None
    try:
        number = int(value, 16)
    except ValueError:
        return None
    if number <= 0 or number > 0xFFFFFFFF:
        return None
    return f"{number:08X}"

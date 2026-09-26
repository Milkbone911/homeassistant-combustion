"""Sanitized diagnostics for Combustion runtime health."""
from __future__ import annotations

from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .cloud.ha import cloud_linked


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> dict[str, Any]:
    """Return allowlisted health only; never expose cloud identities or secrets."""
    runtime = entry.runtime_data
    health = runtime.cloud_health
    return {
        "cloud": {
            "linked": cloud_linked(entry.data),
            "status": health.status,
            "probe_count": health.probe_count,
            "error_category": health.error_category,
            "verified_generation": health.verified_generation,
        },
        "local": {
            "active_connection_enabled": runtime.connection_manager.enabled,
            "optional_task_count": runtime.optional_task_count,
        },
    }

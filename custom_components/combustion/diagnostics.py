"""Sanitized diagnostics for Combustion runtime health."""
from __future__ import annotations

from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .cloud.ha import cloud_linked
from .storage.database import ArchiveStatus


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> dict[str, Any]:
    """Return allowlisted health only; never expose credentials/source identity."""
    runtime = entry.runtime_data
    cloud = runtime.cloud_health
    archive = runtime.archive_health
    database = runtime.archive_database
    if database is not None and archive.status is ArchiveStatus.READY:
        await database.async_refresh_sizes()
    sync = runtime.sync_health
    return {
        "cloud": {
            "linked": cloud_linked(entry.data),
            "status": cloud.status,
            "probe_count": cloud.probe_count,
            "error_category": cloud.error_category,
            "verified_generation": cloud.verified_generation,
        },
        "archive": {
            "status": archive.status,
            "schema_version": archive.schema_version,
            "sqlite_version": archive.sqlite_version,
            "journal_mode": archive.journal_mode,
            "error_category": archive.error_category,
            "database_generation": archive.database_generation,
            "writer_queue_depth": archive.writer_queue_depth,
            "reader_limit": archive.reader_limit,
            "last_success_us": archive.last_success_us,
            "last_backup_us": archive.last_backup_us,
            "database_bytes": archive.database_bytes,
            "wal_bytes": archive.wal_bytes,
        },
        "sync": {
            "status": sync.status,
            "last_success_us": sync.last_success_us,
            "last_discovery_us": sync.last_discovery_us,
            "last_error_category": sync.last_error_category,
            "ready_work": sync.ready_work,
            "running_work": sync.running_work,
            "failed_work": sync.failed_work,
        },
        "local": {
            "active_connection_enabled": runtime.connection_manager.enabled,
            "optional_task_count": runtime.optional_task_count,
        },
    }

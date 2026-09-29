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
    capture = runtime.local_capture_health
    selection = runtime.probe_manager.observation_counters
    source_links = runtime.source_link_health
    projection = runtime.statistics_projection_health
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
        "source_links": {
            "status": source_links.status,
            "cloud_probe_sources": source_links.cloud_probe_sources,
            "local_probe_sources": source_links.local_probe_sources,
            "active_links": source_links.active_links,
            "unresolved_cloud_sources": source_links.unresolved_cloud_sources,
            "ambiguous_cloud_sources": source_links.ambiguous_cloud_sources,
            "last_reconcile_us": source_links.last_reconcile_us,
            "last_error_category": source_links.last_error_category,
        },
        "statistics_projection": {
            "status": projection.status,
            "linked_sources": projection.linked_sources,
            "resolved_entities": projection.resolved_entities,
            "queued_hours": projection.queued_hours,
            "skipped_existing_hours": projection.skipped_existing_hours,
            "last_run_us": projection.last_run_us,
            "last_error_category": projection.last_error_category,
        },
        "local_capture": {
            "status": capture.status,
            "queue_depth": capture.queue_depth,
            "committed_observations": capture.committed_observations,
            "dropped_observations": capture.dropped_observations,
            "coalesced_observations": capture.coalesced_observations,
            "last_commit_us": capture.last_commit_us,
            "last_error_category": capture.last_error_category,
            "received_observations": selection["received"],
            "selected_observations": selection["selected"],
            "suppressed_repeater_observations": selection[
                "suppressed_repeater"
            ],
        },
        "local": {
            "active_connection_enabled": runtime.connection_manager.enabled,
            "optional_task_count": runtime.optional_task_count,
        },
    }

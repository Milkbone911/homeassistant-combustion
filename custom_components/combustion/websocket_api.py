"""Admin-only S3 archive status, query, sync and backup WebSocket API."""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import voluptuous as vol

import homeassistant.helpers.config_validation as cv
from homeassistant.components import websocket_api
from homeassistant.components.websocket_api import (
    ERR_NOT_FOUND,
    ERR_NOT_SUPPORTED,
    ERR_UNKNOWN_ERROR,
)
from homeassistant.core import HomeAssistant, callback

from .const import DOMAIN
from .reconciliation.sync import MAX_WORK_PER_CYCLE
from .storage.database import ArchiveStatus

WS_STATUS = "combustion/archive/status"
WS_SESSIONS = "combustion/archive/sessions"
WS_SAMPLE_VERSIONS = "combustion/archive/sample_versions"
WS_SYNC_NOW = "combustion/archive/sync_now"
WS_BACKUP = "combustion/archive/backup"


@callback
def async_register_websocket_handlers(hass: HomeAssistant) -> None:
    """Register the bounded admin archive commands once."""
    websocket_api.async_register_command(hass, websocket_archive_status)
    websocket_api.async_register_command(hass, websocket_archive_sessions)
    websocket_api.async_register_command(hass, websocket_archive_sample_versions)
    websocket_api.async_register_command(hass, websocket_archive_sync_now)
    websocket_api.async_register_command(hass, websocket_archive_backup)


def _runtime(hass: HomeAssistant):
    """Return the singleton loaded runtime, or None when the entry is unloaded."""
    entries = hass.config_entries.async_loaded_entries(DOMAIN)
    if len(entries) != 1:
        return None
    return entries[0].runtime_data


@websocket_api.require_admin
@websocket_api.websocket_command({vol.Required("type"): WS_STATUS})
@websocket_api.async_response
async def websocket_archive_status(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Return sanitized archive/sync health plus semantic counts."""
    runtime = _runtime(hass)
    if runtime is None:
        connection.send_error(msg["id"], ERR_NOT_FOUND, "Combustion entry is not loaded")
        return

    archive = runtime.archive_health
    sync = runtime.sync_health
    counts = None
    queue = None
    if runtime.archive_repository is not None and archive.status is ArchiveStatus.READY:
        try:
            counts = await runtime.archive_repository.async_archive_counts()
            queue = await runtime.archive_repository.async_queue_counts()
        except Exception:  # noqa: BLE001
            connection.send_error(
                msg["id"], ERR_UNKNOWN_ERROR, "Archive status query failed"
            )
            return

    connection.send_result(
        msg["id"],
        {
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
            "counts": counts,
            "queue": queue,
        },
    )


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_SESSIONS,
        vol.Optional("limit", default=50): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=100)
        ),
        vol.Optional("before_seen_us"): vol.All(vol.Coerce(int), vol.Range(min=0)),
        vol.Optional("before_session_id"): cv.string,
    }
)
@websocket_api.async_response
async def websocket_archive_sessions(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Return one bounded page of source-session summaries."""
    runtime = _runtime(hass)
    repository = None if runtime is None else runtime.archive_repository
    if repository is None:
        connection.send_error(msg["id"], ERR_NOT_SUPPORTED, "Archive is not ready")
        return

    before_seen = msg.get("before_seen_us")
    before_session = msg.get("before_session_id")
    if (before_seen is None) != (before_session is None):
        connection.send_error(
            msg["id"],
            ERR_NOT_SUPPORTED,
            "Both cursor fields are required together",
        )
        return

    try:
        rows = await repository.async_list_sessions(
            limit=msg["limit"],
            before_seen_us=before_seen,
            before_session_id=before_session,
        )
    except Exception:  # noqa: BLE001
        connection.send_error(
            msg["id"], ERR_UNKNOWN_ERROR, "Archive session query failed"
        )
        return

    next_cursor = None
    if len(rows) == msg["limit"] and rows:
        last = rows[-1]
        next_cursor = {
            "before_seen_us": last["last_seen_us"],
            "before_session_id": last["session_id"],
        }

    connection.send_result(
        msg["id"],
        {"sessions": rows, "next_cursor": next_cursor},
    )


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_SAMPLE_VERSIONS,
        vol.Required("session_id"): cv.string,
        vol.Required("sequence"): vol.All(vol.Coerce(int), vol.Range(min=0)),
    }
)
@websocket_api.async_response
async def websocket_archive_sample_versions(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Return bounded version/conflict metadata for one source sample."""
    runtime = _runtime(hass)
    repository = None if runtime is None else runtime.archive_repository
    if repository is None:
        connection.send_error(msg["id"], ERR_NOT_SUPPORTED, "Archive is not ready")
        return

    try:
        versions = await repository.async_sample_versions(
            msg["session_id"], msg["sequence"]
        )
    except Exception:  # noqa: BLE001
        connection.send_error(
            msg["id"], ERR_UNKNOWN_ERROR, "Archive version query failed"
        )
        return
    connection.send_result(msg["id"], {"versions": versions})


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_SYNC_NOW,
        vol.Optional("max_work", default=MAX_WORK_PER_CYCLE): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=MAX_WORK_PER_CYCLE)
        ),
    }
)
@websocket_api.async_response
async def websocket_archive_sync_now(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Run one bounded cycle through the already-owned sync supervisor."""
    runtime = _runtime(hass)
    supervisor = None if runtime is None else runtime.sync_supervisor
    if supervisor is None:
        connection.send_error(
            msg["id"], ERR_NOT_SUPPORTED, "Cloud history sync is not active"
        )
        return
    try:
        processed = await supervisor.async_sync_cycle(
            force_context_refresh=True,
            max_work=msg["max_work"],
        )
    except Exception:  # noqa: BLE001
        connection.send_error(msg["id"], ERR_UNKNOWN_ERROR, "History sync failed")
        return
    connection.send_result(msg["id"], {"processed_work_units": processed})


@websocket_api.require_admin
@websocket_api.websocket_command({vol.Required("type"): WS_BACKUP})
@websocket_api.async_response
async def websocket_archive_backup(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Create a consistent archive backup at the fixed internal backup path."""
    runtime = _runtime(hass)
    database = None if runtime is None else runtime.archive_database
    if (
        database is None
        or runtime is None
        or runtime.archive_health.status is not ArchiveStatus.READY
    ):
        connection.send_error(msg["id"], ERR_NOT_SUPPORTED, "Archive is not ready")
        return

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    destination = database.path.parent / "backups" / f"archive-{stamp}.sqlite3"
    try:
        result = await database.async_backup(destination)
    except Exception:  # noqa: BLE001
        connection.send_error(msg["id"], ERR_UNKNOWN_ERROR, "Archive backup failed")
        return

    connection.send_result(
        msg["id"],
        {
            "database_file": Path(result["database"]).name,
            "manifest_file": Path(result["manifest"]).name,
            "created_at_us": result["created_at_us"],
            "sha256": result["sha256"],
            "counts": result["counts"],
        },
    )

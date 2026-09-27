"""Typed runtime ownership for one Combustion config entry."""
from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .bluetooth_listener import BluetoothListener
from .cloud.ha import CloudLinkHealth
from .connection_manager import ConnectionManager
from .control_manager import ControlManager
from .local_capture import (
    LocalCaptureHealth,
    LocalCaptureStatus,
    LocalCaptureSupervisor,
)
from .prediction_manager import PredictionManager
from .probe_manager import ProbeManager
from .reconciliation.sync import CloudSyncSupervisor, SyncHealth
from .source_link import SourceLinkRepository
from .statistics_projection import StatisticsProjectionHealth
from .storage.database import (
    ArchiveDatabase,
    ArchiveHealth,
    ArchiveShutdownIncomplete,
)
from .storage.repository import ArchiveRepository


@dataclass(slots=True)
class SourceLinkHealth:
    """Sanitized S5 cross-source link health."""

    status: str = "disabled"
    cloud_probe_sources: int = 0
    local_probe_sources: int = 0
    active_links: int = 0
    unresolved_cloud_sources: int = 0
    ambiguous_cloud_sources: int = 0
    last_reconcile_us: int | None = None
    last_error_category: str | None = None


@dataclass(slots=True)
class CombustionRuntime:
    """Objects owned by one loaded Combustion config entry.

    Local BLE/GATT managers retain their established lifecycle callbacks.
    Optional cloud/archive work is owned here so unload can stop intake,
    cancel supervisors, and then close the thread-affine archive in order.
    """

    bluetooth_listener: BluetoothListener
    probe_manager: ProbeManager
    connection_manager: ConnectionManager
    prediction_manager: PredictionManager
    control_manager: ControlManager
    cloud_health: CloudLinkHealth = field(default_factory=CloudLinkHealth)
    archive_health: ArchiveHealth = field(default_factory=ArchiveHealth)
    sync_health: SyncHealth = field(default_factory=SyncHealth)
    local_capture_health: LocalCaptureHealth = field(default_factory=LocalCaptureHealth)
    source_link_health: SourceLinkHealth = field(default_factory=SourceLinkHealth)
    statistics_projection_health: StatisticsProjectionHealth = field(
        default_factory=StatisticsProjectionHealth
    )
    archive_database: ArchiveDatabase | None = field(default=None, repr=False)
    archive_repository: ArchiveRepository | None = field(default=None, repr=False)
    source_link_repository: SourceLinkRepository | None = field(default=None, repr=False)
    sync_supervisor: CloudSyncSupervisor | None = field(default=None, repr=False)
    local_capture_supervisor: LocalCaptureSupervisor | None = field(
        default=None, repr=False
    )
    _optional_tasks: set[asyncio.Task[Any]] = field(
        default_factory=set, init=False, repr=False
    )
    _stopping: bool = field(default=False, init=False, repr=False)
    _stopped: bool = field(default=False, init=False, repr=False)

    @property
    def stopping(self) -> bool:
        """Return whether optional runtime work is being stopped."""
        return self._stopping

    @property
    def stopped(self) -> bool:
        """Return whether optional runtime work completed shutdown."""
        return self._stopped

    @property
    def optional_task_count(self) -> int:
        """Return the number of tracked optional supervisor tasks."""
        return len(self._optional_tasks)

    def create_optional_task(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        coro: Coroutine[Any, Any, Any],
        name: str,
    ) -> asyncio.Task[Any]:
        """Create and track entry-owned optional work."""
        if self._stopping or self._stopped:
            coro.close()
            raise RuntimeError("Combustion runtime is stopping")
        task = entry.async_create_background_task(hass, coro, name)
        self._optional_tasks.add(task)
        task.add_done_callback(self._optional_tasks.discard)
        return task

    async def async_stop(self) -> None:
        """Idempotently stop optional work before existing BLE/GATT cleanup.

        Refuse new archive work first, then cancel/drain supervisors, then
        serialize writer checkpoint/close. A stuck writer is intentionally not
        treated as stopped: its process-local path guard remains held so a
        reload cannot open a second writer on the same archive.
        """
        if self._stopped:
            return
        self._stopping = True

        capture = self.local_capture_supervisor
        if capture is not None:
            try:
                await capture.async_stop()
            except Exception:  # noqa: BLE001 - optional capture must not block unload
                self.local_capture_health.status = LocalCaptureStatus.DEGRADED
                self.local_capture_health.last_error_category = "shutdown"

        database = self.archive_database
        if database is not None:
            database.stop_accepting()

        tasks = tuple(self._optional_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._optional_tasks.clear()

        if database is not None:
            # Local unload remains possible if a filesystem thread is stuck;
            # ArchiveDatabase deliberately retains its ownership guard.
            with suppress(ArchiveShutdownIncomplete):
                await database.async_stop()

        self._stopped = True

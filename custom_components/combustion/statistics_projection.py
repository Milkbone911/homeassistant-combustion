"""S5 Recorder projection gate.

Native Recorder mutation remains disabled until metadata/unit preservation,
source-completeness semantics, ownership/concurrency, bounded workload, and
post-commit confirmation have a separately accepted implementation.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from homeassistant.core import HomeAssistant

from .storage.schema import utc_now_us


class StatisticsProjectionStatus(StrEnum):
    """Sanitized projection state."""

    DISABLED = "disabled"
    RUNNING = "running"
    READY = "ready"
    DEGRADED = "degraded"


@dataclass(slots=True)
class StatisticsProjectionHealth:
    """Projection health using only aggregate counters."""

    status: StatisticsProjectionStatus = StatisticsProjectionStatus.DISABLED
    linked_sources: int = 0
    resolved_entities: int = 0
    queued_hours: int = 0
    skipped_existing_hours: int = 0
    last_run_us: int | None = None
    last_error_category: str | None = None


async def async_project_missing_statistics(
    _hass: HomeAssistant,
    _repository: Any,
    health: StatisticsProjectionHealth,
) -> None:
    """Fail closed until the native-statistics hardening gate is implemented."""
    health.status = StatisticsProjectionStatus.DISABLED
    health.queued_hours = 0
    health.skipped_existing_hours = 0
    health.last_run_us = utc_now_us()
    health.last_error_category = "hardening_required"
    raise RuntimeError("Statistics projection is disabled pending S5 hardening")

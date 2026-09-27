"""S5 cloud-history projection into Recorder statistics."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from homeassistant.components.recorder.const import DOMAIN as RECORDER_DOMAIN
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_import_statistics,
    statistics_during_period,
)
from homeassistant.const import UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util
from homeassistant.util.unit_conversion import TemperatureConverter

from .const import DOMAIN
from .source_link import SourceLinkRepository
from .storage.schema import utc_now_us

_PROJECTION_LAG = timedelta(hours=2)
_IMPORT_BATCH_SIZE = 500
_FIELDS = (
    "virtual_core",
    "virtual_surface",
    "virtual_ambient",
    "t1",
    "t2",
    "t3",
    "t4",
    "t5",
    "t6",
    "t7",
    "t8",
)
_FIELD_UNIQUE_ID_SUFFIX = {
    "virtual_core": "sensor--core",
    "virtual_surface": "sensor--surface",
    "virtual_ambient": "sensor--ambient",
    **{f"t{index}": f"thermistor--{index}" for index in range(1, 9)},
}


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


def datetime_from_us(value: int) -> datetime:
    """Convert archive microseconds to an aware UTC datetime."""
    return datetime.fromtimestamp(value / 1_000_000, tz=dt_util.UTC)


def _entity_id_for_field(
    registry: er.EntityRegistry,
    raw_serial: str,
    field: str,
) -> str | None:
    suffix = _FIELD_UNIQUE_ID_SUFFIX[field]
    return registry.async_get_entity_id(
        "sensor",
        DOMAIN,
        f"{raw_serial}--{suffix}",
    )


def _metadata(entity_id: str) -> StatisticMetaData:
    return {
        "mean_type": StatisticMeanType.ARITHMETIC,
        "has_sum": False,
        "name": None,
        "source": RECORDER_DOMAIN,
        "statistic_id": entity_id,
        "unit_class": TemperatureConverter.UNIT_CLASS,
        "unit_of_measurement": UnitOfTemperature.CELSIUS,
    }


async def _existing_hour_starts(
    hass: HomeAssistant,
    entity_id: str,
    *,
    start: datetime,
    end: datetime,
) -> set[int]:
    """Read existing hours so cloud projection never overwrites Recorder data."""
    existing = await hass.async_add_executor_job(
        statistics_during_period,
        hass,
        start,
        end,
        {entity_id},
        "hour",
        None,
        {"mean", "min", "max"},
    )
    return {
        int(round(float(row["start"]) * 1_000_000))
        for row in existing.get(entity_id, ())
    }

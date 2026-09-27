"""Supported cloud-history projection into Home Assistant long-term statistics."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
import time
from typing import Any, Mapping

from homeassistant.components.recorder.const import DOMAIN as RECORDER_DOMAIN
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_import_statistics,
    get_metadata,
    statistics_during_period,
)
from homeassistant.components.recorder.util import get_instance
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import entity_registry as er
from homeassistant.util.unit_conversion import TemperatureConverter

from .storage.repository import ArchiveRepository

_HOUR_US = 60 * 60 * 1_000_000

RECORDER_TEMPERATURE_TARGET_SUFFIXES: dict[str, str] = {
    **{f"t{index}": f"--thermistor--{index}" for index in range(1, 9)},
    "virtual_core": "--sensor--core",
    "virtual_surface": "--sensor--surface",
    "virtual_ambient": "--sensor--ambient",
}


@dataclass(frozen=True, slots=True)
class RecorderMetricPlan:
    """One validated fill-missing statistics projection."""

    field: str
    entity_id: str
    unit_of_measurement: str
    archive_hours: int
    existing_hours: int
    missing_hours: int
    statistics: tuple[StatisticData, ...]


@dataclass(frozen=True, slots=True)
class RecorderBackfillPlan:
    """Validated bounded projection plan for one explicit cloud source."""

    source_device_id: str
    metrics: tuple[RecorderMetricPlan, ...]

    @property
    def missing_hours(self) -> int:
        """Return total hourly rows that would be queued."""
        return sum(metric.missing_hours for metric in self.metrics)


def recorder_target_candidates(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> dict[str, list[str]]:
    """Return existing Combustion temperature entities eligible for explicit mapping."""
    registry = er.async_get(hass)
    entries = er.async_entries_for_config_entry(registry, entry.entry_id)
    result = {field: [] for field in RECORDER_TEMPERATURE_TARGET_SUFFIXES}
    for registry_entry in entries:
        if registry_entry.domain != "sensor":
            continue
        unique_id = registry_entry.unique_id or ""
        for field, suffix in RECORDER_TEMPERATURE_TARGET_SUFFIXES.items():
            if unique_id.endswith(suffix):
                result[field].append(registry_entry.entity_id)
    for values in result.values():
        values.sort()
    return result


def _validate_entity_mapping(
    hass: HomeAssistant,
    entry: ConfigEntry,
    mapping: Mapping[str, str],
) -> dict[str, str]:
    """Validate explicit cloud-field -> existing Combustion entity mapping."""
    if not mapping:
        raise HomeAssistantError("At least one target entity is required")

    registry = er.async_get(hass)
    validated: dict[str, str] = {}
    seen_entities: set[str] = set()

    for field, entity_id in mapping.items():
        suffix = RECORDER_TEMPERATURE_TARGET_SUFFIXES.get(field)
        if suffix is None:
            raise HomeAssistantError(f"Unsupported Recorder mirror field: {field}")
        if entity_id in seen_entities:
            raise HomeAssistantError("One entity cannot receive multiple source metrics")

        registry_entry = registry.async_get(entity_id)
        if registry_entry is None:
            raise HomeAssistantError(f"Target entity does not exist: {entity_id}")
        if registry_entry.config_entry_id != entry.entry_id:
            raise HomeAssistantError(
                f"Target entity is not owned by this Combustion entry: {entity_id}"
            )
        if registry_entry.domain != "sensor":
            raise HomeAssistantError(f"Target entity is not a sensor: {entity_id}")
        unique_id = registry_entry.unique_id or ""
        if not unique_id.endswith(suffix):
            raise HomeAssistantError(
                f"Target entity does not match source metric {field}: {entity_id}"
            )

        seen_entities.add(entity_id)
        validated[field] = entity_id

    return validated


def _existing_metadata_unit(
    entity_id: str,
    metadata: dict[str, tuple[int, StatisticMetaData]],
) -> str:
    """Return a compatible existing unit or Celsius for a new statistic."""
    row = metadata.get(entity_id)
    if row is None:
        return UnitOfTemperature.CELSIUS

    meta = row[1]
    if meta["source"] != RECORDER_DOMAIN:
        raise HomeAssistantError(
            f"Target statistic has incompatible source: {entity_id}"
        )
    if meta["mean_type"] != StatisticMeanType.ARITHMETIC or meta["has_sum"]:
        raise HomeAssistantError(
            f"Target statistic has incompatible measurement semantics: {entity_id}"
        )

    unit_class = meta.get("unit_class")
    unit = meta.get("unit_of_measurement")
    if unit_class not in (None, TemperatureConverter.UNIT_CLASS):
        raise HomeAssistantError(
            f"Target statistic is not a temperature statistic: {entity_id}"
        )
    if unit not in TemperatureConverter.VALID_UNITS:
        raise HomeAssistantError(
            f"Target statistic has unsupported temperature unit: {entity_id}"
        )
    return str(unit)


async def _async_existing_statistic_hours(
    hass: HomeAssistant,
    entity_id: str,
    start: datetime,
    end: datetime,
) -> set[int]:
    """Read existing hourly long-term statistics on Recorder's DB executor."""
    instance = get_instance(hass)
    result = await instance.async_add_executor_job(
        partial(
            statistics_during_period,
            hass,
            start,
            end,
            {entity_id},
            "hour",
            None,
            {"mean", "min", "max"},
        )
    )
    return {
        int(round(float(row["start"]) * 1_000_000))
        for row in result.get(entity_id, [])
    }


async def async_build_recorder_backfill_plan(
    hass: HomeAssistant,
    entry: ConfigEntry,
    repository: ArchiveRepository,
    *,
    source_device_id: str,
    mapping: Mapping[str, str],
    start_us: int | None = None,
    end_us: int | None = None,
) -> RecorderBackfillPlan:
    """Build a fill-missing-only plan without mutating Recorder."""
    if RECORDER_DOMAIN not in hass.config.components:
        raise HomeAssistantError("Home Assistant Recorder is not loaded")

    validated = _validate_entity_mapping(hass, entry, mapping)
    archive = await repository.async_recorder_hourly_temperature_statistics(
        source_device_id,
        tuple(validated),
        start_us=start_us,
        end_us=end_us,
    )

    # Do not preempt Recorder's native current-hour compiler. Only closed UTC
    # hours are eligible for a historical fill operation.
    current_hour_us = int(time.time() * 1_000_000) // _HOUR_US * _HOUR_US
    archive = {
        field: [
            row for row in rows if int(row["start_us"]) < current_hour_us
        ]
        for field, rows in archive.items()
    }

    entity_ids = set(validated.values())
    instance = get_instance(hass)
    metadata = await instance.async_add_executor_job(
        partial(get_metadata, hass, statistic_ids=entity_ids)
    )

    plans: list[RecorderMetricPlan] = []
    for field, entity_id in validated.items():
        rows = archive.get(field, [])
        target_unit = _existing_metadata_unit(entity_id, metadata)
        convert = TemperatureConverter.converter_factory(
            UnitOfTemperature.CELSIUS,
            target_unit,
        )

        if rows:
            first_us = int(rows[0]["start_us"])
            last_us = int(rows[-1]["start_us"])
            existing = await _async_existing_statistic_hours(
                hass,
                entity_id,
                datetime.fromtimestamp(first_us / 1_000_000, UTC),
                datetime.fromtimestamp(
                    (last_us + _HOUR_US) / 1_000_000,
                    UTC,
                ),
            )
        else:
            existing = set()

        statistics: list[StatisticData] = []
        for row in rows:
            start = int(row["start_us"])
            if start in existing:
                continue
            statistics.append(
                {
                    "start": datetime.fromtimestamp(start / 1_000_000, UTC),
                    "min": convert(float(row["min"])),
                    "max": convert(float(row["max"])),
                    "mean": convert(float(row["mean"])),
                }
            )

        plans.append(
            RecorderMetricPlan(
                field=field,
                entity_id=entity_id,
                unit_of_measurement=target_unit,
                archive_hours=len(rows),
                existing_hours=sum(
                    1 for row in rows if int(row["start_us"]) in existing
                ),
                missing_hours=len(statistics),
                statistics=tuple(statistics),
            )
        )

    return RecorderBackfillPlan(
        source_device_id=source_device_id,
        metrics=tuple(plans),
    )


def async_queue_recorder_backfill(
    hass: HomeAssistant,
    plan: RecorderBackfillPlan,
) -> int:
    """Queue only the missing hourly statistics through Recorder's public API."""
    queued = 0
    for metric in plan.metrics:
        if not metric.statistics:
            continue
        metadata: StatisticMetaData = {
            "mean_type": StatisticMeanType.ARITHMETIC,
            "has_sum": False,
            "name": None,
            "source": RECORDER_DOMAIN,
            "statistic_id": metric.entity_id,
            "unit_class": TemperatureConverter.UNIT_CLASS,
            "unit_of_measurement": metric.unit_of_measurement,
        }
        async_import_statistics(hass, metadata, metric.statistics)
        queued += len(metric.statistics)
    return queued

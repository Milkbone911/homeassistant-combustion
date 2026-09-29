"""Bounded S5 projection into integration-owned Recorder statistics."""
from __future__ import annotations

import asyncio
import hashlib
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from functools import partial
from typing import Any, Mapping, Sequence

from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_metadata,
    statistics_during_period,
)
from homeassistant.const import UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.util.unit_conversion import TemperatureConverter

from .projection_repository import ProjectionJob, ProjectionRepository
from .storage.schema import canonical_json, utc_now_us

CONFIRMATION_TIMEOUT_SECONDS = 30.0
CONFIRMATION_POLL_SECONDS = 0.25
_VALUE_ABS_TOLERANCE = 1e-6
_VALUE_REL_TOLERANCE = 1e-9


class StatisticsProjectionStatus(StrEnum):
    """Sanitized projection runtime state."""

    DISABLED = "disabled"
    PLANNED = "planned"
    RUNNING = "running"
    READY = "ready"
    PARTIAL = "partial"
    CANCELLED = "cancelled"
    DEGRADED = "degraded"


@dataclass(slots=True)
class StatisticsProjectionHealth:
    """Projection health without source identifiers."""

    status: StatisticsProjectionStatus = StatisticsProjectionStatus.DISABLED
    active_job_id: str | None = None
    planned_rows: int = 0
    queued_rows: int = 0
    confirmed_rows: int = 0
    failed_rows: int = 0
    last_run_us: int | None = None
    last_error_category: str | None = None


class ProjectionExecutionError(RuntimeError):
    """Projection execution cannot safely continue."""


def _datetime_from_us(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000_000, tz=UTC)


def _expected_metadata(statistic_id: str) -> StatisticMetaData:
    object_id = statistic_id.split(":", 1)[1]
    return {
        "mean_type": StatisticMeanType.ARITHMETIC,
        "has_sum": False,
        "name": "Combustion Cloud " + object_id.replace("_", " ").title(),
        "source": "combustion",
        "statistic_id": statistic_id,
        "unit_class": TemperatureConverter.UNIT_CLASS,
        "unit_of_measurement": UnitOfTemperature.CELSIUS,
    }


def _compatible_metadata(
    statistic_id: str,
    existing: StatisticMetaData | None,
) -> StatisticMetaData:
    """Preserve compatible metadata; never relabel an existing series."""
    if existing is None:
        return _expected_metadata(statistic_id)
    if existing["statistic_id"] != statistic_id or existing["source"] != "combustion":
        raise ProjectionExecutionError("Projection statistic ownership mismatch")
    if existing["has_sum"] or existing["mean_type"] != StatisticMeanType.ARITHMETIC:
        raise ProjectionExecutionError("Projection statistic semantics mismatch")
    if existing["unit_class"] != TemperatureConverter.UNIT_CLASS:
        raise ProjectionExecutionError("Projection statistic unit class mismatch")
    unit = existing["unit_of_measurement"]
    if unit not in TemperatureConverter.VALID_UNITS:
        raise ProjectionExecutionError("Projection statistic unit is unsupported")
    return dict(existing)


def _convert_row(
    row: Mapping[str, Any],
    unit: str,
) -> dict[str, float | int]:
    convert = TemperatureConverter.converter_factory(
        UnitOfTemperature.CELSIUS,
        unit,
    )
    return {
        "hour_start_us": int(row["hour_start_us"]),
        "min": convert(float(row["min"])),
        "max": convert(float(row["max"])),
        "mean": convert(float(row["mean"])),
    }


def _statistics_match(
    actual: Mapping[str, Any],
    expected: Mapping[str, float | int],
) -> bool:
    for key in ("min", "max", "mean"):
        value = actual.get(key)
        if value is None or not math.isclose(
            float(value),
            float(expected[key]),
            rel_tol=_VALUE_REL_TOLERANCE,
            abs_tol=_VALUE_ABS_TOLERANCE,
        ):
            return False
    return True


def _result_digest(
    statistic_id: str,
    unit: str,
    row: Mapping[str, float | int],
) -> str:
    payload = {
        "statistic_id": statistic_id,
        "unit": unit,
        "hour_start_us": int(row["hour_start_us"]),
        "min": float(row["min"]),
        "max": float(row["max"]),
        "mean": float(row["mean"]),
    }
    return hashlib.sha256(canonical_json(payload, max_bytes=16 * 1024).encode()).hexdigest()


async def _read_metadata(
    hass: HomeAssistant,
    statistic_ids: Sequence[str],
) -> dict[str, tuple[int, StatisticMetaData]]:
    return await hass.async_add_executor_job(
        partial(
            get_metadata,
            hass,
            statistic_ids=set(statistic_ids),
        )
    )


async def _read_statistics(
    hass: HomeAssistant,
    statistic_id: str,
    *,
    start_us: int,
    end_us: int,
) -> dict[int, Mapping[str, Any]]:
    result = await hass.async_add_executor_job(
        partial(
            statistics_during_period,
            hass,
            _datetime_from_us(start_us),
            _datetime_from_us(end_us),
            {statistic_id},
            "hour",
            None,
            {"mean", "min", "max"},
        )
    )
    return {
        int(round(float(row["start"]) * 1_000_000)): row
        for row in result.get(statistic_id, ())
    }


async def _confirm_submitted_rows(
    hass: HomeAssistant,
    repository: ProjectionRepository,
    job_id: str,
    statistic_id: str,
    rows: Sequence[Mapping[str, Any]],
    converted: Mapping[str, Mapping[str, float | int]],
    unit: str,
) -> set[str]:
    """Poll supported Recorder reads until queued rows are observable."""
    if not rows:
        return set()
    start_us = min(int(row["hour_start_us"]) for row in rows)
    end_us = max(int(row["hour_start_us"]) for row in rows) + 60 * 60 * 1_000_000
    deadline = asyncio.get_running_loop().time() + CONFIRMATION_TIMEOUT_SECONDS
    remaining = {str(row["projection_row_id"]): row for row in rows}
    confirmed: dict[str, str] = {}

    while remaining:
        actual = await _read_statistics(
            hass,
            statistic_id,
            start_us=start_us,
            end_us=end_us,
        )
        for row_id, planned in tuple(remaining.items()):
            hour = int(planned["hour_start_us"])
            expected = converted[row_id]
            observed = actual.get(hour)
            if observed is None or not _statistics_match(observed, expected):
                continue
            confirmed[row_id] = _result_digest(statistic_id, unit, expected)
            remaining.pop(row_id)
        if not remaining:
            break
        if asyncio.get_running_loop().time() >= deadline:
            break
        await asyncio.sleep(CONFIRMATION_POLL_SECONDS)

    if confirmed:
        await repository.async_confirm_rows(job_id, confirmed)
    if remaining:
        await repository.async_fail_rows(
            job_id,
            tuple(remaining),
            category="confirmation_timeout",
        )
    return set(confirmed)


def _apply_health(
    health: StatisticsProjectionHealth,
    job: ProjectionJob,
) -> None:
    health.active_job_id = (
        job.projection_job_id if job.state in ("planned", "running") else None
    )
    health.planned_rows = job.planned_rows
    health.queued_rows = job.queued_rows
    health.confirmed_rows = job.confirmed_rows
    health.failed_rows = job.failed_rows
    health.last_run_us = utc_now_us()
    health.last_error_category = job.error_category
    health.status = (
        StatisticsProjectionStatus.READY
        if job.state == "confirmed"
        else StatisticsProjectionStatus.PLANNED
        if job.state == "planned"
        else StatisticsProjectionStatus.RUNNING
        if job.state == "running"
        else StatisticsProjectionStatus.PARTIAL
        if job.state == "partial"
        else StatisticsProjectionStatus.CANCELLED
        if job.state == "cancelled"
        else StatisticsProjectionStatus.DEGRADED
    )


async def _async_execute_projection_job(
    hass: HomeAssistant,
    repository: ProjectionRepository,
    health: StatisticsProjectionHealth,
    execution_lock: asyncio.Lock,
    job_id: str,
) -> ProjectionJob:
    """Execute one bounded external-statistics job and confirm Recorder read-back."""
    async with execution_lock:
        rows = await repository.async_rows_for_execution(job_id)
        if not rows:
            job = await repository.async_complete_empty_job(job_id)
            _apply_health(health, job)
            return job

        health.status = StatisticsProjectionStatus.RUNNING
        health.active_job_id = job_id

        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row["statistic_id"]), []).append(row)

        metadata = await _read_metadata(hass, tuple(grouped))
        for statistic_id, statistic_rows in grouped.items():
            current_job = await repository.async_get_job(job_id)
            if current_job is None:
                raise ProjectionExecutionError("Projection job disappeared")
            if current_job.state == "cancelled":
                _apply_health(health, current_job)
                return current_job

            try:
                existing_meta = metadata.get(statistic_id)
                target_meta = _compatible_metadata(
                    statistic_id,
                    None if existing_meta is None else existing_meta[1],
                )
            except ProjectionExecutionError:
                await repository.async_fail_rows(
                    job_id,
                    tuple(str(row["projection_row_id"]) for row in statistic_rows),
                    category="metadata_incompatible",
                )
                continue

            unit = target_meta["unit_of_measurement"]
            assert unit is not None
            converted = {
                str(row["projection_row_id"]): _convert_row(row, unit)
                for row in statistic_rows
            }
            start_us = min(int(row["hour_start_us"]) for row in statistic_rows)
            end_us = (
                max(int(row["hour_start_us"]) for row in statistic_rows)
                + 60 * 60 * 1_000_000
            )
            existing = await _read_statistics(
                hass,
                statistic_id,
                start_us=start_us,
                end_us=end_us,
            )

            immediate: dict[str, str] = {}
            collisions: list[str] = []
            submit: list[Mapping[str, Any]] = []
            for row in statistic_rows:
                row_id = str(row["projection_row_id"])
                hour = int(row["hour_start_us"])
                observed = existing.get(hour)
                expected = converted[row_id]
                if observed is None:
                    submit.append(row)
                    continue
                if _statistics_match(observed, expected):
                    if bool(row["prior_owned"]) or str(row["state"]) in (
                        "submitting",
                        "queued",
                    ) or (
                        str(row["state"]) == "failed"
                        and row.get("last_error") == "confirmation_timeout"
                    ):
                        immediate[row_id] = _result_digest(
                            statistic_id,
                            unit,
                            expected,
                        )
                    else:
                        collisions.append(row_id)
                    continue
                if bool(row["prior_owned"]):
                    submit.append(row)
                else:
                    collisions.append(row_id)

            if immediate:
                await repository.async_confirm_rows(job_id, immediate)
            if collisions:
                await repository.async_fail_rows(
                    job_id,
                    collisions,
                    category="unowned_existing_hour",
                )
            if not submit:
                continue

            submit_ids = tuple(str(row["projection_row_id"]) for row in submit)
            await repository.async_mark_rows_submitting(job_id, submit_ids)
            statistics: list[StatisticData] = [
                {
                    "start": _datetime_from_us(int(row["hour_start_us"])),
                    "min": float(converted[str(row["projection_row_id"])]["min"]),
                    "max": float(converted[str(row["projection_row_id"])]["max"]),
                    "mean": float(converted[str(row["projection_row_id"])]["mean"]),
                }
                for row in submit
            ]
            try:
                async_add_external_statistics(hass, target_meta, statistics)
            except Exception:
                await repository.async_fail_rows(
                    job_id,
                    submit_ids,
                    category="recorder_enqueue",
                )
                continue
            await repository.async_mark_rows_queued(job_id, submit_ids)
            await _confirm_submitted_rows(
                hass,
                repository,
                job_id,
                statistic_id,
                submit,
                converted,
                unit,
            )

        job = await repository.async_get_job(job_id)
        if job is None:
            raise ProjectionExecutionError("Projection job disappeared")
        _apply_health(health, job)
        return job


async def async_execute_projection_job(
    hass: HomeAssistant,
    repository: ProjectionRepository,
    health: StatisticsProjectionHealth,
    execution_lock: asyncio.Lock,
    job_id: str,
) -> ProjectionJob:
    """Execute with crash-safe job state and sanitized runtime health."""
    try:
        return await _async_execute_projection_job(
            hass,
            repository,
            health,
            execution_lock,
            job_id,
        )
    except asyncio.CancelledError:
        health.status = StatisticsProjectionStatus.PARTIAL
        health.active_job_id = job_id
        health.last_run_us = utc_now_us()
        health.last_error_category = "interrupted"
        raise
    except Exception:
        try:
            job = await repository.async_mark_job_partial(
                job_id,
                category="execution_error",
            )
        except Exception:
            health.status = StatisticsProjectionStatus.DEGRADED
            health.active_job_id = job_id
            health.last_run_us = utc_now_us()
            health.last_error_category = "ledger_error"
            raise
        _apply_health(health, job)
        raise

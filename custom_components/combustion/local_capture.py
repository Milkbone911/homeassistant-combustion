"""Bounded nonblocking S4 BLE/GATT archive capture."""
from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback

from .bluetooth_listener import BluetoothObservation
from .combustion_ble.combustion_probe_data import CombustionProbeData
from .combustion_ble.gauge_data import CombustionGaugeData
from .combustion_ble.node_data import NodeData
from .combustion_ble.prediction_data import (
    PREDICTION_MODES,
    PREDICTION_STATES,
    PREDICTION_TYPES,
)
from .const import LOGGER
from .prediction_manager import PredictionManager, PredictionObservation
from .probe_manager import ProbeManager
from .storage.repository import (
    ArchiveRepository,
    LocalCaptureGapRecord,
    LocalObservationRecord,
    local_observation_identity,
)

_LOGGER = LOGGER.getChild("local-capture")

CAPTURE_POLICY_VERSION = 1
REGULAR_INTERVAL_SECONDS = 1.0
MAX_QUEUE_ITEMS = 256
WRITE_BATCH_SIZE = 50
SHUTDOWN_TIMEOUT_SECONDS = 10.0


class LocalCaptureStatus(StrEnum):
    """Sanitized local capture states."""

    DISABLED = "disabled"
    STARTING = "starting"
    READY = "ready"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    STOPPED = "stopped"


@dataclass(slots=True)
class LocalCaptureHealth:
    """Allowlisted capture health without source identifiers."""

    status: LocalCaptureStatus = LocalCaptureStatus.DISABLED
    queue_depth: int = 0
    committed_observations: int = 0
    dropped_observations: int = 0
    coalesced_observations: int = 0
    last_commit_us: int | None = None
    last_error_category: str | None = None


@dataclass(slots=True)
class _GapAccumulator:
    gap_id: str
    capture_run_id: str
    subject_kind: str | None
    raw_serial: str | None
    scope: str
    reason: str
    first_lost_at_us: int
    last_lost_at_us: int
    dropped_count: int = 1


class LocalCaptureSupervisor:
    """Apply the S4 capture policy without blocking live BLE/GATT callbacks."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        repository: ArchiveRepository,
        probe_manager: ProbeManager,
        prediction_manager: PredictionManager,
        health: LocalCaptureHealth,
    ) -> None:
        """Initialize one entry-owned local archive capture supervisor."""
        self.hass = hass
        self.entry = entry
        self.repository = repository
        self.probe_manager = probe_manager
        self.prediction_manager = prediction_manager
        self.health = health
        self.runtime_generation = str(uuid.uuid4())
        self.capture_run_id: str | None = None

        self._queue: list[LocalObservationRecord] = []
        self._queue_event = asyncio.Event()
        self._pending_regular: dict[tuple[str, str], LocalObservationRecord] = {}
        self._regular_timers: dict[tuple[str, str], asyncio.TimerHandle] = {}
        self._last_regular_emitted: dict[tuple[str, str], float] = {}
        self._last_signature: dict[tuple[str, str], tuple[Any, ...]] = {}
        self._pending_gaps: dict[
            tuple[str | None, str | None, str], _GapAccumulator
        ] = {}
        self._event_ordinal = 0
        self._task: asyncio.Task[Any] | None = None
        self._remove_probe_listener: Callable[[], None] | None = None
        self._remove_prediction_listener: Callable[[], None] | None = None
        self._stopping = False
        self._failed = False

    async def async_start(self) -> None:
        """Recover stale runs, create a run, attach sinks, and start writer."""
        self.health.status = LocalCaptureStatus.STARTING
        try:
            await self.repository.async_recover_interrupted_local_captures()
            self.capture_run_id = await self.repository.async_begin_local_capture(
                runtime_generation=self.runtime_generation,
                policy_version=CAPTURE_POLICY_VERSION,
                regular_interval_ms=int(REGULAR_INTERVAL_SECONDS * 1000),
            )
            self._remove_probe_listener = self.probe_manager.add_observation_listener(
                self._on_ble_observation
            )
            self._remove_prediction_listener = (
                self.prediction_manager.add_observation_listener(
                    self._on_prediction_observation
                )
            )
            self._task = self.entry.async_create_background_task(
                self.hass,
                self._async_writer(),
                "combustion-local-history-capture",
            )
        except BaseException:
            self._failed = True
            self._detach_listeners()
            if self.capture_run_id is not None:
                with suppress(Exception):
                    await self.repository.async_finish_local_capture(
                        self.capture_run_id,
                        terminal_status="interrupted",
                    )
            self.health.status = LocalCaptureStatus.DEGRADED
            self.health.last_error_category = "startup"
            raise
        self.health.status = LocalCaptureStatus.READY

    @callback
    def _on_ble_observation(self, observation: BluetoothObservation) -> None:
        """Normalize one selected HA-delivered BLE observation."""
        if self._stopping or self._failed or self.capture_run_id is None:
            return
        try:
            record, is_regular = self._ble_record(observation)
        except Exception:  # noqa: BLE001
            self.health.dropped_observations += 1
            self.health.last_error_category = "normalize"
            _LOGGER.exception("Failed to normalize selected BLE observation")
            return
        if record is None:
            return

        key = (record.subject_kind, record.raw_serial)
        if is_regular:
            self._offer_regular(key, record)
            return

        # A meaningful transition may occur inside the one-second regular
        # window. Preserve the pending regular point before the transition.
        self._flush_regular(key)
        self._enqueue(record)

    @callback
    def _on_prediction_observation(
        self, observation: PredictionObservation
    ) -> None:
        """Preserve one qualified GATT prediction notification."""
        if self._stopping or self._failed or self.capture_run_id is None:
            return
        prediction = observation.prediction
        payload = {
            "prediction_state": _enum_index(PREDICTION_STATES, prediction.state),
            "prediction_mode": _enum_index(PREDICTION_MODES, prediction.mode),
            "prediction_type": _enum_index(PREDICTION_TYPES, prediction.type),
            "prediction_state_name": prediction.state,
            "prediction_mode_name": prediction.mode,
            "prediction_type_name": prediction.type,
            "prediction_set_point": prediction.setpoint_c,
            "prediction_value_seconds": prediction.seconds_remaining,
            "estimated_core_temperature": prediction.estimated_core_c,
            "heat_start_c": prediction.heat_start_c,
        }
        valid = tuple(
            key
            for key in (
                "prediction_state",
                "prediction_mode",
                "prediction_type",
                "prediction_set_point",
                "prediction_value_seconds",
                "estimated_core_temperature",
            )
            if payload.get(key) is not None
        )
        record = self._new_record(
            subject_kind="probe",
            raw_serial=observation.serial,
            observation_kind="prediction",
            capture_class="prediction",
            received_at_epoch=(
                observation.received_at_epoch
                if observation.received_at_epoch is not None
                else time.time()
            ),
            received_at_monotonic=observation.received_at_monotonic,
            route_kind="gatt",
            freshness_basis="gatt_status",
            valid_fields=valid,
            payload=payload,
            mode_name="normal",
        )
        self._enqueue(record)

    def _ble_record(
        self, observation: BluetoothObservation
    ) -> tuple[LocalObservationRecord | None, bool]:
        data = observation.device_data
        received_epoch = (
            observation.received_at_epoch
            if observation.received_at_epoch is not None
            else time.time()
        )
        freshness_basis = (
            "upstream_monotonic"
            if observation.upstream_time is not None
            else "callback_receipt"
        )
        provenance = {
            "manufacturer_payload_hex": observation.manufacturer_payload_hex,
            "upstream_age_seconds": observation.upstream_age_seconds,
        }

        if isinstance(data, CombustionProbeData):
            serial = data.serial_number
            if serial is None:
                return None, False
            mode_name = data.mode_name
            route = "meat_net" if data.via_repeater else "direct"
            payload: dict[str, Any] = {
                **provenance,
                "battery_ok": data.battery_ok,
                "overheating": data.overheating,
                "overheating_sensor_numbers": data.overheating_sensor_numbers,
                "probe_id": data.probe_id,
                "hops": data.hops,
            }
            valid: list[str] = []
            if mode_name == "normal":
                for index, value in enumerate(data.temperature_data, start=1):
                    key = f"t{index}"
                    payload[key] = value
                    valid.append(key)
                payload["virtual_core"] = data.core_sensor[1]
                payload["virtual_surface"] = data.surface_sensor[1]
                payload["virtual_ambient"] = data.ambient_sensor[1]
                valid.extend(
                    ("virtual_core", "virtual_surface", "virtual_ambient")
                )
            elif mode_name == "instant_read":
                payload["t1"] = data.temperature_data[0]
                valid.append("t1")

            signature = (
                mode_name,
                data.battery_ok,
                data.overheating,
            )
            is_regular = not self._signature_changed("probe", serial, signature)
            return (
                self._new_record(
                    subject_kind="probe",
                    raw_serial=serial,
                    observation_kind="ble",
                    capture_class="regular" if is_regular else "transition",
                    received_at_epoch=received_epoch,
                    received_at_monotonic=observation.received_at_monotonic,
                    route_kind=route,
                    freshness_basis=freshness_basis,
                    valid_fields=tuple(valid),
                    payload=payload,
                    upstream_time=observation.upstream_time,
                    source_address=observation.source_address,
                    scanner_source=observation.scanner_source,
                    rssi=observation.rssi,
                    connectable=observation.connectable,
                    mode_name=mode_name,
                ),
                is_regular,
            )

        if isinstance(data, CombustionGaugeData):
            serial = data.serial_number
            payload = {
                **provenance,
                "t1": data.temperature,
                "sensor_present": data.sensor_present,
                "sensor_overheating": data.sensor_overheating,
                "battery_ok": data.battery_ok,
                "high_alarm": {
                    "is_set": data.high_alarm.is_set,
                    "tripped": data.high_alarm.tripped,
                    "alarming": data.high_alarm.alarming,
                    "temperature": data.high_alarm.temperature,
                },
                "low_alarm": {
                    "is_set": data.low_alarm.is_set,
                    "tripped": data.low_alarm.tripped,
                    "alarming": data.low_alarm.alarming,
                    "temperature": data.low_alarm.temperature,
                },
            }
            signature = (
                data.sensor_present,
                data.sensor_overheating,
                data.battery_ok,
                data.high_alarm.is_set,
                data.high_alarm.tripped,
                data.high_alarm.alarming,
                data.high_alarm.temperature,
                data.low_alarm.is_set,
                data.low_alarm.tripped,
                data.low_alarm.alarming,
                data.low_alarm.temperature,
            )
            is_regular = not self._signature_changed("gauge", serial, signature)
            valid = ("t1",) if data.temperature is not None else ()
            return (
                self._new_record(
                    subject_kind="gauge",
                    raw_serial=serial,
                    observation_kind="ble",
                    capture_class="regular" if is_regular else "transition",
                    received_at_epoch=received_epoch,
                    received_at_monotonic=observation.received_at_monotonic,
                    route_kind="self",
                    freshness_basis=freshness_basis,
                    valid_fields=valid,
                    payload=payload,
                    upstream_time=observation.upstream_time,
                    source_address=observation.source_address,
                    scanner_source=observation.scanner_source,
                    rssi=observation.rssi,
                    connectable=observation.connectable,
                ),
                is_regular,
            )

        if isinstance(data, NodeData):
            serial = data.serial_number
            payload = {
                **provenance,
                "device_type": data.device_type,
                "high_radio_power": data.high_radio_power,
            }
            signature = (data.high_radio_power,)
            is_regular = not self._signature_changed("node", serial, signature)
            return (
                self._new_record(
                    subject_kind="node",
                    raw_serial=serial,
                    observation_kind="ble",
                    capture_class="regular" if is_regular else "transition",
                    received_at_epoch=received_epoch,
                    received_at_monotonic=observation.received_at_monotonic,
                    route_kind="self",
                    freshness_basis=freshness_basis,
                    valid_fields=(),
                    payload=payload,
                    upstream_time=observation.upstream_time,
                    source_address=observation.source_address,
                    scanner_source=observation.scanner_source,
                    rssi=observation.rssi,
                    connectable=observation.connectable,
                ),
                is_regular,
            )

        return None, False

    def _signature_changed(
        self,
        subject_kind: str,
        raw_serial: str,
        signature: tuple[Any, ...],
    ) -> bool:
        key = (subject_kind, raw_serial)
        prior = self._last_signature.get(key)
        self._last_signature[key] = signature
        return prior is None or prior != signature

    def _new_record(
        self,
        *,
        subject_kind: str,
        raw_serial: str,
        observation_kind: str,
        capture_class: str,
        received_at_epoch: float,
        received_at_monotonic: float,
        route_kind: str,
        freshness_basis: str,
        valid_fields: tuple[str, ...],
        payload: dict[str, Any],
        upstream_time: float | None = None,
        source_address: str | None = None,
        scanner_source: str | None = None,
        rssi: float | int | None = None,
        connectable: bool | None = None,
        mode_name: str | None = None,
    ) -> LocalObservationRecord:
        ordinal = self._event_ordinal
        self._event_ordinal += 1
        return LocalObservationRecord(
            observation_id=local_observation_identity(
                self.runtime_generation, ordinal
            ),
            capture_run_id=self.capture_run_id or "",
            subject_kind=subject_kind,
            raw_serial=raw_serial,
            event_ordinal=ordinal,
            observation_kind=observation_kind,
            capture_class=capture_class,
            received_at_us=int(received_at_epoch * 1_000_000),
            received_monotonic_ns=max(0, int(received_at_monotonic * 1_000_000_000)),
            route_kind=route_kind,
            freshness_basis=freshness_basis,
            valid_fields=valid_fields,
            payload=payload,
            upstream_time=upstream_time,
            source_address=source_address,
            scanner_source=scanner_source,
            rssi=rssi,
            connectable=connectable,
            mode_name=mode_name,
        )

    def _offer_regular(
        self,
        key: tuple[str, str],
        record: LocalObservationRecord,
    ) -> None:
        now_mono = record.received_monotonic_ns / 1_000_000_000
        last = self._last_regular_emitted.get(key)
        if last is None or now_mono - last >= REGULAR_INTERVAL_SECONDS:
            self._last_regular_emitted[key] = now_mono
            self._enqueue(record)
            return

        if key in self._pending_regular:
            self.health.coalesced_observations += 1
        self._pending_regular[key] = record
        if key not in self._regular_timers:
            delay = max(0.0, REGULAR_INTERVAL_SECONDS - (now_mono - last))
            self._regular_timers[key] = self.hass.loop.call_later(
                delay, self._flush_regular, key
            )

    @callback
    def _flush_regular(self, key: tuple[str, str]) -> None:
        handle = self._regular_timers.pop(key, None)
        if handle is not None:
            handle.cancel()
        record = self._pending_regular.pop(key, None)
        if record is None:
            return
        self._last_regular_emitted[key] = (
            record.received_monotonic_ns / 1_000_000_000
        )
        self._enqueue(record)

    def _enqueue(self, record: LocalObservationRecord) -> None:
        if self._stopping or self._failed:
            return
        if len(self._queue) >= MAX_QUEUE_ITEMS:
            regular_index = next(
                (
                    index
                    for index, queued in enumerate(self._queue)
                    if queued.capture_class == "regular"
                ),
                None,
            )
            if regular_index is not None:
                dropped = self._queue.pop(regular_index)
            elif record.capture_class == "regular":
                dropped = record
                self._note_drop(dropped, "queue_overflow")
                return
            else:
                dropped = self._queue.pop(0)
            self._note_drop(dropped, "queue_overflow")
        self._queue.append(record)
        self.health.queue_depth = len(self._queue)
        self._queue_event.set()

    def _note_drop(self, record: LocalObservationRecord, reason: str) -> None:
        self.health.dropped_observations += 1
        key = (record.subject_kind, record.raw_serial, reason)
        existing = self._pending_gaps.get(key)
        if existing is None:
            self._pending_gaps[key] = _GapAccumulator(
                gap_id=str(uuid.uuid4()),
                capture_run_id=record.capture_run_id,
                subject_kind=record.subject_kind,
                raw_serial=record.raw_serial,
                scope=record.capture_class,
                reason=reason,
                first_lost_at_us=record.received_at_us,
                last_lost_at_us=record.received_at_us,
            )
            return
        existing.last_lost_at_us = max(
            existing.last_lost_at_us, record.received_at_us
        )
        existing.first_lost_at_us = min(
            existing.first_lost_at_us, record.received_at_us
        )
        existing.dropped_count += 1

    async def _async_writer(self) -> None:
        """Drain bounded local capture work into the archive writer."""
        try:
            while True:
                if not self._queue and not self._pending_gaps:
                    if self._stopping:
                        return
                    self._queue_event.clear()
                    await self._queue_event.wait()
                    continue

                gaps = list(self._pending_gaps.values())
                self._pending_gaps.clear()
                for gap in gaps:
                    await self.repository.async_record_local_capture_gap(
                        LocalCaptureGapRecord(
                            gap_id=gap.gap_id,
                            capture_run_id=gap.capture_run_id,
                            subject_kind=gap.subject_kind,
                            raw_serial=gap.raw_serial,
                            scope=gap.scope,
                            reason=gap.reason,
                            first_lost_at_us=gap.first_lost_at_us,
                            last_lost_at_us=gap.last_lost_at_us,
                            dropped_count=gap.dropped_count,
                            certainty="observed",
                        )
                    )

                batch = self._queue[:WRITE_BATCH_SIZE]
                del self._queue[: len(batch)]
                self.health.queue_depth = len(self._queue)
                if batch:
                    inserted = await self.repository.async_commit_local_observations(
                        batch
                    )
                    self.health.committed_observations += inserted
                    self.health.last_commit_us = int(time.time() * 1_000_000)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            self._failed = True
            self.health.status = LocalCaptureStatus.DEGRADED
            self.health.last_error_category = "archive_write"
            _LOGGER.exception("Local capture writer stopped after archive failure")
        finally:
            self._detach_listeners()

    def _detach_listeners(self) -> None:
        if self._remove_probe_listener is not None:
            self._remove_probe_listener()
            self._remove_probe_listener = None
        if self._remove_prediction_listener is not None:
            self._remove_prediction_listener()
            self._remove_prediction_listener = None

    async def async_stop(self) -> None:
        """Detach intake, flush the selected pending point, then close the run."""
        if self.health.status is LocalCaptureStatus.STOPPED:
            return
        self.health.status = LocalCaptureStatus.STOPPING
        self._detach_listeners()

        # Intake is detached, so no new callbacks can race this final policy
        # flush. Keep enqueueing enabled until the newest pending regular
        # observation for each source has entered the drain queue.
        for key in tuple(self._pending_regular):
            self._flush_regular(key)
        self._stopping = True
        for handle in self._regular_timers.values():
            handle.cancel()
        self._regular_timers.clear()

        self._queue_event.set()
        task = self._task
        if task is not None:
            try:
                async with asyncio.timeout(SHUTDOWN_TIMEOUT_SECONDS):
                    await task
            except TimeoutError:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                self._failed = True
                self.health.last_error_category = "shutdown_timeout"
        self._task = None

        if self.capture_run_id is not None and not self._failed:
            await self.repository.async_finish_local_capture(
                self.capture_run_id,
                terminal_status="clean",
            )
        self.health.queue_depth = len(self._queue)
        self.health.status = (
            LocalCaptureStatus.DEGRADED
            if self._failed
            else LocalCaptureStatus.STOPPED
        )


def _enum_index(values: list[str], value: str) -> int | None:
    try:
        return values.index(value)
    except ValueError:
        return None

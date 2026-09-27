"""S4 bounded local BLE/GATT capture policy tests."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from homeassistant.core import HomeAssistant

from custom_components.combustion.bluetooth_listener import (
    BluetoothObservation,
    parse_advertisement,
)
from custom_components.combustion.combustion_ble.mode_id import ProbeMode
from custom_components.combustion.combustion_ble.prediction_data import PredictionData
from custom_components.combustion.local_capture import (
    LocalCaptureHealth,
    LocalCaptureStatus,
    LocalCaptureSupervisor,
)
from custom_components.combustion.prediction_manager import PredictionObservation
from custom_components.combustion.probe_manager import ProbeManager
from custom_components.combustion.storage.database import ArchiveDatabase
from custom_components.combustion.storage.repository import ArchiveRepository
from tests.utils.bt_utils import create_advertisement, create_combustion_bits


class _FakeEntry:
    """Minimum config-entry task owner used by the capture supervisor."""

    def async_create_background_task(
        self,
        _hass: HomeAssistant,
        coro,
        name: str,
    ) -> asyncio.Task:
        return asyncio.create_task(coro, name=name)


class _FakeObservationManager:
    """Synchronous observation fan-out matching Probe/PredictionManager seams."""

    def __init__(self) -> None:
        self.listeners = []

    def add_observation_listener(self, listener):
        self.listeners.append(listener)

        def remove() -> None:
            if listener in self.listeners:
                self.listeners.remove(listener)

        return remove

    def emit(self, observation) -> None:
        for listener in list(self.listeners):
            listener(observation)


async def _capture(
    hass: HomeAssistant,
    tmp_path: Path,
) -> tuple[
    ArchiveDatabase,
    ArchiveRepository,
    _FakeObservationManager,
    _FakeObservationManager,
    LocalCaptureHealth,
    LocalCaptureSupervisor,
]:
    database = ArchiveDatabase(
        tmp_path / "combustion" / "archive.sqlite3",
        require_qualified_wal=False,
        application_fingerprint="s4-capture-test",
    )
    await database.async_start(allow_create=True)
    repository = ArchiveRepository(database)
    probes = _FakeObservationManager()
    predictions = _FakeObservationManager()
    health = LocalCaptureHealth()
    supervisor = LocalCaptureSupervisor(
        hass,
        _FakeEntry(),  # type: ignore[arg-type]
        repository,
        probes,  # type: ignore[arg-type]
        predictions,  # type: ignore[arg-type]
        health,
    )
    await supervisor.async_start()
    return database, repository, probes, predictions, health, supervisor


def _probe_observation(
    temperature: float,
    *,
    monotonic: float,
    epoch: float,
    mode: ProbeMode = ProbeMode.normal,
) -> BluetoothObservation:
    service_info = create_advertisement(
        create_combustion_bits(
            mode=mode.value,
            temperature_data=[temperature] * 8,
        ),
        connectable=False,
    )
    data = parse_advertisement(service_info)
    assert data is not None
    return BluetoothObservation(
        device_data=data,
        received_at_monotonic=monotonic,
        source_address=service_info.address,
        scanner_source=service_info.source,
        rssi=service_info.rssi,
        upstream_time=service_info.time,
        connectable=service_info.connectable,
        received_at_epoch=epoch,
        upstream_age_seconds=max(0.0, monotonic - float(service_info.time)),
        manufacturer_payload_hex=service_info.manufacturer_data[2503].hex(),
    )


@pytest.mark.asyncio
async def test_regular_policy_keeps_latest_pending_point_on_clean_stop(
    hass: HomeAssistant,
    tmp_path: Path,
):
    """One-second capture coalesces regular points but flushes the newest pending."""
    database, _repository, probes, _predictions, health, supervisor = await _capture(
        hass, tmp_path
    )

    # First observation establishes source/mode evidence as a transition.
    probes.emit(_probe_observation(20.0, monotonic=100.00, epoch=1_000.00))
    # First unchanged-mode regular observation is immediately selected.
    probes.emit(_probe_observation(21.0, monotonic=100.01, epoch=1_000.01))
    # Both of these fall inside the one-second window. Only the newest survives
    # the declared capture policy, and clean shutdown must flush it.
    probes.emit(_probe_observation(22.0, monotonic=100.02, epoch=1_000.02))
    probes.emit(_probe_observation(23.0, monotonic=100.03, epoch=1_000.03))

    await supervisor.async_stop()

    rows = await database.async_read(
        lambda conn: conn.execute(
            """
            SELECT capture_class,t1,received_at_us,raw_json
            FROM local_observations
            ORDER BY event_ordinal
            """
        ).fetchall()
    )
    assert [(row[0], row[1], row[2]) for row in rows] == [
        ("transition", 20.0, 1_000_000_000),
        ("regular", 21.0, 1_000_010_000),
        ("regular", 23.0, 1_000_030_000),
    ]
    retained = json.loads(rows[0][3])
    assert retained["payload"]["manufacturer_payload_hex"]
    assert retained["payload"]["upstream_age_seconds"] == pytest.approx(100.0)
    assert health.coalesced_observations == 1
    assert health.dropped_observations == 0
    assert health.status is LocalCaptureStatus.STOPPED
    assert not probes.listeners
    await database.async_stop()


@pytest.mark.asyncio
async def test_mode_transition_is_not_coalesced_with_regular_samples(
    hass: HomeAssistant,
    tmp_path: Path,
):
    """Instant-read validity/mode transition survives a pending regular point."""
    database, _repository, probes, _predictions, _health, supervisor = await _capture(
        hass, tmp_path
    )

    probes.emit(_probe_observation(20.0, monotonic=200.00, epoch=2_000.00))
    probes.emit(_probe_observation(21.0, monotonic=200.01, epoch=2_000.01))
    probes.emit(_probe_observation(22.0, monotonic=200.02, epoch=2_000.02))
    probes.emit(
        _probe_observation(
            85.0,
            monotonic=200.03,
            epoch=2_000.03,
            mode=ProbeMode.instantRead,
        )
    )
    await supervisor.async_stop()

    rows = await database.async_read(
        lambda conn: conn.execute(
            """
            SELECT capture_class,mode_name,t1,t2
            FROM local_observations
            ORDER BY event_ordinal
            """
        ).fetchall()
    )
    assert rows == [
        ("transition", "normal", 20.0, 20.0),
        ("regular", "normal", 21.0, 21.0),
        ("regular", "normal", 22.0, 22.0),
        ("transition", "instant_read", 85.0, None),
    ]
    await database.async_stop()


@pytest.mark.asyncio
async def test_gatt_prediction_is_archived_independently(
    hass: HomeAssistant,
    tmp_path: Path,
):
    """Qualified GATT prediction observations bypass the regular BLE bucket."""
    database, _repository, _probes, predictions, _health, supervisor = await _capture(
        hass, tmp_path
    )
    prediction = PredictionData(
        state="predicting",
        mode="time_to_removal",
        type="removal",
        setpoint_c=70.0,
        heat_start_c=20.0,
        seconds_remaining=600,
        estimated_core_c=55.0,
    )
    predictions.emit(
        PredictionObservation(
            "10001ccc",
            300.0,
            prediction,
            received_at_epoch=3_000.0,
        )
    )
    await supervisor.async_stop()

    row = await database.async_read(
        lambda conn: conn.execute(
            """
            SELECT observation_kind,capture_class,route_kind,
                prediction_state,prediction_value_seconds,
                prediction_set_point,estimated_core_temperature
            FROM local_observations
            """
        ).fetchone()
    )
    assert row == ("prediction", "prediction", "gatt", 3, 600, 70.0, 55.0)
    await database.async_stop()


@pytest.mark.asyncio
async def test_queue_overflow_prefers_dropping_regular_and_persists_loss(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Overflow protects transition evidence and persists exact observed loss."""
    database, repository, probes, _predictions, health, supervisor = await _capture(
        hass, tmp_path
    )
    monkeypatch.setattr(
        "custom_components.combustion.local_capture.MAX_QUEUE_ITEMS",
        2,
    )

    original_commit = repository.async_commit_local_observations
    writer_entered = asyncio.Event()
    release_writer = asyncio.Event()

    async def slow_commit(observations):
        writer_entered.set()
        await release_writer.wait()
        return await original_commit(observations)

    monkeypatch.setattr(
        repository,
        "async_commit_local_observations",
        slow_commit,
    )

    # The transition is removed from the queue by the writer and held in its
    # blocked first transaction. Later regular observations then exercise only
    # the bounded queue's explicit regular-drop policy.
    probes.emit(_probe_observation(20.0, monotonic=400.0, epoch=4_000.0))
    await asyncio.wait_for(writer_entered.wait(), 1)

    for offset, value in enumerate((21.0, 22.0, 23.0, 24.0), start=2):
        probes.emit(
            _probe_observation(
                value,
                monotonic=400.0 + offset,
                epoch=4_000.0 + offset,
            )
        )

    assert health.dropped_observations == 2
    release_writer.set()
    await supervisor.async_stop()

    gap = await database.async_read(
        lambda conn: conn.execute(
            """
            SELECT scope,reason,dropped_count,certainty
            FROM local_capture_gaps
            WHERE reason='queue_overflow'
            """
        ).fetchone()
    )
    assert gap == ("regular", "queue_overflow", 2, "observed")

    classes = await database.async_read(
        lambda conn: conn.execute(
            """
            SELECT capture_class FROM local_observations
            ORDER BY event_ordinal
            """
        ).fetchall()
    )
    assert classes[0] == ("transition",)
    await database.async_stop()


@pytest.mark.asyncio
async def test_archive_write_failure_degrades_only_capture(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Archive failure detaches capture sinks without raising into BLE fan-out."""
    database, repository, probes, predictions, health, supervisor = await _capture(
        hass, tmp_path
    )

    async def fail_commit(_observations):
        raise OSError("synthetic archive failure")

    monkeypatch.setattr(
        repository,
        "async_commit_local_observations",
        fail_commit,
    )
    probes.emit(_probe_observation(20.0, monotonic=500.0, epoch=5_000.0))

    for _ in range(50):
        if health.status is LocalCaptureStatus.DEGRADED:
            break
        await asyncio.sleep(0)
    else:
        pytest.fail("capture writer did not degrade after archive failure")

    assert health.last_error_category == "archive_write"
    assert not probes.listeners
    assert not predictions.listeners

    # Stop remains bounded after the writer has already failed. The active run
    # is intentionally left for startup recovery rather than falsely marked clean.
    await supervisor.async_stop()
    row = await database.async_read(
        lambda conn: conn.execute(
            "SELECT terminal_status FROM local_capture_runs"
        ).fetchone()
    )
    assert row == ("running",)
    await database.async_stop()


@pytest.mark.asyncio
async def test_interrupted_capture_recovery_marks_uncertain_window(
    tmp_path: Path,
):
    """Startup recovery never invents an exact count for process-loss windows."""
    database = ArchiveDatabase(
        tmp_path / "archive.sqlite3",
        require_qualified_wal=False,
    )
    await database.async_start(allow_create=True)
    repository = ArchiveRepository(database)
    run_id = await repository.async_begin_local_capture(
        runtime_generation="abandoned-runtime"
    )

    recovered = await repository.async_recover_interrupted_local_captures()
    assert recovered == 1

    state = await database.async_read(
        lambda conn: (
            conn.execute(
                """
                SELECT terminal_status FROM local_capture_runs
                WHERE capture_run_id=?
                """,
                (run_id,),
            ).fetchone()[0],
            conn.execute(
                """
                SELECT reason,dropped_count,certainty
                FROM local_capture_gaps
                WHERE capture_run_id=?
                """,
                (run_id,),
            ).fetchone(),
        )
    )
    assert state == (
        "interrupted",
        ("unclean_shutdown", None, "uncertain"),
    )
    await database.async_stop()


@pytest.mark.asyncio
async def test_entity_factory_failure_does_not_stop_archive_capture(
    hass: HomeAssistant,
    tmp_path: Path,
):
    """The S4 sink runs before the ProbeManager entity-failure gate."""
    database = ArchiveDatabase(
        tmp_path / "factory-failure" / "archive.sqlite3",
        require_qualified_wal=False,
    )
    await database.async_start(allow_create=True)
    repository = ArchiveRepository(database)

    probe_manager = ProbeManager(bt_listener=None)
    factory_calls = 0

    def fail_factory(_manager, _data) -> None:
        nonlocal factory_calls
        factory_calls += 1
        raise RuntimeError("synthetic entity factory failure")

    probe_manager.init_sensor_platform(fail_factory)
    probe_manager.init_binary_sensor_platform(fail_factory)
    predictions = _FakeObservationManager()
    health = LocalCaptureHealth()
    supervisor = LocalCaptureSupervisor(
        hass,
        _FakeEntry(),  # type: ignore[arg-type]
        repository,
        probe_manager,
        predictions,  # type: ignore[arg-type]
        health,
    )
    await supervisor.async_start()

    update = probe_manager.create_observation_callback()
    update(_probe_observation(30.0, monotonic=600.0, epoch=6_000.0))
    # ProbeManager remembers the failed device and skips future entity creation,
    # but the archive offer is deliberately before that failed-device return.
    update(_probe_observation(31.0, monotonic=602.0, epoch=6_002.0))

    await supervisor.async_stop()

    assert factory_calls == 1
    rows = await database.async_read(
        lambda conn: conn.execute(
            """
            SELECT capture_class,t1 FROM local_observations
            ORDER BY event_ordinal
            """
        ).fetchall()
    )
    assert rows == [
        ("transition", 30.0),
        ("regular", 31.0),
    ]
    assert health.committed_observations == 2
    await database.async_stop()


@pytest.mark.asyncio
async def test_partial_startup_detaches_listener_and_marks_run_interrupted(
    hass: HomeAssistant,
    tmp_path: Path,
):
    """A startup seam failure cannot leave an archive listener without a writer."""
    database = ArchiveDatabase(
        tmp_path / "startup-failure" / "archive.sqlite3",
        require_qualified_wal=False,
    )
    await database.async_start(allow_create=True)
    repository = ArchiveRepository(database)
    probes = _FakeObservationManager()

    class FailingPredictionManager:
        def add_observation_listener(self, _listener):
            raise RuntimeError("synthetic prediction listener failure")

    health = LocalCaptureHealth()
    supervisor = LocalCaptureSupervisor(
        hass,
        _FakeEntry(),  # type: ignore[arg-type]
        repository,
        probes,  # type: ignore[arg-type]
        FailingPredictionManager(),  # type: ignore[arg-type]
        health,
    )

    with pytest.raises(RuntimeError, match="prediction listener failure"):
        await supervisor.async_start()

    assert not probes.listeners
    assert health.status is LocalCaptureStatus.DEGRADED
    assert health.last_error_category == "startup"
    state = await database.async_read(
        lambda conn: conn.execute(
            "SELECT terminal_status FROM local_capture_runs"
        ).fetchone()
    )
    assert state == ("interrupted",)
    await database.async_stop()

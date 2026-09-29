"""S3 Home Assistant lifecycle and option fault-containment tests."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.combustion.const import (
    CONF_CLOUD_SYNC_ENABLED,
    CONF_HISTORY_ENABLED,
    CONF_LOCAL_CAPTURE_ENABLED,
    DOMAIN,
)
from custom_components.combustion.runtime import CombustionRuntime
from custom_components.combustion.source_link import ProjectionSource
from custom_components.combustion.statistics_projection import (
    StatisticsProjectionHealth,
    async_project_missing_statistics,
)
from custom_components.combustion.storage.database import (
    ArchiveRuntimeUnsupported,
    ArchiveStatus,
)


@pytest.mark.asyncio
async def test_history_disabled_does_not_touch_archive(
    hass: HomeAssistant,
):
    """Existing users retain S2 behavior until history is explicitly enabled."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="combustion_meatnet",
        version=1,
        data={},
        options={},
        title="Meatnet",
    )
    entry.add_to_hass(hass)

    with patch(
        "custom_components.combustion.ArchiveDatabase",
    ) as archive_cls:
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    archive_cls.assert_not_called()
    assert entry.runtime_data.archive_health.status is ArchiveStatus.DISABLED
    assert hass.data[DOMAIN] is entry.runtime_data.probe_manager


@pytest.mark.asyncio
async def test_archive_runtime_failure_does_not_gate_local_setup(
    hass: HomeAssistant,
):
    """Unsupported SQLite degrades history while BLE/GATT remains loaded."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="combustion_meatnet",
        version=1,
        data={},
        options={CONF_HISTORY_ENABLED: True},
        title="Meatnet",
    )
    entry.add_to_hass(hass)

    with patch(
        "custom_components.combustion.ArchiveDatabase.async_start",
        AsyncMock(
            side_effect=ArchiveRuntimeUnsupported(
                "synthetic unsupported SQLite"
            )
        ),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)

    runtime = entry.runtime_data
    assert runtime.archive_health.status is ArchiveStatus.UNSUPPORTED
    assert runtime.archive_health.error_category == "sqlite_runtime"
    assert hass.data[DOMAIN] is runtime.probe_manager

    # Local manager lifecycle remains unloadable after optional archive failure.
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


@pytest.mark.asyncio
async def test_options_reject_cloud_sync_without_archive(
    hass: HomeAssistant,
):
    """Cloud-history sync cannot be enabled without its durable archive."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="combustion_meatnet",
        version=1,
        data={},
        options={},
        title="Meatnet",
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] == "form"

    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        user_input={
            "availability_timeout": 90,
            "update_throttle": 1.0,
            "enable_active_connection": False,
            CONF_HISTORY_ENABLED: False,
            CONF_CLOUD_SYNC_ENABLED: True,
        },
    )
    assert result["type"] == "form"
    assert result["errors"] == {
        "base": "history_required_for_cloud_sync"
    }


@pytest.mark.asyncio
async def test_options_reject_local_capture_without_archive(
    hass: HomeAssistant,
):
    """Local BLE/GATT capture cannot run without its durable archive."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="combustion_meatnet",
        version=1,
        data={},
        options={},
        title="Meatnet",
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    result = await hass.config_entries.options.async_init(entry.entry_id)
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        user_input={
            "availability_timeout": 90,
            "update_throttle": 1.0,
            "enable_active_connection": False,
            CONF_HISTORY_ENABLED: False,
            CONF_LOCAL_CAPTURE_ENABLED: True,
            CONF_CLOUD_SYNC_ENABLED: False,
        },
    )
    assert result["type"] == "form"
    assert result["errors"] == {
        "base": "history_required_for_local_capture"
    }


@pytest.mark.asyncio
async def test_runtime_stops_archive_intake_before_cancelling_sync():
    """Shutdown order is intake fence -> supervisor cancellation -> DB close."""
    events: list[str] = []

    class FakeDatabase:
        def stop_accepting(self):
            events.append("stop_accepting")

        async def async_stop(self):
            events.append("database_stop")

    class FakeCapture:
        async def async_stop(self):
            events.append("capture_stop")

    async def supervisor():
        try:
            await asyncio.Event().wait()
        finally:
            events.append("task_cancelled")

    runtime = CombustionRuntime(
        bluetooth_listener=MagicMock(),
        probe_manager=MagicMock(),
        connection_manager=MagicMock(),
        prediction_manager=MagicMock(),
        control_manager=MagicMock(),
    )
    runtime.archive_database = FakeDatabase()  # type: ignore[assignment]
    runtime.local_capture_supervisor = FakeCapture()  # type: ignore[assignment]
    task = asyncio.create_task(supervisor())
    runtime._optional_tasks.add(task)  # noqa: SLF001 - lifecycle ordering contract
    await asyncio.sleep(0)

    await runtime.async_stop()

    assert events == [
        "capture_stop",
        "stop_accepting",
        "task_cancelled",
        "database_stop",
    ]
    assert runtime.stopped is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cloud_sync_enabled", "expected_checks"),
    [(False, 1), (True, 1)],
)
async def test_archive_failure_keeps_exactly_one_cloud_check_owner(
    hass: HomeAssistant,
    cloud_sync_enabled: bool,
    expected_checks: int,
):
    """Archive failure cannot duplicate the configured cloud-health owner."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="combustion_meatnet",
        version=1,
        data={
            "cloud_api_key": "api",
            "cloud_refresh_token": "refresh",
            "cloud_subject": "subject",
            "cloud_link_generation": 1,
        },
        options={
            CONF_HISTORY_ENABLED: True,
            CONF_CLOUD_SYNC_ENABLED: cloud_sync_enabled,
        },
        title="Meatnet",
    )
    entry.add_to_hass(hass)

    one_shot = AsyncMock()
    with (
        patch(
            "custom_components.combustion.ArchiveDatabase.async_start",
            AsyncMock(
                side_effect=ArchiveRuntimeUnsupported(
                    "synthetic unsupported SQLite"
                )
            ),
        ),
        patch(
            "custom_components.combustion.async_check_linked_account",
            one_shot,
        ),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)

    assert one_shot.await_count == expected_checks
    assert entry.runtime_data.archive_health.status is ArchiveStatus.UNSUPPORTED

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


@pytest.mark.asyncio
async def test_history_sync_path_owns_cloud_client_instead_of_one_shot_check(
    hass: HomeAssistant,
):
    """S3 does not create a second independent auth-refresh owner."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="combustion_meatnet",
        version=1,
        data={
            "cloud_api_key": "api",
            "cloud_refresh_token": "refresh",
            "cloud_subject": "subject",
            "cloud_link_generation": 1,
        },
        options={
            CONF_HISTORY_ENABLED: True,
            CONF_CLOUD_SYNC_ENABLED: True,
        },
        title="Meatnet",
    )
    entry.add_to_hass(hass)

    archive_start = AsyncMock()
    one_shot = AsyncMock()
    with (
        patch(
            "custom_components.combustion._async_start_archive",
            archive_start,
        ),
        patch(
            "custom_components.combustion.async_check_linked_account",
            one_shot,
        ),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)

    archive_start.assert_awaited_once()
    one_shot.assert_not_awaited()


@pytest.mark.asyncio
async def test_s5_projection_skips_existing_recorder_hours(
    hass: HomeAssistant,
):
    """Cloud backfill fills holes without overwriting Recorder-owned hours."""
    registry = er.async_get(hass)
    entity = registry.async_get_or_create(
        "sensor",
        DOMAIN,
        "10014e68--sensor--core",
    )

    repository = AsyncMock()
    repository.async_projection_sources.return_value = (
        ProjectionSource(
            source_device_id="cloud-source",
            local_source_id="local-source",
            canonical_serial="10014E68",
            local_raw_serial="10014e68",
        ),
    )
    hour_one = int(datetime(2026, 1, 1, 1, tzinfo=UTC).timestamp() * 1_000_000)
    hour_two = int(datetime(2026, 1, 1, 2, tzinfo=UTC).timestamp() * 1_000_000)
    repository.async_hourly_temperature_statistics.return_value = (
        {"start_us": hour_one, "min": 10.0, "max": 20.0, "mean": 15.0},
        {"start_us": hour_two, "min": 20.0, "max": 30.0, "mean": 25.0},
    )
    health = StatisticsProjectionHealth()

    with (
        patch(
            "custom_components.combustion.statistics_projection._existing_hour_starts",
            AsyncMock(return_value={hour_one}),
        ),
        patch(
            "custom_components.combustion.statistics_projection.async_import_statistics"
        ) as import_statistics,
    ):
        await async_project_missing_statistics(hass, repository, health)

    import_statistics.assert_called_once()
    called_hass, metadata, rows = import_statistics.call_args.args
    assert called_hass is hass
    assert metadata["statistic_id"] == entity.entity_id
    assert metadata["source"] == "recorder"
    assert metadata["has_sum"] is False
    assert len(rows) == 1
    assert rows[0]["start"] == datetime(2026, 1, 1, 2, tzinfo=UTC)
    assert rows[0]["min"] == 20.0
    assert rows[0]["max"] == 30.0
    assert rows[0]["mean"] == 25.0
    assert health.queued_hours == 1
    assert health.skipped_existing_hours == 1
    assert health.status == "ready"

"""S3 admin WebSocket API tests."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.combustion.const import DOMAIN
from custom_components.combustion.storage.database import ArchiveStatus


async def _loaded_entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="combustion_meatnet",
        version=1,
        data={},
        options={},
        title="Meatnet",
    )
    entry.add_to_hass(hass)
    assert await async_setup_component(hass, DOMAIN, {})
    await hass.async_block_till_done()
    return entry


@pytest.mark.asyncio
async def test_archive_status_websocket_is_sanitized(
    hass: HomeAssistant,
    hass_ws_client,
):
    """Admin status exposes health metadata, not source/account secrets."""
    entry = await _loaded_entry(hass)
    client = await hass_ws_client(hass)

    await client.send_json_auto_id({"type": "combustion/archive/status"})
    response = await client.receive_json()

    assert response["success"] is True
    result = response["result"]
    assert result["archive"]["status"] == "disabled"
    assert result["sync"]["status"] == "disabled"
    assert result["source_links"]["status"] == "disabled"
    assert result["statistics_projection"]["status"] == "disabled"

    rendered = repr(result)
    assert "cloud_subject" not in rendered
    assert "refresh_token" not in rendered
    assert "provider_locator" not in rendered
    assert "raw_serial" not in rendered
    assert entry.runtime_data.archive_repository is None


@pytest.mark.asyncio
async def test_archive_status_refreshes_sizes_before_reporting(
    hass: HomeAssistant,
    hass_ws_client,
):
    """Status refreshes live archive sizes instead of returning startup-era values."""
    entry = await _loaded_entry(hass)
    runtime = entry.runtime_data
    runtime.archive_health.status = ArchiveStatus.READY
    runtime.archive_health.database_bytes = 1
    runtime.archive_health.wal_bytes = 2

    class FakeDatabase:
        def stop_accepting(self) -> None:
            pass

        async def async_stop(self) -> None:
            pass

        async def async_refresh_sizes(self) -> None:
            runtime.archive_health.database_bytes = 123
            runtime.archive_health.wal_bytes = 456

    runtime.archive_database = FakeDatabase()  # type: ignore[assignment]
    runtime.archive_repository = AsyncMock()
    runtime.archive_repository.async_archive_counts.return_value = {
        "devices": 0,
        "sessions": 0,
        "manifests": 0,
        "samples": 0,
        "versions": 0,
        "open_gaps": 0,
    }
    runtime.archive_repository.async_queue_counts.return_value = {
        "ready": 0,
        "running": 0,
        "failed": 0,
    }

    client = await hass_ws_client(hass)
    await client.send_json_auto_id({"type": "combustion/archive/status"})
    response = await client.receive_json()

    assert response["success"] is True
    assert response["result"]["archive"]["database_bytes"] == 123
    assert response["result"]["archive"]["wal_bytes"] == 456


@pytest.mark.asyncio
async def test_archive_backup_websocket_uses_fixed_internal_directory(
    hass: HomeAssistant,
    hass_ws_client,
    tmp_path: Path,
):
    """Clients cannot choose an arbitrary backup destination."""
    entry = await _loaded_entry(hass)
    runtime = entry.runtime_data
    runtime.archive_health.status = ArchiveStatus.READY

    archive_path = tmp_path / "combustion" / "archive.sqlite3"
    archive_path.parent.mkdir(parents=True)

    class FakeDatabase:
        path = archive_path

        def stop_accepting(self) -> None:
            pass

        async def async_stop(self) -> None:
            pass

        async def async_backup(self, destination: Path):
            assert destination.parent == archive_path.parent / "backups"
            assert destination.name.startswith("archive-")
            assert destination.suffix == ".sqlite3"
            return {
                "database": str(destination),
                "binding": str(
                    destination.with_suffix(".sqlite3.binding.json")
                ),
                "manifest": str(
                    destination.with_suffix(".sqlite3.manifest.json")
                ),
                "created_at_us": 123,
                "sha256": "abc",
                "counts": {"cloud_samples": 4},
            }

    runtime.archive_database = FakeDatabase()  # type: ignore[assignment]

    client = await hass_ws_client(hass)
    await client.send_json_auto_id(
        {
            "type": "combustion/archive/backup",
            # Deliberately no path field exists in the command schema.
        }
    )
    response = await client.receive_json()

    assert response["success"] is True
    assert response["result"]["database_file"].startswith("archive-")
    assert "/" not in response["result"]["database_file"]
    assert response["result"]["binding_file"].endswith(".binding.json")
    assert "/" not in response["result"]["binding_file"]
    assert response["result"]["sha256"] == "abc"


@pytest.mark.asyncio
async def test_archive_sync_now_serializes_through_supervisor(
    hass: HomeAssistant,
    hass_ws_client,
):
    """Admin sync-now calls the bounded supervisor cycle rather than raw client I/O."""
    entry = await _loaded_entry(hass)
    supervisor = AsyncMock()
    supervisor.async_sync_cycle.return_value = 3
    entry.runtime_data.sync_supervisor = supervisor

    client = await hass_ws_client(hass)
    await client.send_json_auto_id(
        {
            "type": "combustion/archive/sync_now",
            "max_work": 4,
        }
    )
    response = await client.receive_json()

    assert response["success"] is True
    assert response["result"] == {"processed_work_units": 3}
    supervisor.async_sync_cycle.assert_awaited_once_with(
        force_context_refresh=True,
        max_work=4,
    )


@pytest.mark.asyncio
async def test_s5_source_links_websocket_reconciles_and_refreshes_health(
    hass: HomeAssistant,
    hass_ws_client,
):
    """Admin reconcile exposes aggregate counts without raw source identifiers."""
    entry = await _loaded_entry(hass)
    runtime = entry.runtime_data
    runtime.archive_health.status = ArchiveStatus.READY
    runtime.archive_database = AsyncMock()

    repository = AsyncMock()
    repository.async_counts.return_value = {
        "cloud_probe_sources": 2,
        "local_probe_sources": 1,
        "active_links": 1,
        "total_links": 1,
        "unresolved_cloud_sources": 1,
        "ambiguous_cloud_sources": 0,
    }

    with patch(
        "custom_components.combustion.websocket_api.SourceLinkRepository",
        return_value=repository,
    ):
        client = await hass_ws_client(hass)
        await client.send_json_auto_id(
            {"type": "combustion/archive/source_links"}
        )
        response = await client.receive_json()

    assert response["success"] is True
    assert response["result"]["active_links"] == 1
    assert response["result"]["unresolved_cloud_sources"] == 1
    assert runtime.source_link_health.status == "ready"
    assert runtime.source_link_health.active_links == 1
    repository.async_reconcile.assert_awaited_once()


@pytest.mark.asyncio
async def test_s5_projection_refuses_ambiguous_source_identity(
    hass: HomeAssistant,
    hass_ws_client,
):
    """No Recorder writes are attempted while exact source identity is ambiguous."""
    entry = await _loaded_entry(hass)
    runtime = entry.runtime_data
    runtime.archive_health.status = ArchiveStatus.READY
    runtime.archive_database = AsyncMock()

    repository = AsyncMock()
    repository.async_counts.return_value = {
        "cloud_probe_sources": 1,
        "local_probe_sources": 2,
        "active_links": 0,
        "total_links": 0,
        "unresolved_cloud_sources": 0,
        "ambiguous_cloud_sources": 1,
    }

    with (
        patch(
            "custom_components.combustion.websocket_api.SourceLinkRepository",
            return_value=repository,
        ),
        patch(
            "custom_components.combustion.websocket_api.async_project_missing_statistics",
            new_callable=AsyncMock,
        ) as project,
    ):
        client = await hass_ws_client(hass)
        await client.send_json_auto_id(
            {"type": "combustion/archive/project_statistics"}
        )
        response = await client.receive_json()

    assert response["success"] is False
    assert response["error"]["code"] == "not_supported"
    project.assert_not_awaited()


@pytest.mark.asyncio
async def test_s5_projection_websocket_queues_fill_only_projection(
    hass: HomeAssistant,
    hass_ws_client,
):
    """Admin projection reconciles links first and returns aggregate queue results."""
    entry = await _loaded_entry(hass)
    runtime = entry.runtime_data
    runtime.archive_health.status = ArchiveStatus.READY
    runtime.archive_database = AsyncMock()

    repository = AsyncMock()
    repository.async_counts.return_value = {
        "cloud_probe_sources": 1,
        "local_probe_sources": 1,
        "active_links": 1,
        "total_links": 1,
        "unresolved_cloud_sources": 0,
        "ambiguous_cloud_sources": 0,
    }

    async def project(_hass, _repository, health):
        health.status = "ready"
        health.linked_sources = 1
        health.resolved_entities = 3
        health.queued_hours = 12
        health.skipped_existing_hours = 8
        health.last_run_us = 123

    with (
        patch(
            "custom_components.combustion.websocket_api.SourceLinkRepository",
            return_value=repository,
        ),
        patch(
            "custom_components.combustion.websocket_api.async_project_missing_statistics",
            side_effect=project,
        ) as project_mock,
    ):
        client = await hass_ws_client(hass)
        await client.send_json_auto_id(
            {"type": "combustion/archive/project_statistics"}
        )
        response = await client.receive_json()

    assert response["success"] is True
    assert response["result"] == {
        "status": "ready",
        "linked_sources": 1,
        "resolved_entities": 3,
        "queued_hours": 12,
        "skipped_existing_hours": 8,
        "last_run_us": 123,
    }
    project_mock.assert_awaited_once()

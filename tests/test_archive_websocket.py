"""S3 admin WebSocket API tests."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

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

    rendered = repr(result)
    assert "cloud_subject" not in rendered
    assert "refresh_token" not in rendered
    assert "provider_locator" not in rendered
    assert "raw_serial" not in rendered
    assert entry.runtime_data.archive_repository is None


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

        async def async_backup(self, destination: Path):
            assert destination.parent == archive_path.parent / "backups"
            assert destination.name.startswith("archive-")
            assert destination.suffix == ".sqlite3"
            return {
                "database": str(destination),
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

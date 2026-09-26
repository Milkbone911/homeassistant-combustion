"""S2b runtime cloud-link fault-containment tests."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.combustion.cloud.ha import (
    CloudLinkHealth,
    CloudLinkStatus,
    async_check_linked_account,
)
from custom_components.combustion.cloud.models import CloudAuthError, Probe
from custom_components.combustion.const import (
    CONF_CLOUD_API_KEY,
    CONF_CLOUD_LINK_GENERATION,
    CONF_CLOUD_REFRESH_TOKEN,
    CONF_CLOUD_SUBJECT,
    DOMAIN,
)


def _entry() -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        unique_id="combustion_meatnet",
        version=1,
        title="Combustion Meatnet",
        data={
            CONF_CLOUD_API_KEY: "synthetic-api",
            CONF_CLOUD_REFRESH_TOKEN: "synthetic-refresh",
            CONF_CLOUD_SUBJECT: "synthetic-subject",
            CONF_CLOUD_LINK_GENERATION: 4,
        },
    )


@pytest.mark.asyncio
async def test_linked_startup_keeps_local_setup_independent_of_cloud_check(
    hass: HomeAssistant,
):
    """Optional cloud work starts after local runtime and cannot gate setup."""
    entry = _entry()
    entry.add_to_hass(hass)

    async def fake_check(_hass, _entry, health):
        health.status = CloudLinkStatus.DEGRADED
        health.error_category = "transport"

    with patch(
        "custom_components.combustion.async_check_linked_account",
        side_effect=fake_check,
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    runtime = entry.runtime_data
    assert hass.data[DOMAIN] is runtime.probe_manager
    assert runtime.cloud_health.status is CloudLinkStatus.DEGRADED
    assert runtime.cloud_health.error_category == "transport"


@pytest.mark.asyncio
async def test_rotation_persists_without_reloading_ble_runtime(
    hass: HomeAssistant,
):
    """Routine token rotation updates config-entry data without a local reload."""
    entry = _entry()
    entry.add_to_hass(hass)

    with patch(
        "custom_components.combustion.async_check_linked_account",
        AsyncMock(),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    runtime = entry.runtime_data
    probe_manager = runtime.probe_manager
    health = runtime.cloud_health

    class FakeClient:
        def __init__(self, **kwargs):
            self._rotation = kwargs["on_rotation"]

        async def probes(self):
            await self._rotation("synthetic-refresh", "rotated-refresh")
            return (Probe("A", "key-a"), Probe("B", "key-b"))

    with patch(
        "custom_components.combustion.cloud.ha.CombustionCloudClient",
        FakeClient,
    ):
        await async_check_linked_account(hass, entry, health)
        await hass.async_block_till_done()

    assert entry.data[CONF_CLOUD_REFRESH_TOKEN] == "rotated-refresh"
    assert entry.data[CONF_CLOUD_LINK_GENERATION] == 4
    assert entry.runtime_data is runtime
    assert hass.data[DOMAIN] is probe_manager
    assert len(entry.update_listeners) == 0
    assert health.status is CloudLinkStatus.READY
    assert health.probe_count == 2
    assert health.verified_generation == 4


@pytest.mark.asyncio
async def test_auth_failure_requests_reauth_without_unloading_local_runtime(
    hass: HomeAssistant,
):
    """Persistent cloud auth failure must leave local BLE runtime available."""
    entry = _entry()
    entry.add_to_hass(hass)

    with patch(
        "custom_components.combustion.async_check_linked_account",
        AsyncMock(),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    runtime = entry.runtime_data

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        async def probes(self):
            raise CloudAuthError("synthetic auth failure")

    with (
        patch(
            "custom_components.combustion.cloud.ha.CombustionCloudClient",
            FakeClient,
        ),
        patch.object(entry, "async_start_reauth") as start_reauth,
    ):
        await async_check_linked_account(hass, entry, runtime.cloud_health)

    start_reauth.assert_called_once_with(hass)
    assert entry.runtime_data is runtime
    assert hass.data[DOMAIN] is runtime.probe_manager
    assert runtime.cloud_health.status is CloudLinkStatus.REAUTH_REQUIRED
    assert runtime.cloud_health.error_category == "auth"


@pytest.mark.asyncio
async def test_stale_generation_rotation_exits_without_reauth_or_token_write(
    hass: HomeAssistant,
):
    """Delayed old-account refresh must not mutate or reauth a replacement link."""
    entry = _entry()
    entry.add_to_hass(hass)
    health = CloudLinkHealth()

    class FakeClient:
        def __init__(self, **kwargs):
            self._rotation = kwargs["on_rotation"]

        async def probes(self):
            # Simulate replacement winning while this old request was in flight.
            hass.config_entries.async_update_entry(
                entry,
                data={
                    **entry.data,
                    CONF_CLOUD_SUBJECT: "replacement-subject",
                    CONF_CLOUD_REFRESH_TOKEN: "replacement-refresh",
                    CONF_CLOUD_LINK_GENERATION: 5,
                },
            )
            await self._rotation("synthetic-refresh", "stale-rotated-token")
            return ()

    with (
        patch(
            "custom_components.combustion.cloud.ha.CombustionCloudClient",
            FakeClient,
        ),
        patch.object(entry, "async_start_reauth") as start_reauth,
    ):
        await async_check_linked_account(hass, entry, health)

    start_reauth.assert_not_called()
    assert entry.data[CONF_CLOUD_REFRESH_TOKEN] == "replacement-refresh"
    assert entry.data[CONF_CLOUD_SUBJECT] == "replacement-subject"
    assert entry.data[CONF_CLOUD_LINK_GENERATION] == 5
    assert health.status is CloudLinkStatus.CHECKING

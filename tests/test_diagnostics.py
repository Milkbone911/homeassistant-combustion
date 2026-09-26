"""Tests for sanitized Combustion diagnostics."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.combustion.cloud.ha import CloudLinkStatus
from custom_components.combustion.const import (
    CONF_CLOUD_API_KEY,
    CONF_CLOUD_LINK_GENERATION,
    CONF_CLOUD_REFRESH_TOKEN,
    CONF_CLOUD_SUBJECT,
    DOMAIN,
)
from custom_components.combustion.diagnostics import async_get_config_entry_diagnostics


@pytest.mark.asyncio
async def test_diagnostics_allowlist_excludes_cloud_secrets_and_identity(
    hass: HomeAssistant,
):
    """Diagnostics expose subsystem health but no credential or account values."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="combustion_meatnet",
        version=1,
        title="Combustion Meatnet",
        data={
            CONF_CLOUD_API_KEY: "secret-api-value",
            CONF_CLOUD_REFRESH_TOKEN: "secret-refresh-value",
            CONF_CLOUD_SUBJECT: "private-subject-value",
            CONF_CLOUD_LINK_GENERATION: 9,
        },
    )
    entry.add_to_hass(hass)

    with patch(
        "custom_components.combustion.async_check_linked_account",
        AsyncMock(),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    runtime = entry.runtime_data
    runtime.cloud_health.status = CloudLinkStatus.DEGRADED
    runtime.cloud_health.error_category = "transport"
    runtime.cloud_health.probe_count = 2
    runtime.cloud_health.verified_generation = 9

    data = await async_get_config_entry_diagnostics(hass, entry)
    rendered = repr(data)

    assert data["cloud"] == {
        "linked": True,
        "status": CloudLinkStatus.DEGRADED,
        "probe_count": 2,
        "error_category": "transport",
        "verified_generation": 9,
    }
    assert "secret-api-value" not in rendered
    assert "secret-refresh-value" not in rendered
    assert "private-subject-value" not in rendered

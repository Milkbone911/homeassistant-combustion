"""Test Config Flow."""

import pytest
from homeassistant import config_entries
from homeassistant.config_entries import SOURCE_BLUETOOTH
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.combustion.const import DOMAIN
from tests.utils.bt_utils import (
    COMBUSTION_SERVICE_INFO,
    create_advertisement,
    gauge_payload,
    inject_bt_advertisement,
)


@pytest.mark.asyncio
async def test_bluetooth_discovery(hass: HomeAssistant):
    """Test discovery via bluetooth with a valid device."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_BLUETOOTH},
        data=COMBUSTION_SERVICE_INFO,
    )

    assert result["type"] == FlowResultType.FORM
    assert result["step_id"] == "confirm"
    assert result["description_placeholders"] == {
        "name": "Combustion Meatnet"
    }

    # with patch_async_setup_entry():
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], user_input={"not": "empty"}
    )
    await hass.async_block_till_done()
    assert result["type"] == FlowResultType.CREATE_ENTRY
    assert result["title"] == "Combustion Meatnet"
    assert result["result"].unique_id == "combustion_meatnet"

async def test_bluetooth_discovery_already_setup(hass: HomeAssistant) -> None:
    """Test discovery via bluetooth with a valid device when already setup."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="combustion_meatnet",
    )
    entry.add_to_hass(hass)
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_BLUETOOTH},
        data=COMBUSTION_SERVICE_INFO,
    )
    assert result["type"] == FlowResultType.ABORT
    assert result["reason"] == "already_configured"


async def test_gauge_discovery_creates_entry(hass: HomeAssistant):
    """A Giant Grill Gauge advertisement should be able to bootstrap the entry."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_BLUETOOTH},
        data=create_advertisement(gauge_payload()),
    )
    assert result["type"] is FlowResultType.FORM

    result = await hass.config_entries.flow.async_configure(result["flow_id"], {})
    assert result["type"] is FlowResultType.CREATE_ENTRY


async def test_user_step_finds_discovered_device(hass: HomeAssistant):
    """The manual (user) flow should adopt a device already seen by discovery."""
    inject_bt_advertisement(hass, create_advertisement(gauge_payload()))
    await hass.async_block_till_done()
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] is FlowResultType.FORM


async def test_user_step_aborts_with_nothing_in_range(hass: HomeAssistant):
    """The manual (user) flow should abort cleanly when nothing is in range."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "no_devices_found"

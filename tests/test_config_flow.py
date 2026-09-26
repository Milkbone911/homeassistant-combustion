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
    # The injection also fires HA's automatic bluetooth discovery flow, as an
    # eager background task that plain async_block_till_done does not wait
    # for. Whether it reached async_step_bluetooth before the manual flow
    # below was a scheduling race: if it did, the manual flow aborted with
    # already_in_progress on the shared combustion_meatnet unique_id (the
    # usual outcome on CI; dev boxes usually won the race and passed). Wait
    # for it deterministically, then clear it — this test is about adopting a
    # device from history, not about colliding with an open discovery flow.
    await hass.async_block_till_done(wait_background_tasks=True)
    discovery_flows = hass.config_entries.flow.async_progress_by_handler(DOMAIN)
    assert [flow["context"]["source"] for flow in discovery_flows] == ["bluetooth"]
    for flow in discovery_flows:
        hass.config_entries.flow.async_abort(flow["flow_id"])
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] is FlowResultType.FORM


async def test_user_step_offers_cloud_link_with_nothing_in_range(
    hass: HomeAssistant,
):
    """Sleeping hardware should fall through to optional cloud credential linking."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "cloud_link"

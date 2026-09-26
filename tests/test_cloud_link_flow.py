"""Tests for S2b cloud credential linking, reauth, replacement and unlink."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.combustion.cloud.ha import (
    CloudLinkAccountMismatch,
    CloudLinkValidation,
)
from custom_components.combustion.cloud.models import CloudAuthError
from custom_components.combustion.const import (
    CONF_CLOUD_API_KEY,
    CONF_CLOUD_LINK_GENERATION,
    CONF_CLOUD_REFRESH_TOKEN,
    CONF_CLOUD_SUBJECT,
    DOMAIN,
)

CREDS = {
    CONF_CLOUD_API_KEY: "synthetic-api-key",
    CONF_CLOUD_REFRESH_TOKEN: "synthetic-refresh-token",
}
VALID = CloudLinkValidation(
    subject="synthetic-subject",
    refresh_token="rotated-refresh-token",
    probe_count=2,
)


def _linked_entry(*, generation: int = 3) -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        unique_id="combustion_meatnet",
        version=1,
        title="Combustion Meatnet",
        data={
            CONF_CLOUD_API_KEY: "old-api-key",
            CONF_CLOUD_REFRESH_TOKEN: "old-refresh-token",
            CONF_CLOUD_SUBJECT: "synthetic-subject",
            CONF_CLOUD_LINK_GENERATION: generation,
        },
    )


@pytest.mark.asyncio
async def test_user_without_awake_ble_can_create_cloud_first_singleton(
    hass: HomeAssistant,
):
    """Sleeping local hardware must not prevent an authenticated cloud-first entry."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "cloud_link"

    with patch(
        "custom_components.combustion.config_flow.async_validate_cloud_link",
        AsyncMock(return_value=VALID),
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], user_input=CREDS
        )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    entry = result["result"]
    assert entry.unique_id == "combustion_meatnet"
    assert entry.data[CONF_CLOUD_API_KEY] == CREDS[CONF_CLOUD_API_KEY]
    assert entry.data[CONF_CLOUD_REFRESH_TOKEN] == VALID.refresh_token
    assert entry.data[CONF_CLOUD_SUBJECT] == VALID.subject
    assert entry.data[CONF_CLOUD_LINK_GENERATION] == 1


@pytest.mark.asyncio
async def test_cloud_link_rejects_invalid_auth_without_persisting_secret(
    hass: HomeAssistant,
):
    """A rejected credential must leave no config entry behind."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )

    with patch(
        "custom_components.combustion.config_flow.async_validate_cloud_link",
        AsyncMock(side_effect=CloudAuthError("synthetic failure")),
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], user_input=CREDS
        )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "invalid_auth"}
    assert hass.config_entries.async_entries(DOMAIN) == []


@pytest.mark.asyncio
async def test_reconfigure_local_only_entry_links_cloud_without_second_entry(
    hass: HomeAssistant,
):
    """Existing BLE-only setup must acquire cloud on the same singleton entry."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="combustion_meatnet",
        version=1,
        title="Combustion Meatnet",
        data={"devices": [{"address": "AA:BB"}]},
    )
    entry.add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={
            "source": config_entries.SOURCE_RECONFIGURE,
            "entry_id": entry.entry_id,
        },
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "cloud_link"

    with patch(
        "custom_components.combustion.config_flow.async_validate_cloud_link",
        AsyncMock(return_value=VALID),
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], user_input=CREDS
        )

    assert result["type"] is FlowResultType.ABORT
    assert len(hass.config_entries.async_entries(DOMAIN)) == 1
    assert entry.data["devices"] == [{"address": "AA:BB"}]
    assert entry.data[CONF_CLOUD_SUBJECT] == VALID.subject
    assert entry.data[CONF_CLOUD_LINK_GENERATION] == 1


@pytest.mark.asyncio
async def test_automatic_reauth_requires_same_subject_and_preserves_generation(
    hass: HomeAssistant,
):
    """Reauth may rotate credentials but cannot silently switch accounts."""
    entry = _linked_entry(generation=7)
    entry.add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_REAUTH, "entry_id": entry.entry_id},
        data=entry.data,
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "reauth_confirm"

    with patch(
        "custom_components.combustion.config_flow.async_validate_cloud_link",
        AsyncMock(
            side_effect=CloudLinkAccountMismatch(
                "Authenticated subject does not match linked account"
            )
        ),
    ):
        mismatch = await hass.config_entries.flow.async_configure(
            result["flow_id"], user_input=CREDS
        )
    assert mismatch["type"] is FlowResultType.FORM
    assert mismatch["errors"] == {"base": "wrong_account"}
    assert entry.data[CONF_CLOUD_REFRESH_TOKEN] == "old-refresh-token"
    assert entry.data[CONF_CLOUD_LINK_GENERATION] == 7

    with patch(
        "custom_components.combustion.config_flow.async_validate_cloud_link",
        AsyncMock(return_value=VALID),
    ):
        success = await hass.config_entries.flow.async_configure(
            mismatch["flow_id"], user_input=CREDS
        )
    assert success["type"] is FlowResultType.ABORT
    assert entry.data[CONF_CLOUD_REFRESH_TOKEN] == VALID.refresh_token
    assert entry.data[CONF_CLOUD_SUBJECT] == VALID.subject
    assert entry.data[CONF_CLOUD_LINK_GENERATION] == 7


@pytest.mark.asyncio
async def test_explicit_replacement_requires_different_subject_and_advances_generation(
    hass: HomeAssistant,
):
    """Replacement is distinct from reauth and fences old account work."""
    entry = _linked_entry(generation=4)
    entry.add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={
            "source": config_entries.SOURCE_RECONFIGURE,
            "entry_id": entry.entry_id,
        },
    )
    assert result["type"] is FlowResultType.MENU
    assert result["menu_options"] == [
        "cloud_reauth",
        "cloud_replace",
        "cloud_unlink",
    ]

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"next_step_id": "cloud_replace"}
    )
    assert result["step_id"] == "cloud_replace"

    with patch(
        "custom_components.combustion.config_flow.async_validate_cloud_link",
        AsyncMock(return_value=VALID),
    ):
        same = await hass.config_entries.flow.async_configure(
            result["flow_id"], user_input=CREDS
        )
    assert same["type"] is FlowResultType.FORM
    assert same["errors"] == {"base": "same_account"}

    replacement = CloudLinkValidation(
        subject="replacement-subject",
        refresh_token="replacement-refresh",
        probe_count=1,
    )
    with patch(
        "custom_components.combustion.config_flow.async_validate_cloud_link",
        AsyncMock(return_value=replacement),
    ):
        success = await hass.config_entries.flow.async_configure(
            same["flow_id"], user_input=CREDS
        )

    assert success["type"] is FlowResultType.ABORT
    assert entry.data[CONF_CLOUD_SUBJECT] == "replacement-subject"
    assert entry.data[CONF_CLOUD_REFRESH_TOKEN] == "replacement-refresh"
    assert entry.data[CONF_CLOUD_LINK_GENERATION] == 5


@pytest.mark.asyncio
async def test_unlink_removes_credentials_but_retains_advanced_generation(
    hass: HomeAssistant,
):
    """Unlink is local credential removal, not provider deletion/revocation."""
    entry = _linked_entry(generation=2)
    entry.add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={
            "source": config_entries.SOURCE_RECONFIGURE,
            "entry_id": entry.entry_id,
        },
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"next_step_id": "cloud_unlink"}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "cloud_unlink"

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], user_input={}
    )
    assert result["type"] is FlowResultType.ABORT
    assert CONF_CLOUD_API_KEY not in entry.data
    assert CONF_CLOUD_REFRESH_TOKEN not in entry.data
    assert CONF_CLOUD_SUBJECT not in entry.data
    assert entry.data[CONF_CLOUD_LINK_GENERATION] == 3

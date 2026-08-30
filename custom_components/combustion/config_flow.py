"""Adds config flow for Combustion."""
from __future__ import annotations

from typing import Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.components.bluetooth import (
    BluetoothServiceInfoBleak,
    async_discovered_service_info,
)
from homeassistant.core import callback

from custom_components.combustion.bluetooth_listener import parse_advertisement

from .const import (
    CONF_AVAILABILITY_TIMEOUT,
    CONF_DEVICES,
    CONF_ENABLE_ACTIVE_CONNECTION,
    CONF_UPDATE_THROTTLE,
    DEFAULT_AVAILABILITY_TIMEOUT,
    DEFAULT_ENABLE_ACTIVE_CONNECTION,
    DEFAULT_UPDATE_THROTTLE,
    DOMAIN,
    LOGGER,
)


def format_unique_id(address: str) -> str:
    """Format the unique ID for a device."""
    return address.replace(":", "").lower()

class CombustionFlowHandler(config_entries.ConfigFlow, domain=DOMAIN):
    """Config flow for Combustion."""

    VERSION = 1
    def __init__(self) -> None:
        """Initialize the config flow."""
        self._discovered_adv: object | None = None
        self._all_discovered_devices: dict[str, object] = {}

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: config_entries.ConfigEntry) -> CombustionOptionsFlowHandler:
        """Get the options flow for this handler."""
        return CombustionOptionsFlowHandler(config_entry)

    async def async_step_bluetooth(self, discovery_info: BluetoothServiceInfoBleak) -> config_entries.FlowResult:
        """Bluetooth discovery step."""
        LOGGER.debug("async step bluetooth for device %s", str(discovery_info.as_dict()))

        # For now, only a single "meatnet" is supported. This prevents each device from showing as an independent integration.
        # Instead we ask to configure once, and create devices for each of the entities on the meatnet.
        await self.async_set_unique_id("combustion_meatnet")
        self._abort_if_unique_id_configured()

        data = parse_advertisement(discovery_info)
        if data is None:
            return self.async_abort(reason="not_supported")

        self._all_discovered_devices[discovery_info.address] = data
        self._discovered_adv = data

        self.context["title_placeholders"] = {
            "name": "Combustion Meatnet",
            "address": discovery_info.address,
        }

        return await self.async_step_confirm()

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.FlowResult:
        """Manual setup: adopt any Combustion device already in range."""
        await self.async_set_unique_id("combustion_meatnet")
        self._abort_if_unique_id_configured()

        for discovery_info in async_discovered_service_info(self.hass, False):
            data = parse_advertisement(discovery_info)
            if data is not None:
                self._discovered_adv = data
                self._all_discovered_devices[discovery_info.address] = data
                return await self.async_step_confirm()

        return self.async_abort(reason="no_devices_found")

    async def async_step_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.FlowResult:
        """Confirm a single device."""
        if user_input is not None:
            return await self._async_create_entry_from_discovery(user_input)

        self._set_confirm_only()
        return self.async_show_form(
            step_id="confirm",
            description_placeholders={
                "name": "Combustion Meatnet"
            },
        )

    async def _async_create_entry_from_discovery(
        self, user_input: dict[str, Any]
    ) -> config_entries.FlowResult:
        """Create an entry from a discovery."""
        devices = []
        for (addr, _device) in self._all_discovered_devices.items():
            devices.append({
                "name": "Combustion Meatnet",
                "address": addr,
                "product_type": 2 # hardcode meatnet probe
            })

        return self.async_create_entry(
            title="Combustion Meatnet",
            data={
                **user_input,
                CONF_DEVICES: devices,
            },
        )


class CombustionOptionsFlowHandler(config_entries.OptionsFlow):
    """Options flow for Combustion."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize.

        The entry is kept on a private attribute: assigning to
        OptionsFlow.config_entry was removed in newer Home Assistant versions,
        while older versions don't provide it automatically.
        """
        self._entry = config_entry

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> config_entries.FlowResult:
        """Manage the options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        options = self._entry.options
        schema = vol.Schema({
            vol.Optional(
                CONF_AVAILABILITY_TIMEOUT,
                default=options.get(CONF_AVAILABILITY_TIMEOUT, DEFAULT_AVAILABILITY_TIMEOUT),
            ): vol.All(vol.Coerce(int), vol.Range(min=15, max=600)),
            vol.Optional(
                CONF_UPDATE_THROTTLE,
                default=options.get(CONF_UPDATE_THROTTLE, DEFAULT_UPDATE_THROTTLE),
            ): vol.All(vol.Coerce(float), vol.Range(min=0.0, max=30.0)),
            vol.Optional(
                CONF_ENABLE_ACTIVE_CONNECTION,
                default=options.get(CONF_ENABLE_ACTIVE_CONNECTION, DEFAULT_ENABLE_ACTIVE_CONNECTION),
            ): bool,
        })
        return self.async_show_form(step_id="init", data_schema=schema)

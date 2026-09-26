"""Config flow for Combustion local devices and optional MeatNet Cloud link."""
from __future__ import annotations

from typing import Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.components.bluetooth import (
    BluetoothServiceInfoBleak,
    async_discovered_service_info,
)
from homeassistant.core import callback
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from custom_components.combustion.bluetooth_listener import parse_advertisement

from .cloud.ha import (
    CloudLinkAccountMismatch,
    CloudLinkValidation,
    async_validate_cloud_link,
    cloud_linked,
)
from .cloud.models import (
    CloudAuthError,
    CloudBoundsError,
    CloudConflictError,
    CloudPermissionError,
    CloudSchemaError,
    CloudTransportError,
)
from .const import (
    CONF_AVAILABILITY_TIMEOUT,
    CONF_CLOUD_API_KEY,
    CONF_CLOUD_LINK_GENERATION,
    CONF_CLOUD_REFRESH_TOKEN,
    CONF_CLOUD_SUBJECT,
    CONF_DEVICES,
    CONF_ENABLE_ACTIVE_CONNECTION,
    CONF_UPDATE_THROTTLE,
    DEFAULT_AVAILABILITY_TIMEOUT,
    DEFAULT_ENABLE_ACTIVE_CONNECTION,
    DEFAULT_UPDATE_THROTTLE,
    DOMAIN,
    LOGGER,
)

MEATNET_UNIQUE_ID = "combustion_meatnet"

_CLOUD_CREDENTIAL_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_CLOUD_API_KEY): TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
        vol.Required(CONF_CLOUD_REFRESH_TOKEN): TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
    }
)


def format_unique_id(address: str) -> str:
    """Format the unique ID for a device."""
    return address.replace(":", "").lower()


def _next_link_generation(data: dict[str, Any] | Any) -> int:
    """Return the next account-link generation without trusting malformed data."""
    try:
        current = int(data.get(CONF_CLOUD_LINK_GENERATION, 0))
    except (TypeError, ValueError):
        current = 0
    return max(0, current) + 1


def _cloud_error_key(err: Exception) -> str:
    """Map typed cloud failures to non-secret config-flow errors."""
    if isinstance(err, CloudLinkAccountMismatch):
        return "wrong_account"
    if isinstance(err, CloudAuthError):
        return "invalid_auth"
    if isinstance(err, CloudPermissionError):
        return "cloud_permission"
    if isinstance(err, CloudTransportError):
        return "cannot_connect"
    if isinstance(err, (CloudSchemaError, CloudBoundsError, CloudConflictError)):
        return "cloud_unsupported"
    return "unknown"


class CombustionFlowHandler(config_entries.ConfigFlow, domain=DOMAIN):
    """Configure the one household Combustion Meatnet entry."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self._discovered_adv: object | None = None
        self._all_discovered_devices: dict[str, object] = {}
        self._cloud_target_entry: config_entries.ConfigEntry | None = None

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> CombustionOptionsFlowHandler:
        """Get the options flow for this handler."""
        return CombustionOptionsFlowHandler()

    async def async_step_bluetooth(
        self, discovery_info: BluetoothServiceInfoBleak
    ) -> config_entries.FlowResult:
        """Handle local Bluetooth discovery."""
        LOGGER.debug(
            "async step bluetooth for device %s", str(discovery_info.as_dict())
        )

        await self.async_set_unique_id(MEATNET_UNIQUE_ID)
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
        """Set up from nearby local hardware, or cloud credentials if none is awake."""
        await self.async_set_unique_id(MEATNET_UNIQUE_ID)
        self._abort_if_unique_id_configured()

        for discovery_info in async_discovered_service_info(self.hass, False):
            data = parse_advertisement(discovery_info)
            if data is not None:
                self._discovered_adv = data
                self._all_discovered_devices[discovery_info.address] = data
                return await self.async_step_confirm()

        # Sleeping hardware must not block a cloud-first setup. This is a
        # credential-import flow, not a claim of official Combustion OAuth.
        self._cloud_target_entry = None
        return await self.async_step_cloud_link(user_input)

    async def async_step_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.FlowResult:
        """Confirm a locally discovered Meatnet."""
        if user_input is not None:
            return await self._async_create_entry_from_discovery(user_input)

        self._set_confirm_only()
        return self.async_show_form(
            step_id="confirm",
            description_placeholders={"name": "Combustion Meatnet"},
        )

    async def _async_create_entry_from_discovery(
        self, user_input: dict[str, Any]
    ) -> config_entries.FlowResult:
        """Create the singleton entry from local discovery."""
        devices = [
            {
                "name": "Combustion Meatnet",
                "address": addr,
                "product_type": 2,
            }
            for addr in self._all_discovered_devices
        ]
        return self.async_create_entry(
            title="Combustion Meatnet",
            data={**user_input, CONF_DEVICES: devices},
        )

    async def _async_validate_cloud_input(
        self,
        user_input: dict[str, Any],
        *,
        expected_subject: str | None,
    ) -> tuple[CloudLinkValidation | None, dict[str, str]]:
        """Validate a credential handoff without ever logging its values."""
        try:
            validation = await async_validate_cloud_link(
                self.hass,
                api_key=user_input[CONF_CLOUD_API_KEY],
                refresh_token=user_input[CONF_CLOUD_REFRESH_TOKEN],
                expected_subject=expected_subject,
            )
        except (
            CloudAuthError,
            CloudPermissionError,
            CloudTransportError,
            CloudSchemaError,
            CloudBoundsError,
            CloudConflictError,
        ) as err:
            return None, {"base": _cloud_error_key(err)}
        return validation, {}

    @staticmethod
    def _linked_data(
        base: dict[str, Any] | Any,
        *,
        api_key: str,
        validation: CloudLinkValidation,
        generation: int,
    ) -> dict[str, Any]:
        """Return config-entry data containing exactly one active cloud link."""
        return {
            **dict(base),
            CONF_CLOUD_API_KEY: api_key,
            CONF_CLOUD_REFRESH_TOKEN: validation.refresh_token,
            CONF_CLOUD_SUBJECT: validation.subject,
            CONF_CLOUD_LINK_GENERATION: generation,
        }

    async def async_step_cloud_link(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.FlowResult:
        """Link cloud credentials for a new or currently local-only entry."""
        errors: dict[str, str] = {}
        if user_input is not None:
            validation, errors = await self._async_validate_cloud_input(
                user_input, expected_subject=None
            )
            if validation is not None:
                entry = self._cloud_target_entry
                if entry is None:
                    return self.async_create_entry(
                        title="Combustion Meatnet",
                        data=self._linked_data(
                            {},
                            api_key=user_input[CONF_CLOUD_API_KEY],
                            validation=validation,
                            generation=1,
                        ),
                    )
                data = self._linked_data(
                    entry.data,
                    api_key=user_input[CONF_CLOUD_API_KEY],
                    validation=validation,
                    generation=_next_link_generation(entry.data),
                )
                return self.async_update_reload_and_abort(entry, data=data)

        return self.async_show_form(
            step_id="cloud_link",
            data_schema=_CLOUD_CREDENTIAL_SCHEMA,
            errors=errors,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.FlowResult:
        """Manage the optional cloud account link on the singleton entry."""
        entry = self._get_reconfigure_entry()
        self._cloud_target_entry = entry
        if not cloud_linked(entry.data):
            return await self.async_step_cloud_link()
        return self.async_show_menu(
            step_id="reconfigure",
            menu_options=["cloud_reauth", "cloud_replace", "cloud_unlink"],
        )

    async def async_step_cloud_reauth(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.FlowResult:
        """Manually refresh credentials while preserving the same subject."""
        entry = self._get_reconfigure_entry()
        self._cloud_target_entry = entry
        return await self._async_same_account_credentials(
            entry, user_input, step_id="cloud_reauth"
        )

    async def async_step_reauth(
        self, entry_data: dict[str, Any]
    ) -> config_entries.FlowResult:
        """Start Home Assistant's automatic reauthentication flow."""
        self._cloud_target_entry = self._get_reauth_entry()
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.FlowResult:
        """Accept replacement credentials only for the already-linked subject."""
        entry = self._get_reauth_entry()
        self._cloud_target_entry = entry
        return await self._async_same_account_credentials(
            entry, user_input, step_id="reauth_confirm"
        )

    async def _async_same_account_credentials(
        self,
        entry: config_entries.ConfigEntry,
        user_input: dict[str, Any] | None,
        *,
        step_id: str,
    ) -> config_entries.FlowResult:
        """Validate and persist same-subject credentials with one explicit reload."""
        errors: dict[str, str] = {}
        if user_input is not None:
            expected_subject = entry.data.get(CONF_CLOUD_SUBJECT)
            if not isinstance(expected_subject, str) or not expected_subject:
                return self.async_abort(reason="cloud_not_linked")
            validation, errors = await self._async_validate_cloud_input(
                user_input, expected_subject=expected_subject
            )
            if validation is not None:
                data = self._linked_data(
                    entry.data,
                    api_key=user_input[CONF_CLOUD_API_KEY],
                    validation=validation,
                    generation=max(
                        1, int(entry.data.get(CONF_CLOUD_LINK_GENERATION, 1))
                    ),
                )
                return self.async_update_reload_and_abort(entry, data=data)

        return self.async_show_form(
            step_id=step_id,
            data_schema=_CLOUD_CREDENTIAL_SCHEMA,
            errors=errors,
        )

    async def async_step_cloud_replace(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.FlowResult:
        """Explicitly replace the active account and advance link generation."""
        entry = self._get_reconfigure_entry()
        self._cloud_target_entry = entry
        errors: dict[str, str] = {}
        if user_input is not None:
            validation, errors = await self._async_validate_cloud_input(
                user_input, expected_subject=None
            )
            if validation is not None:
                if validation.subject == entry.data.get(CONF_CLOUD_SUBJECT):
                    errors = {"base": "same_account"}
                else:
                    data = self._linked_data(
                        entry.data,
                        api_key=user_input[CONF_CLOUD_API_KEY],
                        validation=validation,
                        generation=_next_link_generation(entry.data),
                    )
                    return self.async_update_reload_and_abort(entry, data=data)

        return self.async_show_form(
            step_id="cloud_replace",
            data_schema=_CLOUD_CREDENTIAL_SCHEMA,
            errors=errors,
        )

    async def async_step_cloud_unlink(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.FlowResult:
        """Remove active local credentials without claiming vendor revocation."""
        entry = self._get_reconfigure_entry()
        self._cloud_target_entry = entry
        if user_input is not None:
            data = dict(entry.data)
            for key in (
                CONF_CLOUD_API_KEY,
                CONF_CLOUD_REFRESH_TOKEN,
                CONF_CLOUD_SUBJECT,
            ):
                data.pop(key, None)
            data[CONF_CLOUD_LINK_GENERATION] = _next_link_generation(entry.data)
            return self.async_update_reload_and_abort(entry, data=data)

        return self.async_show_form(step_id="cloud_unlink")


class CombustionOptionsFlowHandler(config_entries.OptionsFlowWithReload):
    """Manage local runtime options with Home Assistant as reload owner."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.FlowResult:
        """Manage the options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        options = self.config_entry.options
        schema = vol.Schema(
            {
                vol.Optional(
                    CONF_AVAILABILITY_TIMEOUT,
                    default=options.get(
                        CONF_AVAILABILITY_TIMEOUT, DEFAULT_AVAILABILITY_TIMEOUT
                    ),
                ): vol.All(vol.Coerce(int), vol.Range(min=15, max=600)),
                vol.Optional(
                    CONF_UPDATE_THROTTLE,
                    default=options.get(
                        CONF_UPDATE_THROTTLE, DEFAULT_UPDATE_THROTTLE
                    ),
                ): vol.All(vol.Coerce(float), vol.Range(min=0.0, max=30.0)),
                vol.Optional(
                    CONF_ENABLE_ACTIVE_CONNECTION,
                    default=options.get(
                        CONF_ENABLE_ACTIVE_CONNECTION,
                        DEFAULT_ENABLE_ACTIVE_CONNECTION,
                    ),
                ): bool,
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema)

"""Custom integration to integrate combustion devices with Home Assistant.

For more details about this integration, please refer to
https://github.com/legrego/homeassistant-combustion
"""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.loader import async_get_integration

from custom_components.combustion.bluetooth_listener import BluetoothListener
from custom_components.combustion.connection_manager import ConnectionManager
from custom_components.combustion.control_manager import ControlManager
from custom_components.combustion.prediction_manager import PredictionManager
from custom_components.combustion.probe_manager import ProbeManager

from .cloud.ha import async_check_linked_account, cloud_linked
from .const import (
    CONF_AVAILABILITY_TIMEOUT,
    CONF_ENABLE_ACTIVE_CONNECTION,
    CONF_UPDATE_THROTTLE,
    DEFAULT_AVAILABILITY_TIMEOUT,
    DEFAULT_ENABLE_ACTIVE_CONNECTION,
    DEFAULT_UPDATE_THROTTLE,
    DOMAIN,
    LOGGER,
)
from .runtime import CombustionRuntime

type CombustionConfigEntry = ConfigEntry[CombustionRuntime]

PLATFORMS: list[Platform] = [
    Platform.BINARY_SENSOR,
    Platform.SENSOR,
    Platform.NUMBER,
    Platform.SELECT,
    Platform.BUTTON,
]

FRONTEND_CARD_URL = "/combustion/combustion-card.js"
FRONTEND_REGISTERED_KEY = f"{DOMAIN}_frontend_registered"


async def _async_register_frontend_card(hass: HomeAssistant) -> None:
    """Serve and auto-load the bundled combustion-card Lovelace card.

    Best-effort: dashboards work without it, so a failure (e.g. frontend not
    loaded, as in tests) must never break integration setup.
    """
    if hass.data.get(FRONTEND_REGISTERED_KEY):
        return
    try:
        http = getattr(hass, "http", None)
        if http is None:
            return
        card_path = str(Path(__file__).parent / "www" / "combustion-card.js")
        try:
            from homeassistant.components.http import StaticPathConfig

            await http.async_register_static_paths(
                [StaticPathConfig(FRONTEND_CARD_URL, card_path, True)]
            )
        except ImportError:
            # Home Assistant < 2024.6
            http.register_static_path(FRONTEND_CARD_URL, card_path, True)

        from homeassistant.components.frontend import add_extra_js_url

        integration = await async_get_integration(hass, DOMAIN)
        version = integration.version or "0"
        add_extra_js_url(hass, f"{FRONTEND_CARD_URL}?v={version}")
        hass.data[FRONTEND_REGISTERED_KEY] = True
    except Exception:  # noqa: BLE001
        LOGGER.debug(
            "Could not register the combustion card frontend resource", exc_info=True
        )


# https://developers.home-assistant.io/docs/config_entries_index/#setting-up-an-entry
async def async_setup_entry(
    hass: HomeAssistant, entry: CombustionConfigEntry
) -> bool:
    """Set up this integration using UI."""
    await _async_register_frontend_card(hass)

    availability_timeout = entry.options.get(
        CONF_AVAILABILITY_TIMEOUT, DEFAULT_AVAILABILITY_TIMEOUT
    )
    update_throttle = entry.options.get(
        CONF_UPDATE_THROTTLE, DEFAULT_UPDATE_THROTTLE
    )
    enable_active_connection = entry.options.get(
        CONF_ENABLE_ACTIVE_CONNECTION, DEFAULT_ENABLE_ACTIVE_CONNECTION
    )

    listener = BluetoothListener(hass, entry)
    probe_manager = ProbeManager(
        listener,
        availability_timeout_seconds=float(availability_timeout),
        min_notify_interval_seconds=float(update_throttle),
    )
    connection_manager = ConnectionManager(
        hass, entry, probe_manager, bool(enable_active_connection)
    )
    prediction_manager = PredictionManager(
        hass, entry, connection_manager, probe_manager
    )
    control_manager = ControlManager(connection_manager)

    runtime = CombustionRuntime(
        bluetooth_listener=listener,
        probe_manager=probe_manager,
        connection_manager=connection_manager,
        prediction_manager=prediction_manager,
        control_manager=control_manager,
    )
    entry.runtime_data = runtime

    # Preserve the integration's historical runtime contracts while platforms
    # are migrated incrementally to typed entry.runtime_data in later slices.
    hass.data[DOMAIN] = probe_manager
    hass.data[f"{DOMAIN}_prediction"] = prediction_manager
    hass.data[f"{DOMAIN}_connection"] = connection_manager

    # The entity platforms reach the managers through hass.data[DOMAIN] (the
    # ProbeManager). Attach them here as the single hand-off seam.
    probe_manager.connection_manager = connection_manager
    probe_manager.control_manager = control_manager
    probe_manager.active_enabled = bool(enable_active_connection)

    # Optional S2+ supervisors use runtime.create_optional_task(). The runtime
    # stop callback owns only those tasks; existing BLE/GATT managers retain
    # their established cleanup callbacks.
    entry.async_on_unload(runtime.async_stop)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    probe_manager.async_init()
    listener.async_init()
    prediction_manager.async_init()
    connection_manager.async_init()

    # Optional cloud validation is deliberately background work: a provider
    # outage, invalid token, or response-shape change must never make the
    # established local BLE/GATT setup fail. Persistent auth failure starts
    # Home Assistant's reauth flow instead of raising from async_setup_entry.
    if cloud_linked(entry.data):
        runtime.create_optional_task(
            hass,
            entry,
            async_check_linked_account(hass, entry, runtime.cloud_health),
            "combustion-cloud-link-check",
        )

    # When a device stops advertising, no bluetooth callback fires to push the
    # entities to unavailable; re-notify periodically so availability updates.
    # The @callback decoration is essential: without it Home Assistant runs
    # the job in a thread-pool executor, and the resulting state writes from
    # outside the event loop are rejected (and unsafe).
    @callback
    def _async_availability_tick(_now) -> None:
        probe_manager.notify_listeners()

    availability_check_interval = timedelta(
        seconds=max(5.0, float(availability_timeout) / 3)
    )
    entry.async_on_unload(
        async_track_time_interval(
            hass,
            _async_availability_tick,
            availability_check_interval,
        )
    )

    return True


async def async_unload_entry(
    hass: HomeAssistant, entry: CombustionConfigEntry
) -> bool:
    """Unload one config entry and its existing platform projections."""
    if not await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        return False

    runtime = entry.runtime_data
    # Do not clobber aliases if a future overlapping failure path has already
    # installed a different runtime. Normal operation still preserves the
    # legacy empty-dict sentinel used by older tests/code after unload.
    if hass.data.get(DOMAIN) is runtime.probe_manager:
        hass.data[DOMAIN] = {}
    if hass.data.get(f"{DOMAIN}_prediction") is runtime.prediction_manager:
        hass.data.pop(f"{DOMAIN}_prediction", None)
    if hass.data.get(f"{DOMAIN}_connection") is runtime.connection_manager:
        hass.data.pop(f"{DOMAIN}_connection", None)
    return True

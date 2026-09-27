"""Listen for all Bluetooth advertisements from the Combustion, Inc. manufacturer."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from home_assistant_bluetooth import BluetoothServiceInfoBleak
from homeassistant.components import bluetooth
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from custom_components.combustion.combustion_ble.advertising_data import (
    CombustionProductType,
)
from custom_components.combustion.combustion_ble.combustion_probe_data import (
    CombustionProbeData,
)
from custom_components.combustion.combustion_ble.gauge_data import CombustionGaugeData
from custom_components.combustion.combustion_ble.node_data import NodeData
from custom_components.combustion.const import BT_MANUFACTURER_ID, LOGGER

_LOGGER = LOGGER.getChild("bluetooth-listener")

# Product types whose advertisements carry probe data.
_PROBE_DATA_TYPES = (CombustionProductType.PROBE, CombustionProductType.MEAT_NET_NODE)


@dataclass(frozen=True, slots=True)
class BluetoothObservation:
    """One parsed HA-delivered Bluetooth reception with preserved provenance.

    This is the S2 capture seam, not an archival record. S4 may normalize the
    parser object into durable source rows, but it must not need a second
    Bluetooth callback to recover reception provenance.
    """

    device_data: Any
    received_at_monotonic: float
    source_address: str
    scanner_source: str | None
    rssi: int | float | None
    upstream_time: float | None
    connectable: bool | None


def parse_advertisement(service_info: BluetoothServiceInfoBleak):
    """Parse a manufacturer advertisement into device data, or None to discard.

    Shared by the bluetooth listener and the config flow so that discovery
    accepts exactly the set of devices the integration can actually represent.
    """
    payload = service_info.manufacturer_data.get(BT_MANUFACTURER_ID)
    if not payload:
        return None

    product_type = CombustionProductType.from_byte(payload[0])

    if product_type in _PROBE_DATA_TYPES:
        probe_data = CombustionProbeData.from_advertisement(service_info)
        if probe_data is None or not probe_data.valid:
            _LOGGER.debug("Discarding invalid advertisement from [%s]", service_info.address)
            return None
        return probe_data

    if product_type == CombustionProductType.GAUGE:
        gauge_data = CombustionGaugeData.from_advertisement(service_info)
        if gauge_data is None or not gauge_data.valid:
            _LOGGER.debug("Discarding invalid gauge advertisement from [%s]", service_info.address)
            return None
        return gauge_data

    if product_type in (CombustionProductType.BOOSTER, CombustionProductType.DISPLAY):
        node_data = NodeData.from_advertisement(service_info)
        if node_data is None or not node_data.valid:
            return None
        return node_data

    # Engines and unknown future products aren't handled yet.
    _LOGGER.debug("Ignoring %s advertisement from [%s]", product_type.name, service_info.address)
    return None


class BluetoothListener:
    """Listen for all Bluetooth advertisements from the Combustion, Inc. manufacturer."""

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry):
        """Initialize."""
        self.hass = hass
        self.config_entry = config_entry
        self._listeners = []
        self._observation_listeners = []
        self._parse_failures = set()

    def add_update_listener(self, listener):
        """Add a legacy listener receiving only parsed device data."""
        self._listeners.append(listener)

        def _remove():
            if listener in self._listeners:
                self._listeners.remove(listener)

        return _remove

    def add_observation_listener(self, listener):
        """Add a synchronous listener receiving the reception envelope."""
        self._observation_listeners.append(listener)

        def _remove():
            if listener in self._observation_listeners:
                self._observation_listeners.remove(listener)

        return _remove

    def async_init(self):
        """Async initialization."""
        self.config_entry.async_on_unload(
            bluetooth.async_register_callback(
                self.hass,
                self._bt_callback,
                bluetooth.BluetoothCallbackMatcher(manufacturer_id=BT_MANUFACTURER_ID, connectable=False),
                bluetooth.BluetoothScanningMode.ACTIVE,
            )
        )
        self.config_entry.async_on_unload(self.async_unload)

    def async_unload(self):
        """Async unload."""
        self._listeners.clear()
        self._observation_listeners.clear()

    def _bt_callback(self, service_info: BluetoothServiceInfoBleak, change):
        """Handle incoming BT advertisements."""
        if self.hass.is_stopping:
            return

        try:
            device_data = self._parse_advertisement(service_info)
        except Exception:  # noqa: BLE001 - a parse failure must never propagate into the bluetooth manager
            if service_info.address not in self._parse_failures:
                self._parse_failures.add(service_info.address)
                _LOGGER.warning(
                    "Failed to parse advertisement from [%s]; further failures from this device will not be logged",
                    service_info.address,
                    exc_info=True,
                )
            return

        if device_data is None:
            return

        source = getattr(service_info, "source", None)
        if not isinstance(source, str):
            source = None
        upstream_time = getattr(service_info, "time", None)
        if type(upstream_time) not in (int, float):
            upstream_time = None
        rssi = getattr(service_info, "rssi", None)
        if type(rssi) not in (int, float):
            rssi = None
        connectable = getattr(service_info, "connectable", None)
        if type(connectable) is not bool:
            connectable = None

        observation = BluetoothObservation(
            device_data=device_data,
            received_at_monotonic=time.monotonic(),
            source_address=str(getattr(service_info, "address", "")),
            scanner_source=source,
            rssi=rssi,
            upstream_time=upstream_time,
            connectable=connectable,
        )

        # Fan-out is failure-independent. A future archive queue offer must not
        # be able to stop local entity projection, and one legacy listener must
        # not prevent another from seeing the same valid reception.
        for listener in list(self._observation_listeners):
            try:
                listener(observation)
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Bluetooth observation listener failed")

        for listener in list(self._listeners):
            try:
                listener(device_data)
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Bluetooth update listener failed")

    def _parse_advertisement(self, service_info: BluetoothServiceInfoBleak):
        """Parse a manufacturer advertisement into device data, or None to discard."""
        return parse_advertisement(service_info)

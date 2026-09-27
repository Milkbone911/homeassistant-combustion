"""Read predictions from probes over the shared connectable BLE connection.

Prediction data (ready-in ETA, setpoint, estimated core) is only available
over a connection to the probe's Probe Status characteristic. This requires a
*connectable* bluetooth path — a local adapter or an ESPHome active proxy;
passive proxies (Shelly) cannot provide it.

Opt-in only: nothing here runs unless the "predictions" option is enabled
(enforced by the shared ConnectionManager).
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback

from custom_components.combustion.combustion_ble.mode_id import ModeId, ProbeMode
from custom_components.combustion.combustion_ble.prediction_data import PredictionData
from custom_components.combustion.connection_manager import ConnectionManager
from custom_components.combustion.const import LOGGER
from custom_components.combustion.probe_manager import ProbeManager

_LOGGER = LOGGER.getChild("prediction")

PROBE_STATUS_CHAR = "00000101-caab-3792-3d44-97ae51c1407a"

# The official Combustion iOS BLE framework marks probe data stale after
# 15 seconds without advertisements or notifications. Prediction status is
# notification-only in this integration, so current prediction fitness uses
# the same bound rather than retaining an ETA indefinitely.
PREDICTION_STALE_SECONDS = 15.0


@dataclass(frozen=True, slots=True)
class PredictionObservation:
    """One parsed prediction notification before HA entity projection."""

    serial: str
    received_at_monotonic: float
    prediction: PredictionData


class PredictionManager:
    """Consume Probe Status notifications and surface fresh prediction data."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        connection_manager: ConnectionManager,
        probe_manager: ProbeManager,
    ) -> None:
        """Initialize as a consumer of the shared connection."""
        self.hass = hass
        self.entry = entry
        self.connection_manager = connection_manager
        self.probe_manager = probe_manager

        # Retain the most recently parsed prediction as source evidence, while
        # prediction() applies connection/freshness fitness for current HA
        # projection.
        self.data: dict[str, PredictionData] = {}
        self._last_received: dict[str, float] = {}
        self._expiry_handles: dict[str, asyncio.TimerHandle] = {}

        self._create_sensors_callback = None
        self._listeners = []
        self._observation_listeners = []
        self._known: set[str] = set()

    def init_sensor_platform(self, create_sensors_callback):
        """Register the callback used to add prediction entities."""
        self._create_sensors_callback = create_sensors_callback

    def async_init(self) -> None:
        """Subscribe once to Probe Status and current connection changes."""
        # Preserve the existing single GATT owner/subscription. S4 observers
        # attach inside this manager rather than replacing this characteristic
        # handler.
        self.connection_manager.subscribe(PROBE_STATUS_CHAR, self._on_status)
        remove_connection_listener = self.connection_manager.add_connection_listener(
            self._on_connection_change
        )
        self.entry.async_on_unload(remove_connection_listener)
        self.entry.async_on_unload(self.async_unload)

    def async_unload(self) -> None:
        """Cancel freshness timers and detach manager-local listeners."""
        for handle in self._expiry_handles.values():
            handle.cancel()
        self._expiry_handles.clear()
        self._listeners.clear()
        self._observation_listeners.clear()

    def add_update_listener(self, listener):
        """Add listener to be notified of prediction-current-state updates.

        Returns a callable that removes the listener again.
        """
        self._listeners.append(listener)

        def _remove():
            if listener in self._listeners:
                self._listeners.remove(listener)

        return _remove

    def add_observation_listener(self, listener):
        """Add a synchronous consumer of parsed prediction observations."""
        self._observation_listeners.append(listener)

        def _remove():
            if listener in self._observation_listeners:
                self._observation_listeners.remove(listener)

        return _remove

    def prediction(self, serial_number: str) -> PredictionData | None:
        """Return the latest prediction only while it is fit as current."""
        prediction = self.data.get(serial_number)
        received_at = self._last_received.get(serial_number)
        if prediction is None or received_at is None:
            return None
        if not self.connection_manager.is_connected(serial_number):
            return None
        if time.monotonic() - received_at >= PREDICTION_STALE_SECONDS:
            return None
        return prediction

    def _notify_listeners(self) -> None:
        """Notify entity listeners without allowing one failure to stop fan-out."""
        for listener in list(self._listeners):
            try:
                listener()
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Prediction update listener failed")

    def _offer_observation(self, observation: PredictionObservation) -> None:
        """Offer a parsed prediction before current-state/entity projection."""
        for listener in list(self._observation_listeners):
            try:
                listener(observation)
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Prediction observation listener failed")

    def _schedule_expiry(self, serial: str) -> None:
        """Schedule a state refresh when this prediction becomes stale."""
        prior = self._expiry_handles.pop(serial, None)
        if prior is not None:
            prior.cancel()
        self._expiry_handles[serial] = self.hass.loop.call_later(
            PREDICTION_STALE_SECONDS,
            self._expire_prediction,
            serial,
        )

    @callback
    def _expire_prediction(self, serial: str) -> None:
        """Refresh entities when the 15-second freshness window expires."""
        self._expiry_handles.pop(serial, None)
        received_at = self._last_received.get(serial)
        if received_at is None:
            return

        remaining = PREDICTION_STALE_SECONDS - (time.monotonic() - received_at)
        if remaining > 0:
            # Monotonic timer resolution/clock patching can fire fractionally
            # early; preserve the full evidence-backed freshness window.
            self._expiry_handles[serial] = self.hass.loop.call_later(
                remaining,
                self._expire_prediction,
                serial,
            )
            return
        self._notify_listeners()

    @callback
    def _on_connection_change(self) -> None:
        """Invalidate current predictions immediately when a GATT link drops."""
        for serial in tuple(self.data):
            if not self.connection_manager.is_connected(serial):
                handle = self._expiry_handles.pop(serial, None)
                if handle is not None:
                    handle.cancel()
                # Disconnect is a hard current-fitness boundary. Retain the
                # parsed value in data for future historical capture, but a
                # reconnect must receive a new status notification before the
                # prediction can become current again.
                self._last_received.pop(serial, None)
                self.probe_manager.clear_status_mode(serial)
        self._notify_listeners()

    def _on_status(self, serial: str, data: bytes) -> None:
        """Handle a Probe Status notification from the shared connection."""
        if data is None or len(data) <= 21:
            return

        received_at = time.monotonic()
        mode = ModeId.from_byte(data[21]).mode
        self.probe_manager.update_status_mode(serial, mode, received_at)

        # Combustion's official Android/iOS managers update prediction state
        # only from Normal-mode Probe Status. Instant-Read status has its own
        # independent data path, and interpreting its prediction bytes as
        # current caused live inserted/not_inserted oscillation.
        if mode != ProbeMode.normal:
            return

        prediction = PredictionData.from_status_characteristic(data)
        if prediction is None:
            return

        observation = PredictionObservation(serial, received_at, prediction)

        # Preserve the qualified prediction observation before any entity
        # callback. S4 can register a bounded queue offer here without adding
        # or replacing the sole Probe Status GATT subscription.
        self._offer_observation(observation)

        self.data[serial] = prediction
        self._last_received[serial] = received_at
        self._schedule_expiry(serial)

        if serial not in self._known and self._create_sensors_callback is not None:
            self._known.add(serial)
            self._create_sensors_callback(self, serial)

        self._notify_listeners()

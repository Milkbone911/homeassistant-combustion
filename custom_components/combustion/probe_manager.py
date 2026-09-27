"""Manage discovered Combustion devices (probes and gauges)."""

import time

from homeassistant.core import callback

from custom_components.combustion.bluetooth_listener import (
    BluetoothListener,
    BluetoothObservation,
)
from custom_components.combustion.combustion_ble.mode_id import ProbeMode
from custom_components.combustion.const import LOGGER

_LOGGER = LOGGER.getChild("probe_manager")

# Advertisements arrive roughly every 250ms (and from multiple bluetooth
# proxies); pushing every one of them through the entity state machine and
# recorder overwhelms Home Assistant. Updates are throttled per device.
# Configurable via the integration options.
DEFAULT_MIN_NOTIFY_INTERVAL_SECONDS = 1.0

# A device that has not advertised for this long is considered unavailable.
# Probes advertise every 250ms and gauges/nodes every few seconds, so silence
# means the device is off, out of range, or in its charger. Configurable via
# the integration options.
DEFAULT_AVAILABILITY_TIMEOUT_SECONDS = 90.0

# While direct probe advertisements are arriving, data repeated by MeatNet
# nodes (booster/display) for the same probe is ignored: the repeated copy can
# be slightly stale, and letting it overwrite fresh direct readings causes
# values to flip-flop. Repeated data is used again once the probe itself has
# been silent for this long (e.g. it is out of range of every proxy).
DIRECT_DATA_PREFERENCE_SECONDS = 5.0

# An instant read value older than this is no longer shown; the probe has
# left instant-read mode or gone silent.
INSTANT_READ_STALE_SECONDS = 15.0

# Advertisement mode is not a globally ordered state stream: normal and
# Instant Read packets can coexist through direct/proxy/MeatNet paths. Treat
# packet-mode evidence as recent for a short bounded window; conflicting fresh
# streams are reported as unknown unless the existing GATT Probe Status stream
# provides a current authoritative mode.
MODE_ADVERTISEMENT_FRESH_SECONDS = 5.0
STATUS_MODE_FRESH_SECONDS = 15.0


class ProbeManager:
    """Manage discovered Combustion devices."""

    def __init__(
        self,
        bt_listener: BluetoothListener,
        availability_timeout_seconds: float = DEFAULT_AVAILABILITY_TIMEOUT_SECONDS,
        min_notify_interval_seconds: float = DEFAULT_MIN_NOTIFY_INTERVAL_SECONDS,
    ) -> None:
        """Initialize."""
        self.bluetooth_listener = bt_listener
        self.availability_timeout_seconds = availability_timeout_seconds
        self.min_notify_interval_seconds = min_notify_interval_seconds
        self.create_sensors_callback = None
        self.create_binary_sensors_callback = None

        # Normal/live projection cache. Instant-read advertisements never enter
        # this cache because T2-T8/virtual values in that mode are not valid
        # normal-mode measurements.
        self.data = {}

        self._listeners = []
        self._observation_listeners = []
        self._last_notify: dict[str, float] = {}
        self._last_seen: dict[str, float] = {}
        self._last_direct_seen: dict[str, float] = {}
        self._instant_read: dict[str, tuple[float, float]] = {}
        self._failed_devices: set[str] = set()
        self._known_devices: set[str] = set()
        self._latest_observation: dict[str, BluetoothObservation] = {}
        self._mode_seen: dict[str, dict[ProbeMode, float]] = {}
        self._status_mode_seen: dict[str, dict[ProbeMode, float]] = {}
        self._projected_mode_name: dict[str, str | None] = {}

    def init_sensor_platform(self, create_sensors_callback):
        """Initialize sensor platform."""
        self.create_sensors_callback = create_sensors_callback

    def init_binary_sensor_platform(self, create_sensors_callback):
        """Initialize binary sensor platform."""
        self.create_binary_sensors_callback = create_sensors_callback

    def async_init(self):
        """Async initialization."""
        # The reception envelope is created by the existing Bluetooth callback.
        # No additional HA Bluetooth registration is introduced for archival
        # preparation.
        if hasattr(self.bluetooth_listener, "add_observation_listener"):
            self.bluetooth_listener.add_observation_listener(
                self.create_observation_callback()
            )
        else:
            # Compatibility fallback for older/mocked listeners.
            self.bluetooth_listener.add_update_listener(self.create_update_callback())

    @staticmethod
    def _mode_name(device_data) -> str | None:
        """Return the current probe mode without relying on the normal cache."""
        mode_name = getattr(device_data, "mode_name", None)
        if isinstance(mode_name, str):
            return mode_name

        mode = getattr(device_data, "mode", None)
        return {
            ProbeMode.normal: "normal",
            ProbeMode.instantRead: "instant_read",
            ProbeMode.error: "error",
            ProbeMode.reserved: "reserved",
        }.get(mode)

    def create_update_callback(self):
        """Create the legacy raw-device callback used by older tests/callers."""
        observation_update = self.create_observation_callback()

        @callback
        def update(device_data):
            """Wrap raw device data in a minimal reception envelope."""
            rssi = getattr(device_data, "rssi", None)
            if type(rssi) not in (int, float):
                rssi = None
            observation_update(
                BluetoothObservation(
                    device_data=device_data,
                    received_at_monotonic=time.monotonic(),
                    source_address=str(getattr(device_data, "address", "")),
                    scanner_source=None,
                    rssi=rssi,
                    upstream_time=None,
                    connectable=None,
                )
            )

        return update

    def create_observation_callback(self):
        """Create callback for selected reception handling."""

        @callback
        def update(observation: BluetoothObservation):
            """Handle one parsed HA-delivered Combustion observation."""
            device_data = observation.device_data
            serial = device_data.serial_number
            # Live selection/throttle state keeps its established local clock;
            # the envelope's earlier receipt timestamp is preserved separately
            # for future archival provenance.
            now = time.monotonic()

            # Preserve the historical availability behavior: even a repeated
            # copy rejected by direct-source preference proves that the probe
            # is still represented on MeatNet.
            self._last_seen[serial] = now

            # Apply the existing shared source preference before either the
            # future archive seam or the platform/entity failure gate.
            if device_data.device_type == "PROBE":
                self._last_direct_seen[serial] = now
            elif device_data.device_type == "MEAT_NET_NODE":
                last_direct = self._last_direct_seen.get(serial)
                if (
                    last_direct is not None
                    and now - last_direct < DIRECT_DATA_PREFERENCE_SECONDS
                ):
                    return

            self._latest_observation[serial] = observation
            mode = getattr(device_data, "mode", None)
            if isinstance(mode, ProbeMode):
                self._mode_seen.setdefault(serial, {})[mode] = now
                self._notify_mode_if_changed(serial)

            # S2 capture seam: synchronous/non-awaiting and failure-isolated.
            # S4 can attach a bounded queue offer here without coupling capture
            # to successful entity creation or notification.
            self._offer_selected_observation(observation)

            # Entity/platform failure must not stop selected observation fan-out.
            if serial in self._failed_devices:
                return

            is_instant_read = getattr(device_data, "mode", None) == ProbeMode.instantRead
            if is_instant_read:
                # Only T1 is meaningful in instant-read mode.
                self._instant_read[serial] = (device_data.temperature_data[0], now)
            else:
                # Keep regular data separate. This also means a first-ever
                # instant packet creates the entity identities but cannot seed
                # synthetic T2-T8/core/surface/ambient values.
                self.data[serial] = device_data

            is_new = serial not in self._known_devices
            if is_new:
                _LOGGER.debug("Adding sensors for new device [%s]", serial)
                try:
                    self.create_sensors_callback(self, device_data)
                    self.create_binary_sensors_callback(self, device_data)
                except Exception:  # noqa: BLE001
                    # Never retry a failing device on every advertisement; that
                    # creates an unbounded stream of duplicate entities and log spam.
                    self._failed_devices.add(serial)
                    _LOGGER.exception(
                        "Failed to create entities for device [%s]; ignoring live projection for this device",
                        serial,
                    )
                    return
                self._known_devices.add(serial)

            if (
                not is_new
                and now - self._last_notify.get(serial, 0.0)
                < self.min_notify_interval_seconds
            ):
                return
            self._last_notify[serial] = now

            self.notify_listeners()

        return update

    def _offer_selected_observation(self, observation: BluetoothObservation) -> None:
        """Offer one source-selected observation without blocking live projection."""
        for listener in list(self._observation_listeners):
            try:
                listener(observation)
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Selected observation listener failed")

    def add_observation_listener(self, listener):
        """Register a synchronous selected-observation consumer.

        S4 is expected to register a bounded queue offer here. The callback
        must return immediately; the live BLE path never awaits archival work.
        """
        self._observation_listeners.append(listener)

        def _remove_listener():
            if listener in self._observation_listeners:
                self._observation_listeners.remove(listener)

        return _remove_listener

    def notify_listeners(self):
        """Notify all listeners that device state may have changed."""
        for listener in list(self._listeners):
            try:
                listener()
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Probe update listener failed")

    def instant_read_temperature(self, serial_number: str):
        """Most recent instant read temperature, or None when stale/absent."""
        entry = self._instant_read.get(serial_number)
        if entry is None:
            return None
        value, seen_at = entry
        if time.monotonic() - seen_at > INSTANT_READ_STALE_SECONDS:
            return None
        return value

    def current_mode_name(self, serial_number: str) -> str | None:
        """Return the best-supported current mode without latest-packet flicker."""
        now = time.monotonic()

        recent_modes = {
            mode
            for mode, seen_at in self._mode_seen.get(serial_number, {}).items()
            if now - seen_at < MODE_ADVERTISEMENT_FRESH_SECONDS
        }
        recent_modes.update(
            mode
            for mode, seen_at in self._status_mode_seen.get(serial_number, {}).items()
            if now - seen_at < STATUS_MODE_FRESH_SECONDS
        )

        if len(recent_modes) == 1:
            return self._mode_name_from_enum(next(iter(recent_modes)))
        if recent_modes:
            # Combustion's official frameworks maintain Normal and Instant
            # Read as independent streams for both advertisements and status
            # notifications. Seeing both recently is therefore ambiguous; the
            # arrival order is not evidence of a physical mode transition.
            return "unknown"
        return None

    def _notify_mode_if_changed(self, serial_number: str) -> None:
        """Notify entity listeners when the evidence-derived mode changes."""
        mode_name = self.current_mode_name(serial_number)
        if self._projected_mode_name.get(serial_number) == mode_name:
            return
        self._projected_mode_name[serial_number] = mode_name
        self.notify_listeners()

    @staticmethod
    def _mode_name_from_enum(mode: ProbeMode) -> str:
        """Return the Home Assistant enum value for a probe mode."""
        return {
            ProbeMode.normal: "normal",
            ProbeMode.instantRead: "instant_read",
            ProbeMode.error: "error",
            ProbeMode.reserved: "reserved",
        }.get(mode, "unknown")

    def update_status_mode(
        self, serial_number: str, mode: ProbeMode, received_at: float
    ) -> None:
        """Publish mode evidence from the existing GATT Probe Status stream."""
        self._status_mode_seen.setdefault(serial_number, {})[mode] = received_at
        self._notify_mode_if_changed(serial_number)

    def clear_status_mode(self, serial_number: str) -> None:
        """Clear stale/disconnected GATT mode evidence and refresh projection."""
        self._status_mode_seen.pop(serial_number, None)
        self._notify_mode_if_changed(serial_number)

    def latest_device_data(self, serial_number: str):
        """Latest selected parser object for diagnostic/provenance fields."""
        observation = self._latest_observation.get(serial_number)
        if observation is None:
            raise KeyError(serial_number)
        return observation.device_data

    def device_available(self, serial_number: str) -> bool:
        """Whether the device has advertised recently."""
        last_seen = self._last_seen.get(serial_number)
        if last_seen is None:
            return False
        return time.monotonic() - last_seen < self.availability_timeout_seconds

    def add_update_listener(self, listener):
        """Add listener to be notified of probe updates.

        Returns a callable that removes the listener again.
        """
        self._listeners.append(listener)

        def _remove_listener():
            if listener in self._listeners:
                self._listeners.remove(listener)

        return _remove_listener

    def probe_data(self, serial_number: str):
        """Return normal/live device data for the provided serial number."""
        return self.data[serial_number]

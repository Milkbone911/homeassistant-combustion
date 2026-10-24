"""Parser for Combustion Engine BLE advertisements.

The Engine is a MeatNet repeater that interleaves its own advertisement between
the repeated probe advertisements. See https://github.com/combustion-inc/combustion-documentation
(engine_ble_specification.rst).
"""
from __future__ import annotations

from typing import NamedTuple

from custom_components.combustion.const import LOGGER

# manufacturer_data payload layout (no vendor prefix):
#   [0]     Product Type (Engine = 6)
#   [1:11]  Serial number (10 bytes, alphanumeric)
#   [11:13] Temperature set point (13-bit packed, 0.1C resolution, -20C offset)
#   [13]    Engine status flags
#   [14]    Engine preferences
#   [15:23] Reserved
ENGINE_SERIAL_RANGE = slice(1, 11)
ENGINE_SET_POINT_RANGE = slice(11, 13)
ENGINE_FLAGS_INDEX = 13
ENGINE_PREFERENCES_INDEX = 14
ENGINE_MIN_LENGTH = 15


class EngineData(NamedTuple):
    """Parsed Engine advertisement."""

    serial_number: str
    set_point: float | None
    app_mode: bool
    control_device_connected: bool
    lid_open: bool
    fixed_speed: bool
    high_radio_power: bool

    @staticmethod
    def from_payload(payload: bytes) -> EngineData | None:
        """Create instance from manufacturer_data (no vendor prefix)."""
        if payload is None or len(payload) < ENGINE_MIN_LENGTH:
            LOGGER.debug("Not constructing Engine data; payload too short [%s]", len(payload) if payload else None)
            return None

        serial_number = bytes(payload[ENGINE_SERIAL_RANGE]).rstrip(b'\x00 ').decode('ascii', errors='replace')
        if not serial_number:
            return None

        raw_set_point = int.from_bytes(payload[ENGINE_SET_POINT_RANGE], byteorder='little') & 0x1FFF
        # A zero raw value decodes to -20C, which is below the 0-575C range the
        # spec allows, so treat it as "no set point" rather than a real target.
        set_point = (raw_set_point * 0.1 - 20.0) if raw_set_point else None

        flags = payload[ENGINE_FLAGS_INDEX]
        return EngineData(
            serial_number=serial_number,
            set_point=set_point,
            app_mode=bool(flags & 0x1),
            control_device_connected=bool(flags & 0x2),
            lid_open=bool(flags & 0x4),
            fixed_speed=bool(flags & 0x8),
            high_radio_power=bool(payload[ENGINE_PREFERENCES_INDEX] & 0x1),
        )


class CombustionEngineData:
    """Data for a Combustion Engine, as consumed by the entity platforms."""

    def __init__(self, engine_data: EngineData, rssi: int, address: str) -> None:
        """Initialize the class."""
        self.engine_data = engine_data
        self._rssi = rssi
        self._address = address

    @property
    def valid(self) -> bool:
        """Determine if the engine data is valid."""
        return bool(self.engine_data.serial_number)

    @property
    def address(self) -> str:
        """The address of the device which sent the advertising payload."""
        return self._address

    @property
    def device_type(self) -> str:
        """Type of device which sent the advertising payload."""
        return 'ENGINE'

    @property
    def rssi(self) -> int:
        """Signal strength."""
        return self._rssi

    @property
    def serial_number(self) -> str:
        """Serial number of the engine."""
        return self.engine_data.serial_number

    @property
    def set_point(self) -> float | None:
        """Target temperature in C, or None when none is set."""
        return self.engine_data.set_point

    @property
    def app_mode(self) -> bool:
        """Whether an app controls the set point."""
        return self.engine_data.app_mode

    @property
    def control_device_connected(self) -> bool:
        """Whether the Engine can see its control probe or gauge."""
        return self.engine_data.control_device_connected

    @property
    def lid_open(self) -> bool:
        """Whether the Engine detects the grill lid open."""
        return self.engine_data.lid_open

    @property
    def fixed_speed(self) -> bool:
        """Whether the fan runs at a fixed speed instead of to the set point."""
        return self.engine_data.fixed_speed

    @property
    def high_radio_power(self) -> bool:
        """Whether the Engine transmits at high radio power (+8 dBm)."""
        return self.engine_data.high_radio_power

    @staticmethod
    def from_advertisement(service_info) -> CombustionEngineData | None:
        """Create instance from BT advertisement data."""
        from custom_components.combustion.const import BT_MANUFACTURER_ID

        engine_data = EngineData.from_payload(service_info.manufacturer_data.get(BT_MANUFACTURER_ID))
        if engine_data is None:
            return None
        return CombustionEngineData(engine_data, service_info.rssi, service_info.address)

"""Tests for Combustion Engine self-advertisement parsing."""
from combustion.bluetooth_listener import parse_advertisement
from combustion.combustion_ble.engine_data import CombustionEngineData, EngineData

# Captured from a real Engine (serial E000001790) through HA's Bluetooth
# diagnostics: set point raw 0x0357, app mode on, high radio power on.
REAL_ENGINE_PAYLOAD = bytes([0x06]) + b'E000001790' + bytes([0x57, 0x03, 0x01, 0x01]) + bytes(8)


class _ServiceInfo:
    """Minimal stand-in for BluetoothServiceInfoBleak."""

    def __init__(self, payload: bytes, rssi: int = -60, address: str = 'e6:08:3e:ed:7d:67'):
        self.manufacturer_data = {2503: payload}
        self.rssi = rssi
        self.address = address


def _payload(set_point_raw: int = 0x0357, flags: int = 0x00, prefs: int = 0x00,
             serial: bytes = b'E000001790') -> bytes:
    return bytes([0x06]) + serial + set_point_raw.to_bytes(2, 'little') + bytes([flags, prefs]) + bytes(8)


def test_parses_real_engine_advertisement():
    """The captured advertisement decodes to the Engine's serial, set point and flags."""
    engine = CombustionEngineData.from_advertisement(_ServiceInfo(REAL_ENGINE_PAYLOAD))
    assert engine is not None
    assert engine.valid
    assert engine.device_type == 'ENGINE'
    assert engine.serial_number == 'E000001790'
    assert round(engine.set_point, 1) == 65.5
    assert engine.app_mode is True
    assert engine.control_device_connected is False
    assert engine.lid_open is False
    assert engine.fixed_speed is False
    assert engine.high_radio_power is True
    assert engine.rssi == -60
    assert engine.address == 'e6:08:3e:ed:7d:67'


def test_status_flag_bits():
    """Each status flag maps to its own bit."""
    assert EngineData.from_payload(_payload(flags=0x02)).control_device_connected is True
    assert EngineData.from_payload(_payload(flags=0x04)).lid_open is True
    assert EngineData.from_payload(_payload(flags=0x08)).fixed_speed is True
    all_on = EngineData.from_payload(_payload(flags=0x0F))
    assert all_on.app_mode and all_on.control_device_connected and all_on.lid_open and all_on.fixed_speed


def test_set_point_ignores_padding_bits():
    """Only the low 13 bits carry the set point."""
    engine = EngineData.from_payload(_payload(set_point_raw=0xE000 | 0x0357))
    assert round(engine.set_point, 1) == 65.5


def test_zero_set_point_is_unset():
    """A zero raw set point means none is set, not -20C."""
    assert EngineData.from_payload(_payload(set_point_raw=0)).set_point is None


def test_rejects_short_payload():
    """A payload without the flag and preference bytes is rejected."""
    assert EngineData.from_payload(REAL_ENGINE_PAYLOAD[:14]) is None


def test_rejects_empty_serial():
    """An engine advert with an all-zero serial is rejected."""
    assert EngineData.from_payload(_payload(serial=bytes(10))) is None


def test_listener_routes_engine_advertisements():
    """The shared parser hands Engine advertisements to the Engine parser."""
    data = parse_advertisement(_ServiceInfo(REAL_ENGINE_PAYLOAD))
    assert data is not None
    assert data.device_type == 'ENGINE'
    assert data.serial_number == 'E000001790'

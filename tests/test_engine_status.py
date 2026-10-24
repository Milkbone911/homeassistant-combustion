"""Tests for MeatNet node UART framing and Engine Status (0x70) decoding."""
import pytest
from combustion.combustion_ble.engine_status import (
    ENGINE_STATUS_MESSAGE_TYPE,
    EngineStatus,
    NodeMessageReassembler,
)
from combustion.combustion_ble.uart import crc16_ccitt


def _request_frame(msg_type: int, payload: bytes, request_id: int = 0x12345678) -> bytes:
    body = bytes([msg_type]) + request_id.to_bytes(4, 'little') + bytes([len(payload)]) + payload
    crc = crc16_ccitt(body)
    return b'\xca\xfe' + crc.to_bytes(2, 'little') + body


def _response_frame(msg_type: int, payload: bytes, success: bool = True) -> bytes:
    body = (bytes([msg_type | 0x80]) + (1).to_bytes(4, 'little') + (2).to_bytes(4, 'little')
            + bytes([int(success), len(payload)]) + payload)
    crc = crc16_ccitt(body)
    return b'\xca\xfe' + crc.to_bytes(2, 'little') + body


def _temp_point(celsius: float) -> bytes:
    return round((celsius + 20.0) / 0.1).to_bytes(2, 'little')


def _status_payload(**overrides) -> bytes:
    """Build an Engine Status payload laid out per engine_ble_specification.rst."""
    fields = {
        'serial': b'E000001790',
        'session_id': 0xDEADBEEF,
        'sample_period': 1000,
        'log_min': 5,
        'log_max': 900,
        'battery': bytes([1, 1, 37]),             # low, charging, 3.7V
        'set_point': _temp_point(107.2),
        'control_temperature': _temp_point(98.6),
        'control_device_type': 1,                  # probe
        'control_device_serial': (0x10018CA8).to_bytes(4, 'little') + bytes(8),
        'flags': 0x03,                             # app mode + control device connected
        'fan': bytes([4, 42, 50, 48]) + (1500).to_bytes(4, 'little') + (2500).to_bytes(4, 'little'),
        'controller': (bytes([3, 100, 12, 0x01]) + (1072).to_bytes(2, 'little')
                       + bytes([45]) + (-5).to_bytes(1, 'little', signed=True)),
        'network': 0x01,                           # 2 hops
        'knob_voltage': (1650).to_bytes(2, 'little'),
        'knob_angle': (1805).to_bytes(2, 'little'),
        'charging_fault': (4 << 1) | 1,            # input fault, present
    }
    fields.update(overrides)
    payload = (
        fields['serial']
        + fields['session_id'].to_bytes(4, 'little')
        + fields['sample_period'].to_bytes(2, 'little')
        + fields['log_min'].to_bytes(4, 'little') + fields['log_max'].to_bytes(4, 'little')
        + fields['battery']
        + fields['set_point']
        + fields['control_temperature']
        + bytes([fields['control_device_type']])
        + fields['control_device_serial']
        + bytes([fields['flags']])
        + fields['fan']
        + fields['controller']
        + bytes([fields['network']])
        + fields['knob_voltage']
        + fields['knob_angle']
        + bytes([fields['charging_fault']])
    )
    assert len(payload) == 71
    return payload


def test_decodes_every_status_field():
    """Each Engine Status field decodes to its documented value."""
    status = EngineStatus.from_payload(_status_payload())
    assert status.serial_number == 'E000001790'
    assert status.session_id == 0xDEADBEEF
    assert status.sample_period_ms == 1000
    assert status.log_range == (5, 900)
    assert status.battery_level == 'low'
    assert status.battery_state == 'charging'
    assert status.battery_voltage == pytest.approx(3.7)
    assert status.set_point == pytest.approx(107.2)
    assert status.control_temperature == pytest.approx(98.6)
    assert status.control_device_type == 'probe'
    assert status.control_device_serial == '10018ca8'
    assert status.app_mode is True
    assert status.control_device_connected is True
    assert status.lid_open is False
    assert status.fixed_speed is False
    assert status.fan_state == 'on'
    assert status.fan_duty_cycle == 42
    assert status.fan_commanded_speed == 50
    assert status.fan_measured_speed == 48
    assert status.fan_off_time_ms == 1500
    assert status.fan_on_time_ms == 2500
    assert status.controller_state == 'observe'
    assert status.response_coefficient == pytest.approx(0.2)
    assert status.cycles_completed == 12
    assert status.reached_set_point is True
    assert status.maintenance_mode is False
    assert status.smoothed_temperature == pytest.approx(107.2)
    assert status.time_to_peak_s == 45
    assert status.drift_rate == pytest.approx(-0.005)
    assert status.hop_count == 2
    assert status.knob_voltage == pytest.approx(1.65)
    assert status.knob_angle == pytest.approx(180.5)
    assert status.charging_fault == 'input_fault'


# Captured from a real, idle Engine (E000001790) over HA's Engine connection:
# app mode on, Grill Gauge set as control device but not connected, fan off.
REAL_IDLE_STATUS = bytes.fromhex(
    '45303030303031373930e63e95dd8813d0010000d001000000007a5703c800'
    '0347303030303031363145000001030000008813000000000000000f000000'
    '00000000ff09350200'
)


def test_decodes_real_idle_engine_status():
    """The captured status decodes to the Engine's real, idle state."""
    status = EngineStatus.from_payload(REAL_IDLE_STATUS)
    assert status.serial_number == 'E000001790'
    assert status.sample_period_ms == 5000
    assert status.battery_level == 'ok'
    assert status.battery_state == 'not_charging'
    assert status.battery_voltage == pytest.approx(12.2)
    assert status.set_point == pytest.approx(65.5)
    assert status.control_device_type == 'gauge'
    assert status.control_device_serial == 'G00000161E'
    assert status.app_mode is True
    assert status.control_device_connected is False
    # Raw 200 (0.0C) is sent while the control device is not connected.
    assert status.control_temperature is None
    assert status.fan_state == 'off'
    assert status.controller_state == 'idle'
    assert status.smoothed_temperature is None
    assert status.knob_angle == pytest.approx(56.5)
    assert status.charging_fault == 'none'


def test_node_control_device_serial_is_text():
    """A gauge or node control device carries an alphanumeric serial."""
    payload = _status_payload(control_device_type=3, control_device_serial=b'G00000161E\x00\x00')
    status = EngineStatus.from_payload(payload)
    assert status.control_device_type == 'gauge'
    assert status.control_device_serial == 'G00000161E'


def test_unset_temperatures_are_none():
    """Zero temperature points mean no set point or no control reading."""
    status = EngineStatus.from_payload(_status_payload(set_point=bytes(2), control_temperature=bytes(2)))
    assert status.set_point is None
    assert status.control_temperature is None


def test_no_charging_fault():
    """A zero fault byte reports no fault."""
    assert EngineStatus.from_payload(_status_payload(charging_fault=0)).charging_fault == 'none'


def test_rejects_short_status():
    """A payload shorter than the documented layout is rejected."""
    assert EngineStatus.from_payload(_status_payload()[:70]) is None


def test_accepts_longer_status():
    """Bytes appended by newer firmware are ignored."""
    assert EngineStatus.from_payload(_status_payload() + b'\x00\x00') is not None


def test_reassembles_single_frame():
    """One whole notification yields one message."""
    messages = NodeMessageReassembler().feed(_request_frame(ENGINE_STATUS_MESSAGE_TYPE, _status_payload()))
    assert len(messages) == 1
    assert messages[0].msg_type == ENGINE_STATUS_MESSAGE_TYPE
    assert messages[0].is_response is False
    assert messages[0].payload == _status_payload()


def test_reassembles_fragmented_frame():
    """A frame split across 20-byte notifications is rebuilt once complete."""
    frame = _request_frame(ENGINE_STATUS_MESSAGE_TYPE, _status_payload())
    reassembler = NodeMessageReassembler()
    messages = []
    for i in range(0, len(frame), 20):
        messages.extend(reassembler.feed(frame[i:i + 20]))
    assert len(messages) == 1
    assert messages[0].payload == _status_payload()


def test_splits_concatenated_frames():
    """Two frames in one notification yield two messages."""
    data = _request_frame(0x49, b'\x01\x02') + _request_frame(ENGINE_STATUS_MESSAGE_TYPE, _status_payload())
    messages = NodeMessageReassembler().feed(data)
    assert [m.msg_type for m in messages] == [0x49, ENGINE_STATUS_MESSAGE_TYPE]


def test_parses_response_header():
    """Responses use the 15-byte header with a success byte."""
    messages = NodeMessageReassembler().feed(_response_frame(0x71, b'', success=True))
    assert len(messages) == 1
    assert messages[0].msg_type == 0x71
    assert messages[0].is_response is True
    assert messages[0].success is True


def test_skips_corrupt_frame_and_resyncs():
    """Leading garbage and a bad-CRC frame are dropped; the next good frame survives."""
    good = _request_frame(ENGINE_STATUS_MESSAGE_TYPE, _status_payload())
    bad = bytearray(_request_frame(0x49, b'\x01\x02'))
    bad[-1] ^= 0xFF
    messages = NodeMessageReassembler().feed(b'\x00\x13' + bytes(bad) + good)
    assert len(messages) == 1
    assert messages[0].msg_type == ENGINE_STATUS_MESSAGE_TYPE


def test_sync_split_across_notifications():
    """A frame whose sync bytes straddle two notifications is still found."""
    frame = _request_frame(ENGINE_STATUS_MESSAGE_TYPE, _status_payload())
    reassembler = NodeMessageReassembler()
    assert reassembler.feed(b'\x00' + frame[:1]) == []
    assert len(reassembler.feed(frame[1:])) == 1


class _ServiceInfo:
    """Minimal stand-in for BluetoothServiceInfoBleak."""

    def __init__(self, payload: bytes, address: str = 'e6:08:3e:ed:7d:67'):
        self.manufacturer_data = {2503: payload}
        self.address = address


def _engine_manager(hass):
    from unittest.mock import MagicMock

    from combustion.engine_connection import EngineConnectionManager
    entry = MagicMock()
    entry.async_on_unload = MagicMock()
    return EngineConnectionManager(hass, entry, enabled=True, min_notify_interval_seconds=0.0), entry


@pytest.mark.asyncio
async def test_connects_only_to_engines(hass):
    """Probe and booster advertisements never start an Engine connection."""
    mgr, entry = _engine_manager(hass)
    mgr._connectable_advertisement(_ServiceInfo(bytes([0x01]) + bytes(22)), None)
    mgr._connectable_advertisement(_ServiceInfo(bytes([0x05]) + b'CR1000BC2C' + bytes(12)), None)
    entry.async_create_background_task.assert_not_called()

    engine_adv = bytes([0x06]) + b'E000001790' + bytes([0x57, 0x03, 0x01, 0x01]) + bytes(8)
    mgr._connectable_advertisement(_ServiceInfo(engine_adv), None)
    entry.async_create_background_task.assert_called_once()
    assert mgr._addresses == {'E000001790': 'e6:08:3e:ed:7d:67'}


@pytest.mark.asyncio
async def test_status_published_while_connected(hass):
    """A status notification creates the Engine once and is readable while connected."""
    mgr, _entry = _engine_manager(hass)
    new_engines, updates = [], []
    mgr.add_new_engine_listener(new_engines.append)
    mgr.add_listener(lambda: updates.append(1))

    mgr._clients['E000001790'] = object()
    notify = mgr._make_notify('E000001790', NodeMessageReassembler())
    frame = _request_frame(ENGINE_STATUS_MESSAGE_TYPE, _status_payload())
    notify(None, bytearray(frame))
    notify(None, bytearray(frame))

    assert [s.serial_number for s in new_engines] == ['E000001790']
    assert len(updates) == 2
    assert mgr.status('E000001790').fan_state == 'on'

    del mgr._clients['E000001790']
    assert mgr.status('E000001790') is None


@pytest.mark.asyncio
async def test_status_goes_stale(hass, monkeypatch):
    """A connected Engine that stops reporting reads as unavailable."""
    import combustion.engine_connection as engine_connection

    mgr, _entry = _engine_manager(hass)
    clock = [1000.0]
    monkeypatch.setattr(engine_connection, '_now', lambda: clock[0])
    mgr._clients['E000001790'] = object()
    mgr._make_notify('E000001790', NodeMessageReassembler())(
        None, bytearray(_request_frame(ENGINE_STATUS_MESSAGE_TYPE, _status_payload())))
    assert mgr.status('E000001790') is not None
    clock[0] += engine_connection.STATUS_STALE_SECONDS + 1
    assert mgr.status('E000001790') is None


@pytest.mark.asyncio
async def test_disabled_manager_registers_nothing(hass):
    """With the option off, no Bluetooth callback is registered."""
    from unittest.mock import MagicMock

    from combustion.engine_connection import EngineConnectionManager
    entry = MagicMock()
    EngineConnectionManager(hass, entry, enabled=False).async_init()
    entry.async_on_unload.assert_not_called()

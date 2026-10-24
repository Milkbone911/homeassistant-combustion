"""MeatNet node UART framing and the Engine Status (0x70) message.

Nodes (Booster, Display, Engine) frame UART messages differently from probes:

Request:  CA FE | CRC(2, LE) | type(1) | request ID(4) | len(1) | payload
Response: CA FE | CRC(2, LE) | type(1, high bit set) | request ID(4) |
          response ID(4) | success(1) | len(1) | payload

The CRC (CRC-16/CCITT-FALSE) covers everything after the CRC field. See
meatnet_node_ble_specification.rst and engine_ble_specification.rst in
https://github.com/combustion-inc/combustion-documentation.
"""
from __future__ import annotations

from typing import NamedTuple

from .uart import SYNC, crc16_ccitt

NODE_REQUEST_HEADER_SIZE = 10
NODE_RESPONSE_HEADER_SIZE = 15
RESPONSE_FLAG = 0x80
# A notification stream that never resyncs must not grow without bound.
MAX_BUFFER_SIZE = 1024

ENGINE_STATUS_MESSAGE_TYPE = 0x70
ENGINE_STATUS_PAYLOAD_SIZE = 71

FAN_STATES = {0: 'power_down', 1: 'paused', 2: 'lid_open', 3: 'off', 4: 'on'}
CONTROLLER_STATES = {0: 'idle', 1: 'startup', 2: 'probe', 3: 'observe', 4: 'rest'}
BATTERY_LEVELS = {0: 'ok', 1: 'low', 2: 'critical'}
BATTERY_STATES = {0: 'not_charging', 1: 'charging', 2: 'fully_charged'}
CHARGING_FAULTS = {
    0: 'none',
    1: 'over_temperature',
    2: 'under_temperature',
    3: 'ntc_fault',
    4: 'input_fault',
    5: 'battery_missing',
    6: 'charge_timer_expired',
    7: 'charge_stall',
}
PRODUCT_TYPE_NAMES = {1: 'probe', 2: 'node', 3: 'gauge', 4: 'display', 5: 'booster', 6: 'engine'}


class NodeMessage(NamedTuple):
    """One CRC-checked node UART message."""

    msg_type: int
    is_response: bool
    success: bool | None
    payload: bytes


class NodeMessageReassembler:
    """Split a node's TX notification stream into whole messages.

    A message can span several notifications, and one notification can carry
    several messages, so bytes are buffered until a complete frame is present.
    """

    def __init__(self) -> None:
        """Initialize with an empty buffer."""
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[NodeMessage]:
        """Add notification bytes and return every message they complete."""
        self._buffer.extend(data)
        if len(self._buffer) > MAX_BUFFER_SIZE:
            del self._buffer[:-MAX_BUFFER_SIZE]

        messages: list[NodeMessage] = []
        while True:
            start = self._buffer.find(SYNC)
            if start < 0:
                # Keep a trailing 0xCA in case the next notification starts with 0xFE.
                del self._buffer[:-1]
                return messages
            del self._buffer[:start]
            if len(self._buffer) < 5:
                return messages

            is_response = bool(self._buffer[4] & RESPONSE_FLAG)
            header_size = NODE_RESPONSE_HEADER_SIZE if is_response else NODE_REQUEST_HEADER_SIZE
            if len(self._buffer) < header_size:
                return messages
            frame_size = header_size + self._buffer[header_size - 1]
            if len(self._buffer) < frame_size:
                return messages

            frame = bytes(self._buffer[:frame_size])
            sent_crc = frame[2] | (frame[3] << 8)
            if crc16_ccitt(frame[4:]) != sent_crc:
                # Not a real frame boundary: skip this sync and look for the next.
                del self._buffer[:2]
                continue

            del self._buffer[:frame_size]
            messages.append(NodeMessage(
                msg_type=frame[4] & ~RESPONSE_FLAG,
                is_response=is_response,
                success=bool(frame[13]) if is_response else None,
                payload=frame[header_size:],
            ))


def _temperature_point(raw: bytes) -> float | None:
    """Decode a 13-bit 0.1C temperature point, or None for a zero (unset) value."""
    value = int.from_bytes(raw, byteorder='little') & 0x1FFF
    return (value * 0.1 - 20.0) if value else None


def _control_device_serial(device_type: int, raw: bytes) -> str | None:
    """Decode the Product Serial Number union for the given product type."""
    if device_type == 1:
        serial = int.from_bytes(raw[0:4], byteorder='little')
        return f'{serial:08x}' if serial else None
    text = bytes(raw[0:10]).rstrip(b'\x00 ').decode('ascii', errors='replace')
    return text or None


class EngineStatus(NamedTuple):
    """Decoded Engine Status (0x70) payload."""

    serial_number: str
    session_id: int
    sample_period_ms: int
    log_range: tuple[int, int]
    battery_level: str | None
    battery_state: str | None
    battery_voltage: float
    set_point: float | None
    control_temperature: float | None
    control_device_type: str | None
    control_device_serial: str | None
    app_mode: bool
    control_device_connected: bool
    lid_open: bool
    fixed_speed: bool
    fan_state: str | None
    fan_duty_cycle: int
    fan_commanded_speed: int
    fan_measured_speed: int
    fan_off_time_ms: int
    fan_on_time_ms: int
    controller_state: str | None
    response_coefficient: float
    cycles_completed: int
    reached_set_point: bool
    maintenance_mode: bool
    smoothed_temperature: float | None
    time_to_peak_s: int
    drift_rate: float
    hop_count: int
    knob_voltage: float
    knob_angle: float
    charging_fault: str | None

    @staticmethod
    def from_payload(payload: bytes) -> EngineStatus | None:
        """Decode an Engine Status payload, or None if it is too short."""
        if len(payload) < ENGINE_STATUS_PAYLOAD_SIZE:
            return None

        serial_number = bytes(payload[0:10]).rstrip(b'\x00 ').decode('ascii', errors='replace')
        if not serial_number:
            return None

        def u16(offset: int) -> int:
            return int.from_bytes(payload[offset:offset + 2], byteorder='little')

        def u32(offset: int) -> int:
            return int.from_bytes(payload[offset:offset + 4], byteorder='little')

        device_type = payload[31]
        flags = payload[44]
        control_device_connected = bool(flags & 0x2)
        controller_state = CONTROLLER_STATES.get(payload[57])
        controller_flags = payload[60]
        return EngineStatus(
            serial_number=serial_number,
            session_id=u32(10),
            sample_period_ms=u16(14),
            log_range=(u32(16), u32(20)),
            battery_level=BATTERY_LEVELS.get(payload[24]),
            battery_state=BATTERY_STATES.get(payload[25]),
            battery_voltage=payload[26] / 10.0,
            set_point=_temperature_point(payload[27:29]),
            # A real Engine reports raw 200 (0.0C) while it cannot see its
            # control device, so only trust the reading while it is connected.
            control_temperature=_temperature_point(payload[29:31]) if control_device_connected else None,
            control_device_type=PRODUCT_TYPE_NAMES.get(device_type),
            control_device_serial=_control_device_serial(device_type, payload[32:44]),
            app_mode=bool(flags & 0x1),
            control_device_connected=control_device_connected,
            lid_open=bool(flags & 0x4),
            fixed_speed=bool(flags & 0x8),
            fan_state=FAN_STATES.get(payload[45]),
            fan_duty_cycle=payload[46],
            fan_commanded_speed=payload[47],
            fan_measured_speed=payload[48],
            fan_off_time_ms=u32(49),
            fan_on_time_ms=u32(53),
            controller_state=controller_state,
            response_coefficient=payload[58] / 500.0,
            cycles_completed=payload[59],
            reached_set_point=bool(controller_flags & 0x1),
            maintenance_mode=bool(controller_flags & 0x2),
            # The controller only tracks a temperature once it leaves idle.
            smoothed_temperature=u16(61) / 10.0 if controller_state != 'idle' else None,
            time_to_peak_s=payload[63],
            drift_rate=int.from_bytes(payload[64:65], byteorder='little', signed=True) / 1000.0,
            hop_count=(payload[65] & 0x3) + 1,
            knob_voltage=u16(66) / 1000.0,
            knob_angle=u16(68) / 10.0,
            charging_fault=CHARGING_FAULTS.get(payload[70] >> 1),
        )

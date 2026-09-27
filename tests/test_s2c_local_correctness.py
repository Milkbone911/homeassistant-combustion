"""S2c regressions for local mode/freshness and observation seams."""
from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.combustion.bluetooth_listener import (
    BluetoothListener,
    BluetoothObservation,
)
from custom_components.combustion.combustion_ble.mode_id import ProbeMode
from custom_components.combustion.combustion_ble.prediction_data import (
    PREDICTION_STATUS_OFFSET,
)
from custom_components.combustion.const import DOMAIN
from custom_components.combustion.prediction_manager import (
    PROBE_STATUS_CHAR,
    PredictionManager,
)
from custom_components.combustion.probe_manager import ProbeManager
from tests.utils.bt_utils import (
    create_advertisement,
    create_combustion_bits,
    inject_bt_advertisement,
)


async def _setup_entry(hass: HomeAssistant, unique_id: str) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=1,
        unique_id=unique_id,
        data={},
        title="Meatnet",
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


@pytest.mark.asyncio
async def test_first_discovery_instant_read_never_seeds_normal_temperatures(
    hass: HomeAssistant,
):
    """A first-ever instant packet exposes instant/mode but no fake normal temperatures."""
    await _setup_entry(hass, "s2c-first-instant")

    instant = create_advertisement(
        create_combustion_bits(
            mode=ProbeMode.instantRead.value,
            temperature_data=[85.0] + [25.0] * 7,
        )
    )

    # The first discovery can cause the existing one-time address reload. Send
    # the same instant observation again so the post-reload runtime is the one
    # being asserted.
    inject_bt_advertisement(hass, instant)
    await hass.async_block_till_done()
    inject_bt_advertisement(hass, instant)
    await hass.async_block_till_done()

    registry = entity_registry.async_get(hass)
    entries = list(registry.entities.values())
    instant_id = next(
        e.entity_id
        for e in entries
        if (e.unique_id or "").endswith("--sensor--instant-read")
    )
    core_id = next(
        e.entity_id
        for e in entries
        if (e.unique_id or "").endswith("--sensor--core")
    )
    mode_id = next(
        e.entity_id for e in entries if (e.unique_id or "").endswith("--mode")
    )

    assert float(hass.states.get(instant_id).state) == 85.0
    assert hass.states.get(core_id).state == "unknown"
    assert hass.states.get(mode_id).state == "instant_read"

    normal = create_advertisement(
        create_combustion_bits(
            mode=ProbeMode.normal.value,
            temperature_data=[42.0] * 8,
        )
    )
    with patch(
        "custom_components.combustion.probe_manager.time.monotonic",
        return_value=10_000.0,
    ):
        inject_bt_advertisement(hass, normal)
        await hass.async_block_till_done()

    assert float(hass.states.get(core_id).state) == 42.0
    assert hass.states.get(mode_id).state == "normal"


def test_normal_instant_normal_keeps_normal_cache_age_and_tracks_current_mode():
    """Instant mode changes current mode without refreshing normal temperature data."""
    manager = ProbeManager(bt_listener=None)
    manager.init_sensor_platform(lambda _manager, _data: None)
    manager.init_binary_sensor_platform(lambda _manager, _data: None)
    update = manager.create_update_callback()

    normal_1 = _fake_probe_data("PROBE", mode=ProbeMode.normal, tag="normal-1")
    normal_1.temperature_data = [30.0] * 8
    instant = _fake_probe_data("PROBE", mode=ProbeMode.instantRead, tag="instant")
    instant.temperature_data = [88.0] + [999.0] * 7
    normal_2 = _fake_probe_data("PROBE", mode=ProbeMode.normal, tag="normal-2")
    normal_2.temperature_data = [40.0] * 8

    with patch(
        "custom_components.combustion.probe_manager.time.monotonic",
        return_value=100.0,
    ) as monotonic:
        update(normal_1)
        assert manager.current_mode_name("abc123") == "normal"
        assert manager.probe_data("abc123").temperature_data == [30.0] * 8

        monotonic.return_value = 106.0
        update(instant)
        assert manager.current_mode_name("abc123") == "instant_read"
        assert manager.instant_read_temperature("abc123") == 88.0
        # Invalid instant-mode T2-T8 never refresh the normal cache.
        assert manager.probe_data("abc123").temperature_data == [30.0] * 8

        monotonic.return_value = 112.0
        update(normal_2)
        assert manager.current_mode_name("abc123") == "normal"
        assert manager.probe_data("abc123").temperature_data == [40.0] * 8


def _fake_probe_data(device_type: str, *, mode=ProbeMode.normal, tag: str = ""):
    return SimpleNamespace(
        serial_number="abc123",
        device_type=device_type,
        mode=mode,
        mode_name={
            ProbeMode.normal: "normal",
            ProbeMode.instantRead: "instant_read",
            ProbeMode.error: "error",
            ProbeMode.reserved: "reserved",
        }[mode],
        temperature_data=[50.0] * 8,
        address="AA:BB:CC:DD:EE:FF",
        rssi=-60,
        tag=tag,
    )


def test_conflicting_advertisement_modes_report_unknown_instead_of_flapping():
    """Interleaved mode streams are ambiguous, not physical mode transitions."""
    manager = ProbeManager(bt_listener=None)
    manager.init_sensor_platform(lambda _manager, _data: None)
    manager.init_binary_sensor_platform(lambda _manager, _data: None)
    update = manager.create_update_callback()

    normal = _fake_probe_data("PROBE", mode=ProbeMode.normal, tag="normal")
    instant = _fake_probe_data("PROBE", mode=ProbeMode.instantRead, tag="instant")

    with patch(
        "custom_components.combustion.probe_manager.time.monotonic",
        return_value=100.0,
    ) as monotonic:
        update(normal)
        assert manager.current_mode_name("abc123") == "normal"

        monotonic.return_value = 101.0
        update(instant)
        assert manager.current_mode_name("abc123") == "unknown"

        monotonic.return_value = 102.0
        update(normal)
        assert manager.current_mode_name("abc123") == "unknown"

        monotonic.return_value = 108.0
        assert manager.current_mode_name("abc123") is None


def test_selected_observation_seam_precedes_entity_failure_and_source_rejection():
    """Archive seam survives entity failure and sees only source-selected receptions."""
    manager = ProbeManager(bt_listener=None)
    create_attempts = []

    def fail_entities(_manager, _data):
        create_attempts.append(1)
        raise RuntimeError("synthetic entity registration failure")

    manager.init_sensor_platform(fail_entities)
    manager.init_binary_sensor_platform(lambda _manager, _data: None)

    selected: list[BluetoothObservation] = []
    manager.add_observation_listener(selected.append)
    update = manager.create_update_callback()

    with patch(
        "custom_components.combustion.probe_manager.time.monotonic",
        return_value=100.0,
    ):
        update(_fake_probe_data("PROBE", tag="direct-1"))

    assert len(selected) == 1
    assert len(create_attempts) == 1

    # Entity projection is now failed, but selected observations must continue.
    with patch(
        "custom_components.combustion.probe_manager.time.monotonic",
        return_value=101.0,
    ):
        update(_fake_probe_data("PROBE", tag="direct-2"))

    assert len(selected) == 2
    assert len(create_attempts) == 1

    # A repeated copy inside the five-second direct preference window is not a
    # selected observation and therefore is not offered to the future archive.
    with patch(
        "custom_components.combustion.probe_manager.time.monotonic",
        return_value=102.0,
    ):
        update(_fake_probe_data("MEAT_NET_NODE", tag="repeat-rejected"))

    assert len(selected) == 2

    # Once direct data is stale, the repeated route becomes selected.
    with patch(
        "custom_components.combustion.probe_manager.time.monotonic",
        return_value=107.0,
    ):
        update(_fake_probe_data("MEAT_NET_NODE", tag="repeat-selected"))

    assert len(selected) == 3
    assert selected[-1].device_data.tag == "repeat-selected"


def test_bluetooth_reception_envelope_is_frozen_and_listener_failures_are_isolated():
    """One callback preserves provenance while bad consumers cannot stop fan-out."""
    hass = SimpleNamespace(is_stopping=False)
    entry = MagicMock()
    listener = BluetoothListener(hass, entry)
    device_data = _fake_probe_data("PROBE")

    listener._parse_advertisement = lambda _service_info: device_data

    observations = []
    updates = []

    def fail_observation(_observation):
        raise RuntimeError("synthetic observation consumer failure")

    def fail_update(_device_data):
        raise RuntimeError("synthetic legacy consumer failure")

    listener.add_observation_listener(fail_observation)
    listener.add_observation_listener(observations.append)
    listener.add_update_listener(fail_update)
    listener.add_update_listener(updates.append)

    service_info = SimpleNamespace(
        address="11:22:33:44:55:66",
        source="scanner-a",
        time=123.5,
        rssi=-51,
        connectable=False,
    )

    with patch(
        "custom_components.combustion.bluetooth_listener.time.monotonic",
        return_value=456.0,
    ):
        listener._bt_callback(service_info, None)

    assert updates == [device_data]
    assert len(observations) == 1
    observation = observations[0]
    assert observation.device_data is device_data
    assert observation.received_at_monotonic == 456.0
    assert observation.source_address == "11:22:33:44:55:66"
    assert observation.scanner_source == "scanner-a"
    assert observation.upstream_time == 123.5
    assert observation.rssi == -51
    assert observation.connectable is False

    with pytest.raises(FrozenInstanceError):
        observation.rssi = -99


class _FakeConnectionManager:
    def __init__(self):
        self.subscriptions = {}
        self.listeners = []
        self.connected = set()

    def subscribe(self, char_uuid, handler):
        self.subscriptions[char_uuid] = handler

    def add_connection_listener(self, listener):
        self.listeners.append(listener)

        def remove():
            if listener in self.listeners:
                self.listeners.remove(listener)

        return remove

    def is_connected(self, serial):
        return serial in self.connected

    def fire_connection_change(self):
        for listener in list(self.listeners):
            listener()


def _prediction_status_packet(mode: ProbeMode = ProbeMode.normal) -> bytes:
    # state=predicting(3), mode=time_to_removal(1), type=removal(1),
    # setpoint=65.0C, heat start=10.0C, 1230s, estimated core=62.5C.
    state, prediction_mode, ptype = 3, 1, 1
    setpoint, heatstart, seconds, core = 650, 100, 1230, 825
    d0 = state | (prediction_mode << 4) | (ptype << 6)
    d1 = setpoint & 0xFF
    d2 = ((setpoint >> 8) & 0x03) | ((heatstart & 0x3F) << 2)
    d3 = ((heatstart >> 6) & 0x0F) | ((seconds & 0x0F) << 4)
    d4 = (seconds >> 4) & 0xFF
    d5 = ((seconds >> 12) & 0x1F) | ((core & 0x07) << 5)
    d6 = (core >> 3) & 0xFF
    prefix = bytearray(PREDICTION_STATUS_OFFSET)
    prefix[21] = mode.value
    return bytes(prefix) + bytes([d0, d1, d2, d3, d4, d5, d6])


def test_gatt_mode_evidence_does_not_turn_interleaved_streams_into_flapping(
    hass: HomeAssistant,
):
    """Status packets remain independent evidence rather than latest-packet truth."""
    probe_manager = ProbeManager(bt_listener=None)
    probe_manager.init_sensor_platform(lambda _manager, _data: None)
    probe_manager.init_binary_sensor_platform(lambda _manager, _data: None)
    update = probe_manager.create_update_callback()

    connection = _FakeConnectionManager()
    connection.connected.add("abc123")
    entry = MagicMock()
    entry.async_on_unload = MagicMock()
    prediction_manager = PredictionManager(hass, entry, connection, probe_manager)
    prediction_manager.async_init()

    normal = _fake_probe_data("PROBE", mode=ProbeMode.normal)
    instant = _fake_probe_data("PROBE", mode=ProbeMode.instantRead)

    with patch(
        "custom_components.combustion.probe_manager.time.monotonic",
        return_value=100.0,
    ) as monotonic:
        update(normal)
        monotonic.return_value = 101.0
        update(instant)
        assert probe_manager.current_mode_name("abc123") == "unknown"

        # A Normal status packet cannot erase the fresh Instant-Read evidence.
        with patch(
            "custom_components.combustion.prediction_manager.time.monotonic",
            return_value=101.5,
        ):
            connection.subscriptions[PROBE_STATUS_CHAR](
                "abc123", _prediction_status_packet(ProbeMode.normal)
            )
        monotonic.return_value = 101.5
        assert probe_manager.current_mode_name("abc123") == "unknown"

        # Once the conflicting advertisement evidence ages out, continuing
        # Normal status evidence can truthfully resolve the projection.
        monotonic.return_value = 107.0
        with patch(
            "custom_components.combustion.prediction_manager.time.monotonic",
            return_value=107.0,
        ):
            connection.subscriptions[PROBE_STATUS_CHAR](
                "abc123", _prediction_status_packet(ProbeMode.normal)
            )
        assert probe_manager.current_mode_name("abc123") == "normal"

        # Interleaved Instant-Read status is again ambiguity, not a transition.
        with patch(
            "custom_components.combustion.prediction_manager.time.monotonic",
            return_value=108.0,
        ):
            connection.subscriptions[PROBE_STATUS_CHAR](
                "abc123", _prediction_status_packet(ProbeMode.instantRead)
            )
        monotonic.return_value = 108.0
        assert probe_manager.current_mode_name("abc123") == "unknown"

        connection.connected.remove("abc123")
        connection.fire_connection_change()
        assert probe_manager.current_mode_name("abc123") is None

    prediction_manager.async_unload()


def test_instant_read_status_does_not_overwrite_prediction_state(
    hass: HomeAssistant,
):
    """Prediction follows vendor behavior: only Normal Probe Status updates it."""
    entry = MagicMock()
    entry.async_on_unload = MagicMock()
    connection = _FakeConnectionManager()
    connection.connected.add("abc123")
    probe_manager = MagicMock()
    manager = PredictionManager(hass, entry, connection, probe_manager)

    observations = []
    manager.add_observation_listener(observations.append)
    manager.async_init()

    connection.subscriptions[PROBE_STATUS_CHAR](
        "abc123", _prediction_status_packet(ProbeMode.normal)
    )
    first = manager.prediction("abc123")
    assert first is not None
    assert first.state == "predicting"
    assert len(observations) == 1

    # The bytes in this fixture still contain a prediction-shaped field, but
    # Instant-Read status must not reinterpret it as current prediction data.
    connection.subscriptions[PROBE_STATUS_CHAR](
        "abc123", _prediction_status_packet(ProbeMode.instantRead)
    )

    assert manager.prediction("abc123") is first
    assert len(observations) == 1
    manager.async_unload()


@pytest.mark.asyncio
async def test_prediction_becomes_stale_without_discarding_last_source_observation(
    hass: HomeAssistant,
):
    """A prediction is current only inside the notification freshness window."""
    entry = MagicMock()
    entry.async_on_unload = MagicMock()
    connection = _FakeConnectionManager()
    connection.connected.add("abc123")
    manager = PredictionManager(hass, entry, connection, MagicMock())

    observations = []
    ticks = []
    manager.add_observation_listener(observations.append)
    manager.add_update_listener(lambda: ticks.append(manager.prediction("abc123")))
    manager.async_init()

    with patch(
        "custom_components.combustion.prediction_manager.PREDICTION_STALE_SECONDS",
        0.05,
    ):
        connection.subscriptions[PROBE_STATUS_CHAR](
            "abc123", _prediction_status_packet()
        )
        assert manager.prediction("abc123") is not None
        assert len(observations) == 1
        assert observations[0].serial == "abc123"

        await asyncio.sleep(0.07)

        assert manager.prediction("abc123") is None
        # Source evidence remains retained for future S4 historical handling.
        assert manager.data["abc123"].seconds_remaining == 1230
        assert len(ticks) >= 2

    manager.async_unload()


def test_prediction_invalidates_immediately_on_disconnect_but_retains_raw_value(
    hass: HomeAssistant,
):
    """Disconnect is an immediate current-fitness boundary, not a history delete."""
    entry = MagicMock()
    entry.async_on_unload = MagicMock()
    connection = _FakeConnectionManager()
    connection.connected.add("abc123")
    manager = PredictionManager(hass, entry, connection, MagicMock())
    manager.async_init()

    connection.subscriptions[PROBE_STATUS_CHAR]("abc123", _prediction_status_packet())
    assert manager.prediction("abc123") is not None

    connection.connected.remove("abc123")
    connection.fire_connection_change()

    assert manager.prediction("abc123") is None
    assert manager.data["abc123"].seconds_remaining == 1230

    # A quick reconnect is not evidence that the old ETA became current again.
    connection.connected.add("abc123")
    connection.fire_connection_change()
    assert manager.prediction("abc123") is None

    # Only a fresh Probe Status notification can restore current fitness.
    connection.subscriptions[PROBE_STATUS_CHAR]("abc123", _prediction_status_packet())
    assert manager.prediction("abc123") is not None
    manager.async_unload()


def test_prediction_observation_failure_does_not_stop_current_projection(
    hass: HomeAssistant,
):
    """A future archive consumer failure cannot suppress live prediction state."""
    entry = MagicMock()
    entry.async_on_unload = MagicMock()
    connection = _FakeConnectionManager()
    connection.connected.add("abc123")
    manager = PredictionManager(hass, entry, connection, MagicMock())

    def fail_observer(_observation):
        raise RuntimeError("synthetic archive offer failure")

    manager.add_observation_listener(fail_observer)
    manager.async_init()
    connection.subscriptions[PROBE_STATUS_CHAR]("abc123", _prediction_status_packet())

    assert manager.prediction("abc123") is not None
    manager.async_unload()

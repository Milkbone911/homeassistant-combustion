"""Tests for the shared BLE ConnectionManager."""
import asyncio

import pytest
from combustion.connection_manager import ConnectionManager
from homeassistant.exceptions import HomeAssistantError


class _FakeClient:
    """Minimal stand-in for a bleak BleakClient."""

    def __init__(self):
        """Initialize with empty notification/write records."""
        self.notifications = {}
        self.written = []
        self.connected = True

    async def start_notify(self, char, handler):
        """Record the notify handler registered for a characteristic."""
        self.notifications[char] = handler

    async def write_gatt_char(self, char, data, response=True):
        """Record a GATT write, including the response mode."""
        self.written.append((char, bytes(data), response))

    async def disconnect(self):
        """Mark the fake client as disconnected."""
        self.connected = False


def _manager(hass):
    from unittest.mock import MagicMock
    entry = MagicMock()
    entry.async_on_unload = MagicMock()
    pm = MagicMock()
    return ConnectionManager(hass, entry, pm, enabled=True)


@pytest.mark.asyncio
async def test_subscribe_registers_handler_on_connect(hass):
    """A subscribed char gets start_notify called when a client connects."""
    mgr = _manager(hass)
    seen = []
    mgr.subscribe("char-a", lambda serial, data: seen.append((serial, data)))
    client = _FakeClient()
    await mgr._on_connected("SERIAL1", client)
    assert "char-a" in client.notifications
    # a notification routes to the handler with the serial
    client.notifications["char-a"](None, bytearray(b"\x01\x02"))
    assert seen == [("SERIAL1", b"\x01\x02")]


@pytest.mark.asyncio
async def test_send_command_writes_when_connected(hass):
    """async_send_command writes the frame to the UART RX char."""
    mgr = _manager(hass)
    client = _FakeClient()
    await mgr._on_connected("SERIAL1", client)
    frame = b"\xca\xfe\x00\x00\x0c\x00"
    await mgr.async_send_command("SERIAL1", frame)
    # The user command is the last write (a session-info canary is written on
    # connect first); it must be write-without-response, per the reference app.
    assert client.written[-1] == (ConnectionManager.UART_RX_CHAR, frame, False)


@pytest.mark.asyncio
async def test_notifications_enabled_before_client_is_writable(hass):
    """The probe is not marked connected until its notifications are enabled.

    A command must never be able to write before the UART TX subscription is
    active, so is_connected must stay False until start_notify has run.
    """
    mgr = _manager(hass)
    order = []

    class _OrderingClient(_FakeClient):
        async def start_notify(self, char, handler):
            order.append(("notify", char, mgr.is_connected("SERIAL1")))
            await super().start_notify(char, handler)

    await mgr._on_connected("SERIAL1", _OrderingClient())
    # start_notify ran while is_connected was still False for every subscription.
    assert order and all(connected is False for _, _, connected in order)
    assert mgr.is_connected("SERIAL1") is True


@pytest.mark.asyncio
async def test_uart_response_handler_parses_without_error(hass):
    """The UART TX response handler parses an ack frame and tolerates junk."""
    from combustion.combustion_ble.uart import crc16_ccitt

    mgr = _manager(hass)
    body = bytes([0x05, 0x01, 0x00])  # type=SET_PREDICTION, success=1, len=0
    crc = crc16_ccitt(body)
    frame = bytes([0xCA, 0xFE, crc & 0xFF, (crc >> 8) & 0xFF]) + body
    mgr._on_uart_response("SERIAL1", frame)  # valid ack: must not raise
    mgr._on_uart_response("SERIAL1", b"\x00\x01\x02")  # junk: must not raise


@pytest.mark.asyncio
async def test_send_command_raises_when_not_connected(hass):
    """async_send_command raises when the probe has no live client or address."""
    mgr = _manager(hass)
    with pytest.raises(HomeAssistantError):
        await mgr.async_send_command("UNKNOWN", b"\xca\xfe\x00\x00\x0c\x00")


@pytest.mark.asyncio
async def test_connection_listener_fires_on_state_change(hass):
    """Connect/disconnect notifies connection listeners and flips is_connected."""
    mgr = _manager(hass)
    ticks = []
    mgr.add_connection_listener(lambda: ticks.append(mgr.is_connected("SERIAL1")))
    await mgr._on_connected("SERIAL1", _FakeClient())
    assert mgr.is_connected("SERIAL1") is True
    mgr._on_disconnected("SERIAL1")
    assert mgr.is_connected("SERIAL1") is False
    assert ticks == [True, False]


@pytest.mark.asyncio
async def test_shutdown_awaits_task_teardown_before_returning(hass):
    """Shutdown must await a cancelled task's own unwinding, not just cancel it.

    Cancelling a task only schedules a CancelledError for its next resumption;
    it does not itself run the task's `finally` block. If shutdown returns
    without awaiting that unwinding (e.g. the `asyncio.gather` call is
    dropped), a maintain task's own disconnect never gets a chance to run
    before Home Assistant considers the entry unloaded.
    """
    unwound = []

    async def _loop():
        try:
            await asyncio.Event().wait()
        finally:
            # A real suspend point: proves the event loop actually resumed
            # this task's cancellation before shutdown returned.
            await asyncio.sleep(0)
            unwound.append(True)

    mgr = _manager(hass)
    client = _FakeClient()
    mgr._clients["SERIAL1"] = client
    mgr._tasks["SERIAL1"] = hass.async_create_task(_loop())

    await mgr._async_shutdown()

    assert unwound == [True]
    assert client.connected is False
    assert mgr._clients == {}
    assert mgr._tasks == {}


@pytest.mark.asyncio
async def test_shutdown_disconnects_remaining_clients_if_one_raises(hass):
    """One client's disconnect() raising must not stop the others from disconnecting.

    _async_shutdown suppresses per-client disconnect errors so a single
    misbehaving client can't leave the rest of the fleet connected.
    """
    class _RaisingClient(_FakeClient):
        async def disconnect(self):
            await super().disconnect()
            raise RuntimeError("bleak backend went away")

    mgr = _manager(hass)
    bad = _RaisingClient()
    good = _FakeClient()
    mgr._clients["BAD"] = bad
    mgr._clients["GOOD"] = good

    await mgr._async_shutdown()  # must not raise

    assert bad.connected is False
    assert good.connected is False
    assert mgr._clients == {}


@pytest.mark.asyncio
async def test_shutdown_task_teardown_is_bounded(hass):
    """A maintain task that ignores cancellation must not hang shutdown forever.

    Simulates a wedged BLE adapter: the task catches its own CancelledError
    and then waits on something that never resolves, so it can never finish
    unwinding on its own. Patches SHUTDOWN_TIMEOUT_SECONDS down to a few
    milliseconds so the test proves the bound exists without waiting out a
    real 10s timeout.
    """
    from unittest.mock import patch

    async def _wedged():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.Event().wait()  # never resolves; ignores cancellation
            raise

    mgr = _manager(hass)
    mgr._tasks["SERIAL1"] = hass.async_create_task(_wedged())

    # Must patch the `combustion.connection_manager` import path, not
    # `custom_components.combustion.connection_manager` -- this codebase's
    # test setup loads the package under both dotted paths as *separate*
    # module objects, and ConnectionManager (imported above via
    # `combustion.connection_manager`) reads its own module-level constants,
    # not the `custom_components.` duplicate's.
    with patch("combustion.connection_manager.SHUTDOWN_TIMEOUT_SECONDS", 0.05):
        await mgr._async_shutdown()  # must return, not hang

    assert mgr._tasks == {}


@pytest.mark.asyncio
async def test_shutdown_client_disconnect_is_bounded(hass):
    """A client whose disconnect() never returns must not hang shutdown forever.

    Patches SHUTDOWN_TIMEOUT_SECONDS down to a few milliseconds so the test
    proves the bound exists without waiting out a real 10s timeout.
    """
    from unittest.mock import patch

    class _WedgedClient(_FakeClient):
        async def disconnect(self):
            await asyncio.Event().wait()  # never resolves

    mgr = _manager(hass)
    mgr._clients["SERIAL1"] = _WedgedClient()

    with patch("combustion.connection_manager.SHUTDOWN_TIMEOUT_SECONDS", 0.05):
        await mgr._async_shutdown()  # must return, not hang

    assert mgr._clients == {}


@pytest.mark.asyncio
async def test_flapping_connection_backs_off(hass):
    """A probe that connects and drops immediately must not spin the loop.

    Simulates a probe that establishes a connection and is reported
    disconnected right away (weak signal, contention with the phone app).
    The maintain loop must sleep at least RECONNECT_MIN_SECONDS between
    attempts instead of hammering establish_connection in a tight loop.
    """
    from unittest.mock import patch

    from combustion.connection_manager import RECONNECT_MIN_SECONDS

    from tests.utils.bt_utils import (
        generate_ble_device,
        patch_async_ble_device_from_address,
    )

    class _StopLoop(BaseException):
        """Escapes the maintain loop's `except Exception` like CancelledError does."""

    mgr = _manager(hass)
    mgr._addresses["SERIAL1"] = "cc:cc:cc:cc:cc:cc"

    sleeps: list[float] = []
    attempts = 0

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    async def fake_establish_connection(_client_cls, _ble_device, _name, disconnected_callback=None):
        nonlocal attempts
        attempts += 1
        if attempts > 3:
            # Hard ceiling so a still-broken loop fails fast instead of hanging.
            raise _StopLoop
        client = _FakeClient()
        if disconnected_callback is not None:
            disconnected_callback(client)  # report an immediate drop
        return client

    with (
        patch_async_ble_device_from_address(generate_ble_device(address="cc:cc:cc:cc:cc:cc")),
        patch("bleak_retry_connector.establish_connection", side_effect=fake_establish_connection),
        patch("custom_components.combustion.connection_manager.asyncio.sleep", side_effect=fake_sleep),
        pytest.raises(_StopLoop),
    ):
        await mgr._maintain_connection("SERIAL1")

    # Pin the full escalation, not just its floor -- [5, 10, 20] would also
    # satisfy a looser `min(sleeps) >= RECONNECT_MIN_SECONDS` check even if
    # doubling were broken in some other way.
    assert sleeps == [RECONNECT_MIN_SECONDS, RECONNECT_MIN_SECONDS * 2, RECONNECT_MIN_SECONDS * 4]


@pytest.mark.asyncio
async def test_disconnect_happens_before_backoff_sleep(hass):
    """The dead client is disconnected before the backoff sleep, not after.

    This PR exists to release a probe's GATT slot promptly. If the sleep ran
    before `finally`'s disconnect, a flapping probe would hold that slot for
    the whole backoff window (up to RECONNECT_MAX_SECONDS) on top of no
    longer being useful -- the same failure mode this PR is meant to fix.
    """
    from unittest.mock import patch

    from tests.utils.bt_utils import (
        generate_ble_device,
        patch_async_ble_device_from_address,
    )

    class _StopLoop(BaseException):
        """Escapes the maintain loop's `except Exception` like CancelledError does."""

    class _TrackingClient(_FakeClient):
        async def disconnect(self):
            events.append("disconnect")
            await super().disconnect()

    mgr = _manager(hass)
    mgr._addresses["SERIAL1"] = "cc:cc:cc:cc:cc:cc"

    events: list[str] = []
    attempts = 0

    async def fake_sleep(seconds):
        events.append("sleep")

    async def fake_establish_connection(_client_cls, _ble_device, _name, disconnected_callback=None):
        nonlocal attempts
        attempts += 1
        if attempts > 1:
            # Hard ceiling so a still-broken loop fails fast instead of hanging.
            raise _StopLoop
        client = _TrackingClient()
        if disconnected_callback is not None:
            disconnected_callback(client)  # report an immediate drop
        return client

    with (
        patch_async_ble_device_from_address(generate_ble_device(address="cc:cc:cc:cc:cc:cc")),
        patch("bleak_retry_connector.establish_connection", side_effect=fake_establish_connection),
        patch("custom_components.combustion.connection_manager.asyncio.sleep", side_effect=fake_sleep),
        pytest.raises(_StopLoop),
    ):
        await mgr._maintain_connection("SERIAL1")

    assert events == ["disconnect", "sleep"]


@pytest.mark.asyncio
async def test_stable_connection_resets_backoff(hass):
    """A connection that holds past STABLE_CONNECTION_SECONDS resets the backoff.

    Without the reset, a probe that flaps once early on would keep sleeping an
    escalated backoff on every later drop, even long after the link proved
    itself stable. Drives one flapping iteration (backoff escalates past the
    floor), then one iteration reported as stable (elapsed >= 30s), then a
    second flapping iteration whose recorded sleep must be back at the floor.
    """
    from unittest.mock import patch

    from combustion.connection_manager import RECONNECT_MIN_SECONDS

    from tests.utils.bt_utils import (
        generate_ble_device,
        patch_async_ble_device_from_address,
    )

    class _StopLoop(BaseException):
        """Escapes the maintain loop's `except Exception` like CancelledError does."""

    mgr = _manager(hass)
    mgr._addresses["SERIAL1"] = "cc:cc:cc:cc:cc:cc"

    sleeps: list[float] = []
    attempts = 0
    # connected_at/checkpoint pairs per iteration: iter 1 and 3 report ~0s
    # elapsed (flapping); iter 2 reports 31s elapsed (stable). Falls back to
    # the real clock once exhausted, rather than raising StopIteration, in
    # case the code under test ever calls it more times than scripted here.
    import time as _time_module
    _exhausted = object()
    clock = iter([0.0, 0.0, 100.0, 131.0, 200.0, 200.0])

    def fake_now():
        value = next(clock, _exhausted)
        return _time_module.monotonic() if value is _exhausted else value

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    async def fake_establish_connection(_client_cls, _ble_device, _name, disconnected_callback=None):
        nonlocal attempts
        attempts += 1
        if attempts > 3:
            # Hard ceiling so a still-broken loop fails fast instead of hanging.
            raise _StopLoop
        client = _FakeClient()
        if disconnected_callback is not None:
            disconnected_callback(client)  # report an immediate drop
        return client

    with (
        patch_async_ble_device_from_address(generate_ble_device(address="cc:cc:cc:cc:cc:cc")),
        patch("bleak_retry_connector.establish_connection", side_effect=fake_establish_connection),
        patch("custom_components.combustion.connection_manager.asyncio.sleep", side_effect=fake_sleep),
        # Patches the module's own `_now` indirection, not `time.monotonic`
        # itself -- the real event loop also reads the stdlib clock, and a
        # global patch there previously caused the loop to observe this
        # test's scripted values too (measured 9 calls where 6 were scripted).
        # Must target `combustion.connection_manager` (see the comment in
        # test_shutdown_task_teardown_is_bounded on the dual-module-path quirk).
        patch("combustion.connection_manager._now", side_effect=fake_now),
        pytest.raises(_StopLoop),
    ):
        await mgr._maintain_connection("SERIAL1")

    # Iteration 1 sleeps at the floor, iteration 2 is reported stable (no
    # sleep, backoff resets), iteration 3 sleeps at the floor again -- not at
    # an escalated value carried over from iteration 1.
    assert sleeps == [RECONNECT_MIN_SECONDS, RECONNECT_MIN_SECONDS]


@pytest.mark.asyncio
async def test_new_probe_listener_fires_once_on_first_connect(hass):
    """A new-probe listener fires once on first connect, not on reconnect, and immediately for late registrants."""
    mgr = _manager(hass)
    probe_data = object()
    mgr._probe_data["S1"] = probe_data

    seen = []
    mgr.add_new_probe_listener(seen.append)

    await mgr._on_connected("S1", _FakeClient())
    assert seen == [probe_data]

    # a second connect for the same serial (e.g. reconnect) must not re-fire
    await mgr._on_connected("S1", _FakeClient())
    assert seen == [probe_data]

    # a listener registered after the probe was already seen fires immediately
    late_seen = []
    mgr.add_new_probe_listener(late_seen.append)
    assert late_seen == [probe_data]

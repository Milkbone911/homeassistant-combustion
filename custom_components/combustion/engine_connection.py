"""Keep a connection to each Combustion Engine and decode its status stream.

Fan, controller, battery and knob data are only in the Engine Status (0x70)
UART notification, not in advertisements. This manager connects to Engines
only; it never connects to probes, whose three connection slots are usually
held by the Booster, Engine and phone app. An Engine accepts four inbound
connections. Opt-in via the engine-connection option.
"""
from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable

from homeassistant.components import bluetooth
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback

from custom_components.combustion.combustion_ble.engine_status import (
    ENGINE_STATUS_MESSAGE_TYPE,
    EngineStatus,
    NodeMessageReassembler,
)
from custom_components.combustion.const import BT_MANUFACTURER_ID, LOGGER

_LOGGER = LOGGER.getChild('engine_connection')

ENGINE_PRODUCT_TYPE = 0x06
UART_TX_CHAR = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"

RECONNECT_MIN_SECONDS = 5
RECONNECT_MAX_SECONDS = 300
STABLE_CONNECTION_SECONDS = 30
SHUTDOWN_TIMEOUT_SECONDS = 10
# Status older than this is shown as unavailable rather than stale.
STATUS_STALE_SECONDS = 60.0

_now = time.monotonic


class EngineConnectionManager:
    """Maintain Engine connections and publish the latest decoded status."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        enabled: bool,
        min_notify_interval_seconds: float = 1.0,
    ) -> None:
        """Initialize."""
        self.hass = hass
        self.entry = entry
        self.enabled = enabled
        self._min_notify_interval = min_notify_interval_seconds
        self._addresses: dict[str, str] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._clients: dict[str, object] = {}
        self._status: dict[str, EngineStatus] = {}
        self._status_at: dict[str, float] = {}
        self._notified_at: dict[str, float] = {}
        self._listeners: list[Callable[[], None]] = []
        self._new_engine_listeners: list[Callable[[EngineStatus], None]] = []
        self._logged_first_frame: set[str] = set()

    def async_init(self) -> None:
        """Register the connectable advertisement callback when enabled."""
        if not self.enabled:
            return
        try:
            self.entry.async_on_unload(
                bluetooth.async_register_callback(
                    self.hass,
                    self._connectable_advertisement,
                    bluetooth.BluetoothCallbackMatcher(manufacturer_id=BT_MANUFACTURER_ID, connectable=True),
                    bluetooth.BluetoothScanningMode.ACTIVE,
                )
            )
        except Exception:  # noqa: BLE001
            _LOGGER.warning("Could not register connectable callback; Engine connection disabled", exc_info=True)
            return
        self.entry.async_on_unload(self._async_shutdown)

    def add_listener(self, listener: Callable[[], None]) -> Callable[[], None]:
        """Register a listener called on status or connection changes. Returns remover."""
        self._listeners.append(listener)

        def _remove():
            if listener in self._listeners:
                self._listeners.remove(listener)

        return _remove

    def add_new_engine_listener(self, listener: Callable[[EngineStatus], None]) -> None:
        """Register listener(status) fired once per Engine on its first status.

        Fires immediately for Engines that already reported a status.
        """
        self._new_engine_listeners.append(listener)
        for status in list(self._status.values()):
            listener(status)

    def status(self, serial: str) -> EngineStatus | None:
        """Return the Engine's latest status while connected and fresh, else None."""
        status = self._status.get(serial)
        if status is None or serial not in self._clients:
            return None
        if _now() - self._status_at.get(serial, 0.0) > STATUS_STALE_SECONDS:
            return None
        return status

    def is_connected(self, serial: str) -> bool:
        """Whether a live connection to this Engine exists."""
        return serial in self._clients

    def _notify_listeners(self) -> None:
        for listener in list(self._listeners):
            try:
                listener()
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Engine status listener failed")

    @callback
    def _connectable_advertisement(self, service_info, change) -> None:
        """Start a connection loop for each connectable Engine seen."""
        payload = service_info.manufacturer_data.get(BT_MANUFACTURER_ID)
        if not payload or payload[0] != ENGINE_PRODUCT_TYPE or len(payload) < 11:
            return
        serial = bytes(payload[1:11]).rstrip(b'\x00 ').decode('ascii', errors='replace')
        if not serial:
            return
        self._addresses[serial] = service_info.address
        task = self._tasks.get(serial)
        if task is None or task.done():
            self._tasks[serial] = self.entry.async_create_background_task(
                self.hass, self._maintain_connection(serial), f"combustion-engine-{serial}"
            )

    async def _maintain_connection(self, serial: str) -> None:
        """Keep one connection open to an Engine, reconnecting with backoff."""
        from bleak import BleakClient
        from bleak_retry_connector import establish_connection

        backoff = RECONNECT_MIN_SECONDS
        while True:
            address = self._addresses.get(serial)
            ble_device = (
                bluetooth.async_ble_device_from_address(self.hass, address, connectable=True)
                if address
                else None
            )
            if ble_device is None:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, RECONNECT_MAX_SECONDS)
                continue

            disconnected = asyncio.Event()
            client = None
            delay = 0
            try:
                client = await establish_connection(
                    BleakClient, ble_device, serial,
                    disconnected_callback=lambda _c, ev=disconnected: ev.set(),
                )
                connected_at = _now()
                reassembler = NodeMessageReassembler()
                await client.start_notify(UART_TX_CHAR, self._make_notify(serial, reassembler))
                self._clients[serial] = client
                _LOGGER.info("Connected to Engine [%s] (MTU %s)", serial, getattr(client, "mtu_size", None))
                self._notify_listeners()
                await disconnected.wait()
                if _now() - connected_at >= STABLE_CONNECTION_SECONDS:
                    backoff = RECONNECT_MIN_SECONDS
                else:
                    delay, backoff = backoff, min(backoff * 2, RECONNECT_MAX_SECONDS)
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Connection to Engine [%s] failed", serial, exc_info=True)
                delay, backoff = backoff, min(backoff * 2, RECONNECT_MAX_SECONDS)
            finally:
                if self._clients.pop(serial, None) is not None:
                    self._notify_listeners()
                if client is not None:
                    with contextlib.suppress(Exception):
                        await client.disconnect()
            if delay:
                await asyncio.sleep(delay)

    def _make_notify(self, serial: str, reassembler: NodeMessageReassembler) -> Callable:
        """Build the TX notification handler for one connection."""
        @callback
        def _cb(_char, data: bytearray) -> None:
            for message in reassembler.feed(bytes(data)):
                self._handle_message(serial, message)

        return _cb

    def _handle_message(self, serial: str, message) -> None:
        """Store an Engine Status message and notify listeners (throttled)."""
        if message.msg_type != ENGINE_STATUS_MESSAGE_TYPE or message.is_response:
            _LOGGER.debug("Engine [%s] sent message type 0x%02x", serial, message.msg_type)
            return
        if serial not in self._logged_first_frame:
            self._logged_first_frame.add(serial)
            _LOGGER.debug("First Engine Status from [%s]: %s", serial, message.payload.hex())
        status = EngineStatus.from_payload(message.payload)
        if status is None:
            _LOGGER.debug("Engine [%s] status not decodable (%s bytes)", serial, len(message.payload))
            return

        now = _now()
        is_new = status.serial_number not in self._status
        self._status[status.serial_number] = status
        self._status_at[status.serial_number] = now
        if is_new:
            for listener in list(self._new_engine_listeners):
                listener(status)
        if now - self._notified_at.get(status.serial_number, 0.0) >= self._min_notify_interval:
            self._notified_at[status.serial_number] = now
            self._notify_listeners()

    async def _async_shutdown(self) -> None:
        """Cancel connection tasks and wait for them to disconnect."""
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            try:
                async with asyncio.timeout(SHUTDOWN_TIMEOUT_SECONDS):
                    await asyncio.gather(*tasks, return_exceptions=True)
            except TimeoutError:
                _LOGGER.warning("Timed out waiting for Engine connection tasks to stop")
        clients = list(self._clients.values())
        self._clients.clear()
        for client in clients:
            with contextlib.suppress(Exception):
                async with asyncio.timeout(SHUTDOWN_TIMEOUT_SECONDS):
                    await client.disconnect()
        self._listeners.clear()
        self._new_engine_listeners.clear()

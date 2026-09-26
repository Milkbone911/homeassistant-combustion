"""Typed runtime ownership for the Combustion config entry.

S2a establishes lifecycle ownership without enabling cloud I/O, archive
storage, or changing the existing BLE/GATT managers' behavior.
"""
from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass, field
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .bluetooth_listener import BluetoothListener
from .connection_manager import ConnectionManager
from .control_manager import ControlManager
from .prediction_manager import PredictionManager
from .probe_manager import ProbeManager


@dataclass(slots=True)
class CombustionRuntime:
    """Objects owned by one loaded Combustion config entry.

    Existing entity platforms still consume the historical hass.data aliases
    during S2. The typed runtime becomes the authoritative lifecycle owner and
    gives later optional cloud/history supervisors one bounded, entry-owned
    task seam.
    """

    bluetooth_listener: BluetoothListener
    probe_manager: ProbeManager
    connection_manager: ConnectionManager
    prediction_manager: PredictionManager
    control_manager: ControlManager
    _optional_tasks: set[asyncio.Task[Any]] = field(
        default_factory=set, init=False, repr=False
    )
    _stopping: bool = field(default=False, init=False, repr=False)
    _stopped: bool = field(default=False, init=False, repr=False)

    @property
    def stopping(self) -> bool:
        """Return whether optional runtime work is being stopped."""
        return self._stopping

    @property
    def stopped(self) -> bool:
        """Return whether optional runtime work completed shutdown."""
        return self._stopped

    @property
    def optional_task_count(self) -> int:
        """Return the number of tracked optional supervisor tasks."""
        return len(self._optional_tasks)

    def create_optional_task(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        coro: Coroutine[Any, Any, Any],
        name: str,
    ) -> asyncio.Task[Any]:
        """Create and track entry-owned optional work.

        The config entry remains Home Assistant's owner of the task. The
        runtime additionally tracks it so optional subsystems can be drained
        before later account replacement/archive shutdown work.
        """
        if self._stopping or self._stopped:
            coro.close()
            raise RuntimeError("Combustion runtime is stopping")
        task = entry.async_create_background_task(hass, coro, name)
        self._optional_tasks.add(task)
        task.add_done_callback(self._optional_tasks.discard)
        return task

    async def async_stop(self) -> None:
        """Idempotently stop only optional work owned by this runtime.

        BLE/GATT managers retain their existing entry.async_on_unload cleanup
        callbacks. S2a deliberately does not duplicate or reorder those
        manager shutdown contracts.
        """
        if self._stopped:
            return
        self._stopping = True
        tasks = tuple(self._optional_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._optional_tasks.clear()
        self._stopped = True

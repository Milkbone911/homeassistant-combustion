"""S2a lifecycle ownership and fault-containment regression tests."""
from __future__ import annotations

import asyncio

import pytest
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.combustion.const import DOMAIN
from custom_components.combustion.runtime import CombustionRuntime


async def _setup(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="combustion_meatnet",
        version=1,
        data={},
        title="Combustion Meatnet",
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


@pytest.mark.asyncio
async def test_runtime_data_is_authoritative_while_legacy_aliases_are_preserved(
    hass: HomeAssistant,
):
    """Typed runtime must preserve the existing manager objects and aliases."""
    entry = await _setup(hass)

    runtime = entry.runtime_data
    assert isinstance(runtime, CombustionRuntime)
    assert hass.data[DOMAIN] is runtime.probe_manager
    assert hass.data[f"{DOMAIN}_prediction"] is runtime.prediction_manager
    assert hass.data[f"{DOMAIN}_connection"] is runtime.connection_manager
    assert runtime.probe_manager.connection_manager is runtime.connection_manager
    assert runtime.probe_manager.control_manager is runtime.control_manager
    assert runtime.optional_task_count == 0
    assert runtime.stopping is False
    assert runtime.stopped is False


@pytest.mark.asyncio
async def test_optional_runtime_task_is_entry_owned_and_drained_on_unload(
    hass: HomeAssistant,
):
    """Future optional supervisors must finish cancellation before unload completes."""
    entry = await _setup(hass)
    runtime = entry.runtime_data
    started = asyncio.Event()
    unwound = asyncio.Event()

    async def worker() -> None:
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            unwound.set()

    runtime.create_optional_task(hass, entry, worker(), "combustion-test-optional")
    await started.wait()
    assert runtime.optional_task_count == 1

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    assert unwound.is_set()
    assert runtime.stopping is True
    assert runtime.stopped is True
    assert runtime.optional_task_count == 0
    assert hass.data[DOMAIN] == {}
    assert f"{DOMAIN}_prediction" not in hass.data
    assert f"{DOMAIN}_connection" not in hass.data


@pytest.mark.asyncio
async def test_background_entry_data_update_does_not_reload_local_runtime(
    hass: HomeAssistant,
):
    """Routine future token persistence must not implicitly restart BLE."""
    entry = await _setup(hass)
    runtime = entry.runtime_data
    probe_manager = hass.data[DOMAIN]

    # S2b will use async_update_entry for refresh-token rotation. There is no
    # update listener in S2a, so an ordinary entry-data update must leave the
    # loaded local runtime intact.
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, "s2a_test_marker": "updated"}
    )
    await hass.async_block_till_done()

    assert entry.runtime_data is runtime
    assert hass.data[DOMAIN] is probe_manager
    assert len(entry.update_listeners) == 0


@pytest.mark.asyncio
async def test_runtime_stop_is_idempotent(hass: HomeAssistant):
    """Repeated cleanup calls must not create work or raise."""
    entry = await _setup(hass)
    runtime = entry.runtime_data

    await runtime.async_stop()
    await runtime.async_stop()

    assert runtime.stopped is True
    assert runtime.optional_task_count == 0

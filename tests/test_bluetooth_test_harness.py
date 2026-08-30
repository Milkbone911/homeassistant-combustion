"""Regression test for the shared bluetooth test-harness fixtures.

Every other test in this suite depends on the `mock_bluetooth` /
`enable_bluetooth` fixture chain in conftest.py to give it a working
bluetooth stack. Five later PRs in this series write their own bluetooth
regression tests directly against that harness (BLE-connection lookups,
scanner counts, connection slots). If that harness silently degraded --
for example, by replacing HaScanner with an unspecced mock that a Cython
type check rejects, so the "bluetooth" domain's own config entry dies
during setup instead of loading -- every test built on top of it would
still pass or fail, just for the wrong reason, and nothing would catch it.

This test asserts the harness itself is sound: the "bluetooth" domain's
own MockConfigEntry (created by pytest_homeassistant_custom_component's
enable_bluetooth fixture) must actually reach LOADED, and a scanner must
actually be registered with the manager.
"""
from homeassistant.components import bluetooth
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.util.async_ import get_scheduled_timer_handles


async def test_bluetooth_harness_scanner_loads(hass: HomeAssistant):
    """enable_bluetooth's own config entry must reach LOADED with a scanner registered."""
    entries = hass.config_entries.async_entries("bluetooth")
    assert len(entries) == 1
    assert entries[0].state is ConfigEntryState.LOADED

    assert bluetooth.async_scanner_count(hass) >= 1


async def test_bluetooth_harness_teardown_cancels_expiry_timer(hass: HomeAssistant):
    """Unloading the "bluetooth" entry must cancel BaseHaScanner's expiry timer.

    Guards the `_compensate_ha_scanner_expiry_timer_leak` fixture in
    conftest.py, which works around a Home Assistant core bug: unloading a
    bluetooth config entry never cancels the periodic device-expiry timer
    HaScanner.async_setup() schedules, because
    homeassistant/components/bluetooth/__init__.py:405 discards the one
    callback that would cancel it instead of passing it to
    entry.async_on_unload(...) like every other cleanup in that function. If
    that compensation ever silently stops working (for example, a future
    habluetooth release renaming `_unsetup`), this must fail loudly instead
    of leaving every other test in the suite to fail teardown for an
    unexplained reason.
    """
    entries = hass.config_entries.async_entries("bluetooth")
    entry = entries[0]

    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    lingering = [
        handle
        for handle in get_scheduled_timer_handles(hass.loop)
        if not handle.cancelled() and "_async_expire_devices_schedule_next" in repr(handle)
    ]
    assert not lingering, f"expire-devices timer(s) survived entry unload: {lingering}"

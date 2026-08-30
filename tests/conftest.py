"""Global fixtures for combustion integration."""
# Fixtures allow you to replace functions with a Mock object. You can perform
# many options via the Mock to reflect a particular behavior from the original
# function that you want to see without going through the function's actual logic.
# Fixtures can either be passed into tests as parameters, or if autouse=True, they
# will automatically be used across all tests.
#
# Fixtures that are defined in conftest.py are available across all tests. You can also
# define fixtures within a particular test file to scope them locally.
#
# pytest_homeassistant_custom_component provides some fixtures that are provided by
# Home Assistant core. You can find those fixture definitions here:
# https://github.com/MatthewFlamm/pytest-homeassistant-custom-component/blob/master/pytest_homeassistant_custom_component/common.py
#
# See here for more info: https://docs.pytest.org/en/latest/fixture.html (note that
# pytest includes fixtures OOB which you can use as defined on this page)

import sys
from unittest.mock import patch

import pytest
from bleak.backends import BleakBackend
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

pytest_plugins = "pytest_homeassistant_custom_component"

@pytest.fixture(autouse=True, scope="session")
def _mock_bt_history():
    """Stub BlueZ history: it calls into dbus_fast, absent on non-Linux dev boxes."""
    with patch("bluetooth_adapters.systems.linux.LinuxAdapters.history", {}):
        yield

def _real_scanner_backend_type() -> tuple[type, BleakBackend]:
    """bleak.get_platform_scanner_backend_type(), keyed off sys.platform.

    `enable_bluetooth`'s own "bluetooth" config entry setup constructs a real
    HaScanner -> bleak.BleakScanner (habluetooth's own mocks only cover
    `.start()`/`.stop()`, not construction). `BleakScanner.__init__` picks its
    backend via `bleak.get_platform_scanner_backend_type()`, which in turn
    reads stdlib `platform.system()`. `pytest_homeassistant_custom_component`'s
    `mock_bluetooth_adapters` fixture patches that same shared stdlib
    `platform` module's `system()` to always report "Linux" -- needed so every
    BT-adapter mock behaves identically regardless of the host actually
    running the tests -- so on a non-Linux dev box `bleak` is fooled into
    picking the BlueZ D-Bus backend too. BlueZ D-Bus needs `dbus_fast`, absent
    on non-Linux dev boxes, so `BleakScanner.__init__` raises
    `ModuleNotFoundError`. That isn't a `RuntimeError`/`ScannerStartError`, so
    `bluetooth/__init__.py`'s own `except (RuntimeError, ScannerStartError)`
    around `scanner.async_start()` doesn't catch it -- it propagates out of
    `async_setup_entry`, `config_entries` logs it and marks the entry
    SETUP_ERROR, but `HaScanner.async_setup()` already scheduled its expiry
    timer a few lines earlier, and because the entry never reached LOADED,
    `enable_bluetooth`'s teardown never unregisters it, so the timer lingers
    and fails `verify_cleanup`.

    The fix here reads the *real* host platform via `sys.platform` (never
    mocked) instead of `platform.system()` (mocked), so bleak picks a backend
    this machine can actually construct: CoreBluetooth on macOS dev boxes (no
    dbus_fast needed), BlueZ D-Bus on Linux CI (dbus_fast *is* installed there
    -- see poetry.lock's `platform_system == "Linux"` marker on `dbus-fast` --
    so this resolves to the same backend the unmocked function would have
    picked anyway). HaScanner and BleakScanner stay entirely real; only which
    concrete backend class gets constructed changes.
    """
    if sys.platform == "darwin":
        from bleak.backends.corebluetooth.scanner import (  # noqa: PLC0415
            BleakScannerCoreBluetooth,
        )

        return (BleakScannerCoreBluetooth, BleakBackend.CORE_BLUETOOTH)
    if sys.platform.startswith("linux"):
        from bleak.backends.bluezdbus.scanner import (  # noqa: PLC0415
            BleakScannerBlueZDBus,
        )

        return (BleakScannerBlueZDBus, BleakBackend.BLUEZ_DBUS)
    raise RuntimeError(f"No test-harness bleak backend override for sys.platform={sys.platform!r}")

@pytest.fixture
def _mock_ha_scanner_backend():
    """Patch bleak's backend selection to use the real host platform. See `_real_scanner_backend_type`."""
    with patch("bleak.get_platform_scanner_backend_type", _real_scanner_backend_type):
        yield

# --- BEGIN upstream-bug compensation: remove once HA fixes the leak -------
#
# Home Assistant core 2026.8.2 never cancels the periodic device-expiry timer
# a bluetooth scanner schedules, so unloading a "bluetooth" config entry -- in
# production as well as in tests -- leaks it. Traced end to end:
#
#   * homeassistant/components/bluetooth/__init__.py:405 does a bare
#     `scanner.async_setup()` and discards the return value. Every *other*
#     cleanup callback in the same function is wired through
#     `entry.async_on_unload(...)` (lines 412, 421, 422); this one alone is not.
#   * habluetooth/base_scanner.py:295-299 -- `HaScanner.async_setup()` calls
#     `self._schedule_expire_devices()` and returns `self._unsetup`.
#   * habluetooth/base_scanner.py:359-361 -- `_unsetup()` is the only thing
#     that calls `_cancel_expire_devices()` (:711-715), which cancels the
#     timer.
#   * habluetooth/base_scanner.py:717-725 -- the leaked timer is
#     `loop.call_at(loop.time() + 30, self._async_expire_devices_schedule_next)`,
#     which reschedules itself every 30s forever (:727-730).
#
# Nothing in habluetooth or homeassistant.components.bluetooth ever calls
# `_unsetup`; grepping both packages for it turns up only the return sites,
# the definitions, and `super()._unsetup()` chaining. So the only canceller of
# a self-rescheduling timer is created, handed back, and dropped on the floor.
#
# The one-line upstream fix is `entry.async_on_unload(scanner.async_setup())`
# in place of the bare call at bluetooth/__init__.py:405. Until that lands,
# this fixture does what async_setup_entry should have done: it registers the
# unsetup callback HaScanner.async_setup() returns against whichever
# ConfigEntry is currently being set up (HA sets
# `homeassistant.config_entries.current_entry` for exactly this purpose while
# `async_setup_entry` runs).
#
# HaScanner is a compiled Cython extension type
# (`habluetooth.scanner_bleak.HaScanner`) and its methods can't be
# monkeypatched directly -- `patch.object(HaScanner, "async_setup", ...)`
# raises `TypeError: cannot set 'async_setup' attribute of immutable type`.
# It *can* be subclassed from plain Python, so instead this patches the
# ordinary (mutable) module-level binding
# `homeassistant.components.bluetooth.HaScanner` -- the name
# `bluetooth/__init__.py:404`'s `scanner = HaScanner(mode, adapter, address)`
# actually constructs from -- to a thin subclass that does the one extra
# thing async_setup_entry forgot to.
#
# REMOVAL CONDITION: delete this block, drop
# `_compensate_ha_scanner_expiry_timer_leak` from `mock_bluetooth`'s
# parameters below, and delete
# test_bluetooth_test_harness.py::test_bluetooth_harness_teardown_cancels_expiry_timer,
# once the installed `homeassistant` version passes `scanner.async_setup()`'s
# return value to `entry.async_on_unload` itself.
@pytest.fixture
def _compensate_ha_scanner_expiry_timer_leak():
    """Register HaScanner's dropped `_unsetup` callback for unload. See block comment above."""
    from homeassistant.components.bluetooth import HaScanner  # noqa: PLC0415
    from homeassistant.config_entries import current_entry  # noqa: PLC0415

    class _HaScannerWithUnloadRegistered(HaScanner):
        def async_setup(self):
            unsetup = super().async_setup()
            entry = current_entry.get()
            if entry is not None:
                entry.async_on_unload(unsetup)
            return unsetup

    with patch(
        "homeassistant.components.bluetooth.HaScanner", _HaScannerWithUnloadRegistered
    ):
        yield
# --- END upstream-bug compensation -----------------------------------------

@pytest.fixture(autouse=True)
def mock_bluetooth(
    _mock_bt_history,
    mock_bluetooth_adapters,
    _mock_ha_scanner_backend,
    _compensate_ha_scanner_expiry_timer_leak,
    enable_bluetooth,
):
    """Auto mock bluetooth."""

# This fixture enables loading custom integrations in all tests.
# Remove to enable selective use of this fixture
@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations):
    """Auto enable custom integrations."""
    yield


# This fixture is used to prevent HomeAssistant from attempting to create and dismiss persistent
# notifications. These calls would fail without this fixture since the persistent_notification
# integration is never loaded during a test.
@pytest.fixture(name="skip_notifications", autouse=True)
def skip_notifications_fixture():
    """Skip notification calls."""
    with patch("homeassistant.components.persistent_notification.async_create"), patch(
        "homeassistant.components.persistent_notification.async_dismiss"
    ):
        yield

@pytest.fixture()
def mock_config_entry(hass: HomeAssistant) -> MockConfigEntry:
    """Create a mock config entry and add it to hass."""
    entry = MockConfigEntry(title=None)
    entry.add_to_hass(hass)
    return entry

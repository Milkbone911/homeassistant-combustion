"""Every third-party import must be declared or provided by a dependency."""
import ast
import json
import pathlib
import sys

import pytest

COMPONENT = pathlib.Path(__file__).parent.parent / "custom_components" / "combustion"

# Packages Home Assistant core guarantees are installed for this integration:
#   - voluptuous: a core dependency, used by every config flow
#   - bleak / bleak_retry_connector / home_assistant_bluetooth: installed by the
#     `bluetooth` integration, which manifest.json depends on via bluetooth_adapters
PROVIDED_BY_HA = {
    "voluptuous",
    "bleak",
    "bleak_retry_connector",
    "home_assistant_bluetooth",
}
ALWAYS_OK = {"homeassistant", "custom_components"} | PROVIDED_BY_HA


@pytest.fixture(autouse=True)
def mock_bluetooth():
    """Override tests/conftest.py: this module inspects source, it needs no hass."""
    yield


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations():
    """Override tests/conftest.py: this module loads no integrations."""
    yield


def _top_level_imports() -> set[str]:
    found = set()
    for path in COMPONENT.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                found.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                found.add(node.module.split(".")[0])
    return found


def _declared() -> set[str]:
    manifest = json.loads((COMPONENT / "manifest.json").read_text())
    # "sensor-state-data>=2.20.0" -> "sensor_state_data"
    return {
        req.split("==")[0].split(">=")[0].split("<")[0].strip().replace("-", "_")
        for req in manifest["requirements"]
    }


def test_no_undeclared_third_party_imports():
    """Fail if the package imports a package nothing installs for it."""
    undeclared = (
        _top_level_imports() - sys.stdlib_module_names - ALWAYS_OK - _declared()
    )
    assert undeclared == set(), (
        f"Imported but not in manifest.json requirements: {sorted(undeclared)}. "
        "Add them to requirements, or drop the import."
    )


def test_sensor_description_keys_are_stable():
    """These keys are part of the integration's on-disk shape; do not drift them."""
    from custom_components.combustion.sensor import (
        RSSI_SENSOR_DESCRIPTION,
        TEMPERATURE_SENSOR_DESCRIPTION,
        VIRTUAL_TEMPERATURE_SENSOR_DESCRIPTION,
    )

    assert VIRTUAL_TEMPERATURE_SENSOR_DESCRIPTION.key == "temperature_°C"
    assert TEMPERATURE_SENSOR_DESCRIPTION.key == "temperature_°C"
    assert RSSI_SENSOR_DESCRIPTION.key == "signal_strength_dBm"

"""Custom integration to integrate Combustion devices with Home Assistant."""
from __future__ import annotations

import hashlib
import sqlite3
from datetime import timedelta
from pathlib import Path

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.loader import async_get_integration

from custom_components.combustion.bluetooth_listener import BluetoothListener
from custom_components.combustion.connection_manager import ConnectionManager
from custom_components.combustion.control_manager import ControlManager
from custom_components.combustion.prediction_manager import PredictionManager
from custom_components.combustion.probe_manager import ProbeManager

from .cloud.ha import async_check_linked_account, cloud_linked
from .const import (
    ARCHIVE_DIRECTORY,
    ARCHIVE_FILENAME,
    CONF_AVAILABILITY_TIMEOUT,
    CONF_CLOUD_SYNC_ENABLED,
    CONF_ENABLE_ACTIVE_CONNECTION,
    CONF_HISTORY_ENABLED,
    CONF_UPDATE_THROTTLE,
    DEFAULT_AVAILABILITY_TIMEOUT,
    DEFAULT_CLOUD_SYNC_ENABLED,
    DEFAULT_ENABLE_ACTIVE_CONNECTION,
    DEFAULT_HISTORY_ENABLED,
    DEFAULT_UPDATE_THROTTLE,
    DOMAIN,
    LOGGER,
)
from .reconciliation.sync import CloudSyncSupervisor, SyncStatus
from .runtime import CombustionRuntime
from .storage.database import (
    ArchiveBusyError,
    ArchiveDatabase,
    ArchiveError,
    ArchiveIdentityError,
    ArchiveRuntimeUnsupported,
    ArchiveStatus,
)
from .storage.repository import ArchiveRepository
from .storage.schema import ArchiveSchemaError
from .websocket_api import async_register_websocket_handlers

type CombustionConfigEntry = ConfigEntry[CombustionRuntime]

PLATFORMS: list[Platform] = [
    Platform.BINARY_SENSOR,
    Platform.SENSOR,
    Platform.NUMBER,
    Platform.SELECT,
    Platform.BUTTON,
]

FRONTEND_CARD_URL = "/combustion/combustion-card.js"
FRONTEND_REGISTERED_KEY = f"{DOMAIN}_frontend_registered"
WEBSOCKET_REGISTERED_KEY = f"{DOMAIN}_websocket_registered"


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Register domain-level APIs once, independent of entry load state."""
    if not hass.data.get(WEBSOCKET_REGISTERED_KEY):
        async_register_websocket_handlers(hass)
        hass.data[WEBSOCKET_REGISTERED_KEY] = True
    return True


async def _async_register_frontend_card(hass: HomeAssistant) -> None:
    """Serve and auto-load the bundled combustion-card Lovelace card."""
    if hass.data.get(FRONTEND_REGISTERED_KEY):
        return
    try:
        http = getattr(hass, "http", None)
        if http is None:
            return
        card_path = str(Path(__file__).parent / "www" / "combustion-card.js")
        try:
            from homeassistant.components.http import StaticPathConfig

            await http.async_register_static_paths(
                [StaticPathConfig(FRONTEND_CARD_URL, card_path, True)]
            )
        except ImportError:
            http.register_static_path(FRONTEND_CARD_URL, card_path, True)

        from homeassistant.components.frontend import add_extra_js_url

        integration = await async_get_integration(hass, DOMAIN)
        version = integration.version or "0"
        add_extra_js_url(hass, f"{FRONTEND_CARD_URL}?v={version}")
        hass.data[FRONTEND_REGISTERED_KEY] = True
    except Exception:  # noqa: BLE001
        LOGGER.debug(
            "Could not register the combustion card frontend resource",
            exc_info=True,
        )


def _package_fingerprint_sync() -> str:
    """Hash the installed integration package without trusting manifest version."""
    root = Path(__file__).parent
    digest = hashlib.sha256()
    for path in sorted(
        item
        for item in root.rglob("*")
        if item.is_file()
        and "__pycache__" not in item.parts
        and item.suffix in {".py", ".json", ".js"}
    ):
        relative = path.relative_to(root).as_posix().encode()
        data = path.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()


async def _async_start_archive(
    hass: HomeAssistant,
    entry: CombustionConfigEntry,
    runtime: CombustionRuntime,
    *,
    cloud_sync_enabled: bool,
) -> None:
    """Start optional archive/sync without allowing failure to gate local BLE."""
    fingerprint = await hass.async_add_executor_job(_package_fingerprint_sync)
    archive_path = Path(
        hass.config.path(ARCHIVE_DIRECTORY, ARCHIVE_FILENAME)
    )
    database = ArchiveDatabase(
        archive_path,
        binding_path=Path(
            hass.config.path(".storage", "combustion_archive_binding.json")
        ),
        health=runtime.archive_health,
        application_fingerprint=fingerprint,
        require_qualified_wal=True,
    )
    runtime.archive_database = database

    try:
        await database.async_start(allow_create=True)
        repository = ArchiveRepository(database)
        runtime.archive_repository = repository
        await repository.async_recover_interrupted_work()
    except ArchiveRuntimeUnsupported:
        runtime.archive_health.status = ArchiveStatus.UNSUPPORTED
        runtime.archive_health.error_category = "sqlite_runtime"
    except ArchiveIdentityError:
        runtime.archive_health.status = ArchiveStatus.RECOVERY_REQUIRED
        runtime.archive_health.error_category = "identity"
    except ArchiveBusyError:
        runtime.archive_health.status = ArchiveStatus.RECOVERY_REQUIRED
        runtime.archive_health.error_category = "writer_owned"
    except ArchiveSchemaError:
        runtime.archive_health.status = ArchiveStatus.RECOVERY_REQUIRED
        runtime.archive_health.error_category = "schema"
    except (ArchiveError, sqlite3.Error, OSError):
        runtime.archive_health.status = ArchiveStatus.DEGRADED
        runtime.archive_health.error_category = "storage"
    except Exception:  # noqa: BLE001
        runtime.archive_health.status = ArchiveStatus.DEGRADED
        runtime.archive_health.error_category = "internal"
        LOGGER.exception("Unexpected Combustion archive startup failure")

    if runtime.archive_health.status is not ArchiveStatus.READY:
        # Archive failure never removes basic cloud-link health from diagnostics.
        if cloud_linked(entry.data):
            await async_check_linked_account(hass, entry, runtime.cloud_health)
        return

    if not cloud_sync_enabled:
        return

    if not cloud_linked(entry.data):
        runtime.sync_health.status = SyncStatus.DEGRADED
        runtime.sync_health.last_error_category = "cloud_not_linked"
        return

    assert runtime.archive_repository is not None
    supervisor = CloudSyncSupervisor(
        hass,
        entry,
        runtime.archive_repository,
        runtime.cloud_health,
        runtime.sync_health,
    )
    runtime.sync_supervisor = supervisor
    runtime.create_optional_task(
        hass,
        entry,
        supervisor.async_run(),
        "combustion-cloud-history-sync",
    )


async def async_setup_entry(
    hass: HomeAssistant, entry: CombustionConfigEntry
) -> bool:
    """Set up one Combustion entry; optional failures never gate local BLE."""
    await _async_register_frontend_card(hass)

    availability_timeout = entry.options.get(
        CONF_AVAILABILITY_TIMEOUT, DEFAULT_AVAILABILITY_TIMEOUT
    )
    update_throttle = entry.options.get(
        CONF_UPDATE_THROTTLE, DEFAULT_UPDATE_THROTTLE
    )
    enable_active_connection = entry.options.get(
        CONF_ENABLE_ACTIVE_CONNECTION, DEFAULT_ENABLE_ACTIVE_CONNECTION
    )
    history_enabled = bool(
        entry.options.get(CONF_HISTORY_ENABLED, DEFAULT_HISTORY_ENABLED)
    )
    cloud_sync_enabled = bool(
        entry.options.get(CONF_CLOUD_SYNC_ENABLED, DEFAULT_CLOUD_SYNC_ENABLED)
    )

    listener = BluetoothListener(hass, entry)
    probe_manager = ProbeManager(
        listener,
        availability_timeout_seconds=float(availability_timeout),
        min_notify_interval_seconds=float(update_throttle),
    )
    connection_manager = ConnectionManager(
        hass, entry, probe_manager, bool(enable_active_connection)
    )
    prediction_manager = PredictionManager(
        hass, entry, connection_manager, probe_manager
    )
    control_manager = ControlManager(connection_manager)

    runtime = CombustionRuntime(
        bluetooth_listener=listener,
        probe_manager=probe_manager,
        connection_manager=connection_manager,
        prediction_manager=prediction_manager,
        control_manager=control_manager,
    )
    entry.runtime_data = runtime

    hass.data[DOMAIN] = probe_manager
    hass.data[f"{DOMAIN}_prediction"] = prediction_manager
    hass.data[f"{DOMAIN}_connection"] = connection_manager

    probe_manager.connection_manager = connection_manager
    probe_manager.control_manager = control_manager
    probe_manager.active_enabled = bool(enable_active_connection)

    entry.async_on_unload(runtime.async_stop)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    probe_manager.async_init()
    listener.async_init()
    prediction_manager.async_init()
    connection_manager.async_init()

    if history_enabled:
        runtime.create_optional_task(
            hass,
            entry,
            _async_start_archive(
                hass,
                entry,
                runtime,
                cloud_sync_enabled=cloud_sync_enabled,
            ),
            "combustion-archive-start",
        )
    else:
        runtime.archive_health.status = ArchiveStatus.DISABLED
        if cloud_sync_enabled:
            runtime.sync_health.status = SyncStatus.DEGRADED
            runtime.sync_health.last_error_category = "history_disabled"

    # A history-sync runtime owns the linked cloud client when active. All
    # other linked configurations retain the S2 one-shot health check.
    if cloud_linked(entry.data) and not (history_enabled and cloud_sync_enabled):
        runtime.create_optional_task(
            hass,
            entry,
            async_check_linked_account(hass, entry, runtime.cloud_health),
            "combustion-cloud-link-check",
        )

    @callback
    def _async_availability_tick(_now) -> None:
        probe_manager.notify_listeners()

    availability_check_interval = timedelta(
        seconds=max(5.0, float(availability_timeout) / 3)
    )
    entry.async_on_unload(
        async_track_time_interval(
            hass,
            _async_availability_tick,
            availability_check_interval,
        )
    )

    return True


async def async_unload_entry(
    hass: HomeAssistant, entry: CombustionConfigEntry
) -> bool:
    """Unload one config entry and its existing platform projections."""
    if not await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        return False

    runtime = entry.runtime_data
    if hass.data.get(DOMAIN) is runtime.probe_manager:
        hass.data[DOMAIN] = {}
    if hass.data.get(f"{DOMAIN}_prediction") is runtime.prediction_manager:
        hass.data.pop(f"{DOMAIN}_prediction", None)
    if hass.data.get(f"{DOMAIN}_connection") is runtime.connection_manager:
        hass.data.pop(f"{DOMAIN}_connection", None)
    return True

"""Home Assistant adapter for the optional unofficial cloud account link.

This module owns HA config-entry persistence and health only. The S1 client
remains transport/auth protocol code and knows nothing about Home Assistant.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from ..const import (
    CONF_CLOUD_API_KEY,
    CONF_CLOUD_LINK_GENERATION,
    CONF_CLOUD_REFRESH_TOKEN,
    CONF_CLOUD_SUBJECT,
)
from .client import CombustionCloudClient
from .models import (
    CloudAuthError,
    CloudBoundsError,
    CloudConflictError,
    CloudPermissionError,
    CloudSchemaError,
    CloudTransportError,
)


class CloudLinkAccountMismatch(CloudAuthError):
    """Validated credentials belong to a different Firebase subject."""


class CloudLinkChangedError(CloudAuthError):
    """An awaited request belongs to a superseded account-link generation."""


class CloudLinkStatus(StrEnum):
    """Bounded optional-cloud health states exposed to later diagnostics."""

    UNLINKED = "unlinked"
    CHECKING = "checking"
    READY = "ready"
    REAUTH_REQUIRED = "reauth_required"
    DEGRADED = "degraded"
    COMPATIBILITY_ERROR = "compatibility_error"


@dataclass(slots=True)
class CloudLinkHealth:
    """Sanitized runtime health; never store tokens or account identifiers."""

    status: CloudLinkStatus = CloudLinkStatus.UNLINKED
    probe_count: int | None = None
    error_category: str | None = None
    verified_generation: int | None = None


@dataclass(frozen=True, slots=True)
class CloudLinkValidation:
    """Validated link material safe for config-entry persistence."""

    subject: str
    refresh_token: str
    probe_count: int


def cloud_linked(data: dict[str, Any] | Any) -> bool:
    """Return whether config-entry data contains one complete link tuple."""
    try:
        return bool(
            data.get(CONF_CLOUD_API_KEY)
            and data.get(CONF_CLOUD_REFRESH_TOKEN)
            and data.get(CONF_CLOUD_SUBJECT)
            and int(data.get(CONF_CLOUD_LINK_GENERATION, 0)) >= 1
        )
    except (TypeError, ValueError):
        return False


def linked_account_snapshot(entry: ConfigEntry) -> tuple[int, str]:
    """Return one validated link-generation/subject snapshot."""
    if not cloud_linked(entry.data):
        raise CloudAuthError("Cloud account is not linked")
    return (
        int(entry.data[CONF_CLOUD_LINK_GENERATION]),
        str(entry.data[CONF_CLOUD_SUBJECT]),
    )


def linked_account_matches(
    entry: ConfigEntry, generation: int, subject: str
) -> bool:
    """Return whether awaited work still belongs to the active link."""
    current = entry.data
    try:
        return (
            int(current.get(CONF_CLOUD_LINK_GENERATION, 0)) == generation
            and current.get(CONF_CLOUD_SUBJECT) == subject
        )
    except (TypeError, ValueError):
        return False


def create_linked_cloud_client(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> tuple[CombustionCloudClient, int, str]:
    """Build the single client for one active linked-account runtime.

    The returned client owns auth single-flight for that supervisor. Routine
    refresh-token rotation persists only while the original link generation
    and subject still match.
    """
    generation, expected_subject = linked_account_snapshot(entry)
    data = entry.data

    async def _persist_rotation(_old: str, new: str) -> None:
        if not linked_account_matches(entry, generation, expected_subject):
            raise CloudLinkChangedError("Cloud link changed during token refresh")
        current = entry.data
        if current.get(CONF_CLOUD_REFRESH_TOKEN) == new:
            return
        hass.config_entries.async_update_entry(
            entry,
            data={**current, CONF_CLOUD_REFRESH_TOKEN: new},
        )

    client = CombustionCloudClient(
        session=async_get_clientsession(hass),
        api_key=str(data[CONF_CLOUD_API_KEY]),
        refresh_token=str(data[CONF_CLOUD_REFRESH_TOKEN]),
        expected_subject=expected_subject,
        on_rotation=_persist_rotation,
    )
    return client, generation, expected_subject


async def async_validate_cloud_link(
    hass: HomeAssistant,
    *,
    api_key: str,
    refresh_token: str,
    expected_subject: str | None = None,
) -> CloudLinkValidation:
    """Refresh credentials and prove the account's Firestore association path.

    This is not a vendor-supported OAuth bootstrap. The operator supplies a
    previously obtained Firebase app key and refresh token; the server-returned
    subject becomes authoritative.
    """
    persisted_refresh = refresh_token

    async def _capture_rotation(_old: str, new: str) -> None:
        nonlocal persisted_refresh
        persisted_refresh = new

    client = CombustionCloudClient(
        session=async_get_clientsession(hass),
        api_key=api_key,
        refresh_token=refresh_token,
        expected_subject=None,
        on_rotation=_capture_rotation,
    )
    probes = await client.probes()
    subject = client.subject
    if subject is None:
        raise CloudAuthError("Cloud account did not return an authenticated subject")
    if expected_subject is not None and subject != expected_subject:
        raise CloudLinkAccountMismatch(
            "Authenticated subject does not match linked account"
        )
    return CloudLinkValidation(
        subject=subject,
        refresh_token=persisted_refresh,
        probe_count=len(probes),
    )


async def async_check_linked_account(
    hass: HomeAssistant,
    entry: ConfigEntry,
    health: CloudLinkHealth,
) -> None:
    """Perform one optional startup account check without gating local BLE."""
    data = entry.data
    if not cloud_linked(data):
        health.status = CloudLinkStatus.UNLINKED
        health.probe_count = None
        health.error_category = None
        health.verified_generation = None
        return

    generation = int(data[CONF_CLOUD_LINK_GENERATION])
    expected_subject = str(data[CONF_CLOUD_SUBJECT])
    health.status = CloudLinkStatus.CHECKING
    health.probe_count = None
    health.error_category = None
    health.verified_generation = None

    try:
        client, generation, expected_subject = create_linked_cloud_client(
            hass, entry
        )
        probes = await client.probes()
    except CloudLinkChangedError:
        # Replacement/unlink won the race. The old generation must not start
        # reauth or publish health into the new account's runtime.
        return
    except CloudAuthError:
        health.status = CloudLinkStatus.REAUTH_REQUIRED
        health.error_category = "auth"
        entry.async_start_reauth(hass)
        return
    except CloudPermissionError:
        health.status = CloudLinkStatus.DEGRADED
        health.error_category = "permission"
        return
    except CloudTransportError:
        health.status = CloudLinkStatus.DEGRADED
        health.error_category = "transport"
        return
    except (CloudSchemaError, CloudBoundsError, CloudConflictError):
        health.status = CloudLinkStatus.COMPATIBILITY_ERROR
        health.error_category = "schema"
        return

    # Re-check generation after awaited I/O. A replaced/unlinked account must
    # never publish health for the stale account generation.
    current = entry.data
    if not linked_account_matches(entry, generation, expected_subject):
        return

    health.status = CloudLinkStatus.READY
    health.probe_count = len(probes)
    health.error_category = None
    health.verified_generation = generation

"""Resumable, source-qualified cloud reconciliation for S3."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from ..cloud.client import CombustionCloudClient
from ..cloud.ha import (
    CloudLinkChangedError,
    CloudLinkHealth,
    CloudLinkStatus,
    create_linked_cloud_client,
    linked_account_matches,
)
from ..cloud.models import (
    CloudAuthError,
    CloudBoundsError,
    CloudConflictError,
    CloudPermissionError,
    CloudSchemaError,
    CloudTransportError,
)
from ..cloud.sessions import numeric_session_token
from ..storage.repository import ArchiveRepository, SyncWork
from ..storage.schema import utc_now_us

DISCOVERY_INTERVAL_US = 6 * 60 * 60 * 1_000_000
CONTEXT_REFRESH_US = 15 * 60 * 1_000_000
TRANSIENT_RETRY_US = 5 * 60 * 1_000_000
PERMISSION_RETRY_US = 60 * 60 * 1_000_000
SCHEMA_RETRY_US = 6 * 60 * 60 * 1_000_000
MAX_WORK_PER_CYCLE = 8


class SyncStatus(StrEnum):
    """Sanitized history-sync health states."""

    DISABLED = "disabled"
    STARTING = "starting"
    IDLE = "idle"
    SYNCING = "syncing"
    DEGRADED = "degraded"
    REAUTH_REQUIRED = "reauth_required"
    COMPATIBILITY_ERROR = "compatibility_error"
    STOPPED = "stopped"


@dataclass(slots=True)
class SyncHealth:
    """Bounded sync status without source/account identifiers."""

    status: SyncStatus = SyncStatus.DISABLED
    last_success_us: int | None = None
    last_discovery_us: int | None = None
    last_error_category: str | None = None
    ready_work: int = 0
    running_work: int = 0
    failed_work: int = 0


class CloudSyncSupervisor:
    """Own one linked-account client and process persisted work sequentially."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        repository: ArchiveRepository,
        cloud_health: CloudLinkHealth,
        sync_health: SyncHealth,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.repository = repository
        self.cloud_health = cloud_health
        self.health = sync_health
        self._client: CombustionCloudClient | None = None
        self._generation: int | None = None
        self._subject: str | None = None
        self._account_id: str | None = None
        self._next_context_refresh_us = 0
        self._cycle_lock = asyncio.Lock()

    def _link_matches(self) -> bool:
        return (
            self._generation is not None
            and self._subject is not None
            and linked_account_matches(
                self.entry, self._generation, self._subject
            )
        )

    def _require_link(self) -> None:
        if not self._link_matches():
            raise CloudLinkChangedError("Cloud link changed during history sync")

    async def _refresh_queue_health(self) -> None:
        counts = await self.repository.async_queue_counts()
        self.health.ready_work = int(counts["ready"])
        self.health.running_work = int(counts["running"])
        self.health.failed_work = int(counts["failed"])

    async def _refresh_context(self) -> None:
        """Authenticate once, retain one client and discover current probes."""
        client, generation, subject = create_linked_cloud_client(
            self.hass, self.entry
        )
        probes = await client.probes()
        if not linked_account_matches(self.entry, generation, subject):
            raise CloudLinkChangedError(
                "Cloud link changed during association discovery"
            )

        account_id, sources = await self.repository.async_register_account(
            subject, generation, probes
        )
        self._client = client
        self._generation = generation
        self._subject = subject
        self._account_id = account_id

        self.cloud_health.status = CloudLinkStatus.READY
        self.cloud_health.probe_count = len(probes)
        self.cloud_health.error_category = None
        self.cloud_health.verified_generation = generation

        now = utc_now_us()
        for probe in probes:
            source = sources[probe.serial]
            if not await self.repository.async_discovery_due(
                account_id,
                source.source_device_id,
                now_us=now,
                interval_us=DISCOVERY_INTERVAL_US,
            ):
                continue

            run_id = await self.repository.async_begin_discovery(
                account_id=account_id,
                generation=generation,
                source=source,
            )

            async def persist_page(
                page,
                digest,
                *,
                _run=run_id,
                _source=source,
            ):
                self._require_link()
                await self.repository.async_record_discovery_page(
                    run_id=_run,
                    source=_source,
                    page=page,
                    digest=digest,
                )

            try:
                traversal = await client.sessions(
                    probe,
                    page_observer=persist_page,
                )
                self._require_link()
                await self.repository.async_finish_discovery(
                    run_id,
                    complete=True,
                    terminal_reason=traversal.terminal_reason,
                    snapshot_consistent=traversal.snapshot_consistent,
                )
            except BaseException:
                await self.repository.async_finish_discovery(
                    run_id,
                    complete=False,
                    terminal_reason="interrupted",
                )
                raise

        self.health.last_discovery_us = now
        self._next_context_refresh_us = now + CONTEXT_REFRESH_US

    async def _process_work(self, work: SyncWork) -> None:
        if self._client is None:
            raise CloudTransportError("Cloud sync client is unavailable")
        self._require_link()
        session_id = numeric_session_token(work.source_session_token)

        try:
            if work.kind == "manifest":
                meta = await self._client.session_meta(work.serial, session_id)
                self._require_link()
                await self.repository.async_complete_manifest(work, meta)
            else:
                if work.start_seq is None or work.end_seq is None:
                    raise CloudSchemaError("Persisted sample work is unbounded")
                rows = await self._client.sample_chunk(
                    work.serial,
                    session_id,
                    work.start_seq,
                    work.end_seq,
                )
                self._require_link()
                await self.repository.async_commit_sample_work(work, rows)
        except CloudLinkChangedError:
            raise
        except CloudAuthError:
            await self.repository.async_fail_work(
                work.work_id,
                category="auth",
                retry_delay_us=PERMISSION_RETRY_US,
            )
            raise
        except CloudPermissionError:
            await self.repository.async_fail_work(
                work.work_id,
                category="permission",
                retry_delay_us=PERMISSION_RETRY_US,
            )
            raise
        except CloudTransportError:
            await self.repository.async_fail_work(
                work.work_id,
                category="transport",
                retry_delay_us=TRANSIENT_RETRY_US,
            )
            raise
        except (CloudSchemaError, CloudBoundsError, CloudConflictError):
            if work.kind == "manifest":
                await self.repository.async_record_manifest_failure(
                    work,
                    category="schema",
                    retry_delay_us=SCHEMA_RETRY_US,
                )
            else:
                await self.repository.async_fail_work(
                    work.work_id,
                    category="schema",
                    retry_delay_us=SCHEMA_RETRY_US,
                )
            raise

    def _publish_failure(self, err: BaseException) -> None:
        if isinstance(err, CloudLinkChangedError):
            self.health.status = SyncStatus.STOPPED
            self.health.last_error_category = "link_changed"
            return
        if isinstance(err, CloudAuthError):
            self.health.status = SyncStatus.REAUTH_REQUIRED
            self.health.last_error_category = "auth"
            self.cloud_health.status = CloudLinkStatus.REAUTH_REQUIRED
            self.cloud_health.error_category = "auth"
            self.entry.async_start_reauth(self.hass)
            return
        if isinstance(err, CloudPermissionError):
            self.health.status = SyncStatus.DEGRADED
            self.health.last_error_category = "permission"
            self.cloud_health.status = CloudLinkStatus.DEGRADED
            self.cloud_health.error_category = "permission"
            return
        if isinstance(err, CloudTransportError):
            self.health.status = SyncStatus.DEGRADED
            self.health.last_error_category = "transport"
            self.cloud_health.status = CloudLinkStatus.DEGRADED
            self.cloud_health.error_category = "transport"
            return
        if isinstance(err, (CloudSchemaError, CloudBoundsError, CloudConflictError)):
            self.health.status = SyncStatus.COMPATIBILITY_ERROR
            self.health.last_error_category = "schema"
            self.cloud_health.status = CloudLinkStatus.COMPATIBILITY_ERROR
            self.cloud_health.error_category = "schema"
            return
        self.health.status = SyncStatus.DEGRADED
        self.health.last_error_category = "internal"

    async def async_sync_cycle(
        self,
        *,
        force_context_refresh: bool = False,
        max_work: int = MAX_WORK_PER_CYCLE,
    ) -> int:
        """Perform one serialized bounded cycle; progress is DB-backed."""
        if not 1 <= max_work <= MAX_WORK_PER_CYCLE:
            raise ValueError("Invalid sync cycle budget")

        async with self._cycle_lock:
            self.health.status = SyncStatus.SYNCING
            now = utc_now_us()

            if (
                force_context_refresh
                or self._client is None
                or now >= self._next_context_refresh_us
            ):
                await self._refresh_context()

            if self._account_id is None:
                raise CloudTransportError(
                    "Archive account context is unavailable"
                )

            processed = 0
            for _ in range(max_work):
                work = await self.repository.async_claim_work(
                    account_id=self._account_id,
                    now_us=utc_now_us(),
                )
                if work is None:
                    break
                await self._process_work(work)
                processed += 1

            await self._refresh_queue_health()
            self.health.status = SyncStatus.IDLE
            self.health.last_error_category = None
            self.health.last_success_us = utc_now_us()
            return processed

    async def async_run(self) -> None:
        """Long-running entry-owned supervisor with bounded work opportunities."""
        self.health.status = SyncStatus.STARTING
        await self.repository.async_recover_interrupted_work()

        try:
            while True:
                try:
                    processed = await self.async_sync_cycle(
                        force_context_refresh=self._client is None
                    )
                except asyncio.CancelledError:
                    raise
                except CloudLinkChangedError as err:
                    self._publish_failure(err)
                    return
                except (
                    CloudAuthError,
                    CloudPermissionError,
                    CloudTransportError,
                    CloudSchemaError,
                    CloudBoundsError,
                    CloudConflictError,
                ) as err:
                    self._publish_failure(err)
                    await self._refresh_queue_health()
                    delay = (
                        300
                        if isinstance(err, CloudTransportError)
                        else 1800
                    )
                    await asyncio.sleep(delay)
                    continue
                except Exception as err:  # noqa: BLE001
                    self._publish_failure(err)
                    await self._refresh_queue_health()
                    await asyncio.sleep(300)
                    continue

                await asyncio.sleep(1 if processed else 60)
        finally:
            if self.health.status not in (
                SyncStatus.REAUTH_REQUIRED,
                SyncStatus.COMPATIBILITY_ERROR,
            ):
                self.health.status = SyncStatus.STOPPED

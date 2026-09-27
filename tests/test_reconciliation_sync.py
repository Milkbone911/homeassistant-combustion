"""S3 cloud reconciliation supervisor tests over a real temporary archive."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from custom_components.combustion.cloud.client import IndexTraversal
from custom_components.combustion.cloud.ha import CloudLinkChangedError, CloudLinkHealth
from custom_components.combustion.cloud.models import (
    CloudTransportError,
    IndexPage,
    Probe,
    SampleRow,
    SessionIndex,
    SessionMeta,
)
from custom_components.combustion.reconciliation import sync as sync_module
from custom_components.combustion.reconciliation.sync import (
    CloudSyncSupervisor,
    SyncHealth,
)
from custom_components.combustion.storage.database import ArchiveDatabase
from custom_components.combustion.storage.repository import ArchiveRepository


def _session() -> SessionIndex:
    return SessionIndex(
        source_session_token="123",
        index_id="index-a",
        serial="probe-a",
        uid=None,
        device_type=1,
        sample_period_ms=5000,
        started_at="2026-01-01T00:00:00Z",
        ended_at=None,
        advertised_ranges=((0, 1),),
        raw={
            "device_session_id": 123,
            "sample_period": 5000,
            "sequence_number_ranges": [[0, 1]],
        },
    )


def _page() -> IndexPage:
    return IndexPage(1, 1, 1, (_session(),))


def _row(sequence: int) -> SampleRow:
    raw = {
        "sequence_number": sequence,
        "sampled_at": f"2026-01-01T00:00:0{sequence}Z",
        "t1": 20.0 + sequence,
    }
    return SampleRow(
        sequence,
        raw["sampled_at"],
        {"t1": raw["t1"]},
        (),
        raw,
    )


class _FakeCloudClient:
    def __init__(self, *, fail_after_page: bool = False, mutate_entry=None):
        self.fail_after_page = fail_after_page
        self.mutate_entry = mutate_entry
        self.status_calls = 0
        self.sessions_calls = 0
        self.meta_calls = 0
        self.sample_calls = 0

    async def probes(self):
        return (Probe("probe-a", "provider-locator"),)

    async def status(self, _probe):
        self.status_calls += 1
        raise AssertionError("historical reconciliation must not require current status")

    async def sessions(self, _probe, *, page_observer=None, **_kwargs):
        self.sessions_calls += 1
        page = _page()
        if page_observer is not None:
            await page_observer(page, "digest-1")
        if self.fail_after_page:
            raise CloudTransportError("synthetic page-2 transport failure")
        return IndexTraversal(
            sessions=page.sessions,
            pages=(page,),
            page_digests=("digest-1",),
            terminal_reason="declared_total_pages",
        )

    async def session_meta(self, _serial, _session_id):
        self.meta_calls += 1
        if self.mutate_entry is not None:
            self.mutate_entry()
        return SessionMeta(
            "2026-01-01T00:00:00Z",
            ((0, 1),),
            {
                "started_at": "2026-01-01T00:00:00Z",
                "sequence_number_ranges": [[0, 1]],
            },
        )

    async def sample_chunk(self, _serial, _session_id, start, end):
        self.sample_calls += 1
        return tuple(_row(value) for value in range(start, end + 1))


async def _archive(tmp_path: Path):
    database = ArchiveDatabase(
        tmp_path / "archive.sqlite3",
        require_qualified_wal=False,
    )
    await database.async_start(allow_create=True)
    return database, ArchiveRepository(database)


def _entry():
    return SimpleNamespace(
        data={
            "cloud_link_generation": 1,
            "cloud_subject": "subject-a",
        },
        async_start_reauth=MagicMock(),
    )


@pytest.mark.asyncio
async def test_history_sync_does_not_require_current_probe_status(
    tmp_path: Path, monkeypatch
):
    """Sleeping/offline current status is irrelevant to historical import."""
    database, repository = await _archive(tmp_path)
    client = _FakeCloudClient()
    entry = _entry()
    monkeypatch.setattr(
        sync_module,
        "create_linked_cloud_client",
        lambda _hass, _entry: (client, 1, "subject-a"),
    )

    supervisor = CloudSyncSupervisor(
        MagicMock(),
        entry,
        repository,
        CloudLinkHealth(),
        SyncHealth(),
    )
    processed = await supervisor.async_sync_cycle(
        force_context_refresh=True,
        max_work=8,
    )

    counts = await repository.async_archive_counts()
    assert processed == 2
    assert client.sessions_calls == 1
    assert client.meta_calls == 1
    assert client.sample_calls == 1
    assert client.status_calls == 0
    assert counts["sessions"] == 1
    assert counts["samples"] == 2
    assert counts["open_gaps"] == 0
    await database.async_stop()


@pytest.mark.asyncio
async def test_partial_index_failure_retains_validated_page_evidence(
    tmp_path: Path, monkeypatch
):
    """Validated page 1 survives when a later traversal step fails."""
    database, repository = await _archive(tmp_path)
    client = _FakeCloudClient(fail_after_page=True)
    entry = _entry()
    monkeypatch.setattr(
        sync_module,
        "create_linked_cloud_client",
        lambda _hass, _entry: (client, 1, "subject-a"),
    )
    supervisor = CloudSyncSupervisor(
        MagicMock(),
        entry,
        repository,
        CloudLinkHealth(),
        SyncHealth(),
    )

    with pytest.raises(CloudTransportError):
        await supervisor.async_sync_cycle(
            force_context_refresh=True,
            max_work=1,
        )

    evidence = await database.async_read(
        lambda conn: (
            conn.execute(
                "SELECT terminal_status FROM discovery_runs"
            ).fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM discovery_pages").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM cloud_sessions").fetchone()[0],
        )
    )
    assert evidence == ("partial", 1, 1)
    await database.async_stop()


@pytest.mark.asyncio
async def test_link_generation_change_after_network_blocks_archive_commit(
    tmp_path: Path, monkeypatch
):
    """A stale account generation cannot commit data after awaited cloud I/O."""
    database, repository = await _archive(tmp_path)
    entry = _entry()

    def replace_account():
        entry.data = {
            "cloud_link_generation": 2,
            "cloud_subject": "subject-b",
        }

    client = _FakeCloudClient(mutate_entry=replace_account)
    monkeypatch.setattr(
        sync_module,
        "create_linked_cloud_client",
        lambda _hass, _entry: (client, 1, "subject-a"),
    )
    supervisor = CloudSyncSupervisor(
        MagicMock(),
        entry,
        repository,
        CloudLinkHealth(),
        SyncHealth(),
    )

    with pytest.raises(CloudLinkChangedError):
        await supervisor.async_sync_cycle(
            force_context_refresh=True,
            max_work=1,
        )

    state = await database.async_read(
        lambda conn: (
            conn.execute("SELECT COUNT(*) FROM session_manifests").fetchone()[0],
            conn.execute(
                "SELECT state FROM sync_work WHERE kind='manifest'"
            ).fetchone()[0],
        )
    )
    assert state == (0, "running")

    assert await repository.async_recover_interrupted_work() == 1
    recovered = await database.async_read(
        lambda conn: conn.execute(
            "SELECT state FROM sync_work WHERE kind='manifest'"
        ).fetchone()[0]
    )
    assert recovered == "ready"
    await database.async_stop()

"""S3 archive database ownership, cancellation and backup tests."""
from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path

import pytest

from custom_components.combustion.storage.database import (
    ArchiveBusyError,
    ArchiveDatabase,
    ArchiveError,
    ArchiveIdentityError,
    sqlite_wal_fix_qualified,
)


@pytest.mark.asyncio
async def test_archive_create_reopen_and_identity_binding(tmp_path: Path):
    """An explicitly created archive reopens only with its matching binding."""
    path = tmp_path / "combustion" / "archive.sqlite3"
    db = ArchiveDatabase(path, require_qualified_wal=False)
    metadata = await db.async_start(allow_create=True)

    assert metadata.schema_version == 1
    assert db.binding_path.is_file()
    assert path.is_file()
    assert db.health.journal_mode == "wal"

    await db.async_stop()

    reopened = ArchiveDatabase(path, require_qualified_wal=False)
    again = await reopened.async_start(allow_create=False)
    assert again.archive_id == metadata.archive_id
    assert again.path_binding_hash == metadata.path_binding_hash
    await reopened.async_stop()


@pytest.mark.asyncio
async def test_binding_without_database_refuses_empty_replacement(tmp_path: Path):
    """A missing expected DB must not silently become a fresh empty archive."""
    path = tmp_path / "combustion" / "archive.sqlite3"
    db = ArchiveDatabase(path, require_qualified_wal=False)
    await db.async_start(allow_create=True)
    await db.async_stop()

    path.unlink()

    replacement = ArchiveDatabase(path, require_qualified_wal=False)
    with pytest.raises(ArchiveIdentityError):
        await replacement.async_start(allow_create=True)


@pytest.mark.asyncio
async def test_database_without_binding_requires_recovery(tmp_path: Path):
    """An unbound DB is quarantined rather than adopted automatically."""
    path = tmp_path / "combustion" / "archive.sqlite3"
    db = ArchiveDatabase(path, require_qualified_wal=False)
    await db.async_start(allow_create=True)
    await db.async_stop()

    db.binding_path.unlink()

    replacement = ArchiveDatabase(path, require_qualified_wal=False)
    with pytest.raises(ArchiveIdentityError):
        await replacement.async_start(allow_create=True)


@pytest.mark.asyncio
async def test_process_guard_refuses_second_writer(tmp_path: Path):
    """Only one archive writer can own a path in one HA process."""
    path = tmp_path / "archive.sqlite3"
    first = ArchiveDatabase(path, require_qualified_wal=False)
    await first.async_start(allow_create=True)

    second = ArchiveDatabase(path, require_qualified_wal=False)
    with pytest.raises(ArchiveBusyError):
        await second.async_start(allow_create=False)

    await first.async_stop()


@pytest.mark.asyncio
async def test_writer_and_reader_connections_are_off_event_loop(tmp_path: Path):
    """Writer and bounded reader calls execute on their owned worker threads."""
    path = tmp_path / "archive.sqlite3"
    db = ArchiveDatabase(path, require_qualified_wal=False)
    await db.async_start(allow_create=True)

    writer_thread = await db.async_write(
        lambda conn: threading.current_thread().name
    )
    reader_thread = await db.async_read(
        lambda conn: threading.current_thread().name
    )

    assert writer_thread == "combustion-archive-writer"
    assert reader_thread.startswith("combustion-archive-reader")
    assert writer_thread != threading.current_thread().name
    assert reader_thread != threading.current_thread().name

    await db.async_stop()


@pytest.mark.asyncio
async def test_cancelled_await_does_not_claim_disk_work_stopped(tmp_path: Path):
    """Cancelling the coroutine leaves the thread-owned operation to completion."""
    path = tmp_path / "archive.sqlite3"
    db = ArchiveDatabase(path, require_qualified_wal=False)
    await db.async_start(allow_create=True)

    started = threading.Event()
    release = threading.Event()

    def slow_write(conn: sqlite3.Connection) -> None:
        started.set()
        assert release.wait(5)
        conn.execute("CREATE TABLE cancellation_proof(value INTEGER)")
        conn.execute("INSERT INTO cancellation_proof VALUES(1)")

    task = asyncio.create_task(db.async_write(slow_write))
    assert await asyncio.to_thread(started.wait, 2)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    release.set()

    for _ in range(50):
        try:
            value = await db.async_read(
                lambda conn: conn.execute(
                    "SELECT value FROM cancellation_proof"
                ).fetchone()[0]
            )
        except sqlite3.OperationalError:
            await asyncio.sleep(0.01)
            continue
        assert value == 1
        break
    else:
        pytest.fail("thread-owned write did not complete after await cancellation")

    await db.async_stop()


@pytest.mark.asyncio
async def test_consistent_backup_reopens_and_validates_semantics(tmp_path: Path):
    """Online backup produces a closed copy with hash/schema/count validation."""
    path = tmp_path / "combustion" / "archive.sqlite3"
    db = ArchiveDatabase(
        path,
        require_qualified_wal=False,
        application_fingerprint="test-fingerprint",
    )
    metadata = await db.async_start(allow_create=True)

    destination = path.parent / "backups" / "archive-test.sqlite3"
    result = await db.async_backup(destination)

    assert destination.is_file()
    manifest = Path(result["manifest"])
    assert manifest.is_file()

    validated = ArchiveDatabase.validate_backup_sync(destination, manifest)
    assert validated["archive_id"] == metadata.archive_id
    assert validated["schema_version"] == 1

    await db.async_stop()


def test_backup_hash_tamper_is_rejected(tmp_path: Path):
    """A snapshot manifest cannot bless changed bytes."""
    database = tmp_path / "backup.sqlite3"
    database.write_bytes(b"not-a-real-sqlite-db")
    manifest = tmp_path / "backup.sqlite3.manifest.json"
    manifest.write_text(
        '{"archive_id":"x","sha256":"deadbeef","counts":{}}'
    )

    with pytest.raises(ArchiveError, match="hash"):
        ArchiveDatabase.validate_backup_sync(database, manifest)


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("3.51.3", True),
        ("3.52.0", True),
        ("3.51.2", False),
        ("3.50.7", True),
        ("3.50.8", True),
        ("3.50.6", False),
        ("3.44.6", True),
        ("3.44.9", True),
        ("3.44.5", False),
        ("3.45.0", False),
        ("garbage", False),
    ],
)
def test_sqlite_wal_fix_qualification_is_conservative(
    version: str, expected: bool
):
    """Unknown release lines are not silently declared WAL-safe."""
    assert sqlite_wal_fix_qualified(version) is expected

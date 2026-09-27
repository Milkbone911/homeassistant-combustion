"""S3 archive database ownership, cancellation and backup tests."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import threading
from pathlib import Path

import pytest

from custom_components.combustion.source_link import (
    SourceLinkRepository,
    canonical_probe_serial,
)
from custom_components.combustion.storage.database import (
    ArchiveBusyError,
    ArchiveDatabase,
    ArchiveError,
    ArchiveIdentityError,
    ArchiveShutdownIncomplete,
    ArchiveStatus,
    sqlite_wal_fix_qualified,
)
from custom_components.combustion.storage.schema import (
    SCHEMA_V2_CHECKSUM,
    SCHEMA_V2_SQL,
    install_schema_v1,
)


@pytest.mark.asyncio
async def test_archive_create_reopen_and_identity_binding(tmp_path: Path):
    """An explicitly created archive reopens only with its matching binding."""
    path = tmp_path / "combustion" / "archive.sqlite3"
    db = ArchiveDatabase(path, require_qualified_wal=False)
    metadata = await db.async_start(allow_create=True)

    assert metadata.schema_version == 3
    assert metadata.database_generation == 3
    assert db.binding_path.is_file()
    assert path.is_file()
    assert path.stat().st_mode & 0o777 == 0o600
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(path) + suffix)
        if sidecar.exists():
            assert sidecar.stat().st_mode & 0o777 == 0o600
    assert db.health.journal_mode == "wal"

    await db.async_stop()

    reopened = ArchiveDatabase(path, require_qualified_wal=False)
    again = await reopened.async_start(allow_create=False)
    assert again.archive_id == metadata.archive_id
    assert again.path_binding_hash == metadata.path_binding_hash
    await reopened.async_stop()


@pytest.mark.asyncio
async def test_v1_archive_migrates_to_v3_without_changing_identity_or_cloud_rows(
    tmp_path: Path,
):
    """S5 forward migrations preserve an identity-qualified S3 archive."""
    path = tmp_path / "combustion" / "archive.sqlite3"
    path.parent.mkdir(parents=True)
    archive_id = "11111111-2222-3333-4444-555555555555"
    binding_hash = hashlib.sha256(
        (archive_id + "\0" + os.path.abspath(path)).encode()
    ).hexdigest()

    conn = sqlite3.connect(path, isolation_level=None)
    legacy = install_schema_v1(
        conn,
        archive_id=archive_id,
        application_fingerprint="s3-fixture",
        path_binding_hash=binding_hash,
    )
    assert legacy.schema_version == 1
    assert legacy.database_generation == 1

    now = 1_790_000_000_000_000
    conn.execute(
        """
        INSERT INTO accounts(
            account_id,provider,project,subject_ref,created_at_us,archived
        ) VALUES('acct','firebase','combustion-production-apps','subject',?,0)
        """,
        (now,),
    )
    conn.execute(
        """
        INSERT INTO source_devices(
            source_device_id,account_id,source_kind,provider_type,raw_serial,
            source_key,provider_locator,first_seen_us,last_seen_us
        ) VALUES('src','acct','cloud_probe',1,'serial','cloud-probe:serial',
            'locator',?,?)
        """,
        (now, now),
    )
    conn.execute(
        """
        INSERT INTO cloud_sessions(
            session_id,source_device_id,source_session_token,index_id,
            first_seen_us,last_seen_us,identity_state
        ) VALUES('session','src','42',NULL,?,?,'source_only')
        """,
        (now, now),
    )
    conn.close()

    binding = path.with_name("archive.binding.json")
    binding.write_text(
        json.dumps(
            {
                "binding_version": 1,
                "archive_id": archive_id,
                "database_path": str(path),
                "path_binding_hash": binding_hash,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    os.chmod(binding, 0o600)

    db = ArchiveDatabase(
        path,
        require_qualified_wal=False,
        application_fingerprint="s4-test",
    )
    migrated = await db.async_start(allow_create=False)

    assert migrated.archive_id == archive_id
    assert migrated.schema_version == 3
    assert migrated.minimum_reader_version == 3
    assert migrated.database_generation == 3
    assert migrated.path_binding_hash == binding_hash

    preserved = await db.async_read(
        lambda read: (
            read.execute("SELECT COUNT(*) FROM accounts").fetchone()[0],
            read.execute("SELECT COUNT(*) FROM source_devices").fetchone()[0],
            read.execute("SELECT COUNT(*) FROM cloud_sessions").fetchone()[0],
            read.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall(),
            read.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name IN "
                "('local_sources','local_capture_runs','local_observations',"
                "'local_capture_gaps','identity_links') ORDER BY name"
            ).fetchall(),
        )
    )
    assert preserved[:3] == (1, 1, 1)
    assert preserved[3] == [(1,), (2,), (3,)]
    assert preserved[4] == [
        ("identity_links",),
        ("local_capture_gaps",),
        ("local_capture_runs",),
        ("local_observations",),
        ("local_sources",),
    ]

    await db.async_stop()


@pytest.mark.asyncio
async def test_v2_archive_migrates_to_v3_preserving_s4_local_rows(tmp_path: Path):
    """S5 migration preserves an accepted S4 local archive boundary."""
    path = tmp_path / "combustion" / "archive.sqlite3"
    path.parent.mkdir(parents=True)
    archive_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    binding_hash = hashlib.sha256(
        (archive_id + "\0" + os.path.abspath(path)).encode()
    ).hexdigest()

    conn = sqlite3.connect(path, isolation_level=None)
    install_schema_v1(
        conn,
        archive_id=archive_id,
        application_fingerprint="s3-fixture",
        path_binding_hash=binding_hash,
    )
    conn.executescript("BEGIN IMMEDIATE;\n" + SCHEMA_V2_SQL)
    conn.execute(
        """
        UPDATE archive_meta
        SET schema_version=2,
            minimum_reader_version=2,
            database_generation=2,
            application_fingerprint='s4-fixture'
        WHERE id=1
        """
    )
    conn.execute(
        """
        INSERT INTO schema_migrations(
            version,checksum,applied_at_us,application_fingerprint
        ) VALUES(2,?,?,?)
        """,
        (SCHEMA_V2_CHECKSUM, 1_790_000_000_000_000, "s4-fixture"),
    )
    conn.execute("COMMIT")
    conn.execute(
        """
        INSERT INTO local_sources(
            local_source_id,subject_kind,raw_serial,first_seen_us,last_seen_us
        ) VALUES('local-probe','probe','10014e68',1,2)
        """
    )
    conn.close()

    binding = path.with_name("archive.binding.json")
    binding.write_text(
        json.dumps(
            {
                "binding_version": 1,
                "archive_id": archive_id,
                "database_path": str(path),
                "path_binding_hash": binding_hash,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    os.chmod(binding, 0o600)

    db = ArchiveDatabase(
        path,
        require_qualified_wal=False,
        application_fingerprint="s5-test",
    )
    migrated = await db.async_start(allow_create=False)

    assert migrated.archive_id == archive_id
    assert migrated.schema_version == 3
    assert migrated.minimum_reader_version == 3
    assert migrated.database_generation == 3

    preserved = await db.async_read(
        lambda read: (
            read.execute(
                "SELECT subject_kind,raw_serial FROM local_sources"
            ).fetchall(),
            read.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall(),
            read.execute(
                "SELECT COUNT(*) FROM identity_links"
            ).fetchone()[0],
        )
    )
    assert preserved == (
        [("probe", "10014e68")],
        [(1,), (2,), (3,)],
        0,
    )
    await db.async_stop()


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
            row = await db.async_read(
                lambda conn: conn.execute(
                    "SELECT value FROM cancellation_proof"
                ).fetchone()
            )
        except sqlite3.OperationalError:
            row = None
        if row is None:
            await asyncio.sleep(0.01)
            continue
        assert row[0] == 1
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
    assert destination.stat().st_mode & 0o777 == 0o600
    binding = Path(result["binding"])
    manifest = Path(result["manifest"])
    assert binding.is_file()
    assert manifest.is_file()
    assert binding.stat().st_mode & 0o777 == 0o600
    assert manifest.stat().st_mode & 0o777 == 0o600

    binding_payload = json.loads(binding.read_text())
    assert set(binding_payload) == {
        "archive_id",
        "binding_version",
        "database_path",
        "path_binding_hash",
    }

    validated = ArchiveDatabase.validate_backup_sync(destination, manifest)
    assert validated["archive_id"] == metadata.archive_id
    assert validated["schema_version"] == 3
    assert validated["counts"]["identity_links"] == 0

    await db.async_stop()


@pytest.mark.asyncio
async def test_backup_validator_rejects_db_backed_manifest_tamper(tmp_path: Path):
    """Manifest fields derived from the snapshot must agree with the snapshot."""
    path = tmp_path / "combustion" / "archive.sqlite3"
    db = ArchiveDatabase(
        path,
        require_qualified_wal=False,
        application_fingerprint="test-fingerprint",
    )
    await db.async_start(allow_create=True)
    destination = path.parent / "backups" / "archive-test.sqlite3"
    result = await db.async_backup(destination)
    manifest = Path(result["manifest"])
    original = json.loads(manifest.read_text())

    cases = (
        ("database_bytes", int(original["database_bytes"]) + 1, "size"),
        ("schema_version", 999, "schema"),
        ("minimum_reader_version", 999, "reader"),
        ("database_generation", 999, "generation"),
        ("last_committed_receipt_us", 999, "receipt"),
    )
    for field, value, message in cases:
        changed = {**original, field: value}
        manifest.write_text(json.dumps(changed))
        with pytest.raises(ArchiveError, match=message):
            ArchiveDatabase.validate_backup_sync(destination, manifest)

    manifest.write_text(json.dumps(original))
    ArchiveDatabase.validate_backup_sync(destination, manifest)

    binding = manifest.parent / original["binding_file"]
    binding_bytes = binding.read_bytes()
    binding.write_bytes(binding_bytes + b" ")
    with pytest.raises(ArchiveError, match="binding hash"):
        ArchiveDatabase.validate_backup_sync(destination, manifest)
    binding.write_bytes(binding_bytes)
    ArchiveDatabase.validate_backup_sync(destination, manifest)
    await db.async_stop()


@pytest.mark.asyncio
async def test_shutdown_checkpoint_failure_remains_fenced(
    tmp_path: Path, monkeypatch
):
    """A failed final WAL checkpoint cannot be reported as a clean stop."""
    real_connect = sqlite3.connect

    class FailingCheckpointConnection:
        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def execute(self, sql, *args):
            if "wal_checkpoint(TRUNCATE)" in sql:
                raise sqlite3.OperationalError("injected final checkpoint failure")
            return self._inner.execute(sql, *args)

    def connect(*args, **kwargs):
        inner = real_connect(*args, **kwargs)
        if kwargs.get("uri"):
            return inner
        return FailingCheckpointConnection(inner)

    monkeypatch.setattr(
        "custom_components.combustion.storage.database.sqlite3.connect",
        connect,
    )

    path = tmp_path / "archive.sqlite3"
    db = ArchiveDatabase(path, require_qualified_wal=False)
    await db.async_start(allow_create=True)

    with pytest.raises(ArchiveShutdownIncomplete, match="ownership remains fenced"):
        await db.async_stop()

    assert db.health.status is ArchiveStatus.DEGRADED
    assert db.health.error_category == "shutdown_incomplete"

    second = ArchiveDatabase(path, require_qualified_wal=False)
    with pytest.raises(ArchiveBusyError):
        await second.async_start(allow_create=False)


def test_backup_hash_tamper_is_rejected(tmp_path: Path):
    """A snapshot manifest cannot bless changed bytes."""
    database = tmp_path / "backup.sqlite3"
    database.write_bytes(b"not-a-real-sqlite-db")
    binding = tmp_path / "backup.sqlite3.binding.json"
    binding.write_text("{}")
    binding_sha256 = hashlib.sha256(binding.read_bytes()).hexdigest()
    manifest = tmp_path / "backup.sqlite3.manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "binding_file": binding.name,
                "binding_sha256": binding_sha256,
                "sha256": "deadbeef",
            }
        )
    )

    with pytest.raises(ArchiveError, match="hash"):
        ArchiveDatabase.validate_backup_sync(database, manifest)


@pytest.mark.asyncio
async def test_unqualified_runtime_uses_full_rollback_journal(
    tmp_path: Path, monkeypatch
):
    """Unknown SQLite lines use DELETE journal instead of unqualified WAL."""
    monkeypatch.setattr(
        "custom_components.combustion.storage.database.sqlite_wal_fix_qualified",
        lambda _version=sqlite3.sqlite_version: False,
    )
    db = ArchiveDatabase(
        tmp_path / "archive.sqlite3",
        require_qualified_wal=True,
    )
    await db.async_start(allow_create=True)

    assert db.health.wal_qualified is False
    assert db.health.journal_mode == "delete"

    synchronous = await db.async_read(
        lambda conn: conn.execute("PRAGMA synchronous").fetchone()[0]
    )
    # SQLite FULL is numeric level 2.
    assert synchronous == 2
    await db.async_stop()


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


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("10014e68", "10014E68"),
        ("0A1B2C3D", "0A1B2C3D"),
        ("a1b2c3d", "0A1B2C3D"),
        ("00000001", "00000001"),
        ("", None),
        ("0", None),
        ("not-hex", None),
        ("100000000", None),
    ],
)
def test_s5_probe_serial_normalization(value: str, expected: str | None):
    """Cloud/local serial formatting is normalized without fuzzy matching."""
    assert canonical_probe_serial(value) == expected


@pytest.mark.asyncio
async def test_s5_exact_serial_link_is_durable_and_idempotent(tmp_path: Path):
    """One exact physical serial links one cloud source to one local probe."""
    db = ArchiveDatabase(tmp_path / "archive.sqlite3", require_qualified_wal=False)
    await db.async_start(allow_create=True)

    def seed(conn):
        now = 1_790_000_000_000_000
        conn.execute(
            """
            INSERT INTO accounts(
                account_id,provider,project,subject_ref,created_at_us,archived
            ) VALUES('acct','firebase','combustion-production-apps','subject',?,0)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO source_devices(
                source_device_id,account_id,source_kind,provider_type,raw_serial,
                source_key,provider_locator,first_seen_us,last_seen_us
            ) VALUES(
                'cloud-src','acct','cloud_probe',1,'0A1B2C3D',
                'cloud-probe:test','provider-locator',?,?
            )
            """,
            (now, now),
        )
        conn.execute(
            """
            INSERT INTO local_sources(
                local_source_id,subject_kind,raw_serial,first_seen_us,last_seen_us
            ) VALUES('local-src','probe','a1b2c3d',?,?)
            """,
            (now, now),
        )

    await db.async_write(seed)
    links = SourceLinkRepository(db)
    first = await links.async_reconcile()
    second = await links.async_reconcile()

    assert first["linked"] == 1
    assert first["unresolved"] == 0
    assert first["ambiguous"] == 0
    assert second["linked"] == 0
    assert second["unchanged"] == 1

    row = await db.async_read(
        lambda conn: conn.execute(
            """
            SELECT canonical_serial,evidence_kind,evidence_json,
                   ended_at_us,end_reason
            FROM identity_links
            """
        ).fetchone()
    )
    assert row[0] == "0A1B2C3D"
    assert row[1] == "exact_serial"
    assert '"cloud_raw_serial":"0A1B2C3D"' in row[2]
    assert '"local_raw_serial":"a1b2c3d"' in row[2]
    assert row[3:] == (None, None)
    assert (await links.async_counts())["active_links"] == 1
    await db.async_stop()


@pytest.mark.asyncio
async def test_s5_unmatched_serial_is_not_linked(tmp_path: Path):
    """No local exact serial evidence leaves the cloud source unresolved."""
    db = ArchiveDatabase(tmp_path / "archive.sqlite3", require_qualified_wal=False)
    await db.async_start(allow_create=True)

    def seed(conn):
        now = 1_790_000_000_000_000
        conn.execute(
            """
            INSERT INTO accounts(
                account_id,provider,project,subject_ref,created_at_us,archived
            ) VALUES('acct','firebase','combustion-production-apps','subject',?,0)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO source_devices(
                source_device_id,account_id,source_kind,provider_type,raw_serial,
                source_key,provider_locator,first_seen_us,last_seen_us
            ) VALUES(
                'cloud-src','acct','cloud_probe',1,'0A1B2C3D',
                'cloud-probe:test','provider-locator',?,?
            )
            """,
            (now, now),
        )
        conn.execute(
            """
            INSERT INTO local_sources(
                local_source_id,subject_kind,raw_serial,first_seen_us,last_seen_us
            ) VALUES('local-src','probe','10014e68',?,?)
            """,
            (now, now),
        )

    await db.async_write(seed)
    links = SourceLinkRepository(db)
    result = await links.async_reconcile()
    assert result["linked"] == 0
    assert result["unresolved"] == 1
    assert (await links.async_counts())["active_links"] == 0
    await db.async_stop()

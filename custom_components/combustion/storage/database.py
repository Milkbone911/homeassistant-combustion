"""Thread-affine SQLite ownership, bounded readers and consistent backup."""
from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import os
import queue
import sqlite3
import threading
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, TypeVar

from .schema import (
    ArchiveMetadata,
    ArchiveSchemaError,
    canonical_json,
    install_schema_v1,
    read_metadata,
    utc_now_us,
)

_T = TypeVar("_T")
_WRITE = Callable[[sqlite3.Connection], _T]
_READ = Callable[[sqlite3.Connection], _T]

_BINDING_VERSION = 1
_BUSY_TIMEOUT_MS = 5_000
_CHECKPOINT_EVERY_COMMANDS = 50
_OWNERSHIP_LOCK = threading.Lock()
_OWNED_PATHS: set[str] = set()


class ArchiveError(Exception):
    """Base archive failure safe to classify without leaking private data."""


class ArchiveIdentityError(ArchiveError):
    """Archive path/binding identity is missing or inconsistent."""


class ArchiveRuntimeUnsupported(ArchiveError):
    """Runtime SQLite build is not qualified for the selected WAL contract."""


class ArchiveBusyError(ArchiveError):
    """Archive writer path is already owned by this process."""


class ArchiveShutdownIncomplete(ArchiveError):
    """Archive worker failed to exit; ownership must remain fenced."""


class ArchiveStatus(StrEnum):
    """Sanitized archive subsystem states."""

    DISABLED = "disabled"
    STARTING = "starting"
    READY = "ready"
    DEGRADED = "degraded"
    RECOVERY_REQUIRED = "recovery_required"
    UNSUPPORTED = "unsupported"
    STOPPING = "stopping"
    STOPPED = "stopped"


@dataclass(slots=True)
class ArchiveHealth:
    """Allowlisted operational health without source/account identifiers."""

    status: ArchiveStatus = ArchiveStatus.DISABLED
    schema_version: int | None = None
    sqlite_version: str = sqlite3.sqlite_version
    journal_mode: str | None = None
    error_category: str | None = None
    database_generation: int | None = None
    writer_queue_depth: int = 0
    reader_limit: int = 2
    last_success_us: int | None = None
    last_backup_us: int | None = None
    database_bytes: int | None = None
    wal_bytes: int | None = None


@dataclass(slots=True)
class _Command:
    fn: _WRITE[Any]
    future: concurrent.futures.Future[Any]


_STOP = object()


def sqlite_wal_fix_qualified(version: str = sqlite3.sqlite_version) -> bool:
    """Return whether the runtime is in a documented fixed SQLite line.

    The project review pins the WAL-reset fix to SQLite >=3.51.3, or the
    documented 3.50.7 / 3.44.6 maintenance backports. Other release lines are
    deliberately not guessed safe.
    """
    try:
        parts = tuple(int(piece) for piece in version.split(".")[:3])
    except ValueError:
        return False
    parts = (*parts, 0, 0, 0)[:3]
    if parts >= (3, 51, 3):
        return True
    if (3, 50, 7) <= parts < (3, 51, 0):
        return True
    return (3, 44, 6) <= parts < (3, 45, 0)


def _binding_hash(archive_id: str, path: Path) -> str:
    return hashlib.sha256(
        (archive_id + "\0" + os.path.abspath(path)).encode()
    ).hexdigest()


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class ArchiveDatabase:
    """Own exactly one archive writer connection and bounded readers."""

    def __init__(
        self,
        path: Path,
        *,
        binding_path: Path | None = None,
        health: ArchiveHealth | None = None,
        application_fingerprint: str = "unknown",
        require_qualified_wal: bool = True,
        reader_limit: int = 2,
    ) -> None:
        """Initialize without touching disk."""
        if not 1 <= reader_limit <= 2:
            raise ValueError("reader_limit must be between 1 and 2")
        self.path = Path(os.path.abspath(path))
        self.binding_path = Path(
            os.path.abspath(
                binding_path
                if binding_path is not None
                else self.path.with_name("archive.binding.json")
            )
        )
        self.health = health or ArchiveHealth()
        self.application_fingerprint = application_fingerprint
        self.require_qualified_wal = require_qualified_wal
        self.reader_limit = reader_limit

        self._commands: queue.Queue[_Command | object] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._init_future: concurrent.futures.Future[ArchiveMetadata] | None = None
        self._reader_pool: concurrent.futures.ThreadPoolExecutor | None = None
        self._accepting = False
        self._owns_path = False
        self._stopped = False
        self._metadata: ArchiveMetadata | None = None

    @property
    def metadata(self) -> ArchiveMetadata | None:
        """Return validated metadata after a successful start."""
        return self._metadata

    @property
    def accepting(self) -> bool:
        """Return whether new writer work is accepted."""
        return self._accepting

    def _acquire_guard(self) -> None:
        key = str(self.path)
        with _OWNERSHIP_LOCK:
            if key in _OWNED_PATHS:
                raise ArchiveBusyError("Archive writer is already owned")
            _OWNED_PATHS.add(key)
        self._owns_path = True

    def _release_guard(self) -> None:
        if not self._owns_path:
            return
        with _OWNERSHIP_LOCK:
            _OWNED_PATHS.discard(str(self.path))
        self._owns_path = False

    async def async_start(self, *, allow_create: bool) -> ArchiveMetadata:
        """Start the dedicated writer and validate/create archive identity."""
        if self._thread is not None:
            if self._metadata is None:
                raise ArchiveError("Archive start is already in progress")
            return self._metadata

        self.health.status = ArchiveStatus.STARTING
        if self.require_qualified_wal and not sqlite_wal_fix_qualified():
            self.health.status = ArchiveStatus.UNSUPPORTED
            self.health.error_category = "sqlite_runtime"
            raise ArchiveRuntimeUnsupported(
                "SQLite runtime is not qualified for archive WAL operation"
            )

        self._acquire_guard()
        self._init_future = concurrent.futures.Future()
        self._thread = threading.Thread(
            target=self._writer_main,
            args=(allow_create,),
            name="combustion-archive-writer",
            daemon=True,
        )
        self._thread.start()

        try:
            metadata = await asyncio.shield(
                asyncio.wrap_future(self._init_future)
            )
        except asyncio.CancelledError:
            # The writer thread cannot be killed by cancelling this await.
            # Runtime shutdown retains ownership and explicitly drains it.
            raise
        except BaseException as err:
            thread = self._thread
            if thread is not None:
                await asyncio.to_thread(thread.join, 2.0)
                if thread.is_alive():
                    self.health.status = ArchiveStatus.DEGRADED
                    self.health.error_category = "start_incomplete"
                    raise ArchiveShutdownIncomplete(
                        "Archive start failed while writer thread is still alive"
                    ) from err
            self._thread = None
            self._release_guard()
            raise

        self._metadata = metadata
        self._reader_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.reader_limit,
            thread_name_prefix="combustion-archive-reader",
        )
        self._accepting = True
        self.health.status = ArchiveStatus.READY
        self.health.schema_version = metadata.schema_version
        self.health.database_generation = metadata.database_generation
        self.health.error_category = None
        self.health.last_success_us = utc_now_us()
        await self.async_refresh_sizes()
        return metadata

    def _load_binding(self) -> dict[str, Any]:
        try:
            raw = self.binding_path.read_bytes()
            if len(raw) > 16 * 1024:
                raise ArchiveIdentityError("Archive binding is oversized")
            value = json.loads(raw)
        except (OSError, ValueError, UnicodeError) as err:
            raise ArchiveIdentityError("Archive binding is unreadable") from err
        if not isinstance(value, dict):
            raise ArchiveIdentityError("Archive binding is invalid")
        return value

    def _write_new_binding(self, archive_id: str) -> str:
        binding_hash = _binding_hash(archive_id, self.path)
        payload = {
            "binding_version": _BINDING_VERSION,
            "archive_id": archive_id,
            "database_path": str(self.path),
            "path_binding_hash": binding_hash,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.binding_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.binding_path.with_suffix(self.binding_path.suffix + ".tmp")
        if tmp.exists():
            raise ArchiveIdentityError("Incomplete archive binding already exists")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        fd = os.open(tmp, flags, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(canonical_json(payload, max_bytes=16 * 1024))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            with suppress(OSError):
                tmp.unlink()
            raise
        os.replace(tmp, self.binding_path)
        _fsync_file(self.binding_path)
        _fsync_dir(self.binding_path.parent)
        return binding_hash

    def _validate_binding(self, metadata: ArchiveMetadata) -> None:
        binding = self._load_binding()
        if binding.get("binding_version") != _BINDING_VERSION:
            raise ArchiveIdentityError("Archive binding version is unsupported")
        if binding.get("archive_id") != metadata.archive_id:
            raise ArchiveIdentityError("Archive identity does not match binding")
        if binding.get("database_path") != str(self.path):
            raise ArchiveIdentityError("Archive path does not match binding")
        expected = _binding_hash(metadata.archive_id, self.path)
        if (
            binding.get("path_binding_hash") != expected
            or metadata.path_binding_hash != expected
        ):
            raise ArchiveIdentityError("Archive path binding hash mismatch")

    def _open_writer(self, allow_create: bool) -> tuple[sqlite3.Connection, ArchiveMetadata]:
        db_exists = self.path.exists()
        binding_exists = self.binding_path.exists()

        if binding_exists and not db_exists:
            raise ArchiveIdentityError("Expected archive database is missing")
        if db_exists and not binding_exists:
            raise ArchiveIdentityError("Existing archive has no identity binding")
        if not db_exists and not binding_exists and not allow_create:
            raise ArchiveIdentityError("Archive has not been explicitly created")

        new_archive = not db_exists
        binding_hash: str | None = None
        archive_id: str | None = None
        if new_archive:
            archive_id = str(uuid.uuid4())
            binding_hash = self._write_new_binding(archive_id)

        conn = sqlite3.connect(
            self.path,
            timeout=_BUSY_TIMEOUT_MS / 1000,
            isolation_level=None,
        )
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
            conn.execute("PRAGMA synchronous=FULL")
            mode = str(conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
            if mode != "wal":
                raise ArchiveRuntimeUnsupported("Archive filesystem did not enable WAL")
            conn.execute("PRAGMA wal_autocheckpoint=0")
            self.health.journal_mode = mode

            if new_archive:
                assert archive_id is not None and binding_hash is not None
                metadata = install_schema_v1(
                    conn,
                    archive_id=archive_id,
                    application_fingerprint=self.application_fingerprint,
                    path_binding_hash=binding_hash,
                )
            else:
                metadata = read_metadata(conn)
                self._validate_binding(metadata)

            fk = conn.execute("PRAGMA foreign_key_check").fetchone()
            if fk is not None:
                raise ArchiveSchemaError("Archive foreign-key validation failed")
            return conn, metadata
        except BaseException:
            conn.close()
            raise

    def _writer_main(self, allow_create: bool) -> None:
        conn: sqlite3.Connection | None = None
        try:
            conn, metadata = self._open_writer(allow_create)
            assert self._init_future is not None
            self._init_future.set_result(metadata)
            commands_since_checkpoint = 0

            while True:
                command = self._commands.get()
                if command is _STOP:
                    break
                assert isinstance(command, _Command)
                if command.future.cancelled():
                    continue
                try:
                    result = command.fn(conn)
                    commands_since_checkpoint += 1
                    if commands_since_checkpoint >= _CHECKPOINT_EVERY_COMMANDS:
                        conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
                        commands_since_checkpoint = 0
                except BaseException as err:
                    command.future.set_exception(err)
                else:
                    command.future.set_result(result)

            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            conn.close()
            conn = None
        except BaseException as err:
            if self._init_future is not None and not self._init_future.done():
                self._init_future.set_exception(err)
            else:
                self.health.status = ArchiveStatus.DEGRADED
                self.health.error_category = "writer"
        finally:
            if conn is not None:
                with suppress(sqlite3.Error):
                    conn.close()

    async def async_write(self, fn: _WRITE[_T]) -> _T:
        """Serialize one connection-affine operation on the writer thread."""
        if not self._accepting:
            raise ArchiveError("Archive is not accepting writer work")
        future: concurrent.futures.Future[_T] = concurrent.futures.Future()
        self._commands.put(_Command(fn, future))
        self.health.writer_queue_depth = self._commands.qsize()
        try:
            result = await asyncio.shield(asyncio.wrap_future(future))
        finally:
            self.health.writer_queue_depth = self._commands.qsize()
        self.health.last_success_us = utc_now_us()
        return result

    def _read_call(self, fn: _READ[_T]) -> _T:
        uri = f"file:{self.path}?mode=ro"
        conn = sqlite3.connect(
            uri,
            uri=True,
            timeout=_BUSY_TIMEOUT_MS / 1000,
            isolation_level=None,
        )
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
            conn.execute("PRAGMA query_only=ON")
            return fn(conn)
        finally:
            conn.close()

    async def async_read(self, fn: _READ[_T]) -> _T:
        """Run a short read on one of at most two thread-confined connections."""
        if self._reader_pool is None or self._stopped:
            raise ArchiveError("Archive reader pool is unavailable")
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(self._reader_pool, self._read_call, fn)
        result = await asyncio.shield(future)
        self.health.last_success_us = utc_now_us()
        return result

    async def async_refresh_sizes(self) -> None:
        """Refresh bounded file-size diagnostics off the event loop."""
        def stat_sizes() -> tuple[int, int]:
            try:
                db_size = self.path.stat().st_size
            except OSError:
                db_size = 0
            wal = Path(str(self.path) + "-wal")
            try:
                wal_size = wal.stat().st_size
            except OSError:
                wal_size = 0
            return db_size, wal_size

        db_size, wal_size = await asyncio.to_thread(stat_sizes)
        self.health.database_bytes = db_size
        self.health.wal_bytes = wal_size

    def _backup_sync(self, destination: Path) -> dict[str, Any]:
        backup_root = self.path.parent / "backups"
        backup_root.mkdir(parents=True, exist_ok=True)
        destination = Path(os.path.abspath(destination))
        if destination.parent != Path(os.path.abspath(backup_root)):
            raise ArchiveError("Backup destination is outside the archive backup directory")
        if destination.exists():
            raise ArchiveError("Backup destination already exists")

        source = sqlite3.connect(
            f"file:{self.path}?mode=ro",
            uri=True,
            timeout=_BUSY_TIMEOUT_MS / 1000,
            isolation_level=None,
        )
        dest = sqlite3.connect(destination, isolation_level=None)
        try:
            source.execute("PRAGMA query_only=ON")
            source.backup(dest, pages=256, sleep=0.01)
            dest.execute("PRAGMA foreign_keys=ON")
            quick = dest.execute("PRAGMA quick_check").fetchone()
            if quick is None or quick[0] != "ok":
                raise ArchiveSchemaError("Backup quick_check failed")
            metadata = read_metadata(dest)
            counts = {
                table: int(dest.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in (
                    "source_devices",
                    "cloud_sessions",
                    "session_manifests",
                    "cloud_samples",
                    "cloud_sample_versions",
                    "gaps",
                    "sync_receipts",
                )
            }
            last_receipt = dest.execute(
                "SELECT MAX(committed_at_us) FROM sync_receipts"
            ).fetchone()[0]
        except BaseException:
            dest.close()
            source.close()
            with suppress(OSError):
                destination.unlink()
            raise
        finally:
            with suppress(sqlite3.Error):
                dest.close()
            with suppress(sqlite3.Error):
                source.close()

        digest = hashlib.sha256()
        with destination.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)

        created = utc_now_us()
        manifest = {
            "archive_id": metadata.archive_id,
            "schema_version": metadata.schema_version,
            "minimum_reader_version": metadata.minimum_reader_version,
            "database_generation": metadata.database_generation,
            "application_fingerprint": self.application_fingerprint,
            "created_at_us": created,
            "database_bytes": destination.stat().st_size,
            "sha256": digest.hexdigest(),
            "counts": counts,
            "last_committed_receipt_us": last_receipt,
        }
        manifest_path = destination.with_suffix(destination.suffix + ".manifest.json")
        tmp = manifest_path.with_suffix(manifest_path.suffix + ".tmp")
        tmp.write_text(canonical_json(manifest, max_bytes=64 * 1024) + "\n")
        os.chmod(tmp, 0o600)
        _fsync_file(tmp)
        os.replace(tmp, manifest_path)
        _fsync_file(manifest_path)
        _fsync_dir(manifest_path.parent)
        return {
            "database": str(destination),
            "manifest": str(manifest_path),
            "created_at_us": created,
            "counts": counts,
            "sha256": manifest["sha256"],
        }

    async def async_backup(self, destination: Path) -> dict[str, Any]:
        """Create a consistent SQLite online backup and semantic manifest."""
        if self._reader_pool is None:
            raise ArchiveError("Archive reader pool is unavailable")
        loop = asyncio.get_running_loop()
        result = await asyncio.shield(
            loop.run_in_executor(self._reader_pool, self._backup_sync, destination)
        )
        self.health.last_backup_us = int(result["created_at_us"])
        await self.async_refresh_sizes()
        return result

    @staticmethod
    def validate_backup_sync(database: Path, manifest: Path) -> dict[str, Any]:
        """Open a closed snapshot and verify hash/schema/integrity/count semantics."""
        database = Path(database)
        manifest = Path(manifest)
        payload = json.loads(manifest.read_text())
        digest = hashlib.sha256()
        with database.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != payload.get("sha256"):
            raise ArchiveError("Backup hash mismatch")
        conn = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
        try:
            quick = conn.execute("PRAGMA quick_check").fetchone()
            if quick is None or quick[0] != "ok":
                raise ArchiveError("Backup integrity check failed")
            metadata = read_metadata(conn)
            if metadata.archive_id != payload.get("archive_id"):
                raise ArchiveError("Backup archive identity mismatch")
            counts = {
                table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in (
                    "source_devices",
                    "cloud_sessions",
                    "session_manifests",
                    "cloud_samples",
                    "cloud_sample_versions",
                    "gaps",
                    "sync_receipts",
                )
            }
        finally:
            conn.close()
        if counts != payload.get("counts"):
            raise ArchiveError("Backup semantic counts mismatch")
        return {
            "archive_id": metadata.archive_id,
            "schema_version": metadata.schema_version,
            "database_generation": metadata.database_generation,
            "counts": counts,
        }

    def stop_accepting(self) -> None:
        """Refuse new archive work before supervisor cancellation/drain."""
        if self._stopped:
            return
        self._accepting = False
        self.health.status = ArchiveStatus.STOPPING

    async def async_stop(self, *, timeout: float = 10.0) -> None:
        """Stop accepting work, drain queued writes, checkpoint and close."""
        if self._stopped:
            return
        self.stop_accepting()
        thread = self._thread
        if thread is not None:
            self._commands.put(_STOP)
            await asyncio.to_thread(thread.join, timeout)
            if thread.is_alive():
                self.health.status = ArchiveStatus.DEGRADED
                self.health.error_category = "shutdown_incomplete"
                raise ArchiveShutdownIncomplete(
                    "Archive writer did not exit; ownership remains fenced"
                )
        self._thread = None

        pool = self._reader_pool
        self._reader_pool = None
        if pool is not None:
            await asyncio.to_thread(pool.shutdown, True)

        self._stopped = True
        self._release_guard()
        self.health.status = ArchiveStatus.STOPPED
        self.health.writer_queue_depth = 0

    def discard_unstarted(self) -> None:
        """Release a guard after a failed start only when no worker survives."""
        thread = self._thread
        if thread is not None and thread.is_alive():
            return
        self._release_guard()

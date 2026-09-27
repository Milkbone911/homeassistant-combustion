"""Source-preserving archive repository and durable reconciliation work ledger."""
from __future__ import annotations

import hashlib
import sqlite3
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ..cloud.client import IndexTraversal
from ..cloud.models import Probe, SampleRow, SessionMeta, normalize_ranges
from .database import ArchiveDatabase
from .schema import canonical_json, content_hash, parse_utc_us, utc_now_us

_ACCOUNT_NS = uuid.UUID("ad0d4ca4-3791-48ee-998f-96cba4712f65")
_SOURCE_NS = uuid.UUID("9ff4e1be-802e-4ef1-a1c9-a09943a935e0")
_SESSION_NS = uuid.UUID("08aa1f1b-6f73-42ce-a67e-ef52d2e61f33")
_MANIFEST_NS = uuid.UUID("30c11718-5a92-465b-a4cb-0f2c49df013a")
_VERSION_NS = uuid.UUID("2e762711-c4ca-48c6-b6fc-a232735af9a7")

MANIFEST_RECHECK_US = 6 * 60 * 60 * 1_000_000
FULL_AUDIT_INTERVAL_US = 7 * 24 * 60 * 60 * 1_000_000
MISSING_RETRY_US = 60 * 60 * 1_000_000
MAX_WORK_ATTEMPTS = 8
CHUNK_SIZE = 1000


@dataclass(frozen=True, slots=True)
class SourceDeviceRecord:
    """Stable archive identity for one cloud source probe."""

    source_device_id: str
    account_id: str
    serial: str
    provider_locator: str


@dataclass(frozen=True, slots=True)
class SyncWork:
    """One persisted work unit claimed transactionally by the supervisor."""

    work_id: str
    kind: str
    session_id: str
    manifest_id: str | None
    start_seq: int | None
    end_seq: int | None
    attempts: int
    account_id: str
    source_device_id: str
    serial: str
    provider_locator: str
    source_session_token: str


@dataclass(frozen=True, slots=True)
class ManifestRecord:
    """Stored manifest revision selected for reconciliation."""

    manifest_id: str
    session_id: str
    revision: int
    changed: bool
    ranges: tuple[tuple[int, int], ...]


FaultInjector = Callable[[str], None]


def _account_identity(subject: str) -> tuple[str, str]:
    subject_ref = hashlib.sha256(subject.encode()).hexdigest()
    return str(uuid.uuid5(_ACCOUNT_NS, f"firebase:{subject_ref}")), subject_ref


def _source_device_identity(account_id: str, serial: str) -> tuple[str, str]:
    source_key = f"cloud-probe:{serial}"
    return str(uuid.uuid5(_SOURCE_NS, f"{account_id}:{source_key}")), source_key


def _session_identity(source_device_id: str, token: str) -> str:
    return str(uuid.uuid5(_SESSION_NS, f"{source_device_id}:{token}"))


def _manifest_identity(session_id: str, metadata_hash: str) -> str:
    return str(uuid.uuid5(_MANIFEST_NS, f"{session_id}:{metadata_hash}"))


def _version_identity(session_id: str, sequence: int, payload_hash: str) -> str:
    return str(uuid.uuid5(_VERSION_NS, f"{session_id}:{sequence}:{payload_hash}"))


def _tx(conn: sqlite3.Connection):
    """Small explicit transaction context compatible with isolation_level=None."""
    class _Transaction:
        def __enter__(self):
            conn.execute("BEGIN IMMEDIATE")
            return conn

        def __exit__(self, exc_type, exc, tb):
            if exc_type is None:
                conn.execute("COMMIT")
            else:
                conn.execute("ROLLBACK")
            return False

    return _Transaction()


def _enqueue_work(
    conn: sqlite3.Connection,
    *,
    kind: str,
    session_id: str,
    manifest_id: str | None = None,
    start_seq: int | None = None,
    end_seq: int | None = None,
    priority: int = 100,
    not_before_us: int | None = None,
) -> bool:
    now = utc_now_us()
    try:
        conn.execute(
            """
            INSERT INTO sync_work(
                work_id,kind,session_id,manifest_id,start_seq,end_seq,priority,
                state,attempts,not_before_us,last_error,created_at_us,updated_at_us
            ) VALUES(?,?,?,?,?,?,?,'ready',0,?,NULL,?,?)
            """,
            (
                str(uuid.uuid4()),
                kind,
                session_id,
                manifest_id,
                start_seq,
                end_seq,
                priority,
                now if not_before_us is None else not_before_us,
                now,
                now,
            ),
        )
    except sqlite3.IntegrityError:
        return False
    return True


def _coverage_for(
    conn: sqlite3.Connection, session_id: str, start: int, end: int
) -> list[tuple[int, int]]:
    return [
        (int(row[0]), int(row[1]))
        for row in conn.execute(
            """
            SELECT start_seq,end_seq
            FROM coverage_ranges
            WHERE session_id=? AND validity_class='identified'
              AND end_seq>=? AND start_seq<=?
            ORDER BY start_seq
            """,
            (session_id, start, end),
        )
    ]


def _subtract_coverage(
    start: int, end: int, covered: Sequence[tuple[int, int]]
) -> list[tuple[int, int]]:
    current = start
    missing: list[tuple[int, int]] = []
    for cov_start, cov_end in covered:
        if cov_end < current:
            continue
        if cov_start > end:
            break
        if cov_start > current:
            missing.append((current, min(end, cov_start - 1)))
        current = max(current, cov_end + 1)
        if current > end:
            break
    if current <= end:
        missing.append((current, end))
    return missing


def _first_missing_chunk(
    conn: sqlite3.Connection, session_id: str, manifest_id: str
) -> tuple[int, int] | None:
    for start, end in conn.execute(
        """
        SELECT start_seq,end_seq FROM manifest_ranges
        WHERE manifest_id=? ORDER BY start_seq
        """,
        (manifest_id,),
    ):
        missing = _subtract_coverage(
            int(start), int(end), _coverage_for(conn, session_id, int(start), int(end))
        )
        if missing:
            first, last = missing[0]
            return first, min(last, first + CHUNK_SIZE - 1)
    return None


def _next_manifest_chunk_after(
    conn: sqlite3.Connection, manifest_id: str, prior_end: int | None
) -> tuple[int, int] | None:
    rows = conn.execute(
        "SELECT start_seq,end_seq FROM manifest_ranges WHERE manifest_id=? ORDER BY start_seq",
        (manifest_id,),
    ).fetchall()
    if not rows:
        return None
    if prior_end is None:
        start, end = map(int, rows[0])
        return start, min(end, start + CHUNK_SIZE - 1)
    for raw_start, raw_end in rows:
        start, end = int(raw_start), int(raw_end)
        if prior_end < start:
            return start, min(end, start + CHUNK_SIZE - 1)
        if start <= prior_end < end:
            nxt = prior_end + 1
            return nxt, min(end, nxt + CHUNK_SIZE - 1)
    return None


def _merge_coverage(
    conn: sqlite3.Connection, session_id: str, start: int, end: int
) -> None:
    rows = conn.execute(
        """
        SELECT start_seq,end_seq FROM coverage_ranges
        WHERE session_id=? AND validity_class='identified'
          AND end_seq>=? AND start_seq<=?
        ORDER BY start_seq
        """,
        (session_id, start - 1, end + 1),
    ).fetchall()
    merged_start, merged_end = start, end
    for raw_start, raw_end in rows:
        merged_start = min(merged_start, int(raw_start))
        merged_end = max(merged_end, int(raw_end))
    if rows:
        conn.execute(
            """
            DELETE FROM coverage_ranges
            WHERE session_id=? AND validity_class='identified'
              AND end_seq>=? AND start_seq<=?
            """,
            (session_id, start - 1, end + 1),
        )
    conn.execute(
        """
        INSERT INTO coverage_ranges(session_id,start_seq,end_seq,validity_class)
        VALUES(?,?,?,'identified')
        """,
        (session_id, merged_start, merged_end),
    )


def _contiguous(values: Sequence[int]) -> list[tuple[int, int]]:
    if not values:
        return []
    ordered = sorted(set(values))
    output: list[tuple[int, int]] = []
    start = previous = ordered[0]
    for value in ordered[1:]:
        if value == previous + 1:
            previous = value
            continue
        output.append((start, previous))
        start = previous = value
    output.append((start, previous))
    return output


class ArchiveRepository:
    """Async source repository backed by one serialized ArchiveDatabase writer."""

    def __init__(
        self,
        database: ArchiveDatabase,
        *,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        """Initialize the repository over one archive database."""
        self.database = database
        self._fault = fault_injector or (lambda _point: None)

    async def async_recover_interrupted_work(self) -> int:
        """Return running work to ready after a process/reload interruption."""
        def write(conn: sqlite3.Connection) -> int:
            now = utc_now_us()
            with _tx(conn):
                cursor = conn.execute(
                    """
                    UPDATE sync_work
                    SET state='ready', not_before_us=?, last_error='interrupted',
                        updated_at_us=?
                    WHERE state='running'
                    """,
                    (now, now),
                )
                return int(cursor.rowcount)

        return await self.database.async_write(write)

    async def async_register_account(
        self,
        subject: str,
        generation: int,
        probes: Sequence[Probe],
    ) -> tuple[str, Mapping[str, SourceDeviceRecord]]:
        """Persist source identities without storing bearer credentials."""
        account_id, subject_ref = _account_identity(subject)
        now = utc_now_us()

        def write(conn: sqlite3.Connection):
            records: dict[str, SourceDeviceRecord] = {}
            with _tx(conn):
                conn.execute(
                    """
                    INSERT INTO accounts(
                        account_id,provider,project,subject_ref,created_at_us,archived
                    ) VALUES(?, 'firebase', 'combustion-production-apps', ?, ?, 0)
                    ON CONFLICT(account_id) DO UPDATE SET archived=0
                    """,
                    (account_id, subject_ref, now),
                )
                for probe in probes:
                    source_device_id, source_key = _source_device_identity(
                        account_id, probe.serial
                    )
                    conn.execute(
                        """
                        INSERT INTO source_devices(
                            source_device_id,account_id,source_kind,provider_type,
                            raw_serial,source_key,provider_locator,first_seen_us,last_seen_us
                        ) VALUES(?,?, 'cloud_probe',1,?,?,?,?,?)
                        ON CONFLICT(source_device_id) DO UPDATE SET
                            provider_locator=excluded.provider_locator,
                            last_seen_us=excluded.last_seen_us
                        """,
                        (
                            source_device_id,
                            account_id,
                            probe.serial,
                            source_key,
                            probe.device_key,
                            now,
                            now,
                        ),
                    )
                    records[probe.serial] = SourceDeviceRecord(
                        source_device_id,
                        account_id,
                        probe.serial,
                        probe.device_key,
                    )
            return account_id, records

        return await self.database.async_write(write)

    async def async_discovery_due(
        self,
        account_id: str,
        source_device_id: str,
        *,
        now_us: int,
        interval_us: int,
    ) -> bool:
        """Check the latest discovery receipt for one source device."""
        def read(conn: sqlite3.Connection) -> bool:
            row = conn.execute(
                """
                SELECT MAX(started_at_us) FROM discovery_runs
                WHERE account_id=? AND source_device_id=?
                  AND terminal_status='complete'
                """,
                (account_id, source_device_id),
            ).fetchone()
            return row[0] is None or int(row[0]) + interval_us <= now_us

        return await self.database.async_read(read)

    async def async_begin_discovery(
        self,
        *,
        account_id: str,
        generation: int,
        source: SourceDeviceRecord,
    ) -> str:
        """Create a durable run before the first index request."""
        run_id = str(uuid.uuid4())
        now = utc_now_us()

        def write(conn: sqlite3.Connection) -> str:
            with _tx(conn):
                conn.execute(
                    """
                    INSERT INTO discovery_runs(
                        run_id,account_id,link_generation,source_device_id,
                        started_at_us,completed_at_us,terminal_status,
                        terminal_reason,snapshot_consistent
                    ) VALUES(?,?,?,?,?,NULL,'running',NULL,0)
                    """,
                    (
                        run_id,
                        account_id,
                        generation,
                        source.source_device_id,
                        now,
                    ),
                )
            return run_id

        return await self.database.async_write(write)

    async def async_record_discovery_page(
        self,
        *,
        run_id: str,
        source: SourceDeviceRecord,
        page,
        digest: str,
    ) -> int:
        """Persist one validated index page and its source-session candidates."""
        now = utc_now_us()

        def write(conn: sqlite3.Connection) -> int:
            with _tx(conn):
                run = conn.execute(
                    "SELECT terminal_status FROM discovery_runs WHERE run_id=?",
                    (run_id,),
                ).fetchone()
                if run is None or run[0] != "running":
                    raise RuntimeError("Discovery run is not active")

                existing_page = conn.execute(
                    """
                    SELECT digest FROM discovery_pages
                    WHERE run_id=? AND page=?
                    """,
                    (run_id, page.requested_page),
                ).fetchone()
                if existing_page is not None:
                    if existing_page[0] != digest:
                        raise RuntimeError("Discovery page changed within one run")
                    return 0

                raw = {
                    "requested_page": page.requested_page,
                    "returned_page": page.returned_page,
                    "total_pages": page.total_pages,
                    "sessions": [item.raw for item in page.sessions],
                }
                conn.execute(
                    """
                    INSERT INTO discovery_pages(
                        run_id,page,digest,session_count,total_pages,raw_json
                    ) VALUES(?,?,?,?,?,?)
                    """,
                    (
                        run_id,
                        page.requested_page,
                        digest,
                        len(page.sessions),
                        page.total_pages,
                        canonical_json(raw),
                    ),
                )

                inserted = 0
                for item in page.sessions:
                    session_id = _session_identity(
                        source.source_device_id, item.source_session_token
                    )
                    existing = conn.execute(
                        """
                        SELECT index_id,identity_state FROM cloud_sessions
                        WHERE session_id=?
                        """,
                        (session_id,),
                    ).fetchone()
                    identity_state = "source_only"
                    index_id = item.index_id
                    if existing is not None:
                        old_index = existing[0]
                        identity_state = str(existing[1])
                        if (
                            old_index is not None
                            and item.index_id is not None
                            and old_index != item.index_id
                        ):
                            identity_state = "index_conflict"
                            index_id = old_index
                    else:
                        inserted += 1

                    conn.execute(
                        """
                        INSERT INTO cloud_sessions(
                            session_id,source_device_id,source_session_token,index_id,
                            first_seen_us,last_seen_us,identity_state,sample_period_ms,
                            started_at_source,ended_at_source,last_full_audit_us
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,NULL)
                        ON CONFLICT(session_id) DO UPDATE SET
                            last_seen_us=excluded.last_seen_us,
                            identity_state=excluded.identity_state,
                            sample_period_ms=COALESCE(
                                excluded.sample_period_ms,cloud_sessions.sample_period_ms
                            ),
                            started_at_source=COALESCE(
                                cloud_sessions.started_at_source,
                                excluded.started_at_source
                            ),
                            ended_at_source=COALESCE(
                                excluded.ended_at_source,
                                cloud_sessions.ended_at_source
                            )
                        """,
                        (
                            session_id,
                            source.source_device_id,
                            item.source_session_token,
                            index_id,
                            now,
                            now,
                            identity_state,
                            item.sample_period_ms,
                            item.started_at,
                            item.ended_at,
                        ),
                    )
                    _enqueue_work(
                        conn,
                        kind="manifest",
                        session_id=session_id,
                        priority=50,
                    )
                return inserted

        return await self.database.async_write(write)

    async def async_finish_discovery(
        self,
        run_id: str,
        *,
        complete: bool,
        terminal_reason: str,
        snapshot_consistent: bool = False,
    ) -> None:
        """Close a run explicitly as complete or partial."""
        now = utc_now_us()

        def write(conn: sqlite3.Connection) -> None:
            with _tx(conn):
                conn.execute(
                    """
                    UPDATE discovery_runs
                    SET completed_at_us=?,terminal_status=?,terminal_reason=?,
                        snapshot_consistent=?
                    WHERE run_id=? AND terminal_status='running'
                    """,
                    (
                        now,
                        "complete" if complete else "partial",
                        terminal_reason[:64],
                        int(snapshot_consistent),
                        run_id,
                    ),
                )

        await self.database.async_write(write)

    async def async_record_discovery(
        self,
        *,
        account_id: str,
        generation: int,
        source: SourceDeviceRecord,
        traversal: IndexTraversal,
    ) -> str:
        """Atomically record one bounded index traversal and schedule manifests."""
        run_id = str(uuid.uuid4())
        now = utc_now_us()

        def write(conn: sqlite3.Connection) -> str:
            with _tx(conn):
                conn.execute(
                    """
                    INSERT INTO discovery_runs(
                        run_id,account_id,link_generation,source_device_id,started_at_us,
                        completed_at_us,terminal_status,terminal_reason,snapshot_consistent
                    ) VALUES(?,?,?,?,?,?, 'complete', ?, ?)
                    """,
                    (
                        run_id,
                        account_id,
                        generation,
                        source.source_device_id,
                        now,
                        now,
                        traversal.terminal_reason,
                        int(traversal.snapshot_consistent),
                    ),
                )
                for page, digest in zip(
                    traversal.pages, traversal.page_digests, strict=True
                ):
                    raw = {
                        "requested_page": page.requested_page,
                        "returned_page": page.returned_page,
                        "total_pages": page.total_pages,
                        "sessions": [item.raw for item in page.sessions],
                    }
                    conn.execute(
                        """
                        INSERT INTO discovery_pages(
                            run_id,page,digest,session_count,total_pages,raw_json
                        ) VALUES(?,?,?,?,?,?)
                        """,
                        (
                            run_id,
                            page.requested_page,
                            digest,
                            len(page.sessions),
                            page.total_pages,
                            canonical_json(raw),
                        ),
                    )

                for item in traversal.sessions:
                    session_id = _session_identity(
                        source.source_device_id, item.source_session_token
                    )
                    existing = conn.execute(
                        """
                        SELECT index_id,identity_state FROM cloud_sessions
                        WHERE session_id=?
                        """,
                        (session_id,),
                    ).fetchone()
                    identity_state = "source_only"
                    index_id = item.index_id
                    if existing is not None:
                        old_index = existing[0]
                        identity_state = str(existing[1])
                        if (
                            old_index is not None
                            and item.index_id is not None
                            and old_index != item.index_id
                        ):
                            identity_state = "index_conflict"
                            index_id = old_index
                    conn.execute(
                        """
                        INSERT INTO cloud_sessions(
                            session_id,source_device_id,source_session_token,index_id,
                            first_seen_us,last_seen_us,identity_state,sample_period_ms,
                            started_at_source,ended_at_source,last_full_audit_us
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,NULL)
                        ON CONFLICT(session_id) DO UPDATE SET
                            last_seen_us=excluded.last_seen_us,
                            identity_state=excluded.identity_state,
                            sample_period_ms=COALESCE(
                                excluded.sample_period_ms,cloud_sessions.sample_period_ms
                            ),
                            started_at_source=COALESCE(
                                cloud_sessions.started_at_source,excluded.started_at_source
                            ),
                            ended_at_source=COALESCE(
                                excluded.ended_at_source,cloud_sessions.ended_at_source
                            )
                        """,
                        (
                            session_id,
                            source.source_device_id,
                            item.source_session_token,
                            index_id,
                            now,
                            now,
                            identity_state,
                            item.sample_period_ms,
                            item.started_at,
                            item.ended_at,
                        ),
                    )
                    _enqueue_work(
                        conn,
                        kind="manifest",
                        session_id=session_id,
                        priority=50,
                    )
            return run_id

        return await self.database.async_write(write)

    async def async_claim_work(
        self, *, account_id: str, now_us: int
    ) -> SyncWork | None:
        """Claim exactly one due unit for the active account namespace."""
        def write(conn: sqlite3.Connection) -> SyncWork | None:
            with _tx(conn):
                row = conn.execute(
                    """
                    SELECT
                        w.work_id,w.kind,w.session_id,w.manifest_id,w.start_seq,w.end_seq,
                        w.attempts,d.account_id,s.source_device_id,d.raw_serial,
                        d.provider_locator,s.source_session_token
                    FROM sync_work AS w
                    JOIN cloud_sessions AS s ON s.session_id=w.session_id
                    JOIN source_devices AS d ON d.source_device_id=s.source_device_id
                    WHERE w.state='ready' AND w.not_before_us<=?
                      AND d.account_id=?
                    ORDER BY w.priority ASC,w.not_before_us ASC,w.created_at_us ASC
                    LIMIT 1
                    """,
                    (now_us, account_id),
                ).fetchone()
                if row is None:
                    return None
                cursor = conn.execute(
                    """
                    UPDATE sync_work SET state='running',attempts=attempts+1,
                        updated_at_us=? WHERE work_id=? AND state='ready'
                    """,
                    (now_us, row[0]),
                )
                if cursor.rowcount != 1:
                    return None
                return SyncWork(
                    str(row[0]),
                    str(row[1]),
                    str(row[2]),
                    None if row[3] is None else str(row[3]),
                    None if row[4] is None else int(row[4]),
                    None if row[5] is None else int(row[5]),
                    int(row[6]) + 1,
                    str(row[7]),
                    str(row[8]),
                    str(row[9]),
                    str(row[10]),
                    str(row[11]),
                )

        return await self.database.async_write(write)

    async def async_fail_work(
        self,
        work_id: str,
        *,
        category: str,
        retry_delay_us: int,
    ) -> None:
        """Retry bounded failures; never overwrite a work unit already committed."""
        def write(conn: sqlite3.Connection) -> None:
            now = utc_now_us()
            with _tx(conn):
                row = conn.execute(
                    "SELECT state,attempts FROM sync_work WHERE work_id=?",
                    (work_id,),
                ).fetchone()
                if row is None or row[0] == "done":
                    return
                attempts = int(row[1])
                state = "failed" if attempts >= MAX_WORK_ATTEMPTS else "ready"
                conn.execute(
                    """
                    UPDATE sync_work SET state=?,not_before_us=?,last_error=?,
                        updated_at_us=? WHERE work_id=?
                    """,
                    (
                        state,
                        now + max(0, retry_delay_us),
                        category[:64],
                        now,
                        work_id,
                    ),
                )

        await self.database.async_write(write)

    async def async_complete_manifest(
        self,
        work: SyncWork,
        meta: SessionMeta,
    ) -> ManifestRecord:
        """Persist immutable manifest revisions and schedule one bounded next unit."""
        now = utc_now_us()
        normalized = normalize_ranges(meta.ranges)
        ranges_value = [[start, end] for start, end in normalized]
        range_hash = content_hash(ranges_value)
        metadata_hash = content_hash(meta.raw)
        manifest_id = _manifest_identity(work.session_id, metadata_hash)

        def write(conn: sqlite3.Connection) -> ManifestRecord:
            resolved_manifest_id = manifest_id
            with _tx(conn):
                state = conn.execute(
                    "SELECT state FROM sync_work WHERE work_id=?", (work.work_id,)
                ).fetchone()
                if state is None:
                    raise RuntimeError("Manifest work disappeared")
                if state[0] == "done":
                    existing = conn.execute(
                        """
                        SELECT manifest_id,revision FROM session_manifests
                        WHERE session_id=? AND metadata_hash=?
                        """,
                        (work.session_id, metadata_hash),
                    ).fetchone()
                    if existing is None:
                        raise RuntimeError("Committed manifest receipt is inconsistent")
                    return ManifestRecord(
                        str(existing[0]),
                        work.session_id,
                        int(existing[1]),
                        False,
                        normalized,
                    )
                if state[0] != "running":
                    raise RuntimeError("Manifest work is not running")

                existing = conn.execute(
                    """
                    SELECT manifest_id,revision FROM session_manifests
                    WHERE session_id=? AND metadata_hash=?
                    """,
                    (work.session_id, metadata_hash),
                ).fetchone()
                changed = existing is None
                if existing is None:
                    revision = int(
                        conn.execute(
                            """
                            SELECT COALESCE(MAX(revision),0)+1
                            FROM session_manifests WHERE session_id=?
                            """,
                            (work.session_id,),
                        ).fetchone()[0]
                    )
                    period = conn.execute(
                        "SELECT sample_period_ms FROM cloud_sessions WHERE session_id=?",
                        (work.session_id,),
                    ).fetchone()[0]
                    conn.execute(
                        """
                        INSERT INTO session_manifests(
                            manifest_id,session_id,revision,fetched_at_us,range_hash,
                            metadata_hash,started_at_source,period_ms,validation_state,raw_json
                        ) VALUES(?,?,?,?,?,?,?,?, 'valid', ?)
                        """,
                        (
                            manifest_id,
                            work.session_id,
                            revision,
                            now,
                            range_hash,
                            metadata_hash,
                            meta.started_at,
                            period,
                            canonical_json(meta.raw),
                        ),
                    )
                    for start, end in normalized:
                        conn.execute(
                            """
                            INSERT INTO manifest_ranges(resolved_manifest_id,start_seq,end_seq)
                            VALUES(?,?,?)
                            """,
                            (resolved_manifest_id, start, end),
                        )
                else:
                    resolved_manifest_id, revision = (
                        str(existing[0]),
                        int(existing[1]),
                    )

                conn.execute(
                    """
                    INSERT INTO manifest_checks(
                        check_id,session_id,manifest_id,checked_at_us,result,error_category
                    ) VALUES(?,?,?,?,?,NULL)
                    """,
                    (
                        str(uuid.uuid4()),
                        work.session_id,
                        manifest_id,
                        now,
                        "changed" if changed else "unchanged",
                    ),
                )
                conn.execute(
                    """
                    UPDATE sync_work SET state='done',last_error=NULL,updated_at_us=?
                    WHERE work_id=?
                    """,
                    (now, work.work_id),
                )

                missing = _first_missing_chunk(conn, work.session_id, resolved_manifest_id)
                if missing is not None:
                    _enqueue_work(
                        conn,
                        kind="sample",
                        session_id=work.session_id,
                        manifest_id=resolved_manifest_id,
                        start_seq=missing[0],
                        end_seq=missing[1],
                        priority=60,
                    )
                else:
                    last_audit = conn.execute(
                        "SELECT last_full_audit_us FROM cloud_sessions WHERE session_id=?",
                        (work.session_id,),
                    ).fetchone()[0]
                    if last_audit is None or int(last_audit) + FULL_AUDIT_INTERVAL_US <= now:
                        audit = _next_manifest_chunk_after(conn, resolved_manifest_id, None)
                        if audit is not None:
                            _enqueue_work(
                                conn,
                                kind="audit",
                                session_id=work.session_id,
                                manifest_id=resolved_manifest_id,
                                start_seq=audit[0],
                                end_seq=audit[1],
                                priority=200,
                            )

                _enqueue_work(
                    conn,
                    kind="manifest",
                    session_id=work.session_id,
                    priority=150,
                    not_before_us=now + MANIFEST_RECHECK_US,
                )

                self._fault("before_commit")
            self._fault("after_commit")
            return ManifestRecord(
                str(resolved_manifest_id),
                work.session_id,
                int(revision),
                changed,
                normalized,
            )

        return await self.database.async_write(write)

    async def async_record_manifest_failure(
        self,
        work: SyncWork,
        *,
        category: str,
        retry_delay_us: int,
    ) -> None:
        """Retain a failed assessment and reschedule without false completeness."""
        def write(conn: sqlite3.Connection) -> None:
            now = utc_now_us()
            with _tx(conn):
                conn.execute(
                    """
                    INSERT INTO manifest_checks(
                        check_id,session_id,manifest_id,checked_at_us,result,error_category
                    ) VALUES(?,?,NULL,?,'invalid',?)
                    """,
                    (str(uuid.uuid4()), work.session_id, now, category[:64]),
                )
            # Retry mutation is a separate atomic unit; no sample/coverage changed.

        await self.database.async_write(write)
        await self.async_fail_work(
            work.work_id,
            category=category,
            retry_delay_us=retry_delay_us,
        )

    async def async_commit_sample_work(
        self,
        work: SyncWork,
        rows: Sequence[SampleRow],
    ) -> Mapping[str, int]:
        """Atomically commit versions, coverage, gaps and receipt for one request."""
        if work.start_seq is None or work.end_seq is None or work.manifest_id is None:
            raise ValueError("Sample/audit work is missing bounded range metadata")
        if work.end_seq - work.start_seq >= CHUNK_SIZE:
            raise ValueError("Archive work exceeds one chunk")

        now = utc_now_us()

        def write(conn: sqlite3.Connection) -> Mapping[str, int]:
            with _tx(conn):
                state = conn.execute(
                    "SELECT state FROM sync_work WHERE work_id=?", (work.work_id,)
                ).fetchone()
                if state is None:
                    raise RuntimeError("Sample work disappeared")
                if state[0] == "done":
                    receipt = conn.execute(
                        """
                        SELECT committed_rows,conflict_rows,missing_rows
                        FROM sync_receipts WHERE work_id=?
                        ORDER BY committed_at_us DESC LIMIT 1
                        """,
                        (work.work_id,),
                    ).fetchone()
                    if receipt is None:
                        raise RuntimeError("Committed work has no receipt")
                    return {
                        "committed_rows": int(receipt[0]),
                        "conflict_rows": int(receipt[1]),
                        "missing_rows": int(receipt[2]),
                    }
                if state[0] != "running":
                    raise RuntimeError("Sample work is not running")

                unique_rows: dict[int, SampleRow] = {}
                for row in rows:
                    if not work.start_seq <= row.sequence <= work.end_seq:
                        raise ValueError("Sample row is outside the claimed work range")
                    if row.sequence in unique_rows:
                        raise ValueError("Duplicate sample row in archive commit")
                    unique_rows[row.sequence] = row

                newly_seen = 0
                conflicts = 0
                request_hash_values: list[str] = []
                for sequence in sorted(unique_rows):
                    row = unique_rows[sequence]
                    payload_hash = content_hash(row.raw)
                    request_hash_values.append(payload_hash)
                    version_id = _version_identity(
                        work.session_id, row.sequence, payload_hash
                    )
                    sample_exists = conn.execute(
                        """
                        SELECT 1 FROM cloud_samples WHERE session_id=? AND sequence=?
                        """,
                        (work.session_id, row.sequence),
                    ).fetchone()
                    if sample_exists is None:
                        conn.execute(
                            """
                            INSERT INTO cloud_samples(
                                session_id,sequence,selected_version_id,
                                first_ingested_at_us,conflict_state
                            ) VALUES(?,?,NULL,?,'single')
                            """,
                            (work.session_id, row.sequence, now),
                        )

                    existing_version = conn.execute(
                        """
                        SELECT version_id FROM cloud_sample_versions
                        WHERE session_id=? AND sequence=? AND payload_hash=?
                        """,
                        (work.session_id, row.sequence, payload_hash),
                    ).fetchone()

                    fields = row.fields
                    invalid_json = canonical_json(list(row.invalid_fields), max_bytes=16 * 1024)
                    valid_mask = ",".join(sorted(fields))
                    if existing_version is None:
                        conn.execute(
                            """
                            INSERT INTO cloud_sample_versions(
                                version_id,session_id,sequence,payload_hash,
                                sampled_at_source,sampled_at_us,timestamp_basis,
                                valid_field_mask,invalid_fields_json,
                                t1,t2,t3,t4,t5,t6,t7,t8,
                                virtual_core,virtual_surface,virtual_ambient,
                                estimated_core_temperature,prediction_set_point,
                                virtual_core_sensor,virtual_surface_sensor,virtual_ambient_sensor,
                                prediction_state,prediction_mode,prediction_type,
                                prediction_value_seconds,raw_json,first_seen_us,last_seen_us
                            ) VALUES(
                                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                            )
                            """,
                            (
                                version_id,
                                work.session_id,
                                row.sequence,
                                payload_hash,
                                row.sampled_at,
                                parse_utc_us(row.sampled_at),
                                (
                                    "vendor_sampled_at"
                                    if row.sampled_at is not None
                                    else "missing_or_invalid"
                                ),
                                valid_mask,
                                invalid_json,
                                fields.get("t1"), fields.get("t2"),
                                fields.get("t3"), fields.get("t4"),
                                fields.get("t5"), fields.get("t6"),
                                fields.get("t7"), fields.get("t8"),
                                fields.get("virtual_core"),
                                fields.get("virtual_surface"),
                                fields.get("virtual_ambient"),
                                fields.get("estimated_core_temperature"),
                                fields.get("prediction_set_point"),
                                fields.get("virtual_core_sensor"),
                                fields.get("virtual_surface_sensor"),
                                fields.get("virtual_ambient_sensor"),
                                fields.get("prediction_state"),
                                fields.get("prediction_mode"),
                                fields.get("prediction_type"),
                                fields.get("prediction_value_seconds"),
                                canonical_json(row.raw),
                                now,
                                now,
                            ),
                        )
                        newly_seen += 1
                    else:
                        conn.execute(
                            """
                            UPDATE cloud_sample_versions SET last_seen_us=?
                            WHERE version_id=?
                            """,
                            (now, existing_version[0]),
                        )

                    versions = int(
                        conn.execute(
                            """
                            SELECT COUNT(*) FROM cloud_sample_versions
                            WHERE session_id=? AND sequence=?
                            """,
                            (work.session_id, row.sequence),
                        ).fetchone()[0]
                    )
                    if versions == 1:
                        selected = conn.execute(
                            """
                            SELECT version_id FROM cloud_sample_versions
                            WHERE session_id=? AND sequence=? LIMIT 1
                            """,
                            (work.session_id, row.sequence),
                        ).fetchone()[0]
                        conn.execute(
                            """
                            UPDATE cloud_samples
                            SET selected_version_id=?,conflict_state='single'
                            WHERE session_id=? AND sequence=?
                            """,
                            (selected, work.session_id, row.sequence),
                        )
                    else:
                        conflicts += 1
                        conn.execute(
                            """
                            UPDATE cloud_samples
                            SET selected_version_id=NULL,conflict_state='conflict'
                            WHERE session_id=? AND sequence=?
                            """,
                            (work.session_id, row.sequence),
                        )

                present = [
                    int(row[0])
                    for row in conn.execute(
                        """
                        SELECT sequence FROM cloud_samples
                        WHERE session_id=? AND sequence BETWEEN ? AND ?
                        ORDER BY sequence
                        """,
                        (work.session_id, work.start_seq, work.end_seq),
                    )
                ]
                for start, end in _contiguous(present):
                    _merge_coverage(conn, work.session_id, start, end)

                missing = _subtract_coverage(
                    work.start_seq,
                    work.end_seq,
                    _coverage_for(
                        conn, work.session_id, work.start_seq, work.end_seq
                    ),
                )
                conn.execute(
                    """
                    UPDATE gaps SET retry_state='resolved',last_seen_us=?
                    WHERE scope='cloud_sequence' AND session_id=? AND manifest_id=?
                      AND reason='source_missing' AND retry_state!='resolved'
                      AND end_seq>=? AND start_seq<=?
                    """,
                    (
                        now,
                        work.session_id,
                        work.manifest_id,
                        work.start_seq,
                        work.end_seq,
                    ),
                )
                missing_rows = 0
                for start, end in missing:
                    missing_rows += end - start + 1
                    try:
                        conn.execute(
                            """
                            INSERT INTO gaps(
                                gap_id,scope,session_id,manifest_id,start_seq,end_seq,
                                reason,retry_state,first_seen_us,last_seen_us,certainty
                            ) VALUES(?, 'cloud_sequence',?,?,?,?, 'source_missing',
                                'retry',?,?, 'observed')
                            """,
                            (
                                str(uuid.uuid4()),
                                work.session_id,
                                work.manifest_id,
                                start,
                                end,
                                now,
                                now,
                            ),
                        )
                    except sqlite3.IntegrityError:
                        conn.execute(
                            """
                            UPDATE gaps SET retry_state='retry',last_seen_us=?
                            WHERE scope='cloud_sequence' AND session_id=? AND manifest_id=?
                              AND start_seq=? AND end_seq=? AND reason='source_missing'
                            """,
                            (
                                now,
                                work.session_id,
                                work.manifest_id,
                                start,
                                end,
                            ),
                        )

                request_hash = hashlib.sha256(
                    (
                        f"{work.kind}:{work.session_id}:{work.start_seq}:{work.end_seq}:"
                        + ":".join(request_hash_values)
                    ).encode()
                ).hexdigest()
                conn.execute(
                    """
                    INSERT INTO sync_receipts(
                        receipt_id,work_id,request_start,request_end,request_hash,
                        committed_rows,conflict_rows,missing_rows,committed_at_us
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        str(uuid.uuid4()),
                        work.work_id,
                        work.start_seq,
                        work.end_seq,
                        request_hash,
                        newly_seen,
                        conflicts,
                        missing_rows,
                        now,
                    ),
                )
                conn.execute(
                    """
                    UPDATE sync_work SET state='done',last_error=NULL,updated_at_us=?
                    WHERE work_id=?
                    """,
                    (now, work.work_id),
                )

                if missing:
                    first, end = missing[0]
                    _enqueue_work(
                        conn,
                        kind="sample" if work.kind == "sample" else "audit",
                        session_id=work.session_id,
                        manifest_id=work.manifest_id,
                        start_seq=first,
                        end_seq=min(end, first + CHUNK_SIZE - 1),
                        priority=80 if work.kind == "sample" else 210,
                        not_before_us=now + MISSING_RETRY_US,
                    )
                elif work.kind == "sample":
                    nxt = _first_missing_chunk(
                        conn, work.session_id, work.manifest_id
                    )
                    if nxt is not None:
                        _enqueue_work(
                            conn,
                            kind="sample",
                            session_id=work.session_id,
                            manifest_id=work.manifest_id,
                            start_seq=nxt[0],
                            end_seq=nxt[1],
                            priority=60,
                        )
                else:
                    nxt = _next_manifest_chunk_after(
                        conn, work.manifest_id, work.end_seq
                    )
                    if nxt is not None:
                        _enqueue_work(
                            conn,
                            kind="audit",
                            session_id=work.session_id,
                            manifest_id=work.manifest_id,
                            start_seq=nxt[0],
                            end_seq=nxt[1],
                            priority=210,
                        )
                    else:
                        conn.execute(
                            """
                            UPDATE cloud_sessions SET last_full_audit_us=?
                            WHERE session_id=?
                            """,
                            (now, work.session_id),
                        )

                self._fault("before_commit")
            self._fault("after_commit")
            return {
                "committed_rows": newly_seen,
                "conflict_rows": conflicts,
                "missing_rows": missing_rows,
            }

        return await self.database.async_write(write)

    async def async_queue_counts(self) -> Mapping[str, int]:
        """Return bounded operational work counts."""
        def read(conn: sqlite3.Connection):
            result = {"ready": 0, "running": 0, "failed": 0}
            for state, count in conn.execute(
                """
                SELECT state,COUNT(*) FROM sync_work
                WHERE state IN ('ready','running','failed')
                GROUP BY state
                """
            ):
                result[str(state)] = int(count)
            return result

        return await self.database.async_read(read)

    async def async_archive_counts(self) -> Mapping[str, int]:
        """Return semantic counts used by diagnostics/backup validation."""
        def read(conn: sqlite3.Connection):
            return {
                "devices": int(
                    conn.execute("SELECT COUNT(*) FROM source_devices").fetchone()[0]
                ),
                "sessions": int(
                    conn.execute("SELECT COUNT(*) FROM cloud_sessions").fetchone()[0]
                ),
                "manifests": int(
                    conn.execute("SELECT COUNT(*) FROM session_manifests").fetchone()[0]
                ),
                "samples": int(
                    conn.execute("SELECT COUNT(*) FROM cloud_samples").fetchone()[0]
                ),
                "versions": int(
                    conn.execute("SELECT COUNT(*) FROM cloud_sample_versions").fetchone()[0]
                ),
                "open_gaps": int(
                    conn.execute(
                        "SELECT COUNT(*) FROM gaps WHERE retry_state!='resolved'"
                    ).fetchone()[0]
                ),
            }

        return await self.database.async_read(read)

    async def async_list_sessions(
        self,
        *,
        limit: int = 50,
        before_seen_us: int | None = None,
        before_session_id: str | None = None,
    ) -> list[Mapping[str, Any]]:
        """Return a bounded admin session summary without raw private locators."""
        if not 1 <= limit <= 100:
            raise ValueError("Session query limit must be 1..100")

        def read(conn: sqlite3.Connection):
            params: list[Any] = []
            where = ""
            if before_seen_us is not None and before_session_id is not None:
                where = (
                    "WHERE (s.last_seen_us < ? OR "
                    "(s.last_seen_us = ? AND s.session_id < ?))"
                )
                params.extend([before_seen_us, before_seen_us, before_session_id])
            params.append(limit)
            rows = conn.execute(
                f"""
                SELECT
                    s.session_id,s.source_session_token,s.first_seen_us,s.last_seen_us,
                    s.identity_state,s.sample_period_ms,s.started_at_source,s.ended_at_source,
                    (SELECT COUNT(*) FROM cloud_samples c WHERE c.session_id=s.session_id),
                    (SELECT COUNT(*) FROM gaps g
                        WHERE g.session_id=s.session_id AND g.retry_state!='resolved'),
                    (SELECT MAX(revision) FROM session_manifests m
                        WHERE m.session_id=s.session_id)
                FROM cloud_sessions AS s
                {where}
                ORDER BY s.last_seen_us DESC,s.session_id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
            return [
                {
                    "session_id": str(row[0]),
                    "first_seen_us": int(row[2]),
                    "last_seen_us": int(row[3]),
                    "identity_state": str(row[4]),
                    "sample_period_ms": row[5],
                    "started_at_source": row[6],
                    "ended_at_source": row[7],
                    "sample_count": int(row[8]),
                    "open_gap_count": int(row[9]),
                    "manifest_revision": row[10],
                }
                for row in rows
            ]

        return await self.database.async_read(read)

    async def async_sample_versions(
        self, session_id: str, sequence: int
    ) -> list[Mapping[str, Any]]:
        """Expose bounded conflict metadata without returning raw payloads."""
        def read(conn: sqlite3.Connection):
            rows = conn.execute(
                """
                SELECT version_id,payload_hash,sampled_at_source,timestamp_basis,
                    valid_field_mask,invalid_fields_json,first_seen_us,last_seen_us
                FROM cloud_sample_versions
                WHERE session_id=? AND sequence=?
                ORDER BY first_seen_us
                LIMIT 20
                """,
                (session_id, sequence),
            ).fetchall()
            return [
                {
                    "version_id": row[0],
                    "payload_hash": row[1],
                    "sampled_at_source": row[2],
                    "timestamp_basis": row[3],
                    "valid_field_mask": row[4],
                    "invalid_fields": row[5],
                    "first_seen_us": row[6],
                    "last_seen_us": row[7],
                }
                for row in rows
            ]

        return await self.database.async_read(read)

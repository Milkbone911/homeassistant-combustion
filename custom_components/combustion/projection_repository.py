"""Bounded source-qualified planning for S5 external Recorder statistics."""
from __future__ import annotations

import hashlib
import sqlite3
import uuid
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from .storage.database import ArchiveDatabase
from .storage.schema import canonical_json, parse_utc_us, utc_now_us

PROJECTION_ALGORITHM_VERSION = 1
HOUR_US = 60 * 60 * 1_000_000
MAX_PROJECTION_SPAN_US = 31 * 24 * HOUR_US
MAX_PROJECTION_ROWS = 1000
PROJECTABLE_FIELDS = (
    "virtual_core",
    "virtual_surface",
    "virtual_ambient",
    "t1",
    "t2",
    "t3",
    "t4",
    "t5",
    "t6",
    "t7",
    "t8",
)
_FIELD_SUFFIX = {
    "virtual_core": "core_temperature",
    "virtual_surface": "surface_temperature",
    "virtual_ambient": "ambient_temperature",
    **{f"t{index}": f"temperature_{index}" for index in range(1, 9)},
}
_PROJECTION_NS = uuid.uuid5(uuid.NAMESPACE_URL, "combustion:s5:external-statistics")


@dataclass(frozen=True, slots=True)
class ProjectionPlanRow:
    """One source-qualified hourly external statistic."""

    projection_row_id: str
    identity_link_id: str
    source_device_id: str
    statistic_id: str
    field_name: str
    hour_start_us: int
    source_revision_hash: str
    source_evidence_json: str
    min_value: float
    max_value: float
    mean_value: float
    known_duration_us: int
    sample_count: int


@dataclass(frozen=True, slots=True)
class ProjectionJob:
    """Durable projection-job summary."""

    projection_job_id: str
    state: str
    requested_start_us: int
    requested_end_us: int
    algorithm_version: int
    planned_rows: int
    queued_rows: int
    confirmed_rows: int
    failed_rows: int
    plan_summary: Mapping[str, int]
    error_category: str | None


@dataclass(frozen=True, slots=True)
class _Segment:
    session_id: str
    manifest_id: str
    manifest_revision: int
    version_id: str
    sequence: int
    start_us: int
    end_us: int
    value: float


class ProjectionPlanError(RuntimeError):
    """Projection cannot be planned without weakening source semantics."""


def external_statistic_id(canonical_serial: str, field_name: str) -> str:
    """Return the stable integration-owned external statistic ID."""
    if field_name not in _FIELD_SUFFIX:
        raise ValueError("Unsupported projection field")
    serial = canonical_serial.lower()
    return f"combustion:probe_{serial}_{_FIELD_SUFFIX[field_name]}"


def _subtract_coverage(
    start: int,
    end: int,
    covered: Sequence[tuple[int, int]],
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


def _range_fully_covered(
    start: int,
    end: int,
    covered: Sequence[tuple[int, int]],
) -> bool:
    return not _subtract_coverage(start, end, covered)


def _split_segment_by_hour(
    segment: _Segment,
    *,
    requested_start_us: int,
    requested_end_us: int,
) -> Iterable[tuple[int, _Segment]]:
    cursor = max(segment.start_us, requested_start_us)
    end = min(segment.end_us, requested_end_us)
    while cursor < end:
        hour_start = (cursor // HOUR_US) * HOUR_US
        part_end = min(end, hour_start + HOUR_US)
        yield hour_start, _Segment(
            session_id=segment.session_id,
            manifest_id=segment.manifest_id,
            manifest_revision=segment.manifest_revision,
            version_id=segment.version_id,
            sequence=segment.sequence,
            start_us=cursor,
            end_us=part_end,
            value=segment.value,
        )
        cursor = part_end


def _plan_rows_sync(
    conn: sqlite3.Connection,
    *,
    requested_start_us: int,
    requested_end_us: int,
    fields: tuple[str, ...],
    max_rows: int,
) -> tuple[list[ProjectionPlanRow], dict[str, int]]:
    summary = {
        "active_links": 0,
        "ambiguous_link_groups": 0,
        "qualified_sessions": 0,
        "rejected_open_sessions": 0,
        "rejected_incomplete_sessions": 0,
        "rejected_conflicted_sessions": 0,
        "rejected_timestamp_sessions": 0,
        "rejected_overlap_hours": 0,
        "unchanged_confirmed_hours": 0,
        "planned_rows": 0,
    }

    link_rows = conn.execute(
        """
        SELECT
            links.identity_link_id,
            links.source_device_id,
            links.local_source_id,
            links.canonical_serial
        FROM identity_links AS links
        JOIN local_sources AS local
          ON local.local_source_id=links.local_source_id
        WHERE links.ended_at_us IS NULL
          AND links.evidence_kind='exact_serial'
          AND local.subject_kind='probe'
        ORDER BY links.local_source_id,links.source_device_id
        """
    ).fetchall()
    summary["active_links"] = len(link_rows)

    by_local: dict[str, list[tuple[str, str, str, str]]] = defaultdict(list)
    for row in link_rows:
        by_local[str(row[2])].append(tuple(str(item) for item in row))

    segments: dict[tuple[str, str, int], list[_Segment]] = defaultdict(list)
    row_owner: dict[tuple[str, str, int], tuple[str, str]] = {}

    for local_links in by_local.values():
        if len(local_links) != 1:
            summary["ambiguous_link_groups"] += 1
            continue
        identity_link_id, source_device_id, _local_source_id, canonical_serial = (
            local_links[0]
        )

        sessions = conn.execute(
            """
            SELECT
                sessions.session_id,
                sessions.sample_period_ms,
                sessions.ended_at_source,
                manifests.manifest_id,
                manifests.revision,
                manifests.period_ms,
                manifests.validation_state
            FROM cloud_sessions AS sessions
            JOIN session_manifests AS manifests
              ON manifests.session_id=sessions.session_id
            WHERE sessions.source_device_id=?
              AND manifests.revision=(
                  SELECT MAX(newest.revision)
                  FROM session_manifests AS newest
                  WHERE newest.session_id=sessions.session_id
              )
            ORDER BY sessions.session_id
            """,
            (source_device_id,),
        ).fetchall()

        for session in sessions:
            session_id = str(session[0])
            period_ms = session[5] if session[5] is not None else session[1]
            ended_at_us = parse_utc_us(session[2])
            manifest_id = str(session[3])
            manifest_revision = int(session[4])

            if ended_at_us is None:
                summary["rejected_open_sessions"] += 1
                continue
            if (
                period_ms is None
                or int(period_ms) <= 0
                or str(session[6]) != "valid"
            ):
                summary["rejected_incomplete_sessions"] += 1
                continue
            period_us = int(period_ms) * 1000

            manifest_ranges = [
                (int(row[0]), int(row[1]))
                for row in conn.execute(
                    """
                    SELECT start_seq,end_seq
                    FROM manifest_ranges
                    WHERE manifest_id=?
                    ORDER BY start_seq
                    """,
                    (manifest_id,),
                )
            ]
            if not manifest_ranges:
                summary["rejected_incomplete_sessions"] += 1
                continue
            coverage = [
                (int(row[0]), int(row[1]))
                for row in conn.execute(
                    """
                    SELECT start_seq,end_seq
                    FROM coverage_ranges
                    WHERE session_id=? AND validity_class='identified'
                    ORDER BY start_seq
                    """,
                    (session_id,),
                )
            ]
            if any(
                not _range_fully_covered(start, end, coverage)
                for start, end in manifest_ranges
            ):
                summary["rejected_incomplete_sessions"] += 1
                continue
            open_gaps = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM gaps
                    WHERE session_id=? AND manifest_id=?
                      AND retry_state!='resolved'
                    """,
                    (session_id, manifest_id),
                ).fetchone()[0]
            )
            if open_gaps:
                summary["rejected_incomplete_sessions"] += 1
                continue

            expected = sum(end - start + 1 for start, end in manifest_ranges)
            source_rows = conn.execute(
                """
                SELECT
                    samples.sequence,
                    samples.selected_version_id,
                    versions.sampled_at_us
                FROM cloud_samples AS samples
                LEFT JOIN cloud_sample_versions AS versions
                  ON versions.version_id=samples.selected_version_id
                WHERE samples.session_id=?
                  AND EXISTS(
                      SELECT 1 FROM manifest_ranges AS ranges
                      WHERE ranges.manifest_id=?
                        AND samples.sequence BETWEEN ranges.start_seq AND ranges.end_seq
                  )
                ORDER BY samples.sequence
                """,
                (session_id, manifest_id),
            ).fetchall()
            if len(source_rows) != expected:
                summary["rejected_incomplete_sessions"] += 1
                continue
            if any(row[1] is None for row in source_rows):
                summary["rejected_conflicted_sessions"] += 1
                continue
            if any(row[2] is None for row in source_rows):
                summary["rejected_timestamp_sessions"] += 1
                continue

            timestamps = [int(row[2]) for row in source_rows]
            if any(
                later <= earlier
                for earlier, later in zip(timestamps, timestamps[1:], strict=False)
            ):
                summary["rejected_timestamp_sessions"] += 1
                continue

            summary["qualified_sessions"] += 1
            window_start = max(0, requested_start_us - period_us)
            selected = conn.execute(
                f"""
                SELECT
                    samples.sequence,
                    versions.version_id,
                    versions.sampled_at_us,
                    versions.valid_field_mask,
                    {",".join("versions." + field for field in PROJECTABLE_FIELDS)}
                FROM cloud_samples AS samples
                JOIN cloud_sample_versions AS versions
                  ON versions.version_id=samples.selected_version_id
                WHERE samples.session_id=?
                  AND versions.sampled_at_us>=?
                  AND versions.sampled_at_us<?
                ORDER BY samples.sequence
                """,
                (session_id, window_start, requested_end_us),
            ).fetchall()
            if not selected:
                continue

            for index, row in enumerate(selected):
                sample_start = int(row[2])
                if sample_start >= ended_at_us:
                    continue
                next_start = (
                    int(selected[index + 1][2])
                    if index + 1 < len(selected)
                    else sample_start + period_us
                )
                sample_end = min(
                    sample_start + period_us,
                    next_start,
                    ended_at_us,
                )
                if sample_end <= sample_start:
                    continue
                valid = {part for part in str(row[3]).split(",") if part}
                for field_index, field in enumerate(PROJECTABLE_FIELDS, start=4):
                    if field not in fields or field not in valid:
                        continue
                    value = row[field_index]
                    if value is None:
                        continue
                    statistic_id = external_statistic_id(canonical_serial, field)
                    segment = _Segment(
                        session_id=session_id,
                        manifest_id=manifest_id,
                        manifest_revision=manifest_revision,
                        version_id=str(row[1]),
                        sequence=int(row[0]),
                        start_us=sample_start,
                        end_us=sample_end,
                        value=float(value),
                    )
                    for hour_start, piece in _split_segment_by_hour(
                        segment,
                        requested_start_us=requested_start_us,
                        requested_end_us=requested_end_us,
                    ):
                        key = (statistic_id, field, hour_start)
                        segments[key].append(piece)
                        row_owner[key] = (identity_link_id, source_device_id)

    planned: list[ProjectionPlanRow] = []
    for (statistic_id, field, hour_start), pieces in sorted(segments.items()):
        ordered = sorted(
            pieces,
            key=lambda item: (item.start_us, item.end_us, item.session_id, item.sequence),
        )
        previous_end: int | None = None
        overlap = False
        for piece in ordered:
            if previous_end is not None and piece.start_us < previous_end:
                overlap = True
                break
            previous_end = piece.end_us
        if overlap:
            summary["rejected_overlap_hours"] += 1
            continue

        duration = sum(piece.end_us - piece.start_us for piece in ordered)
        if duration <= 0:
            continue
        weighted = sum(
            piece.value * (piece.end_us - piece.start_us) for piece in ordered
        )
        values = [piece.value for piece in ordered]
        evidence_by_session: dict[
            tuple[str, str, int], list[tuple[int, str]]
        ] = defaultdict(list)
        for piece in ordered:
            evidence_by_session[
                (
                    piece.session_id,
                    piece.manifest_id,
                    piece.manifest_revision,
                )
            ].append((piece.sequence, piece.version_id))
        evidence_sessions: list[dict[str, Any]] = []
        for (
            session_id,
            manifest_id,
            manifest_revision,
        ), versions in sorted(evidence_by_session.items()):
            version_material = "|".join(
                f"{sequence}:{version_id}"
                for sequence, version_id in sorted(set(versions))
            )
            evidence_sessions.append(
                {
                    "session_id": session_id,
                    "manifest_id": manifest_id,
                    "manifest_revision": manifest_revision,
                    "version_digest": hashlib.sha256(
                        version_material.encode()
                    ).hexdigest(),
                    "version_count": len(set(versions)),
                }
            )
        evidence = {
            "algorithm_version": PROJECTION_ALGORITHM_VERSION,
            "field": field,
            "hour_start_us": hour_start,
            "known_duration_us": duration,
            "sessions": evidence_sessions,
        }
        evidence_json = canonical_json(evidence, max_bytes=64 * 1024)
        revision_hash = hashlib.sha256(evidence_json.encode()).hexdigest()

        prior = conn.execute(
            """
            SELECT 1 FROM projection_rows
            WHERE statistic_id=? AND hour_start_us=?
              AND source_revision_hash=? AND algorithm_version=?
              AND state='confirmed'
            LIMIT 1
            """,
            (
                statistic_id,
                hour_start,
                revision_hash,
                PROJECTION_ALGORITHM_VERSION,
            ),
        ).fetchone()
        if prior is not None:
            summary["unchanged_confirmed_hours"] += 1
            continue

        identity_link_id, source_device_id = row_owner[
            (statistic_id, field, hour_start)
        ]
        row_id = str(
            uuid.uuid5(
                _PROJECTION_NS,
                f"{statistic_id}:{hour_start}:{revision_hash}:"
                f"{PROJECTION_ALGORITHM_VERSION}",
            )
        )
        planned.append(
            ProjectionPlanRow(
                projection_row_id=row_id,
                identity_link_id=identity_link_id,
                source_device_id=source_device_id,
                statistic_id=statistic_id,
                field_name=field,
                hour_start_us=hour_start,
                source_revision_hash=revision_hash,
                source_evidence_json=evidence_json,
                min_value=min(values),
                max_value=max(values),
                mean_value=weighted / duration,
                known_duration_us=duration,
                sample_count=len(ordered),
            )
        )
        if len(planned) > max_rows:
            raise ProjectionPlanError("Projection plan exceeds the requested row bound")

    summary["planned_rows"] = len(planned)
    return planned, summary


class ProjectionRepository:
    """Archive-backed planning/provenance for integration-owned statistics."""

    def __init__(self, database: ArchiveDatabase) -> None:
        """Bind projection state to the archive's existing owner."""
        self.database = database

    async def async_plan(
        self,
        *,
        requested_start_us: int,
        requested_end_us: int,
        fields: Sequence[str] = PROJECTABLE_FIELDS,
        max_rows: int = MAX_PROJECTION_ROWS,
    ) -> ProjectionJob:
        """Create one bounded durable dry-run plan without touching Recorder."""
        if requested_start_us < 0 or requested_end_us <= requested_start_us:
            raise ValueError("Invalid projection interval")
        if requested_start_us % HOUR_US or requested_end_us % HOUR_US:
            raise ValueError("Projection bounds must align to UTC hours")
        if requested_end_us - requested_start_us > MAX_PROJECTION_SPAN_US:
            raise ValueError("Projection interval exceeds 31 days")
        if not 1 <= max_rows <= MAX_PROJECTION_ROWS:
            raise ValueError("Invalid projection row bound")
        selected_fields = tuple(dict.fromkeys(fields))
        if not selected_fields or any(
            field not in PROJECTABLE_FIELDS for field in selected_fields
        ):
            raise ValueError("Unsupported projection field")

        rows, summary = await self.database.async_read(
            lambda conn: _plan_rows_sync(
                conn,
                requested_start_us=requested_start_us,
                requested_end_us=requested_end_us,
                fields=selected_fields,
                max_rows=max_rows,
            )
        )
        now = utc_now_us()
        job_id = str(uuid.uuid4())
        summary_json = canonical_json(summary, max_bytes=16 * 1024)

        def write(conn: sqlite3.Connection) -> ProjectionJob:
            conn.execute("BEGIN IMMEDIATE")
            try:
                active = conn.execute(
                    """
                    SELECT projection_job_id FROM projection_jobs
                    WHERE target_kind='external_statistics'
                      AND state IN ('planned','running')
                    LIMIT 1
                    """
                ).fetchone()
                if active is not None:
                    raise ProjectionPlanError("Another projection job is active")
                conn.execute(
                    """
                    INSERT INTO projection_jobs(
                        projection_job_id,target_kind,
                        requested_start_us,requested_end_us,
                        algorithm_version,state,max_rows,
                        planned_rows,queued_rows,confirmed_rows,failed_rows,
                        plan_summary_json,created_at_us,updated_at_us,error_category
                    ) VALUES(
                        ?,'external_statistics',?,?,?,'planned',?,
                        ?,0,0,0,?,?,?,NULL
                    )
                    """,
                    (
                        job_id,
                        requested_start_us,
                        requested_end_us,
                        PROJECTION_ALGORITHM_VERSION,
                        max_rows,
                        len(rows),
                        summary_json,
                        now,
                        now,
                    ),
                )
                for row in rows:
                    conn.execute(
                        """
                        INSERT INTO projection_rows(
                            projection_row_id,projection_job_id,identity_link_id,
                            source_device_id,statistic_id,field_name,hour_start_us,
                            source_revision_hash,source_evidence_json,
                            algorithm_version,unit_of_measurement,
                            min_value,max_value,mean_value,
                            known_duration_us,sample_count,state,
                            result_digest,last_error,created_at_us,updated_at_us
                        ) VALUES(
                            ?,?,?,?,?,?,?,?,?,?,'°C',?,?,?,?,?,'planned',
                            NULL,NULL,?,?
                        )
                        """,
                        (
                            row.projection_row_id,
                            job_id,
                            row.identity_link_id,
                            row.source_device_id,
                            row.statistic_id,
                            row.field_name,
                            row.hour_start_us,
                            row.source_revision_hash,
                            row.source_evidence_json,
                            PROJECTION_ALGORITHM_VERSION,
                            row.min_value,
                            row.max_value,
                            row.mean_value,
                            row.known_duration_us,
                            row.sample_count,
                            now,
                            now,
                        ),
                    )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            return ProjectionJob(
                projection_job_id=job_id,
                state="planned",
                requested_start_us=requested_start_us,
                requested_end_us=requested_end_us,
                algorithm_version=PROJECTION_ALGORITHM_VERSION,
                planned_rows=len(rows),
                queued_rows=0,
                confirmed_rows=0,
                failed_rows=0,
                plan_summary=dict(summary),
                error_category=None,
            )

        return await self.database.async_write(write)

    async def async_get_job(self, job_id: str) -> ProjectionJob | None:
        """Return one durable projection job without source identifiers."""

        def read(conn: sqlite3.Connection) -> ProjectionJob | None:
            row = conn.execute(
                """
                SELECT
                    projection_job_id,state,requested_start_us,requested_end_us,
                    algorithm_version,planned_rows,queued_rows,confirmed_rows,
                    failed_rows,plan_summary_json,error_category
                FROM projection_jobs WHERE projection_job_id=?
                """,
                (job_id,),
            ).fetchone()
            if row is None:
                return None
            import json

            return ProjectionJob(
                projection_job_id=str(row[0]),
                state=str(row[1]),
                requested_start_us=int(row[2]),
                requested_end_us=int(row[3]),
                algorithm_version=int(row[4]),
                planned_rows=int(row[5]),
                queued_rows=int(row[6]),
                confirmed_rows=int(row[7]),
                failed_rows=int(row[8]),
                plan_summary=json.loads(row[9]),
                error_category=None if row[10] is None else str(row[10]),
            )

        return await self.database.async_read(read)

    async def async_rows_for_execution(
        self,
        job_id: str,
    ) -> tuple[Mapping[str, Any], ...]:
        """Claim one planned job and return its bounded rows."""

        def write(conn: sqlite3.Connection) -> tuple[Mapping[str, Any], ...]:
            now = utc_now_us()
            conn.execute("BEGIN IMMEDIATE")
            try:
                job = conn.execute(
                    "SELECT state FROM projection_jobs WHERE projection_job_id=?",
                    (job_id,),
                ).fetchone()
                if job is None:
                    raise ProjectionPlanError("Projection job not found")
                if job[0] not in ("planned", "partial"):
                    raise ProjectionPlanError("Projection job is not executable")
                conn.execute(
                    """
                    UPDATE projection_jobs
                    SET state='running',updated_at_us=?,error_category=NULL
                    WHERE projection_job_id=?
                    """,
                    (now, job_id),
                )
                rows = conn.execute(
                    """
                    SELECT
                        projection_row_id,statistic_id,hour_start_us,
                        source_revision_hash,min_value,max_value,mean_value,
                        known_duration_us,sample_count,state
                    FROM projection_rows
                    WHERE projection_job_id=?
                      AND state IN ('planned','queued','failed')
                    ORDER BY statistic_id,hour_start_us
                    """,
                    (job_id,),
                ).fetchall()
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            return tuple(
                {
                    "projection_row_id": str(row[0]),
                    "statistic_id": str(row[1]),
                    "hour_start_us": int(row[2]),
                    "source_revision_hash": str(row[3]),
                    "min": float(row[4]),
                    "max": float(row[5]),
                    "mean": float(row[6]),
                    "known_duration_us": int(row[7]),
                    "sample_count": int(row[8]),
                    "state": str(row[9]),
                }
                for row in rows
            )

        return await self.database.async_write(write)

    async def async_mark_rows_queued(
        self,
        job_id: str,
        row_ids: Sequence[str],
    ) -> None:
        """Persist queue submission separately from Recorder confirmation."""
        if not row_ids:
            return
        now = utc_now_us()

        def write(conn: sqlite3.Connection) -> None:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executemany(
                    """
                    UPDATE projection_rows
                    SET state='queued',last_error=NULL,updated_at_us=?
                    WHERE projection_job_id=? AND projection_row_id=?
                      AND state IN ('planned','failed','queued')
                    """,
                    ((now, job_id, row_id) for row_id in row_ids),
                )
                queued = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM projection_rows
                        WHERE projection_job_id=? AND state='queued'
                        """,
                        (job_id,),
                    ).fetchone()[0]
                )
                conn.execute(
                    """
                    UPDATE projection_jobs SET queued_rows=?,updated_at_us=?
                    WHERE projection_job_id=?
                    """,
                    (queued, now, job_id),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

        await self.database.async_write(write)

    async def async_confirm_rows(
        self,
        job_id: str,
        results: Mapping[str, str],
    ) -> ProjectionJob:
        """Confirm read-back results and supersede prior owned revisions."""
        now = utc_now_us()

        def write(conn: sqlite3.Connection) -> ProjectionJob:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for row_id, digest in results.items():
                    target = conn.execute(
                        """
                        SELECT statistic_id,hour_start_us
                        FROM projection_rows
                        WHERE projection_job_id=? AND projection_row_id=?
                        """,
                        (job_id, row_id),
                    ).fetchone()
                    if target is None:
                        continue
                    conn.execute(
                        """
                        UPDATE projection_rows
                        SET state='superseded',updated_at_us=?
                        WHERE statistic_id=? AND hour_start_us=?
                          AND state='confirmed'
                          AND projection_row_id!=?
                        """,
                        (now, target[0], target[1], row_id),
                    )
                    conn.execute(
                        """
                        UPDATE projection_rows
                        SET state='confirmed',result_digest=?,
                            last_error=NULL,updated_at_us=?
                        WHERE projection_job_id=? AND projection_row_id=?
                        """,
                        (digest, now, job_id, row_id),
                    )
                confirmed = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM projection_rows
                        WHERE projection_job_id=? AND state='confirmed'
                        """,
                        (job_id,),
                    ).fetchone()[0]
                )
                failed = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM projection_rows
                        WHERE projection_job_id=? AND state='failed'
                        """,
                        (job_id,),
                    ).fetchone()[0]
                )
                total = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM projection_rows
                        WHERE projection_job_id=?
                        """,
                        (job_id,),
                    ).fetchone()[0]
                )
                state = (
                    "confirmed"
                    if confirmed == total
                    else "partial"
                    if confirmed
                    else "failed"
                )
                conn.execute(
                    """
                    UPDATE projection_jobs
                    SET state=?,confirmed_rows=?,failed_rows=?,
                        updated_at_us=?,error_category=?
                    WHERE projection_job_id=?
                    """,
                    (
                        state,
                        confirmed,
                        failed,
                        now,
                        None if state == "confirmed" else "incomplete_confirmation",
                        job_id,
                    ),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

            row = conn.execute(
                """
                SELECT
                    projection_job_id,state,requested_start_us,requested_end_us,
                    algorithm_version,planned_rows,queued_rows,confirmed_rows,
                    failed_rows,plan_summary_json,error_category
                FROM projection_jobs WHERE projection_job_id=?
                """,
                (job_id,),
            ).fetchone()
            import json

            return ProjectionJob(
                projection_job_id=str(row[0]),
                state=str(row[1]),
                requested_start_us=int(row[2]),
                requested_end_us=int(row[3]),
                algorithm_version=int(row[4]),
                planned_rows=int(row[5]),
                queued_rows=int(row[6]),
                confirmed_rows=int(row[7]),
                failed_rows=int(row[8]),
                plan_summary=json.loads(row[9]),
                error_category=None if row[10] is None else str(row[10]),
            )

        return await self.database.async_write(write)

    async def async_fail_rows(
        self,
        job_id: str,
        row_ids: Sequence[str],
        *,
        category: str,
    ) -> ProjectionJob:
        """Persist bounded execution failure without claiming confirmation."""
        now = utc_now_us()

        def write(conn: sqlite3.Connection) -> ProjectionJob:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executemany(
                    """
                    UPDATE projection_rows
                    SET state='failed',last_error=?,updated_at_us=?
                    WHERE projection_job_id=? AND projection_row_id=?
                      AND state!='confirmed'
                    """,
                    (
                        (category[:64], now, job_id, row_id)
                        for row_id in row_ids
                    ),
                )
                confirmed = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM projection_rows
                        WHERE projection_job_id=? AND state='confirmed'
                        """,
                        (job_id,),
                    ).fetchone()[0]
                )
                failed = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM projection_rows
                        WHERE projection_job_id=? AND state='failed'
                        """,
                        (job_id,),
                    ).fetchone()[0]
                )
                state = "partial" if confirmed else "failed"
                conn.execute(
                    """
                    UPDATE projection_jobs
                    SET state=?,confirmed_rows=?,failed_rows=?,
                        updated_at_us=?,error_category=?
                    WHERE projection_job_id=?
                    """,
                    (state, confirmed, failed, now, category[:64], job_id),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            row = conn.execute(
                """
                SELECT
                    projection_job_id,state,requested_start_us,requested_end_us,
                    algorithm_version,planned_rows,queued_rows,confirmed_rows,
                    failed_rows,plan_summary_json,error_category
                FROM projection_jobs WHERE projection_job_id=?
                """,
                (job_id,),
            ).fetchone()
            import json

            return ProjectionJob(
                projection_job_id=str(row[0]),
                state=str(row[1]),
                requested_start_us=int(row[2]),
                requested_end_us=int(row[3]),
                algorithm_version=int(row[4]),
                planned_rows=int(row[5]),
                queued_rows=int(row[6]),
                confirmed_rows=int(row[7]),
                failed_rows=int(row[8]),
                plan_summary=json.loads(row[9]),
                error_category=None if row[10] is None else str(row[10]),
            )

        return await self.database.async_write(write)

    async def async_recover_incomplete_jobs(self) -> int:
        """Mark abandoned running jobs partial without inventing completion."""
        now = utc_now_us()

        def write(conn: sqlite3.Connection) -> int:
            cursor = conn.execute(
                """
                UPDATE projection_jobs
                SET state='partial',updated_at_us=?,
                    error_category='interrupted'
                WHERE state='running'
                """,
                (now,),
            )
            return int(cursor.rowcount)

        return await self.database.async_write(write)

"""SQLite schema, forward migrations and bounded serialization helpers."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION = 4
MINIMUM_READER_VERSION = 4
SCHEMA_V1_MINIMUM_READER_VERSION = 1
MAX_RAW_JSON_BYTES = 256 * 1024


class ArchiveSchemaError(Exception):
    """Archive schema or semantic metadata is unsupported."""


@dataclass(frozen=True, slots=True)
class ArchiveMetadata:
    """Validated archive identity and compatibility metadata."""

    archive_id: str
    schema_version: int
    minimum_reader_version: int
    created_at_us: int
    database_generation: int
    application_fingerprint: str
    path_binding_hash: str


def utc_now_us() -> int:
    """Return signed UTC microseconds without float round trips."""
    return int(datetime.now(UTC).timestamp() * 1_000_000)


def parse_utc_us(value: str | None) -> int | None:
    """Parse a timezone-qualified source timestamp into UTC microseconds."""
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
    except (ValueError, OverflowError):
        return None
    return int(parsed.astimezone(UTC).timestamp() * 1_000_000)


def canonical_json(value: Any, *, max_bytes: int = MAX_RAW_JSON_BYTES) -> str:
    """Serialize exact bounded JSON for hashes/audit retention."""
    rendered = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    if len(rendered.encode()) > max_bytes:
        raise ArchiveSchemaError("Archive JSON value exceeds retention bound")
    return rendered


def content_hash(value: Any) -> str:
    """Hash canonical JSON while preserving null/absent distinctions."""
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


SCHEMA_V1_SQL = """
CREATE TABLE archive_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    archive_id TEXT NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
    minimum_reader_version INTEGER NOT NULL CHECK (minimum_reader_version >= 1),
    created_at_us INTEGER NOT NULL,
    database_generation INTEGER NOT NULL CHECK (database_generation >= 1),
    application_fingerprint TEXT NOT NULL,
    path_binding_hash TEXT NOT NULL
);

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    checksum TEXT NOT NULL UNIQUE,
    applied_at_us INTEGER NOT NULL,
    application_fingerprint TEXT NOT NULL
);

CREATE TABLE accounts (
    account_id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    project TEXT NOT NULL,
    subject_ref TEXT NOT NULL,
    created_at_us INTEGER NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
    UNIQUE(provider, project, subject_ref)
);

CREATE TABLE source_devices (
    source_device_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
    source_kind TEXT NOT NULL,
    provider_type INTEGER NOT NULL,
    raw_serial TEXT NOT NULL,
    source_key TEXT NOT NULL,
    provider_locator TEXT,
    first_seen_us INTEGER NOT NULL,
    last_seen_us INTEGER NOT NULL,
    UNIQUE(account_id, source_kind, source_key)
);
CREATE INDEX idx_source_devices_account ON source_devices(account_id, source_kind);

CREATE TABLE cloud_sessions (
    session_id TEXT PRIMARY KEY,
    source_device_id TEXT NOT NULL REFERENCES source_devices(source_device_id) ON DELETE RESTRICT,
    source_session_token TEXT NOT NULL,
    index_id TEXT,
    first_seen_us INTEGER NOT NULL,
    last_seen_us INTEGER NOT NULL,
    identity_state TEXT NOT NULL DEFAULT 'source_only',
    sample_period_ms INTEGER CHECK (sample_period_ms IS NULL OR sample_period_ms > 0),
    started_at_source TEXT,
    ended_at_source TEXT,
    last_full_audit_us INTEGER,
    UNIQUE(source_device_id, source_session_token)
);
CREATE INDEX idx_cloud_sessions_device_seen ON cloud_sessions(source_device_id, last_seen_us DESC);

CREATE TABLE discovery_runs (
    run_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
    link_generation INTEGER NOT NULL CHECK (link_generation >= 1),
    source_device_id TEXT REFERENCES source_devices(source_device_id) ON DELETE RESTRICT,
    started_at_us INTEGER NOT NULL,
    completed_at_us INTEGER,
    terminal_status TEXT NOT NULL,
    terminal_reason TEXT,
    snapshot_consistent INTEGER NOT NULL DEFAULT 0 CHECK (snapshot_consistent IN (0, 1))
);
CREATE INDEX idx_discovery_runs_account_time ON discovery_runs(account_id, started_at_us DESC);

CREATE TABLE discovery_pages (
    run_id TEXT NOT NULL REFERENCES discovery_runs(run_id) ON DELETE RESTRICT,
    page INTEGER NOT NULL CHECK (page >= 1),
    digest TEXT NOT NULL,
    session_count INTEGER NOT NULL CHECK (session_count >= 0),
    total_pages INTEGER CHECK (total_pages IS NULL OR total_pages >= 1),
    raw_json TEXT NOT NULL,
    PRIMARY KEY(run_id, page)
);

CREATE TABLE session_manifests (
    manifest_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES cloud_sessions(session_id) ON DELETE RESTRICT,
    revision INTEGER NOT NULL CHECK (revision >= 1),
    fetched_at_us INTEGER NOT NULL,
    range_hash TEXT NOT NULL,
    metadata_hash TEXT NOT NULL,
    started_at_source TEXT,
    period_ms INTEGER CHECK (period_ms IS NULL OR period_ms > 0),
    validation_state TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    UNIQUE(session_id, revision),
    UNIQUE(session_id, metadata_hash)
);
CREATE INDEX idx_manifests_session_revision ON session_manifests(session_id, revision DESC);

CREATE TABLE manifest_checks (
    check_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES cloud_sessions(session_id) ON DELETE RESTRICT,
    manifest_id TEXT REFERENCES session_manifests(manifest_id) ON DELETE RESTRICT,
    checked_at_us INTEGER NOT NULL,
    result TEXT NOT NULL,
    error_category TEXT
);
CREATE INDEX idx_manifest_checks_session_time ON manifest_checks(session_id, checked_at_us DESC);

CREATE TABLE manifest_ranges (
    manifest_id TEXT NOT NULL REFERENCES session_manifests(manifest_id) ON DELETE RESTRICT,
    start_seq INTEGER NOT NULL CHECK (start_seq >= 0),
    end_seq INTEGER NOT NULL CHECK (end_seq >= start_seq),
    PRIMARY KEY(manifest_id, start_seq, end_seq)
);
CREATE INDEX idx_manifest_ranges_manifest ON manifest_ranges(manifest_id, start_seq);

CREATE TABLE cloud_samples (
    session_id TEXT NOT NULL REFERENCES cloud_sessions(session_id) ON DELETE RESTRICT,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    selected_version_id TEXT,
    first_ingested_at_us INTEGER NOT NULL,
    conflict_state TEXT NOT NULL,
    PRIMARY KEY(session_id, sequence)
);

CREATE TABLE cloud_sample_versions (
    version_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    payload_hash TEXT NOT NULL,
    sampled_at_source TEXT,
    sampled_at_us INTEGER,
    timestamp_basis TEXT NOT NULL,
    valid_field_mask TEXT NOT NULL,
    invalid_fields_json TEXT NOT NULL,
    t1 REAL, t2 REAL, t3 REAL, t4 REAL, t5 REAL, t6 REAL, t7 REAL, t8 REAL,
    virtual_core REAL, virtual_surface REAL, virtual_ambient REAL,
    estimated_core_temperature REAL, prediction_set_point REAL,
    virtual_core_sensor INTEGER, virtual_surface_sensor INTEGER, virtual_ambient_sensor INTEGER,
    prediction_state INTEGER, prediction_mode INTEGER, prediction_type INTEGER,
    prediction_value_seconds INTEGER,
    raw_json TEXT NOT NULL,
    first_seen_us INTEGER NOT NULL,
    last_seen_us INTEGER NOT NULL,
    UNIQUE(session_id, sequence, payload_hash),
    FOREIGN KEY(session_id, sequence)
        REFERENCES cloud_samples(session_id, sequence) ON DELETE RESTRICT
);
CREATE INDEX idx_versions_session_sequence ON cloud_sample_versions(session_id, sequence);
CREATE INDEX idx_versions_sampled_at ON cloud_sample_versions(session_id, sampled_at_us, sequence);

CREATE TABLE sync_work (
    work_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('manifest', 'sample', 'audit')),
    session_id TEXT NOT NULL REFERENCES cloud_sessions(session_id) ON DELETE RESTRICT,
    manifest_id TEXT REFERENCES session_manifests(manifest_id) ON DELETE RESTRICT,
    start_seq INTEGER CHECK (start_seq IS NULL OR start_seq >= 0),
    end_seq INTEGER CHECK (end_seq IS NULL OR end_seq >= start_seq),
    priority INTEGER NOT NULL DEFAULT 100,
    state TEXT NOT NULL CHECK (state IN ('ready', 'running', 'done', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    not_before_us INTEGER NOT NULL,
    last_error TEXT,
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
);
CREATE UNIQUE INDEX idx_sync_active_equivalent
ON sync_work(
    kind,
    session_id,
    COALESCE(manifest_id, ''),
    COALESCE(start_seq, -1),
    COALESCE(end_seq, -1)
)
WHERE state IN ('ready', 'running');
CREATE INDEX idx_sync_ready ON sync_work(state, not_before_us, priority, created_at_us);

CREATE TABLE sync_receipts (
    receipt_id TEXT PRIMARY KEY,
    work_id TEXT NOT NULL REFERENCES sync_work(work_id) ON DELETE RESTRICT,
    request_start INTEGER,
    request_end INTEGER,
    request_hash TEXT NOT NULL,
    committed_rows INTEGER NOT NULL CHECK (committed_rows >= 0),
    conflict_rows INTEGER NOT NULL CHECK (conflict_rows >= 0),
    missing_rows INTEGER NOT NULL CHECK (missing_rows >= 0),
    committed_at_us INTEGER NOT NULL
);
CREATE INDEX idx_receipts_work ON sync_receipts(work_id, committed_at_us);

CREATE TABLE coverage_ranges (
    session_id TEXT NOT NULL REFERENCES cloud_sessions(session_id) ON DELETE RESTRICT,
    start_seq INTEGER NOT NULL CHECK (start_seq >= 0),
    end_seq INTEGER NOT NULL CHECK (end_seq >= start_seq),
    validity_class TEXT NOT NULL,
    PRIMARY KEY(session_id, start_seq, end_seq, validity_class)
);
CREATE INDEX idx_coverage_session ON coverage_ranges(session_id, start_seq, end_seq);

CREATE TABLE gaps (
    gap_id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    session_id TEXT REFERENCES cloud_sessions(session_id) ON DELETE RESTRICT,
    manifest_id TEXT REFERENCES session_manifests(manifest_id) ON DELETE RESTRICT,
    start_seq INTEGER CHECK (start_seq IS NULL OR start_seq >= 0),
    end_seq INTEGER CHECK (end_seq IS NULL OR end_seq >= start_seq),
    reason TEXT NOT NULL,
    retry_state TEXT NOT NULL,
    first_seen_us INTEGER NOT NULL,
    last_seen_us INTEGER NOT NULL,
    certainty TEXT NOT NULL,
    UNIQUE(scope, session_id, manifest_id, start_seq, end_seq, reason)
);
CREATE INDEX idx_gaps_session_state ON gaps(session_id, retry_state, start_seq);
"""

SCHEMA_V1_CHECKSUM = hashlib.sha256(SCHEMA_V1_SQL.encode()).hexdigest()


SCHEMA_V2_SQL = """
CREATE TABLE local_sources (
    local_source_id TEXT PRIMARY KEY,
    subject_kind TEXT NOT NULL CHECK (subject_kind IN ('probe', 'gauge', 'node')),
    raw_serial TEXT NOT NULL,
    first_seen_us INTEGER NOT NULL,
    last_seen_us INTEGER NOT NULL,
    UNIQUE(subject_kind, raw_serial)
);
CREATE INDEX idx_local_sources_seen
ON local_sources(subject_kind, last_seen_us DESC);

CREATE TABLE local_capture_runs (
    capture_run_id TEXT PRIMARY KEY,
    runtime_generation TEXT NOT NULL UNIQUE,
    started_at_us INTEGER NOT NULL,
    ended_at_us INTEGER,
    terminal_status TEXT NOT NULL
        CHECK (terminal_status IN ('running', 'clean', 'interrupted')),
    policy_version INTEGER NOT NULL CHECK (policy_version >= 1),
    regular_interval_ms INTEGER NOT NULL CHECK (regular_interval_ms > 0)
);
CREATE INDEX idx_local_capture_runs_started
ON local_capture_runs(started_at_us DESC);

CREATE TABLE local_observations (
    observation_id TEXT PRIMARY KEY,
    capture_run_id TEXT NOT NULL
        REFERENCES local_capture_runs(capture_run_id) ON DELETE RESTRICT,
    local_source_id TEXT NOT NULL
        REFERENCES local_sources(local_source_id) ON DELETE RESTRICT,
    event_ordinal INTEGER NOT NULL CHECK (event_ordinal >= 0),
    observation_kind TEXT NOT NULL
        CHECK (observation_kind IN ('ble', 'prediction')),
    capture_class TEXT NOT NULL
        CHECK (capture_class IN ('regular', 'transition', 'prediction')),
    received_at_us INTEGER NOT NULL,
    received_monotonic_ns INTEGER NOT NULL CHECK (received_monotonic_ns >= 0),
    upstream_time REAL,
    source_address TEXT,
    scanner_source TEXT,
    rssi REAL,
    connectable INTEGER CHECK (connectable IS NULL OR connectable IN (0, 1)),
    route_kind TEXT NOT NULL,
    mode_name TEXT,
    freshness_basis TEXT NOT NULL,
    valid_field_mask TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    t1 REAL, t2 REAL, t3 REAL, t4 REAL, t5 REAL, t6 REAL, t7 REAL, t8 REAL,
    virtual_core REAL, virtual_surface REAL, virtual_ambient REAL,
    estimated_core_temperature REAL, prediction_set_point REAL,
    prediction_state INTEGER, prediction_mode INTEGER, prediction_type INTEGER,
    prediction_value_seconds INTEGER,
    raw_json TEXT NOT NULL,
    committed_at_us INTEGER NOT NULL,
    UNIQUE(capture_run_id, event_ordinal)
);
CREATE INDEX idx_local_observations_source_time
ON local_observations(local_source_id, received_at_us, event_ordinal);
CREATE INDEX idx_local_observations_kind_time
ON local_observations(observation_kind, received_at_us);

CREATE TABLE local_capture_gaps (
    gap_id TEXT PRIMARY KEY,
    capture_run_id TEXT REFERENCES local_capture_runs(capture_run_id)
        ON DELETE RESTRICT,
    local_source_id TEXT REFERENCES local_sources(local_source_id)
        ON DELETE RESTRICT,
    scope TEXT NOT NULL,
    reason TEXT NOT NULL,
    first_lost_at_us INTEGER NOT NULL,
    last_lost_at_us INTEGER NOT NULL,
    dropped_count INTEGER CHECK (dropped_count IS NULL OR dropped_count > 0),
    certainty TEXT NOT NULL CHECK (certainty IN ('observed', 'uncertain')),
    created_at_us INTEGER NOT NULL,
    CHECK (last_lost_at_us >= first_lost_at_us)
);
CREATE INDEX idx_local_capture_gaps_time
ON local_capture_gaps(first_lost_at_us, last_lost_at_us);
"""

SCHEMA_V2_CHECKSUM = hashlib.sha256(SCHEMA_V2_SQL.encode()).hexdigest()

SCHEMA_V3_SQL = """
CREATE TABLE identity_links (
    identity_link_id TEXT PRIMARY KEY,
    source_device_id TEXT NOT NULL
        REFERENCES source_devices(source_device_id) ON DELETE RESTRICT,
    local_source_id TEXT NOT NULL
        REFERENCES local_sources(local_source_id) ON DELETE RESTRICT,
    canonical_serial TEXT NOT NULL,
    evidence_kind TEXT NOT NULL CHECK (evidence_kind IN ('exact_serial')),
    evidence_json TEXT NOT NULL,
    evidence_hash TEXT NOT NULL,
    created_at_us INTEGER NOT NULL,
    ended_at_us INTEGER,
    end_reason TEXT,
    CHECK (
        (ended_at_us IS NULL AND end_reason IS NULL)
        OR (ended_at_us IS NOT NULL AND end_reason IS NOT NULL)
    ),
    CHECK (ended_at_us IS NULL OR ended_at_us >= created_at_us)
);
CREATE UNIQUE INDEX idx_identity_links_active_source
ON identity_links(source_device_id)
WHERE ended_at_us IS NULL;
CREATE INDEX idx_identity_links_local
ON identity_links(local_source_id, created_at_us DESC);
CREATE INDEX idx_identity_links_serial
ON identity_links(canonical_serial, created_at_us DESC);
"""

SCHEMA_V3_CHECKSUM = hashlib.sha256(SCHEMA_V3_SQL.encode()).hexdigest()

SCHEMA_V4_SQL = """
CREATE TABLE projection_jobs (
    projection_job_id TEXT PRIMARY KEY,
    target_kind TEXT NOT NULL
        CHECK (target_kind IN ('external_statistics')),
    requested_start_us INTEGER NOT NULL,
    requested_end_us INTEGER NOT NULL,
    algorithm_version INTEGER NOT NULL CHECK (algorithm_version >= 1),
    state TEXT NOT NULL
        CHECK (state IN (
            'planned','running','confirmed','partial','failed','cancelled'
        )),
    max_rows INTEGER NOT NULL CHECK (max_rows > 0),
    planned_rows INTEGER NOT NULL DEFAULT 0 CHECK (planned_rows >= 0),
    queued_rows INTEGER NOT NULL DEFAULT 0 CHECK (queued_rows >= 0),
    confirmed_rows INTEGER NOT NULL DEFAULT 0 CHECK (confirmed_rows >= 0),
    failed_rows INTEGER NOT NULL DEFAULT 0 CHECK (failed_rows >= 0),
    plan_summary_json TEXT NOT NULL,
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL,
    error_category TEXT,
    CHECK (requested_end_us > requested_start_us)
);
CREATE UNIQUE INDEX idx_projection_jobs_one_active
ON projection_jobs(target_kind)
WHERE state IN ('planned','running');
CREATE INDEX idx_projection_jobs_created
ON projection_jobs(created_at_us DESC);

CREATE TABLE projection_rows (
    projection_row_id TEXT PRIMARY KEY,
    projection_job_id TEXT NOT NULL
        REFERENCES projection_jobs(projection_job_id) ON DELETE RESTRICT,
    identity_link_id TEXT NOT NULL
        REFERENCES identity_links(identity_link_id) ON DELETE RESTRICT,
    source_device_id TEXT NOT NULL
        REFERENCES source_devices(source_device_id) ON DELETE RESTRICT,
    statistic_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    hour_start_us INTEGER NOT NULL,
    source_revision_hash TEXT NOT NULL,
    source_evidence_json TEXT NOT NULL,
    algorithm_version INTEGER NOT NULL CHECK (algorithm_version >= 1),
    unit_of_measurement TEXT NOT NULL,
    min_value REAL NOT NULL,
    max_value REAL NOT NULL,
    mean_value REAL NOT NULL,
    known_duration_us INTEGER NOT NULL CHECK (known_duration_us > 0),
    sample_count INTEGER NOT NULL CHECK (sample_count > 0),
    state TEXT NOT NULL
        CHECK (state IN (
            'planned','queued','confirmed','failed','superseded'
        )),
    result_digest TEXT,
    last_error TEXT,
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL,
    UNIQUE(
        statistic_id,hour_start_us,source_revision_hash,algorithm_version
    )
);
CREATE INDEX idx_projection_rows_job_state
ON projection_rows(projection_job_id,state,hour_start_us);
CREATE INDEX idx_projection_rows_target_hour
ON projection_rows(statistic_id,hour_start_us,created_at_us DESC);
"""

SCHEMA_V4_CHECKSUM = hashlib.sha256(SCHEMA_V4_SQL.encode()).hexdigest()
_MIGRATION_CHECKSUMS = {
    1: SCHEMA_V1_CHECKSUM,
    2: SCHEMA_V2_CHECKSUM,
    3: SCHEMA_V3_CHECKSUM,
    4: SCHEMA_V4_CHECKSUM,
}


def install_schema_v1(
    conn: sqlite3.Connection,
    *,
    archive_id: str,
    application_fingerprint: str,
    path_binding_hash: str,
) -> ArchiveMetadata:
    """Install schema v1 into an empty database in one explicit transaction."""
    now = utc_now_us()
    try:
        conn.executescript("BEGIN IMMEDIATE;\n" + SCHEMA_V1_SQL)
        conn.execute(
            """
            INSERT INTO archive_meta(
                id,archive_id,schema_version,minimum_reader_version,created_at_us,
                database_generation,application_fingerprint,path_binding_hash
            ) VALUES(1,?,?,?,?,?,?,?)
            """,
            (
                archive_id,
                1,
                SCHEMA_V1_MINIMUM_READER_VERSION,
                now,
                1,
                application_fingerprint,
                path_binding_hash,
            ),
        )
        conn.execute(
            """
            INSERT INTO schema_migrations(
                version,checksum,applied_at_us,application_fingerprint
            ) VALUES(1,?,?,?)
            """,
            (SCHEMA_V1_CHECKSUM, now, application_fingerprint),
        )
        conn.execute("COMMIT")
    except sqlite3.Error as err:
        with suppress(sqlite3.Error):
            conn.execute("ROLLBACK")
        raise ArchiveSchemaError("Archive schema initialization failed") from err
    return read_metadata_compatible(conn)


def read_metadata_compatible(conn: sqlite3.Connection) -> ArchiveMetadata:
    """Read a supported current-or-older archive without mutating it."""
    try:
        row = conn.execute(
            "SELECT archive_id,schema_version,minimum_reader_version,created_at_us,"
            "database_generation,application_fingerprint,path_binding_hash "
            "FROM archive_meta WHERE id=1"
        ).fetchone()
    except sqlite3.Error as err:
        raise ArchiveSchemaError("Archive metadata unavailable") from err
    if row is None:
        raise ArchiveSchemaError("Archive metadata missing")

    metadata = ArchiveMetadata(*row)
    if metadata.schema_version < 1 or metadata.schema_version > SCHEMA_VERSION:
        raise ArchiveSchemaError("Archive schema version is unsupported")
    if metadata.minimum_reader_version > SCHEMA_VERSION:
        raise ArchiveSchemaError("Archive requires a newer reader")

    try:
        migrations = {
            int(version): str(checksum)
            for version, checksum in conn.execute(
                "SELECT version,checksum FROM schema_migrations ORDER BY version"
            ).fetchall()
        }
    except sqlite3.Error as err:
        raise ArchiveSchemaError("Archive migration ledger unavailable") from err

    expected_versions = set(range(1, metadata.schema_version + 1))
    if set(migrations) != expected_versions:
        raise ArchiveSchemaError("Archive migration ledger is inconsistent")
    for version in expected_versions:
        if migrations[version] != _MIGRATION_CHECKSUMS[version]:
            raise ArchiveSchemaError("Archive migration checksum mismatch")
    return metadata


def _apply_migration(
    conn: sqlite3.Connection,
    *,
    from_version: int,
    to_version: int,
    minimum_reader_version: int,
    sql: str,
    checksum: str,
    application_fingerprint: str,
) -> None:
    """Apply one audited forward migration transaction."""
    now = utc_now_us()
    try:
        conn.executescript("BEGIN IMMEDIATE;\n" + sql)
        conn.execute(
            """
            UPDATE archive_meta
            SET schema_version=?,
                minimum_reader_version=?,
                database_generation=database_generation+1,
                application_fingerprint=?
            WHERE id=1 AND schema_version=?
            """,
            (
                to_version,
                minimum_reader_version,
                application_fingerprint,
                from_version,
            ),
        )
        if conn.execute("SELECT changes()").fetchone()[0] != 1:
            raise ArchiveSchemaError("Archive migration metadata update failed")
        conn.execute(
            """
            INSERT INTO schema_migrations(
                version,checksum,applied_at_us,application_fingerprint
            ) VALUES(?,?,?,?)
            """,
            (to_version, checksum, now, application_fingerprint),
        )
        conn.execute("COMMIT")
    except (sqlite3.Error, ArchiveSchemaError) as err:
        with suppress(sqlite3.Error):
            conn.execute("ROLLBACK")
        if isinstance(err, ArchiveSchemaError):
            raise
        raise ArchiveSchemaError("Archive schema migration failed") from err


def migrate_schema(
    conn: sqlite3.Connection,
    *,
    application_fingerprint: str,
) -> ArchiveMetadata:
    """Forward-migrate one already identity-qualified archive to current schema."""
    metadata = read_metadata_compatible(conn)
    if metadata.schema_version == SCHEMA_VERSION:
        return metadata

    if metadata.schema_version == 1:
        _apply_migration(
            conn,
            from_version=1,
            to_version=2,
            minimum_reader_version=2,
            sql=SCHEMA_V2_SQL,
            checksum=SCHEMA_V2_CHECKSUM,
            application_fingerprint=application_fingerprint,
        )
        metadata = read_metadata_compatible(conn)

    if metadata.schema_version == 2:
        _apply_migration(
            conn,
            from_version=2,
            to_version=3,
            minimum_reader_version=3,
            sql=SCHEMA_V3_SQL,
            checksum=SCHEMA_V3_CHECKSUM,
            application_fingerprint=application_fingerprint,
        )
        metadata = read_metadata_compatible(conn)

    if metadata.schema_version == 3:
        _apply_migration(
            conn,
            from_version=3,
            to_version=4,
            minimum_reader_version=4,
            sql=SCHEMA_V4_SQL,
            checksum=SCHEMA_V4_CHECKSUM,
            application_fingerprint=application_fingerprint,
        )
        metadata = read_metadata_compatible(conn)

    if metadata.schema_version != SCHEMA_VERSION:
        raise ArchiveSchemaError("Archive schema has no supported migration path")
    return read_metadata(conn)


def read_metadata(conn: sqlite3.Connection) -> ArchiveMetadata:
    """Read and validate an archive at the current schema version."""
    metadata = read_metadata_compatible(conn)
    if metadata.schema_version != SCHEMA_VERSION:
        raise ArchiveSchemaError("Archive schema version is not current")
    if metadata.minimum_reader_version != MINIMUM_READER_VERSION:
        raise ArchiveSchemaError("Archive minimum reader version is inconsistent")
    return metadata

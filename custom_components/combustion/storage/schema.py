"""SQLite schema and bounded serialization helpers for archive schema v1."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION = 1
MINIMUM_READER_VERSION = 1
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
                SCHEMA_VERSION,
                MINIMUM_READER_VERSION,
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
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise ArchiveSchemaError("Archive schema initialization failed") from err
    return read_metadata(conn)


def read_metadata(conn: sqlite3.Connection) -> ArchiveMetadata:
    """Read and validate the singleton compatibility row."""
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
    if metadata.minimum_reader_version > SCHEMA_VERSION:
        raise ArchiveSchemaError("Archive requires a newer reader")
    if metadata.schema_version != SCHEMA_VERSION:
        raise ArchiveSchemaError("Archive schema version is unsupported")
    try:
        migration = conn.execute(
            "SELECT checksum FROM schema_migrations WHERE version=1"
        ).fetchone()
    except sqlite3.Error as err:
        raise ArchiveSchemaError("Archive migration ledger unavailable") from err
    if migration is None or migration[0] != SCHEMA_V1_CHECKSUM:
        raise ArchiveSchemaError("Archive migration checksum mismatch")
    return metadata

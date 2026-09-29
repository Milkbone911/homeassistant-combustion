"""Evidence-backed S5 cross-source probe linking."""
from __future__ import annotations

import hashlib
import sqlite3
import uuid
from dataclasses import dataclass

from .storage.database import ArchiveDatabase
from .storage.schema import canonical_json, utc_now_us

_FIVE_MINUTES_US = 5 * 60 * 1_000_000
_HOUR_US = 60 * 60 * 1_000_000
SERIAL_PARSER_POLICY_VERSION = 1
_ASCII_HEX = frozenset("0123456789abcdefABCDEF")
PROJECTABLE_TEMPERATURE_FIELDS = (
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


@dataclass(frozen=True, slots=True)
class ProjectionSource:
    """One active evidence-backed cloud-to-local probe link."""

    source_device_id: str
    local_source_id: str
    canonical_serial: str
    local_raw_serial: str


def canonical_probe_serial(value: str) -> str | None:
    """Normalize the evidenced bare-ASCII-hex 32-bit probe serial syntax."""
    value = value.strip()
    if not 1 <= len(value) <= 8:
        return None
    if any(character not in _ASCII_HEX for character in value):
        return None
    number = int(value, 16)
    if number == 0:
        return None
    return f"{number:08X}"


class SourceLinkRepository:
    """S5 source-link reads/writes using the archive's existing writer owner."""

    def __init__(self, database: ArchiveDatabase) -> None:
        """Initialize."""
        self.database = database

    async def async_reconcile(self) -> dict[str, int]:
        """Link cloud/local probes only from exact manufacturer serial evidence."""
        now = utc_now_us()

        def write(conn: sqlite3.Connection) -> dict[str, int]:
            cloud_rows = conn.execute(
                """
                SELECT source_device_id,raw_serial
                FROM source_devices
                WHERE source_kind='cloud_probe'
                ORDER BY source_device_id
                """
            ).fetchall()
            local_rows = conn.execute(
                """
                SELECT local_source_id,raw_serial
                FROM local_sources
                WHERE subject_kind='probe'
                ORDER BY local_source_id
                """
            ).fetchall()
            active_rows = {
                str(row[0]): (str(row[1]), str(row[2]))
                for row in conn.execute(
                    """
                    SELECT source_device_id,local_source_id,canonical_serial
                    FROM identity_links
                    WHERE ended_at_us IS NULL
                    """
                ).fetchall()
            }

            locals_by_serial: dict[str, list[tuple[str, str]]] = {}
            for local_source_id, raw_serial in local_rows:
                canonical = canonical_probe_serial(str(raw_serial))
                if canonical is None:
                    continue
                locals_by_serial.setdefault(canonical, []).append(
                    (str(local_source_id), str(raw_serial))
                )

            linked = 0
            unchanged = 0
            superseded = 0
            unresolved = 0
            ambiguous = 0
            conn.execute("BEGIN IMMEDIATE")
            try:
                for source_device_id, cloud_raw_serial in cloud_rows:
                    source_id = str(source_device_id)
                    canonical = canonical_probe_serial(str(cloud_raw_serial))
                    if canonical is None:
                        unresolved += 1
                        continue

                    candidates = locals_by_serial.get(canonical, [])
                    if not candidates:
                        unresolved += 1
                        continue
                    if len(candidates) != 1:
                        if source_id in active_rows:
                            conn.execute(
                                """
                                UPDATE identity_links
                                SET ended_at_us=?,end_reason='ambiguous_exact_serial'
                                WHERE source_device_id=? AND ended_at_us IS NULL
                                """,
                                (now, source_id),
                            )
                            active_rows.pop(source_id, None)
                            superseded += 1
                        ambiguous += 1
                        continue

                    local_source_id, local_raw_serial = candidates[0]
                    active = active_rows.get(source_id)
                    if active == (local_source_id, canonical):
                        unchanged += 1
                        continue

                    if active is not None:
                        conn.execute(
                            """
                            UPDATE identity_links
                            SET ended_at_us=?,end_reason='superseded_exact_serial'
                            WHERE source_device_id=? AND ended_at_us IS NULL
                            """,
                            (now, source_id),
                        )
                        superseded += 1

                    evidence = {
                        "canonical_serial": canonical,
                        "cloud_raw_serial": str(cloud_raw_serial),
                        "local_raw_serial": local_raw_serial,
                        "serial_parser_policy": SERIAL_PARSER_POLICY_VERSION,
                    }
                    evidence_json = canonical_json(evidence, max_bytes=16 * 1024)
                    conn.execute(
                        """
                        INSERT INTO identity_links(
                            identity_link_id,source_device_id,local_source_id,
                            canonical_serial,evidence_kind,evidence_json,
                            evidence_hash,created_at_us,ended_at_us,end_reason
                        ) VALUES(?,?,?,?, 'exact_serial', ?, ?, ?, NULL, NULL)
                        """,
                        (
                            str(uuid.uuid4()),
                            source_id,
                            local_source_id,
                            canonical,
                            evidence_json,
                            hashlib.sha256(evidence_json.encode()).hexdigest(),
                            now,
                        ),
                    )
                    active_rows[source_id] = (local_source_id, canonical)
                    linked += 1
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

            return {
                "cloud_probe_sources": len(cloud_rows),
                "local_probe_sources": len(local_rows),
                "linked": linked,
                "unchanged": unchanged,
                "superseded": superseded,
                "unresolved": unresolved,
                "ambiguous": ambiguous,
            }

        return await self.database.async_write(write)

    async def async_counts(self) -> dict[str, int]:
        """Return sanitized source-link counts."""

        def read(conn: sqlite3.Connection) -> dict[str, int]:
            cloud_rows = conn.execute(
                """
                SELECT source_device_id,raw_serial
                FROM source_devices
                WHERE source_kind='cloud_probe'
                """
            ).fetchall()
            local_rows = conn.execute(
                """
                SELECT local_source_id,raw_serial
                FROM local_sources
                WHERE subject_kind='probe'
                """
            ).fetchall()
            local_canonical: dict[str, int] = {}
            for _local_source_id, raw_serial in local_rows:
                canonical = canonical_probe_serial(str(raw_serial))
                if canonical is not None:
                    local_canonical[canonical] = (
                        local_canonical.get(canonical, 0) + 1
                    )

            unresolved = 0
            ambiguous = 0
            for _source_device_id, raw_serial in cloud_rows:
                canonical = canonical_probe_serial(str(raw_serial))
                matches = 0 if canonical is None else local_canonical.get(canonical, 0)
                if matches == 0:
                    unresolved += 1
                elif matches > 1:
                    ambiguous += 1

            return {
                "cloud_probe_sources": len(cloud_rows),
                "local_probe_sources": len(local_rows),
                "active_links": int(
                    conn.execute(
                        "SELECT COUNT(*) FROM identity_links "
                        "WHERE ended_at_us IS NULL"
                    ).fetchone()[0]
                ),
                "total_links": int(
                    conn.execute("SELECT COUNT(*) FROM identity_links").fetchone()[0]
                ),
                "unresolved_cloud_sources": unresolved,
                "ambiguous_cloud_sources": ambiguous,
            }

        return await self.database.async_read(read)

    async def async_projection_sources(self) -> tuple[ProjectionSource, ...]:
        """Return active exact-serial links for the statistics projection."""

        def read(conn: sqlite3.Connection) -> tuple[ProjectionSource, ...]:
            rows = conn.execute(
                """
                SELECT
                    links.source_device_id,
                    links.local_source_id,
                    links.canonical_serial,
                    local.raw_serial
                FROM identity_links AS links
                JOIN local_sources AS local
                  ON local.local_source_id=links.local_source_id
                WHERE links.ended_at_us IS NULL
                  AND links.evidence_kind='exact_serial'
                  AND local.subject_kind='probe'
                ORDER BY links.source_device_id
                """
            ).fetchall()
            return tuple(
                ProjectionSource(
                    source_device_id=str(row[0]),
                    local_source_id=str(row[1]),
                    canonical_serial=str(row[2]),
                    local_raw_serial=str(row[3]),
                )
                for row in rows
            )

        return await self.database.async_read(read)

    async def async_hourly_temperature_statistics(
        self,
        source_device_id: str,
        field: str,
        *,
        before_us: int,
    ) -> tuple[dict[str, int | float], ...]:
        """Aggregate selected cloud samples with HA-like five-minute weighting."""
        if field not in PROJECTABLE_TEMPERATURE_FIELDS:
            raise ValueError("Unsupported projected temperature field")
        if before_us < 0:
            raise ValueError("before_us must be nonnegative")

        def read(conn: sqlite3.Connection) -> tuple[dict[str, int | float], ...]:
            rows = conn.execute(
                f"""
                WITH five_minute AS (
                    SELECT
                        (versions.sampled_at_us / ?) * ? AS bucket_us,
                        MIN(versions.{field}) AS min_value,
                        MAX(versions.{field}) AS max_value,
                        AVG(versions.{field}) AS mean_value
                    FROM cloud_samples AS samples
                    JOIN cloud_sample_versions AS versions
                      ON versions.version_id=samples.selected_version_id
                    JOIN cloud_sessions AS sessions
                      ON sessions.session_id=samples.session_id
                    WHERE sessions.source_device_id=?
                      AND versions.sampled_at_us IS NOT NULL
                      AND versions.sampled_at_us < ?
                      AND versions.{field} IS NOT NULL
                      AND instr(
                          ',' || versions.valid_field_mask || ',',
                          ',' || ? || ','
                      ) > 0
                    GROUP BY bucket_us
                )
                SELECT
                    (bucket_us / ?) * ? AS hour_start_us,
                    MIN(min_value),
                    MAX(max_value),
                    AVG(mean_value),
                    COUNT(*)
                FROM five_minute
                GROUP BY hour_start_us
                ORDER BY hour_start_us
                """,
                (
                    _FIVE_MINUTES_US,
                    _FIVE_MINUTES_US,
                    source_device_id,
                    before_us,
                    field,
                    _HOUR_US,
                    _HOUR_US,
                ),
            ).fetchall()
            return tuple(
                {
                    "start_us": int(row[0]),
                    "min": float(row[1]),
                    "max": float(row[2]),
                    "mean": float(row[3]),
                    "five_minute_buckets": int(row[4]),
                }
                for row in rows
            )

        return await self.database.async_read(read)

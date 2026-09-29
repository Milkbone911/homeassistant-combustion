"""S3 durable repository, reconciliation work and crash-boundary tests."""
from __future__ import annotations

from pathlib import Path

import pytest

from custom_components.combustion.cloud.models import (
    IndexPage,
    Probe,
    SampleRow,
    SessionIndex,
    SessionMeta,
)
from custom_components.combustion.storage.database import ArchiveDatabase
from custom_components.combustion.storage.repository import (
    MANIFEST_RECHECK_US,
    MISSING_RETRY_US,
    ArchiveRepository,
    LocalCaptureGapRecord,
    LocalObservationRecord,
    local_observation_identity,
)
from custom_components.combustion.storage.schema import ArchiveSchemaError, utc_now_us


def _index(token: str = "123") -> SessionIndex:
    raw = {
        "device_session_id": int(token),
        "sample_period": 5000,
        "sequence_number_ranges": [[0, 2]],
    }
    return SessionIndex(
        source_session_token=token,
        index_id="index-a",
        serial="probe-a",
        uid=None,
        device_type=1,
        sample_period_ms=5000,
        started_at="2026-01-01T00:00:00Z",
        ended_at=None,
        advertised_ranges=((0, 2),),
        raw=raw,
    )


def _row(sequence: int, *, t1: float | None = None) -> SampleRow:
    value = float(sequence if t1 is None else t1)
    raw = {
        "sequence_number": sequence,
        "sampled_at": f"2026-01-01T00:00:{sequence:02d}Z",
        "t1": value,
    }
    return SampleRow(
        sequence=sequence,
        sampled_at=raw["sampled_at"],
        fields={"t1": value},
        invalid_fields=(),
        raw=raw,
    )


async def _database(tmp_path: Path) -> ArchiveDatabase:
    db = ArchiveDatabase(
        tmp_path / "combustion" / "archive.sqlite3",
        require_qualified_wal=False,
        application_fingerprint="test",
    )
    await db.async_start(allow_create=True)
    return db


async def _prepare_manifest_work(
    repo: ArchiveRepository,
    *,
    token: str = "123",
):
    account_id, sources = await repo.async_register_account(
        "private-subject-do-not-store-raw",
        1,
        [Probe("probe-a", "provider-locator")],
    )
    source = sources["probe-a"]
    run = await repo.async_begin_discovery(
        account_id=account_id,
        generation=1,
        source=source,
    )
    page = IndexPage(
        requested_page=1,
        returned_page=1,
        total_pages=1,
        sessions=(_index(token),),
    )
    await repo.async_record_discovery_page(
        run_id=run,
        source=source,
        page=page,
        digest="page-digest",
    )
    await repo.async_finish_discovery(
        run,
        complete=True,
        terminal_reason="declared_total_pages",
    )
    work = await repo.async_claim_work(
        account_id=account_id,
        now_us=utc_now_us() + 1,
    )
    assert work is not None
    assert work.kind == "manifest"
    return account_id, work


async def _prepare_sample_work(
    repo: ArchiveRepository,
    *,
    ranges=((0, 2),),
):
    account_id, manifest_work = await _prepare_manifest_work(repo)
    meta = SessionMeta(
        started_at="2026-01-01T00:00:00Z",
        ranges=ranges,
        raw={
            "started_at": "2026-01-01T00:00:00Z",
            "sequence_number_ranges": [list(item) for item in ranges],
        },
    )
    manifest = await repo.async_complete_manifest(manifest_work, meta)
    sample_work = await repo.async_claim_work(
        account_id=account_id,
        now_us=utc_now_us() + 1,
    )
    assert sample_work is not None
    assert sample_work.kind == "sample"
    assert sample_work.manifest_id == manifest.manifest_id
    return account_id, manifest, sample_work


@pytest.mark.asyncio
async def test_account_archive_never_stores_raw_subject_or_tokens(tmp_path: Path):
    """Archive account identity is a protected reference, never a credential dump."""
    db = await _database(tmp_path)
    repo = ArchiveRepository(db)
    subject = "private-subject-do-not-store-raw"
    await repo.async_register_account(
        subject, 1, [Probe("probe-a", "provider-locator")]
    )

    row = await db.async_read(
        lambda conn: conn.execute(
            "SELECT subject_ref FROM accounts"
        ).fetchone()
    )
    assert row is not None
    assert row[0] != subject
    assert len(row[0]) == 64

    rendered = await db.async_read(
        lambda conn: "\n".join(
            str(value)
            for table in ("accounts", "source_devices")
            for record in conn.execute(f"SELECT * FROM {table}")
            for value in record
        )
    )
    assert subject not in rendered
    assert "refresh_token" not in rendered.lower()
    assert "id_token" not in rendered.lower()
    await db.async_stop()


@pytest.mark.asyncio
async def test_partial_discovery_retains_validated_pages_and_candidates(
    tmp_path: Path,
):
    """A later-page failure does not erase earlier validated index evidence."""
    db = await _database(tmp_path)
    repo = ArchiveRepository(db)
    account_id, sources = await repo.async_register_account(
        "subject", 1, [Probe("probe-a", "locator")]
    )
    source = sources["probe-a"]
    run = await repo.async_begin_discovery(
        account_id=account_id, generation=1, source=source
    )
    await repo.async_record_discovery_page(
        run_id=run,
        source=source,
        page=IndexPage(1, 1, 2, (_index(),)),
        digest="digest-1",
    )
    await repo.async_finish_discovery(
        run,
        complete=False,
        terminal_reason="transport",
    )

    state = await db.async_read(
        lambda conn: (
            conn.execute(
                "SELECT terminal_status FROM discovery_runs WHERE run_id=?",
                (run,),
            ).fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM discovery_pages").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM cloud_sessions").fetchone()[0],
            conn.execute(
                "SELECT COUNT(*) FROM sync_work WHERE kind='manifest'"
            ).fetchone()[0],
        )
    )
    assert state == ("partial", 1, 1, 1)
    await db.async_stop()


@pytest.mark.asyncio
async def test_startup_recovery_marks_abandoned_discovery_partial(
    tmp_path: Path,
):
    """Reload recovery closes stale discovery runs without claiming completion."""
    db = await _database(tmp_path)
    repo = ArchiveRepository(db)
    account_id, sources = await repo.async_register_account(
        "subject", 1, [Probe("probe-a", "locator")]
    )
    run = await repo.async_begin_discovery(
        account_id=account_id,
        generation=1,
        source=sources["probe-a"],
    )

    assert await repo.async_recover_interrupted_work() == 0
    status = await db.async_read(
        lambda conn: conn.execute(
            """
            SELECT terminal_status,terminal_reason,completed_at_us
            FROM discovery_runs WHERE run_id=?
            """,
            (run,),
        ).fetchone()
    )
    assert status[0:2] == ("partial", "interrupted")
    assert status[2] is not None
    await db.async_stop()


@pytest.mark.asyncio
async def test_duplicate_import_is_idempotent_and_receipt_backed(tmp_path: Path):
    """Repeating committed work does not increase source-key counts."""
    db = await _database(tmp_path)
    repo = ArchiveRepository(db)
    _, _, work = await _prepare_sample_work(repo)

    first = await repo.async_commit_sample_work(
        work, [_row(0), _row(1), _row(2)]
    )
    second = await repo.async_commit_sample_work(
        work, [_row(0), _row(1), _row(2)]
    )
    counts = await repo.async_archive_counts()

    assert first["committed_rows"] == 3
    assert second == first
    assert counts["samples"] == 3
    assert counts["versions"] == 3
    assert counts["open_gaps"] == 0
    await db.async_stop()


@pytest.mark.asyncio
async def test_before_commit_failure_advances_no_progress(tmp_path: Path):
    """A fault before COMMIT leaves no rows/coverage/receipt progress."""
    db = await _database(tmp_path)

    def fault(point: str) -> None:
        if point == "before_commit":
            raise RuntimeError("synthetic pre-commit crash")

    clean = ArchiveRepository(db)
    account_id, _, work = await _prepare_sample_work(clean)
    repo = ArchiveRepository(db, fault_injector=fault)

    with pytest.raises(RuntimeError, match="pre-commit"):
        await repo.async_commit_sample_work(work, [_row(0), _row(1), _row(2)])

    state = await db.async_read(
        lambda conn: (
            conn.execute("SELECT COUNT(*) FROM cloud_samples").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM coverage_ranges").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM sync_receipts").fetchone()[0],
            conn.execute(
                "SELECT state FROM sync_work WHERE work_id=?", (work.work_id,)
            ).fetchone()[0],
        )
    )
    assert state == (0, 0, 0, "running")

    assert await clean.async_recover_interrupted_work() == 1
    retry = await clean.async_claim_work(
        account_id=account_id, now_us=utc_now_us() + 1
    )
    assert retry is not None
    await clean.async_commit_sample_work(retry, [_row(0), _row(1), _row(2)])
    assert (await clean.async_archive_counts())["samples"] == 3
    await db.async_stop()


@pytest.mark.asyncio
async def test_after_commit_lost_ack_is_safe_to_retry(tmp_path: Path):
    """A fault after COMMIT may lose the acknowledgment but not durable truth."""
    db = await _database(tmp_path)

    def fault(point: str) -> None:
        if point == "after_commit":
            raise RuntimeError("synthetic lost acknowledgment")

    clean = ArchiveRepository(db)
    _, _, work = await _prepare_sample_work(clean)
    repo = ArchiveRepository(db, fault_injector=fault)

    with pytest.raises(RuntimeError, match="lost acknowledgment"):
        await repo.async_commit_sample_work(work, [_row(0), _row(1), _row(2)])

    state = await db.async_read(
        lambda conn: (
            conn.execute("SELECT COUNT(*) FROM cloud_samples").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM sync_receipts").fetchone()[0],
            conn.execute(
                "SELECT state FROM sync_work WHERE work_id=?", (work.work_id,)
            ).fetchone()[0],
        )
    )
    assert state == (3, 1, "done")

    receipt = await clean.async_commit_sample_work(
        work, [_row(0), _row(1), _row(2)]
    )
    assert receipt["committed_rows"] == 3
    assert (await clean.async_archive_counts())["samples"] == 3
    await db.async_stop()


@pytest.mark.asyncio
async def test_partial_response_creates_gap_and_later_fill_resolves_it(
    tmp_path: Path,
):
    """Source absence remains an explicit retryable gap until keys arrive."""
    db = await _database(tmp_path)
    repo = ArchiveRepository(db)
    account_id, _, work = await _prepare_sample_work(repo)

    result = await repo.async_commit_sample_work(work, [_row(0), _row(2)])
    assert result["missing_rows"] == 1
    assert (await repo.async_archive_counts())["open_gaps"] == 1

    retry = await repo.async_claim_work(
        account_id=account_id,
        now_us=utc_now_us() + 2 * 60 * 60 * 1_000_000,
    )
    assert retry is not None
    assert (retry.start_seq, retry.end_seq) == (1, 1)
    await repo.async_commit_sample_work(retry, [_row(1)])

    counts = await repo.async_archive_counts()
    assert counts["samples"] == 3
    assert counts["open_gaps"] == 0
    await db.async_stop()


@pytest.mark.asyncio
async def test_permanent_early_hole_does_not_starve_forward_acquisition(
    tmp_path: Path,
):
    """A retryable early hole cannot monopolize later manifest ranges."""
    db = await _database(tmp_path)
    repo = ArchiveRepository(db)
    account_id, _, first = await _prepare_sample_work(
        repo,
        ranges=((0, 1999),),
    )
    assert (first.start_seq, first.end_seq, first.attempts) == (0, 999, 1)

    def row(sequence: int) -> SampleRow:
        raw = {"sequence_number": sequence, "t1": float(sequence)}
        return SampleRow(
            sequence=sequence,
            sampled_at=None,
            fields={"t1": float(sequence)},
            invalid_fields=(),
            raw=raw,
        )

    result = await repo.async_commit_sample_work(
        first,
        [row(sequence) for sequence in range(1, 1000)],
    )
    assert result["missing_rows"] == 1

    forward = await repo.async_claim_work(
        account_id=account_id,
        now_us=utc_now_us() + 1,
    )
    assert forward is not None
    assert forward.work_id != first.work_id
    assert (forward.start_seq, forward.end_seq) == (1000, 1999)
    await repo.async_commit_sample_work(
        forward,
        [row(sequence) for sequence in range(1000, 2000)],
    )

    repair = await repo.async_claim_work(
        account_id=account_id,
        now_us=utc_now_us() + MISSING_RETRY_US + 1,
    )
    assert repair is not None
    assert repair.work_id == first.work_id
    assert (repair.start_seq, repair.end_seq, repair.attempts) == (0, 0, 2)
    await repo.async_commit_sample_work(repair, [])

    second_repair = await repo.async_claim_work(
        account_id=account_id,
        now_us=utc_now_us() + MISSING_RETRY_US + 1,
    )
    assert second_repair is not None
    assert second_repair.work_id == first.work_id
    assert second_repair.attempts == 3
    assert (second_repair.start_seq, second_repair.end_seq) == (0, 0)

    counts = await repo.async_archive_counts()
    assert counts["samples"] == 1999
    assert counts["open_gaps"] == 1
    await db.async_stop()


@pytest.mark.asyncio
async def test_changed_same_key_payload_is_versioned_not_overwritten(
    tmp_path: Path,
):
    """A later content audit retains both payloads and marks source conflict."""
    db = await _database(tmp_path)
    repo = ArchiveRepository(db)
    account_id, manifest, sample_work = await _prepare_sample_work(
        repo, ranges=((0, 0),)
    )
    await repo.async_commit_sample_work(sample_work, [_row(0, t1=20.0)])

    # Claim the scheduled manifest recheck using a future scheduling clock.
    manifest_work = await repo.async_claim_work(
        account_id=account_id,
        now_us=utc_now_us() + MANIFEST_RECHECK_US + 1,
    )
    assert manifest_work is not None
    assert manifest_work.kind == "manifest"
    await repo.async_complete_manifest(
        manifest_work,
        SessionMeta(
            "2026-01-01T00:00:00Z",
            ((0, 0),),
            {
                "started_at": "2026-01-01T00:00:00Z",
                "sequence_number_ranges": [[0, 0]],
            },
        ),
    )

    audit = await repo.async_claim_work(
        account_id=account_id,
        now_us=utc_now_us() + 1,
    )
    assert audit is not None
    assert audit.kind == "audit"
    assert audit.manifest_id == manifest.manifest_id
    await repo.async_commit_sample_work(audit, [_row(0, t1=21.5)])

    row = await db.async_read(
        lambda conn: (
            conn.execute("SELECT COUNT(*) FROM cloud_samples").fetchone()[0],
            conn.execute(
                "SELECT COUNT(*) FROM cloud_sample_versions"
            ).fetchone()[0],
            conn.execute(
                "SELECT conflict_state,selected_version_id FROM cloud_samples"
            ).fetchone(),
        )
    )
    assert row[0] == 1
    assert row[1] == 2
    assert row[2][0] == "conflict"
    assert row[2][1] is None
    await db.async_stop()


@pytest.mark.asyncio
async def test_manifest_growth_and_shrink_never_delete_committed_rows(
    tmp_path: Path,
):
    """Manifest revisions schedule new coverage but never erase old evidence."""
    db = await _database(tmp_path)
    repo = ArchiveRepository(db)
    account_id, _, sample = await _prepare_sample_work(
        repo, ranges=((0, 2),)
    )
    await repo.async_commit_sample_work(sample, [_row(0), _row(1), _row(2)])

    recheck = await repo.async_claim_work(
        account_id=account_id,
        now_us=utc_now_us() + MANIFEST_RECHECK_US + 1,
    )
    assert recheck is not None and recheck.kind == "manifest"
    grown = await repo.async_complete_manifest(
        recheck,
        SessionMeta(
            "2026-01-01T00:00:00Z",
            ((0, 5),),
            {
                "started_at": "2026-01-01T00:00:00Z",
                "sequence_number_ranges": [[0, 5]],
            },
        ),
    )
    assert grown.revision == 2

    growth_work = await repo.async_claim_work(
        account_id=account_id,
        now_us=utc_now_us() + MANIFEST_RECHECK_US + 1,
    )
    assert growth_work is not None
    assert growth_work.kind == "sample"
    assert (growth_work.start_seq, growth_work.end_seq) == (3, 5)
    await repo.async_commit_sample_work(
        growth_work, [_row(3), _row(4), _row(5)]
    )

    shrink_check = await repo.async_claim_work(
        account_id=account_id,
        now_us=utc_now_us() + 2 * MANIFEST_RECHECK_US + 2,
    )
    assert shrink_check is not None and shrink_check.kind == "manifest"
    shrunk = await repo.async_complete_manifest(
        shrink_check,
        SessionMeta(
            "2026-01-01T00:00:00Z",
            ((0, 1),),
            {
                "started_at": "2026-01-01T00:00:00Z",
                "sequence_number_ranges": [[0, 1]],
            },
        ),
    )
    assert shrunk.revision == 3

    assert (await repo.async_archive_counts())["samples"] == 6
    await db.async_stop()


@pytest.mark.asyncio
async def test_huge_manifest_schedules_only_one_bounded_chunk(tmp_path: Path):
    """Large source ranges are not expanded into whole-history memory scans."""
    db = await _database(tmp_path)
    repo = ArchiveRepository(db)
    account_id, manifest_work = await _prepare_manifest_work(repo)

    await repo.async_complete_manifest(
        manifest_work,
        SessionMeta(
            None,
            ((0, 1_000_000_000),),
            {"sequence_number_ranges": [[0, 1_000_000_000]]},
        ),
    )
    work = await repo.async_claim_work(
        account_id=account_id,
        now_us=utc_now_us() + 1,
    )
    assert work is not None
    assert work.kind == "sample"
    assert (work.start_seq, work.end_seq) == (0, 999)
    await db.async_stop()


@pytest.mark.asyncio
async def test_work_claims_are_isolated_by_account_namespace(tmp_path: Path):
    """A replacement account cannot process retained old-account jobs."""
    db = await _database(tmp_path)
    repo = ArchiveRepository(db)
    old_account, old_work = await _prepare_manifest_work(repo, token="123")

    new_account, _ = await repo.async_register_account(
        "different-subject",
        2,
        [Probe("probe-b", "other-locator")],
    )

    assert old_account != new_account
    assert (
        await repo.async_claim_work(
            account_id=new_account, now_us=utc_now_us() + 1
        )
        is None
    )
    assert old_work.account_id == old_account
    await db.async_stop()


@pytest.mark.asyncio
async def test_local_observations_are_idempotent_and_source_separated(
    tmp_path: Path,
):
    """S4 local evidence stays outside cloud source identity until S5."""
    db = await _database(tmp_path)
    repo = ArchiveRepository(db)
    runtime_generation = "test-runtime-generation"
    run_id = await repo.async_begin_local_capture(
        runtime_generation=runtime_generation,
        policy_version=1,
        regular_interval_ms=1000,
    )

    first = LocalObservationRecord(
        observation_id=local_observation_identity(runtime_generation, 0),
        capture_run_id=run_id,
        subject_kind="probe",
        raw_serial="abc123",
        event_ordinal=0,
        observation_kind="ble",
        capture_class="regular",
        received_at_us=100,
        received_monotonic_ns=1_000,
        route_kind="direct",
        freshness_basis="callback_receipt",
        valid_fields=("t1", "t2"),
        payload={"t1": 50.0, "t2": 50.0},
        source_address="AA:BB",
        scanner_source="proxy-1",
        rssi=-60,
        connectable=False,
        mode_name="normal",
    )
    second = LocalObservationRecord(
        observation_id=local_observation_identity(runtime_generation, 1),
        capture_run_id=run_id,
        subject_kind="probe",
        raw_serial="abc123",
        event_ordinal=1,
        observation_kind="ble",
        capture_class="regular",
        received_at_us=1_100_000,
        received_monotonic_ns=1_101_000,
        route_kind="meat_net",
        freshness_basis="callback_receipt",
        valid_fields=("t1", "t2"),
        payload={"t1": 50.0, "t2": 50.0},
        source_address="CC:DD",
        scanner_source="proxy-2",
        rssi=-70,
        connectable=False,
        mode_name="normal",
    )
    prediction = LocalObservationRecord(
        observation_id=local_observation_identity(runtime_generation, 2),
        capture_run_id=run_id,
        subject_kind="probe",
        raw_serial="abc123",
        event_ordinal=2,
        observation_kind="prediction",
        capture_class="prediction",
        received_at_us=1_200_000,
        received_monotonic_ns=1_201_000,
        route_kind="gatt",
        freshness_basis="gatt_status",
        valid_fields=("prediction_state", "prediction_value_seconds"),
        payload={"prediction_state": 3, "prediction_value_seconds": 900},
        mode_name="normal",
    )

    assert await repo.async_commit_local_observations(
        [first, second, prediction]
    ) == 3
    assert await repo.async_commit_local_observations(
        [first, second, prediction]
    ) == 0

    counts = await repo.async_local_capture_counts()
    assert counts == {
        "sources": 1,
        "runs": 1,
        "observations": 3,
        "gaps": 0,
    }
    assert await db.async_read(
        lambda conn: conn.execute(
            "SELECT COUNT(*) FROM source_devices"
        ).fetchone()[0]
    ) == 0

    rows = await db.async_read(
        lambda conn: conn.execute(
            """
            SELECT observation_kind,route_kind,t1,prediction_state
            FROM local_observations ORDER BY event_ordinal
            """
        ).fetchall()
    )
    assert rows == [
        ("ble", "direct", 50.0, None),
        ("ble", "meat_net", 50.0, None),
        ("prediction", "gatt", None, 3),
    ]

    gap = LocalCaptureGapRecord(
        gap_id="gap-1",
        capture_run_id=run_id,
        scope="regular_observation",
        reason="queue_overflow",
        first_lost_at_us=1_300_000,
        last_lost_at_us=1_500_000,
        dropped_count=4,
        certainty="observed",
        subject_kind="probe",
        raw_serial="abc123",
    )
    await repo.async_record_local_capture_gap(gap)
    assert (await repo.async_local_capture_counts())["gaps"] == 1

    await repo.async_finish_local_capture(run_id)
    terminal = await db.async_read(
        lambda conn: conn.execute(
            "SELECT terminal_status FROM local_capture_runs WHERE capture_run_id=?",
            (run_id,),
        ).fetchone()[0]
    )
    assert terminal == "clean"

    destination = db.path.parent / "backups" / "archive-s4.sqlite3"
    result = await db.async_backup(destination)
    validated = ArchiveDatabase.validate_backup_sync(
        destination,
        Path(result["manifest"]),
    )
    assert validated["counts"]["local_sources"] == 1
    assert validated["counts"]["local_capture_runs"] == 1
    assert validated["counts"]["local_observations"] == 3
    assert validated["counts"]["local_capture_gaps"] == 1
    await db.async_stop()


@pytest.mark.asyncio
async def test_local_observation_identity_conflict_is_rejected(tmp_path: Path):
    """One local event identity cannot silently bless changed source evidence."""
    db = await _database(tmp_path)
    repo = ArchiveRepository(db)
    runtime_generation = "conflict-runtime"
    run_id = await repo.async_begin_local_capture(
        runtime_generation=runtime_generation
    )
    observation_id = local_observation_identity(runtime_generation, 0)
    base = {
        "observation_id": observation_id,
        "capture_run_id": run_id,
        "subject_kind": "gauge",
        "raw_serial": "g1",
        "event_ordinal": 0,
        "observation_kind": "ble",
        "capture_class": "transition",
        "received_at_us": 100,
        "received_monotonic_ns": 1000,
        "route_kind": "self",
        "freshness_basis": "callback_receipt",
        "valid_fields": ("t1",),
        "mode_name": None,
    }
    first = LocalObservationRecord(payload={"t1": 100.0}, **base)
    changed = LocalObservationRecord(payload={"t1": 101.0}, **base)

    assert await repo.async_commit_local_observations([first]) == 1
    with pytest.raises(ArchiveSchemaError, match="identity conflict"):
        await repo.async_commit_local_observations([changed])

    assert (await repo.async_local_capture_counts())["observations"] == 1
    await db.async_stop()

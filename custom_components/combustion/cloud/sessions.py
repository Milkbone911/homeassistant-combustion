"""Pure session-range helpers; archive scheduling/checkpoints belong to S3."""
from __future__ import annotations

from collections.abc import Iterable, Iterator

from .models import (
    MAX_SIGNED_INT,
    CloudBoundsError,
    CloudSchemaError,
    exact_int,
    normalize_ranges,
)


def numeric_session_token(token: str) -> int:
    """Validate a source token only at the current numeric endpoint boundary."""
    if (
        not isinstance(token, str) or not token or len(token) > 20
        or not token.isascii() or not token.isdigit()
    ):
        raise CloudSchemaError("Session token is not a valid endpoint integer")
    return exact_int(int(token), "device_session_id")


def expected_count(ranges: tuple[tuple[int, int], ...]) -> int:
    """Count union coverage with no sequence expansion or overlap double count."""
    return sum(end - start + 1 for start, end in normalize_ranges(ranges))


def missing_intervals(
    expected: tuple[tuple[int, int], ...], present: Iterable[int],
) -> tuple[tuple[int, int], ...]:
    """Fixture-size pure helper; the S3 database uses indexed interval seeks.

    The present collection is limited so this is not used to load an entire
    cloud history into memory in the long-running HA process.
    """
    normalized = normalize_ranges(expected)
    seen: set[int] = set()
    for value in present:
        seen.add(exact_int(value, "present sequence"))
        if len(seen) > 10_000:
            raise CloudBoundsError("Too many in-memory sequence keys")
    output: list[tuple[int, int]] = []
    for start, end in normalized:
        current = start
        for value in sorted(seq for seq in seen if start <= seq <= end):
            if value > current:
                output.append((current, value - 1))
            current = value + 1
        if current <= end:
            output.append((current, end))
    return tuple(output)


def chunk_intervals(
    intervals: tuple[tuple[int, int], ...], *, size: int = 1000,
) -> Iterator[tuple[int, int]]:
    """Yield bounded request chunks lazily, including very large source ranges."""
    if not 1 <= size <= 1000:
        raise CloudBoundsError("Invalid chunk size")
    for start, end in normalize_ranges(intervals):
        exact_int(start, "range start")
        exact_int(end, "range end")
        if end > MAX_SIGNED_INT or end < start:
            raise CloudSchemaError("Invalid range")
        while start <= end:
            last = min(end, start + size - 1)
            yield start, last
            start = last + 1

"""Strict, bounded models for the unofficial Combustion cloud contract.

CPT-Crawl is protocol evidence, not a correctness oracle. No Home Assistant
imports, filesystem activity, or network activity belong in this module.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


MAX_SIGNED_INT = 2**63 - 1
MAX_JSON_DEPTH = 32
MAX_JSON_NODES = 50_000
TEMPERATURE_FIELDS = (
    "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8",
    "virtual_core", "virtual_surface", "virtual_ambient",
    "estimated_core_temperature", "prediction_set_point",
)
PREDICTION_FIELDS = (
    "virtual_core_sensor", "virtual_surface_sensor",
    "virtual_ambient_sensor", "prediction_state", "prediction_mode",
    "prediction_type", "prediction_value_seconds",
)


class CloudError(Exception):
    """Base typed error; messages must never contain tokens, URLs or bodies."""


class CloudSchemaError(CloudError):
    """An endpoint response violates the observed contract."""


class CloudBoundsError(CloudError):
    """A request/response exceeds a declared resource bound."""


class CloudAuthError(CloudError):
    """The current account needs explicit credential recovery."""


class CloudPermissionError(CloudError):
    """Provider denied access for reasons other than an expired token."""


class CloudTransportError(CloudError):
    """A transient HTTP or connection failure."""


class CloudRateLimitError(CloudTransportError):
    """Provider rate limit; retry was exhausted."""


class CloudUnavailableError(CloudTransportError):
    """Provider unavailable after bounded retry."""


class CloudConflictError(CloudSchemaError):
    """Source identity, pagination or sequence data conflict."""


def _pairs_unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CloudSchemaError("Duplicate JSON object key")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise CloudSchemaError("Non-finite JSON number")


def strict_json(raw: bytes | str) -> Any:
    """Parse JSON without silent duplicate-key or NaN/Infinity coercion."""
    try:
        result = json.loads(
            raw, object_pairs_hook=_pairs_unique, parse_constant=_reject_constant,
        )
    except (ValueError, UnicodeError, RecursionError):
        raise CloudSchemaError("Invalid JSON response") from None
    nodes = 0

    def visit(value: Any, depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            raise CloudBoundsError("JSON structure exceeds limits")
        if isinstance(value, dict):
            for child in value.values():
                visit(child, depth + 1)
        elif isinstance(value, list):
            for child in value:
                visit(child, depth + 1)
        elif isinstance(value, float) and not math.isfinite(value):
            raise CloudSchemaError("Non-finite JSON number")

    visit(result, 0)
    return result


def object_value(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise CloudSchemaError(f"{field} must be an object")
    return value


def required_str(source: Mapping[str, Any], field: str) -> str:
    value = source.get(field)
    if not isinstance(value, str) or not value.strip():
        raise CloudSchemaError(f"Missing or invalid {field}")
    if len(value) > 2048:
        raise CloudBoundsError(f"{field} exceeds maximum length")
    return value


def optional_str(source: Mapping[str, Any], field: str) -> str | None:
    if field not in source or source[field] is None:
        return None
    return required_str(source, field)


def exact_int(value: Any, field: str, *, allow_string: bool = False, minimum: int = 0) -> int:
    if allow_string and isinstance(value, str):
        if not value or len(value) > 20 or (
            not value.isascii() or
            (value[0] == "-" and not value[1:].isdigit()) or
            (value[0] != "-" and not value.isdigit())
        ):
            raise CloudSchemaError(f"Invalid integer field {field}")
        value = int(value)
    if type(value) is not int or not minimum <= value <= MAX_SIGNED_INT:
        raise CloudSchemaError(f"Invalid integer field {field}")
    return value


def optional_int(source: Mapping[str, Any], field: str, *, minimum: int = 0) -> int | None:
    if field not in source or source[field] is None:
        return None
    return exact_int(source[field], field, minimum=minimum)


def optional_time(source: Mapping[str, Any], field: str) -> str | None:
    value = optional_str(source, field)
    if value is None:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError("Naive timestamp")
    except (ValueError, OverflowError):
        raise CloudSchemaError(f"Invalid timestamp field {field}") from None
    return value


def _ranges(value: Any, field: str) -> tuple[tuple[int, int], ...]:
    if not isinstance(value, list) or len(value) > 10_000:
        raise CloudSchemaError(f"Invalid {field} array")
    result: list[tuple[int, int]] = []
    for pair in value:
        if not isinstance(pair, list) or len(pair) != 2:
            raise CloudSchemaError(f"Invalid {field} pair")
        start = exact_int(pair[0], field)
        end = exact_int(pair[1], field)
        if start > end:
            raise CloudSchemaError(f"Reversed {field} range")
        result.append((start, end))
    return tuple(result)


def normalize_ranges(ranges: tuple[tuple[int, int], ...]) -> tuple[tuple[int, int], ...]:
    """Compute an inclusive union without expanding the sequence numbers."""
    merged: list[tuple[int, int]] = []
    for start, end in sorted(ranges):
        if merged and start <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    return tuple(merged)


def bounded_chunks(
    ranges: tuple[tuple[int, int], ...], *, size: int = 1000,
) -> tuple[tuple[int, int], ...]:
    if not 1 <= size <= 1000:
        raise CloudBoundsError("Invalid chunk size")
    # Callers pass one bounded work unit during S3. Avoid materializing
    # an entire large session's chunk list inside the long-running client.
    result: list[tuple[int, int]] = []
    for first, last in normalize_ranges(ranges):
        while first <= last:
            if len(result) >= 10_000:
                raise CloudBoundsError("Too many chunks in one request plan")
            end = min(last, first + size - 1)
            result.append((first, end))
            first = end + 1
    return tuple(result)


@dataclass(frozen=True, slots=True)
class Probe:
    serial: str
    device_key: str


@dataclass(frozen=True, slots=True)
class ProbeStatus:
    session_id: int
    sample_period_ms: int


@dataclass(frozen=True, slots=True)
class SessionIndex:
    source_session_token: str
    index_id: str | None
    serial: str | None
    uid: str | None
    device_type: int | None
    sample_period_ms: int | None
    started_at: str | None
    ended_at: str | None
    advertised_ranges: tuple[tuple[int, int], ...] | None
    raw: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class IndexPage:
    requested_page: int
    returned_page: int | None
    total_pages: int | None
    sessions: tuple[SessionIndex, ...]


@dataclass(frozen=True, slots=True)
class SessionMeta:
    started_at: str | None
    ranges: tuple[tuple[int, int], ...]
    raw: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class SampleRow:
    sequence: int
    sampled_at: str | None
    fields: Mapping[str, int | float | None]
    invalid_fields: tuple[str, ...]
    raw: Mapping[str, Any]


def parse_session_index(item: Any) -> SessionIndex:
    source = object_value(item, "session")
    raw_token = source.get("device_session_id")
    # Source tokens are opaque strings. JSON integer tokens are retained exactly.
    if type(raw_token) is int:
        raw_token = str(exact_int(raw_token, "device_session_id"))
    if not isinstance(raw_token, str) or not raw_token.strip() or len(raw_token) > 64:
        raise CloudSchemaError("Invalid device_session_id")
    advertised = (
        None if "sequence_number_ranges" not in source
        else _ranges(source["sequence_number_ranges"], "sequence_number_ranges")
    )
    return SessionIndex(
        raw_token, optional_str(source, "id"),
        optional_str(source, "device_serial_number"),
        optional_str(source, "uid"),
        optional_int(source, "device_type"),
        optional_int(source, "sample_period", minimum=1),
        optional_time(source, "started_at"), optional_time(source, "ended_at"),
        advertised, dict(source),
    )


def parse_index_page(data: Any, *, page: int, page_size: int = 100) -> IndexPage:
    source = object_value(data, "session index")
    items = source.get("sessions")
    if not isinstance(items, list) or len(items) > page_size:
        raise CloudSchemaError("Invalid session index page")
    returned = optional_int(source, "page", minimum=1)
    total = optional_int(source, "total_pages", minimum=1)
    if returned is not None and returned != page:
        raise CloudConflictError("Session index page mismatch")
    returned_size = optional_int(source, "page_size", minimum=1)
    if returned_size is not None and returned_size != page_size:
        raise CloudConflictError("Session index page size mismatch")
    if total is None and len(items) == page_size:
        raise CloudConflictError("Full index page without total_pages")
    if total is not None and total < page:
        raise CloudConflictError("Invalid session index page total")
    return IndexPage(page, returned, total, tuple(parse_session_index(item) for item in items))


def parse_session_meta(data: Any) -> SessionMeta:
    source = object_value(data, "session metadata")
    if "sequence_number_ranges" not in source:
        raise CloudSchemaError("Missing sequence_number_ranges")
    return SessionMeta(
        optional_time(source, "started_at"),
        _ranges(source["sequence_number_ranges"], "sequence_number_ranges"),
        dict(source),
    )


def parse_sample_row(data: Any) -> SampleRow:
    source = object_value(data, "sample row")
    if "sequence_number" not in source:
        raise CloudSchemaError("Missing sequence_number")
    seq = exact_int(source["sequence_number"], "sequence_number")
    fields: dict[str, int | float | None] = {}
    invalid: list[str] = []
    for name in TEMPERATURE_FIELDS + PREDICTION_FIELDS:
        if name not in source:
            continue
        value = source[name]
        if value is None:
            fields[name] = None
        elif name in PREDICTION_FIELDS:
            try:
                fields[name] = exact_int(value, name)
            except CloudSchemaError:
                invalid.append(name)
        elif type(value) in (float, int) and -1e308 < value < 1e308:
            fields[name] = value
        else:
            invalid.append(name)
    try:
        timestamp = optional_time(source, "sampled_at")
    except CloudSchemaError:
        timestamp = None
        invalid.append("sampled_at")
    if "sampled_at" not in source:
        invalid.append("sampled_at")
    return SampleRow(seq, timestamp, fields, tuple(invalid), dict(source))


def parse_sample_chunk(
    data: Any, *, start: int, end: int,
) -> tuple[SampleRow, ...]:
    exact_int(start, "start")
    exact_int(end, "end")
    if end < start or end - start >= 1000:
        raise CloudBoundsError("Sample request exceeds one bounded chunk")
    source = object_value(data, "sample chunk")
    returned_page = optional_int(source, "page", minimum=1)
    if returned_page is not None and returned_page != 1:
        raise CloudConflictError("Unexpected sample page")
    entries = source.get("data")
    if not isinstance(entries, dict) or len(entries) > 1000:
        raise CloudSchemaError("Invalid sample data mapping")
    rows: dict[int, SampleRow] = {}
    for item in entries.values():
        row = parse_sample_row(item)
        if not start <= row.sequence <= end:
            raise CloudConflictError("Out-of-range sample sequence")
        if row.sequence in rows:
            raise CloudConflictError("Duplicate sample sequence")
        rows[row.sequence] = row
    # A partial response remains partial: S3 owns gaps and completeness.
    return tuple(rows[key] for key in sorted(rows))

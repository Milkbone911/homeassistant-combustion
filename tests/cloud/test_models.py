"""S1 strict models: reference-compatible positives and intentionally stricter negatives."""
# ruff: noqa: D103

from __future__ import annotations

import json
import uuid

import pytest

from custom_components.combustion.cloud.firestore import (
    associated_probes,
    firestore_document,
    firestore_value,
    probe_status,
    user_document_key,
)
from custom_components.combustion.cloud.models import (
    CloudBoundsError,
    CloudConflictError,
    CloudSchemaError,
    bounded_chunks,
    exact_int,
    normalize_ranges,
    parse_index_page,
    parse_sample_chunk,
    parse_sample_row,
    parse_session_meta,
    strict_json,
)
from custom_components.combustion.cloud.sessions import (
    chunk_intervals,
    expected_count,
    missing_intervals,
    numeric_session_token,
)


def typed(value, kind="stringValue"):
    return {kind: value}


def doc(**fields):
    return {"fields": fields}


def test_uuid5_user_key_matches_independent_stdlib_algorithm():
    namespace = uuid.UUID("c6639a3c-0b0a-4dd9-8cc1-046a2da8a5f1")
    for uid in ("synthetic-account", "case-Sensitive", "ü-nicode"):
        assert user_document_key(uid) == str(uuid.uuid5(namespace, uid)).upper()
    assert user_document_key("User") != user_document_key("user")


@pytest.mark.parametrize("value", ["", None, 123])
def test_invalid_firebase_subject(value):
    with pytest.raises(CloudSchemaError):
        user_document_key(value)


def test_firestore_exact_integer_and_nested_values():
    big = 9_007_199_254_740_993
    value = firestore_document(doc(
        large=typed(str(big), "integerValue"),
        negative=typed("-12", "integerValue"),
        empty=typed({"values": []}, "arrayValue"),
        obj=typed({"fields": {"ok": typed(True, "booleanValue")}}, "mapValue"),
        nil=typed("NULL_VALUE", "nullValue"),
    ))
    assert value == {
        "large": big, "negative": -12, "empty": [], "obj": {"ok": True}, "nil": None,
    }


@pytest.mark.parametrize("value", [
    typed("2.1", "integerValue"), typed("NaN", "integerValue"),
    typed("9223372036854775808", "integerValue"),
    typed(3.5, "integerValue"), typed(True, "integerValue"),
    typed(float("inf"), "doubleValue"),
    {"integerValue": "1", "stringValue": "two"},
    {}, {"arrayValue": {"values": "wrong"}},
])
def test_firestore_strict_invalid_types(value):
    with pytest.raises((CloudSchemaError, CloudBoundsError)):
        firestore_value(value)


def test_firestore_probe_associations_and_status():
    data = doc(associations=typed({"values": [
        typed({"fields": {
            "type": typed("PROBE"),
            "serial_number": typed("0ABCD123"),
            "device_key": typed("synthetic-key-A"),
        }}, "mapValue"),
        typed({"fields": {"type": typed("GAUGE")}}, "mapValue"),
    ]}, "arrayValue"))
    probes = associated_probes(data)
    assert len(probes) == 1
    assert probes[0].serial == "0ABCD123"
    assert probes[0].device_key == "synthetic-key-A"
    assert probe_status(doc(
        session_id=typed("3597489004", "integerValue"),
        sample_period=typed("5000", "integerValue"),
    )).session_id == 3597489004


@pytest.mark.parametrize("fields", [
    {},
    {"session_id": typed("1", "integerValue")},
    {"session_id": typed("1", "integerValue"), "sample_period": typed("0", "integerValue")},
    {"session_id": typed("x", "stringValue"), "sample_period": typed("5000", "integerValue")},
])
def test_missing_or_invalid_status_is_never_zero_or_empty(fields):
    with pytest.raises(CloudSchemaError):
        probe_status({"fields": fields})


@pytest.mark.parametrize("payload", [
    b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}',
    b'{"a":[1,,2]}', b'{"a": "\xff"}',
])
def test_strict_json_rejects_ambiguous_or_invalid_bytes(payload):
    with pytest.raises(CloudSchemaError):
        strict_json(payload)


def test_json_depth_limit_and_empty_known_manifest():
    with pytest.raises(CloudBoundsError):
        strict_json(json.dumps([[[0]] * 1] * 1 if False else nested(40)))
    assert parse_session_meta({"sequence_number_ranges": []}).ranges == ()


def nested(depth):
    value = 1
    for _ in range(depth):
        value = [value]
    return value


@pytest.mark.parametrize("bad", [
    {}, {"sequence_number_ranges": None},
    {"sequence_number_ranges": [[2, 1]]},
    {"sequence_number_ranges": [[True, 1]]},
    {"sequence_number_ranges": [[0, 1, 2]]},
    {"sequence_number_ranges": [[-1, 0]]},
])
def test_manifest_absent_null_wrong_or_reversed_not_observed_empty(bad):
    with pytest.raises(CloudSchemaError):
        parse_session_meta(bad)


def test_range_union_inclusive_count_and_chunk_boundaries():
    ranges = ((0, 1000), (1000, 1002), (5, 10), (2000, 2000))
    assert normalize_ranges(ranges) == ((0, 1002), (2000, 2000))
    assert expected_count(ranges) == 1004
    assert list(chunk_intervals(ranges)) == [(0, 999), (1000, 1002), (2000, 2000)]
    assert bounded_chunks(((0, 1000),)) == ((0, 999), (1000, 1000))
    assert missing_intervals(((0, 6),), [0, 2, 3, 6]) == ((1, 1), (4, 5))
    # Large ranges are lazy. No materialization of one billion sequence keys.
    iterator = chunk_intervals(((0, 10**9),))
    assert next(iterator) == (0, 999)
    assert next(iterator) == (1000, 1999)


@pytest.mark.parametrize("token", ["", "-2", "NaN", "1.0", "١", "9223372036854775808"])
def test_endpoint_numeric_session_tokens_are_validated(token):
    with pytest.raises(CloudSchemaError):
        numeric_session_token(token)
    # Retain leading-zero token in source namespace elsewhere.
    assert numeric_session_token("00042") == 42


def test_index_full_page_without_total_cannot_appear_complete():
    items = [{"device_session_id": str(i)} for i in range(100)]
    with pytest.raises(CloudConflictError):
        parse_index_page({"sessions": items}, page=1)
    assert parse_index_page({"sessions": [], "page": 1}, page=1).sessions == ()


@pytest.mark.parametrize("payload", [
    {"sessions": [], "page": 2},
    {"sessions": [], "total_pages": 0},
    {"sessions": {}, "total_pages": 1},
    {"sessions": [{"id": "missing-session-token"}]},
])
def test_invalid_index_pages(payload):
    with pytest.raises((CloudSchemaError, CloudConflictError)):
        parse_index_page(payload, page=1)


def test_optional_sample_fields_preserve_absent_null_and_bad_field():
    row = parse_sample_row({
        "sequence_number": 0, "sampled_at": "2026-01-01T00:00:00Z",
        "t1": 0, "t2": None, "t3": "not-a-number",
        "prediction_value_seconds": 131071,
    })
    assert row.fields["t1"] == 0
    assert row.fields["t2"] is None
    assert "t4" not in row.fields
    assert "t3" in row.invalid_fields
    assert row.fields["prediction_value_seconds"] == 131071
    assert row.sampled_at == "2026-01-01T00:00:00Z"


@pytest.mark.parametrize("row", [
    {}, {"sequence_number": None}, {"sequence_number": False},
    {"sequence_number": 0.0}, {"sequence_number": -1},
])
def test_sample_missing_or_invalid_sequence_fails_entire_work_unit(row):
    with pytest.raises(CloudSchemaError):
        parse_sample_row(row)


def test_sample_chunk_detects_conflict_but_missing_middle_is_explicit_partial():
    rows = parse_sample_chunk({"data": {
        "zero": {"sequence_number": 0, "sampled_at": "2026-01-01T00:00:00Z"},
        "two": {"sequence_number": 2, "sampled_at": "2026-01-01T00:00:10Z"},
    }}, start=0, end=2)
    assert [row.sequence for row in rows] == [0, 2]
    with pytest.raises(CloudConflictError):
        parse_sample_chunk({"data": {"a": {"sequence_number": 1}, "b": {"sequence_number": 1}}}, start=0, end=2)
    with pytest.raises(CloudConflictError):
        parse_sample_chunk({"data": {"a": {"sequence_number": 3}}}, start=0, end=2)
    with pytest.raises(CloudBoundsError):
        parse_sample_chunk({"data": {}}, start=0, end=1000)


def test_boolean_and_large_integers_are_not_accepted_as_sequence():
    with pytest.raises(CloudSchemaError):
        exact_int(True, "sequence")
    with pytest.raises(CloudSchemaError):
        exact_int(2**63, "sequence")

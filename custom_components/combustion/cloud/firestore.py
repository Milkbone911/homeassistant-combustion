"""Firestore REST value decoding and source-scoped Combustion probe discovery.

Fields/UUID algorithm are ported from pinned CPT-Crawl source; strict decoding
intentionally differs from its float64 and missing-value fallbacks.
"""
from __future__ import annotations

import math
import uuid
from typing import Any, Mapping

from .models import (
    CloudBoundsError, CloudSchemaError, Probe, ProbeStatus, exact_int,
    object_value, required_str,
)

USER_KEY_NAMESPACE = uuid.UUID("c6639a3c-0b0a-4dd9-8cc1-046a2da8a5f1")
VALUE_TYPES = frozenset((
    "stringValue", "integerValue", "doubleValue", "booleanValue",
    "timestampValue", "nullValue", "arrayValue", "mapValue",
))


def user_document_key(firebase_subject: str) -> str:
    if not isinstance(firebase_subject, str) or not firebase_subject or len(firebase_subject) > 2048:
        raise CloudSchemaError("Invalid Firebase subject")
    return str(uuid.uuid5(USER_KEY_NAMESPACE, firebase_subject)).upper()


def firestore_value(value: Any, *, depth: int = 0) -> Any:
    """Decode one Firestore typed value with exact 64-bit integer semantics."""
    if depth > 24:
        raise CloudBoundsError("Firestore nesting exceeds limit")
    obj = object_value(value, "Firestore value")
    keys = VALUE_TYPES.intersection(obj)
    if len(keys) != 1:
        raise CloudSchemaError("Firestore value must have exactly one known type")
    kind = next(iter(keys))
    content = obj[kind]
    if kind == "stringValue":
        if not isinstance(content, str):
            raise CloudSchemaError("Invalid Firestore string")
        return content
    if kind == "integerValue":
        # Firestore REST wraps int64 in a decimal string; do not float-convert.
        return exact_int(content, "integerValue", allow_string=True, minimum=-(2**63))
    if kind == "doubleValue":
        if type(content) not in (int, float) or not math.isfinite(content):
            raise CloudSchemaError("Invalid Firestore double")
        return float(content)
    if kind == "booleanValue":
        if type(content) is not bool:
            raise CloudSchemaError("Invalid Firestore boolean")
        return content
    if kind == "nullValue":
        if content != "NULL_VALUE":
            raise CloudSchemaError("Invalid Firestore null")
        return None
    if kind == "timestampValue":
        if not isinstance(content, str) or not content:
            raise CloudSchemaError("Invalid Firestore timestamp")
        from .models import optional_time  # keep import-time side effects absent
        return optional_time({"timestamp": content}, "timestamp")
    if kind == "arrayValue":
        nested = object_value(content, "Firestore array")
        values = nested.get("values", [])
        if not isinstance(values, list) or len(values) > 10_000:
            raise CloudBoundsError("Firestore array exceeds limit")
        return [firestore_value(item, depth=depth + 1) for item in values]
    nested = object_value(content, "Firestore map")
    fields = nested.get("fields", {})
    if not isinstance(fields, dict) or len(fields) > 10_000:
        raise CloudBoundsError("Firestore map exceeds limit")
    return {key: firestore_value(item, depth=depth + 1) for key, item in fields.items()}


def firestore_document(data: Any) -> Mapping[str, Any]:
    root = object_value(data, "Firestore document")
    fields = root.get("fields")
    if not isinstance(fields, dict) or len(fields) > 10_000:
        raise CloudSchemaError("Missing or invalid Firestore fields")
    return {key: firestore_value(value) for key, value in fields.items()}


def associated_probes(data: Any) -> tuple[Probe, ...]:
    decoded = firestore_document(data)
    associations = decoded.get("associations")
    if not isinstance(associations, list):
        raise CloudSchemaError("Missing associations array")
    probes: list[Probe] = []
    seen: dict[str, str] = {}
    for item in associations:
        if not isinstance(item, dict):
            raise CloudSchemaError("Invalid association object")
        if item.get("type") != "PROBE":
            # Unknown/gauge types are not evidence of supported cloud acquisition.
            continue
        serial = required_str(item, "serial_number")
        device_key = required_str(item, "device_key")
        previous = seen.setdefault(serial, device_key)
        if previous != device_key:
            raise CloudSchemaError("Conflicting probe association")
        if previous == device_key and not any(p.serial == serial for p in probes):
            probes.append(Probe(serial, device_key))
    return tuple(sorted(probes, key=lambda probe: probe.serial))


def probe_status(data: Any) -> ProbeStatus:
    fields = firestore_document(data)
    if "session_id" not in fields or "sample_period" not in fields:
        raise CloudSchemaError("Missing probe status fields")
    return ProbeStatus(
        exact_int(fields["session_id"], "session_id"),
        exact_int(fields["sample_period"], "sample_period", minimum=1),
    )

"""Mock-transport cloud contract; synthetic responses only, never vendor credentials."""
# ruff: noqa: D101, D102, D103, D107

from __future__ import annotations

import asyncio
import json
from urllib.parse import parse_qs, urlsplit

import pytest

from custom_components.combustion.cloud.client import (
    CombustionCloudClient, JsonTransport,
)
from custom_components.combustion.cloud.models import (
    CloudAuthError, CloudBoundsError, CloudConflictError,
    CloudRateLimitError, CloudSchemaError, CloudTransportError,
    CloudUnavailableError, Probe,
)


def wire(payload):
    return json.dumps(payload, separators=(",", ":")).encode()


class FakeContent:
    def __init__(self, payload):
        self.payload = payload

    async def iter_chunked(self, count):
        for start in range(0, len(self.payload), count):
            yield self.payload[start:start + count]


class FakeResponse:
    def __init__(self, status=200, payload=None, *, headers=None, raw=None):
        self.status = status
        body = wire(payload if payload is not None else {}) if raw is None else raw
        self.content = FakeContent(body)
        self.headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            **(headers or {}),
        }

    async def __aenter__(self):
        return self

    async def __aexit__(self, _type, _value, _traceback):
        return None


class FakeSession:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if not self.responses:
            raise AssertionError("Unexpected HTTP request")
        outcome = self.responses.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def token_response(token="synthetic-id", refresh="synthetic-refresh"):
    return {
        "user_id": "synthetic-uid", "id_token": token,
        "refresh_token": refresh, "expires_in": "3600",
    }


def probe_document():
    return {"fields": {
        "associations": {"arrayValue": {"values": [
            {"mapValue": {"fields": {
                "type": {"stringValue": "PROBE"},
                "serial_number": {"stringValue": "0A1B2C3D"},
                "device_key": {"stringValue": "synthetic-device-key"},
            }}},
        ]}},
    }}


def mk_client(session, transport=None):
    return CombustionCloudClient(
        session=session, api_key="test-api-key",
        refresh_token="synthetic-refresh",
        expected_subject="synthetic-uid", transport=transport,
    )


@pytest.mark.asyncio
async def test_probes_contract_and_fixed_https_destinations():
    session = FakeSession(
        FakeResponse(payload=token_response()),
        FakeResponse(payload=probe_document()),
    )
    result = await mk_client(session).probes()
    assert result == (Probe("0A1B2C3D", "synthetic-device-key"),)
    post, get = session.calls
    assert post[0] == "POST" and urlsplit(post[1]).hostname == "securetoken.googleapis.com"
    assert "refresh_token=synthetic-refresh" in post[2]["data"]
    assert get[0] == "GET" and urlsplit(get[1]).hostname == "firestore.googleapis.com"
    assert "Bearer synthetic-id" == get[2]["headers"]["Authorization"]
    assert get[2]["allow_redirects"] is False


@pytest.mark.asyncio
async def test_current_status_and_independent_historical_session_request():
    session = FakeSession(
        FakeResponse(payload=token_response()),
        FakeResponse(payload={"fields": {
            "session_id": {"integerValue": "3597489004"},
            "sample_period": {"integerValue": "5000"},
        }}),
        FakeResponse(payload={
            "started_at": "2026-01-01T00:00:00Z",
            "sequence_number_ranges": [[0, 0], [4, 6]],
        }),
        FakeResponse(payload={"data": {
            "first": {"sequence_number": 4, "sampled_at": "2026-01-01T00:00:20Z", "t1": 42.5},
            "second": {"sequence_number": 5, "sampled_at": "2026-01-01T00:00:25Z", "t1": 43.0},
        }}),
    )
    client = mk_client(session)
    assert (await client.status(Probe("0A1B2C3D", "synthetic-device-key"))).sample_period_ms == 5000
    assert (await client.session_meta("0A1B2C3D", 3597489004)).ranges == ((0, 0), (4, 6))
    rows = await client.sample_chunk("0A1B2C3D", 3597489004, 4, 6)
    assert [row.sequence for row in rows] == [4, 5]  # gap is NOT silent completeness
    params = parse_qs(urlsplit(session.calls[-1][1]).query)
    assert params["sequence_number_ranges"] == ["[[4,6]]"]
    assert params["page"] == ["1"]
    assert params["uid"] == ["synthetic-uid"]


@pytest.mark.asyncio
async def test_index_pagination_and_bounded_first_page_revisit():
    item = {
        "uid": "synthetic-uid", "device_serial_number": "0A1B2C3D",
        "device_type": 1, "device_session_id": "100",
    }
    first = {"page": 1, "total_pages": 2, "sessions": [item]}
    second = {"page": 2, "total_pages": 2, "sessions": [
        {**item, "device_session_id": "101"},
    ]}
    session = FakeSession(
        FakeResponse(payload=token_response()),
        FakeResponse(payload=first), FakeResponse(payload=second),
        FakeResponse(payload=first),
    )
    result = await mk_client(session).sessions(Probe("0A1B2C3D", "synthetic-device-key"))
    assert len(result.sessions) == 2
    assert result.terminal_reason == "declared_total_pages"
    assert result.snapshot_consistent is False
    assert [parse_qs(urlsplit(c[1]).query)["page"][0] for c in session.calls[1:]] == ["1", "2", "1"]


@pytest.mark.asyncio
async def test_index_mutation_does_not_claim_complete_traversal():
    item = {"device_session_id": "100"}
    session = FakeSession(
        FakeResponse(payload=token_response()),
        FakeResponse(payload={"sessions": [item]}),
        FakeResponse(payload={"sessions": [item, {"device_session_id": "101"}]}),
    )
    with pytest.raises(CloudConflictError):
        await mk_client(session).sessions(Probe("A", "key"))


@pytest.mark.asyncio
async def test_duplicate_index_token_with_disagreeing_fields_quarantines():
    item = {"device_session_id": "100"}
    session = FakeSession(
        FakeResponse(payload=token_response()),
        FakeResponse(payload={"page": 1, "total_pages": 2, "sessions": [item]}),
        FakeResponse(payload={"page": 2, "total_pages": 2, "sessions": [
            {"device_session_id": "100", "started_at": "2026-01-01T00:00:00Z"},
        ]}),
    )
    with pytest.raises(CloudConflictError):
        await mk_client(session).sessions(Probe("A", "key"))


@pytest.mark.asyncio
async def test_transport_bounded_429_retry_with_no_body_logging():
    delays = []
    async def sleep(delay):
        delays.append(delay)
    session = FakeSession(
        FakeResponse(status=429, raw=b"synthetic-secret", headers={"Retry-After": "9999"}),
        FakeResponse(payload={"ok": True}),
    )
    value = await JsonTransport(session, sleep=sleep, jitter=lambda: 0).request_json(
        "GET", "https://data-api.combustion.inc/v1/session",
        headers={"Authorization": "Bearer private-synthetic-token"},
    )
    assert value == {"ok": True}
    assert delays == [10.0]
    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_transport_never_follows_redirect_or_leaks_bearer():
    session = FakeSession(FakeResponse(status=302, headers={"Location": "https://evil.test/path"}))
    with pytest.raises(CloudSchemaError) as error:
        await JsonTransport(session).request_json(
            "GET", "https://data-api.combustion.inc/v1/session",
            headers={"Authorization": "Bearer private-synthetic-token"},
        )
    assert "private-synthetic-token" not in str(error.value)
    assert session.calls[0][2]["allow_redirects"] is False
    with pytest.raises(CloudSchemaError):
        await JsonTransport(FakeSession()).request_json("GET", "https://evil.test/private")


@pytest.mark.asyncio
async def test_transport_rejects_bad_content_type_and_oversized_decoded_body():
    session = FakeSession(FakeResponse(headers={"Content-Type": "text/html"}))
    with pytest.raises(CloudSchemaError):
        await JsonTransport(session).request_json(
            "GET", "https://data-api.combustion.inc/v1/session",
        )
    huge = b'{"padding":"' + b"A" * 80 + b'"}'
    session = FakeSession(FakeResponse(raw=huge, headers={"Content-Length": "1"}))
    with pytest.raises(CloudBoundsError):
        await JsonTransport(session, max_body=32).request_json(
            "GET", "https://data-api.combustion.inc/v1/session",
        )


@pytest.mark.asyncio
async def test_transport_auth_error_does_not_include_vendor_body():
    session = FakeSession(FakeResponse(status=401, raw=b"private-synthetic-token"))
    with pytest.raises(CloudAuthError) as error:
        await JsonTransport(session).request_json(
            "GET", "https://data-api.combustion.inc/v1/session",
        )
    assert "private-synthetic-token" not in str(error.value)


@pytest.mark.asyncio
async def test_auth_401_refreshes_once_then_retries_and_rotation_is_in_memory_only():
    session = FakeSession(
        FakeResponse(payload=token_response("id-1")),
        FakeResponse(status=401),
        FakeResponse(payload=token_response("id-2", "refresh-2")),
        FakeResponse(payload=probe_document()),
    )
    assert len(await mk_client(session).probes()) == 1
    assert [call[0] for call in session.calls] == ["POST", "GET", "POST", "GET"]
    assert session.calls[-1][2]["headers"]["Authorization"] == "Bearer id-2"


@pytest.mark.asyncio
async def test_auth_second_401_raises_and_never_loops():
    session = FakeSession(
        FakeResponse(payload=token_response("id-1")),
        FakeResponse(status=401),
        FakeResponse(payload=token_response("id-2")),
        FakeResponse(status=401),
    )
    with pytest.raises(CloudAuthError):
        await mk_client(session).probes()
    assert len(session.calls) == 4


@pytest.mark.asyncio
async def test_failed_transport_retries_are_bounded_and_cancellation_propagates():
    sleeps = []
    async def sleep(delay):
        sleeps.append(delay)
    session = FakeSession(asyncio.TimeoutError(), asyncio.TimeoutError())
    with pytest.raises(CloudTransportError):
        await JsonTransport(session, attempts=2, sleep=sleep, jitter=lambda: 0).request_json(
            "GET", "https://data-api.combustion.inc/v1/session",
        )
    assert len(sleeps) == 1
    session = FakeSession(asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await JsonTransport(session).request_json(
            "GET", "https://data-api.combustion.inc/v1/session",
        )


@pytest.mark.asyncio
async def test_permanent_rate_limit_and_5xx_classification():
    session = FakeSession(FakeResponse(status=429, raw=b"secret"))
    with pytest.raises(CloudRateLimitError):
        await JsonTransport(session, attempts=1).request_json(
            "GET", "https://data-api.combustion.inc/v1/session",
        )
    session = FakeSession(FakeResponse(status=503, raw=b"secret"))
    with pytest.raises(CloudUnavailableError):
        await JsonTransport(session, attempts=1).request_json(
            "GET", "https://data-api.combustion.inc/v1/session",
        )


@pytest.mark.asyncio
async def test_no_live_endpoints_are_used_on_module_import_or_client_construction():
    session = FakeSession()
    mk_client(session)
    assert session.calls == []

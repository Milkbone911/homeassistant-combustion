"""S1 isolated token lifecycle: no HA config persistence or vendor bootstrap."""
from __future__ import annotations

import asyncio
import pytest

from custom_components.combustion.cloud.auth import (
    TokenManager, parse_refresh_response,
)
from custom_components.combustion.cloud.models import CloudAuthError, CloudSchemaError


def fresh(*, subject="synthetic-subject", token="synthetic-id", refresh="synthetic-refresh"):
    return {
        "user_id": subject, "id_token": token,
        "refresh_token": refresh, "expires_in": "3600",
    }


@pytest.mark.parametrize("payload", [
    {}, {"user_id": "synthetic-subject", "id_token": "id"},
    {**fresh(), "expires_in": "invalid"},
    {**fresh(), "expires_in": True},
    {**fresh(), "expires_in": "0"},
])
def test_required_refresh_fields_are_not_defaulted(payload):
    with pytest.raises((CloudAuthError, CloudSchemaError)):
        parse_refresh_response(payload, None)


def test_refresh_subject_mismatch_is_fatal_and_secret_not_repr():
    with pytest.raises(CloudAuthError):
        parse_refresh_response(fresh(subject="other-subject"), "expected-subject")
    # No auth value should be represented in logged model display.
    from custom_components.combustion.cloud.auth import AuthSnapshot
    snapshot = AuthSnapshot("subject", "secret-id", "secret-refresh", 100.0, 1)
    assert "secret-id" not in repr(snapshot)
    assert "secret-refresh" not in repr(snapshot)


@pytest.mark.asyncio
async def test_single_flight_refresh_rotation_and_late_401():
    calls = []
    rotations = []

    async def refresh(_api_key, token):
        calls.append(token)
        await asyncio.sleep(0)
        return fresh(token=f"id-{len(calls)}", refresh=f"refresh-{len(calls)}")

    async def on_rotation(before, after):
        rotations.append((before, after))

    mgr = TokenManager(
        api_key="synthetic-api", refresh_token="original-secret",
        refresh=refresh, on_rotation=on_rotation,
        expected_subject="synthetic-subject",
    )
    first, second, third = await asyncio.gather(mgr.token(), mgr.token(), mgr.token())
    assert len(calls) == 1
    assert first == second == third
    assert len(rotations) == 1
    # A late unauthorized response for a superseded access token must not
    # consume the just-rotated refresh token.
    after = await mgr.refresh(failed_token="older-token")
    assert after == first
    assert len(calls) == 1
    next_snapshot = await mgr.refresh(failed_token=first.id_token)
    assert next_snapshot.generation == 2
    assert calls == ["original-secret", "refresh-1"]


@pytest.mark.asyncio
async def test_wrong_subject_refuses_rotation_and_keeps_original_generation():
    async def refresh(_key, _token):
        return fresh(subject="unrelated-account")

    mgr = TokenManager(
        api_key="synthetic-api", refresh_token="synthetic-refresh",
        refresh=refresh, expected_subject="linked-account",
    )
    with pytest.raises(CloudAuthError):
        await mgr.token()
    assert mgr.subject == "linked-account"
    assert mgr.generation == 0


@pytest.mark.asyncio
async def test_short_token_lifetime_reuses_token_until_its_proportional_margin():
    now = [1000.0]
    calls = []

    async def refresh(_key, _token):
        calls.append(1)
        return {**fresh(), "expires_in": "40"}

    mgr = TokenManager(
        api_key="key", refresh_token="refresh", refresh=refresh,
        clock=lambda: now[0],
    )
    first = await mgr.token()
    now[0] += 10
    assert await mgr.token() == first
    assert len(calls) == 1
    now[0] += 25
    assert (await mgr.token()).generation == 2


@pytest.mark.asyncio
async def test_rotation_callback_failure_surfaces_without_leaking_secret():
    async def refresh(_key, _token):
        return fresh(refresh="replacement-secret")

    async def fail(_old, _new):
        raise ValueError("replacement-secret should never appear")

    mgr = TokenManager(
        api_key="key", refresh_token="original-secret",
        refresh=refresh, on_rotation=fail,
    )
    with pytest.raises(CloudAuthError) as error:
        await mgr.token()
    assert "replacement-secret" not in str(error.value)
    assert mgr.generation == 1  # in-memory state, NOT disk durability proof

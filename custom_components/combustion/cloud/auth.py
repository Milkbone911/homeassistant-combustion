"""Single-flight Firebase refresh-token lifecycle for the unofficial API.

No login/bootstrap, config-entry persistence, or filesystem access is included
in S1. A future HA adapter owns the protected refresh-credential callback.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Mapping

from .models import CloudAuthError, CloudSchemaError, exact_int, required_str

RefreshRequest = Callable[[str, str], Awaitable[Mapping[str, Any]]]
RotationHook = Callable[[str, str], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class AuthSnapshot:
    """An in-memory verified identity and token generation."""

    subject: str
    id_token: str = field(repr=False)
    refresh_token: str = field(repr=False)
    expiry_monotonic: float
    generation: int


def parse_refresh_response(data: Mapping[str, Any], prior_subject: str | None) -> tuple[str, str, str, int]:
    """Reject missing token fields and cross-account refresh."""
    subject = required_str(data, "user_id")
    if prior_subject is not None and subject != prior_subject:
        raise CloudAuthError("Authenticated subject does not match linked account")
    id_token = required_str(data, "id_token")
    refresh_token = required_str(data, "refresh_token")
    # Firebase REST returns a decimal string. Missing/invalid expiry must
    # not inherit CPT-Crawl's unqualified default of 3600 seconds.
    expires = exact_int(data.get("expires_in"), "expires_in", allow_string=True, minimum=1)
    if expires > 86_400:
        raise CloudSchemaError("Token lifetime exceeds configured bound")
    return subject, id_token, refresh_token, expires


class TokenManager:
    """Account-bound in-memory tokens; no HA reload or durable-save claims."""

    def __init__(
        self,
        *, api_key: str, refresh_token: str, refresh: RefreshRequest,
        expected_subject: str | None = None,
        on_rotation: RotationHook | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not api_key or not refresh_token or len(api_key) > 2048 or len(refresh_token) > 8192:
            raise CloudAuthError("Missing or oversized cloud credential")
        self._api_key = api_key
        self._refresh_token = refresh_token
        self._subject = expected_subject
        self._refresh = refresh
        self._on_rotation = on_rotation
        self._clock = clock
        self._lock = asyncio.Lock()
        self._snapshot: AuthSnapshot | None = None

    @property
    def subject(self) -> str | None:
        return self._subject

    @property
    def generation(self) -> int:
        return self._snapshot.generation if self._snapshot is not None else 0

    async def token(self) -> AuthSnapshot:
        snapshot = self._snapshot
        if snapshot is not None:
            # Avoid continuous refresh for unusually short token lifetimes.
            remaining = snapshot.expiry_monotonic - self._clock()
            if remaining > 0 and remaining > min(300, max(1, self._last_ttl / 4)):
                return snapshot
        return await self.refresh()

    async def refresh(self, *, failed_token: str | None = None) -> AuthSnapshot:
        """Refresh once; a concurrent/late 401 for an older token reuses the winner."""
        async with self._lock:
            current = self._snapshot
            if failed_token is not None and current is not None and current.id_token != failed_token:
                return current
            if failed_token is None and current is not None:
                remaining = current.expiry_monotonic - self._clock()
                if remaining > 0 and remaining > min(300, max(1, self._last_ttl / 4)):
                    return current

            try:
                response = await self._refresh(self._api_key, self._refresh_token)
                subject, id_token, new_refresh, ttl = parse_refresh_response(
                    response, self._subject,
                )
            except CloudAuthError:
                raise
            except (CloudSchemaError, ValueError, TypeError):
                raise CloudAuthError("Invalid token refresh response") from None

            old_refresh = self._refresh_token
            # Update in memory before notifying the future HA persistence
            # owner. No claim is made that a callback durably wrote to disk.
            generation = 1 if current is None else current.generation + 1
            self._subject = subject
            self._refresh_token = new_refresh
            self._last_ttl = ttl
            new_snapshot = AuthSnapshot(
                subject, id_token, new_refresh, self._clock() + ttl, generation,
            )
            self._snapshot = new_snapshot
            if self._on_rotation is not None and new_refresh != old_refresh:
                try:
                    await self._on_rotation(old_refresh, new_refresh)
                except Exception:
                    # A future adapter must surface its actual persistence
                    # result separately. Never include secret callback errors.
                    raise CloudAuthError("Refresh-token persistence callback failed") from None
            return new_snapshot

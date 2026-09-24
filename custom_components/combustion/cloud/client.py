"""Bounded HTTP transport and optional cloud client; no HA lifecycle coupling.

Only the five observed CPT-Crawl endpoint families are accepted. S1 uses an
injected aiohttp session and does not log URLs, bodies, identities or tokens.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import random
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

import aiohttp

from .auth import TokenManager
from .firestore import associated_probes, probe_status, user_document_key
from .models import (
    CloudAuthError, CloudBoundsError, CloudConflictError,
    CloudPermissionError, CloudRateLimitError, CloudSchemaError,
    CloudTransportError, CloudUnavailableError, IndexPage, Probe,
    ProbeStatus, SampleRow, SessionIndex, SessionMeta, exact_int,
    parse_index_page, parse_sample_chunk, parse_session_meta, strict_json,
)

PROJECT = "combustion-production-apps"
FIREBASE = "https://securetoken.googleapis.com"
FIRESTORE = "https://firestore.googleapis.com"
DATA_API = "https://data-api.combustion.inc"
MAX_BODY = 4 * 1024 * 1024
MAX_INDEX_PAGES = 100
INDEX_PAGE_SIZE = 100

_SLEEP = Callable[[float], Awaitable[None]]


def _check_endpoint(url: str, method: str) -> None:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.username or parsed.password or parsed.fragment:
        raise CloudSchemaError("Disallowed cloud endpoint")
    host = parsed.hostname
    if parsed.port is not None and parsed.port != 443:
        raise CloudSchemaError("Disallowed cloud endpoint")
    path = parsed.path
    if method == "POST" and host == "securetoken.googleapis.com" and path == "/v1/token":
        return
    if method == "GET" and host == "firestore.googleapis.com" and (
        path.startswith(f"/v1/projects/{PROJECT}/databases/(default)/documents/users/")
        or path.startswith(f"/v1/projects/{PROJECT}/databases/(default)/documents/probes/")
    ):
        return
    if method == "GET" and host == "data-api.combustion.inc" and path in (
        "/v1/sessions", "/v1/session", "/v1/session_data",
    ):
        return
    raise CloudSchemaError("Disallowed cloud endpoint")


def _retry_after(value: str | None) -> float:
    if value and value.isascii() and value.isdigit():
        return float(min(int(value[:10]), 10))
    return 1.0


class JsonTransport:
    """HTTP adapter: client-owned request budget, caller-owned shared session."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        max_body: int = MAX_BODY,
        attempts: int = 3,
        sleep: _SLEEP = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        """Initialize the bounded caller-owned request adapter."""
        if not 1 <= max_body <= MAX_BODY or not 1 <= attempts <= 4:
            raise CloudBoundsError("Invalid transport budget")
        self._session = session
        self._max_body = max_body
        self._attempts = attempts
        self._sleep = sleep
        self._jitter = jitter

    async def request_json(
        self, method: str, url: str, *,
        headers: Mapping[str, str] | None = None,
        data: str | None = None,
        refresh: bool = False,
    ) -> Any:
        """Send a bounded JSON request without credential-bearing redirects."""
        _check_endpoint(url, method)
        if method not in ("GET", "POST"):
            raise CloudSchemaError("Unsupported request method")
        if method == "POST" and not refresh:
            raise CloudSchemaError("Unsupported POST operation")
        if method == "GET" and data is not None:
            raise CloudSchemaError("Unexpected request body")

        for attempt in range(self._attempts):
            retry_delay: float | None = None
            try:
                timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
                async with self._session.request(
                    method, url, headers=headers, data=data,
                    allow_redirects=False, timeout=timeout,
                ) as response:
                    status = response.status
                    if 300 <= status < 400:
                        # Never forward bearer/auth form data through redirects.
                        raise CloudSchemaError("Cloud endpoint redirected")
                    if status == 401 or (refresh and status == 400):
                        raise CloudAuthError("Cloud authorization failed")
                    if status == 403:
                        raise CloudPermissionError("Cloud access denied")
                    if status == 404:
                        raise CloudSchemaError("Cloud resource not found")
                    if status == 429 or 500 <= status < 600:
                        retry_delay = (
                            _retry_after(response.headers.get("Retry-After"))
                            if status == 429 else min(2**attempt, 8) + self._jitter()
                        )
                        if attempt + 1 == self._attempts:
                            if status == 429:
                                raise CloudRateLimitError("Cloud rate limit exceeded")
                            raise CloudUnavailableError("Cloud provider unavailable")
                    elif not 200 <= status < 300:
                        raise CloudSchemaError("Cloud request rejected")
                    else:
                        content_type = response.headers.get("Content-Type", "")
                        media_type = content_type.split(";", 1)[0].strip().lower()
                        if not (media_type == "application/json" or media_type.endswith("+json")):
                            raise CloudSchemaError("Cloud response is not JSON")
                        declared = response.headers.get("Content-Length")
                        if declared is not None:
                            try:
                                if int(declared) > self._max_body:
                                    raise CloudBoundsError("Cloud body exceeds limit")
                            except ValueError:
                                raise CloudSchemaError("Invalid response length") from None
                        body = bytearray()
                        async for block in response.content.iter_chunked(8192):
                            body.extend(block)
                            # aiohttp decompresses content before this loop.
                            if len(body) > self._max_body:
                                raise CloudBoundsError("Cloud body exceeds limit")
                        return strict_json(bytes(body))
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, TimeoutError):
                if attempt + 1 == self._attempts:
                    raise CloudTransportError("Cloud transport failure") from None
                retry_delay = min(2**attempt, 8) + self._jitter()
            if retry_delay is not None:
                await self._sleep(retry_delay)
        raise CloudTransportError("Cloud request budget exhausted")


@dataclass(frozen=True, slots=True)
class IndexTraversal:
    """Evidence from a bounded index traversal, not a vendor snapshot."""

    sessions: tuple[SessionIndex, ...]
    pages: tuple[IndexPage, ...]
    page_digests: tuple[str, ...]
    terminal_reason: str
    snapshot_consistent: bool = False  # No vendor snapshot token is known.


class CombustionCloudClient:
    """S1 acquisition API, source-only; no SQL, entities, cooks or auto-polling."""

    def __init__(
        self,
        *, session: aiohttp.ClientSession, api_key: str,
        refresh_token: str, expected_subject: str | None = None,
        on_rotation: Callable[[str, str], Awaitable[None]] | None = None,
        transport: JsonTransport | None = None,
    ) -> None:
        """Initialize the bounded caller-owned request adapter."""
        self._http = transport or JsonTransport(session)

        async def refresh(api_key_value: str, refresh_value: str) -> Mapping[str, Any]:
            url = f"{FIREBASE}/v1/token?" + urlencode({"key": api_key_value})
            result = await self._http.request_json(
                "POST", url, refresh=True,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=urlencode({
                    "grant_type": "refresh_token", "refresh_token": refresh_value,
                }),
            )
            if not isinstance(result, dict):
                raise CloudSchemaError("Invalid Firebase refresh envelope")
            return result

        self._auth = TokenManager(
            api_key=api_key, refresh_token=refresh_token, refresh=refresh,
            expected_subject=expected_subject, on_rotation=on_rotation,
        )

    @property
    def subject(self) -> str | None:
        """Return the configured or authenticated source account subject."""
        return self._auth.subject

    async def _get(self, url: str) -> Any:
        snapshot = await self._auth.token()
        for attempt in range(2):
            headers = {
                "Authorization": f"Bearer {snapshot.id_token}",
                "Content-Type": "application/json",
                # Observed CPT profile; not a vendor-stable supported contract.
                "CI-AppVersion": "v3.2.4",
                "CI-OSVersion": "35",
                "CI-Locale": "en-US",
                "CI-DateTime": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            }
            try:
                return await self._http.request_json("GET", url, headers=headers)
            except CloudAuthError:
                if attempt:
                    raise
                snapshot = await self._auth.refresh(failed_token=snapshot.id_token)
        raise CloudAuthError("Cloud authorization failed")

    async def probes(self) -> tuple[Probe, ...]:
        """Read associated probe source locators from Firestore."""
        snapshot = await self._auth.token()
        key = user_document_key(snapshot.subject)
        path = f"/v1/projects/{PROJECT}/databases/(default)/documents/users/{key}"
        return associated_probes(await self._get(FIRESTORE + path))

    async def status(self, probe: Probe) -> ProbeStatus:
        """Fetch current probe status independently of historical acquisition."""
        # Device keys are source locators, not operator-provided arbitrary URLs.
        key = quote(probe.device_key, safe="")
        path = f"/v1/projects/{PROJECT}/databases/(default)/documents/probes/{key}/probe_status/current"
        return probe_status(await self._get(FIRESTORE + path))

    def _source_params(self, serial: str, session_id: int | None = None) -> dict[str, str]:
        subject = self._auth.subject
        if subject is None:
            raise CloudAuthError("Cloud account has not been authenticated")
        if not isinstance(serial, str) or not serial or len(serial) > 128:
            raise CloudSchemaError("Invalid source device serial")
        result = {"uid": subject, "device_serial_number": serial, "device_type": "1"}
        if session_id is not None:
            result["device_session_id"] = str(exact_int(session_id, "device_session_id"))
        return result

    async def session_meta(self, serial: str, session_id: int) -> SessionMeta:
        """Fetch and strictly validate one source session manifest."""
        await self._auth.token()
        params = self._source_params(serial, session_id)
        return parse_session_meta(await self._get(DATA_API + "/v1/session?" + urlencode(params)))

    async def sample_chunk(
        self, serial: str, session_id: int, start: int, end: int,
    ) -> tuple[SampleRow, ...]:
        """Read one bounded sequence range; absence remains partial."""
        exact_int(start, "start")
        exact_int(end, "end")
        if end < start or end - start >= 1000:
            raise CloudBoundsError("Sample request exceeds one bounded chunk")
        await self._auth.token()
        params = self._source_params(serial, session_id)
        params["sequence_number_ranges"] = json.dumps([[start, end]], separators=(",", ":"))
        params["page"] = "1"  # Observed in CPT; not asserted as a vendor paging contract.
        return parse_sample_chunk(
            await self._get(DATA_API + "/v1/session_data?" + urlencode(params)),
            start=start, end=end,
        )

    async def sessions(self, probe: Probe, *, revisit_first_page: bool = True) -> IndexTraversal:
        """Bounded index traversal; never claims a vendor-consistent snapshot."""
        await self._auth.token()
        params = self._source_params(probe.serial)
        pages: list[IndexPage] = []
        digests: list[str] = []
        seen_pages: set[str] = set()
        seen_sessions: dict[str, SessionIndex] = {}
        total_pages: int | None = None
        terminal = "unknown"

        async def request_page(page: int) -> tuple[IndexPage, str]:
            query = {**params, "page": str(page), "page_size": str(INDEX_PAGE_SIZE)}
            result = await self._get(DATA_API + "/v1/sessions?" + urlencode(query))
            response = parse_index_page(result, page=page)
            for session in response.sessions:
                if session.serial is not None and session.serial != probe.serial:
                    raise CloudConflictError("Index device serial mismatch")
                if session.uid is not None and session.uid != self._auth.subject:
                    raise CloudConflictError("Index account mismatch")
                if session.device_type is not None and session.device_type != 1:
                    raise CloudConflictError("Index device type mismatch")
            digest = hashlib.sha256(
                json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
            ).hexdigest()
            return response, digest

        for page_number in range(1, MAX_INDEX_PAGES + 1):
            page, digest = await request_page(page_number)
            if total_pages is None:
                total_pages = page.total_pages
            elif page.total_pages is not None and page.total_pages != total_pages:
                raise CloudConflictError("Index total_pages changed during traversal")
            if digest in seen_pages and page.sessions:
                raise CloudConflictError("Repeated index page")
            seen_pages.add(digest)
            digests.append(digest)
            pages.append(page)
            for item in page.sessions:
                prior = seen_sessions.setdefault(item.source_session_token, item)
                if prior != item:
                    raise CloudConflictError("Conflicting session index records")
            if total_pages is not None and page_number >= total_pages:
                terminal = "declared_total_pages"
                break
            if total_pages is None and len(page.sessions) < INDEX_PAGE_SIZE:
                terminal = "short_page"
                break
            if total_pages is not None and not page.sessions:
                raise CloudConflictError("Unexpected empty index page")
        else:
            raise CloudBoundsError("Session index exceeds page guard")
        if revisit_first_page and pages:
            first, first_digest = await request_page(1)
            if first_digest != digests[0]:
                raise CloudConflictError("Index changed during bounded revisit")
        return IndexTraversal(tuple(seen_sessions.values()), tuple(pages), tuple(digests), terminal)

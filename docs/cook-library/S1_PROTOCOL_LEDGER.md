# S1 — Python cloud protocol evidence, deviations and proof gates

**Status:** S1 implementation in a development branch; tests and CI are tracked in PR #2. No cloud integration is registered with Home Assistant and no authenticated account request has been made. This file is not a vendor interoperability certificate.

**Pinned reference:** [CPT-Crawl fa1dd582a84aa570ed11c65a5cab1542ee3da411](https://github.com/joshp123/CPT-Crawl/commit/fa1dd582a84aa570ed11c65a5cab1542ee3da411), MIT. Port portions of [client.go](https://github.com/joshp123/CPT-Crawl/blob/fa1dd582a84aa570ed11c65a5cab1542ee3da411/internal/combustion/client.go), [sessions.go](https://github.com/joshp123/CPT-Crawl/blob/fa1dd582a84aa570ed11c65a5cab1542ee3da411/internal/combustion/sessions.go) and [types.go](https://github.com/joshp123/CPT-Crawl/blob/fa1dd582a84aa570ed11c65a5cab1542ee3da411/internal/combustion/types.go). Original fork code retains its own MIT license and author credits. The source here is adapted Python code, not an official SDK.

## Contract ownership

- `cloud/models.py`: typed errors; strict bounded JSON/manifest/index/sample decoding, no missing→zero or float-int coercion; source raw fields preserved, invalid optional sample fields explicitly marked.
- `cloud/firestore.py`: UUIDv5 Firebase subject key, exact Firestore int64, association and status parsing.
- `cloud/auth.py`: account-bound in-memory single-flight refresh and generation, no HA config-entry writes or bootstrap.
- `cloud/client.py`: caller-injected aiohttp session, fixed HTTPS hosts and endpoint paths, strict body/timeout/retry/redirect limits, observed vendor headers, index traversal and per-chunk reads.
- `cloud/sessions.py`: lazy inclusive chunk iterator, union counting and bounded pure gap fixtures. No durable cursors.
- Tests are synthetic/reference-compatible and intentionally adversarial; they are not private account recordings. No separate Go runtime is required.

## Observed endpoint mapping, not promises of long-term vendor support

| Operation | Observed CPT shape | Python boundary |
| --- | --- | --- |
| Firebase refresh | POST `securetoken.googleapis.com/v1/token?key=...` with form grant_type/refresh_token | strict subject/id/rotation/ttl, account match, single-flight, no request body logging |
| Firestore account document | `projects/combustion-production-apps/databases/(default)/documents/users/{UPPER-UUIDv5(uid)}` | only PROBE associations modeled; unknown types do not imply gauge cloud support |
| Firestore current status | `probes/{device_key}/probe_status/current` | exact session/period; optional call, not prerequisite for history |
| Session index | `GET /v1/sessions`, uid/serial/device_type/page/page_size | page_size 100, max 100, repeat/total/identity checks and first-page revisit |
| Session metadata | `GET /v1/session` with numeric session token | absent/null ranges invalid; explicit [] is observed empty |
| Sample chunk | `GET /v1/session_data`, JSON-encoded inclusive ranges, page=1 | at most 1,000 requested IDs; partial rows returned as partial, never certified complete |

The client retains observed CI-AppVersion/OSVersion/Locale/DateTime headers in one compatibility profile. The API, identity provider, real session-ID collisions, retention, clock basis and sampling limits remain **unverified**. Do not silently update spoofed app version strings.

## Intentional Go divergences

| Go reference behavior | Python adaptation and reason |
| --- | --- |
| Missing typed/numeric JSON may become zero/default; Firestore int64 passes through float64 | Strict required fields; exact int64; explicit null/absent/valid-zero tests |
| Go JSON permits duplicate keys by last-value behavior | Duplicate JSON keys reject entire envelope, including nested keys |
| Go `watch` emits latest of a new session and keeps an in-memory high-water mark | No S1 watcher or archive; S3 uses durable missing-range jobs, manifest revisions and content audits |
| Go `SessionData` missing rows are detected later by CLI summary | S1 chunk returns only identified, requested rows; missing rows remain partial for S3 transaction/gap accounting |
| Go `latestRanges` substitutes 5000ms for malformed sample period | Invalid required period rejected; no inferred historical clock or active-recency claim |
| Go `getWithRetry` can expose error bodies/URL and has unbounded read | Typed non-sensitive error, fixed endpoint policy, bounded decoded bytes/timeouts/retries |
| Go refresh has no single-flight lock and rotated credential only in memory | Per-account lock, stale-401 suppression, explicit optional persistence hook; still no durability promise |
| Go index 100x100 guard does not establish stable listing | Store page/terminal evidence in return model; reject repeated/conflicting pages, revisit first; `snapshot_consistent=False` always |
| Go whole-session aggregation into memory | Client fetches one bounded chunk at a time; S3 will schedule/resume committed work |
| Go shell/age decrypt and Apple/bootstrap agent flow | Deliberately excluded; S2 must prove a user-controlled supported bootstrap before cloud deployment |

## S1 validation matrix

`tests/cloud/test_models.py`: Firestore integer >2^53, typed null/arrays/maps, exact UID key, strict invalid fields, NaN/duplicate keys, ranges and 1000 boundary, source sample identities and partial payload.

`tests/cloud/test_auth.py`: missing fields, cross-account subject mismatch, concurrent refresh single flight, late 401, short lifetime, rotation callback failure and secret-safe exception/repr.

`tests/cloud/test_client.py`: fixed endpoint path/query, sample/current/history independence, multi-page index and mutation detection, credential refresh, 401/429/5xx, timeout/cancellation, redirects, body/content-type bounds and no constructor network.

Full HA/Python 3.14.2 and existing local suite, Ruff, Node card tests, Hassfest/HACS: see GitHub PR #2 actual check results. A green test against mocked HTTP proves only implementation against fixtures. A vendor account trial and stable auth onboarding are **S2 evidence gates**; physical serial matching is **S5**, archive storage/restore is **S3/S8**.

## No-touch and rollback

No edits to existing `custom_components/combustion/__init__.py`, `config_flow.py`, parser, BLE/Probe/Prediction/Connection/Control managers, entity platforms, bundled card or HA manifest in S1. No network or filesystem I/O at module import. Remove the unused `cloud/` package to roll back S1; it is not loaded by Home Assistant startup. No account credentials, real probes/session IDs or private cook histories belong in this repository or test reports.

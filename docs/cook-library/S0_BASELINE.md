# Combustion Cook Library — S0 repository baseline

**Stage:** S0, source/offline baseline; no Cook Library runtime code. **Date:** 2026-09-23. **Scope:** `Milkbone911/homeassistant-combustion`, not the installed Home Assistant instance.

This is the repository's execution entry point for the *Combustion Cook Library — Final Consolidated Engineering Plan and Implementation Baseline*, version 2.0 (2026-09-23). The final plan is the governing specification; this S0 note preserves executable baseline evidence and the gates for S1/installation. The independent adversarial review is evidence, not a competing implementation specification. Neither design nor an upstream test run proves live behavior.

## Pinned source evidence

| Item | Observed value | Evidence boundary |
| --- | --- | --- |
| User fork `main` | `cdd732ca0b0ccf0bd0b828c6c54fbdddd0a49ab9` | Inspected before the S0 feature branch was created; recheck before any subsequent PR |
| `raww` upstream `main` | `cdd732ca0b0ccf0bd0b828c6c54fbdddd0a49ab9` | Identical main commit and tree at this checkpoint; does not assert installed equivalence |
| Both tree IDs | `0d018e4c0cf2c33fd94f87a5e6d723491735e547` | Repository comparison only |
| CPT-Crawl source | `fa1dd582a84aa570ed11c65a5cab1542ee3da411` | Unofficial cloud protocol reference; no authenticated account check |
| Existing HA test target | `homeassistant==2026.8.2` | `pyproject.toml`, not the user's deployed HA version |
| Python | `>=3.14.2,<3.15` | `pyproject.toml` |
| Test plugin | `pytest-homeassistant-custom-component==0.13.356` | `pyproject.toml` |
| Poetry | `~=2.3` | Existing GitHub Actions jobs; S0 aligns devcontainer |
| Frontend test runner | Node 22 / built-in `node --test` | S0 CI lane, independent of Python/HA |
| Manifest version | `0.0.0` | Not a reliable deployed-package identity; record an installed file digest/commit |

Source links: [fork commit](https://github.com/Milkbone911/homeassistant-combustion/commit/cdd732ca0b0ccf0bd0b828c6c54fbdddd0a49ab9), [upstream commit](https://github.com/raww/homeassistant-combustion/commit/cdd732ca0b0ccf0bd0b828c6c54fbdddd0a49ab9), [CPT-Crawl pin](https://github.com/joshp123/CPT-Crawl/commit/fa1dd582a84aa570ed11c65a5cab1542ee3da411).

## Source-level contracts that S1 must not disturb

- The domain is `combustion`; BLE/manual entry uniqueness uses `combustion_meatnet`. Existing ConfigFlow VERSION=1 and the entry's actual `entry_id` must not be replaced by a second cloud entry.
- At the inspected baseline `hass.data[DOMAIN]` is a **ProbeManager object**, not a dictionary; existing suffixed manager keys and platform forwarding are still in use.
- Platforms: `binary_sensor`, `sensor`, `number`, `select`, `button`. Preserve physical device identifiers `(combustion, original_serial)`, unique IDs, disabled preferences, and entity registry associations.
- Probe serials are lowercase unpadded hex in existing HA IDs; gauge/node serials are ASCII. Do not normalize and rewrite legacy registry identifiers.
- Primary passive/nonconnectable BLE listener and optional active GATT callback have distinct purposes. Do not introduce a third listener, duplicate GATT manager, or second handler for the prediction-status characteristic.
- ProbeManager's five-second direct-versus-repeater preference, 90-second default availability and one-second default entity-notification throttle are **current source behavior**, not archive completeness guarantees.
- Instant-read initial discovery, cached mode, prediction freshness and the failed-entity gate have documented baseline deficiencies. They are **S2/S4 correction work**, not permission to rewrite the local pipeline in S0/S1.
- Existing bundled `www/combustion-card.js` resolves HA entity/device identities; the historical cook UI will be separate.
- No cloud account flow, archive, cook engine, or historical Cook Library API is present in the pinned source. Historical HA Recorder UI is not a Cook Library.
- Existing HACS `homeassistant` minimum is older than this project's pinned development target; do not silently claim a tested release compatibility range or alter HACS packaging in S0.

## Reproduce the source baseline

Run on the S0 branch (or the exact pinned commit), not on the live HA config directory:

```bash
python3 --version                         # must satisfy >=3.14.2,<3.15
python3 -m pip install 'poetry~=2.3'
poetry --version
poetry check --lock
poetry install --sync
poetry run pytest
poetry run ./scripts/lint                 # read-only Ruff check after S0
node --version                             # CI uses Node 22
node --test tests/js/*.test.mjs
```

`poetry.lock` is retained unchanged in S0. Install dependencies only in the disposable test environment. `pytest` produces `test_results/pytest.xml` and coverage artifacts; record their commit, run URL, failures and exact tool versions. A successful Docker build or dependency install alone is not a test pass.

### Baseline evidence available at S0 preparation

For **upstream at the exact source commit**, GitHub Actions [Validate](https://github.com/raww/homeassistant-combustion/actions/runs/33437932993) reports passing tests, Hassfest and HACS validation; [Lint](https://github.com/raww/homeassistant-combustion/actions/runs/33437933099) reports passing Ruff. These are historical **upstream runs**, not runs on the user's fork or the S0 diff. The independent review additionally reported 11/11 JS resolver checks in its isolated environment. Re-run these lanes on the S0 PR; do not carry a green upstream badge forward as an S0-branch pass.

The external task runner used for this repository preparation has Python 3.13.5 and cannot truthfully execute the HA/Python 3.14.2 suite. The CI and corrected devcontainer are the supported test environments. If fork Actions are unavailable, mark the fork-run gate **BLOCKED**, retain historical upstream evidence separately and do not assert S0 full validation.

## S0 vs deployment gates

**S0 offline → S1 development:** pinned source; corrected reproducible dev/test configuration; baseline test outcomes explicitly recorded; acceptance/rollback checklist; no runtime source changes and no live activity. S1 can start with historical known-green upstream source evidence even if a fork-only CI run is not yet available, but cannot be declared accepted without its required tests.

**Before any installation:** obtain an authorized, fresh, locally protected read-back of actual installed code/version and entry/registry/options/traces; compare with this baseline; inventory recoverable backups and a rollback package. See [LIVE_EVIDENCE_CHECKLIST.md](LIVE_EVIDENCE_CHECKLIST.md). Not completing that read-back does not prohibit isolated offline S1 work.

**Before S3 live archive:** verify actual Python/SQLite patch level, archive mount/path identity, capacity, writer fencing and consistent snapshot; Ironvale owns the underlying storage implementation. **Before production:** semantic restore in an independent recovery location, account proof and household permission policy.

## Non-negotiable S0 change boundary

S0 changes development/CI configuration and documentation only. It does not touch the deployed HA, manifest/domain/version, device IDs, local runtime, BLE/GATT, cloud account, credentials, database, automation YAML or Cook Library features. Do not merge or install S1 functionality as part of an S0 housekeeping PR.

## Gate ownership

| Evidence/gate | Owner | Where recorded |
| --- | --- | --- |
| Fork commit, CI and source regressions | Integration repository / developer | This file plus S0 PR CI and review |
| Deployed HA/entry/registry/automation read-back | Smart Home / HA operator | Protected local evidence; sanitized status in live checklist |
| Persistent mount, fencing, SQLite and restore | Ironvale / platform operator with Smart Home application checks | Protected operational evidence; only status/pointer in repo |
| Account bootstrap/actual vendor contract | Smart Home / authorized account operator | Local protected trial; sanitized fixtures if suitable for public code |
| Capture/retention and household history permission decision | User / Smart Home | Later accepted decision; not inferred from S0 |

**Current state:** source baseline pinned, S0 branch work only; installed-system comparison and protected backup read-back **NOT PERFORMED**. No new cloud/archive code exists at S0.

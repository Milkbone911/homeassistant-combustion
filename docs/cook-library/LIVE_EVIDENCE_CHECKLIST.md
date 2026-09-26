# Pre-installation live baseline, backup and rollback checklist

**Status:** TEMPLATE / NOT PERFORMED. Do not mark items complete based on the GitHub source tree, prior HA screenshots, the final design document, or successful CI. Do not publish private registry exports, account IDs, tokens, locations, device serials or a database backup in the public repository.

This checklist is required **before installing or enabling** any S2+ Cook Library version; it is not a prerequisite for isolated, offline S1 contract work.

## 1. Read-only installed-state inventory

- [ ] Date/time, HA deployment namespace/release and current HA core version obtained from live HA/effective Helm/Kubernetes, with appropriate authority. Ironvale owns the underlying platform configuration.
- [ ] Python version, `sqlite3.sqlite_version`, installed integration location and installed content hashes or artifact/commit captured. The manifest version `0.0.0` alone is insufficient. Compare changed installed files with pinned `cdd732ca...` and review divergence before migration.
- [ ] Existing `combustion` entry ID and unique ID `combustion_meatnet` read back; configuration flow version, link status, current options and relevant enabled/disabled feature states recorded **without copying secret fields**.
- [ ] Existing device registry identities and entity unique IDs captured in a locally protected file; record count, entity domains, rename/disabled preferences, and the current UI card references. Repo notes may say only “verified against protected snapshot” and a locally meaningful evidence handle.
- [ ] Probe, gauge, MeatNet and optional GATT cases inventoried, including which were actually observed. Do not wake sleeping hardware merely to make a false availability claim.
- [ ] Existing cook-related native HA automations, helper IDs, triggers/conditions/actions, traces and entity relationships read back. Reuse verified existing automation behavior at S6; do not assume a YAML package exists or rewrite it during S0.
- [ ] A representative **local-only** BLE input→entity state→card test performed (where hardware available); unavailable devices remain explicitly untested. Preserve old entity_id/unique_id/device associations.
- [ ] Confirm baseline logger/Recorder faults, failed entities and BLE/proxy/GATT constraints separately, so preexisting problems cannot be counted as new regressions or silently “fixed” by archive work.

## 2. Protected recovery inventory — not proof of restore

- [ ] Identify last known-good **installed integration package/commit and file hashes**, not merely GitHub main or a HACS version label.
- [ ] A protected HA configuration backup covers config-entry/registries, relevant automations/scripts/helpers, integration files and required deployment configuration. Do not commit/export raw backup here.
- [ ] Actual backup destination, encryption/access, freshness and restore procedure verified with Ironvale as applicable; classify **inventory/backup exists** separately from **restore tested**.
- [ ] For any later archive schema migration: a consistent archive snapshot exists with the matching code/schema/config tuple, and the destination has adequate space. Preserve the newer database if code is rolled back; never let an older writer open an unsupported newer schema.
- [ ] Rollback action and operator trigger agreed: disable optional features first where possible; preserve old entry ID, registry and retained source evidence; do not delete/re-add integration to solve migration.
- [ ] No production changes until all relevant checkboxes are evidenced. S0 repository preparation itself needs no production backup operation.

## 3. Application/source comparison worksheet (keep real identifiers in protected local record)

| Field | Source baseline | Installed evidence/status |
| --- | --- | --- |
| HA/Python/SQLite | Dev HA 2026.8.2 / Python >=3.14.2 / SQLite uninspected | **UNVERIFIED** |
| Integration artifact | Fork `cdd732ca...`, manifest `0.0.0` | **UNVERIFIED** |
| Domain/entry | `combustion` / `combustion_meatnet` | **UNVERIFIED** |
| Config entry ID/version/options | Flow version 1; defaults available in const/options code | **UNVERIFIED** |
| Registry/entity IDs, disabled flags | Exact source schemes; source fixtures only | **UNVERIFIED** |
| BLE/proxy/GATT and card | Implemented in source | **UNVERIFIED LIVE** |
| Existing cook automations/traces | Not inspected | **UNVERIFIED** |
| Rollback package/config backup | None collected by S0 repository work | **UNVERIFIED** |
| Mounted archive/SQLite/restore | No Cook Library archive in source baseline | **NOT STARTED / LATER GATE** |

Store a date, evidence owner, and sanitized reference when each row is actually observed. An old HA screenshot or Hjarni status is orientation, not current live read-back.

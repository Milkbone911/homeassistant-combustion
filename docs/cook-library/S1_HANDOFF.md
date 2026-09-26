# S0 exit record and S1-only engineering handoff

**Governing design:** *Combustion Cook Library — Final Consolidated Engineering Plan and Implementation Baseline* v2.0, 2026-09-23. S0 source pin and repository acceptance context are in [S0_BASELINE.md](S0_BASELINE.md). The [live checklist](LIVE_EVIDENCE_CHECKLIST.md) remains a separate **installation** gate.

## S0 exit gates: evidence semantics

| Check | Accepted evidence | What it does not prove |
| --- | --- | --- |
| Identical source pin | Fork/upstream `main` commit `cdd732ca...` and tree `0d018e4...` at observed time | Installed HA matches |
| Source baseline regression | Exact-commit upstream Validate+Lint run succeeded; S0 PR must show fork-run results separately | New S0 branch passed before CI reports |
| Supported environment | `pyproject.toml` Python >=3.14.2,<3.15, HA 2026.8.2; aligned devcontainer/CI, lock retained | Any unbuilt image is already validated |
| Compatibility inventory | Domain, singleton, manager alias, platforms, unique-ID and card contracts in S0_BASELINE | Actual registry or hardware acceptance |
| Rollback plan | Protected/live evidence template and code/schema compatibility rules | A backup or restore has occurred |
| Next-stage scope | S1 modules and no-touch list below | Vendor API, account bootstrap or deployment authorization |

## First implementation PR — only S1 offline Python cloud contract

Add `custom_components/combustion/cloud/{__init__,auth,client,firestore,models,sessions}.py` as needed; `tests/cloud/` with **sanitized, synthetic** positive and adversarial fixtures; a protocol-source and intentional-Go-divergence ledger; MIT attribution as appropriate. Only test/dependency metadata actually needed for those modules may change.

The port must have injected async HTTP transport, exact Firebase UID→Firestore user key, typed Firebase refresh, strict Firestore numeric/null decode, probe discovery, status/index/session metadata/ranged sample model, inclusive range validation, response and concurrency bounds, safe redirects, classified/retriable errors and no import-time networking/filesystem activity. Separate **matches observed Go protocol** from **intentionally rejects unsafe Go behavior**. No Go executable or shell-based auth-bundle runtime is introduced.

**S1 acceptance (all mandatory):** fixture endpoint tests, invalid/missing/null/zero/duplicate-key/range/truncation cases, timeout/cancel/401/429/retry/redirect/redaction/single-flight cases, Python/HA baseline test and JS card lane in supported CI, scope review and attribution. Keep actual authenticated cloud compatibility and bootstrap explicitly **UNVERIFIED** until authorized account testing.

**No-touch for S1:** existing integration `__init__.py`, `config_flow.py`, `bluetooth_listener.py`, Probe/Prediction/Connection/Control managers, all entity platforms, `combustion_ble/*`, existing `www/*`, current config-entry/device/entity registries, deployed HA, credentials, archive, automations and Ironvale infrastructure.

**S1 stop condition:** do not install, enable cloud, create a Cook Library database, change local source selection, or start S2 to work around a failing S1 gate. Document any baseline defects as preexisting versus newly introduced. First real cloud trial is a separate authorized, protected S2/account-proof step.

## Later acceptance and rollback ownership

S2a preserves live identity and unload/reload; S2b proves account bootstrap; S2c qualifies narrow local mode/prediction changes; S3 adds crash-safe storage/jobs and actual SQLite placement; S4 qualifies bounded BLE capture; S5 requires paired alias evidence; S6 supplies user-confirmed cooks/UI-native automations; S7 obtains an explicit backend-enforced household permission decision; S8 proves semantic external restore/reschedule and production behavior.

S0 grants **no** live deployment permission and does **not** declare acceptance for F07, F15 or F21. Admin-only private-history access is a development default, not a permanent household policy. No automatic food-safety or smoker-heat control is in scope.

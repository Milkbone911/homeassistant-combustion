# S2c — local BLE correctness and observation seams

**Stage:** S2c source implementation / hardware-acceptance preparation  
**Source status:** implemented and CI-validated when the exact branch head is green  
**Live hardware status:** requires the representative-device gate below  
**Storage/cook status:** S3/S4/S6 are not started by this slice

S2c is the narrow local-correctness slice from the Cook Library adversarial
review. It fixes demonstrated current-state defects and prepares failure-
independent observation seams without changing the integration's identity,
adding an archive, or creating another Bluetooth/GATT owner.

## Preserved contracts

- one Home Assistant config entry with unique ID `combustion_meatnet`;
- existing entity unique IDs and device identifiers;
- one existing Home Assistant Bluetooth advertisement registration;
- one existing Probe Status GATT subscription owned by `PredictionManager`;
- existing direct-over-MeatNet preference window;
- existing opt-in active-connection/GATT behavior and controls;
- no SQLite/archive/history writer, cook inference, or historical frontend.

## First-discovery instant-read correction

Before S2c, `ProbeManager` stored a first-ever instant-read advertisement in
its normal-data cache because the cache assignment allowed `is_new`. That made
T2-T8 and virtual core/surface/ambient fields from an instant packet available
as if they were valid normal-mode measurements.

S2c separates three facts:

1. current selected mode;
2. the latest instant-read T1 value;
3. the last valid normal/live data object.

An instant packet updates current mode and instant T1 but never refreshes the
normal-data cache. A first-ever instant packet may still create the established
entity identities, but regular temperature entities remain unknown until a
valid normal-mode observation arrives. After normal data exists, an instant
transition preserves that prior normal data without presenting it as newly
measured.

The mode entity no longer uses "latest advertisement wins". Live hardware
showed that normal and Instant Read advertisement streams can be interleaved
through direct/proxy/MeatNet paths, producing rapid false mode transitions.
This matches the vendor SDK architecture: Combustion's Android and iOS
frameworks arbitrate Normal and Instant Read independently and keep separate
last-update/source-priority state.

S2c therefore treats advertisement-only mode as evidence, not a total order:

- one recent advertisement mode -> that mode may be projected;
- multiple recent advertisement modes -> `unknown`, because arrival order
  cannot prove a physical transition;
- a fresh mode from the already-owned GATT Probe Status stream overrides
  ambiguous advertisement evidence;
- disconnect or status staleness removes that stronger evidence rather than
  resurrecting an old mode.

This preserves the Mode entity/unique ID while avoiding fabricated transition
churn. Its source/provenance attributes still use the latest selected packet.

## BLE reception and selected-observation seam

The existing `BluetoothListener` callback now creates one frozen reception
envelope containing:

- parsed device data;
- local monotonic receipt time;
- broadcaster/source address;
- scanner source when Home Assistant supplies one;
- RSSI;
- upstream receipt/time value when available;
- connectable state when available.

No additional Home Assistant Bluetooth callback is registered.

`ProbeManager` applies the existing direct-versus-MeatNet selection policy
before offering a selected observation to its observation listeners. The offer
occurs before the entity/platform failure gate and is synchronous/non-awaiting.
A future S4 queue can therefore receive selected HA-delivered observations even
when entity creation has failed, without making live BLE wait for archival I/O.

Observation and legacy/entity fan-out are exception-isolated: one consumer
failure does not prevent other consumers from seeing the same valid reception.

This seam does **not** claim every RF packet or every hardware sample. It
preserves selected, parsed observations that Home Assistant delivered to this
integration. S4 still owns normalized immutable archival DTOs, queue bounds,
drop accounting, replay/age qualification, and durable persistence.

## Prediction freshness correction

Prediction status is received only through the existing Probe Status GATT
notification subscription.

S2c retains the last parsed prediction internally as source evidence, but
`prediction(serial)` exposes it as current only while:

- the probe is currently connected through the shared `ConnectionManager`;
- a valid Probe Status notification has been received since the most recent
  disconnect; and
- that notification is less than 15 seconds old.

The 15-second bound follows Combustion's official open-source iOS BLE framework,
whose public `Probe.stale` contract marks a probe stale when no advertising
data or notifications have arrived within 15 seconds:

https://github.com/combustion-inc/combustion-ios-ble

A disconnect immediately invalidates current prediction fitness. A rapid
reconnect does not resurrect the old ETA; a new Probe Status notification is
required. A timer refreshes Home Assistant entities when the freshness window
expires even if no later GATT notification arrives.

Prediction observations are offered inside `PredictionManager`, before entity
projection, so a future S4 observer does not replace the Probe Status
subscription or compete with the existing GATT owner.

## Source acceptance

The S2c source gate requires:

- first-ever instant-read does not populate normal/core/surface/ambient values;
- normal -> instant -> normal preserves only the last valid normal data during
  the instant interval and tracks mode without latest-packet flip-flop;
- conflicting recent normal/Instant-Read advertisement streams resolve to
  `unknown` unless a fresh existing Probe Status notification supplies the
  stronger current mode;
- selected BLE observations are offered before entity-failure gating;
- direct-over-repeated selection remains shared and unchanged;
- listener/observer failures do not stop healthy fan-out;
- reception envelope fields are frozen at the seam;
- fresh prediction becomes stale after the bounded notification interval;
- disconnect immediately invalidates current prediction;
- reconnect without a fresh status notification does not restore an old
  prediction;
- the raw last prediction remains available internally for later historical
  treatment;
- a prediction-observation consumer failure cannot suppress live prediction;
- there is still one HA advertisement callback and one Probe Status GATT
  subscription;
- full Python/Home Assistant regression suite, Ruff, HACS, Hassfest and bundled
  card resolver remain green.

Passing this gate establishes **IMPLEMENTED / SOURCE-VALIDATED** S2c behavior.
It is not physical-device acceptance.

## Required live hardware gate

Before calling S2c complete for live rollout, exercise representative real
hardware on the intended Home Assistant deployment:

1. **First instant discovery**
   - start/reload with no valid normal sample cached for the test probe;
   - place/wake the probe in instant-read mode;
   - confirm the Instant Read entity reports T1;
   - confirm regular core/surface/ambient/thermistor values are not fabricated
     from the instant packet;
   - confirm the Mode entity reports `instant_read` when evidence is
     unambiguous; `unknown` is truthful if conflicting recent advertisement
     streams exist before a fresh Probe Status notification.

2. **Normal -> instant -> normal**
   - observe valid normal values;
   - enter instant-read mode;
   - confirm normal values do not jump to invalid instant-mode T2-T8 values;
   - confirm Mode does not oscillate rapidly between `normal` and
     `instant_read`; a short `unknown` ambiguity interval is acceptable
     until stale advertisement evidence clears or fresh Probe Status resolves it;
   - return to normal mode and confirm new normal readings resume.

3. **Fresh -> stale prediction**
   - with active connection enabled and a real prediction present, confirm the
     prediction entities are current;
   - interrupt/disconnect the GATT path and confirm they become unavailable
     without affecting ordinary BLE entities;
   - reconnect without relying on an old ETA; a fresh status notification must
     restore current prediction state;
   - separately allow the status stream to go quiet long enough to verify the
     stale timeout behavior where practical.

4. **Route regression**
   - where representative repeater/direct coverage is available, confirm fresh
     direct data remains preferred and repeated data resumes after the direct
     path becomes stale.

5. **Existing active-control/card regression**
   - confirm the existing Connected diagnostic still follows the real GATT
     link;
   - confirm the existing card renders the same probe/device identities;
   - exercise only already-authorized non-destructive controls needed to prove
     the existing GATT path remains functional. Do not reset a probe or Food
     Safe state merely for this gate.

Record observed results and limitations. A source test or simulated clock is
not a substitute for the real-device gate.

## Rollback

S2c has no schema/config-entry/archive migration. Rollback is code-only: restore
the prior known-good integration package/commit and restart/reload Home
Assistant. No S2c database or new persistent state needs downgrade handling.

## Boundary to later stages

S2c creates observation seams only. It does not:

- create archive tables/files or a writer;
- persist BLE or prediction observations;
- claim radio/sample completeness;
- reconcile cloud and BLE identity/history;
- classify cooks or foods;
- register historical UI/API/export behavior.

Those responsibilities remain in their later owning stages.

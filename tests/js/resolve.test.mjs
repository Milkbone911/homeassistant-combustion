// Unit tests for the combustion-card entity resolver.
//
// The card is a self-contained Lovelace element with no build step and no
// test framework in this repo — this file uses only Node's built-in test
// runner (`node --test`). To import the card module in Node (outside a
// browser), we shim just enough DOM for its module-level side effects
// (font injection, customElements.define) to run without throwing. No DOM
// guards were added to the production file for this — the shim satisfies
// what's already there.
import { test } from 'node:test';
import assert from 'node:assert/strict';

globalThis.HTMLElement = class {};
globalThis.customElements = { define() {}, get() {} };
globalThis.document = {
  getElementById: () => null,
  createElement: () => ({ style: {}, appendChild() {} }),
  head: { appendChild() {} },
};
// The card's module-level footer registers `window.customCards` (unrelated
// to the resolver under test); Node has no `window` global, so provide one.
globalThis.window = globalThis;

const {
  resolveEntities,
  hasPopulatedRegistry,
  computeGaugeDeviceIds,
  isPickableCombustionEntity,
} = await import('../../custom_components/combustion/www/combustion-card.js');

// ---- fixture helpers ----
//
// Shape mirrors the real `hass.entities` / `hass.devices` collections the HA
// frontend provides: entities keyed by entity_id with a `device_id` (and
// `platform`), devices keyed by device_id with an `identifiers` list of
// [domain, id] tuples.

function makeHass(devices, entitiesByDevice) {
  const devicesReg = {};
  for (const dev of devices) {
    devicesReg[dev.id] = { id: dev.id, identifiers: dev.identifiers };
  }
  const entitiesReg = {};
  for (const [deviceId, entityIds] of Object.entries(entitiesByDevice)) {
    for (const entityId of entityIds) {
      entitiesReg[entityId] = { device_id: deviceId, platform: 'combustion' };
    }
  }
  return { entities: entitiesReg, devices: devicesReg };
}

// A probe device whose entities were renamed away from the
// `predictive_thermometer_<serial>_*` naming the legacy string-builder would
// guess. Any test that resolves this fixture correctly can only be doing so
// via the device registry, not string concatenation — which is also exactly
// the rename-survival property the resolver is meant to provide.
const RENAMED_PROBE_DEVICE = { id: 'device-brisket', identifiers: [['combustion', '10014030']] };
const RENAMED_PROBE_ENTITIES = {
  'device-brisket': [
    'sensor.brisket_core_temperature',
    'sensor.brisket_ambient_temperature',
    'sensor.brisket_instant_read_temperature',
    'sensor.brisket_mode',
    'sensor.brisket_ready_in',
    'sensor.brisket_cook_target',
    'sensor.brisket_prediction',
    'number.brisket_target_temperature',
    'binary_sensor.brisket_battery',
    'binary_sensor.brisket_overheating',
  ],
};

test('probe resolved from a picked _core_temperature entity — every role mapped', () => {
  const hass = makeHass([RENAMED_PROBE_DEVICE], RENAMED_PROBE_ENTITIES);
  const { isGauge, entities } = resolveEntities(
    { entity: 'sensor.brisket_core_temperature' },
    hass,
  );

  assert.equal(isGauge, false);
  assert.equal(entities.core, 'sensor.brisket_core_temperature');
  assert.equal(entities.ambient, 'sensor.brisket_ambient_temperature');
  assert.equal(entities.instant, 'sensor.brisket_instant_read_temperature');
  assert.equal(entities.mode, 'sensor.brisket_mode');
  assert.equal(entities.ready_in, 'sensor.brisket_ready_in');
  assert.equal(entities.cook_target, 'sensor.brisket_cook_target');
  assert.equal(entities.prediction, 'sensor.brisket_prediction');
  assert.equal(entities.target, 'number.brisket_target_temperature');
  assert.equal(entities.battery, 'binary_sensor.brisket_battery');
  assert.equal(entities.overheating, 'binary_sensor.brisket_overheating');
  // Revert check: drop the `entity:` device-registry seed (device lookup) and
  // this falls to string-building. The regex extracting a serial from
  // `sensor.brisket_core_temperature` fails (it doesn't match the
  // predictive_thermometer_<serial>_ prefix), so the guessed IDs come out
  // malformed (e.g. "sensor.predictive_thermometer__ambient_temperature")
  // and every assertion above fails.
});

test('gauge resolved from a picked _temperature entity — isGauge true via _zone', () => {
  const device = { id: 'device-pit', identifiers: [['combustion', 'g000000123']] };
  const entitiesByDevice = {
    'device-pit': [
      'sensor.pit_gauge_temperature',
      'sensor.pit_gauge_zone',
      'binary_sensor.pit_gauge_sensor_connected',
      'binary_sensor.pit_gauge_overheating',
      'binary_sensor.pit_gauge_high_alarm',
      'binary_sensor.pit_gauge_low_alarm',
      'binary_sensor.pit_gauge_battery',
    ],
  };
  const hass = makeHass([device], entitiesByDevice);
  const { isGauge, entities } = resolveEntities(
    { entity: 'sensor.pit_gauge_temperature' },
    hass,
  );

  assert.equal(isGauge, true);
  assert.equal(entities.core, 'sensor.pit_gauge_temperature');
  assert.equal(entities.sensor_connected, 'binary_sensor.pit_gauge_sensor_connected');
  assert.equal(entities.overheating, 'binary_sensor.pit_gauge_overheating');
  assert.equal(entities.high_alarm, 'binary_sensor.pit_gauge_high_alarm');
  assert.equal(entities.low_alarm, 'binary_sensor.pit_gauge_low_alarm');
  assert.equal(entities.battery, 'binary_sensor.pit_gauge_battery');
  // Revert check: if the `_zone`-based capability check is removed (e.g.
  // reverted to guessing gauge/probe from serial shape), there is no serial
  // in this config at all, so isGauge would come from the legacy-fallback
  // path with an empty serial — and entities.core would be the malformed
  // guess "sensor.grill_gauge__temperature", not "sensor.pit_gauge_temperature".
});

test('legacy serial: resolved via device identifiers', () => {
  const hass = makeHass([RENAMED_PROBE_DEVICE], RENAMED_PROBE_ENTITIES);
  const { isGauge, entities } = resolveEntities({ serial: '10014030' }, hass);

  assert.equal(isGauge, false);
  assert.equal(entities.core, 'sensor.brisket_core_temperature');
  assert.equal(entities.ambient, 'sensor.brisket_ambient_temperature');
  // Revert check: if serial: no longer looks the device up by `identifiers`
  // and falls straight to string-building, entities.core would be the
  // guessed "sensor.predictive_thermometer_10014030_core_temperature" —
  // which does not exist in this (renamed) fixture and does not equal the
  // asserted value.
});

test('legacy serial: with no device in the registry falls back to string-building', () => {
  const hass = makeHass([], {});

  const probe = resolveEntities({ serial: '10007dc0' }, hass);
  assert.equal(probe.isGauge, false);
  assert.equal(probe.entities.core, 'sensor.predictive_thermometer_10007dc0_core_temperature');
  assert.equal(probe.entities.ambient, 'sensor.predictive_thermometer_10007dc0_ambient_temperature');
  assert.equal(probe.entities.target, 'number.predictive_thermometer_10007dc0_target_temperature');

  const gauge = resolveEntities({ serial: 'g000000123' }, hass);
  assert.equal(gauge.isGauge, true);
  assert.equal(gauge.entities.core, 'sensor.grill_gauge_g000000123_temperature');
  assert.equal(gauge.entities.high_alarm, 'binary_sensor.grill_gauge_g000000123_high_alarm');
  // Revert check: if the "no device found" branch were removed (e.g. an
  // unconditional `throw` or an empty {isGauge:false, entities:{}} return
  // whenever the registry has no match), every assertion above fails —
  // this is the one case that must still behave exactly like the original
  // string-concatenation code.
});

test('a Display device is never classified as a gauge (the T100000WW7 case)', () => {
  const device = { id: 'device-display', identifiers: [['combustion', 'T100000WW7']] };
  const entitiesByDevice = {
    'device-display': [
      'sensor.display_t100000ww7_rssi',
      'binary_sensor.display_t100000ww7_high_radio_power',
    ],
  };
  const hass = makeHass([device], entitiesByDevice);
  const { isGauge } = resolveEntities(
    { entity: 'sensor.display_t100000ww7_rssi' },
    hass,
  );

  assert.equal(isGauge, false);
  // Revert check: this is precisely the bug the brief describes.
  // `PROBE_SERIAL_RE.test('t100000ww7')` is false (alphanumeric, not 8 hex
  // chars), so the old "not probe-shaped means gauge" logic returns true
  // here. Capability-based detection (no `_zone` sibling) returns false —
  // reverting to the serial-shape test flips this assertion.
});

test('_core_temperature is not swallowed by the _temperature suffix match', () => {
  // A gauge device (has _zone) with a *decoy* entity ending in
  // `_core_temperature` inserted before the genuine `_temperature` entity,
  // to catch an implementation that scans for the first entity ending in
  // `_temperature` without excluding more specific suffixes.
  const device = { id: 'device-decoy', identifiers: [['combustion', 'decoy1']] };
  const entitiesByDevice = {
    'device-decoy': [
      'sensor.decoy_core_temperature', // decoy — probe-only suffix, listed first
      'sensor.decoy_temperature', // genuine gauge core reading
      'sensor.decoy_zone', // capability signal -> isGauge
    ],
  };
  const hass = makeHass([device], entitiesByDevice);
  const { isGauge, entities } = resolveEntities(
    { entity: 'sensor.decoy_temperature' },
    hass,
  );

  assert.equal(isGauge, true);
  assert.equal(entities.core, 'sensor.decoy_temperature');
  assert.notEqual(entities.core, 'sensor.decoy_core_temperature');
  // Revert check: replace assignRoles with a naive
  // `siblingIds.find(id => id.endsWith(suffix))` scan per role in array
  // order (no "longest match across all roles first" step) and this test
  // fails — the decoy is listed first and also ends with `_temperature`, so
  // it would be picked as `core` instead of the genuine entity.
});

test('config.entities overrides beat resolved values', () => {
  const hass = makeHass([RENAMED_PROBE_DEVICE], RENAMED_PROBE_ENTITIES);
  const { entities } = resolveEntities(
    {
      entity: 'sensor.brisket_core_temperature',
      entities: { ambient: 'sensor.some_other_ambient_override' },
    },
    hass,
  );

  assert.equal(entities.ambient, 'sensor.some_other_ambient_override');
  assert.equal(entities.core, 'sensor.brisket_core_temperature');
  // Revert check: apply the resolved defaults *after* the override merge
  // (i.e. swap the Object.assign order) and entities.ambient reverts to the
  // resolved "sensor.brisket_ambient_temperature", failing this assertion.
});

test('a device missing an optional entity leaves that role undefined rather than guessing an ID', () => {
  const device = { id: 'device-partial', identifiers: [['combustion', 'partial1']] };
  const entitiesByDevice = {
    'device-partial': [
      'sensor.partial_core_temperature',
      'sensor.partial_ambient_temperature',
      'binary_sensor.partial_battery',
      // no instant, mode, ready_in, cook_target, prediction, target, overheating
    ],
  };
  const hass = makeHass([device], entitiesByDevice);
  const { entities } = resolveEntities(
    { entity: 'sensor.partial_core_temperature' },
    hass,
  );

  assert.equal(entities.core, 'sensor.partial_core_temperature');
  assert.equal(entities.ambient, 'sensor.partial_ambient_temperature');
  assert.equal(entities.battery, 'binary_sensor.partial_battery');
  assert.equal(entities.prediction, undefined);
  assert.equal(entities.ready_in, undefined);
  assert.equal(entities.cook_target, undefined);
  assert.equal(entities.target, undefined);
  assert.equal(entities.mode, undefined);
  assert.equal(entities.instant, undefined);
  assert.equal(entities.overheating, undefined);
  // Revert check: if resolution fell back to (or was mixed with)
  // string-building for missing roles, entities.prediction would be the
  // guessed "sensor.predictive_thermometer_partial1_prediction" instead of
  // undefined — the current code must leave absent siblings unassigned, not
  // invent a placeholder ID.
});

test('select.<probe>_power_mode does not collide with sensor.<probe>_mode, in either registry order', () => {
  // Both entities live on the same probe device and both end in `_mode`
  // ("...power_mode" and "...mode"). Only the domain prefix ("select." vs
  // "sensor.") tells them apart — a suffix-only match would pick whichever
  // one Object.keys(hass.entities) happens to list first, which is registry
  // insertion order, not anything meaningful. Pin both orders explicitly so
  // this can't pass by luck.
  const device = { id: 'device-collide', identifiers: [['combustion', 'collide1']] };
  const modeSensor = 'sensor.collide_mode';
  const powerModeSelect = 'select.collide_power_mode';
  const coreEntity = 'sensor.collide_core_temperature';

  for (const order of [
    [coreEntity, modeSensor, powerModeSelect],
    [coreEntity, powerModeSelect, modeSensor],
  ]) {
    const hass = makeHass([device], { 'device-collide': order });
    const { entities } = resolveEntities({ entity: coreEntity }, hass);

    assert.equal(entities.mode, modeSensor, `order ${JSON.stringify(order)}`);
    assert.notEqual(entities.mode, powerModeSelect, `order ${JSON.stringify(order)}`);
  }
  // Revert check: drop the domain field from ROLE_SUFFIXES / assignRoles
  // (match by suffix alone, as the first implementation did) and this fails
  // on the `[coreEntity, powerModeSelect, modeSensor]` order — the select
  // entity is encountered first, also ends in `_mode`, and wins the role
  // before the real sensor is ever considered.
});

test('hasPopulatedRegistry: false for an empty or missing hass.entities, true once populated', () => {
  assert.equal(hasPopulatedRegistry(undefined), false);
  assert.equal(hasPopulatedRegistry({}), false);
  assert.equal(hasPopulatedRegistry({ entities: {} }), false);
  assert.equal(hasPopulatedRegistry({ entities: { 'sensor.x': {} } }), true);
  // Revert check: hardcode this to `return true;` (equivalent to the
  // pre-fix behaviour, where CombustionCard cached whatever the very first
  // `hass` write produced even off an empty registry) and the first three
  // assertions fail.
});

test('the editor picker filter excludes a probe\'s _surface_temperature reading', () => {
  // Real entities: a gauge device (core temperature + zone) and a probe
  // device that additionally exposes a virtual "Surface Temperature"
  // sensor — a real, shipped entity (see sensor.py CombustionVirtualSurfaceSensor)
  // that also happens to end in `_temperature`, same as the gauge's core
  // reading. A denylist-based filter has to know to exclude this suffix by
  // name; a capability check does not.
  const gaugeDevice = { id: 'device-pit', identifiers: [['combustion', 'g1']] };
  const probeDevice = { id: 'device-brisket', identifiers: [['combustion', 'p1']] };
  const hass = makeHass([gaugeDevice, probeDevice], {
    'device-pit': ['sensor.pit_temperature', 'sensor.pit_zone'],
    'device-brisket': ['sensor.brisket_core_temperature', 'sensor.brisket_surface_temperature'],
  });

  const gaugeIds = computeGaugeDeviceIds(hass);
  assert.equal(gaugeIds.has('device-pit'), true);
  assert.equal(gaugeIds.has('device-brisket'), false);

  const pick = (entityId) => isPickableCombustionEntity(entityId, hass.entities[entityId], gaugeIds);

  assert.equal(pick('sensor.pit_temperature'), true); // gauge core -> pickable
  assert.equal(pick('sensor.pit_zone'), true); // gauge zone -> pickable
  assert.equal(pick('sensor.brisket_core_temperature'), true); // probe core -> pickable
  assert.equal(pick('sensor.brisket_surface_temperature'), false); // probe secondary reading -> NOT pickable
  // Revert check: restore the original denylist implementation
  // (`id.endsWith('_temperature') && !id.endsWith('_ambient_temperature') &&
  // !id.endsWith('_instant_read_temperature') && !id.endsWith('_target_temperature')`)
  // and the last assertion fails — `_surface_temperature` isn't on that
  // denylist, so it's (wrongly) treated as pickable.
});

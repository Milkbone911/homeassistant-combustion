# S2b — optional MeatNet Cloud account link

**Stage:** S2b source implementation / authenticated-trial preparation  
**Vendor API status:** unofficial and reverse engineered  
**Deployment status:** not established by this document or CI

S2b connects the isolated S1 cloud client to the single Home Assistant
`combustion_meatnet` config entry without making local BLE/GATT setup depend
on cloud availability.

## Authentication boundary

Combustion's public setup documentation confirms that MeatNet Cloud accounts
are created and used through the official Combustion app, including
email/password, Google and Apple identity choices. Combustion's public
developer page documents the open Bluetooth SDKs/protocol but does not publish
a supported Home Assistant/cloud REST login contract.

The pinned CPT-Crawl evidence uses Firebase credentials obtained through an
operator-controlled bootstrap and explicitly has no standalone product login.
Accordingly, S2b **does not** implement or advertise "Sign in with Combustion",
OAuth callback ownership, Apple authorization handling, or password login.

The S2b UI is a credential-import bridge for development/qualified use:

- Firebase Web API key used by the vendor app;
- Firebase refresh token for the operator's MeatNet Cloud account.

Obtain both outside this repository and outside chat/public issue transcripts.
The flow masks both inputs. Home Assistant config-entry storage and its backups
are **not application-level encrypted-at-rest secret storage**.

Protocol evidence is pinned to CPT-Crawl
`fa1dd582a84aa570ed11c65a5cab1542ee3da411`; that source is unofficial and is
not vendor authorization for this integration.

## Implemented account contract

- There remains exactly one Home Assistant entry with unique ID
  `combustion_meatnet`.
- Cloud-first setup is possible when no local device is awake; later Bluetooth
  discovery finds the already-configured singleton.
- An existing local-only entry links cloud through Home Assistant reconfigure,
  not through a second config entry.
- Link validation performs a real Firebase refresh plus the Firestore
  association read through the bounded S1 client. The **server-returned
  Firebase subject** is authoritative; the user never types an account ID.
- Only the API key, refresh token, subject and `cloud_link_generation` are
  stored in config-entry data. Short-lived ID tokens are not persisted.
- Reauth requires the same authenticated subject and preserves link generation.
- Explicit replacement requires a different authenticated subject and advances
  link generation.
- Unlink removes active local credentials and advances generation. It does not
  claim provider revocation or delete remote/history data.
- A rotated refresh token is persisted with
  `hass.config_entries.async_update_entry`. S2a removed the config-entry update
  listener, so routine token persistence does **not** reload local BLE/GATT.
- A delayed old-generation token update is discarded and does not start reauth
  for a replacement account.

## Runtime fault containment

Local managers/platforms are constructed and started first. A linked account
gets one optional entry-owned background validation check afterward.

Sanitized cloud health states are:

- `unlinked`
- `checking`
- `ready`
- `reauth_required`
- `degraded`
- `compatibility_error`

An authentication failure starts Home Assistant's reauth flow but does not
raise `ConfigEntryAuthFailed` from the whole Combustion setup. Permission,
transport and schema failures remain cloud-specific health states. None changes
local device `last_seen`, availability, entity values or connected state.

Diagnostics are allowlisted to linked/status/probe-count/error-category/
verified-generation plus local active-connection/task counts. They do not
include API keys, refresh or ID tokens, Firebase subject, raw cloud bodies,
URLs, probe serials or account history.

## Source acceptance

S2b fixture/source acceptance requires:

- full Python/HA regression suite;
- Ruff, Hassfest, HACS and existing Node card tests;
- no external network dependency in tests;
- cloud-first singleton flow;
- existing-entry link without duplicate entry;
- same-subject reauth;
- different-subject explicit replacement with generation increment;
- local unlink;
- background token rotation without BLE reload;
- auth failure requests reauth while local runtime remains loaded;
- stale-generation rotation cannot mutate/recover the new link;
- diagnostics contain no synthetic secret/subject values.

## Evidence still required before calling S2b complete

CI proves source contracts only. S2b remains **not authenticated-live-validated**
until an authorized operator-controlled account trial establishes, without
placing credentials in chat/repository artifacts:

1. current bootstrap material can be obtained safely;
2. the real refresh endpoint returns the expected subject;
3. Firestore association discovery succeeds for the user's account;
4. expected probe serial associations are observed in a protected local record;
5. normal refresh-token rotation survives an HA restart;
6. revoked/invalid credentials trigger reauth while BLE remains healthy;
7. same-account reauth succeeds and different-account reauth is rejected;
8. explicit replacement is isolated by link generation;
9. logs/diagnostics remain redacted.

That trial is account/live evidence, not permission to start S3 storage or to
claim the unofficial vendor API is stable/supported.

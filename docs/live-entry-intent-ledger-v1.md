# Live Entry Intent Ledger V1

## Purpose

`live_entry_intents_v1` is immutable, append-only evidence that a producer
prepared one specific ENTRY order intent. It is not an order, fill, position,
inventory record, outcome, or mutable state machine.

LEI1A adds only the schema and pure domain model. It does not activate a
runtime writer, submit an order, consume the ledger from fill ingestion, or
create a position.

## Canonical contract

Accepted environments are `paper` and `live`. Accepted deployment identities
are `local-paper`, `local-live`, `vps-paper`, and `vps-live`. Aliases are
rejected rather than inferred.

The current SPOT LONG contract requires `order_purpose=ENTRY`, `side=BUY`, a
positive Decimal quantity, explicit adoption/generation attribution, and
`contract_version=LIVE_ENTRY_INTENT_V1`.

The slot identity is:

```text
{SYMBOL}:{STRATEGY}:{interval}
```

The deterministic UUIDv5 identity covers:

```text
environment, deployment_id, exchange_source, client_order_id
```

The SHA-256 content fingerprint covers all semantic content, including frozen
adoption/generation attribution and canonical Decimal quantity. `prepared_at`
is intentionally excluded so a retry of the same semantic intent can be
recognized even if its observation time differs.

## Idempotency

The natural key is:

```text
(environment, deployment_id, exchange_source, client_order_id)
```

Repository behavior required by LEI1B:

```text
missing row                         -> CREATED
same natural key, same fingerprint -> IDEMPOTENT_EXISTING
same natural key, other fingerprint -> CONFLICT (fail closed)
```

An implementation must not use `ON CONFLICT DO NOTHING` without reading and
comparing the stored fingerprint.

## Adoption validation

The database checks that `adoption_id`, `generation`, `environment`, and
`deployment_id` identify the same
`FEE_AWARE_INVENTORY_C2_2` adoption row. Attribution is frozen at intent
creation and must not be re-resolved from whichever generation is ACTIVE
later.

## Immutability and storage

Database triggers reject UPDATE and DELETE. Later order, reconciliation, fill,
position, and alert state belongs in separate projections or ledgers.

A row is expected to remain below 2 KiB. Besides the primary and natural-key
indexes, V1 has only three future-safe read indexes: slot/time,
exchange/client-order, and adoption/generation/time.

## Micro-patch plan

### LEI1A — schema and domain model

- Scope: enums, frozen value object, deterministic identity/fingerprint,
  append-only schema, validation, indexes, tests, documentation.
- Files: the migration, `common/entry_intent.py`, targeted unit/PostgreSQL
  tests, and this document.
- Runtime impact and flags: none.
- Dependencies: existing `runtime_contract_adoption_v2`.
- Rollback: no runtime rollback; unused additive objects can remain. Once
  evidence exists, it must not be deleted.
- Entry criteria: containment active and baseline clean.
- Exit criteria: isolated PostgreSQL contract and full suite pass; production
  table remains absent/unapplied.
- PAPER validation: pure unit and disposable PostgreSQL only.
- LIVE shadow: not applicable until LEI1B.
- VPS impact: none.

### LEI1B — runtime producer and ACK linkage

- Scope: shared repository/helper; create and commit intent before network;
  fail closed if commit fails; link ACK to intent without mutating intent.
- Files: new repository module, `common/execution.py`, four strategy producers,
  an additive order-link migration, tests, and documentation.
- Schema: `binance_orders.entry_intent_id` plus a separate ACK/order
  projection if needed; no fill changes.
- Feature mode: `OFF`, `SHADOW`, `ENFORCE`, default `OFF`.
- Dependencies: LEI1A schema review and rollout.
- Rollback: set `OFF`; retain immutable evidence.
- Entry criteria: LEI1A installed and empty on the target environment.
- Exit criteria: committed intent precedes every enabled send; failed intent
  commit proves zero network calls; ACK linkage is idempotent.
- PAPER validation: repository and mock exchange, then normal PAPER cycles
  without forced trades.
- LIVE shadow: containment remains active; synthetic no-send validation.
- VPS impact: none until a separate pull-only rollout approval.

### LEI1C — fill attribution consumer

- Scope: resolve attribution from linked intent/order; persist immutable fill
  evidence before any position; keep unapplied duplicate pending.
- Files: fill change-control/ingestion modules, additive fill evidence
  migration, targeted tests and docs.
- Schema: immutable fill observation/revision evidence and attribution
  linkage; no position creation.
- Feature mode: `OFF`, `SHADOW`, `ENFORCE`, default `OFF`.
- Dependencies: LEI1B order linkage.
- Rollback: set `OFF`; never delete evidence.
- Entry criteria: producer/ACK linkage validated.
- Exit criteria: position-independent fill evidence is exactly-once and an
  unapplied replay never reports applied success.
- PAPER validation: mock exchange plus synthetic evidence replay.
- LIVE shadow: read-only comparison while containment remains active.
- VPS impact: none until separate approval.

### LEI1D — idempotent position creation

Position creation should remain a separate patch. It will project one
position after the first authoritative fill, aggregate partial fills by
high-water, emit transactional lifecycle evidence, and preserve the unresolved
slot guard. Combining it with LEI1C would make rollback and failure attribution
materially less precise.

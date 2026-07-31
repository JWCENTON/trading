# Live Entry Submission and ACK Linkage V1 (LEI1B)

## Boundary

LEI1B adds a committed-before-network admission boundary and immutable exchange
ACK linkage.  It does not ingest fills, create or update positions, repair
legacy incidents, classify learning outcomes, or change signal, sizing, exit,
or PAPER simulation semantics.

The three records have deliberately separate responsibilities:

1. `live_entry_intents_v1` — immutable semantic ENTRY intent (LEI1A);
2. `live_entry_submissions_v1` — immutable claim of the single V1 network
   submission attempt;
3. `live_entry_order_acks_v1` — immutable linkage from the committed intent and
   attempt to the exchange order identity.

No mutable order/fill/position status is stored in the intent ledger.  The ACK
stores only the exchange status observed at acknowledgement or bounded CID
recovery time; it is not an order-status projection.

## Mode contract

`LIVE_ENTRY_SUBMISSION_MODE` accepts exactly `OFF`, `SHADOW`, or `ENFORCE`.
Missing configuration defaults to `OFF`; an unknown value fails configuration
validation.

- `OFF`: no LEI1B repository work; the existing callback behavior is retained.
- `SHADOW`: intent evidence is best effort and never controls network
  admission.  It is intended only for approved PAPER/disposable validation;
  LIVE containment remains the controlling outer boundary.
- `ENFORCE`: a successful committed intent and committed attempt claim are
  mandatory before the one allowed network callback.  This mode is not enabled
  by the migration and must not be activated by this patch rollout.

## Canonical ENFORCE transaction sequence

Every repository method owns and closes a dedicated connection.  No network
call runs in an open repository transaction.

```text
deterministic CID and immutable intent
  -> INSERT/read/compare intent fingerprint
  -> COMMIT intent transaction
  -> if CREATED: INSERT/read/compare deterministic attempt claim
  -> COMMIT attempt transaction
  -> only attempt CREATED may call the exchange
  -> persist immutable ACK in a third committed transaction
```

An `IDEMPOTENT_EXISTING` intent with no ACK never claims and sends.  It first
performs an exact exchange lookup by the original client order ID.  This covers
the crash-after-intent-commit boundary and prevents a concurrency loser from
claiming an attempt ahead of the transaction that created the intent.

The V1 attempt ordinal is constrained to `1`.  NOT_FOUND is not an implicit
retry authorization; a later versioned policy must explicitly authorize any
second network attempt.

## Identity and idempotency

The submission attempt UUID is deterministic over:

```text
intent_id, attempt_ordinal=1, LIVE_ENTRY_SUBMISSION_V1
```

Its fingerprint freezes the intent fingerprint, environment/deployment,
adoption/generation, runtime Git SHA, original CID, exchange, slot, ENTRY/BUY,
quantity, and logical producer.  `submitted_at` is retry metadata and is not in
the fingerprint.

The ACK UUID is deterministic over:

```text
intent_id, LIVE_ENTRY_ORDER_ACK_V1
```

Its fingerprint freezes the complete intent/attempt attribution plus exchange
order ID and observed exchange status.  Observation time, recovery pathway,
and persistence producer are excluded so direct and exact-CID-recovered
evidence for the same ACK are idempotent.

Repository outcomes are explicit:

```text
intent:     CREATED | IDEMPOTENT_EXISTING | CONFLICT
attempt:    CREATED | IDEMPOTENT_EXISTING | CONFLICT
ACK:        PERSISTED | IDEMPOTENT_EXISTING | CONFLICT
CID lookup: FOUND | NOT_FOUND | AMBIGUOUS | ERROR
```

The ACK table has one canonical row per intent/original CID.  A different
exchange order ID or payload under that identity is `CONFLICT`; the prior row
is retained and never overwritten.

## Failure and recovery contract

| Boundary | V1 behavior |
|---|---|
| Intent insert/transaction fails | rollback, `ENTRY_INTENT_COMMIT_FAILED`, zero network calls |
| Intent fingerprint conflict | fail closed, zero network calls |
| Intent commit result unknown | fail closed, zero network calls; CID recovery required |
| Crash after intent commit, before send | retry performs CID lookup first; never blind-sends |
| Attempt claim fails or is unknown | fail closed, zero network calls |
| Send returns ACK, ACK persistence fails | exact CID lookup, persist recovered ACK; never second send |
| Send times out/has uncertain result | exact CID lookup |
| CID FOUND | require the committed attempt and persist recovered ACK |
| CID NOT_FOUND | explicit retry policy required; V1 blocks |
| CID AMBIGUOUS or ERROR | fail closed |
| Duplicate ACK fingerprint | idempotent no-op |
| Different ACK under same intent/CID | conflict, prior audit evidence preserved |

Recovery never searches by symbol and time alone.  The adapter is called with
the original producer CID; exchange-specific wire normalization is internal to
the adapter and does not replace the persisted identity.

## Runtime attribution and isolation

Before constructing an intent, the shared repository can resolve exactly one
ACTIVE `FEE_AWARE_INVENTORY_C2_2` adoption for the requested environment and
deployment.  The adoption Git revision must exactly match the runtime SHA.
Zero rows, multiple rows, deployment/environment mismatch, or SHA mismatch
fail closed.

Submission and ACK triggers revalidate every duplicated attribution field
against the immutable intent and adoption row.  ACK triggers additionally
validate the committed attempt.  PAPER and LIVE deployment identities cannot
cross.

All four strategies use the same shared execution contract.  The schema does
not activate a writer or feature mode, and the migration performs no backfill
or DML against orders, fills, or positions.

## Events

The shared flow emits these explicit outcomes to its injected event sink:

```text
ENTRY_INTENT_CREATED
ENTRY_INTENT_IDEMPOTENT_EXISTING
ENTRY_INTENT_CONFLICT
ENTRY_INTENT_COMMIT_FAILED
ENTRY_NETWORK_BLOCKED_NO_COMMITTED_INTENT
ENTRY_SUBMISSION_ATTEMPTED
ENTRY_ACK_PERSISTED
ENTRY_ACK_RECOVERED_BY_CLIENT_ORDER_ID
ENTRY_ACK_CONFLICT
ENTRY_SUBMISSION_AMBIGUOUS
```

Event transport cannot move the database/network boundary.  The intent,
attempt, and ACK ledgers remain canonical evidence.

## Migration and rollback

The additive migration is
`20260731_live_entry_submission_ack_v1.sql`.  Its checked object and checksum
manifest is `20260731_live_entry_submission_ack_v1_manifest.json`.  It may be
run twice, creates no rows, and installs no triggers on large existing trading
tables.

`20260731_live_entry_submission_ack_v1_rollback.sql` is safe only while both
LEI1B ledgers are empty.  It succeeds on an absent/empty schema and fails closed
on partial schema or any real attempt/ACK evidence.  LEI1A intent evidence and
the append-only migration history are never deleted by this rollback.

## Next boundary

LEI1C may consume this immutable intent/order linkage to attribute exchange
fill observations.  It must retain unapplied evidence as unapplied, distinguish
true applied duplicates, and must not create positions; idempotent position
projection remains LEI1D.

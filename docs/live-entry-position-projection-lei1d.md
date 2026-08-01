# LEI1D immutable fill position projection

LEI1D completes the forward LIVE entry evidence chain without changing the
default runtime. `LIVE_ENTRY_POSITION_PROJECTION_MODE` is `OFF` when unset.
No migration activates the feature and there is no historical backfill.

## Existing ownership audit

LEI1A creates `live_entry_intents_v1` before network admission. LEI1B commits
the single submission claim and immutable exchange ACK in separate owned
transactions. LEI1C commits immutable fill evidence, then an append-only
application decision in a later transaction so the evidence/application crash
window is retryable. `APPLIED` and `TRUE_DUPLICATE_APPLIED` mean that the
immutable evidence has exact committed local-fill proof; they do not mean that
a position was projected.

The existing LIVE entry-side position writers are the synchronous strategy
ACK writer and `common.entry_fill_reconciliation`. Under LEI1D `ENFORCE`, an
ACK is returned as awaiting immutable-fill projection and the legacy pending
entry reconciler is not invoked. The new projector therefore extends the
existing ingest/reconciliation integration point instead of running beside it.
OFF and SHADOW retain the legacy writer path.

## Identity and transaction boundary

The canonical lifecycle identity is the immutable LEI1A `intent_id`. It is
uniquely linked to exactly one `positions.id` by both
`positions.entry_intent_id` and `live_entry_position_projections_v1`. Exchange
order ID, client order ID, submission ID and ACK ID are retained as immutable
provenance, but slot/time similarity never establishes ownership.

The ingest transaction calls the projector after LEI1C/local-fill proof is
committed. The projector takes an intent advisory transaction lock and a slot
lock for first creation, locks its durable projection row, mutates the position,
advances the high-water, links immutable fills and inserts the outbox event in
one caller-owned transaction. It never commits internally.

## Eligibility and high-water

Only the latest LEI1C decision for an exact fill can project, and only when it
is `APPLIED` or `TRUE_DUPLICATE_APPLIED` with `BOT_OWNED_ATTRIBUTED` or
`BOT_OWNED_MISSING_POSITION` complete LEI1A/B lineage. Observation-only,
external/manual, ambiguous, correction and conflict evidence cannot mutate a
trusted position.

The invariant is:

```
newly_applied_entry_qty =
  canonical_cumulative_eligible_entry_qty - projected_gross_entry_qty
```

`delta <= 0` is a no-op. A decreasing cumulative value is a fail-closed
high-water conflict. Gross quantity, base-denominated entry fee, net entry
inventory and remaining inventory use PostgreSQL `NUMERIC`/Python `Decimal`.
Weighted entry price is `sum(fill_qty * fill_price) / sum(fill_qty)`.

## Lifecycle

The first positive projection inserts `POSITION_OPENED` into the existing C2.2
transactional lifecycle outbox. A partial unique index on position and event
kind makes the event exactly once independently of retry or cumulative value.
Later partial fills update the same position and never insert another open
event. Exit lifecycle semantics and Financial Truth read models are unchanged.

## Future rollout boundary

The future operational sequence is separate: apply schema to LOCAL PAPER test
harness only after a PAPER adapter exists, validate SHADOW read-only evidence,
then qualify forward LIVE evidence before any ENFORCE activation. This commit
performs none of those operations.

# Canonical Decision Observation Durable Transport V1

Status: review-only foundation; runtime defaults OFF, shadow OFF, auto-apply OFF, kill switch ACTIVE.

## Architecture and single writer

Finalized `FinalDecision` values are converted without mutation and appended to PostgreSQL
`causal_decision_observation_outbox_v1`. The table is durable, append-only in its semantic fields,
and unique on `(deployment_id, decision_key)`. Only processing metadata is mutable. The
automation-runner is the sole consumer and sole writer of `causal_decision_observation_v1` and
its observation-only Replay/Warehouse projections. Bot processes never write causal repositories.

Delivery is at-least-once. Claims use bounded `FOR UPDATE SKIP LOCKED` batches ordered by
`decision_created_at, inserted_at, event_id`. Duplicate decisions with identical payload are
idempotent; changed payload becomes `IDEMPOTENCY_CONFLICT`. No global business ordering is
claimed. Slot order is recovered deterministically from deployment, strategy, symbol, interval,
decision time, insertion time, and event id.

## Transaction and failure boundaries

The producer must be called only after a decision is finalized. It returns the same object and
fails open: a bounded DB write failure is logged and counted while trading continues. NO_TRADE,
HOLD, blocked, TRADE and EXIT do not depend on a position or order.

No common transactional decision sink currently spans all four bots. Therefore production
producer wiring is deliberately absent. Until a central sink owns the relevant local persistence
transaction, a crash after finalization but before append is a **KNOWN PRODUCER ATOMICITY GAP**.
At-least-once delivery starts only after a successful outbox insert; a crash after append loses
nothing. TRADE outcome linkage must carry `decision_key` through order intent,
order and position; symbol/time heuristics are forbidden and outcome wiring is deferred.

## Retry, recovery and poison events

Statuses are PENDING, PROCESSING, RETRY, PROCESSED, DEAD_LETTER and IDEMPOTENCY_CONFLICT.
Stale PROCESSING leases return to RETRY. Repository insertion and processed marking occur in one
consumer transaction, so a crash rolls both back; a repeated delivery remains idempotent. Retry
has configured delay and limit. Exhausted or conflicting events retain their payload permanently
and increment an alertable metric. Pending events are never deleted automatically.

## Flags and kill switch

`CAUSAL_DECISION_OBSERVATION_ENABLED`, `CAUSAL_SHADOW_OBSERVATION_ENABLED`,
`CAUSAL_LEARNING_AUTO_APPLY`, `CAUSAL_LEARNING_KILL_SWITCH`, `DEPLOYMENT_ID`,
`CAUSAL_OUTBOX_BATCH_SIZE`, `CAUSAL_OUTBOX_RETRY_LIMIT`, and
`CAUSAL_OUTBOX_RETRY_BACKOFF_SECONDS` are validated by the shared contract. Valid identities are
local-live, local-paper, vps-live and vps-paper. Kill switch blocks producer writes, claims and all
causal downstream work without blocking trading; pending events remain pending.

Shadow OFF means baseline observation plus observation-only Replay/Warehouse projection only:
no attribution lookup, would-trade, recommendation snapshot, activation, experiment, TREATMENT,
promotion consumption or counterfactual outcome write.

## Observability

Metrics cover created, processed, retry, dead-letter and conflict totals; oldest pending age;
last poll and last successful batch; batch in progress and duration; and producer write failures.
Structured producer errors include event id, decision key, deployment, source, status, attempt and
error code, never the payload. Consumer poll health is independent of the long automation loop
heartbeat. Docker healthcheck is intentionally unchanged.

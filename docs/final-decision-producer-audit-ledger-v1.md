# FinalDecision Producer Audit Ledger V1

## Purpose

`final_decision_producer_audit_v1` is the durable, append-only denominator for
the boundary between an immutable `FinalDecision` and the causal observation
outbox. It records every non-`None` final result before observation feature
guards, then records either a legal skip or one producer attempt and its
classified outcome.

The ledger supports these closed-window equations:

```text
FINALIZED
= SKIPPED_DISABLED
 + SKIPPED_KILL_SWITCH
 + PRODUCER_ATTEMPTED

PRODUCER_ATTEMPTED
= ACCEPTED
 + IDEMPOTENT_EXISTING
 + IDEMPOTENCY_CONFLICT
 + VALIDATION_REJECTED
 + SERIALIZATION_FAILED
 + OUTBOX_WRITE_FAILED
 + in-flight
```

For a stable closed window, `in-flight = 0` and every equation difference must
be zero before observation coverage can pass.

## Non-goals

The ledger does not change or evaluate strategy output, order or position
state, execution, risk, ORC, MME, Learning recommendations, promotion,
treatment, outcome effectiveness, or auto-apply. It does not backfill
historical decisions. It does not change the existing causal kill-switch
contract or the observation decision-kind mapper.

## Feature flag

`FINAL_DECISION_PRODUCER_AUDIT_LEDGER_ENABLED` is independent of observation,
shadow, kill-switch, and auto-apply flags. Its safe default is `0`.

- ledger OFF: no ledger connection or write; legacy runtime behavior;
- ledger ON, observation OFF: `FINALIZED -> SKIPPED_DISABLED`;
- ledger ON, kill-switch ON: `FINALIZED -> SKIPPED_KILL_SWITCH`;
- ledger ON, producer active: `FINALIZED -> PRODUCER_ATTEMPTED -> outcome`.

Enabling the flag requires an already-applied schema and a separately
authorized environment rollout.

## Event model and state machine

Every transition is a new immutable row:

```text
FINALIZED
  +-- SKIPPED_DISABLED
  +-- SKIPPED_KILL_SWITCH
  `-- PRODUCER_ATTEMPTED
        +-- ACCEPTED
        +-- IDEMPOTENT_EXISTING
        +-- IDEMPOTENCY_CONFLICT
        +-- VALIDATION_REJECTED
        +-- SERIALIZATION_FAILED
        `-- OUTBOX_WRITE_FAILED
```

V1 makes one synchronous producer attempt per finalizer invocation. It does
not persistently retry failed producer attempts. A future retry must use a new
positive `attempt_ordinal` and its deterministically derived `attempt_id`;
replaying the same V1 attempt remains idempotent and uses ordinal 1.

`PRODUCER_ATTEMPTED` without a terminal row is an observable in-flight/crash
gap. `ACCEPTED` is emitted only after a newly inserted outbox row commits.
`IDEMPOTENT_EXISTING` is emitted only after the existing outbox payload is
confirmed identical. A conflict is terminal and alertable.

## Identity and idempotency

- FinalDecision identity: `(deployment_id, decision_key)`.
- `decision_key`: the existing deterministic producer key.
- Finalized identity: UUIDv5 of deployment, decision key, `FINALIZED`, and
  payload version.
- Attempt identity: UUIDv5 of finalized event ID and attempt ordinal.
- Audit event identity: UUIDv5 of finalized event ID, attempt or legal-skip
  identity, event type, and payload version.
- Outbox identity: the existing deterministic outbox UUID.
- Semantic digest: content evidence only; never a unique identity.

The table primary key prevents duplicate audit events. A partial unique index
enforces one `FINALIZED` row per `(deployment_id, decision_key)`. Another
partial unique index enforces one event of each type per attempt. Re-inserting
the exact event is an idempotent no-op. The writer compares a bounded event
digest after every insert/no-op; the same identity with different content is a
ledger identity conflict and degrades denominator health.

Equivalent decision keys in different deployments are legal and isolated.

## SYSTEM_NOT_EVALUATED

The ledger stores both `original_decision_type` and `decision_kind`.
`original_decision_type` remains `SYSTEM_NOT_EVALUATED`. `decision_kind`
retains the current observation mapper result (`NO_TRADE`) to avoid changing
the observation contract in this patch. Audits must group primarily by
`original_decision_type`, so non-evaluations cannot disappear analytically
inside business `NO_TRADE`.

## Fail-open and health semantics

Ledger failures never change the returned `FinalDecision` and never suppress
or alter the configured observation producer. A failed append:

1. increments the process-local denominator failure counter;
2. sets process-local health to `DENOMINATOR_DEGRADED`;
3. emits a structured, rate-limited error without payloads or secrets;
4. causes observation coverage PASS to be forbidden for the affected runtime
   interval.

The health signal is deliberately small and in-process in V1; durable
completeness comes from the ledger itself. Operators must combine ledger
equations with absence of `DENOMINATOR_DEGRADED` diagnostics.

## Atomicity and connection behavior

The lifecycle does not use a distributed transaction:

1. one ledger connection appends and commits `FINALIZED`;
2. the same ledger connection appends and commits a legal skip or
   `PRODUCER_ATTEMPTED`;
3. the existing producer connection performs and commits outbox work;
4. the ledger connection appends and commits the classified outcome;
5. both connections close.

This is normally two or three small ledger inserts per final decision using
one ledger connection, plus the existing producer connection. Separate commits
make `FINALIZED` durable before guards and make crash gaps measurable. An
`ACCEPTED` ledger event can lag a committed outbox row after a process crash,
but can never precede one. The audit reports that lag as in-flight rather than
claiming atomic exactly-once behavior.

## Database schema

The additive PostgreSQL table stores UUID identities, explicit deployment and
slot identity, original and mapped decision classification, bounded error
classification, semantic digest, optional outbox linkage, timestamps, payload
version, attempt ordinal, and an event digest. It stores no complete
`FinalDecision` payload and no traceback.

Rows are protected by a trigger that rejects every `UPDATE` and `DELETE`.
Indexes support time-window equations, decision lookup, deployment/time,
event/status time, source/time, slot/time, and outbox linkage. Retention is
indefinite in V1. Deletion, partitioning, or archival requires a later reviewed
policy and must preserve frozen audit windows.

## Error classification

- `VALIDATION_REJECTED`: unsupported/missing deployment, decision deployment
  mismatch, environment mismatch, or event contract validation.
- `SERIALIZATION_FAILED`: event payload conversion, canonical JSON, or digest
  construction failure.
- `OUTBOX_WRITE_FAILED`: connection, SQL, transaction, commit, or outbox read
  failure.
- `IDEMPOTENCY_CONFLICT`: same deployment/decision identity with a different
  outbox payload.

Error rows contain only a stable bounded `error_class`, not exception messages,
tracebacks, credentials, or connection strings.

## Audit dimensions and queries

Canonical queries live in
`docs/final-decision-producer-audit-queries-v1.sql`. They freeze a literal
half-open time window and report equations and lifecycle gaps by deployment,
environment, source service, strategy, symbol, interval, original decision
type, mapped decision kind, and UTC hour.

The denominator for eligible business decisions is `FINALIZED` filtered by the
explicit original decision types approved by the audit contract. It must never
be reconstructed from outbox rows.

## Rollout gates

Before enabling in any environment:

1. tracked worktree and schema fingerprint are approved;
2. migration and PostgreSQL contract tests pass;
3. runtime image contains the exact approved commit;
4. ledger flag is present but OFF by default;
5. observation and causal kill-switch semantics are unchanged;
6. no order, position, risk, ORC, MME, Learning, or promotion diff exists.

Enable LOCAL PAPER only in a separate task. Require natural data, zero
unclassified gaps after scheduler latency, zero ledger conflicts, no
`DENOMINATOR_DEGRADED`, stable strategy cadence, and zero trading mutation
before advancing to LOCAL LIVE, GitHub, VPS PAPER, and VPS LIVE.

## Rollback

Set `FINAL_DECISION_PRODUCER_AUDIT_LEDGER_ENABLED=0` and recreate only the
separately authorized strategy runtime. Do not drop the table or delete ledger
history. Observation enablement and the causal kill switch remain independent.
Code or schema rollback is not required to stop ledger writes.

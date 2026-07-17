# FinalDecision Causal Observation Producer Wiring V1

## Purpose and scope

RSI, TREND, SUPERTREND and BBRANGE pass their already-finalized immutable
`FinalDecision` through one shared fail-open epilogue. The epilogue may append
an observation event to `causal_decision_observation_outbox_v1`; it cannot
change a trading decision or write orders, fills, positions, `bot_control`,
recommendation state, experiments or counterfactual outcomes.

The strategy implementation remains the authority for signal, sizing,
execution and side-effect order. Existing return paths execute unchanged
inside a private runtime function. The public runtime entrypoint calls
`finalize_decision_observation` once after that function and its `finally`
block have completed. The returned object is the identical `FinalDecision`.
Legacy cycles with no candle row and therefore no canonical candle identity
continue to return `None` and cannot produce an observation.

## Flags and deployment identity

Repository defaults remain:

```text
CAUSAL_DECISION_OBSERVATION_ENABLED=0
CAUSAL_SHADOW_OBSERVATION_ENABLED=0
CAUSAL_LEARNING_AUTO_APPLY=0
CAUSAL_LEARNING_KILL_SWITCH=1
DEPLOYMENT_ID unset
```

Disabled observation and an active kill switch return immediately without
constructing DB work, opening a connection, running a query or logging a
configuration error. When enabled, `DEPLOYMENT_ID` must be one of
`local-live`, `local-paper`, `vps-live`, `vps-paper`; it must equal the
deployment embedded in `FinalDecision`, and its LIVE/PAPER class must match
the decision environment. Unknown and `legacy-unknown` are never inferred.
Invalid configuration is a bounded fail-open diagnostic and creates no row.

Shadow observation and auto-apply remain off. This wiring does not consume a
recommendation and does not create activation, treatment, would-trade or
outcome records.

## Identity, durability and failure model

The existing canonical builder derives `decision_key` from deployment,
environment, slot, candle, finalization time and decision classification. The
existing event adapter derives the semantic digest and payload hash. A retry
of the same finalized decision produces the same key, event UUID and hashes.
Deployment is part of identity, so equivalent local-paper and vps-paper
decisions remain isolated. An identical retry is an idempotent no-op; a
different payload for the same key is marked `IDEMPOTENCY_CONFLICT` without
changing trading.

Producer and database failures are fail-open. They are logged and counted as
telemetry failures while the original decision is returned unchanged.

This pipeline is not exactly-once. `FinalDecision` may be finalized before the
outbox insert. A process crash in that gap can lose the observation while
leaving the trading decision intact. The guarantees are:

```text
at-most-one producer attempt per runtime finalization path
idempotent outbox identity
durable at-least-once delivery after successful outbox insert
idempotent consumer and observation-only projections
```

The automation-runner consumer remains the single observation writer. It
creates only `causal_decision_observation_v1` and baseline
`OBSERVATION_ONLY` Replay/Warehouse projections carrying
`NO_ACTIVE_RECOMMENDATION`.

## Monitoring and failure modes

Monitor created, processed, retry, dead-letter, idempotency-conflict and write
failure counters, oldest pending age, last successful consumer poll and batch
duration. Expected fail-open reasons include observation disabled, active kill
switch, missing/invalid/mismatched deployment, environment mismatch and DB
unavailability. None authorizes a trading mutation.

## Rollout and rollback

No rollout is performed by this change. A future rollout sequence is:

1. verify migrations and schema fingerprint on an isolated disposable DB;
2. keep auto-apply, attribution and treatment disabled;
3. validate default-off runtime neutrality;
4. enable only **LOCAL PAPER SHADOW OBSERVATION** under the kill switch;
5. measure decision coverage, conflicts, retries and projection neutrality;
6. stop by restoring `CAUSAL_DECISION_OBSERVATION_ENABLED=0` or activating the
   kill switch.

Rollback is flag-only; code rollback is not required to stop writes. No
service restart, flag change or database migration is part of this task.

## Known limitations and next phase

The outbox append does not share the transaction that persists every trading
side effect, so the pre-insert crash gap remains. Cycles without a canonical
candle identity are not observable. Completeness must be demonstrated before
adding frozen recommendation context.

The next phase is **LOCAL PAPER SHADOW OBSERVATION**, not a recommendation
experiment.

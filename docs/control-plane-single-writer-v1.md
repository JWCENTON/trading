# Control Plane Single Writer V1

Status: implementation decision record
Candidate base: `9a818392904fc0b7bbf9da29139ff06039a9e736`

## Decision

Authoritative desired-state writer:

```text
automation-runner
```

Process supervision:

```text
services/bot_runner
```

The `bot-runner-orchestrator` service is an observer of control, risk, market,
and process health. It may persist its own metrics and decision telemetry, but
it must not calculate or persist an independent `bot_control` desired state.

## Desired state

`bot_control` is the durable desired-state contract for each
`symbol × interval × strategy` slot:

- `enabled`: whether a strategy process should exist;
- `live_orders_enabled`: whether an existing process may submit LIVE entries;
- `regime_enabled` and `regime_mode`: regime-gate desired state;
- `control_mode` and `control_source`: ownership boundary;
- `reason`: authoritative policy explanation.

Automation-runner derives this state from ORC/policy views and materializes it
in one transaction. It uses `IS DISTINCT FROM` predicates so unchanged state
does not create audit noise.

## Actual state

`services/bot_runner` reads `bot_control` and reconciles operating-system
processes:

- enabled and absent: start in deterministic order;
- disabled or removed and running: stop;
- enabled and running: no action;
- enabled process exited: restart after the existing backoff;
- repeated reconciliation: idempotent.

The process supervisor never writes `bot_control`.

## Ownership boundaries

| Component | May write bot_control | Role |
|---|---|---|
| automation-runner ORC apply | yes | authoritative automatic desired state |
| bot-runner | no | actual-process reconciliation |
| bot-runner-orchestrator | no | risk/control observer and telemetry |
| strategy workers | insert missing default only; mode safety remains legacy | runtime consumer |
| authenticated admin API | yes | explicit manual/admin authority |
| Learning Engine | no | SHADOW recommendations only |

Manual `control_mode=MANUAL` rows remain outside automatic ORC mutation.

## Concurrency

The authoritative apply obtains PostgreSQL transaction advisory lock
`917263003` using `pg_try_advisory_xact_lock`. If another apply is active, the
second apply exits without calculating or writing desired state. The lock is
released automatically on commit/rollback/connection loss.

The existing due timestamp remains the cadence/idempotency gate after the
concurrency lock.

## Audit behavior

`bot_control_audit` continues to capture full before/after rows, timestamp,
reason, and `control_source`. Automatic ORC mutations now originate from one
runtime family: `ORC_INTEGRATION_V2`.

Expected properties:

- one logical OFF→ON decision produces at most one logical mutation;
- one logical ON→OFF decision produces at most one logical mutation;
- unchanged state produces no mutation;
- orchestrator reconciliation produces no `bot_control_audit` row.

Policy version and picks hash remain available in automation-runner apply
statistics. A future observability-only change may add explicit writer metadata
to audit records, but no fencing generation is required while physical
single-writer ownership is maintained.

## Failure modes

- Automation-runner unavailable: desired state remains at the last committed
  value; process supervisor continues reconciling that value.
- Process supervisor unavailable: desired state remains durable; restart
  reconstructs processes from `bot_control`.
- Concurrent automation apply: one transaction obtains the advisory lock; the
  other skips.
- Automation transaction failure: PostgreSQL rollback preserves the previous
  complete desired state.
- Observer/orchestrator restart: metrics may be replayed, but desired state is
  never changed.

## Restart behavior

Restarting bot-runner reads current `bot_control` and reconstructs actual
workers. It has no cached desired generation to restore and cannot overwrite a
newer policy decision.

Restarting bot-runner-orchestrator cannot mutate desired state.

## Rollback

Rollback is code/image based:

1. preserve current service image tags before rollout;
2. deploy automation-runner, bot-runner, and bot-runner-orchestrator only as
   required by their changed images;
3. if validation fails, restore the previous images;
4. do not rewrite `bot_control` during rollback;
5. verify worker parity against the durable desired state.

## Rollout plan (not executed)

1. LOCAL LIVE pre-state: Git SHA, images, container IDs, restart counts,
   `bot_control` snapshot, 24h audit churn, transitions, locks, idle
   transactions, runtime errors.
2. Preserve rollback images.
3. Build changed services only.
4. Roll out LOCAL LIVE with minimal service scope.
5. Validate writer-family churn, OFF→ON/ON→OFF, worker parity, heartbeats,
   restarts, locks, idle transactions, tracebacks, and HTTP 429.
6. Repeat on LOCAL PAPER.
7. After stability and review, commit/push under a separate instruction.
8. VPS LIVE pull-only, targeted rollout, validation.
9. VPS PAPER rollout and validation.

The rollout gate is zero new `ORC_V2` bot-control reasons and no alternating
`ORC_V2 ↔ ORC_INTEGRATION_V2` audit pattern.

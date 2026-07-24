# FinalDecision Producer Audit Ledger V1 rollout

This plan prepares rollout but authorizes none. The feature remains OFF until a
separate environment-specific task explicitly changes runtime configuration.

## Preflight

1. Require `main`, clean tracked worktree, and exact approved commit/image SHA.
2. Verify the additive migration and fingerprint on a disposable PostgreSQL
   database.
3. Run ledger, transport, FinalDecision, strategy, Learning/replay/warehouse,
   full-suite, compile, and diff checks.
4. Confirm no strategy, execution, order, position, risk, ORC, MME, Learning
   apply, or promotion diff.
5. Apply the migration using the normal separately authorized schema process.
6. Keep `FINAL_DECISION_PRODUCER_AUDIT_LEDGER_ENABLED=0`.

## Activation order

The reviewed order is:

```text
LOCAL PAPER
LOCAL LIVE
push approved Git commit
VPS PAPER
VPS LIVE
```

Only LOCAL PAPER may be the first runtime activation. VPS remains pull-only.
Every transition requires a separate task and evidence review.

## LOCAL PAPER gate

Before activation, require schema readiness, healthy strategy workers, exact
deployment identity, zero outbox retry/dead-letter/conflict backlog, and frozen
pre-state counts for orders, positions, fills, bot control, ORC, Learning, and
promotion.

Enable only:

```text
FINAL_DECISION_PRODUCER_AUDIT_LEDGER_ENABLED=1
```

Do not change causal observation, shadow, kill-switch, auto-apply, execution,
or strategy flags as part of ledger activation.

After controlled runtime recreation, require:

- exact image and environment identity;
- natural events from RSI, TREND, SUPERTREND, and BBRANGE;
- one `FINALIZED` per decision identity;
- exactly one branch per finalized event;
- exactly one terminal result per producer attempt after bounded latency;
- durable `SKIPPED_DISABLED` or `SKIPPED_KILL_SWITCH` when those guards apply;
- zero ledger identity conflicts;
- no `DENOMINATOR_DEGRADED` diagnostic;
- accepted terminal links resolve to real outbox rows;
- unchanged trading and control-plane invariants.

Use one frozen half-open window and the canonical audit queries. Do not derive
the denominator from outbox rows.

## Failure interpretation

- missing FINALIZED: denominator failure; coverage PASS prohibited;
- FINALIZED without branch: finalizer crash or ledger append failure;
- attempt without terminal: in-flight/crash gap or terminal append failure;
- validation/serialization/outbox failure: classified fail-open producer loss;
- audit identity conflict: hard audit-contract violation;
- `DENOMINATOR_DEGRADED`: ledger evidence is incomplete even if transport is
  otherwise healthy.

Any failed gate stops advancement. Do not backfill missing historical events.

## Performance observation

Measure event rate, average row size, ledger/outbox DB connection rate, insert
latency, strategy loop duration, DB locks, and index growth. Expected steady
cost is two rows for a legal skip or three rows for an active producer
lifecycle. One ledger connection is reused within each lifecycle.

## Rollback

Set the ledger flag to `0` and recreate only the separately authorized strategy
runtime. Preserve the table and all rows. Do not drop schema, delete events,
change the causal kill switch, or alter observation transport. Re-run worker,
execution, and control-plane invariants after rollback.

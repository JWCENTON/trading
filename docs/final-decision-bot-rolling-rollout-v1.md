# FinalDecision bot image rolling rollout contract V1

## Scope and immutable target

The producer wiring exists in exactly four runtime workers:

| Compose service | Strategy | Runtime source | Source service |
|---|---|---|---|
| `bot-rsi-btc` | RSI | `bot/main.py` | `bot-rsi` |
| `bot-trend-btc` | TREND | `bot_trend/main.py` | `bot-trend` |
| `bot-supertrend-btc` | SUPERTREND | `bot_supertrend/main.py` | `bot-supertrend` |
| `bot-bbrange-btc` | BBRANGE | `bot_bbrange/main.py` | `bot-bbrange` |

There are no other production strategy entrypoints using
`finalize_decision_observation`. Each Dockerfile copies `common/` and its
strategy `main.py`, and records one mandatory build argument, OCI revision
label and runtime environment variable named `GIT_SHA`. A release operator
must resolve one full target commit SHA before any build. The build must fail
when the SHA is absent or malformed; it must never inspect a mutable working
tree from inside a running container.

Before building, verify the checkout is clean and `HEAD` equals the approved
target. Export `GIT_SHA` to that exact 40- or 64-character lowercase
hexadecimal SHA. Build all four final images from that one source tree, record
their immutable image IDs and OCI revision labels, and do not rebuild between
workers.

## Runtime neutrality

Repository defaults remain:

```text
CAUSAL_DECISION_OBSERVATION_ENABLED=0
CAUSAL_SHADOW_OBSERVATION_ENABLED=0
CAUSAL_LEARNING_AUTO_APPLY=0
CAUSAL_LEARNING_KILL_SWITCH=1
```

With observation disabled or the kill switch active, the shared sink returns
the identical `FinalDecision` without constructing a producer or opening a
database connection. When enabled later under a separately approved plan, the
sink remains fail-open: configuration, construction, database and transport
errors cannot change FinalDecision, execution, orders, fills, positions,
sizing, risk, `bot_control`, Learning activation or experiments.

## One-worker-at-a-time sequence

Use this fixed order:

1. `bot-rsi-btc`
2. `bot-trend-btc`
3. `bot-supertrend-btc`
4. `bot-bbrange-btc`

Before the first worker, record DB/outbox/duplicate baselines and explicitly
preserve the old image ID and tag for every worker. For each worker:

1. Confirm all other three workers still use their prior image IDs.
2. Recreate exactly the current worker with the already-built target image.
3. Require container running, restart count zero and a fresh healthy heartbeat.
4. Verify `/app/main.py` imports the shared sink, `/app/common` is present, and
   the OCI revision plus runtime `GIT_SHA` equal the approved target SHA.
5. Observe at least one normal strategy processing cycle without import,
   schema, configuration, decision, order or execution errors.
6. Confirm FinalDecision identity is deterministic and the returned decision
   object/action is unchanged by the sink.
7. Confirm outbox backlog remains bounded and there are no new `RETRY`,
   `DEAD_LETTER` or `IDEMPOTENCY_CONFLICT` rows.
8. Confirm duplicate decision keys, event IDs and cross-deployment identities
   have zero delta.
9. Only then proceed to the next worker.

Do not recreate a Compose group, automation-runner, database, API, market-data
worker, regime worker or orchestrator as part of this rolling contract.

## Stop and rollback gate

Stop before the next worker on any failed heartbeat, restart, import/wiring
failure, missing/mismatched target SHA, strategy processing regression,
execution delta, unbounded backlog, retry/dead-letter/conflict delta,
duplicate identity, cross-deployment row, lock/deadlock regression or
unexpected service recreation.

Rollback only the current worker:

1. set observation OFF or the kill switch ON;
2. recreate that worker with its preserved old immutable image ID;
3. require running/restart-zero/fresh-heartbeat and strategy processing gates;
4. verify outbox and duplicate deltas have stopped;
5. leave later workers on their untouched old images.

No schema rollback is required to stop the observation producer. A rollout to
VPS requires a separate explicit authorization; this document performs none.

# FinalDecision consolidated bot-runner rollout contract V2

## Production topology and version boundary

The only production strategy component in scope is Compose service
`bot-runner`, built by `services/bot_runner/Dockerfile`. Its PID 1 supervisor
is `services/bot_runner/main.py`. It reads `bot_control` without writing it and
spawns one child for each enabled `(strategy, symbol, interval)` identity:

- RSI → `/app/bot/main.py`
- TREND → `/app/bot_trend/main.py`
- SUPERTREND → `/app/bot_supertrend/main.py`
- BBRANGE → `/app/bot_bbrange/main.py`

The four strategy modules already invoke
`finalize_decision_observation` in the child process. Do not add a supervisor
sink and do not run the profiled services `bot-rsi-btc`, `bot-trend-btc`,
`bot-supertrend-btc` or `bot-bbrange-btc` beside the consolidated runner.
Those services remain dormant Compose profiles, not rollout targets, and
must not be rebuilt or recreated by this procedure.

`bot-runner-orchestrator` is a separate control-plane supervisor. It does not
calculate `FinalDecision`, does not invoke the sink and does not produce
causal evidence. It is therefore outside this image rollout and must not be
rebuilt or recreated.

The consolidated image is one atomic version boundary. It requires one exact
40- or 64-character lowercase hexadecimal `GIT_SHA`; the same build argument
becomes the OCI `org.opencontainers.image.revision` label and runtime
`GIT_SHA`. There is no `COMMIT_SHA` fallback and no runtime working-tree
lookup.

The API image follows the same immutable identity contract. Compose passes
`GIT_SHA`; `api/Dockerfile` validates the complete revision, exports it to the
runtime and writes `org.opencontainers.image.revision`. Candidate validation
must inspect this label before replacing any PAPER or LIVE API component.

## Expected LOCAL LIVE process inventory

The reviewed invariant is 28 unique child identities:

| Strategy | Expected identities |
|---|---|
| RSI | BNBUSDC/1m, BNBUSDC/5m, BTCUSDC/1m, BTCUSDC/5m, ETHUSDC/1m, ETHUSDC/5m, SOLUSDC/1m, SOLUSDC/5m |
| BBRANGE | BNBUSDC/1m, BNBUSDC/5m, BTCUSDC/5m, ETHUSDC/5m, SOLUSDC/1m, SOLUSDC/5m |
| TREND | BNBUSDC/1m, BNBUSDC/5m, BTCUSDC/5m, ETHUSDC/1m, ETHUSDC/5m, SOLUSDC/1m, SOLUSDC/5m |
| SUPERTREND | BNBUSDC/1m, BNBUSDC/5m, BTCUSDC/5m, ETHUSDC/1m, ETHUSDC/5m, SOLUSDC/1m, SOLUSDC/5m |

`BotKey(strategy, symbol, interval)` is the supervisor identity and the same
three fields are written by each child to `bot_heartbeat`. The supervisor
writes service identity `bot-runner` to `worker_heartbeats`. A rollout must
capture the target environment's exact desired inventory immediately before
recreate and require it to equal the approved 28-identity set; a changed
desired universe is a STOP, not an implicit topology update.

## Producer call path and neutrality

For every strategy family the path is:

```text
child strategy calculation
→ one finalized FinalDecision return path
→ finalize_decision_observation
→ deterministic decision key and UUID
→ causal_decision_observation_outbox_v1
```

The wrappers return the identical `FinalDecision` object. Default OFF or an
active kill switch returns before producer construction or DB access.
Construction, configuration and transport exceptions are fail-open and do
not block the strategy loop or execution. Retry/replay uses the deterministic
deployment/decision identity and `ON CONFLICT (deployment_id, decision_key)`;
different payloads fail closed as `IDEMPOTENCY_CONFLICT`. This is
observation-only: it cannot enable Learning, TREATMENT, PAPER_EXPERIMENT,
change execution, sizing, risk, strategy output or `bot_control`.

## Pre-recreate gate

STOP unless all checks pass:

1. DB and every required LIVE service are healthy.
2. Exactly one consolidated `bot-runner` container exists; no second runner
   and none of the four profiled services is running.
3. Snapshot `bot_control` and require exactly the 28 expected unique process
   identities above.
4. Require 28/28 cadence-aware fresh child heartbeats and a fresh healthy
   `bot-runner` supervisor heartbeat.
5. Require duplicate order, client-order, trade and position IDs = 0.
6. Require outbox `RETRY`, `DEAD_LETTER` and `IDEMPOTENCY_CONFLICT` = 0.
7. Require manual overrides = 0.
8. Preserve the old immutable image ID and tag and record the complete
   process/container inventory.
9. Resolve one approved exact target commit and build the final image with
   `GIT_SHA` equal to that commit. Inspect the label and ENV before use.

## Atomic recreate and post-recreate gate

Recreate only `bot-runner` on LIVE. Do not start profiled services and do not
restart DB, automation-runner, `bot-runner-orchestrator`, any other LIVE
service, or PAPER.

After recreate require:

1. exactly one `bot-runner`; changed container ID; exact approved image
   revision; `restart_count=0`; `OOMKilled=false`;
2. exactly 28 expected child processes, 28 unique identities, no unexpected
   children and 28/28 cadence-aware fresh heartbeats;
3. producer module import PASS inside a child-equivalent image environment;
4. normal event progression for RSI, TREND, SUPERTREND and BBRANGE;
5. exactly one sink wrapper per eligible finalized decision path and no
   supervisor-level or duplicate wiring;
6. no duplicate decisions, orders, client-order IDs, trades or positions;
7. bounded outbox with no new retry/dead-letter/conflict loop;
8. no execution regression and all other LIVE services still healthy.

Any failed check ends the rollout. There is no partial per-strategy advance:
all 28 children share one container image.

## Rollback

Disable observation or engage the kill switch, then recreate only
`bot-runner` from the preserved old immutable image. Do not rebuild the old
image. Re-run the complete 28/28 inventory, heartbeat, event progression,
duplicate, outbox and execution gates. Because the container is the atomic
version boundary, rollback cannot leave a mixed-version child population.

PAPER and VPS operations require separate explicit authorization and are
excluded from this contract.

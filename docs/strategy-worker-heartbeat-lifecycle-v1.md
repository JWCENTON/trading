# Strategy Worker Heartbeat Lifecycle V1

## 1. Incident Summary

The schema-convoy candidate removed runtime DDL and schema lock waits, but its
ten-minute LIVE gate ended at 25/28 fresh enabled slots. BNBUSDC, ETHUSDC and
SOLUSDC SUPERTREND 1m completed their first iterations in 351-363 seconds and
emitted `RUN_START` and `RUN_END`, while their `bot_heartbeat.last_seen` values
remained older than the six-minute stale threshold.

## 2. Existing Heartbeat Semantics

Strategy health is stored in `bot_heartbeat`; `worker_heartbeats` is reserved
for service processes such as bot-runner and automation-runner. Before this
change, strategy heartbeat meant that execution had reached a strategy-specific
snapshot point. It did not consistently mean loop start, loop completion or
process liveness.

## 3. Four-Strategy Comparison

| Strategy | Start heartbeat | End heartbeat | Error heartbeat | Periodic heartbeat |
| --- | --- | --- | --- | --- |
| RSI | No | No; snapshot/indicator branches only | No | No |
| BBRANGE | No | No; valid snapshot only | No | No |
| TREND | No | No; valid snapshot/HALT only | No | No |
| SUPERTREND before V1 | No | No; valid post-indicator snapshot only | No | No |
| SUPERTREND with V1 | Yes | Yes, `CYCLE_OK` | Yes, `ERROR` | No |

The observed stale gate is SUPERTREND-specific: its full-history indicator
preparation is materially longer and its early BLOCKED branches precede the old
heartbeat write. The other strategies are therefore not changed in V1.

## 4. SUPERTREND Long-Cycle Timeline

The worker starts an iteration, optionally ingests fills, loads parameters,
fetches and saves klines, recomputes indicators over the complete symbol and
interval history, loads the last closed candles, and only then enters
`run_strategy` and emits `RUN_START`. `RUN_END` is emitted by the strategy
`finally` block, followed by a 60-second sleep at loop level.

Candidate evidence showed exchange fetches completing in about 0.26-0.30
seconds. The interval from the initial fetch completion to `RUN_START` was about
346 seconds. Source inspection identifies `update_indicators()` as the dominant
operation: it selects the full ordered candle history and runs a Python loop
over every row to calculate SuperTrend before updating the last 50 rows.

## 5. Root Cause

The heartbeat was written only after indicator readiness inside
`run_strategy`. A long indicator phase produced no progress record, and
`INDICATORS_NOT_READY`, HALT or PANIC could return before the write. A completed
loop could therefore leave the previous container's heartbeat unchanged.

## 6. Chosen Heartbeat Semantics

`bot_heartbeat.last_seen` now means the main SUPERTREND process reached a real
loop boundary: it started an iteration or finished that iteration. The JSON
metadata records `RUNNING`, `CYCLE_OK` or `ERROR`, update time, duration and the
last loop error. Lifecycle metadata is merged into the existing rich strategy
snapshot so price, regime and position fields are not discarded.

## 7. Start, End and Error Model

`RUNNING` is written immediately before runtime operations. `CYCLE_OK` is
written after the iteration returns, which is after `RUN_END` when strategy
evaluation occurred. An exception retains the legacy ERROR event and adds an
`ERROR` boundary heartbeat from `finally`. Heartbeat writes are best-effort and
cannot prevent or skip trading work.

## 8. Why No Background Heartbeat Is Used

No thread, process or timer updates heartbeat independently. A background
watchdog could keep a DB-locked or exchange-blocked worker falsely healthy.
With boundary-only updates, a call that makes no progress still crosses the
six-minute stale threshold. A successfully completed 363-second call is fresh
immediately after its end heartbeat.

## 9. Stale Detection

The monitoring threshold remains unchanged. PID existence alone never implies
health. After `RUNNING`, absence of a real end boundary for more than the
configured stale interval remains stale. Normal subsequent SUPERTREND cycles
were observed at roughly 69-112 seconds after the expensive initial cycle.

## 10. Test Matrix

Offline tests cover start/runtime/RUN_END/end ordering, NO_SIGNAL, BLOCKED,
exception/error, a completed 363-second iteration, a no-progress interval over
360 seconds, and fail-open DB telemetry. No test uses a real DB or exchange.

## 11. Rollout Plan

Run static and regression suites, preserve immutable images, recreate only
LOCAL LIVE bot-runner, and observe all 28 enabled slots for at least 15 minutes.
Require fresh heartbeats, completed cycles, no schema DDL/locks, no restarts and
no severe logs. Proceed to PAPER only after full LIVE PASS and apply the same
15-minute gate to all 32 enabled slots.

## 12. Rollback Plan

Retag the recorded immutable previous image as the Compose bot-runner image and
force-recreate only the affected bot-runner. Do not rebuild, reverse the schema
migration or modify bot control.

## 13. Known Gaps

Full-history SuperTrend recomputation remains expensive. Optimizing it is a
separate strategy/data-performance change. Boundary heartbeats can be briefly
stale if one indivisible operation itself exceeds the threshold; V1 deliberately
does not conceal that condition.

## 14. Explicit Non-Goals

V1 does not change indicators, signals, thresholds, regime policy, ORC,
Learning, bot control, sizing, orders, positions, exits, FinalDecision,
monitoring thresholds, database schema, migrations or exchange fallback.

## 15. PAPER Long-Cycle Finding

The boundary-heartbeat candidate completed LIVE validation, but PAPER exposed
SUPERTREND 1m cycles lasting up to 459 seconds. During `update_indicators()` the
worker crossed the unchanged 360-second stale threshold and became fresh again
when the cycle completed. This showed that the worker was computing rather than
dead, but boundary telemetry could not distinguish those states.

## 16. Why Boundary Heartbeat Was Insufficient

`RUNNING` and `CYCLE_OK` prove entry to and completion of a cycle. They do not
provide evidence while the full-history Python SuperTrend loop is advancing.
Because that real operation can outlast the stale threshold, a healthy worker
can temporarily appear stale despite making continuous progress.

## 17. Progress Heartbeat Semantics

`update_indicators()` accepts an optional callback carrying `phase`,
`processed_rows` and `total_rows`. Instrumented phases are `LOAD_HISTORY`,
`EMA`, `RSI`, `ATR`, `SUPERTREND_LOOP` and `PERSIST_LATEST`. The callback is
observational: the indicator formulas, iteration order and latest-50-row
persistence remain unchanged.

Progress writes merge these fields into the existing heartbeat JSON:
`lifecycle_status=INDICATOR_PROGRESS`, `phase`, `processed_rows`, `total_rows`,
`progress_pct`, `cycle_started_at` and `progress_updated_at`. Existing price,
candle, regime, position and strategy metadata are preserved. The terminal
`CYCLE_OK` or `ERROR` write replaces the progress lifecycle status.

## 18. Main-Thread Only Model

The callback is invoked synchronously by the worker's main thread at completed
indicator phases and every 5,000 completed SuperTrend rows. There is no
watchdog, background thread, timer thread or asynchronous liveness writer. A
blocked operation that makes no observable progress can therefore still become
stale.

## 19. Time Gating and Real Progress

Writes are throttled with a monotonic clock to one at most every 90 seconds,
leaving a wide margin below the 360-second stale threshold. UTC remains the
database-facing timestamp source. Elapsed time alone is insufficient: a write
is eligible only after the phase changes or `processed_rows` increases. Repeated
callbacks for the same phase and row count do not refresh health.

## 20. Fail-Open Behavior

A callback or progress-heartbeat failure cannot interrupt indicator work,
change indicator output or alter the order path. The first callback exception
is logged with a stack trace and disables further callback attempts for that
indicator invocation, preventing repetitive log and database-write storms.
Boundary lifecycle writes retain their independent best-effort behavior.

## 21. Indicator Output Parity

Deterministic offline tests run `update_indicators()` with and without the
callback and compare every value and identifier in all 50 persisted rows, their
count and their order. Additional tests cover callback absence, phase coverage,
row-level write suppression, monotonic time gating, real-progress enforcement,
phase transitions, a 459-second cycle and callback failure.

## 22. Stale Worker Detection

The stale threshold remains 360 seconds. During continuous long-loop progress,
eligible main-thread writes keep the maximum expected heartbeat age near 90
seconds. If the same row or phase stops advancing, no progress write is emitted
and the worker is allowed to become stale. PID existence is still not treated
as proof of health.

## 23. LIVE/PAPER Rollout Gates

The candidate is first deployed only to LOCAL LIVE and observed for at least 15
minutes. Acceptance requires 28/28 fresh enabled slots throughout, progress and
terminal heartbeats for every enabled SUPERTREND 1m slot, at least two completed
cycles per slot, and zero runtime DDL, schema convoy, restarts or severe logs.

Only after LIVE passes is the identical candidate image deployed to LOCAL
PAPER. PAPER is observed for at least 20 minutes because its prior cycle reached
459 seconds. Acceptance requires 32/32 fresh enabled slots after startup, no
transient stale state during real progress, progress for all four SUPERTREND 1m
slots, a terminal `CYCLE_OK`, and either two completed cycles or one completed
cycle plus real progress in the second. Runtime and trading-state parity is
checked in both environments. Any failed gate rolls back only the affected
bot-runner to its recorded immutable previous image.

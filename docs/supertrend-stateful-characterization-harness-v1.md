# SUPERTREND Stateful Characterization Harness V1

## Purpose and boundary

This harness freezes the behavior of `bot_supertrend/main.py` at baseline
`763e4e51d506582d1604c02c9b1066bba55787fe`. It is test-only. It adds no
`FinalDecision`, `ExecutionOutcome` mapping, persistence, event, order path, DB
writer, scheduler, or runtime thread.

## State machine

Worker startup lazily obtains the exchange client, installs control defaults,
seeds runtime parameters, and then runs one `run_loop_cycle()` every 60 seconds.
Each cycle emits lifecycle `RUNNING`, loads runtime parameters, fetches and saves
klines, rebuilds indicators, reads the latest two closed candles, evaluates only
a new candle, and finally emits `CYCLE_OK` or `ERROR`.

Indicator rebuilding reads the full candle history and reports the existing
phases `LOAD_HISTORY`, `EMA`, `RSI`, `ATR`, sparse `SUPERTREND_LOOP` progress, and
`PERSIST_LATEST`. `IndicatorProgressHeartbeat` converts real, time-gated growth
into existing `INDICATOR_PROGRESS` lifecycle heartbeats. It creates no watchdog
or background thread.

`run_strategy()` emits `RUN_START`, regime telemetry and `TICK`, applies controls
and strategy logic, and always emits `RUN_END` through `finally`. The surrounding
cycle emits `CYCLE_OK` after `RUN_END`. Deduplication happens only after runtime
parameters, kline fetch/save and the complete indicator update. A duplicate candle
therefore retains that outer work and its indicator callbacks, but skips
`run_strategy()` entirely: there is no strategy event/heartbeat, RUN_START,
RUN_END, execution, or position mutation. NO_ROW is detected at the same late
boundary after indicator update and finishes with outer `CYCLE_OK`.

## Stateful variables

- `LAST_PROCESSED_OPEN_TIME` deduplicates closed candles across loop cycles.
- The persisted OPEN position carries side, quantity, entry price and entry time.
- SuperTrend direction/bands and ATR are rebuilt from stored history each cycle;
  there is no separate process-local ATR or band cache.
- Runtime control mode and enabled state are reloaded through the runtime snapshot.
- PAPER/LIVE mode, execution permission and the exchange response affect mutation.
- Profit-lock state derives from the persisted position path/high-watermark.
- Time-exit state uses persisted entry time and runtime maximum age.
- Lifecycle progress keeps last phase, processed row count and monotonic emission
  time for the duration of one worker cycle.
- The harness retains fake time, last candle, position, mode, execution response,
  profit-lock state and the chronological operation log across cycles.

## Entry paths

SUPERTREND is SPOT LONG-only. The sole entry signal is a SuperTrend direction flip
from `-1` to `+1`. Indicator readiness, enabled state, UTC disabled hours, LIVE
positions-based daily loss, minimum ATR percentage and entry regime are evaluated
before execution. Continuation (`+1→+1`), continued downtrend and non-flip
reversals produce `BLOCKED / NO_SIGNAL`; low volatility produces `ATR_TOO_LOW`.

Sizing uses the existing notional calculation, optional manual addon and win-streak
boost. There is no explicit strategy-level zero-quantity rejection: the resulting
quantity is passed to the existing execution boundary. Successful LIVE entry
position creation occurs inside `execute_and_record()` as SSOT. PAPER returns a
simulated success and the caller emits `POSITION_OPENED /
SSOT_EXECUTE_AND_RECORD`, but the current PAPER path does not call the production
position writer or materialize position state. LIVE suppression and attempted
no-fill do not create a position.

## Exit paths

For an OPEN LONG, priority is take profit, stop loss, profit lock, hard time exit,
then optional SuperTrend flip `+1→-1`. Each exit executes SELL before local close.
Ledger failure or unsuccessful LIVE execution emits `BLOCKED / EXIT_BLOCKED` and
leaves the position open. With no trigger, the strategy silently holds after its
heartbeat and `RUN_END`. An unsupported non-LONG position emits `ERROR /
UNSUPPORTED_POSITION_SIDE`.

There is no separate early-exit, break-even, or soft-exit branch in the current
SUPERTREND runtime. The unique reversal path is optional `EXIT_ON_FLIP_DOWN`,
closing with `FLIP_DOWN_EXIT`.

PANIC looks up the position before normal indicator/position logic. With no
position it changes the mode to HALT. With an OPEN LONG it executes the exit and
returns immediately after close or block; consequently the historical code does
not reach `set_mode("HALT")` in that branch. The harness characterizes this
behavior without correcting it.

## Strategy-level and full-cycle coverage

Strategy-level matrix tests call `run_strategy()` directly. They characterize
signals, controls, events, strategy heartbeat, execution, position/mode mutation
and the real `RUN_END` finally boundary. `strategy_cycle()` makes exactly one
direct `run_strategy()` call: it has no dedup state and does not touch
`LAST_PROCESSED_OPEN_TIME` or invent outer `RUNNING`/`CYCLE_OK` lifecycle entries.

Full-cycle tests call the real
`run_loop_cycle() → run_loop_iteration() → update_indicators() → run_strategy()`
chain. Successful no-signal, LIVE confirmed-fill entry and PANIC full-exit cases
cover one real shared chronology from outer `RUNNING`, through indicator
read/calculation/persistence and strategy evaluation, to `RUN_END` and
`CYCLE_OK`. Duplicate and NO_ROW cover the same outer indicator work while
correctly omitting strategy evaluation. Failure cases use the production outer
catch/finally behavior.

## Multi-cycle scenarios

The stateful suite uses LIVE confirmed fills for entry→HOLD→HOLD→flip exit and
NO_SIGNAL→entry→duplicate→HOLD. Profit-lock position-management tests seed an
explicit pre-existing position rather than deriving it from PAPER. The suite also
covers duplicate→duplicate→new candle and PAPER/LIVE boundaries. It freezes PANIC
with and without a position; the former
attempts exit and retains PANIC because of the historical early return, while the
latter transitions to HALT. A four-cycle full-loop sequence lets the module set
`LAST_PROCESSED_OPEN_TIME` naturally: new T1, duplicate T1, a second duplicate
T1, then new T2. The global is cleared once at setup and is not reseeded between
cycles. Existing indicator tests cover startup history loading, indicator phase
progress and normal persistence output with and without a callback.

## Heartbeat and operation chronology

The shared recorder observes lifecycle heartbeats, strategy heartbeat, events,
execution and mutation. Only full-cycle tests contain outer lifecycle entries.
For evaluated full cycles the important ordering is:

```text
lifecycle RUNNING
→ indicator LOAD_HISTORY / optional INDICATOR_PROGRESS
→ RUN_START
→ strategy heartbeat
→ execution boundary (when applicable)
→ position mutation (only when allowed)
→ RUN_END
→ lifecycle CYCLE_OK
```

Execution precedes OPEN/CLOSE mutation. `RUN_END` follows all strategy work and
`CYCLE_OK` is the final successful cycle boundary. Indicator-not-ready returns
before the strategy heartbeat but still passes through `RUN_END`. Integrated
outer-loop tests execute the real `run_loop_iteration()`, `update_indicators()`
and, for evaluated successful candles, `run_strategy()`. Duplicate, NO_ROW,
successful no-signal, LIVE entry, PANIC exit, calculation failure, writer
execute/commit failure and callback failure chronology comes from actual calls
and callbacks rather than hand-written lifecycle entries.

## PAPER/LIVE boundaries

The harness replaces all exchange, DB, execution and mutation boundaries before
strategy evaluation. PAPER records zero private exchange calls, real orders,
cancels and `get_my_trades` calls. LIVE tests prove only whether the existing
execution boundary is reached and whether its returned flags permit local
mutation; they never submit a real order.

The fixture execution boundary and production `execute_and_record()` receive the
same complete immutable scenario input for PAPER, DB guard, LIVE suppression,
rejection, ACK-only, partial and full outcomes. That input includes client/order
identities, quantities, acceptance/execution flags, statuses, block reason and
the complete raw response. The two implementations independently produce raw
result dictionaries which are compared using full value equality; metadata is
also mutation-tested to prevent fixture hardcoding. Missing keys remain distinct
from keys whose value is `None`. Unexpected exchange access fails immediately
through a strict exchange boundary; normal behavioral paths have zero unexpected
boundary violations, while one controlled guard test verifies the exception.

The harness has one execution-outcome source of truth: the active immutable
execution scenario. No parallel mutable execution flags exist. Every
strategy-level and full-cycle test that reaches execution selects a complete
`ENTRY_*`, `EXIT_*` or `PANIC_*` scenario containing identities, quantities, statuses,
blocked reason, ledger outcome and raw response. Parametrized invariants reject
any mixture of flags or metadata from different outcomes, including DB guard,
LIVE suppression and PANIC DB guard.

`apply_execution_scenario()` accepts any valid mapping, validates the exact field
contract, creates an independent recursive immutable snapshot, and installs only
that snapshot. Later changes to the source mapping or its nested raw response
cannot alter the active scenario or the resulting execution behavior.

## Behavioral invariants and known gaps

- Signal definitions, indicator formulas, thresholds, sizing and exit priority
  remain production-owned and unchanged.
- HOLD/no-signal/control blocks never call execution.
- A single evaluated action produces at most one execution attempt.
- Position mutation follows execution and the existing ledger/LIVE gates.
- The harness does not run real PostgreSQL, exchange internals, reconciliation,
  containers, the infinite loop or runtime services.
- Progress heartbeat is cooperative; a blocked call with no progress can still
  become stale because no watchdog thread exists.
- PANIC-with-position early return and lack of a sizing-zero strategy guard are
  documented behavior, not fixes delivered by this harness.

## Findings

- **Medium — PANIC state transition:** with an OPEN LONG, both successful and
  blocked PANIC exits return before `set_mode("HALT")`; the mode remains PANIC.
- **Medium — partial LIVE exit SSOT:** any positive exit quantity makes legacy
  `live_ok=true`, after which the caller closes the complete local position. No
  partial quantity mutation or durable pending-exit guard exists here.
- **Medium — silent HOLD:** an OPEN position with no exit trigger emits heartbeat
  and `RUN_END` but no explicit HOLD event.
- **Medium — PAPER position event/state mismatch:** PAPER entry reports simulated
  success and emits `POSITION_OPENED`, but does not call the production position
  writer, so later cycles do not observe a materialized OPEN position from it.
- **Low — sizing zero:** there is no explicit strategy gate after sizing; zero is
  forwarded to `execute_and_record()`.
- **Info — LONG-only:** entries are BUY/LONG and exits are SELL. There is no SHORT
  entry or SHORT management path; a historical SHORT emits
  `UNSUPPORTED_POSITION_SIDE` and is left untouched.

These findings are characterization only. The harness does not repair them.

## Semantic equivalence

Production files are unchanged. EMA, RSI, ATR, SuperTrend calculation, entry and
exit signals, thresholds, regime behavior, sizing, PAPER/LIVE configuration,
order payloads, position mutation, events, heartbeat cadence, ingestion and
reconciliation remain HEAD-equivalent. The patch adds tests and this document
only.

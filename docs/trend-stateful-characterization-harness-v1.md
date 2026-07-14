# TREND stateful characterization harness V1

## Scope

This test-only harness freezes the behavior of `bot_trend/main.py` at baseline
`cfae897711aa1492dcd55951569e3dfcd9911470`. It does not integrate TREND with
`FinalDecision` or `ExecutionOutcome`, add a decision sink, persist decisions, or
change production code.

## Runtime state machine

`main_loop()` owns ingestion and candle deduplication. On each iteration it loads
runtime parameters, fetches and stores klines, updates indicators, and reads the
last closed candle. A changed `open_time` is assigned to
`LAST_PROCESSED_OPEN_TIME` before `run_trend_strategy()` is called. A repeated
`open_time` emits `IDLE / NO_NEW_CANDLE` without invoking the strategy.

`run_trend_strategy()` loads at least `EMA_SLOW + 5` rows (normally 100), computes
EMA fast/slow from close prices, and returns silently when data or EMA slow is not
ready. Once ready, it emits `RUN_START`, baseline regime telemetry, reads runtime
control, and always emits `RUN_END` through `finally`.

The evaluated order is:

1. HALT or PANIC control.
2. Trend calculation and mutation of `LAST_TREND_STATE`.
3. Position lookup and heartbeat.
4. Existing-position exits in priority order.
5. Entry controls: enabled, disabled hours, daily loss, active trend.
6. Breakout, EMA slope, and price-momentum filters.
7. Entry-only regime gate.
8. distance-from-EMA filter, sizing, optional notional add-ons.
9. `execute_and_record()` boundary.

## Inputs and outputs

The strategy consumes descending DB-shaped candle rows:

`symbol, interval, open_time, close, ema_21, rsi_14`.

EMA fast and slow are recomputed from closes. Stored `ema_21` and `rsi_14` may be
`None`; the current runtime still evaluates signals and passes those values to the
execution boundary.

The production function has no structured return. Observable outputs are events,
heartbeat, execution/ledger calls, position changes, runtime mode changes, and the
module-level trend state. The harness exposes these as `TrendObservation`, with:

- cycle and input open time;
- candle state;
- position before/after;
- strategy events and heartbeat calls;
- execution attempts and position mutations;
- module state changes;
- one chronological `operation_log`;
- test-only observed action and reason.

The test-only actions are `IDLE`, `NO_ACTION`, `BLOCKED`, `ENTRY_ATTEMPT`, `HOLD`,
`EXIT_ATTEMPT`, and `EXIT`. They are observations, not a production contract.

## Stateful variables

- `LAST_PROCESSED_OPEN_TIME`: main-loop candle deduplication.
- `LAST_TREND_STATE`: last computed `UP`, `DOWN`, or `FLAT`; it is updated before
  position management and entry filters.
- `_exchange_client`: lazy process-wide exchange client cache.
- runtime-loaded module parameters, including SL/TP, time-exit, sizing, filters,
  early-cut and profit guards.
- the open position and its path/high-watermark state in persistent storage.

The harness keeps fake candle time, last processed candle, open position, runtime
mode, execution response, and guard state across cycles.

## Entry paths

- Flat trend: `SKIP / TREND_NOT_ACTIVE_FLAT`.
- Down trend: `SKIP / TREND_DOWN_LONG_ONLY`, even when `ALLOW_SHORT=true`.
- Up trend without a new EMA-fast breakout: `SKIP / NO_SIGNAL`.
- Breakout filters: `EMA_SLOPE_REJECT`, `PRICE_MOMENTUM_REJECT`, and
  `MAX_DIST_FROM_EMA`.
- Controls: `BOT_MODE_HALT`, `BOT_DISABLED`, `DISABLE_HOURS`, daily-loss block,
  entry regime block, and zero sizing.
- PAPER entry uses the simulated ledger/position path.
- LIVE suppression remains not attempted; an attempted failure emits
  `LIVE_ENTRY_NOT_FILLED` in the strategy layer.

There is no separate named readiness subsystem in TREND. Sizing zero, entry
permissions, preflight, and runtime/policy gates are the effective readiness and
suppression boundaries.

## Position and exit paths

Both LONG and SHORT positions are managed. Exit side mapping is LONG to SELL and
SHORT to BUY. New SHORT entry is not produced by the current strategy.

The existing exit paths, in runtime priority order, are:

1. LONG/SHORT take profit.
2. LONG/SHORT stop loss.
3. LONG guarded profit (when enabled).
4. Adaptive early-cut telemetry for LONG, explicitly shadow-only.
5. Shared profit lock for LONG or SHORT.
6. LONG/SHORT `EARLY_CUT` after minimum age.
7. Optional time management: keep-profit window, profit-faded exit, hard exit,
   and the legacy generic timeout branch.
8. PANIC close before normal trend/position management.

There is no TREND_FLIP/SIGNAL_FLIP, break-even-protect, or SOFT_EXIT branch in the
current file. Trend deterioration can lead to HOLD, early cut, SL, profit guard,
or time exit, but does not itself close a position.

Successful PAPER exits and successful LIVE boundary results call
`close_position`. Ledger failure, LIVE suppression, and LIVE failure leave the
position open. Regime is telemetry-only for exits and cannot block them.

## Operation chronology

The single operation log records:

- `strategy_event:<type>`;
- `heartbeat:update`;
- `execution:entry|exit`;
- `state_change:open|close|mode`.

Characterization asserts `RUN_START` before heartbeat, heartbeat before entry
execution, execution before fake position mutation, and mutation before
`RUN_END`. Exit execution precedes close mutation and `RUN_END`. Duplicate candles
contain only the `IDLE` event and no execution.

## Stateful scenarios

- Sequence A: no signal, entry, duplicate candle, next-candle hold, continued hold.
- Sequence B: entry, profitable hold, profit-lock armed, profit-lock exit.
- Sequence C: entry, trend/price deterioration hold, aged `EARLY_CUT_LONG`.

Independent tests cover insufficient data, missing optional indicators, controls,
regime and sizing blocks, PAPER/LIVE entry, LIVE suppression/failure, LONG/SHORT
TP and SL, HOLD, early cut, guarded profit, profit lock, all active default
time-exit classes, PANIC, and ledger failure.

## PAPER and LIVE boundaries

All exchange and database boundaries are replaced before strategy execution.
Unexpected network, exchange, database, or background-thread activity fails the
tests. PAPER changes only fake ledger/position state. LIVE records intent through
test doubles and never sends an order.

The current TREND `execute_and_record()` behavior is intentionally frozen:
`NEW + order_accepted=true + executedQty=0` is returned as `live_ok=true`, and a
local position is opened using requested quantity when executed quantity is zero.
This is legacy behavior, not an endorsement or a correction.

## Findings

| Severity | Path | Current behavior | Potential impact | Covered |
|---|---|---|---|---|
| High | LIVE entry ACK | Accepted `NEW` with zero fill is promoted to `live_ok=true`; local position is opened at requested qty. | Local position may precede confirmed fill. | Yes, direct execution-boundary test. |
| Medium | Open-position HOLD | Heartbeat and `RUN_END` occur, but there is no explicit HOLD event or reason. | Downstream event-only observers cannot distinguish HOLD from several silent paths. | Yes. |
| Medium | Insufficient candles/EMA | Return occurs before `RUN_START`, heartbeat, and `RUN_END`. | Startup data-readiness is not visible through normal strategy lifecycle events. | Yes. |
| Medium | SHORT entry | `ALLOW_SHORT` exists, but every DOWN trend returns `TREND_DOWN_LONG_ONLY`; only management of pre-existing SHORT is active. | Configuration suggests an entry capability that runtime does not provide. | Yes. |
| Low | Default time-exit ordering | With default keep floor 0.05% and protect floor 0.20%, the generic timeout branch is shadowed by keep-profit or profit-faded branches inside the extension window. | The generic `TIMEOUT` reason is normally absent under defaults. | Branch ordering audited; active default branches tested. |
| Info | Adaptive early cut | LONG-only telemetry opens a throttle connection and never executes an exit. | It must not be interpreted as an active early-cut order path. | Static audit; active exits use separate tests. |

Findings are characterization only and are deliberately not fixed here.

## Isolation and conscious gaps

The harness does not instrument exchange internals, ledger internals, or
reconciliation. It freezes the strategy boundary and uses deterministic doubles.
It does not run the infinite main loop, containers, runtime DDL, migrations, or
production services. Dedupe behavior is reproduced test-only from the exact
`main_loop` boundary and checked against the source behavior.

There are no production seams, production code changes, FinalDecision imports,
decision-sink calls, or decision persistence.

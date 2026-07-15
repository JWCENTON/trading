# TREND stateful characterization harness V1

## Scope

This test-only harness was introduced at baseline
`cfae897711aa1492dcd55951569e3dfcd9911470` and now also protects the TREND LIVE
ACK/fill regression. All four strategies use the same pure
`normalize_entry_execution_outcome()` adapter to interpret LIVE entry results.
TREND now returns a canonical `FinalDecision` from evaluated terminal paths. This
is return-value integration only: there is no decision sink, decision event, or
decision persistence.

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

The production function returns an immutable `FinalDecision`. Observable outputs are events,
heartbeat, execution/ledger calls, position changes, runtime mode changes, and the
module-level trend state. The harness exposes these as `TrendObservation`, with:

- cycle and input open time;
- candle state;
- position before/after;
- strategy events and heartbeat calls;
- execution attempts and position mutations;
- module state changes;
- one chronological `operation_log`;
- test-only observed action and reason;
- the production `final_decision` return value.

The test-only observed actions remain distinct from the production contract.

## FinalDecision and ExecutionOutcome mapping

| Runtime terminal path | Type / subtype | Action | Execution truth |
|---|---|---|---|
| duplicate candle | `SYSTEM_NOT_EVALUATED / NO_NEW_MARKET_DATA` | `IDLE` | no attempt |
| no signal / FLAT / DOWN long-only | `NO_TRADE / NO_SIGNAL` | none | no attempt |
| open-position hold / keep window | `NO_TRADE / POSITION_MANAGEMENT` | `HOLD` | no attempt |
| policy or quality filter | `SIGNAL_REJECTED / READINESS_BLOCKED` | `REJECT` | no attempt |
| regime or sizing gate | `ENTRY_BLOCKED / REGIME_BLOCKED|RISK_BLOCKED` | `BLOCK` | no attempt |
| execution/preflight suppression | `ACTION_SUPPRESSED / EXECUTION_NOT_ATTEMPTED` | `SUPPRESS` | no attempt |
| PAPER entry / exit | `PAPER_SIMULATION / PAPER_ONLY|EXIT_EXECUTED` | `SIMULATE|EXIT` | no real ACK/fill |
| LIVE full entry / exit | `TRADE_EXECUTED / EXECUTED|EXIT_EXECUTED` | `EXECUTE|EXIT` | confirmed full fill |
| LIVE partial fill | `TECHNICAL_FAILURE / PARTIAL_EXECUTION` | `ERROR` | ACK and positive partial quantity preserved |
| ACK without fill | `TECHNICAL_FAILURE / ORDER_ACCEPTED_NOT_FILLED` | `ERROR` | ACK true, executed false |
| rejection before ACK | `TECHNICAL_FAILURE / ORDER_REJECTED` | `ERROR` | attempted true, ACK false |
| ledger failure | `TECHNICAL_FAILURE / LEDGER_FAILURE` | `ERROR` | real ACK/fill flags preserved |

`ExecutionOutcome` is the classification SSOT. `order_submitted` equals
`order_accepted`; `trade_executed` equals confirmed `executed`. Order IDs and
pending statuses do not imply ACK, ACK does not imply a fill, and requested
quantity is never substituted for executed quantity. Details include the frozen
stage, quantities, IDs, status, block reason, and recursively frozen raw result.

### Known legacy partial-exit limitation

`ExecutionOutcome` correctly distinguishes rejection, ACK-only, partial, and
full fills, and `FinalDecision` reports partial execution as
`TECHNICAL_FAILURE / PARTIAL_EXECUTION` with the real confirmed quantity. This
return value does not control execution or position mutation.

The legacy TREND caller still uses its historical `live_ok` contract. Therefore
its existing ACK/partial position-mutation behavior is deliberately unchanged by
this integration, even when the canonical decision reports ACK-without-fill or a
partial fill. The current exit SSOT and PnL model does not implement a complete
multi-order partial-exit lifecycle, and there is no durable pending-exit guard.
This is a known risk, not a correction delivered by this patch.

There is no new shared exit reconciliation, decision persistence, or canonical
decision write path in this change.

### Future work: Partial Exit Reconciliation V1

A separate cross-strategy project must cover a durable pending-exit guard,
multiple exit orders per position, cumulative quantity/fees/weighted exit price,
all four strategies, PnL attribution, idempotency, runtime-full versus deferred
partial behavior, a complete audit/status model, and real PostgreSQL transaction
and concurrency tests.

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

Successful PAPER exits call `close_position`. LIVE position mutation deliberately
retains the HEAD-era caller contract: an accepted ACK-only or partial result can
still cause the historical full `close_position`, as can a full fill. Ledger
failure, LIVE suppression, and rejection leave the position open. Regime is
telemetry-only for exits and cannot block them.

These are three separate layers. Exchange execution truth records the actual fill
quantity. Legacy runtime mutation may still fully close the local position for an
accepted ACK-only or partial result. `FinalDecision` reports ACK-only as
`ORDER_ACCEPTED_NOT_FILLED`, partial as `PARTIAL_EXECUTION`, and full fill as
`EXIT_EXECUTED`. The decision describes execution truth; it does not repair the
legacy position mutation.

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
time-exit classes, the LIVE standard/PANIC exit outcome matrices, legacy partial
mutation chronology, PANIC execution truth, and the ledger failures that each
production adapter can actually return.

## PAPER and LIVE boundaries

All exchange and database boundaries are replaced before strategy execution.
Unexpected network, exchange, database, or background-thread activity fails the
tests. PAPER changes only fake ledger/position state. LIVE records intent through
test doubles and never sends an order.

TREND `execute_and_record()` now preserves the canonical distinction between ACK
and fill. `NEW` or `ACCEPTED` with `order_accepted=true` and zero executed quantity
remains pending: `live_ok=false`, with no local position. Partial and full fills
open a position only for the confirmed `executed_qty`. The same shared boundary is
used by RSI, SUPERTREND, and BBRANGE. A legacy result without an explicit ACK may
infer acceptance only from a positive confirmed fill, never from an order ID or
pending status alone.

Entry event types are unchanged. Their reasons are truthful at the execution
boundary: `ORDER_REJECTED` before ACK, `ORDER_ACCEPTED_PENDING_FILL` for ACK with
zero fill, and `OK` for a positive partial or full fill. A confirmed fill followed
by a failed local position write emits
`BLOCKED / LIVE_ENTRY_FILL_BUT_POSITION_NOT_OPENED`; the returned execution flags
still preserve the exchange fill.

That post-fill ledger-failure branch is specific to entry position creation. The
current TREND exit adapter does not perform a post-fill position write and does
not produce a separate post-partial or post-full ledger failure. Exit and PANIC
tests therefore cover the reachable DB-guard failure before exchange submission:
`BLOCKED / DB_GUARD_DUPLICATE`, no order placement, no close mutation, and
`TECHNICAL_FAILURE / LEDGER_FAILURE`. `FinalDecision` classifies only outcomes
that the runtime boundary can return; it does not invent an exit failure after a
fill.

## Findings

| Severity | Path | Current behavior | Potential impact | Covered |
|---|---|---|---|---|
| Resolved | Four-strategy LIVE entry ACK/fill | RSI, TREND, SUPERTREND, and BBRANGE previously had divergent ACK/fill interpretation; the shared canonical adapter now requires positive `executed_qty` and uses that exact quantity. | False requested-quantity positions are prevented. | Yes, shared rejection/pending/partial/full/exception/legacy/ledger matrix. |
| Resolved by Pending Entry Fill Reconciliation V1 | Delayed pending-entry fill recovery | Complete entry identity is retained in `binance_orders` (RSI through the existing event mirror; other strategies through the shared executor), and central fill ingest reconciles changed positive fill aggregates before exit reconciliation. | Late fills create or update one OPEN position using exchange quantity only; ambiguous/manual/exit fills remain audit-only. | Yes, bounded four-strategy reconciliation, partial-fill, idempotency and race matrix. |
| Known legacy limitation | TREND ACK/partial exit mutation | Canonical decisions distinguish ACK-only and partial truth, while the unchanged legacy caller still uses its historical `live_ok` mutation contract. | Local position mutation can disagree with canonical execution completeness. | Behavioral tests freeze both the canonical return and unchanged legacy mutation. |
| Resolved | TREND PANIC result | PANIC reduced execution truth to bool. | Attempted/ACK/partial/ledger truth was lost. | PANIC runtime matrix maps the complete result through canonical `ExecutionOutcome`. |
| Medium | Open-position HOLD | Heartbeat and `RUN_END` occur, but there is no explicit HOLD event or reason. | Downstream event-only observers cannot distinguish HOLD from several silent paths. | Yes. |
| Medium | Insufficient candles/EMA | Return occurs before `RUN_START`, heartbeat, and `RUN_END`. | Startup data-readiness is not visible through normal strategy lifecycle events. | Yes. |
| Medium | SHORT entry | `ALLOW_SHORT` exists, but every DOWN trend returns `TREND_DOWN_LONG_ONLY`; only management of pre-existing SHORT is active. | Configuration suggests an entry capability that runtime does not provide. | Yes. |
| Low | Default time-exit ordering | With default keep floor 0.05% and protect floor 0.20%, the generic timeout branch is shadowed by keep-profit or profit-faded branches inside the extension window. | The generic `TIMEOUT` reason is normally absent under defaults. | Branch ordering audited; active default branches tested. |
| Info | Adaptive early cut | LONG-only telemetry opens a throttle connection and never executes an exit. | It must not be interpreted as an active early-cut order path. | Static audit; active exits use separate tests. |

Findings are characterization only and are deliberately not fixed here.

## Isolation and conscious gaps

The strategy harness does not instrument exchange internals or the real database.
It freezes the strategy boundary with deterministic doubles.
It does not run the infinite main loop, containers, runtime DDL, migrations, or
production services. Dedupe behavior is reproduced test-only from the exact
`main_loop` boundary and checked against the source behavior.

There are no new exchange calls, retries, runtime DDL, decision-sink calls, or
decision persistence. TREND integration is not a canonical write path. The only
intentional contract output addition is the immutable return value. `NO_ROW`/no
candle rows is the conscious exception because
there is no `candle_open_time` with which to construct canonical identity.

## Semantic-equivalence statement

Signal thresholds, EMA and distance filters, breakout/regime logic, sizing,
entry/exit priority, profit lock, `EARLY_CUT`, `TIME_EXIT`, PAPER/LIVE
configuration, order payloads, order submission count, and runtime DB connection
count are unchanged. Return-value construction adds no execution.

Order submission, event chronology, reconciliation, and position mutation remain
HEAD-equivalent. Canonical classification may describe the existing legacy
ACK/partial mutation as incomplete or failed; it does not alter that mutation.
PANIC preserves the full execution result for classification without another
position lookup, while keeping its historical execution/event/mode ordering.

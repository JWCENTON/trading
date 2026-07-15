# BBRANGE FinalDecision Integration V1

## Baseline and selected pattern

Baseline: `06d412c68553291ac7e0d697c2a882726402e420`.

The integration uses the RSI/SUPERTREND observational-return pattern with a
BBRANGE-local execution adapter. TREND's broader execution mapping informed
the exit classification, but its larger orchestration structure was not
copied. The shared immutable contract remains defined in
`common.decision_contract`; no shared contract change was required.

## Intentional Return Contract Migration

Legacy BBRANGE returned `None`, while its entry-only optional sink already
received a `FinalDecision`. This integration aligns BBRANGE with RSI, TREND and
SUPERTREND:

```text
run_strategy(...) -> immutable FinalDecision
```

The sink remains an observational, fail-open attempt. A sink exception is
logged by the existing handler and the same decision is still returned.
Trading side effects and their order do not change. The only intentional
production contract change is the explicit observational return value.

## Boundary and mapping

The evaluation context preserves deployment, environment, symbol, interval,
strategy, candle time, runtime enablement, PAPER/LIVE mode and contract version.
For a missing row, evaluation time is used only as the required canonical
identity timestamp; `details.has_row` remains false.

| Legacy path | Canonical result | Existing side effects retained |
|---|---|---|
| NO_ROW, missing close, indicators/bands/candles unavailable | SYSTEM_NOT_EVALUATED | SKIP then RUN_END |
| HALT, disabled bot/hour, PANIC without position | ENTRY_SUPPRESSED | existing BLOCKED/mode effects |
| NO_SIGNAL | NO_TRADE/NO_SIGNAL | legacy SKIP taxonomy |
| filters | SIGNAL_REJECTED | existing BLOCKED reason/metadata |
| regime/daily-loss block | ENTRY_BLOCKED | regime and telemetry events |
| PAPER entry | PAPER_SIMULATION | one ledger/open/POSITION_OPENED path |
| LIVE confirmed fill | TRADE_EXECUTED | one order/open/POSITION_OPENED path |
| LIVE not attempted | ENTRY_SUPPRESSED | existing ledger artifact retained |
| rejection/duplicate | TECHNICAL_FAILURE | existing execution/event path |
| open position without exit | NO_TRADE/POSITION_MANAGEMENT/HOLD | POSITION_OPEN_NO_EXIT |
| TP/SL | EXIT result with TAKE_PROFIT/STOP_LOSS | one execution and close |
| profit-lock trail/floor | EXIT result with PROFIT_LOCK | persistence event, execution, close |
| time exit | EXIT result with TIME_EXIT | EXIT_TIME, execution, close |
| PANIC close | EXIT result with STRATEGY_EXIT | execution, close, HALT mode effect |
| suppressed/failed exit | ACTION_SUPPRESSED or TECHNICAL_FAILURE | EXIT_BLOCKED, no close |
| invalid SHORT defensive paths | TECHNICAL_FAILURE | ERROR and existing HALT effect |

`RUN_END` remains in `finally` and therefore occurs after decision derivation
and before the caller observes the returned value. Unhandled exceptions retain
their legacy propagation behavior; `RUN_END` still executes, and no stale
decision is stored for the next cycle.

## Side-effect and exactly-once boundary

All existing evaluation, event, execution and position calls run before the
decision is returned. Constructing or returning `FinalDecision` performs no
DB, exchange, event or position I/O. Tests assert per cycle:

- at most one entry or exit execution;
- at most one open or close mutation;
- no execution for HOLD, SKIP or BLOCKED paths;
- unchanged RUN_START/terminal/RUN_END ordering;
- one entry sink notification where the legacy sink boundary existed;
- no second effect caused by returning the decision.

## Stateful sequences

The harness verifies no-signal -> entry -> hold -> exit; block -> unblock ->
suppression -> retry; profit-lock armed -> retained position -> giveback exit;
and exception -> recovery -> a fresh NO_TRADE decision. It checks returned
decisions, ordered events, execution/mutation counts and persisted position
state at each cycle.

## Preserved legacy reasons and metadata

Canonical reason codes correspond to the exact legacy event reason. Detailed
entry and exit strings, execution stage/result, blocking reason, requested and
executed quantities, position ID, side and reference price are retained in
`reason_text` and/or immutable `details`. Existing strategy event payloads are
unchanged.

## Legacy findings — DOCUMENTED_NOT_FIXED

### BBRANGE-FD-LEGACY-001 — PANIC without terminal event reason

The existing event stream remains RUN_START -> RUN_END plus mode mutation. The
returned observational decision maps it to suppressed/PANIC_NO_POSITION without
adding an event. Guarded by the PANIC no-position characterization test.
Status: `DOCUMENTED_NOT_FIXED`.

### BBRANGE-FD-LEGACY-002 — PAPER `live_ok`

PAPER helper success still uses legacy `live_ok=True`. It maps to
PAPER_SIMULATION and is not reinterpreted as a live fill. Guarded by helper and
stateful PAPER tests. Status: `DOCUMENTED_NOT_FIXED`.

### BBRANGE-FD-LEGACY-003 — ledger artifact after suppression

The simulated-order artifact preceding LIVE_ENTRY_NOT_ATTEMPTED is unchanged.
The decision reports suppression without modifying retry/duplicate behavior.
Guarded by existing execute-and-record and retry sequence tests.
Status: `DOCUMENTED_NOT_FIXED`.

### BBRANGE-FD-LEGACY-004 — environment-neutral POSITION_OPENED

PAPER and LIVE success still emit POSITION_OPENED. PAPER/LIVE distinction is
carried by FinalDecision type and evaluation context, without event changes.
Guarded by entry outcome tests. Status: `DOCUMENTED_NOT_FIXED`.

### BBRANGE-FD-LEGACY-005 — legacy SKIP/BLOCKED taxonomy

NO_SIGNAL remains SKIP and position hold remains BLOCKED with
POSITION_OPEN_NO_EXIT. Canonical return taxonomy is additive and does not
rewrite those events. Guarded by lifecycle and hold tests.
Status: `DOCUMENTED_NOT_FIXED`.

## Validation and impact

Coverage includes data/control/filter paths, PAPER/LIVE entry outcomes,
confirmed-fill behavior, HOLD, TP, SL, profit lock, PANIC, failed exits,
exactly-once effects, sink failure and multi-cycle recovery.

Database schema, runtime configuration, deployment, ORC, sizing parameters,
execution layer and VPS are untouched. Production trading behavior is
preserved; only the explicit return contract changes from `None` to
`FinalDecision`.

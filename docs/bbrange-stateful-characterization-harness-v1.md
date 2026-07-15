# BBRANGE Stateful Characterization Harness V1

## Purpose and baseline

This offline harness freezes the observed stateful behavior of BBRANGE at
baseline `43765cfa7063ee72588a8b901916be49b4897b3c`. It is characterization,
not a strategy correction. Production code, database schema, runtime config,
deployment and VPS state are unchanged.

The repository already contained isolated import and single-cycle tests. V1
adds a mutable state model and multi-cycle scenarios without changing
`bot_bbrange/main.py`.

## Lifecycle and fixture model

The real `run_strategy(row)` is imported under an isolated module name with a
synthetic environment and a strict exchange object. Unexpected exchange access
fails immediately. Fixed UTC candles and deterministic band frames control
data and indicators. Runtime mode, bot enablement, regime, PAPER/LIVE mode,
execution result and profit-lock result are explicit harness state.

The recorder retains ordered strategy events, regime events, heartbeat, fake
DB lifecycle, execution calls, position open/close mutations, mode changes and
profit-lock persistence events. The returned value and position are captured
after every cycle. No real DB, network, order or background loop is used.

Observed lifecycle:

```text
RUN_START -> runtime snapshot/regime/heartbeat
-> position management OR entry gates/signal/sizing/execution
-> terminal event -> RUN_END (finally)
```

## Behavior matrix

| Scenario | Initial state | Input | Expected path/effects | Persisted state |
|---|---|---|---|---|
| No signal | no position | neutral candle | SKIP/NO_SIGNAL, no execution | none |
| Invalid data | no position | missing EMA or invalid history | SKIP, no execution | none |
| Control block | HALT/disabled | entry candidate | BLOCKED, no execution | none |
| Regime block | regime deny | entry candidate | REGIME_BLOCK, no execution | none |
| PAPER entry | no position | below-band candidate | one execution/open/event | position |
| LIVE fill | no position | candidate + fill | one order/open/event | position |
| Suppressed/rejected/duplicate | no position | candidate + outcome | one attempt, no open | none |
| Hold | open LONG | neutral range | POSITION_OPEN_NO_EXIT | same position |
| TP/SL | open LONG | threshold candle | one exit and one close | none |
| Profit lock | open LONG | armed then giveback | persisted event then one close | none after trigger |
| PANIC | open LONG | PANIC cycle | one exit/close; existing lifecycle retained | none |
| Error/recovery | no position | injected runtime error then neutral candle | RUN_END on error; next cycle succeeds | isolated |

## Stateful sequences

- no position/no signal -> entry candidate -> position open -> hold -> take-profit close;
- regime block -> unblock -> LIVE suppression -> retry -> confirmed entry;
- profit-lock armed event -> state retained -> trailing giveback -> close;
- exception in one cycle -> restored dependency -> normal next cycle.

The existing single-cycle suite additionally covers insufficient candles,
unavailable/narrow bands, trend and RSI filters, daily loss, zero sizing,
time exit, PAPER ledger duplication, LIVE pending/fill boundaries and
FinalDecision sink behavior already present in the baseline.

## Exactly-once guarantees characterized

Entry outcome tests assert one execution call and at most one position open.
Hold asserts zero execution calls. TP, SL, profit-lock and PANIC assert one
execution and one close mutation. Event order asserts RUN_START first and
RUN_END last. Multi-cycle assertions use only effects created within each
cycle, preventing accumulated recorder state from hiding duplicates.

## Legacy findings — DOCUMENTED_NOT_FIXED

### BBR-SC-001 — PANIC without a position has no terminal strategy reason

- Path: `run_strategy`, PANIC/no-position branch.
- Observed: mode changes to HALT between RUN_START/RUN_END without a terminal
  strategy reason.
- Test: `test_panic_without_position_preserves_halt_side_effects`.
- Risk: telemetry consumers may see an apparently reasonless cycle.
- Future recommendation: define a canonical operational terminal outcome.
- Status: `DOCUMENTED_NOT_FIXED`.

### BBR-SC-002 — PAPER success reports legacy `live_ok=True`

- Path: `execute_and_record`, PAPER entry.
- Observed: the flag acts as generic success although no live order occurred.
- Test: `test_execute_and_record_duplicate_paper_and_live_suppressed`.
- Risk: ambiguous environment semantics for downstream consumers.
- Future recommendation: separate ledger success from live fill status.
- Status: `DOCUMENTED_NOT_FIXED`.

### BBR-SC-003 — LIVE suppression leaves a simulated-order ledger artifact

- Path: `execute_and_record`, LIVE execution-disabled path.
- Observed: SIM_ORDER_CREATED precedes LIVE_ENTRY_NOT_ATTEMPTED; retry can meet
  DB_GUARD_DUPLICATE.
- Test: `test_execute_and_record_duplicate_paper_and_live_suppressed`.
- Risk: a suppressed attempt can influence retry eligibility.
- Future recommendation: review ledger identity in a separate execution slice.
- Status: `DOCUMENTED_NOT_FIXED`.

### BBR-SC-004 — POSITION_OPENED is environment-neutral

- Path: successful PAPER and LIVE outer entry completion.
- Observed: both emit POSITION_OPENED although their execution semantics differ.
- Test: `test_entry_execution_outer_contract` and stateful entry outcomes.
- Risk: event-name-only consumers cannot distinguish simulation from live fill.
- Future recommendation: use canonical decision metadata, not event-name inference.
- Status: `DOCUMENTED_NOT_FIXED`.

### BBR-SC-005 — Several no-action outcomes use SKIP/BLOCKED taxonomy

- Path: data, filter and no-signal early returns.
- Observed: NO_SIGNAL is SKIP; position hold is BLOCKED with
  POSITION_OPEN_NO_EXIT.
- Test: lifecycle/no-signal and entry-gate characterization tests.
- Risk: legacy event taxonomy can conflate hold, rejection and no trade.
- Future recommendation: retain events but consume the existing canonical return
  contract in a separately authorized integration stage.
- Status: `DOCUMENTED_NOT_FIXED`.

## NOT_APPLICABLE and explicit boundaries

- SHORT entry: not supported by the SPOT LONG-only strategy; defensive SELL is
  structurally unreachable from the hard-coded BUY decision.
- Band/mean-reversion exit: bands create entry signals, not a distinct open-
  position exit branch.
- Regime-invalidation exit: regime gating is entry-only.
- Partial exit: no separate partial-position lifecycle exists in run_strategy.
- Restart/reload state: strategy state is persisted through positions/path data;
  there is no in-memory restart state machine to invoke.
- Maker/cancel internals remain below the characterized execution boundary.

## Future contract work

The baseline already returns `FinalDecision` on the full entry evaluation
boundary. Any future expansion of that contract to position-management or
outer-loop outcomes must be a separate production change and must preserve
these ordered effects and state transitions. Characterization expectations
must not be edited merely to make such a refactor pass.

Production code changes in this stage: **0**. Deploy/runtime/VPS impact: **0**.

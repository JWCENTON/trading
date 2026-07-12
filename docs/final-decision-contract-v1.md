# Final Decision Contract V1 — BBRANGE Pilot

## 1. Four-Strategy Scope

`common.decision_contract` is shared by RSI, BBRANGE, TREND and SUPERTREND. Strategy is free text, not a closed enum. Strategy-specific indicators and thresholds belong only in immutable `EvaluationContext.context` or `FinalDecision.details`.

## 2. Shared Contract

`EvaluationContext` is a frozen, deeply immutable record of deployment, environment, slot, exact candle, evaluation start, engine provenance and runtime/regime context. It rejects blank identity fields and naive timestamps. `identity_components()` returns deployment, environment, symbol, interval, strategy and exact ISO candle time.

`FinalDecision` is frozen and deeply immutable. It records common decision type/subtype, structured and legacy reason, execution flags/IDs, price/side, eligibility and technical status. Invariants reject impossible execution ordering, invalid type flags, learning-enabled technical/system states and PAPER results in a LIVE context.

Python cannot expose a boolean dataclass field and classmethod with the same name. Therefore the two colliding factories are explicitly named `trade_executed_result` and `technical_failure_result`; the required public fields remain `trade_executed` and `technical_failure`. Other factories use the requested names.

## 3. Why BBRANGE Pilot

BBRANGE has an explicit spot-long entry section, the fewest execution calls of the representative bots and a proven 29-return offline harness. It covers the shared regime, risk, readiness, sizing, PAPER, LIVE suppression/failure/fill and duplicate-ledger patterns.

## 4. Strategy-Neutral Design

Types cover executed trade, no trade, signal rejection, entry block/suppression, PAPER simulation, system-not-evaluated and technical failure. Subtypes cover no signal/data, regime/ORC/readiness/risk, runtime/execution, duplicate/order rejection and reserved position/exit categories. No BB, RSI, EMA, Supertrend or trend-strength field exists in the main models.

## 5. BBRANGE Integration

`run_strategy(row, decision_sink=None)` retains legacy `None` return. The new argument is optional and the main loop does not pass it. EvaluationContext is constructed only after data/runtime readiness, HALT/PANIC and existing-position management, at the start of the existing ENTRY section.

Every terminal entry return now creates one typed result after all legacy side effects at that branch. The sink is never invoked for NO_ROW, indicator warm-up, HALT/PANIC, existing-position hold or exits. No event text, gate order, execution call, DB path, threshold or public runtime return changed.

## 6. Exactly-Once Model

A local `finish()` calls the optional sink once and returns `None`. It is used at every terminal return in the full entry section. Parameterized tests prove one call for NO_SIGNAL, regime block, PAPER success, duplicate guard and LIVE suppression, and zero calls outside entry evaluation.

## 7. Legacy Compatibility

With the default sink `None`, behavior is observational only. The original 28 characterization scenarios still assert legacy return, event types/reasons, DB/exchange boundary operations, simulated order and position effects. FinalDecision is created after the corresponding legacy event/action; it cannot change the prior branch decision.

## 8. Review and Parity Results

Formal review found and corrected two contract-boundary defects: sink exceptions could escape into runtime, and `reference_price` did not reject non-`Decimal` values. The sink is now fail-open with an exception log, and financial reference prices are type-checked. Tests cover both corrections.

The characterization suite expanded from 28 to 37 tests. All baseline assertions pass unchanged. Combined contract/pilot scope has 54 tests. Three deterministic combined runs produced 54/54/54 passes. Existing unrelated Learning contract tests remain 18/18 passing. `py_compile` and `git diff --check` pass.

## 9. RSI Compatibility Review

| Review item | Result |
|---|---|
| Real terminal categories | no signal/rebound, data readiness, regime/risk/runtime blocks, sizing/execution failure, PAPER/LIVE execution, open-position exits |
| Type/subtype sufficiency | yes |
| Missing common fields | none |
| Strategy data | RSI/EMA/ATR and rebound state -> context/details |
| Invariant concern | LIVE result may not expose position ID to outer caller; field remains optional |

## 10. TREND Compatibility Review

| Review item | Result |
|---|---|
| Real terminal categories | flat/down/ATR/EMA/distance filters, regime/risk/runtime blocks, duplicate/PAPER/LIVE execution, exit management |
| Type/subtype sufficiency | yes; filters map to SIGNAL_REJECTED/READINESS_BLOCKED |
| Missing common fields | none |
| Strategy data | slope, trend bucket, distance and streak data -> context/details |
| Invariant concern | entry logic is in a differently shaped `run_trend_strategy`; needs its own boundary extraction |

## 11. SUPERTREND Compatibility Review

| Review item | Result |
|---|---|
| Real terminal categories | data readiness, no signal, direction/filter rejection, regime/risk/runtime blocks, PAPER/LIVE execution and exits |
| Type/subtype sufficiency | yes |
| Missing common fields | none |
| Strategy data | Supertrend state/line and flip metadata -> context/details |
| Invariant concern | existing-position and flip exits must remain outside entry decision sink |

## 12. Migration Sequence for Remaining Strategies

1. BBRANGE: pilot, offline harness and LOCAL LIVE/PAPER rollout.
2. RSI: next contract integration.
3. TREND: isolate its entry boundary, then add the sink.
4. SUPERTREND: final strategy integration.
5. Only after all four strategies: canonical DB write path.

Each migration requires a stateful characterization harness, unchanged legacy assertions, exactly-once sink tests and three deterministic runs.

## 13. Known Gaps

- Contract is not persisted and has no decision-cycle UUID.
- Deployment falls back visibly to `UNKNOWN` when runtime ENV is absent.
- Current BBRANGE outer execution result does not expose durable simulated-order or position ID on every success, so those fields can be NULL.
- Market regime is currently not copied into the context before the later entry gate; gate evidence is stored in decision details where available.
- No integration exists for the other three strategies.

## 14. LOCAL Pilot Validation — 2026-07-12

The shared `bot-runner` image launches all four strategies; there is no separate BBRANGE container. Only BBRANGE and shared contract sources changed, so the controlled rollout rebuilt and force-recreated only `bot-runner`, first on LOCAL LIVE and then LOCAL PAPER. DB, API, frontend, automation, market-data, regime and orchestrator services were not restarted.

LIVE container `bb92bee53555` started at `15:16:59Z` with restart count zero. Seven post-rollout BBRANGE `RUN_START` events had seven matching `RUN_END` events; active 1m/5m slots completed multiple cycles with unchanged `NO_SIGNAL`, `TREND_NOT_FLAT`, `INDICATORS_NOT_READY` and `NO_NEW_CANDLE` semantics. There were no BBRANGE open positions, new execution effects, import errors or tracebacks. Worker heartbeat was healthy at `15:30:36Z`; ORC remained `ORC_V6_3` and Learning V1.4 apply remained `0`.

PAPER container `25afe3cde891` started at `15:21:17Z` with restart count zero. Its cold start retained all 32 worker processes and all eight BBRANGE control slots. The observed BBRANGE sample completed five matched decisions: three 1m and two 5m cycles, with unchanged readiness/no-signal/no-new-candle semantics. The four pre-existing open BBRANGE positions retained the same IDs and entry data; no post-rollout simulated order or PAPER close was created during the sample. Worker heartbeat was healthy at `15:30:41Z`; ORC remained `ORC_V6_3` and Learning V1.4 apply remained `0`.

The production caller still omits `decision_sink`. Therefore the pilot emitted no FinalDecision persistence/log side effect, introduced no table/schema change and preserved the legacy `None` return and DB/order paths.

## 15. Explicit Non-Goals

No DB canonical event, registry ingestion, NO-TRADE backfill, replay/outcome, Learning V2, parameter change, strategy/execution rewrite, migration, commit or push.

## Verdict

**READY FOR COMMIT AND VPS ROLLOUT**

# Canonical NO-TRADE Decision Events V1 — Boundary Audit and Design

Audit date: 2026-07-12 UTC
Repository: `main` at `40ef1cf2996c5c2d55abf92523d3a5958471b42d`, equal to `origin/main`
Scope: local repository, read-only LOCAL LIVE/PAPER inspection; no runtime or database change.

## 1. Problem Statement

`decision_registry_v1` has a safe identity for `TRADE_EXECUTED` because `positions.id` is a durable one-row-per-position source. No equivalent parent event exists for a strategy evaluation that produces no trade. `strategy_events` contains useful telemetry but not a proven one-row-per-decision record. Building NO-TRADE decisions by grouping these rows would violate the required architecture.

## 2. Existing Event Granularity Audit

All four bots emit `RUN_START`, then many possible telemetry events, and generally emit `RUN_END` from a `finally` block. The events have stable integer primary keys, but the emit functions do not return those IDs to the caller and do not share a cycle identifier. The nullable `strategy_events.run_id` exists but was unused in every event inspected during the 24-hour audit.

Observed 24-hour data:

| Measure | LOCAL LIVE | LOCAL PAPER |
|---|---:|---:|
| strategy events | 118,311 | 106,434 |
| events with `run_id` | 0 | 0 |
| RUN_START | 17,118 | 21,131 |
| RUN_END | 17,118 | 21,131 |
| SKIP | 14,917 | 12,666 |
| BLOCKED | 2,184 | 7,652 |
| IDLE / NO_NEW_CANDLE | 13,104 | 13,151 |
| SIM_ORDER_CREATED | 24 | 158 |
| slot/candle groups with multiple terminal candidates | 22 | 0 |

Counts of RUN_START and RUN_END balance, but there is no relational pairing. LIVE examples with multiple candidate events prove that `(slot, candle)` alone cannot identify which existing row is the authoritative terminal event. Event emission also mirrors into `entry_trace_events`, producing another ID without a parent relation.

Answers to the required audit questions:

1. A cycle begins when a bot sees `LAST_PROCESSED_OPEN_TIME != latest closed candle open_time` and calls `run_strategy`; `RUN_START` is only telemetry emitted inside that call.
2. Control flow ends at one of many early returns or normal completion; `RUN_END` is emitted in `finally`, but it does not identify the preceding final decision event.
3. Yes. One cycle can emit intermediate gate, sizing, signal, simulated-order and blocked events; LIVE data contains multi-candidate groups.
4. No. The four strategies have materially different signal, open-position, PAPER, regime and execution branches and different reason vocabularies.
5. No persistent cycle/run parent exists. `LAST_PROCESSED_OPEN_TIME` is process memory only; `strategy_events.run_id` is nullable and unused.
6. Individual events have stable integer IDs, but none is designated the parent or final event.
7. Yes. A process restart resets `LAST_PROCESSED_OPEN_TIME` and may evaluate the same closed candle again.
8. Current data cannot distinguish a deliberate retry from a second evaluation without imposing a new canonical idempotency contract.
9. `NO_NEW_CANDLE` is `SYSTEM_NOT_EVALUATED`, not a business decision; strategy logic was not run.
10. `INDICATORS_NOT_READY` is `SYSTEM_NOT_EVALUATED / DATA_NOT_READY`, not Learning-eligible decision evidence.
11. `DB_GUARD_DUPLICATE` is an execution/lifecycle protection outcome. It is not automatically a valid business decision and may follow an earlier simulated or position write.
12. `LIVE_ENTRY_NOT_ATTEMPTED` is emitted after execution gating. Its surrounding ledger/simulation behavior must be proven per strategy before classification; treating it alone as final could hide an earlier artifact.

## 3. Canonical Decision Boundary

The required boundary is one invocation of the entry-evaluation portion of a strategy for one canonical closed market-data candle and slot. It begins only after new-market-data detection and ends once with an explicit final result object. Exit management, heartbeat, idle polling and ingestion are outside this boundary.

The boundary cannot be reconstructed safely from current telemetry. The next implementation must make it explicit in runtime with a shared evaluation context and exactly one finalization call. A repeated evaluation of the same slot/candle must reuse the same cycle ID and idempotently refresh the same event.

## 4. Decision Type Taxonomy

| Type | Meaning | Example subtype |
|---|---|---|
| `TRADE_EXECUTED` | completed trade entry; map to existing position decision | `POSITION_OPENED` |
| `NO_TRADE` | complete evaluation found no actionable signal | `NO_SIGNAL` |
| `SIGNAL_REJECTED` | signal existed but strategy-quality rule rejected it | `ATR_TOO_LOW`, `TREND_FILTER` |
| `ENTRY_BLOCKED` | external/gate safety prevented an otherwise eligible entry | `REGIME_BLOCKED`, `ORC_BLOCKED`, `RISK_BLOCKED` |
| `ENTRY_SUPPRESSED` | runtime policy disabled submission | `LIVE_DISABLED`, `EXECUTION_DISABLED` |
| `PAPER_SIMULATION` | completed PAPER simulation with durable simulated-order ID | `PAPER_ONLY` |
| `SYSTEM_NOT_EVALUATED` | no complete business evaluation occurred | `NO_NEW_MARKET_DATA`, `DATA_NOT_READY`, `TECHNICAL_FAILURE` |

`SYSTEM_NOT_EVALUATED` is required in the canonical event schema but is not currently accepted by `decision_registry_v1`. A future migration must extend that registry constraint deliberately or exclude these events from registry mapping while retaining operational audit.

## 5. Reason Taxonomy

| Canonical reason | Type / subtype | Learning | Replay | Technical | Business |
|---|---|---:|---:|---:|---:|
| `NO_SIGNAL` | NO_TRADE / NO_SIGNAL | yes | yes | no | yes |
| `NO_NEW_CANDLE` | SYSTEM_NOT_EVALUATED / NO_NEW_MARKET_DATA | no | no | no | no |
| `INDICATORS_NOT_READY` | SYSTEM_NOT_EVALUATED / DATA_NOT_READY | no | no | no | no |
| `ATR_TOO_LOW` | SIGNAL_REJECTED / READINESS_BLOCKED | yes | yes | no | yes |
| `TREND_NOT_ACTIVE_FLAT`, `TREND_DOWN_LONG_ONLY` | SIGNAL_REJECTED / STRATEGY_FILTER | yes | yes | no | yes |
| `REGIME_BLOCK` | ENTRY_BLOCKED / REGIME_BLOCKED | yes | yes | no | yes |
| `ORC_BLOCK` | ENTRY_BLOCKED / ORC_BLOCKED | yes | yes | no | yes |
| `READINESS_BLOCK` | ENTRY_BLOCKED / READINESS_BLOCKED | yes | yes | no | yes |
| `RISK_BLOCK` | ENTRY_BLOCKED / RISK_BLOCKED | conditional | yes | no | yes |
| `LIVE_DISABLED` | ENTRY_SUPPRESSED / LIVE_DISABLED | conditional | yes | no | yes |
| `PAPER_MODE` | PAPER_SIMULATION / PAPER_ONLY | yes | yes | no | yes |
| `DB_GUARD_DUPLICATE` | SYSTEM_NOT_EVALUATED / DUPLICATE_BLOCKED | no | no | yes | no |
| `ORDER_PREFLIGHT_FAILED` | SYSTEM_NOT_EVALUATED / TECHNICAL_FAILURE | no | no | yes | no |
| `ORDER_SUBMISSION_FAILED` | SYSTEM_NOT_EVALUATED / TECHNICAL_FAILURE | no | no | yes | no |

Mapping must be a central constant/model, never inferred from free text.

## 6. Decision Cycle Identity

Recommended UUIDv5 payload:

```text
deployment_id|environment|symbol|interval|strategy|
market_data_source|canonical_candle_open_time|ENTRY_EVALUATION_V1
```

The market identity is the exact closed candle `open_time` already used by each loop; no rounding is allowed. `evaluation_started_at` is evidence, not identity. Retry after restart reuses the ID. If multiple legitimate evaluations per candle are later required, the architecture must first introduce a durable source-cycle sequence; runtime time must not be substituted.

## 7. Source Natural Keys

- Entry evaluation: full canonical payload above.
- Executed entry: existing `deployment/environment/positions.id/TRADE_EXECUTED`; canonical event maps to it and must not create another decision.
- PAPER simulation: only a durable `simulated_orders.id` linked to the cycle is acceptable.
- Historical strategy event IDs are evidence references, not cycle identity.

## 8. Database Model

The proposed `canonical_decision_events_v1` fields in the task are sufficient with these additions: `reference_price`, `potential_side`, `learning_eligible`, `replay_eligible`, `technical_failure`, `business_decision`, and `registry_decision_id`. Unique constraints must cover `decision_cycle_id` and the full deployment/environment/source natural identity. A CHECK must enforce one coherent terminal-state combination.

No migration was created in this audit because a table without a safe producer would invite unsupported historical backfill and a competing empty SSOT.

## 9. Registry Mapping

The canonical event is the source; `decision_registry_v1` remains universal identity. Non-trade registry identity should use `canonical_decision_events_v1.decision_event_id` as `source_record_id`. `TRADE_EXECUTED` must find the existing registry row by deployment, environment and `position_id`, then store that `decision_id` on the event. Missing or multiple executed mappings are hard validation failures.

## 10. Runtime Write Path

Required future design:

```text
main loop detects new closed candle
-> EvaluationContext.begin(slot, candle)
-> strategy returns FinalDecision (no telemetry inference)
-> record_decision_event(FinalDecision), fail-open
-> existing RUN_END telemetry
```

The helper must not be called from generic `emit_blocked`, because that helper also records exits and intermediate states. Each strategy should be adapted to return a typed final result instead of returning `None` from dozens of branches. This touches four strategy implementations and is therefore a separately reviewed refactor, not a safe minimal patch in this task.

## 11. Failure Handling

Canonical telemetry is fail-open for trading. A write failure must log the event ID, slot/candle and exception and update a dedicated heartbeat/audit counter using an independent safe telemetry path. It must never change gate order, exception handling, order submission or position state. Database constraints and upsert identity must make retries safe.

## 12. Learning Eligibility

Only completed business evaluations are eligible. `NO_SIGNAL`, deterministic strategy-filter rejection and explicit external gate blocks can qualify. Idle polls, missing data, indicator warm-up, duplicate guards and technical execution failures cannot be used as evidence of strategy quality.

## 13. Replay Eligibility

Replay needs symbol, interval, exact market-data timestamp, reference price and direction semantics. `NO_SIGNAL` without a potential direction supports absolute move/excursion only. A rejected BUY supports directional LONG forward outcomes. Eligibility must be stored at event creation, not inferred later from reason text.

## 14. Historical Backfill Rules

No historical NO-TRADE backfill is authorized. Current rows lack a persistent parent ID and authoritative terminal marker. Balanced RUN_START/RUN_END counts do not prove pairing. Any grouping by slot/candle/time or selecting “last BLOCKED/SKIP” is heuristic and prohibited.

Future backfill may start only after canonical runtime emission exists, using canonical rows themselves. Earlier telemetry remains evidence-only legacy data.

## 15. Idempotency

UUIDv5 and a unique `decision_cycle_id` guarantee retry stability. Descriptive context may refresh, but type/subtype changes after first successful finalization must be audited as a conflict, not silently overwritten. One slot/candle must never yield multiple IDs.

## 16. Provenance

Require explicit `WALTRADE_DEPLOYMENT_ID`, database environment verification, engine/schema version, exact market source/candle, source code version, evaluation timestamps and emitter version. `UNKNOWN` is auditable but not production-ready.

## 17. Safety Model

This audit made no runtime, database, ORC, bot-control, order, position, strategy, Docker or service change. The future helper must write only new canonical/audit tables, after a final result is known, and be fail-open. Static tests must prove it has no dependency path into execution decisions.

## 18. Rollout Plan

1. Separate change: introduce typed reason taxonomy and `FinalDecision`/`EvaluationContext` without database writes; unit-test every return branch.
2. Add idempotent canonical-event schema and helper behind an explicit reporting-only feature flag.
3. Integrate one strategy in LOCAL PAPER first, despite normal environment rollout order, using synthetic/unit tests before any running service change; obtain explicit authorization for restart.
4. Validate retry of the same candle, one finalization, no execution changes and registry mapping.
5. Only then proceed through separately authorized LOCAL LIVE/PAPER and VPS rollout.

## 19. Rollback Plan

Disable the canonical telemetry feature flag; no trading behavior changes. Preserve event/audit history. Remove integration calls in a later commit if necessary. Dropping tables is not required and must be separately authorized.

## 20. Known Gaps

- No persistent cycle ID or parent event.
- No authoritative final-event pointer.
- Four incompatible branch/reason lifecycles.
- `run_id` unused.
- Restart can retry the same candle.
- Existing `SYSTEM_NOT_EVALUATED` registry constraint support is absent.
- PAPER simulation linkage to a canonical cycle is not proven across all strategies.
- Deployment ID is not yet wired into strategy runtime.

## 21. Explicit Non-Goals

No heuristic backfill, Recommendation Outcome Attribution V2, confidence evolution, adaptive calibration, ORC/strategy/exit/capital tuning, auto-apply or forward replay implementation.

## 22. Recommended Next Slice

Implement and test a shared, database-free `FinalDecision` taxonomy and `EvaluationContext` contract in isolation. Convert exactly one strategy only after enumerating all entry-evaluation returns, while leaving order/execution calls untouched. The acceptance test is: one closed candle retry produces one deterministic cycle ID and exactly one typed final result; exit/idle/technical paths cannot masquerade as NO TRADE. Database emission should be a subsequent slice.

## Final Verdict

**NOT READY FOR COMMIT**

Stop conditions encountered: the historical boundary is ambiguous; safe implementation requires a broader four-strategy lifecycle refactor; current telemetry cannot identify one authoritative final decision; and attaching to generic telemetry could alter or misclassify the execution path. No migration, backfill, helper or runtime integration was attempted.

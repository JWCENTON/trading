# WalTrade — Learning Engine V2 Architecture Audit

Audit date: 2026-07-12 (UTC)  
Scope: local repository, LOCAL LIVE PostgreSQL, LOCAL PAPER PostgreSQL; read-only inspection except this report.  
Repository revision: `8ca8452db299f1cb534f6f6fca7fa75e8dfcd1ee`.

## 1. Executive Summary

WalTrade already has substantial entry, exit, replay, warehouse, slot, MME, ORC and shadow-learning infrastructure. Learning Feedback V1.4 is demonstrably shadow-only: its database function writes only its own proposal/run tables and `automation_kv`; the run table constrains `apply_enabled IS FALSE`; no V1–V1.4 SQL path writes `bot_control`, runtime/strategy/ORC parameters, orders, positions or capital allocation. The runner reports `engine_mode=SHADOW` and `apply_enabled=false` in both local environments.

The system is not yet ready to implement the full V2 design. Three blocking gaps were confirmed:

1. The nominal data flow is cyclic/inverted: historical shadow recommendations are produced directly from entry snapshots and realized PnL; the feature warehouse consumes those recommendations; `decision_replay_v1` then consumes the warehouse. Replay is therefore not an independent upstream truth against which a recommendation can be evaluated.
2. `decision_key` is an MD5 of `symbol|interval|strategy|entry_time`, with no engine version, environment in the hash, decision type, regime or durable source event ID. It covers accepted trades/snapshots but does not represent all BUY and NO BUY decisions. NO TRADE uses `entry_trace_events.id` in `missed_opportunity_replay`, not the same identity model.
3. LOCAL LIVE and LOCAL PAPER have confirmed schema drift in ORC/slot/exit-audit objects. Relevant learning tables have matching column signatures, but the environment schemas as a whole are not equivalent.

The recommended first slice remains **Recommendation Outcome Attribution V2**, but only after introducing an immutable recommendation observation identity and an independent, temporally correct outcome/replay contract. It must remain reporting-only and SHADOW.

## 2. Current Architecture Inventory

Current effective flow (implementation, not names):

```text
entry_trace_events / positions / realtime / MME / ORC
  -> entry_context_snapshot_v1 + Model-B audit views
  -> learning_feedback_shadow_recommendations
  -> learning_feature_warehouse_v1
  -> decision_replay_v1
  -> learning_slot_statistics_v1
  -> learning_calibration_proposals_v1 (V1/V1.1)
  -> refresh run scheduler (V1.2)
  -> immutable observations + current validation state (V1.3)
  -> bounded shadow confidence delta proposals (V1.4)
```

Parallel paths are `missed_opportunity_replay` for rejected/no-buy events, `exit_trace_v1/v2/v3` plus `exit_learning_v1`, Slot Brain snapshots, market-memory/MME, and ORC V6.3/V7-shadow. These paths are only partially unified by `position_id` and slot dimensions.

## 3. Existing Data Assets

### Entry intelligence

| Element | Source implementation | Object / grain / key | Write/refresh | BUY / NO BUY; env; retention |
|---|---|---|---|---|
| Entry trace | `common/entry_trace.py`; bot callers; migration `20260702_entry_trace_realtime_v1.sql` | `entry_trace_events`, one evaluation event, PK `id`; no position FK | synchronous event insert | both decisions where caller emits; LIVE/PAPER through same code; no explicit retention |
| Entry snapshot | `refresh_entry_context_snapshot_v1`, V2-fast migration | `entry_context_snapshot_v1`, one accepted position, PK `position_id` | periodic upsert after position exists | BUY/trades only; both envs; no retention |
| Realtime context | `common/realtime_engine.py`, realtime V2 migrations | entry event JSON/components and `v_realtime_score_latest` | synchronous + views | evaluation coverage depends on caller; both envs |
| Regime | `common/regime.py`, regime worker, position/trace fields | text regime plus slot/regime views | runtime capture + periodic stats | both envs; not immutable/versioned everywhere |
| Slot Brain context | slot migrations and `refresh_slot_brain_snapshot` | `slot_brain_snapshot`, windowed slot snapshot | periodic upsert per window | aggregate, not per decision; both envs |
| ORC context | ORC snapshots/decisions and Model-B compatibility views | position/slot/time joins, not a universal decision FK | runtime/periodic | primarily selected BUY path; both env variants differ |
| MME context | market-memory migrations and Model-B audit views | slot/time context JSON | periodic sequence/ranking/snapshot refresh | context may be missing/expired; both envs |
| Rejected/no trade | `refresh_missed_opportunity_replay_v1` | `missed_opportunity_replay`, PK/FK `entry_trace_event_id` | periodic look-forward replay | NO BUY only when an entry event exists; separate identity; both envs |

There is no universal `entry_trace_id -> position_id -> decision_key` constraint. Snapshot matching uses symbol, interval, strategy and exact `entry_time`; this is vulnerable to timestamp-format and duplicate-event ambiguity.

### Exit intelligence

| Layer | Grain and timing | Structural vs textual derivation |
|---|---|---|
| `positions` | one position; exit fields written at close | structured PnL/fees/time/price, textual `exit_reason` |
| `exit_trace_v1` | one row per `position_id`; trigger at close, later refresh enrichment | MFE/MAE/giveback from `v_trade_mfe_mae`; family and decision class parse reason text |
| `exit_trace_v2` | one row per position, trigger/refreshed from V1 | capture/giveback ratios structured; peak/current/floor/trail/age regex-parsed from reason |
| `exit_trace_v3` | one row per position, periodic refresh | JSON `decision_context`, thesis/priority derived from V2 classes |
| `exit_learning_v1` | aggregate by strategy/symbol/interval/family/class | later aggregation; upsert overwrites current aggregate |
| replay views | one closed trade / aggregate | derive MFE, MAE, exit and giveback from position-path data |

MFE/MAE and realized PnL are structural. Exit family, TIME/PROFIT_LOCK/TRAIL/FLOOR/STOP/EARLY_CUT classification and V2 embedded values depend on free text. Risks: wording changes silently reclassify history; locale/precision changes break regex; one reason can match several families with ordering-dependent result; reconciled exits are not equivalent to strategy exits; malformed text yields NULL rather than a schema error. V2 should preserve raw reason but consume a versioned structured `exit_reason_code` and `exit_decision_payload` captured at exit.

## 4. Source Code Map

| Area | Canonical files / functions |
|---|---|
| Runtime entry capture | `common/entry_trace.py`, `common/realtime_engine.py`, `common/regime.py`, strategy bot `main.py` callers |
| Entry snapshots / no-trade | `20260705_entry_context_snapshot_v2_fast.sql`; `refresh_entry_context_snapshot_v1`; `20260702_missed_opportunity_replay_v1.sql` |
| Exit capture / classification | `common/exit_reason_context.py`, `common/adaptive_time_exit.py`, `common/exit_guards/*`; exit-trace migrations V1–V3 |
| Replay / warehouse | decision replay V1–V1.3 migrations; feature warehouse V1; trade entry/exit replay view |
| Shadow recommendations | shadow recommendation V1/V1.1 migrations |
| Learning V1–V1.4 | five `20260710_learning_feedback_engine_*` migrations, V1.4 docs and contract tests |
| Slot Brain | `20260620_slot_profile_*`, `20260628_slot_brain_v11.sql`, `20260628_slot_intelligence_v1.sql` |
| ORC / capital | ORC V6.1–V6.3 migrations, ORC integration V2, V7 readiness gate; `automation_runner.run_orc_v5_apply` |
| Scheduling | `automation_runner/main.py`: entry, learning telemetry, shadow pipeline, V1.2/V1.3/V1.4 functions |

## 5. Database Object Map

Key learning objects and constraints confirmed in both local databases:

| Object | Grain / identity | Mutation semantics |
|---|---|---|
| `decision_replay_v1` | unique `(environment, decision_key)` | upsert overwrites JSON/context; no replay history |
| `learning_feature_warehouse_v1` | unique `(environment, decision_key, recommendation_type, recommendation_action)` | upsert overwrites evidence for same recommendation tuple |
| `learning_feedback_shadow_recommendations` | same tuple as warehouse | append across changed type/action, update conflict otherwise; no explicit active flag |
| `learning_slot_statistics_v1` | environment + slot + `window_days` | current aggregate upsert |
| `learning_calibration_proposals_v1` | unique `proposal_key` | V1.1 deletes unresolved current proposals then rebuilds/upserts one per slot/window |
| `learning_feedback_refresh_runs_v1` | scheduler run | persistent audit/idempotency source |
| `learning_proposal_observations_v1` | unique refresh run + slot/window | immutable recommendation observation |
| `learning_proposal_validation_state_v1` | unique environment + slot + window | mutable current state; resets on action change |
| `learning_shadow_confidence_proposals_v1` | unique deterministic proposal key; one partial-unique ACTIVE per slot/window | history retained; superseded, never applied |
| `learning_shadow_confidence_runs_v1` | unique source refresh run + environment | `apply_enabled IS FALSE` check |

No relevant materialized views were found. Exit V1/V2 and V1.3 use triggers; replay/warehouse are function-driven. Relevant tables have PKs, unique constraints and supporting slot/status indexes, but most cross-layer relationships are logical rather than foreign keys. V1.3/V1.4 have FKs to refresh/validation state; replay, warehouse, recommendation and position links do not.

### LOCAL LIVE vs LOCAL PAPER drift

Column-signature comparison found all named core learning/replay/entry/exit/market-memory tables equal. Confirmed broader drift:

- LIVE-only: ORC snapshot/opportunity/learning tables; `slot_kelly_allocation`; many ORC profitability, cooldown and latest-decision views; `v_adaptive_exit_shadow_v1`; slot cooldown/7d/regime views.
- PAPER-only: exit context coverage/north-star audit views; orchestrator simulation/candidate views.
- Same-name structural drift: `slot_capital_policy`; `v_slot_adaptive_sizing_v1`.
- Object absence is itself drift even where intentionally environment-specific. No V2 rollout should assume four-environment equivalence.

## 6. Automation Flow

Effective order in each runner tick:

```text
market-memory events -> clusters -> timeline -> opportunity -> ranking -> sequence
-> MME/ORC context -> ORC candidate context -> missed-opportunity replay
-> market-memory snapshots -> Slot Brain -> MFE/MAE snapshot
-> entry context snapshot -> exit telemetry V1/V2/V3/learning
-> shadow recommendations -> feature warehouse -> decision replay
-> V1.2 due gate -> V1/V1.1 refresh -> V1.3 trigger -> commit
-> V1.4 in isolated transaction (only after new successful source run)
-> later, independently: PAPER promotion publisher / LIVE allocator / LIVE ORC apply
```

Each analytics task checks enable/interval state, function existence, records `automation_kv` heartbeat/status/stats, and generally commits internally. Errors are caught, rolled back and the loop continues. V1.2 uses an advisory lock and due gate (default 12 h); run rows and unique keys provide idempotency. V1.3 is triggered by a successful refresh-run insert/update. V1.4 uses a separate transaction and source-run uniqueness, so its failure cannot roll back V1.2/V1.3.

Risks:

- The runner's shadow order is internally consistent with its current dependencies but opposite the desired V2 pipeline; replay is stale by one upstream conceptual generation if treated as an input.
- Separate commits create partially refreshed cross-object snapshots. There is no shared `pipeline_run_id` or evidence cutoff across MME, entry, exit, warehouse and replay.
- A task failure is isolated, but downstream tasks can run against mixed-age data in the same tick.
- Lookback upserts do not expire/delete older current aggregates consistently; stale records can remain.
- Refreshes are duplicated across telemetry, shadow and feedback branches with different cadence/windows.
- `automation_kv` is current-state telemetry, not immutable run audit for every stage.

Observed 2026-07-12: LIVE replay 19 distinct decisions, warehouse/recommendations 61 rows over 19 decisions; PAPER replay 273 decisions, warehouse 540 rows/275 decisions, recommendations 542 rows/276 decisions. Thus one decision has multiple recommendation rows, and PAPER currently has recommendations not yet represented in replay.

## 7. Key and Identity Model

```text
entry_trace_events.id -> missed_opportunity_replay.entry_trace_event_id  (NO BUY branch)
position_id -> entry_context_snapshot_v1 / exit_trace_v1/v2/v3          (executed branch)
slot = environment + symbol + interval + strategy [+ regime/window]
decision_key = md5(symbol|interval|strategy|entry_time)
decision_key -> recommendation rows -> warehouse rows -> one replay row
refresh_run_id -> proposal observations -> validation current state
validation_state.id -> V1.4 shadow confidence proposal
```

Answers:

1. There is no system-wide parent decision ID. `decision_key` is parent only inside the shadow recommendation/warehouse/replay branch.
2. A represented position normally maps to one computed key, but this is not enforced by a unique `position_id` constraint; exact-time matching and duplicate snapshots can violate the assumption.
3. NO TRADE has no common `decision_key`; it has an entry event ID and missed-opportunity replay record.
4. The key is reproducible only if all four string representations, especially timestamptz text, are identical. It is deterministic inside one PostgreSQL representation, but not a robust cross-system canonical ID.
5. Historical shadow recommendations are decision-linked. V1 calibration proposals and V1.3/V1.4 state are slot-window aggregate-linked, not decision-linked.
6. V1.3 observations capture the proposal of each refresh, but validation state follows current slot/action. It resets when action changes; it does not maintain an independent lifecycle row for every proposal lineage.
7. Action change resets counters correctly, but V1.1 deletes unresolved proposal rows and reuses/upserts keys, so proposal-history fidelity before the immutable V1.3 observation layer is incomplete.
8. Environment is present in database rows/slot keys, but database name is used as environment; no deployment-instance ID distinguishes LOCAL from VPS. Window and engine versions are absent from decision identity and inconsistently present in evidence, creating contamination risk if data is combined.

## 8. Current V1.4 Safety Model

V1 added 30-day slot statistics and shadow calibration proposals. V1.1 enforced minimum samples, one current proposal per slot/window and corrected PF summary behavior. V1.2 added due-aware 12-hour scheduling, run audit, advisory locking and status telemetry. V1.3 added immutable observations, mutable validation state, run audit and stability rules (defaults: 3 observations, 24 hours, action sample 30, minimum confidence 0.60). V1.4 converts only STABLE increase/reduce actions to bounded delta-only proposals, retains superseded history and isolates failure.

Confidence V1 is a bounded heuristic based on sample/context/edge evidence; V1.4 delta is capped by sample tier and source confidence tier, with absolute cap 0.05. PF sentinel values are evidence only and do not scale V1.4 delta. Sample quality is expressed through decision counts, context coverage and minimum gates, not a fully versioned quality model.

Safety is layered:

- SQL implementations contain no apply function or writes to trading/runtime objects.
- V1/V1.1/V1.2/V1.3 repeatedly persist apply flags as `0`/false.
- V1.4 run constraint rejects `apply_enabled=true`.
- V1.4 evidence states `runtime_mutation_allowed=false`.
- Runner calls V1.4 only after committed V1.2/V1.3 and in an isolated transaction.
- The later runner contains real ORC/bot-control mutation functions, but they are separate call paths and do not consume V1.4 tables/views.

Both databases reported runner `engine_mode=SHADOW`, `apply_enabled=0`, V1.4 status `ok`, zero stable inputs and zero V1.4 proposals. This proves isolation, not predictive quality.

## 9. Confirmed Gaps

1. No immutable universal decision ledger covering BUY, rejected entry and NO TRADE.
2. No independent replay truth upstream of recommendations.
3. No immutable recommendation observation tied to an evidence cutoff and later outcome horizon at decision grain.
4. No canonical FK chain across entry trace, position, decision, recommendation, warehouse and replay.
5. No structured/versioned exit reason contract.
6. No single SSOT for current configurable slot confidence; V1.4 intentionally stores delta only.
7. No outcome attribution separating entry, exit, market, MME, ORC, fees, execution and noise.
8. No counterfactual methodology for “recommendation would have improved expectancy/drawdown.”
9. No shared pipeline run/cutoff providing atomic evidence consistency.
10. No LOCAL/VPS deployment identity; no full four-environment schema parity proof.
11. No explicit retention policy for learning evidence, traces and proposals.
12. No engine/version fields in several base identities and snapshots.

## 10. Duplicated or Overlapping Components

- Slot metrics exist in Slot Brain, slot intelligence, ORC profitability views, learning slot statistics and exit learning, with 3d/7d/14d/30d/90d windows. They are not interchangeable.
- MFE/MAE/giveback appear in path views, exit traces V1–V3, replay and aggregates.
- Recommendation rows, warehouse raw evidence and replay learning/feature JSON copy much of the same payload.
- Multiple “replay” objects exist: missed-opportunity, trade-entry-exit and decision replay; none is the universal SSOT.
- Multiple confidence concepts exist: Slot Brain evidence confidence, regime confidence, recommendation confidence, validation confidence and calibration confidence.

SSOT should be assigned by domain: realized trade/accounting = positions/fills; path excursion = canonical MFE/MAE source; historical slot edge = versioned slot-stat snapshot; current readiness = realtime/MME/regime; portfolio allocation = ORC; learning recommendation = immutable observation. V2 must reference these, not recalculate their internal policies.

## 11. Learning Engine V2 Responsibilities

V2 owns recommendation observation history, temporally valid outcome attribution, confidence evolution proposals, regression/stability detection, evidence snapshots and promotion-gate state. It must not execute trades, mutate strategies/ORC/runtime/bot control, allocate capital or auto-apply calibration.

It must consume four explicitly separated products:

- **Historical edge:** versioned Slot Brain/learning statistics at a cutoff.
- **Current market readiness:** realtime, regime and MME snapshots at decision time.
- **Portfolio allocation:** ORC selection and constraints as observed facts.
- **Learning recommendation:** versioned hypothesis with target, delta and evidence.

## 12. Proposed V2 Data Model

Minimal design reuses V1.3 observations and V1.4 proposals as source data but does not mutate them.

| Object | Grain / keys | Core fields, lifecycle, retention, indexes | Source -> consumer / idempotency |
|---|---|---|---|
| `learning_engine_runs_v2` | one stage run; PK UUID; unique `(environment_id, engine_version, stage, cutoff_at, config_hash)` | started/finished, source watermarks, status RUNNING/SUCCEEDED/FAILED/SKIPPED, error, code/config hash; retain permanently | runner -> audit; deterministic run key |
| `learning_recommendation_observations_v2` | one immutable observed recommendation version; PK UUID; unique source lineage + observed cutoff | target, action, proposed delta/value, source proposal/state/run, slot, engine versions, evidence snapshot/hash, `observed_at`; immutable/permanent | V1.3/V1.4 -> attribution; content hash |
| `learning_recommendation_outcomes_v2` | one recommendation observation + outcome horizon/model version; PK UUID; unique `(observation_id, horizon, attribution_version)` | replay ID, position/entry event, realized and counterfactual metrics, component scores, coverage, reason; PENDING/ATTRIBUTED/INSUFFICIENT/INVALIDATED/EXPIRED; permanent | independent replay -> confidence/promotion; source watermark hash |
| `learning_confidence_history_v2` | one versioned confidence proposal state transition; PK UUID; unique `(target_key, sequence_no)` and idempotency key | target, old/proposed/delta, window, sample, expectancy, PF, drawdown, stability, recommendation confidence, reason, engine version; SHADOW/VALIDATING/REJECTED/EXPIRED/PAPER_CANDIDATE; permanent | outcomes -> review; append only |
| `learning_calibration_candidates_v2` | one candidate lineage/version | target/parameter, bounded change, status, evidence set hash, gates, expiry, supersedes; unique lineage+version | confidence history -> shadow replay/human review |
| `learning_calibration_evidence_v2` | bridge from candidate to immutable observation/outcome/evidence | PK `(candidate_id,evidence_type,evidence_id)`; contribution, included reason | normalized references, no payload duplication |

Separate `learning_engine_audit_v2` is unnecessary initially: immutable run rows, state-transition history and evidence hashes provide the audit. Add it only if actor/event-level compliance requires a generic append-only log. Index environment/time, slot/time, status/expiry, source IDs and evidence hash. Never cascade-delete evidence; use retention tiers only for bulky copied market payloads after a reproducible archive exists.

## 13. Proposed V2 Pipeline

```text
Canonical decision ledger (BUY + NO BUY)
-> independent decision replay (as-of cutoff, outcome horizons)
-> feature references/evidence snapshot
-> import V1.3/V1.4 recommendation observation
-> outcome attribution
-> versioned confidence evolution
-> calibration candidate
-> shadow replay validation
-> PAPER experiment candidate
-> human review
```

Every transition must record input watermark, output ID, engine/config/schema version, immutable evidence hash, idempotency key, status, reason and timestamps. A run reads only data at or before its declared cutoff; outcomes use explicit later horizons. Failed/incomplete runs never publish a “current” candidate. A consumer sees only SUCCEEDED run outputs.

## 14. Outcome Attribution Model

Attribution is not `profit => good`. For each observation compare the actual path, a defined baseline and a bounded counterfactual using net-of-fees metrics and uncertainty:

| Component | Evidence / question |
|---|---|
| `ENTRY_QUALITY` | price vs subsequent path, MAE before MFE, timing, rejected opportunity outcome |
| `EXIT_QUALITY` | MFE capture, giveback, avoided downside, early/late classification using structured exit payload |
| `MARKET_REGIME` | as-of regime and later transition; was evidence in-distribution? |
| `REALTIME_CONTEXT` | component scores at decision cutoff, never latest view |
| `MME_CONTEXT` | sequence/readiness/expiry at cutoff and coverage |
| `ORC_SELECTION` | whether allocation/selection constrained action; do not duplicate ORC score logic |
| `FEE_PRESSURE` | gross-to-net erosion and PF without sentinel arithmetic |
| `EXECUTION_QUALITY` | slippage, fill delay/quality and reconciliation status |
| `SAMPLE_NOISE` | uncertainty, effective sample size, dependence and regime coverage |

Outputs: directionally correct/incorrect/indeterminate, net-expectancy delta with interval, drawdown delta, attribution shares and coverage. A small disciplined loss may score correct under capital protection; a lucky low-quality win may score incorrect/indeterminate. “No recommendation” is evaluated against a predeclared baseline, not retrospectively selected winners. Counterfactuals that cannot be replayed are marked `INSUFFICIENT`, never imputed as proof.

## 15. Confidence Evolution Model

Confidence history is append-only. Each record includes target, old value (nullable until a confidence SSOT exists), proposed value, delta, engine/config version, evidence window/cutoff, sample/effective sample, net expectancy, PF plus finite/raw flag, drawdown impact, stability, recommendation confidence, reason, created time and status. A new evidence set creates a new version; it never overwrites the prior claim. Conflicting recommendations coexist as evidence but cannot both become active candidates for one target/version. Allowed V2 states are exactly `SHADOW`, `VALIDATING`, `REJECTED`, `EXPIRED`, `PAPER_CANDIDATE`.

Safe first target: reporting-only slot confidence delta. PAPER required: bounded slot-confidence multiplier and any entry-quality threshold experiment. Too risky for first V2: ORC weights, realtime weights, MME influence, exit parameters and risk-sizing multiplier because they create coupled feedback. Outside V2: direct strategy, allocation, execution or LIVE changes.

## 16. Promotion Gates

All numeric values below are calibration proposals, not production thresholds.

| Gate | Evidence requirements |
|---|---|
| SHADOW_PROPOSAL | valid identity/version/cutoff; non-empty evidence; proposed minimum effective sample 30 and 14 days; full audit/idempotency |
| STABLE | proposed >=3 independent observations across >=7 days and >=2 regimes; same direction; no material evidence regression |
| OUTCOME_VALIDATED | proposed >=50 attributable outcomes, >=30 days; positive lower-bound net expectancy vs baseline; PF finite and > baseline; drawdown no worse beyond tolerance; context coverage >=90% |
| REPLAY_CONFIRMED | proposed replay coverage >=95%; multiple horizons; no look-ahead/leakage tests; result consistent by regime and time split |
| PAPER_CANDIDATE | all prior gates; bounded reversible parameter; documented owner/expiry/rollback; no conflicting candidate |
| PAPER_VALIDATED | prospective PAPER A/B or sequential comparison, proposed >=100 independent decisions and >=30 days; net expectancy non-inferior/improved; drawdown guard; execution/fee parity |
| HUMAN_APPROVED | named reviewer, exact artifact/config hash, expiry, rollback and blast-radius approval; no automatic LIVE transition |

Every gate expires when evidence age exceeds its window, engine/schema version changes, coverage falls, direction flips, drawdown guard is breached or regime distribution materially drifts. “AI proposes, data proves” means the proposing sample is separated from validation/replay/PAPER samples.

## 17. Failure Modes

| Risk | Impact | Detection | Mitigation / stop condition |
|---|---|---|---|
| Small sample / lucky streak | overconfident promotion | effective N, bootstrap/time splits | minimum span/N; stop on unstable interval |
| Regime drift / non-stationarity | obsolete edge | PSI/distribution and per-regime results | expire/revalidate; stop on material drift |
| Survivorship bias | ignores disabled/rejected slots | universe reconciliation | include all decisions/no-trades; stop on missing universe |
| Look-ahead / leakage | false attribution | as-of tests and watermark audit | immutable cutoffs; invalidate run on future field |
| Duplicate evidence | inflated N | evidence hash/source uniqueness | bridge uniqueness; stop on duplicates |
| Stale/conflicting recommendation | wrong target/direction | expiry and one-active-target checks | supersede/expire; block promotion |
| Cross-environment contamination | invalid proof | deployment/environment IDs | physical/logical isolation; invalidate mixed evidence |
| LIVE/PAPER mismatch | non-transferable result | schema/config/data-distribution diff | PAPER validation plus parity gate |
| LOCAL/VPS history difference | incomplete lineage | source instance watermarks | never merge without provenance |
| Sentinel PF | infinite-looking evidence | finite/raw PF flag | cap/display separately; never scale confidence |
| Missing context | biased subset | component coverage | `INSUFFICIENT`; stop below coverage gate |
| Partial refresh / race | mixed-age evidence | shared run/cutoff and source watermark | publish only successful coherent runs |
| Schema/engine drift | incomparable records | schema/code/config hashes | new lineage; block cross-version aggregation |
| Feedback amplification | self-reinforcing confidence | exposure-adjusted monitoring/control group | bounded deltas; PAPER only; freeze on divergence |
| Temporary drawdown reaction | destroys valid edge | drawdown duration/regime decomposition | hysteresis; no immediate reduction |
| Lucky promotion | future regression | holdout/prospective validation | independent PAPER gate; reject on holdout failure |

## 18. Implementation Phases

0. Contract hardening: canonical environment/deployment ID, universal decision ledger design, structured exit reason, source cutoffs and replay SSOT decision.
1. Reporting-only observation import and outcome attribution tables/views; backfill in isolated V2 schema/tables.
2. Shadow confidence history with immutable versions and regression/stability reporting.
3. Shadow replay candidate validation with holdouts and gate audit.
4. PAPER candidate export only; no automatic parameter write.
5. Separate future project for controlled PAPER experiments after review. LIVE influence is explicitly not part of V2.

## 19. Validation Plan Across Four Environments

Develop and validate sequentially: LOCAL LIVE read-only shadow, LOCAL PAPER shadow/backfill, then (in a separately authorized rollout) VPS LIVE shadow and VPS PAPER shadow. Before each stage compare Git SHA, schema/object/function hashes, engine/config versions, timezone, environment/deployment ID and source watermarks. Validate counts, uniqueness/FKs, as-of cutoffs, no future leakage, idempotent rerun hashes, failure rollback, zero runtime-table writes and safety-audit zero rows. Cross-environment comparisons must aggregate metrics, never merge identities without provenance. This audit did not connect to VPS.

## 20. Explicit Non-Goals

No execution, order placement, strategy changes, ORC score/pick changes, runtime/bot-control writes, capital allocation, risk sizing, automatic apply, migration execution, service/container changes, deployment or LIVE/PAPER influence. No claim that historical association is causal.

## 21. Open Questions

1. Which event can become the durable universal decision ID before strategy evaluation, and can every NO BUY path emit it?
2. What is the intended SSOT and semantics of configurable slot confidence?
3. Which replay is authoritative, and what exact counterfactual actions/horizons are permitted?
4. Can exit code/context be captured structurally at close without parsing legacy text?
5. Which slot definition is canonical when regime changes after entry?
6. What are approved retention and privacy/storage limits for immutable market evidence?
7. Are environment-specific ORC schema differences intentional and documented?
8. How will LOCAL versus VPS deployment provenance be represented?
9. Which metrics/windows are authoritative for promotion, and how is trade dependence handled?

## 22. Recommended First Implementation Slice

**Recommendation Outcome Attribution V2 — reporting only.** First create a V2 run ledger, immutable import of V1.3/V1.4 recommendation observations, and outcome rows that reference an independent replay snapshot at explicit horizons. Limit scope to existing slot-confidence recommendations and closed executed decisions with complete structural entry/exit/fee evidence. Do not compute/apply new confidence yet. Add NO TRADE only after the universal decision-ID contract is available.

Acceptance: 100% SHADOW; writes only new V2 tables; deterministic content/run keys; immutable evidence and as-of tests; rerun produces no duplicates; zero dependencies from runtime/ORC; separately removable scheduler flag; audit of all source IDs/versions; explicit `INSUFFICIENT` for missing/counterfactual evidence.

## Pre-state and Audit Evidence

```text
hostname: srv
pwd: /home/jacek/trading-bot
branch: main
HEAD: 8ca8452db299f1cb534f6f6fca7fa75e8dfcd1ee
origin/main: 8ca8452db299f1cb534f6f6fca7fa75e8dfcd1ee
```

Recent commits: `8ca8452`, `6f7727a`, `31ec9e1`, `c4d7106`, `a56606f`, `c8bac22`, `4520dd5`, `ed26b31`, `768e7fb`, `c6449c1`.

Worktree was not clean before audit. Existing untracked files comprised four automation-runner `.bak` files, a tar archive, local audit outputs and LOCAL LIVE/PAPER table/view probes. They were not modified. HEAD matched GitHub-tracking `origin/main`; no fetch was performed.

Database inspection used PostgreSQL 16.11 catalog queries inside LOCAL `trading_live` and `trading_paper`, all wrapped in `BEGIN READ ONLY`. Source/Git/schema/log inspection was read-only. No migration, Docker Compose operation, restart, service/runtime/database change, commit, push or VPS access occurred.

## Final Verdict

**NOT READY FOR V2 IMPLEMENTATION**

The system is ready for a narrowly scoped design-hardening and reporting-only attribution slice, but not the full V2 engine. Implementation must first establish an independent replay/outcome truth, a universal decision identity including NO TRADE, temporally valid immutable recommendation observations, and resolved/documented schema provenance. Starting confidence evolution or adaptive calibration before those contracts would measure recommendations against data derived from those same recommendations and would amplify identity, leakage and cross-environment risks.

# WalTrade Master Execution Roadmap

CURRENT_PHASE=SYSTEM_CONFORMANCE_MR1_MR2_LOCAL_PAPER_ACCEPTED

CURRENT_PRIMARY_RESEARCH=PAUSED_PENDING_SYSTEM_CONFORMANCE_REPAIR

CURRENT_EXECUTION_SCOPE=MINIMUM_WALTRADE_CONFORMANCE_RECOVERY

VPS_PAPER_RUNTIME_REVISION_STATUS=NON_UNIFORM_NOT_ACCEPTED

SUPERSEDED_VPS_PAPER_RUNTIME_SHA=d60c4517892c220b6450876c47f27d99e8bf4dc8

OWNERSHIP_CANDIDATE_INTRODUCED_SHA=d60c4517892c220b6450876c47f27d99e8bf4dc8

Execution order:

`LOCAL implementation/tests -> commit -> push exact SHA to GitHub -> LOCAL PAPER deploy exact SHA -> LOCAL acceptance -> VPS fetch and inspect -> VPS pull --ff-only exact approved history -> VPS PAPER deploy -> independent VPS acceptance -> frozen LIVE environments last`

The North Star is frozen in [WALTRADE_CONSTITUTION.md](WALTRADE_CONSTITUTION.md). This roadmap is the living execution plan and records current verified truth rather than preserving superseded blockers.

## 0. System conformance repair gate

Four completed conformance audits—LOCAL PAPER, VPS PAPER, LOCAL LIVE and VPS
LIVE—have been consolidated into
[WALTRADE_SYSTEM_CONFORMANCE_REPAIR_V1.md](WALTRADE_SYSTEM_CONFORMANCE_REPAIR_V1.md).
The bounded repair plan contains 5 unique P0, 9 unique P1, 7 unique P2 and 3
unique P3 findings. P3 remains backlog unless a direct repair dependency proves
it necessary.

`NEW_ECONOMIC_EXPERIMENTS=PAUSED`.
`ACTIVE_HOLDOUT_EFFICACY_INSPECTION=PROHIBITED_DURING_REPAIR`.
`LOCAL_LIVE_NEW_ENTRY_AUTHORITY=OFF`.
`VPS_LIVE_NEW_ENTRY_AUTHORITY=OFF`.
`VPS_PAPER_RUNTIME_REVISION_STATUS=NON_UNIFORM_NOT_ACCEPTED_BY_CONFORMANCE_AUDITS`.
The former `d60c451...` runtime value is historical/superseded and is not
current deployment truth.

`REGIME_POLICY_SOURCE_CHECK=RESOLVED_BY_CORRECTED_POPULATION_REPRODUCTION`;
`POLICY_SEMANTICS_UNRESOLVED=NO`. The canonical 20/20 matrix reproduces the
accepted corrected-population counterfactual exactly: 245 of 971 positions,
`-34.148466549209 USDC` net. Its fingerprint is
`585ab57f906dff274e5df344475eb24de6f4977a3985535427edb7852093eb3e`.

Current minimum recovery order:

`MR1_MINIMAL_RELEASE_TRUTH -> MR2_CORE_ADMISSION_REPAIR -> MR3_DIRECT_PAPER_ENFORCEMENT_AND_ECONOMIC_ACCEPTANCE`

MR1 and MR2 are implemented and technically accepted on LOCAL PAPER. The
touched runtime revision is `03e5bec329520e9d16966078c5614f8ed666af09`;
32/32 slots use direct `ENFORCE`, the gate and watchdog share `market_regime`,
and missing/stale/unknown/policy/PANIC uncertainty blocks entry. MR3 technical
direct enforcement has begun without shadow; its forward economic verdict is
separate and pending. LIVE remains frozen.

Original Wave 3/4 remainder, H12, Risk Budget, Slot Brain, ORC, Learning and
migration-wide cleanup remain deferred and authority-OFF, not PASS. The
minimum API/UI truth needed for this repair was completed locally because the
existing UI omitted semantic and authority health.

Complete-register planning order, retained for deferred defects:

`WAVE_1_RELEASE_TRUTH -> WAVE_2_REGIME_SSOT -> WAVE_3_ACTIVE_CAPABILITIES -> WAVE_4_SCHEMA_DEPLOYMENT_UI -> WAVE_5_DIRECT_PAPER_ENFORCEMENT`

The authorized LOCAL implementation completed MR1 and MR2 and started MR3
direct LOCAL PAPER enforcement. Deferred Wave 3/4 work is not a prerequisite
for this bounded recovery and receives no authority or PASS. Shadow is not a
substitute for a proven repair. LIVE entry authority remains frozen throughout.

At the first deployment that changes entry/runtime decision semantics, close
all affected earlier regime-dependent economic cohorts at the exact deployment
cutoff as `PRE_CONFORMANCE_LEGACY_EVIDENCE`, `UNTREATED`, and
`REGIME_NOT_ENFORCED`. Never pool them with post-repair evidence and do not
inspect their efficacy during repair. These labels do not apply to independent
Fee V2, Financial Truth, entry-atomicity or infrastructure proofs.

`FORWARD_BASELINE_CUTOFF=2026-09-06T20:33:57.907250026Z`.

### LOCAL PAPER MR1/MR2 acceptance

- `RELEASE_TRUTH=PASS`: repository/image/runtime/config identities are
  independently reported; the pre-repair revision split was detected and all
  touched services now run `03e5bec329520e9d16966078c5614f8ed666af09`.
- `REGIME_SSOT=market_regime`; gate/watchdog contract
  `REGIME_GATE_MARKET_REGIME_SSOT_V1`; canonical policy coverage `20/20`;
  active legacy `SUPER_TREND` rows `0`.
- `DIRECT_LOCAL_PAPER_ENFORCEMENT=ACTIVE`; 32/32 slots are enabled ENFORCE.
  Deterministic E2E proves allow/block/missing/stale/PANIC/SUPERTREND paths
  without placing an order.
- Bounded acceptance: 32/32 fresh slots, 5/5 support workers healthy, blocked
  DB sessions `0`, critical errors/tracebacks `0`, restart counts stable.
- PAPER and LIVE API/UI edge routes return HTTP 200. No LIVE service, config,
  authority, position or order was changed.
- Forward MR3 economic collection begins at the cutoff above; no economic
  result is claimed yet.

## 1. Foundation state

| Foundation | Current verified state |
| --- | --- |
| PAPER entry atomicity | `PAPER_ENTRY_ATOMICITY=PASS_2_2` |
| Risk Budget immutable-event contract | Component contract passed historically; current four-environment runtime activation/parity requires Wave 3 re-acceptance |
| Risk Budget promoted commit | `f965a0b35f8be1b900cbd0e73332c653b003ca0a` |
| Risk Budget influence | `OFF` |
| Learning auto-apply | `OFF` |
| Fee V2 | `0.35% per side` |
| Full roundtrip break-even movement | `~0.7024586051%` |
| LOCAL LIVE capital preservation | `ACTIVE`; new entries `NO`; exit/close `YES` |
| VPS LIVE capital preservation | `ACTIVE`; new entries `NO`; exit/close `YES` |
| Expected VPS LIVE slots | `28`; prior `32` used the full PAPER universe as the LIVE denominator |
| LOCAL full-opportunity throughput fix | `PASS`; exact projection indexes |

PAPER entry order/commitment, canonical ENTRY fill, position linkage, and frozen Fee V2 contract are atomic across all four strategies. Missing or conflicting frozen ENTRY fee evidence fails closed before PAPER exit intent. The Risk Budget STATE_EVALUATION immutable-event contract passed its component proof, but the conformance audits supersede any claim that its current four-environment deployment/activation is accepted. Wave 3 must re-prove valid policy resolution, runtime revision, provenance normalization, idempotent same-cutoff retry, and true-conflict fail-closed behavior while influence remains OFF.

Git SHA alone is insufficient: relevant rollouts require Git, contract, direct schema dependency, and runtime/semantic parity.

`CURRENT_EXPECTED_VPS_LIVE_SLOT_COUNT=28`.
`WHY_28_VS_32=EARLIER_STATUS_USED_FULL_PAPER_UNIVERSE_AS_LIVE_DENOMINATOR`.
This clarification changes no config contract, runtime contract, or safety
authority: `CONFIG_CONTRACT_CHANGE=NO`, `RUNTIME_CONTRACT_CHANGE=NO`, and
`SAFETY_IMPACT=NO`.

### LOCAL / VPS governance

LOCAL and VPS consume the same promoted shared software contract, but they are
operationally isolated and retain independent runtime and data histories.
LOCAL is the only implementation and Git promotion authority: implementation,
tests, commits, pushes, and canonical documentation updates originate on
LOCAL. VPS is strictly pull-only. It may `git fetch`, `git pull --ff-only`,
deploy promoted artifacts, and validate them independently. VPS must never
commit, push, create VPS-only shared-code fixes or schema magic, or change
frozen candidate semantics.

Public communication is governed by
[WALTRADE_PUBLIC_COMMUNICATION.md](WALTRADE_PUBLIC_COMMUNICATION.md). It
consumes canonical evidence but creates no project evidence, experiment
authority, Roadmap priority, or LIVE authority.

Canonical promotion path:

`LOCAL implementation/tests -> commit -> push exact SHA to GitHub -> LOCAL PAPER deploy exact SHA -> LOCAL acceptance -> VPS fetch and inspect -> VPS pull --ff-only exact approved history -> VPS PAPER deploy -> independent VPS acceptance -> frozen LIVE environments last`

For corresponding environments—`LOCAL PAPER ↔ VPS PAPER` and, where
applicable, `LOCAL LIVE ↔ VPS LIVE`—the promoted shared contract requires
`GIT_PARITY`, `CONTRACT_PARITY`, `DIRECT_SCHEMA_DEPENDENCY_PARITY`,
`SCHEMA_OBJECT_PARITY`, `RUNTIME_SEMANTIC_PARITY`, and
`CONFIG_CONTRACT_PARITY` where shared and applicable. The rollout gate is:

The immutable acceptance gate then requires `GIT_PARITY`, `CONTRACT_PARITY`,
`DIRECT_SCHEMA_DEPENDENCY_PARITY`, `SCHEMA_OBJECT_PARITY`, and
`RUNTIME_SEMANTIC_PARITY` at each applicable deployment stage; no stage is
skipped because another stage passed.

A common Git SHA alone never proves deployment parity.

`SCHEMA_OBJECT_PARITY` means every object required by the active promoted
runtime contract is semantically equivalent between corresponding LOCAL and
VPS environments. Where applicable this covers tables, columns, column types,
nullability, defaults, primary keys, foreign keys, unique constraints, check
constraints, indexes, views, materialized views, functions, procedures,
triggers, extensions, required migration state, and direct runtime schema
dependencies. For example, if LOCAL has a required
`public.orc_apply_slot_decisions_v1` object and the corresponding VPS
environment does not, parity fails even when `GIT_PARITY=PASS`.

Parity does not mean blindly applying every migration everywhere. It requires
the migrations and schema objects used by the active contract for that
corresponding environment. Environment-specific objects are allowed only when
they are explicit parts of the promoted environment contract; LOCAL-only or
VPS-only schema magic is forbidden.

`BUSINESS_DATA_PARITY=NOT_REQUIRED` and
`BUSINESS_DATA_DIVERGENCE=EXPECTED`. Row counts, trades, positions, fills,
orders, balances, Financial Truth rows, opportunity observations,
counterfactual evidence, timestamps, market outcomes, learning/evidence
history, and runtime-generated audit rows naturally differ. Those differences
are not parity failures by themselves and must never be copied merely to make
counts or histories match. Canonical principle:
`SAME_SHARED_CONTRACT; DIFFERENT_NATURAL_DATA`.

Research operating model:

- `LOCAL_PAPER_CAUSAL_TREATMENTS=ONE_ACTIVE_AT_A_TIME`.
- When a LOCAL experiment is active, VPS PAPER remains its untreated natural
  reference and may also provide read-only discovery for future experiments.
- No simultaneous second VPS treatment is authorized while VPS is required as
  that untreated reference.
- Evidence used to select or evaluate a candidate is burned for validation of
  that candidate; future causal validation requires new natural evidence.

### Equity UI canonical read authority

`EQUITY_UI_CANONICAL_AUTHORITY=VPS_LIVE_COMPLETE`. LIVE `/ui/equity` reads the
accepted managed-capital baseline plus canonical managed-equity history without
a legacy fallback or fabricated period history. Closure evidence is:

- `EQUITY_UI_GIT_PARITY=PASS`
- `EQUITY_UI_CONTRACT_PARITY=PASS`
- `EQUITY_UI_DIRECT_SCHEMA_DEPENDENCY_PARITY=PASS`
- `EQUITY_UI_RUNTIME_SEMANTIC_PARITY=PASS`

No new schema, engine, endpoint, frontend model, or data repair was required.
PAPER does not consume the LIVE managed-capital authority and was not rolled
out for this change.

### Full PAPER Opportunity Observation throughput

- `VPS_PAPER_DIRECT_SCHEMA_DEPENDENCY_REPAIR=COMPLETE`.
- `ROOT_CAUSE=EXPENSIVE_PROJECTION_LOOKUPS_PLUS_FIFO_SINGLE_CONSUMER`.
- `LOCAL_THROUGHPUT_FIX=PASS`: exact indexes now serve the evidence and entry-trace projection lookups without changing FIFO or canonical payload semantics.
- `OWNERSHIP_V1_FROZEN_COHORT=UNCHANGED`.
- `FUNCTIONAL_CORRECTNESS=PASS`.
- `FULL_PAPER_OPPORTUNITY_OBSERVATION_HEALTH=PASS`.
- `FORWARD_CANONICAL_FRESHNESS=PASS`.
- `PAPER_RUNTIME_HEALTH=PASS`; `WORKERS=32/32_FRESH`.
- `DB_HEALTH=PASS`; `BLOCKED_DB_SESSIONS=0`.
- `CUTOFF_MISSING_LOGICAL_KEYS=0`; `CUTOFF_MISSING_1M_KEYS=0`;
  `CUTOFF_MISSING_5M_KEYS=0`.
- `DUPLICATE_OBSERVATION_KEYS=0`; `DUPLICATE_CAUSAL_EVENT_IDS=0`.
- `NOT_EXACTLY_ONE_KEYS=0`.
- `ELIGIBLE_UNPROCESSED_THROUGH_CUTOFF=0`.
- `DIRECT_SCHEMA_DEPENDENCY_PARITY=PASS`.
- `ROWS_PER_MINUTE_EFFECTIVE=4123.713`;
  `FORWARD_SOURCE_ARRIVAL_RATE=25.200_PER_MINUTE`.
- `SERVICE_RATE_ABOVE_ARRIVAL=YES`;
  `SERVICE_RATE_HEADROOM=4098.513_ROWS_PER_MINUTE`.
- `THROUGHPUT_COMPARISON_VERDICT=METRIC_DEFINITION_INCONSISTENCY_CATCHUP_DRAIN_CAPACITY_VS_STEADY_STATE_REALIZED_RATE_NO_ACTUAL_REGRESSION_PROVEN`.
- `CURRENT_PIPELINE_RISK=MODERATE`; `PIPELINE_CHANGE_REQUIRED=NO`.

The forward-health contract is PASS. Catch-up drain capacity and steady-state
realized rate are different metric definitions; no actual regression is
proven. No queue change is authorized.

## 2. Current economic reality

PRIMARY_ECONOMIC_DEFECT=`INSUFFICIENT_MOVEMENT_RELATIVE_TO_FULL_COST`

Latest VPS PAPER forensic baseline:

| Metric | Value |
| --- | ---: |
| Trades | 1,515 |
| Net | -183.346579 USDC |
| Fees | 212.195019 USDC |
| Insufficient-movement rate | 78.2178% |

The immediate goal is to lose less, then reach break-even after all costs, then demonstrate a repeatable positive edge. Risk Budget numeric influence, dynamic sizing, Capital Allocation, and Learning authority remain premature.

LOCAL PAPER movement-capacity research uses a different, explicitly bounded
economic concept and does not overwrite the VPS baseline above. In the
canonical 1,517-trade LOCAL cohort, 828 opportunities (54.581%) reached the
0.7024586051% full-cost hurdle through maximum favorable excursion during the
next 240 minutes; 689 (45.419%) did not. Only 172/1,517 finished with positive
authoritative net PnL, and aggregate final net was -177.145833 USDC.
`240M_MFE_FULL_COST_COVER` is counterfactual movement capacity, not final
realized profitability. The earlier 78.2178% figure belongs to its recorded
VPS cohort and trade-level forensic definition; the two rates must not be
merged.

Earlier binary-label evidence found movement-capacity signal led by `ATR_PCT`,
BBRANGE `BB_WIDTH_PCT`, and absolute EMA slope, but later corrected-label
walk-forward evidence did not support a stable current `STRONG` predictability
claim. It separates final non-value-add admissions only weakly. For 923 admissions made while
same-symbol risk already existed, marginal portfolio value was -103.994083
USDC and 822/923 (89.057%) were non-value-add. No new score or engine is
required.

### PAPER research and LIVE economic authority

PAPER is the `RESEARCH_UNIVERSE`, not a portfolio that must be net-profitable
as a whole. It may intentionally contain bad and blocked opportunities, failed
hypotheses, counterfactuals, exploratory decisions, and negative aggregate
PnL. `TOTAL_PAPER_PNL > 0` is not a universal progress gate.
`PAPER_RESEARCH_UNIVERSE_MAY_BE_NET_NEGATIVE=YES`.

- `RESEARCH_UNIVERSE`: the broad PAPER opportunity set.
- `QUALIFIED_UNIVERSE`: frozen candidate policies that survived independent validation.
- `LIVE_UNIVERSE`: only explicitly approved decisions from the qualified universe.

LIVE never inherits the full PAPER universe automatically. `LESS_NEGATIVE` is
research progress and `NEAR_ZERO` is promising; neither grants LIVE authority.
Potential LIVE eligibility requires a frozen class, cohort, or policy with
LOCAL PAPER PASS, VPS PAPER independent validation PASS, positive expected net
after all costs with acceptable uncertainty, acceptable `GOOD_MISSED` and
drawdown/risk, execution/Financial Truth/auditability PASS, and explicit
Product Owner approval. No individual trade is presumed profitable in advance.
`EXPECTED_NET_AFTER_ALL_COSTS > 0` is mandatory for potential LIVE eligibility.

Historical winners may not be selected after outcome to define the LIVE
cohort. Selection semantics must precede outcomes:
`candidate frozen → independent PAPER evidence → qualified cohort economics → risk review → LIVE eligibility`.

## 3. Ranked STOP LOSING mechanisms

Prior research order was probabilistic and capital-first; it is superseded as
current work by system conformance repair:

1. preserve the current Movement V1 forward holdout untouched and uninspected;
2. run the frozen, non-blocking P4 same-thesis forward holdout independently;
3. preserve H12 as complete with verdict B and no capital authority;
4. preserve the untouched H12 forward calibration persistence evidence without
   inspection; it is not current work and cannot start H13;
5. only after historical qualification and new forward proof may bounded
   risk/capital influence be reviewed;
6. LIVE remains last and requires explicit Product Owner approval.

Economic Floor boundary/recovery collection remains passive evidence only and
is not the automatic next economic priority. Completed read-only discovery
does not itself schedule an active treatment.

Current experiment roadmap:

- `#1 Ownership=COMPLETE`.
- `#2 Economic Floor V1/V2 cadence family=COMPLETE_FOR_CADENCE_QUESTION`.
- `PRIOR_PRIMARY_RESEARCH=PROBABILISTIC_CAPITAL_DECISION_FOUNDATION`;
  `CURRENT_PRIMARY_RESEARCH=PAUSED_PENDING_SYSTEM_CONFORMANCE_REPAIR`.
- `MOVEMENT_V1_FORWARD_HOLDOUT=ACTIVE_UNINSPECTED`.
- `H11_P4_FORWARD_HOLDOUT=ACTIVE_UNINSPECTED_NON_BLOCKING`.
- `#4 New Risk=UNSUPPORTED_CURRENT_HYPOTHESIS`.
- `#5 Same Thesis=H11_HISTORICALLY_CONFIRMED_P4_FROZEN_NOT_AUTHORIZED`.
- `#6 Fee Velocity=FEE_DRAG_YES_VELOCITY_GATE_NO`.
- `#7 Mixed Duplication=PARTIAL_NO_INTERVAL_GATE`.

Additional-admission economics:

| Metric | Value |
| --- | ---: |
| Additional admissions | 965 |
| Non-value-add admissions | 868 |
| Non-value-add rate | 89.948% |
| Non-value-add fees | 121.513802 USDC |
| Non-value-add net | -122.770951 USDC |

## 4. Ordered strategy-pair evidence

| Ordered pair | N | Winners | Cost cover | Net |
| --- | ---: | ---: | ---: | ---: |
| BBRANGE → RSI | 134 | 1 | 0.746% | -18.591305 USDC |
| RSI → BBRANGE | 48 | 7 | 16.667% | -4.445463 USDC |
| BBRANGE → TREND | 212 | — | — | -26.227686 USDC |
| TREND → BBRANGE | 144 | — | — | -15.420940 USDC |

The asymmetry matters. A global one-strategy-per-symbol rule is not justified. Ownership policies must be ordered, evidence-specific, and causally validated.

### Regime-dependent strategy economics principle

`STRATEGY_VALUE_IS_CONTEXT_DEPENDENT`. Current BBRANGE dominance does not
prove that BBRANGE is globally bad, just as historical periods dominated by
TREND or SUPERTREND do not prove that either strategy is globally bad.
Strategy economics must be evaluated conditional on `REGIME`, `STRATEGY`,
`SYMBOL`, `INTERVAL`, `PORTFOLIO_STATE`, and `COST_ENVIRONMENT`.

When enough authoritative evidence exists, evaluate
`STRATEGY × REGIME × SYMBOL × INTERVAL` with at least `N`,
`ADMISSION_SHARE`, `NET_AFTER_COSTS`, `FEES`, `WIN_RATE`, `MFE`, `MAE`,
`FULL_COST_COVER_RATE`, `CAPITAL_HOURS`, `ADDITIONAL_RISK_RATE`, and
`MARGINAL_PORTFOLIO_VALUE`. Where sample size permits, also evaluate
same-symbol existing exposure, 1m/5m overlap, same-thesis exposure, and exit
reason distribution.

Do not infer `BBRANGE_ACTIVE_NOW → BBRANGE_GLOBAL_BLOCK` or
`TREND_LOST_IN_PRIOR_PERIOD → TREND_GLOBAL_BLOCK`. Determine whether a
strategy is economically useful inside a specific market and regime context.
Reuse existing ORC and regime evidence first under `REUSE_BEFORE_BUILD`; this
principle does not authorize a new regime engine or a global strategy block.

Further findings:

- `SAME_THESIS_ADMISSIONS=572`
- `SAME_THESIS_NET=-69.028271 USDC`
- `REGIME_TRANSITION_ADMISSIONS=387`
- `REGIME_TRANSITION_NET=-45.423024 USDC`
- `OWNERSHIP_FAILURE_IS_TRANSITION_DRIVEN=NO`

Historical LOCAL and VPS evidence strongly supports the RSI-after-BBRANGE hypothesis, but no experimental treatment is economically proven until sufficient causal evidence matures.

## 5. 1m/5m semantics and portfolio structure

- `1M_5M_SEMANTIC_VERDICT=MIXED`
- `1M_AFTER_5M_ADMISSIONS=310`
- `1M_AFTER_5M_NET=-35.277603 USDC`
- `5M_AFTER_1M_ADMISSIONS=325`
- `5M_AFTER_1M_NET=-35.998338 USDC`
- `PORTFOLIO_CROWDING_EFFECT=WEAK`

The evidence does not authorize a global interval block. Continue counterfactual observation and distinguish independent horizons, confirmation, and duplicate thesis before intervention.

## 6. Winner tail and exit giveback

- `WINNER_TAIL_DEPENDENCE=LOW`
- `TOP_10_PERCENT_WINNERS_LOSS_OFFSET=1.875%`
- Immediate tiny-positive exit: `REJECTED`
- Post-cost-cover giveback: `PROVEN SECONDARY LEAK`
- `112/115` tiny-positive-to-loss outcomes ended through `PROFIT_LOCK_TRAIL_DROP`

`ECONOMIC_FLOOR_AFTER_COST_COVER_V1` completed its bounded first active review.
The cost-cover/realizable-net mechanism is validated, but the exact-zero active
boundary is not: at the observed 60–300 second evaluation cadence it acted too
late to preserve non-negative final execution. V1 remains separate from entry
treatments, is not classified as harmful, and is not qualified for VPS or LIVE.

The active LOCAL PAPER exit-only research candidate is
`ECONOMIC_FLOOR_V2_CADENCE_INDEPENDENT_PROTECTION`. It asks whether the same
authoritative protection can run at a common causal cadence independent of the
originating strategy interval by reusing existing market/evidence
infrastructure and avoiding a new engine where possible. V2 freezes no buffer,
ratchet, or strategy-, symbol-, or regime-specific floor.

### Economic Floor V1 first active review and closure

`ECONOMIC_FLOOR_V1_STATUS=COMPLETE`;
`ECONOMIC_FLOOR_V1_MECHANISM_VALIDATED=YES`;
`ECONOMIC_FLOOR_V1_ACTIVE_BOUNDARY_VALIDATED=NO`;
`ECONOMIC_FLOOR_V1_VPS_PROMOTION_READY=NO`;
`ECONOMIC_FLOOR_V1_LIVE_ELIGIBILITY=NO`;
`ECONOMIC_FLOOR_V2_IMPLEMENTED=YES_LOCAL_PAPER_ONLY`;
`ECONOMIC_FLOOR_V1_FINAL_VERDICT=MECHANISM_PROMISING_BOUNDARY_TOO_LATE_AT_DISCRETE_EVALUATION_CADENCE`.

The authoritative LOCAL PAPER cohort contains `ACTIVE_ARMED_COUNT=12`,
`ACTIVE_FLOOR_EXIT_COUNT=7`, and `ARMED_BUT_EXISTING_EXIT_WON=5`. The seven
floor exits produced net sum `-0.0827958600555 USDC`, average
`-0.0118279800079 USDC`, minimum `-0.02196618160 USDC`, and maximum
`-0.005944715805 USDC`. No upside interference or premature-exit harm is
proven.

The mechanism operated as designed and mechanically limited post-cover
giveback, but counterfactual net benefit is not proven. All seven exit paths
had complete causal market data and ordering, and final net matched realizable
net at the exit evaluation. Fee/rounding error, stale market data, and an
implementation defect are not supported. The result is
`ZERO_BOUNDARY_EXECUTION_VERDICT=A_EXACT_ZERO_BOUNDARY_PLUS_DISCRETE_EVALUATION_CADENCE`:
the exact-zero boundary was too late at the observed 60–300 second cadence.

For boundary research,
`MIN_OBSERVED_POSITIVE_BUFFER_NEEDED_TO_HAVE_EXITED_NON_NEGATIVE=NOT_IDENTIFIABLE_FOR_ALL_7_UNDER_NO_SAME_EVALUATION_EXIT`.
Five of seven positions had a later positive evaluation; the uniform observed
buffer for only that subset was `0.02508313990 USDC`. This is descriptive
evidence, not an authorized or frozen threshold. No `+0.025` buffer, parameter
sweep, or in-place V1 tuning is permitted.

V2's research question is whether the same protection can be evaluated at a
common causal cadence independent of strategy interval. Reuse existing
infrastructure first. Do not add a new engine if avoidable, and do not add a
buffer, ratchet, or strategy-, symbol-, or regime-specific floor.
`STEPWISE_RATCHET` remains future research after the basic protection boundary
is reliable.

### Economic Floor V2 final cadence decision

`ECONOMIC_FLOOR_V2_STATUS=COMPLETE`;
`ECONOMIC_FLOOR_V2_FINAL_VERDICT=CADENCE_FIX_VALIDATED_BOUNDARY_REMAINS_PRIMARY_ISSUE`.

`V2_1M_ORIGIN_EXIT_COUNT=2`; `V2_5M_ORIGIN_EXIT_COUNT=6`;
`FIVE_MINUTE_ORIGIN_ARM_COUNT=8`;
`FIVE_MINUTE_ORIGIN_FLOOR_EXIT_COUNT=6`.

The authoritative LOCAL PAPER cadence cohort contains
`TOTAL_V2_ARM_EVENTS=14`, `TOTAL_V2_EXIT_INTENTS=8`, and
`TOTAL_COMPLETED_V2_FLOOR_EXITS=8`. Two floor exits originated on 1m and six
on 5m; eight 5m-origin positions armed and six produced floor exits.

`CADENCE_DEFECT_FIXED_ARCHITECTURALLY=YES`;
`V2_CADENCE_INDEPENDENCE_VALIDATED=YES`;
`V2_UPSIDE_PRESERVED=YES`;
`V2_BOUNDARY_VALIDATED=NO`.

The remaining negative-exit primary cause is the exact-zero boundary, not the
removed 5m architectural cadence dependency. No upside interference or
premature-exit harm is proven. V1's three 5m exits averaged
`-0.0151844783127 USDC` with average cross-zero gap
`0.0282080437513 USDC`; V2's six 5m exits averaged
`-0.0128205821774 USDC` with average cross-zero gap
`0.01745574779875 USDC`. These small, non-equivalent cohorts provide
descriptive/mechanistic evidence only and authorize no statistical improvement
claim.

`SMALL_V2_ECONOMIC_COHORT_SUFFICIENT=YES`;
`SUFFICIENCY_SCOPE=CADENCE_EXPERIMENT_CLOSURE_DECISION_ONLY_NOT_STATISTICAL_PERFORMANCE_PROOF`;
`ECONOMIC_FLOOR_V2_VPS_PROMOTION_READY=NO`;
`ECONOMIC_FLOOR_V2_LIVE_ELIGIBILITY=NO`.

V2 is not harmful, must not be tuned in place, and requires no further evidence
collection merely to increase N. The docs task changes no deployed runtime or
treatment configuration.

The historically proposed separate experiment was
`ECONOMIC_FLOOR_BOUNDARY_REFINEMENT_V1`: determine what positive protected
economic boundary can produce non-negative realized execution under the
validated common 1m cadence while preserving upside and avoiding premature
exits. It is not implemented, started, or authorized by this record. No
numeric buffer—including no `+0.025` assumption—parameter sweep, ratchet, or
strategy-, symbol-, interval-, or regime-specific threshold is frozen.
`STEPWISE_RATCHET` remains later research.

`BOUNDARY_REFINEMENT_IMPLEMENTED=NO`;
`BOUNDARY_REFINEMENT_STARTED=NO`. It is no longer the automatic current/next
priority; its existing collector remains passive only.

### Economic Floor V2 active economic evaluation closure

The cumulative matched LOCAL PAPER versus untreated VPS PAPER economic review
contained 14 non-ambiguous Floor-triggered pairs. Seven favored LOCAL and seven
favored VPS; avoided loss totaled `0.211732391635 USDC`, lost upside totaled
`0.418828994501 USDC`, and matched net effect was
`-0.207096602866 USDC`. This is descriptive matched evidence, not randomized
A/B proof.

`CURRENT_FLOOR_MATCHED_ECONOMIC_DIRECTION=NEGATIVE`;
`LOSS_PROTECTION_VALUE=MODERATE`;
`RECOVERABLE_WINNER_OPPORTUNITY_COST=HIGH`;
`CURRENT_FLOOR_V2_DIRECTIONALLY_USEFUL=MIXED`.

`ECONOMIC_FLOOR_V2_ACTIVE_EXIT_AUTHORITY=NOT_QUALIFIED`;
`FURTHER_ACTIVE_V2_TREATMENT=NOT_JUSTIFIED`;
`ECONOMIC_FLOOR_V2_EXIT_AUTHORITY=OFF`;
`ECONOMIC_FLOOR_V2_EXIT_AUTHORITY_DISABLED_AT_UTC=2026-09-05T09:48:30.152449138Z`.

LOCAL PAPER retains the common 60-second V2 owner cycle only as a read-only
causal observer. `BOUNDARY_EVIDENCE_COLLECTION=ACTIVE` and
`RECOVERY_EVIDENCE_COLLECTION=ACTIVE`, both with zero trading authority;
existing strategy exits and entry semantics are unchanged. No positive
boundary, recovery rule, ratchet, context-specific threshold, VPS promotion,
or LIVE authority is created. Any future Floor treatment requires a separate
recovery-aware evidence decision.

## 7. Current causal work

The bounded V1 experiment tested this broad frozen rule:

> If an RSI admission candidate has a same-symbol OPEN BBRANGE position with positive remaining inventory, block only that additional RSI PAPER admission.

Reason: `STOP_LOSING_OWNERSHIP_RSI_AFTER_BBRANGE`.

Sequence: `CONTROL → TREATMENT → CONTROL`. Primary outcome: `BAD_AVOIDED`
versus `GOOD_MISSED`. Fewer trades alone is not success. Counterfactual
observation must mature through 240-minute MFE, MAE, full-cost cover,
time-to-cost-cover, and economic viability. No second V1 treatment is
authorized.

Current verified state:

- `LOCAL_OWNERSHIP_EXPERIMENT=TERMINAL`
- `VPS_OWNERSHIP_V1_DECISION=TERMINAL`
- `OWNERSHIP_V1_FINAL_VERDICT=NARROWING_HYPOTHESIS_SUPPORTED`
- `TERMINAL_REASON=GLOBAL_DB_SAFETY_FAIL`
- `GLOBAL_DB_RISK_ACTUALLY_PRESENT=YES`
- `SAFETY_ABORT_CLASSIFICATION=CORRECT_REAL_GLOBAL_DB_RISK`
- `TRADING_IMPACT=NONE`
- `BLOCKED_SESSION_DIAGNOSTIC_CAPTURE=COMPLETE`
- `TESTS=40_PASS`
- `FAIL_CLOSED_BEHAVIOR_WEAKENED=NO`

The abort correctly detected a real blocked-session condition. The LOCAL
experiment harness now persists a coherent single-statement snapshot of every
blocked session, blocker, blocked-to-blocker relationship, query, transaction
age, application/client identity, and lock detail before failing closed. This
is an experiment-only diagnostic improvement; shared WalTrade runtime and
trading semantics were unchanged.

All four preserved LOCAL treatment counterfactuals have matured:
`LOCAL_MATURE_240M=4`, `LOCAL_PENDING_240M=0`, `LOCAL_BAD_AVOIDED=4`, and
`LOCAL_GOOD_MISSED=0`.
This is
`LOCAL_OWNERSHIP_STATUS=PROMISING_INITIAL_CAUSAL_EVIDENCE_NOT_PROVEN`, not proof of
a final ownership policy. `LOCAL_DISCOVERY=COMPLETE_FOR_CANDIDATE_FREEZE` and
the LOCAL `RSI_AFTER_BBRANGE_OWNERSHIP_V1` candidate evidence is `FROZEN`.
The VPS PAPER V3 frozen cohort contains `MATURE_240M=34`, `BAD_AVOIDED=23`,
and `GOOD_MISSED=11`. Raw observations are correlated and must not be treated
as independent trades. The cohort spans `16` distinct blocking portfolio
states, `9` distinct bad-avoided portfolio states, and `8` distinct
good-missed portfolio states. All 11 good-missed observations are
portfolio-redundant: `TRUE_INCREMENTAL_GOOD_MISSED_COUNT=0`,
`PORTFOLIO_REDUNDANT_GOOD_MISSED_COUNT=11`, and
`INSUFFICIENT_EVIDENCE_GOOD_MISSED_COUNT=0`.

Same-interval evidence is `13` bad avoided and `2` good missed, with
`SAME_INTERVAL_GOOD_RATE=13.33_PERCENT`. Cross-interval evidence is `10` bad
avoided and `9` good missed, with `CROSS_INTERVAL_GOOD_RATE=47.37_PERCENT` and
`CROSS_INTERVAL_VS_SAME_INTERVAL_RATE_RATIO=3.55`.
`SYMBOL_INTERVAL_CONCENTRATION=BTCUSDC_1m_ONLY`.

`CURRENT_RULE_ECONOMICALLY_BENEFICIAL=NOT_PROVEN`,
`GOOD_MISSED_RATE_ACCEPTABLE=NO`, and
`CROSS_INTERVAL_MATERIALLY_WORSE=YES`. Genuine rule over-blocking is not
proven, regime-dependent failure is not supported, and no true-incremental
good-missed outcome is currently proven.

Final V1 decision:

- `OWNERSHIP_V1_STATUS=TERMINAL`
- `OWNERSHIP_V1_FINAL_VERDICT=NARROWING_HYPOTHESIS_SUPPORTED`
- `OWNERSHIP_V1_GLOBAL_RULE_QUALIFIED=NO`
- `OWNERSHIP_MECHANISM_REJECTED=NO`
- `SAME_INTERVAL_OWNERSHIP_PROMISING=YES`
- `CROSS_INTERVAL_OWNERSHIP_CONCERN=YES`

Ownership as a mechanism is not rejected, but broad V1 is not qualified
unchanged. Further V1 observation is not required for the decision, and V1
must not be tuned in place. LOCAL discovery remains separate from VPS
acceptance; their evidence is not pooled as one formal statistical sample.

The next frozen ownership hypothesis is documentation-only:
`RSI_AFTER_BBRANGE_OWNERSHIP_V2_SAME_INTERVAL`.

> If the candidate strategy is RSI, a same-symbol OPEN BBRANGE position exists,
> the BBRANGE interval equals the RSI candidate interval, and remaining
> inventory is greater than zero, block only that additional RSI PAPER
> admission.

A BBRANGE position on another interval alone must not confer ownership. V2 is
not implemented or active, adds no regime, symbol, age, PnL, MFE, score, or
other condition, and performs no parameter tuning. Any future authorized V2
activation must use new natural evidence.

`OWNERSHIP_V1=COMPLETE`; `OWNERSHIP_V2_IMPLEMENTED=NO`;
`LIVE_ELIGIBILITY=NO`; no second Ownership V1 treatment is authorized.
Economic Floor V1 is also complete as a bounded research decision. Economic
Floor V2 cadence-independent protection is complete for the cadence question.
Its cadence fix is validated, its exact-zero boundary remains the primary
issue, and it is not ready for VPS promotion or LIVE.

`LONG_RUN_SAFETY_PREFLIGHT=PASS`: the terminal-condition catalog is complete,
recent logs pass replay against it, background errors are classified, task
attribution rules are tested, genuine dangerous conditions fail closed, and
restore plus manual read-only status paths pass.

### Movement-capacity candidate qualification

A chronological split was locked before holdout evaluation:

- DEVELOPMENT: 1,062 trades from 2026-08-14 09:32:52Z through 2026-08-24 18:36:10Z.
- HOLDOUT: 455 trades from 2026-08-24 18:36:29Z through 2026-08-28 14:25:37Z.

Three interpretable existing-field rule families were evaluated on DEVELOPMENT
only. The frozen research candidate blocks when both
`ATR_PCT <= 0.05275618624106265` and `REALTIME_SCORE <= 18.24315`; missing ATR
is allowed. DEVELOPMENT bad rejection was 17.687% with 92.110% good retention.
Unchanged on HOLDOUT, it blocked 68/455, avoided 49 insufficient-movement
cases, missed 19 cost-covering cases, retained 90.821% of good cases, rejected
19.758% of bad cases, and improved diagnostic allowed-cohort net from
-58.776807 to -48.945039 USDC while remaining materially negative.

Aggregate behavior reproduced, but subgroup behavior did not satisfy the
candidate gate: RSI good retention was 46.67%, and 5m bad rejection was only
1.41%. After counterfactually applying the unchanged frozen ownership rule
first, 29 movement blocks overlapped ownership and 39 were incremental;
incremental movement blocks avoided 28 bad cases, missed 11 good cases, and
had -6.132816 USDC realized net. Therefore
`HOLDOUT_GENERALIZATION=WEAK_GENERALIZATION`,
`NO_TRADE_INTERPRETATION=PARTIAL`, and
`CAUSAL_ELIGIBILITY=NEEDS_MORE_RESEARCH`. No Movement Capacity treatment is
authorized.

`GLOBAL_MOVEMENT_GATE_READY=NO`; `CURRENT_HOLDOUT_BURNED=YES`. The 455-trade
holdout is burned for further tuning of this candidate family. Any redesigned
or context-specific ATR/realtime hypothesis must use new natural data or
another genuinely untouched validation set. `NEXT_MOVEMENT_RESEARCH` is
`CONTEXT_SPECIFIC_ON_NEW_DATA_ONLY`.

The frozen Movement V1 forward holdout now runs as an untouched observational
contract and is deliberately not inspected here:
`MOVEMENT_V1_FORWARD_HOLDOUT=ACTIVE_UNINSPECTED`.

## 8. Completed read-only discovery conveyor #3–#7

`DISCOVERY_CONVEYOR_STATUS=COMPLETE`. The conveyor used VPS PAPER read-only
evidence while preserving VPS as the untreated natural reference during the
LOCAL Economic Floor V2 experiment. It created no treatment authority.

### #3 Movement Capacity

`EXPERIMENT_3=MOVEMENT_CAPACITY`;
`DISCOVERY_STATUS=COMPLETE`; `MECHANISM_SUPPORTED=PARTIAL`;
`PRE_ENTRY_PREDICTABILITY=MODERATE`; `FINAL_NET_PREDICTABILITY=WEAK`;
`GLOBAL_GATE_SUPPORTED=NO`;
`CONTEXT_SPECIFIC_GATE_SUPPORTED=YES_RESEARCH_ONLY`.

The frozen candidate remains exactly
`ATR_PCT <= 0.05275618624106265 AND REALTIME_SCORE <= 18.24315`, with missing
ATR allowed. In the 458-row VPS read-only cohort it blocked 78, avoided 62 bad
outcomes, missed 16 good outcomes, and produced diagnostic net delta
`+11.622090 USDC`. All inspected data is burned for this candidate. The frozen
candidate is ready for a future LOCAL test, but treatment is inactive and that
test must use new natural evidence after activation.

### #4 Economic No-Trade / New Risk vs Keep Existing Risk

`EXPERIMENT_4=ECONOMIC_NO_TRADE_NEW_RISK_VS_KEEP_EXISTING_RISK`;
`DISCOVERY_STATUS=COMPLETE`; `MECHANISM_SUPPORTED=NO`;
`MARGINAL_VALUE_SIGNAL_STRENGTH=WEAK`; `GLOBAL_GATE_SUPPORTED=NO`;
`CONTEXT_SPECIFIC_GATE_SUPPORTED=NO`; `FROZEN_CANDIDATE_READY=NO`;
`LOCAL_TREATMENT=NO`. Additional admissions were economically negative, but
existing portfolio risk was not isolated as the causal discriminator.

### #5 Same-Thesis Control — superseded by H11

The initial conveyor result was partial and authorized no gate. H11 supersedes
that current interpretation: redundant episode exposure is confirmed on
historical replicated partitions, while causal forward proof remains absent.
The frozen P4 first-signal candidate is observational only and has no treatment
authority. `INTERACTION_WITH_MOVEMENT_CAPACITY=INDEPENDENT_NON_BLOCKING_LABELS`.

### #6 Fee Velocity

`EXPERIMENT_6=FEE_VELOCITY`;
`DISCOVERY_STATUS=COMPLETE`; `FEE_DRAG_STRONGLY_CONFIRMED=YES`;
`FEE_VELOCITY_AS_CAUSE=NOT_SUPPORTED`. The cohort produced
`TOTAL_FINAL_NET=-66.680402_USDC`, `TOTAL_FEES=65.513810_USDC`, and
`TOTAL_GROSS_BEFORE_FEES=-1.166592_USDC`, with
`FEE_DOMINATED_LOSS_RATE=75.6381_PERCENT` and
`GROSS_POSITIVE_BUT_NET_NEGATIVE_COUNT=252`.
`GLOBAL_FEE_VELOCITY_GATE_SUPPORTED=NO`;
`CONTEXT_SPECIFIC_FEE_VELOCITY_GATE_SUPPORTED=NO`; `LOCAL_TREATMENT=NO`.
Fast reentry was not intrinsically worse than quiet entry; no time-based
reentry suppression is authorized.

### #7 1m/5m Mixed Duplication

`EXPERIMENT_7=1M_5M_MIXED_DUPLICATION`;
`DISCOVERY_STATUS=COMPLETE`; `MECHANISM_SUPPORTED=PARTIAL`;
`SIGNAL_STRENGTH=WEAK`; `GLOBAL_MIXED_DUPLICATION_GATE_SUPPORTED=NO`;
`CONTEXT_SPECIFIC_MIXED_DUPLICATION_GATE_SUPPORTED=NO`;
`DIRECTION_ASYMMETRY_SUPPORTED=NO`; `LOCAL_TREATMENT=NO`.
Cross-interval admission economics showed no incremental harm. Correlated
lifecycle evidence contained 296 pairs, 250 both-loss pairs, and 6 both-win
pairs; pair gross before fees was `+0.076939 USDC`, fees were
`82.597695 USDC`, and net was `-82.520756 USDC`. Redundant/correlated exposure
is descriptive evidence; interval mixing is not an authorized gate.

### Conveyor synthesis

`STRONGEST_REPEATED_ECONOMIC_SIGNAL=INSUFFICIENT_MOVEMENT_RELATIVE_TO_FIXED_COSTS_PLUS_REDUNDANT_CORRELATED_EXPOSURE`.
This does not authorize a combined policy. Unsupported simple hypotheses are:
any existing risk implies block new risk; fast reentry implies bad;
cross-interval overlap implies bad; and a same-thesis proxy implies a global
gate. Supported directions are Economic Floor protection, contextual Movement
Capacity filtering, and later authoritative thesis/marginal-capital-value
synthesis if evidence contracts improve.

## 9. H11 closure, P4 forward holdout, and H12 outcome surface

### H11 — episode redundancy and BTC market authority

`H11_STATUS=COMPLETE`;
`H11_PLAN_FINGERPRINT=c4286e14160d1d8467329d7a14636a035903b40ad87ea8d8c1480171e5e55826`.

The corrected cohort contains `971` positions in `534` 15-minute UTC market
episodes; `236` episodes contain multiple positions and the maximum episode
size is `8`. Position count correlates `-0.677527` with episode net and
`+0.688393` with episode loss. At least two symbols lost together in `197`
episodes and all four symbols lost together in `18`.

`H11_REDUNDANT_EXPOSURE=CONFIRMED_HISTORICALLY_REPLICATED_NOT_FORWARD_PROVEN`.
Marginal position value is negative at every observed ordinal and fees grow
with episode size. BTC causal leadership is not supported; BTC co-movement is
supported, but it yields no useful BTC-to-alt veto authority.

The qualified historical candidate is
`SAME_THESIS_EPISODE_REDUNDANCY_V1_FIRST_SIGNAL`, fingerprint
`951b7b6a41fb210fed6b4e37f6f63ff0a8d5e6083d1e8583a83890d7d80ee2fe`.
For LONG candidates, a later signal is observationally redundant only when an
earlier accepted candidate exists in the same 15-minute UTC decision episode
with identical symbol, side, and causal regime. Ordering is
`decision_created_at`, then `position_id`; missing regime admits and later
replacement is forbidden.

| Partition | Admitted | Rejected | Rejected A/B/C/D | Promising retained | Winner retained | Net delta | Fees avoided |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| TRAIN | 551 | 49 | 3/4/16/26 | 90.73% | 93.75% | +6.810305 USDC | 6.860148 USDC |
| VALIDATION | 180 | 8 | 0/1/3/4 | 96.26% | 100% | +0.800396 USDC | 1.121082 USDC |
| HISTORICAL_HOLDOUT | 167 | 16 | 0/1/4/11 | 93.42% | 100% | +2.376732 USDC | 2.239686 USDC |

Same-rate random outperformance exists with a weak validation margin;
historical context stability passes, but forward confirmation is required.
No P4 entry authority or LIVE eligibility exists.

### P4 untouched forward holdout

`H11_P4_CANDIDATE=FROZEN_NOT_AUTHORIZED`;
`H11_P4_FORWARD_HOLDOUT=ACTIVE_UNINSPECTED`;
`P4_FORWARD_HOLDOUT_START_UTC=2026-09-06T17:24:04.625559215Z`.

The immutable non-blocking contract is stored outside Git at
`~/waltrade-experiments/same-thesis-episode-redundancy-v1-forward-holdout/holdout-contract.json`.
Prior H11 evidence is burned. Existing immutable entry evidence can derive
P4 `WOULD_BLOCK` after the fact, so new telemetry and runtime authority are
unnecessary. P4 and Movement V1 are independent observational labels on the
same natural decisions: Movement uses each row's frozen ATR/Realtime fields;
P4 uses episode ordering and same symbol/side/regime. Neither changes the
sample or the other's inputs.

Only integrity/health/count checks are allowed before formal checkpoints.
Outcome efficacy remains untouched until 25 complete outcomes for integrity,
at least 100 complete outcomes plus adequate redundant/non-redundant exposure
for the first formal review, and 200 only if the first review leaves material
uncertainty. `P4_TREATMENT_ACTIVE=NO`; `P4_TRADES_BLOCKED=NO`.

### H12 — multi-horizon probabilistic outcome surface

`H12_PLAN_FINGERPRINT=a2b33fa89149dfed6fb49744688d53bf9ae026da37e34ecebf94e84d3afeb5f2`;
`H12_POPULATION=971`; `H12_EPISODES=534`.

The plan was frozen before results and used only decisions strictly before
`2026-09-06T10:35:00Z`. It preserved complete episodes across chronological
TRAIN/VALIDATION/HISTORICAL_HOLDOUT partitions and never inspected either
current forward holdout. Finalized canonical 1m closes, authoritative ENTRY
fills, COMPLETE inventory evidence, and frozen Fee V2 costs produced complete
15m/30m/60m/120m paths for `971/971` positions and complete 240m paths for
`969/971`; two incomplete 240m paths were not imputed. Non-comparable 1d/7d/30d
coverage was excluded.

The deterministic walk-forward hierarchy generated `4,853` fixed-horizon
position predictions and `275` support-qualified context rows. It used
episode-balanced empirical estimates, fixed hierarchical shrinkage, and 500
deterministic episode-block bootstrap repetitions. For cost-cover events, the
hierarchical surface was worse than the expanding global base rate in both
validation (Brier `0.171277` versus `0.167286`) and historical holdout (Brier
`0.136815` versus `0.134768`); log loss showed the same direction. Aggregate
binary prediction coverage against episode-bootstrap 95% intervals was
`67.78%`. Therefore `H12_PROBABILITY_CALIBRATION=NOT_SUPPORTED` and
`H12_CONTEXT_STABILITY=FAIL` for capital authority.

`H12_DECISION=B_SIGNAL_EXISTS_BUT_CALIBRATION_OR_SUPPORT_INSUFFICIENT`.
The research surface is reproducible and informative locally, but it is not
calibrated for H13 and its executable estimator is not promoted/reproducible on
VPS. Its untouched forward calibration-persistence contract is preserved
uninspected but is not current work.

`H12_CASH_BASELINE_AVAILABLE=YES`;
`H12_CONTINUE_CLOSE_REDEPLOY_IDENTIFIABLE=NO`;
`H12_NEW_TELEMETRY_REQUIRED=YES_FOR_CONTINUE_CLOSE_REDEPLOY_UTILITY_NOT_FOR_ENTRY_OUTCOME_SURFACE`;
`H12_FORWARD_CALIBRATION=PRESERVED_UNINSPECTED_BUT_NOT_CURRENT_WORK`.

#### H12 untouched forward calibration persistence

`H12_ARTIFACT_AUDIT=PASS`;
`PREDECLARED_FROZEN_ESTIMATOR_EXISTS=YES`;
`ESTIMATOR_VERSION=H12_HIERARCHICAL_EPISODE_BALANCED_SHRUNK_EMPIRICAL_V1`;
`ESTIMATOR_FINGERPRINT=a51b12b9f02eb7de0e7dbe4dfc6fa051ead3a60d7f40f3175411131a6ebfe2a2`.

The sole historical estimator—not a post-result selected alternative—is now
frozen for untouched persistence testing. It retains the exact predeclared
episode-balanced hierarchy, support thresholds, parent-equivalent shrinkage
weight of 20 episodes, fixed 15/30/60/120/240-minute horizons, finalized 1m
close outcome contract, and authoritative per-position Fee V2 costs.

`H12_FORWARD_CALIBRATION=PRESERVED_UNINSPECTED_BUT_NOT_CURRENT_WORK`;
`H12_FORWARD_START_UTC=2026-09-06T17:42:01.083408677Z`;
`H12_FORWARD_CONTRACT_SHA256=9ea37d62208ced970637efc74f9e31cde829a2e69dfbea12bdaa6eaddae566ea`.

The immutable contract is outside Git under
`~/waltrade-experiments/h12-forward-calibration-persistence-v1/`. Predictions
are deterministically reconstructable from immutable causal entry evidence
and outcomes completed before the evaluated entry episode; no runtime write or
new entry telemetry is required. Unsupported estimates remain `UNKNOWN`.

Primary calibration covers cost-cover, +0.5% net, +1% net, drawdown worse than
-1%, and expected terminal net at each frozen horizon. Brier score,
calibration-in-the-large, slope, predeclared-band ECE/reliability, coverage,
episode support, and paired Brier difference versus the causal unconditional
base rate are primary. Episode-block uncertainty bounds—not arbitrary PASS
numbers—govern the result; AUC remains diagnostic only.

The integrity checkpoint is 25 complete 240m outcomes with no efficacy read.
The first formal checkpoint requires at least 100 complete 240m outcomes and
inherited adequate episode support; 200 is used only if material uncertainty
remains. Counts never create an automatic PASS. Movement V1 and P4 remain
unchanged and uninspected. All three contracts derive independent labels from
the same natural PAPER stream and have no trading authority or interaction.

Cash/no-trade is authoritatively available as the zero-net, zero-fee,
zero-incremental-capital baseline. Fixed-horizon expected net, supported tail
risk, capital-hours, and Fee V2 costs are available; correlated heat is only
partially identifiable from causal actual exposure. `CONTINUE_HOLD` versus
`CLOSE_TO_CASH` versus `REDEPLOY` is not identifiable because the cohort lacks
action-time executable-close state, remaining-horizon passive paths tied to
that action, contemporaneous eligible redeployment alternatives, and their
causal capital/heat outcomes. New telemetry is required only for that later
action-utility question, not for the entry outcome surface.

Cost-cover is a diagnostic event, not sufficient economic utility. Class D
means no cost cover inside its fixed complete horizon, not permanently bad;
class C means bounded post-exit movement capacity, not realizable profit.
A/B/C/D remain secondary diagnostics. The future primary target is
risk-adjusted net capital utility after full costs, time, drawdown, correlation,
and opportunity cost, with NO TRADE/CASH as baseline. PAPER explores; LIVE
requires positive expected net with uncertainty control.

## 10. Permanent research and LIVE authority gates

Every material research cycle records before final evaluation:

- `HYPOTHESIS_ID`
- `ECONOMIC_MECHANISM`
- `PRIMARY_METRIC` and `SECONDARY_METRICS`
- `CANDIDATE_FAMILY_COUNT`
- `PARAMETER_OR_RULE_VARIANTS_TRIED`
- `DEVELOPMENT_WINDOW` and `HOLDOUT_WINDOW`
- `CANDIDATE_FREEZE_TIMESTAMP_OR_STATE`
- `STOP_RULE` and `GOOD_MISSED_RULE`
- `RESULT=ACCEPTED|NEEDS_MORE_RESEARCH|REJECTED`

This is a methodology and documentation invariant; it requires no new DB
table or application module. The number of material alternatives tried must
be reported because selection among more rules or thresholds increases the
risk of choosing a statistical fluke. The best historical result is not
unbiased evidence. Sufficiently broad future optimization should consider the
Deflated Sharpe Ratio, Probability of Backtest Overfitting, or an equivalent
statistically justified diagnostic; these are not mechanical requirements for
every small causal experiment.

Once a holdout has been inspected for a candidate, it is burned for further
tuning of that candidate family. Inspecting it, changing the rule, and
retesting on the same observations is not out-of-sample validation.

The ownership gate `MATURE_240M >= 5` means first formal review, not proof and
not automatic LIVE eligibility. Subsequent review is sequential and considers
effect magnitude, `BAD_AVOIDED`, `GOOD_MISSED`, economic value, avoided fees,
uncertainty, and stability across natural exposure. Five of five alone cannot
authorize LIVE, and no arbitrary N=20 or N=30 is imposed.

## 11. Ordered execution plan

### Now

1. `OWNERSHIP_V1=COMPLETE`; do not tune V1 in place.
2. Preserve Ownership V2 as a documentation-only frozen hypothesis; do not implement or activate it.
3. `ECONOMIC_FLOOR_V1_STATUS=COMPLETE`; do not tune its exact-zero boundary in place.
4. `ECONOMIC_FLOOR_V2_STATUS=COMPLETE`; active V2 exit authority is OFF.
5. Preserve `MOVEMENT_V1_FORWARD_HOLDOUT=ACTIVE_UNINSPECTED` without reading or
   changing its results.
6. Preserve `H11_P4_FORWARD_HOLDOUT=ACTIVE_UNINSPECTED_NON_BLOCKING`; no trade
   is blocked and no efficacy result is read before its checkpoints.
7. Record H12 verdict B; do not confer filter, Capital Allocation, Risk Budget,
   Learning, or LIVE authority. `H12=PRESERVED_UNINSPECTED_BUT_NOT_CURRENT_WORK`.
8. Continue boundary/recovery collection as passive evidence only.

### Next

1. `PRIOR_PRIMARY_RESEARCH=PROBABILISTIC_CAPITAL_DECISION_FOUNDATION`;
   `CURRENT_PRIMARY_RESEARCH=PAUSED_PENDING_SYSTEM_CONFORMANCE_REPAIR`.
2. Execute no H12 checkpoint work during conformance repair;
   `H12=PRESERVED_UNINSPECTED_BUT_NOT_CURRENT_WORK`.
3. Resume no economic roadmap item until the conformance repair exit gates and
   a separate Product Owner decision permit it.
4. Keep `LIVE_ELIGIBILITY=NO`; LIVE is last and requires explicit Product Owner
   approval.

The economic-floor family follows a proven secondary leak: 115
tiny-positive-to-final-loss cases, of which 112 ended through
`PROFIT_LOCK_TRAIL_DROP`. V1 validated the mechanism but not its exact-zero
boundary at discrete strategy cadence. V2 validated a common causal evaluation
cadence on LOCAL PAPER and is complete for that question. This does not
authorize a buffer, final exit
rule, fixed take-profit, immediate tiny-positive exit, ratchet, or `TIME_EXIT`
as an economic exit. Entry and exit treatments must not be mixed.

### Only after economic proof

- Risk Contribution policy
- volatility targeting
- Risk Budget numeric policy
- drawdown modes and dynamic risk reduction
- Capital Allocation
- edge-dependent or portfolio-impact sizing
- limited Learning authority
- bounded LIVE eligibility with Product Owner approval

## 12. Permanent long-run experiment standard

Every experiment longer than 30 minutes requires all of:

- `TERMINAL_CONDITION_CATALOG_COMPLETE=YES`
- `RECENT_LOG_REPLAY_AGAINST_TERMINAL_CATALOG=PASS`
- `KNOWN_BACKGROUND_ERRORS_CLASSIFIED=YES`
- `TASK_ATTRIBUTION_RULES_TESTED=YES`
- `GENUINE_DANGEROUS_CONDITIONS_FAIL_CLOSED=YES`
- `RESTORE_PATH=PASS`
- `MANUAL_READ_ONLY_STATUS_PATH=PASS`

Do not fix only the last observed error and restart blindly. Preflight the complete known terminal surface while preserving fail-closed protection for genuine risk.

For every material producer/consumer pipeline, independent acceptance also
requires all of:

- `FUNCTIONAL_CORRECTNESS=PASS`
- `EFFECTIVE_SERVICE_RATE > OBSERVED_SOURCE_ARRIVAL_RATE`
- after each completed consumer cycle, every eligible source event through the cycle-start cutoff has exactly one canonical observation
- the canonical watermark reaches the cycle-start cutoff with no eligible gaps

Healthy workers, fresh heartbeats, zero DB blockers, or individual successful
inserts alone do not prove forward health.

## 13. Hard scope control

Do not add a new engine, brain, strategy family, ML classifier, portfolio framework, parameter sweep, DCA, campaign, averaging down, hold-until-green policy, global 1m/5m suppression, or blind one-strategy-per-symbol rule. Do not enable Risk Budget influence, Capital Allocation, or Learning auto-apply. Do not run discovery experiments on LIVE. VPS PAPER may provide read-only discovery while remaining the untreated natural reference, but it may not run a simultaneous second treatment or confer treatment authority.

Fixed 20 USDC sizing remains appropriate during current causal alpha and
admission research because it isolates decision quality. Read-only
`RISK_NORMALIZATION_RESEARCH` may nevertheless measure volatility-normalized
risk, risk contribution, correlation concentration, and MAE/exposure
normalization without changing sizing. This is distinct from
`DYNAMIC_CAPITAL_ALLOCATION_AUTHORITY`, which remains unauthorized together
with dynamic sizing and Risk Budget influence until economic proof.

Before meaningful LIVE re-enablement, PAPER execution economics must be
periodically calibrated against authoritative LIVE execution evidence where
available: actual exchange fees, maker/taker behavior, spread, slippage,
partial fills, latency, and economically relevant execution paths. LIVE is not
used for strategy discovery; its evidence improves PAPER cost realism. Fee V2
remains canonical until a separately validated model change.

BTC, ETH, SOL, and BNB are multiple instruments but remain a correlated crypto
complex. Institutional-quality diversification will likely require broader
markets, asset classes, or independent return drivers. This is a long-term
constraint, not current scope, and must not delay proof of the OKX core.

## 14. Success ladders

Research quality ladder:

`UNKNOWN → MEASURED → BAD MECHANISM IDENTIFIED → CANDIDATE → LOCAL CAUSAL EVIDENCE → FROZEN → VPS INDEPENDENT VALIDATION → QUALIFIED COHORT`

Capital / LIVE ladder:

`NO LIVE AUTHORITY → QUALIFIED POSITIVE EXPECTED-NET COHORT → BOUNDED LIVE ELIGIBILITY → POSITIVE DAYS → POSITIVE WEEKS → POSITIVE MONTHS → BETTER DRAWDOWN → BETTER CAPITAL EFFICIENCY`

Total PAPER PnL may remain negative while its research universe remains broad.
Capital Allocation follows economic proof; it does not create it. The LIVE
equity curve remains the ultimate capital KPI.

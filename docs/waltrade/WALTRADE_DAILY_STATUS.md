# WalTrade Daily Status

LAST_UPDATED=2026-09-02

VPS_PAPER_RUNTIME_SHA=d60c4517892c220b6450876c47f27d99e8bf4dc8

OWNERSHIP_CANDIDATE_INTRODUCED_SHA=d60c4517892c220b6450876c47f27d99e8bf4dc8

CURRENT_PHASE=STOP_LOSING

CURRENT_P0=ECONOMIC_FLOOR_V2_CADENCE_INDEPENDENT_PROTECTION_SMALL_UNCHANGED_LOCAL_COHORT

This document is current truth only. Git history retains prior states.

## Current status

| Area | Current truth |
| --- | --- |
| North Star | FROZEN: Autonomous Capital Management Platform; CAPITAL; EQUITY CURVE HEALTH |
| Foundation | PASS |
| Exchange | OKX foundation and exchange-neutral contracts established |
| Financial Truth | Forward health PASS; no heuristic economics permitted |
| PAPER Entry Atomicity | `PASS_2_2` |
| Risk Budget Contract | `PASS_4_4`; immutable-event commit `f965a0b35f8be1b900cbd0e73332c653b003ca0a` |
| Risk Budget Influence | OFF |
| Learning | Auto-apply OFF |
| Strategy Ownership | V1 `TERMINAL`; narrowing hypothesis supported; broad rule not qualified unchanged |
| Movement vs Cost | Predictability `STRONG`; temporal-holdout candidate `NEEDS_MORE_RESEARCH` |
| Fee Velocity | High fee velocity / same-thesis duplication is a leading mechanism |
| New Risk vs Keep Risk | Strong diagnostic candidate; policy not yet proven |
| Same-Thesis Duplication | 572 admissions; net -69.028271 USDC |
| 1m/5m | MIXED; no global block authorized |
| Portfolio Crowding | WEAK effect |
| Winner Tail | LOW dependence; top 10% winners offset 1.875% of losses |
| Exit Giveback | Economic Floor V2 cadence independence validated; exact-zero boundary remains the primary issue |
| LOCAL PAPER | HEALTHY / ECONOMIC FLOOR V2 ACTIVE; first review complete; small unchanged cohort continues |
| Harness Safety | Correct real global DB risk abort; blocked-session diagnostic capture COMPLETE; 40 tests passed |
| VPS PAPER | Ownership V1 bounded research decision `TERMINAL`; runtime unchanged by docs task |
| Full Opportunity throughput | VPS functional correctness, forward canonical freshness, and service-rate headroom PASS |
| Read-only discovery conveyor | Experiments #3–#7 COMPLETE; no new treatment authority |
| LOCAL LIVE | Capital Preservation Mode `ACTIVE`; new entries `NO`; exit/close `YES` |
| Equity UI canonical authority | `VPS_LIVE_COMPLETE`; Git, contract, direct-schema, and runtime-semantic parity PASS |
| VPS LIVE | Capital Preservation Mode `ACTIVE`; new entries `NO`; exit/close `YES` |
| Economic Proof | Stable positive net expectancy after costs is NOT PROVEN |
| Capital Allocation | NOT NOW; requires economic and Risk Budget policy proof |
| Public Communication | `ACTIVE`; LinkedIn and X active; governance documented |

## Public communication

`PUBLIC_COMMUNICATION_LAYER=ACTIVE`

`PUBLIC_LINKEDIN=ACTIVE`

`PUBLIC_X=ACTIVE`

`PUBLIC_COMMUNICATION_GOVERNANCE=DOCUMENTED`

Public communication follows
[WALTRADE_PUBLIC_COMMUNICATION.md](WALTRADE_PUBLIC_COMMUNICATION.md). It
consumes canonical evidence and does not change project evidence, experiment
gates, economic priorities, or LIVE authority.

## Authority and research methodology

- LOCAL and VPS share promoted contracts but have independent runtime and data histories.
- `LOCAL_GIT_PROMOTION_AUTHORITY=ONLY`; implementation, tests, commits, pushes, and canonical doc updates originate on LOCAL.
- `VPS_GIT_AUTHORITY=PULL_ONLY`; VPS may fetch, pull `--ff-only`, deploy, and validate, but never commit, push, patch shared code/schema locally, or change frozen semantics.
- Promotion path: `LOCAL → GitHub → VPS pull --ff-only → independent validation`.
- Corresponding `LOCAL PAPER ↔ VPS PAPER` and, where applicable, `LOCAL LIVE ↔ VPS LIVE` environments require `GIT_PARITY`, `CONTRACT_PARITY`, `DIRECT_SCHEMA_DEPENDENCY_PARITY`, `SCHEMA_OBJECT_PARITY`, `RUNTIME_SEMANTIC_PARITY`, and applicable shared `CONFIG_CONTRACT_PARITY`.
- `SCHEMA_OBJECT_PARITY` covers every schema object and migration state required by the active promoted runtime contract, not merely table-name existence. Environment-specific objects must be explicit in the promoted contract; LOCAL-only and VPS-only schema magic are forbidden.
- `BUSINESS_DATA_PARITY=NOT_REQUIRED`; `BUSINESS_DATA_DIVERGENCE=EXPECTED`. Natural differences in runtime-generated rows, histories, evidence, timestamps, balances, and market outcomes are not parity failures and must not be copied to manufacture equality.
- Canonical parity principle: `SAME_SHARED_CONTRACT; DIFFERENT_NATURAL_DATA`. A common Git SHA alone is insufficient, and business-data equality is never a rollout gate.
- `RESEARCH_UNIVERSE` is broad PAPER evidence and may remain aggregate-negative.
- `PAPER_RESEARCH_UNIVERSE_MAY_BE_NET_NEGATIVE=YES`.
- `LOCAL_PAPER_CAUSAL_TREATMENTS=ONE_ACTIVE_AT_A_TIME`.
- VPS PAPER remains the untreated natural reference for the current LOCAL
  experiment and may also support read-only discovery for future experiments.
  No simultaneous second VPS treatment is authorized while that reference is
  required.
- Evidence used to select or evaluate a candidate is burned for validation of
  that candidate; future causal validation requires new natural evidence.
- `QUALIFIED_UNIVERSE` contains frozen policies that survived independent validation.
- `LIVE_UNIVERSE` contains only explicitly approved qualified decisions; it never inherits all PAPER decisions automatically.
- Less-negative is research progress and near-zero is promising, but potential LIVE eligibility requires independently validated positive expected net after all costs, acceptable uncertainty, good-missed and risk/drawdown, execution/Financial Truth/auditability PASS, and explicit Product Owner approval.
- `EXPECTED_NET_AFTER_ALL_COSTS > 0` is a mandatory potential-LIVE gate.
- Historical winners cannot be selected after outcome to define a LIVE cohort; selection semantics must exist before outcome.
- Every material research trial records its hypothesis, mechanism, metrics, candidate-family and variant counts, development/holdout windows, freeze state, stop and good-missed rules, and final disposition.
- An inspected holdout is burned for further tuning of that candidate family. A redesign requires new natural data or another untouched validation set.
- Multiple alternatives increase overfitting risk; the best historical result is not unbiased evidence.

## Ownership V1 final decision

`OWNERSHIP_V1_STATUS=TERMINAL`;
`OWNERSHIP_V1_FINAL_VERDICT=NARROWING_HYPOTHESIS_SUPPORTED`;
`OWNERSHIP_V1_GLOBAL_RULE_QUALIFIED=NO`;
`OWNERSHIP_MECHANISM_REJECTED=NO`.

| V3 frozen cohort field | Value |
| --- | ---: |
| Mature 240m | 34 |
| Bad avoided | 23 |
| Good missed | 11 |
| True incremental good missed | 0 |
| Portfolio-redundant good missed | 11 |
| Insufficient-evidence good missed | 0 |
| Distinct blocking portfolio states | 16 |
| Distinct bad-avoided portfolio states | 9 |
| Distinct good-missed portfolio states | 8 |

- `SAME_INTERVAL_BAD_AVOIDED=13`; `SAME_INTERVAL_GOOD_MISSED=2`;
  `SAME_INTERVAL_GOOD_RATE=13.33%`.
- `CROSS_INTERVAL_BAD_AVOIDED=10`; `CROSS_INTERVAL_GOOD_MISSED=9`;
  `CROSS_INTERVAL_GOOD_RATE=47.37%`.
- `CROSS_INTERVAL_VS_SAME_INTERVAL_RATE_RATIO=3.55`.
- `CURRENT_RULE_ECONOMICALLY_BENEFICIAL=NOT_PROVEN`;
  `GOOD_MISSED_RATE_ACCEPTABLE=NO`.
- `GENUINE_RULE_OVER_BLOCKING=NOT_PROVEN`;
  `CROSS_INTERVAL_MATERIALLY_WORSE=YES`.
- `REGIME_DEPENDENT_FAILURE_SUPPORTED=NO`;
  `SYMBOL_INTERVAL_CONCENTRATION=BTCUSDC_1m_ONLY`.
- Raw observations remain correlated and are not independent trades; distinct
  blocking portfolio states are the more appropriate exposure context.

Ownership as a mechanism is not rejected, but broad V1 is not qualified
unchanged. Same-interval ownership remains promising; cross-interval ownership
is materially weaker and supports a narrowing hypothesis. All 11 current
good-missed observations are portfolio-redundant and
`TRUE_INCREMENTAL_GOOD_MISSED_COUNT=0`. Further V1 observation is not required
for the V1 decision, and V1 must not be tuned in place. LOCAL discovery remains
separate from VPS acceptance; the samples are not pooled.

`SAME_INTERVAL_OWNERSHIP_PROMISING=YES`;
`CROSS_INTERVAL_OWNERSHIP_CONCERN=YES`;
`LIVE_ELIGIBILITY=NO`.

### Frozen V2 research hypothesis

`RSI_AFTER_BBRANGE_OWNERSHIP_V2_SAME_INTERVAL` is documentation-only and not
implemented or active:

> If the candidate strategy is RSI, a same-symbol OPEN BBRANGE position exists,
> the BBRANGE interval equals the RSI candidate interval, and remaining
> inventory is greater than zero, block only that additional RSI PAPER
> admission.

A BBRANGE position on another interval alone must not confer ownership. V2
adds no regime, symbol, age, PnL, MFE, score, or other condition and performs no
parameter tuning. If activated in a future authorized task, it must use new
natural evidence.

`OWNERSHIP_V1=COMPLETE`;
`OWNERSHIP_V2_IMPLEMENTED=NO`;
`ECONOMIC_FLOOR_V1_STATUS=COMPLETE`;
`ECONOMIC_FLOOR_V1_MECHANISM_VALIDATED=YES`;
`ECONOMIC_FLOOR_V1_ACTIVE_BOUNDARY_VALIDATED=NO`;
`ECONOMIC_FLOOR_V1_VPS_PROMOTION_READY=NO`;
`ECONOMIC_FLOOR_V1_LIVE_ELIGIBILITY=NO`;
`NEXT_ECONOMIC_RESEARCH_PRIORITY=ECONOMIC_FLOOR_V2_CADENCE_INDEPENDENT_PROTECTION`.

## Economic Floor V1 first active review and final decision

`ECONOMIC_FLOOR_V1_FINAL_VERDICT=MECHANISM_PROMISING_BOUNDARY_TOO_LATE_AT_DISCRETE_EVALUATION_CADENCE`.

| Active LOCAL PAPER evidence | Value |
| --- | ---: |
| Armed positions | 12 |
| Economic Floor exits | 7 |
| Armed positions where existing exit won | 5 |
| Floor-exit net sum | -0.0827958600555 USDC |
| Floor-exit average net | -0.0118279800079 USDC |
| Floor-exit minimum net | -0.02196618160 USDC |
| Floor-exit maximum net | -0.005944715805 USDC |

- `UPSIDE_INTERFERENCE_PROVEN=NO`; `PREMATURE_EXIT_HARM_PROVEN=NO`.
- `FLOOR_MECHANISM_WORKING_AS_DESIGNED=YES`.
- `FLOOR_LIMITS_POST_COVER_GIVEBACK=YES_MECHANICALLY_COUNTERFACTUAL_NET_BENEFIT_NOT_PROVEN`.
- `EXACT_ZERO_BOUNDARY_TOO_LOW_FOR_EXECUTION=YES_AT_OBSERVED_60_TO_300_SECOND_CADENCE`.
- `ZERO_BOUNDARY_EXECUTION_VERDICT=A_EXACT_ZERO_BOUNDARY_PLUS_DISCRETE_EVALUATION_CADENCE`.
- `FEE_OR_ROUNDING_ERROR_SUPPORTED=NO`; `STALE_MARKET_DATA_SUPPORTED=NO`;
  `IMPLEMENTATION_DEFECT_SUPPORTED=NO`.
- `CAUSAL_MARKET_DATA_COMPLETE=7/7`; `CAUSAL_ORDERING_AVAILABLE=7/7`;
  `FINAL_NET_MATCHED_REALIZABLE_NET_AT_EXIT=7/7`.

The first active review validates the cost-cover/realizable-net mechanism, not
the exact-zero active boundary. V1 is complete as a bounded research decision,
is not classified as harmful, is not qualified for VPS promotion, and creates
no LIVE eligibility. This documentation task does not change the currently
deployed LOCAL PAPER treatment or any runtime state.

Boundary evidence is descriptive only:
`MIN_OBSERVED_POSITIVE_BUFFER_NEEDED_TO_HAVE_EXITED_NON_NEGATIVE=NOT_IDENTIFIABLE_FOR_ALL_7_UNDER_NO_SAME_EVALUATION_EXIT`;
`POSITIONS_WITH_LATER_POSITIVE_EVALUATION=5/7`;
`UNIFORM_OBSERVED_BUFFER_FOR_THOSE_5=0.02508313990_USDC`.
The observed value is not a frozen threshold and must not become one without a
new experiment.

## Economic Floor V2 first active review

`ECONOMIC_FLOOR_V2_CADENCE_INDEPENDENT_PROTECTION` asks whether the same
authoritative cost-cover and realizable-net protection can be evaluated at a
common causal cadence independent of the originating strategy interval, using
existing market/evidence infrastructure and avoiding a new engine where
possible.

`ECONOMIC_FLOOR_V2_FIRST_REVIEW=CADENCE_FIX_VALIDATED_BOUNDARY_REMAINS_PRIMARY_ISSUE`.

`ECONOMIC_FLOOR_V2_STATUS=ACTIVE_LOCAL_PAPER`;
`CADENCE_FIX_VALIDATED=YES`; `BOUNDARY_REMAINS_PRIMARY_ISSUE=YES`;
`VPS_PROMOTION_READY=NO`.

| Active LOCAL PAPER V2 evidence | Value |
| --- | ---: |
| Arm events | 6 |
| Exit intents | 3 |
| Completed floor exits | 3 |
| Duplicate arm events | 0 |
| Duplicate close events | 0 |
| Armed by originating interval | `1m:5, 5m:1` |
| Floor exits by originating interval | `1m:2, 5m:1` |
| Floor-exit net sum | -0.064340584790 USDC |
| Floor-exit average net | -0.0214468615966667 USDC |
| Floor-exit minimum net | -0.04087355760 USDC |
| Floor-exit maximum net | -0.006972296870 USDC |
| 1m-origin average cross-zero gap | 0.038252924520 USDC |
| 5m-origin average cross-zero gap | 0.008452631880 USDC |

- `CADENCE_DEFECT_FIXED_ARCHITECTURALLY=YES`;
  `FIVE_MINUTE_POSITION_NOW_EVALUATED_ON_1M_CADENCE=YES`;
  `V2_5M_PROTECTION_LATENCY_REDUCED=YES`.
- `V2_MECHANISM_WORKING_AS_DESIGNED=YES`;
  `V2_CADENCE_INDEPENDENCE_VALIDATED=YES`; `V2_UPSIDE_PRESERVED=YES`.
- `REMAINING_NEGATIVE_EXIT_PRIMARY_CAUSE=EXACT_ZERO_BOUNDARY`;
  `V2_BOUNDARY_VALIDATED=NO`.
- `UPSIDE_INTERFERENCE_PROVEN=NO`; `PREMATURE_EXIT_HARM_PROVEN=NO`.
- `V2_READY_FOR_VPS_PROMOTION=NO`;
  `ADDITIONAL_LOCAL_V2_EVIDENCE_REQUIRED=YES_SMALL_UNCHANGED_ECONOMIC_COHORT`.

The frozen V1 5m cadence was 300 seconds; V2 evaluates all originating
intervals at a common 60-second cadence. V1's three 5m floor exits averaged
`-0.0151844783126667 USDC`, with cross-zero gaps `0.03144316240`,
`0.02655752220`, and `0.0266234466540 USDC`. The first natural V2 5m floor
exit finished at `-0.006972296870 USDC` with a `0.008452631880 USDC`
cross-zero gap. This is descriptive, mechanistic evidence from one V2 5m
example, not authority to claim statistical improvement.

V2 remains active and unchanged on LOCAL PAPER for a small economic cohort; it
is not closed or qualified for VPS promotion. The remaining research question
is the economic boundary. Any boundary refinement must be a separate
experiment after V2 closure. No numeric buffer is frozen, no ratchet is added,
and no strategy-, symbol-, or regime-specific floor is authorized.
`STEPWISE_RATCHET` remains later research.

## Equity UI canonical read authority

- `EQUITY_UI_CANONICAL_AUTHORITY=VPS_LIVE_COMPLETE`
- `EQUITY_UI_GIT_PARITY=PASS`
- `EQUITY_UI_CONTRACT_PARITY=PASS`
- `EQUITY_UI_DIRECT_SCHEMA_DEPENDENCY_PARITY=PASS`
- `EQUITY_UI_RUNTIME_SEMANTIC_PARITY=PASS`
- LIVE reads the accepted baseline and canonical managed-equity history without legacy fallback or fabricated period history.
- The legacy LIVE daily writer is disabled; PAPER behavior is unchanged.

## Ownership terminal result and diagnostic closure

- `GLOBAL_DB_RISK_ACTUALLY_PRESENT=YES`; the safety decision was correct and failed closed.
- `SAFETY_ABORT_CLASSIFICATION=CORRECT_REAL_GLOBAL_DB_RISK`.
- Trading impact: NONE.
- `BLOCKED_SESSION_DIAGNOSTIC_CAPTURE=COMPLETE`; coherent blocked-session, blocker, relationship, query, transaction-age, application/client, and lock evidence is persisted before failure.
- `TESTS=40_PASS`; `FAIL_CLOSED_BEHAVIOR_WEAKENED=NO`.
- `LOCAL_MATURE_240M=4`; `LOCAL_PENDING_240M=0`; `LOCAL_BAD_AVOIDED=4`; `LOCAL_GOOD_MISSED=0`.
- `LOCAL_OWNERSHIP_ECONOMIC_VERDICT=PROMISING_INITIAL_CAUSAL_EVIDENCE`; LOCAL discovery alone is not proof.
- `LOCAL_OWNERSHIP_STATUS=PROMISING_INITIAL_CAUSAL_EVIDENCE_NOT_PROVEN`.
- `LOCAL_DISCOVERY=COMPLETE_FOR_CANDIDATE_FREEZE`.
- `LOCAL_OWNERSHIP_CANDIDATE=FROZEN` (`RSI_AFTER_BBRANGE_OWNERSHIP_V1`).
- `LIVE_ELIGIBILITY=NO`.

## Capital preservation

- `LOCAL_LIVE_CAPITAL_PRESERVATION_MODE=ACTIVE`.
- `VPS_LIVE_CAPITAL_PRESERVATION_MODE=ACTIVE`.
- `NEW_LIVE_ENTRY_ALLOWED=NO`.
- `EXIT_CLOSE_CAPABILITY_AVAILABLE=YES`.
- `CURRENT_EXPECTED_VPS_LIVE_SLOT_COUNT=28`.
- `WHY_28_VS_32=EARLIER_STATUS_USED_FULL_PAPER_UNIVERSE_AS_LIVE_DENOMINATOR`.
- `CONFIG_CONTRACT_CHANGE=NO`; `RUNTIME_CONTRACT_CHANGE=NO`;
  `SAFETY_IMPACT=NO`.

## Full PAPER Opportunity Observation throughput

- `VPS_PAPER_DIRECT_SCHEMA_DEPENDENCY_REPAIR=COMPLETE`.
- `ROOT_CAUSE=EXPENSIVE_PROJECTION_LOOKUPS_PLUS_FIFO_SINGLE_CONSUMER`.
- `LOCAL_THROUGHPUT_FIX=PASS`.
- The exact projection lookup indexes preserve FIFO, observation identity, Fee V2 evidence, and 240-minute outcome linkage.
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
- No queue change is authorized.

Permanent pipeline acceptance invariant: functional correctness must pass,
effective service rate must exceed observed source arrival rate, each completed
consumer cycle must produce exactly one canonical observation for every
eligible event through its cycle-start cutoff, and the canonical watermark
must reach that cutoff with no eligible gaps. Worker health, heartbeat
freshness, zero blockers, and isolated successful inserts are insufficient by
themselves.

## Current economic baseline

- Fee V2: 0.35% per side.
- Full roundtrip break-even movement: approximately 0.7024586051%.
- Latest VPS PAPER forensic: 1,515 trades, net -183.346579 USDC, fees 212.195019 USDC, insufficient-movement rate 78.2178%.
- Additional admissions: 965; non-value-add admissions: 868 (89.948%); fees 121.513802 USDC; net -122.770951 USDC.

## Movement-capacity qualification

- Canonical LOCAL cohort: 1,517 mature trades; full-cost hurdle 0.7024586051%.
- `240M_MFE_FULL_COST_COVER=828/1517` (54.581%); insufficient 689/1,517 (45.419%).
- `FINAL_NET_POSITIVE=172/1517`; final net -177.145833 USDC.
- 240-minute MFE cost-cover viability is not final realized profitability and does not replace the differently defined 78.2178% VPS forensic.
- `PRIOR_LOCAL_MOVEMENT_CAPACITY_PREDICTABILITY=STRONG`.
- `PRE_ENTRY_NON_VALUE_ADD_SEPARABILITY=WEAK`.
- `ECONOMIC_NO_TRADE_REGION_SUPPORTED=PARTIAL`.
- `MARGINAL_PORTFOLIO_VALUE=-103.994083_USDC_FOR_923_ADDITIONAL_ADMISSIONS`.
- Existing evidence is sufficient; `NO_NEW_SCORE_REQUIRED=YES`; `NEW_ENGINE_REQUIRED=NO`.
- Temporal split: DEVELOPMENT 1,062; HOLDOUT 455.
- Frozen research rule: block only when `ATR_PCT <= 0.05275618624106265` and `REALTIME_SCORE <= 18.24315`; missing ATR is allowed.
- HOLDOUT: 68 blocked; 49 bad avoided; 19 good missed; good retention 90.821%; bad rejection 19.758%.
- HOLDOUT final net baseline -58.776807 USDC; allowed cohort -48.945039 USDC; fees avoided 9.518102 USDC.
- After ownership: 29 overlapping blocks; 39 incremental blocks; 28 incremental bad avoided; 11 incremental good missed; diagnostic incremental value +6.132816 USDC.
- `HOLDOUT_GENERALIZATION=WEAK_GENERALIZATION`: RSI good retention 46.67%; 5m bad rejection 1.41%.
- `NO_TRADE_INTERPRETATION=PARTIAL`; `CAUSAL_ELIGIBILITY=NEEDS_MORE_RESEARCH`.
- `GLOBAL_MOVEMENT_GATE_READY=NO`; `CURRENT_HOLDOUT_BURNED=YES`.
- `NEXT_MOVEMENT_RESEARCH=CONTEXT_SPECIFIC_ON_NEW_DATA_ONLY`.
- No Movement Capacity treatment is active or authorized. Economic Floor V2
  remains the sole active LOCAL PAPER treatment.
- After Economic Floor V2 closes, ordered work is: separate boundary
  refinement if the exact-zero boundary remains limiting; Movement Capacity
  LOCAL treatment on new natural evidence; then later synthesis only after
  clean individual experiment results.

## Read-only discovery conveyor #3–#7

`DISCOVERY_CONVEYOR_STATUS=COMPLETE`. These VPS PAPER results are read-only
discovery, not treatment authority. Economic Floor V2 remains the sole active
LOCAL PAPER causal treatment.

### Experiment #3 — Movement Capacity

- `EXPERIMENT_3=MOVEMENT_CAPACITY`.
- `DISCOVERY_STATUS=COMPLETE`; `MECHANISM_SUPPORTED=PARTIAL`.
- `PRE_ENTRY_PREDICTABILITY=MODERATE`; `FINAL_NET_PREDICTABILITY=WEAK`.
- `GLOBAL_GATE_SUPPORTED=NO`;
  `CONTEXT_SPECIFIC_GATE_SUPPORTED=YES_RESEARCH_ONLY`.
- The existing frozen candidate is unchanged: block when
  `ATR_PCT <= 0.05275618624106265` and
  `REALTIME_SCORE <= 18.24315`; missing ATR remains allowed.
- VPS read-only cohort: `COHORT_N=458`, `BLOCKED=78`, `BAD_AVOIDED=62`,
  `GOOD_MISSED=16`, `NET_DELTA_DIAGNOSTIC=+11.622090_USDC`.
- All inspected evidence is burned for this candidate.
  `FROZEN_CANDIDATE_READY_FOR_FUTURE_LOCAL_TEST=YES`, but any future LOCAL
  treatment requires new natural evidence after activation.
- `TREATMENT_ACTIVE=NO`.

### Experiment #4 — Economic No-Trade / New Risk vs Keep Existing Risk

- `EXPERIMENT_4=ECONOMIC_NO_TRADE_NEW_RISK_VS_KEEP_EXISTING_RISK`.
- `DISCOVERY_STATUS=COMPLETE`; `MECHANISM_SUPPORTED=NO`;
  `MARGINAL_VALUE_SIGNAL_STRENGTH=WEAK`.
- `GLOBAL_GATE_SUPPORTED=NO`; `CONTEXT_SPECIFIC_GATE_SUPPORTED=NO`;
  `FROZEN_CANDIDATE_READY=NO`; `LOCAL_TREATMENT=NO`.
- Additional admissions were economically negative, but existing portfolio
  risk was not isolated as the causal discriminator.

### Experiment #5 — Same-Thesis Control

- `EXPERIMENT_5=SAME_THESIS_CONTROL`.
- `DISCOVERY_STATUS=COMPLETE`; `MECHANISM_SUPPORTED=PARTIAL`;
  `THESIS_PROXY_SIGNAL_STRENGTH=WEAK`.
- `GLOBAL_GATE_SUPPORTED=NO`; `CONTEXT_SPECIFIC_GATE_SUPPORTED=NO`;
  `LOCAL_TREATMENT=NO`.
- Redundant fee drag and correlated losses exist, but authoritative
  position-to-thesis identity does not. Current proxies cannot authorize a
  gate. `INTERACTION_WITH_MOVEMENT_CAPACITY=MODERATE`.

### Experiment #6 — Fee Velocity

- `EXPERIMENT_6=FEE_VELOCITY`.
- `DISCOVERY_STATUS=COMPLETE`; `FEE_DRAG_STRONGLY_CONFIRMED=YES`;
  `FEE_VELOCITY_AS_CAUSE=NOT_SUPPORTED`.
- `TOTAL_FINAL_NET=-66.680402_USDC`; `TOTAL_FEES=65.513810_USDC`;
  `TOTAL_GROSS_BEFORE_FEES=-1.166592_USDC`.
- `FEE_DOMINATED_LOSS_RATE=75.6381_PERCENT`;
  `GROSS_POSITIVE_BUT_NET_NEGATIVE_COUNT=252`.
- `GLOBAL_FEE_VELOCITY_GATE_SUPPORTED=NO`;
  `CONTEXT_SPECIFIC_FEE_VELOCITY_GATE_SUPPORTED=NO`;
  `LOCAL_TREATMENT=NO`.
- Fast reentry was not intrinsically worse than quiet entry. This evidence
  does not authorize time-based reentry suppression.

### Experiment #7 — 1m/5m Mixed Duplication

- `EXPERIMENT_7=1M_5M_MIXED_DUPLICATION`.
- `DISCOVERY_STATUS=COMPLETE`; `MECHANISM_SUPPORTED=PARTIAL`;
  `SIGNAL_STRENGTH=WEAK`.
- `GLOBAL_MIXED_DUPLICATION_GATE_SUPPORTED=NO`;
  `CONTEXT_SPECIFIC_MIXED_DUPLICATION_GATE_SUPPORTED=NO`;
  `DIRECTION_ASYMMETRY_SUPPORTED=NO`.
- Cross-interval admission economics did not show incremental harm.
- Correlated lifecycle evidence: `PAIR_COUNT=296`, `BOTH_LOSE_COUNT=250`,
  `BOTH_WIN_COUNT=6`, `PAIR_GROSS_BEFORE_FEES=+0.076939_USDC`,
  `PAIR_FEES=82.597695_USDC`, `PAIR_NET=-82.520756_USDC`.
- Redundant/correlated exposure exists descriptively, but interval mixing is
  not an authorized gate. `LOCAL_TREATMENT=NO`.

### Cross-experiment synthesis

`STRONGEST_REPEATED_ECONOMIC_SIGNAL=INSUFFICIENT_MOVEMENT_RELATIVE_TO_FIXED_COSTS_PLUS_REDUNDANT_CORRELATED_EXPOSURE`.
This is not an authorized combined policy.

Unsupported simple gates are: any existing risk implies block new risk; fast
reentry implies bad; cross-interval overlap implies bad; and a same-thesis
proxy implies a global gate. Supported research directions remain Economic
Floor protection, Movement Capacity contextual filtering, and later
authoritative thesis/marginal-capital-value synthesis if evidence contracts
improve. No New Risk, Same Thesis, Fee Velocity, or Mixed Duplication treatment
is scheduled from current evidence.

## Regime-dependent strategy economics

- `STRATEGY_VALUE_IS_CONTEXT_DEPENDENT`.
- Evaluate strategy value conditional on `REGIME`, `STRATEGY`, `SYMBOL`,
  `INTERVAL`, `PORTFOLIO_STATE`, and `COST_ENVIRONMENT`.
- Current BBRANGE dominance is not evidence for a global BBRANGE block;
  historical TREND or SUPERTREND underperformance is not evidence for a global
  block of either strategy.
- Future authoritative research evaluates
  `STRATEGY × REGIME × SYMBOL × INTERVAL` using `N`, `ADMISSION_SHARE`,
  `NET_AFTER_COSTS`, `FEES`, `WIN_RATE`, `MFE`, `MAE`,
  `FULL_COST_COVER_RATE`, `CAPITAL_HOURS`, `ADDITIONAL_RISK_RATE`, and
  `MARGINAL_PORTFOLIO_VALUE`. Where sample size permits, it also evaluates
  same-symbol existing exposure, 1m/5m overlap, same-thesis exposure, and exit
  reason distribution.
- Reuse existing ORC and regime evidence first: `REUSE_BEFORE_BUILD`. No new
  regime engine or global strategy block is authorized.
- Movement Capacity remains `NEEDS_MORE_RESEARCH`. Economic Floor V1 is a
  completed bounded research decision; V2 cadence-independent protection is
  active on LOCAL PAPER and continues unchanged for a small economic cohort.

## NOW

- `OWNERSHIP_V1=COMPLETE`; do not tune V1 in place.
- Preserve Ownership V2 as a documentation-only frozen hypothesis; do not
  implement or activate it.
- `ECONOMIC_FLOOR_V1_STATUS=COMPLETE`; do not tune its boundary in place.
- Continue `ECONOMIC_FLOOR_V2_CADENCE_INDEPENDENT_PROTECTION` unchanged on
  LOCAL PAPER for a small economic cohort; do not close or tune V2.
- `ACTIVE_LOCAL_CAUSAL_TREATMENT_COUNT=1`; discovery #3–#7 changed no active
  treatment authority.
- Monitor forward freshness and the moderate pipeline risk; no pipeline change is required.

## NEXT

- `CURRENT_STAGE=ECONOMIC_FLOOR_V2_CADENCE_INDEPENDENT_PROTECTION_ACTIVE_LOCAL_PAPER`.
- Gather the small unchanged V2 economic cohort. Cadence independence is
  validated; the remaining boundary question is deferred to a separate future
  experiment after V2 closure.
- If V2 closure confirms that exact zero remains limiting, perform Boundary
  Refinement as a separate Economic Floor experiment. Then test the frozen
  Movement Capacity candidate locally using new natural evidence.
- Re-evaluate any thesis/marginal-capital-value synthesis only after clean
  results from the individual experiments. Do not schedule New Risk, Same
  Thesis, Fee Velocity, or Mixed Duplication treatments from current evidence.
- Any future Ownership V2 activation or Economic Floor boundary experiment
  requires separate authorization and new natural evidence.
- Keep `LIVE_ELIGIBILITY=NO`.
- Movement capacity remains `NEEDS_MORE_RESEARCH`; `SECOND_CAUSAL_TREATMENT_AUTHORIZED=NO`.
- Economic Floor V1 is not qualified for VPS or LIVE; active LOCAL PAPER V2 is
  not yet qualified for VPS promotion or LIVE.

## DO_NOT_DO

- No new engine.
- No new brain.
- No new strategy.
- No parameter sweep.
- No global 1m/5m block.
- No blind one-strategy-per-symbol rule.
- No Risk Budget influence.
- No Capital Allocation yet.
- No Learning auto-apply.
- No LIVE discovery experiment.

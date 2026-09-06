# WalTrade Daily Status

LAST_UPDATED=2026-09-06

VPS_PAPER_RUNTIME_REVISION_STATUS=NON_UNIFORM_NOT_ACCEPTED

SUPERSEDED_VPS_PAPER_RUNTIME_SHA=d60c4517892c220b6450876c47f27d99e8bf4dc8

OWNERSHIP_CANDIDATE_INTRODUCED_SHA=d60c4517892c220b6450876c47f27d99e8bf4dc8

CURRENT_PHASE=SYSTEM_CONFORMANCE_REPAIR_PLANNING_COMPLETE

CURRENT_PRIMARY_RESEARCH=PAUSED_PENDING_SYSTEM_CONFORMANCE_REPAIR

CURRENT_EXECUTION_SCOPE=MINIMUM_WALTRADE_CONFORMANCE_RECOVERY

CURRENT_P0=SYSTEM_CONFORMANCE_REPAIR_REQUIRED

This document is current truth only. Git history retains prior states.

## Current status

| Area | Current truth |
| --- | --- |
| North Star | FROZEN: Autonomous Capital Management Platform; CAPITAL; EQUITY CURVE HEALTH |
| Foundation | Historical component proofs retained; current deployment/runtime conformance is not accepted pending repair |
| Exchange | OKX foundation and exchange-neutral contracts established |
| Financial Truth | Forward health PASS; no heuristic economics permitted |
| PAPER Entry Atomicity | `PASS_2_2` |
| Risk Budget Contract | Component tests passed at immutable-event commit `f965a0b35f8be1b900cbd0e73332c653b003ca0a`; current 4-environment activation/parity requires Wave 3 re-acceptance |
| Risk Budget Influence | OFF |
| Learning | Auto-apply OFF; VPS LIVE freshness FAIL pending Wave 4 repair |
| Strategy Ownership | V1 `TERMINAL`; narrowing hypothesis supported; broad rule not qualified unchanged |
| Movement vs Cost | Corrected-label generalization not stable; frozen forward holdout `ACTIVE_UNINSPECTED` |
| Fee Velocity | High fee velocity / same-thesis duplication is a leading mechanism |
| New Risk vs Keep Risk | Strong diagnostic candidate; policy not yet proven |
| Same-Thesis Duplication | H11 historical redundancy confirmed; P4 frozen non-blocking holdout `ACTIVE_UNINSPECTED` |
| 1m/5m | MIXED; no global block authorized |
| Portfolio Crowding | WEAK effect |
| Winner Tail | LOW dependence; top 10% winners offset 1.875% of losses |
| Exit Giveback | Economic Floor V2 cadence experiment COMPLETE; exact-zero boundary remains the primary issue |
| LOCAL PAPER | HEALTHY; Economic Floor V2 exit authority OFF; boundary/recovery evidence collection ACTIVE |
| Harness Safety | Correct real global DB risk abort; blocked-session diagnostic capture COMPLETE; 40 tests passed |
| VPS PAPER | Runtime revisions `NON_UNIFORM_NOT_ACCEPTED`; former `d60c451...` value is superseded by conformance audits |
| Full Opportunity throughput | VPS functional correctness, forward canonical freshness, and service-rate headroom PASS |
| Read-only discovery conveyor | Experiments #3–#7 COMPLETE; no new treatment authority |
| LOCAL LIVE | Capital Preservation Mode `ACTIVE`; new entries `NO`; exit/close `YES` |
| Equity UI canonical authority | Historical component proof retained; current semantic/authority health and deployment acceptance are incomplete pending Wave 4 |
| VPS LIVE | Capital Preservation Mode `ACTIVE`; new entries `NO`; exit/close `YES` |
| Economic Proof | Stable positive net expectancy after costs is NOT PROVEN |
| Capital Allocation | NOT NOW; requires economic and Risk Budget policy proof |
| Public Communication | `ACTIVE`; LinkedIn and X active; governance documented |

## System conformance repair

`AUDITS_CONSOLIDATED=4`

`UNIQUE_P0=5`

`UNIQUE_P1=9`

`UNIQUE_P2=7`

`UNIQUE_P3=3`

The bounded planning and defect-register contract is
[WALTRADE_SYSTEM_CONFORMANCE_REPAIR_V1.md](WALTRADE_SYSTEM_CONFORMANCE_REPAIR_V1.md).
It defines five dependency-ordered waves, the permanent four-part deployment
acceptance gate, and the Capability Activation Ledger. It grants no runtime,
database, config, PAPER or LIVE authority.

`NEW_ECONOMIC_EXPERIMENTS=PAUSED`

`ACTIVE_HOLDOUTS=UNCHANGED_UNINSPECTED`

`H12=PRESERVED_UNINSPECTED_BUT_NOT_CURRENT_WORK`

`LIVE_ENTRY_AUTHORITY=OFF_LOCAL_AND_VPS`

`NEXT_IMPLEMENTATION_UNIT=MR1_MINIMAL_RELEASE_TRUTH`

`NEXT_ACTION=MR1_MINIMAL_RELEASE_TRUTH`

`MINIMUM_RECOVERY_SEQUENCE=MR1_MINIMAL_RELEASE_TRUTH,MR2_CORE_ADMISSION_REPAIR,MR3_DIRECT_PAPER_ENFORCEMENT_AND_ECONOMIC_ACCEPTANCE`

`POLICY_SOURCE_CHECK_STATUS=CONTRADICTION_STOP`

`POLICY_SEMANTICS_UNRESOLVED=YES`

The history-derived 20/20 candidate conflicts with LOCAL PAPER DB for
`BBRANGE/RANGE_LOWVOL` and `SUPERTREND/RANGE_LOWVOL`. MR2 is stopped before
implementation pending explicit authority; no policy value is guessed.

`DEFERRED_SCOPE=ORIGINAL_WAVE_3_REMAINDER,ORIGINAL_WAVE_4,H12,RISK_BUDGET,SLOT_BRAIN,ORC,LEARNING,UI,MIGRATION_WIDE_CLEANUP`

`DEFERRED_AUTHORITY=OFF_NOT_PASS`

Regime-dependent economic cohorts preceding the first entry/runtime-semantic
repair deployment are preserved as `PRE_CONFORMANCE_LEGACY_EVIDENCE`, `UNTREATED`, and
`REGIME_NOT_ENFORCED` at its exact cutoff. They are not pooled with post-repair
evidence or inspected during repair. Independent Fee V2, Financial Truth,
entry-atomicity and infrastructure proofs retain their original status.

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
- Promotion path: `LOCAL implementation/tests -> commit -> push exact SHA to GitHub -> LOCAL PAPER deploy exact SHA -> LOCAL acceptance -> VPS fetch and inspect -> VPS pull --ff-only exact approved history -> VPS PAPER deploy -> independent VPS acceptance -> frozen LIVE environments last`.
- Corresponding `LOCAL PAPER ↔ VPS PAPER` and, where applicable, `LOCAL LIVE ↔ VPS LIVE` environments require `GIT_PARITY`, `CONTRACT_PARITY`, `DIRECT_SCHEMA_DEPENDENCY_PARITY`, `SCHEMA_OBJECT_PARITY`, `RUNTIME_SEMANTIC_PARITY`, and applicable shared `CONFIG_CONTRACT_PARITY`.
- `SCHEMA_OBJECT_PARITY` covers every schema object and migration state required by the active promoted runtime contract, not merely table-name existence. Environment-specific objects must be explicit in the promoted contract; LOCAL-only and VPS-only schema magic are forbidden.
- `BUSINESS_DATA_PARITY=NOT_REQUIRED`; `BUSINESS_DATA_DIVERGENCE=EXPECTED`. Natural differences in runtime-generated rows, histories, evidence, timestamps, balances, and market outcomes are not parity failures and must not be copied to manufacture equality.
- Canonical parity principle: `SAME_SHARED_CONTRACT; DIFFERENT_NATURAL_DATA`. A common Git SHA alone is insufficient, and business-data equality is never a rollout gate.
- `RESEARCH_UNIVERSE` is broad PAPER evidence and may remain aggregate-negative.
- `PAPER_RESEARCH_UNIVERSE_MAY_BE_NET_NEGATIVE=YES`.
- `LOCAL_PAPER_CAUSAL_TREATMENTS=ONE_ACTIVE_AT_A_TIME`.
- When a LOCAL experiment is active, VPS PAPER remains its untreated natural
  reference and may also support read-only discovery for future experiments.
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
`PRIOR_NEXT_ECONOMIC_RESEARCH_PRIORITY=ECONOMIC_FLOOR_V2_CADENCE_INDEPENDENT_PROTECTION`;
`CURRENT_ECONOMIC_RESEARCH=PAUSED_PENDING_SYSTEM_CONFORMANCE_REPAIR`.

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

## Economic Floor V2 final cadence decision

`ECONOMIC_FLOOR_V2_CADENCE_INDEPENDENT_PROTECTION` asks whether the same
authoritative cost-cover and realizable-net protection can be evaluated at a
common causal cadence independent of the originating strategy interval, using
existing market/evidence infrastructure and avoiding a new engine where
possible.

`ECONOMIC_FLOOR_V2_STATUS=COMPLETE`;
`ECONOMIC_FLOOR_V2_FINAL_VERDICT=CADENCE_FIX_VALIDATED_BOUNDARY_REMAINS_PRIMARY_ISSUE`.

`V2_1M_ORIGIN_EXIT_COUNT=2`; `V2_5M_ORIGIN_EXIT_COUNT=6`;
`FIVE_MINUTE_ORIGIN_ARM_COUNT=8`;
`FIVE_MINUTE_ORIGIN_FLOOR_EXIT_COUNT=6`.

| Final LOCAL PAPER V2 cadence cohort | Value |
| --- | ---: |
| Arm events | 14 |
| Exit intents | 8 |
| Completed floor exits | 8 |
| 1m-origin floor exits | 2 |
| 5m-origin arms | 8 |
| 5m-origin floor exits | 6 |
| V2 5m average final net | -0.0128205821774 USDC |
| V2 5m average cross-zero gap | 0.01745574779875 USDC |

- `CADENCE_DEFECT_FIXED_ARCHITECTURALLY=YES`;
  `V2_CADENCE_INDEPENDENCE_VALIDATED=YES`; `V2_UPSIDE_PRESERVED=YES`.
- `REMAINING_NEGATIVE_EXIT_PRIMARY_CAUSE=EXACT_ZERO_BOUNDARY`;
  `V2_BOUNDARY_VALIDATED=NO`.
- `UPSIDE_INTERFERENCE_PROVEN=NO`; `PREMATURE_EXIT_HARM_PROVEN=NO`.
- `SMALL_V2_ECONOMIC_COHORT_SUFFICIENT=YES`;
  `SUFFICIENCY_SCOPE=CADENCE_EXPERIMENT_CLOSURE_DECISION_ONLY_NOT_STATISTICAL_PERFORMANCE_PROOF`.
- `ECONOMIC_FLOOR_V2_VPS_PROMOTION_READY=NO`;
  `ECONOMIC_FLOOR_V2_LIVE_ELIGIBILITY=NO`.

The frozen V1 5m cadence was 300 seconds; V2 evaluated every originating
interval at a common 60-second cadence. V1's three 5m exits averaged
`-0.0151844783127 USDC` with average cross-zero gap
`0.0282080437513 USDC`. V2's six 5m exits averaged
`-0.0128205821774 USDC` with average cross-zero gap
`0.01745574779875 USDC`. The cohorts are small and non-equivalent: this is
descriptive/mechanistic evidence and authorizes no statistical improvement
claim.

V2 is complete as a bounded cadence experiment and is not harmful, qualified
for VPS promotion, or eligible for LIVE. It must not be tuned in place or kept
open merely to increase N. This docs task does not change the deployed LOCAL
PAPER runtime or treatment configuration.

The historically proposed separate experiment was
`ECONOMIC_FLOOR_BOUNDARY_REFINEMENT_V1`: determine what positive protected
economic boundary can produce non-negative realized execution under the
validated common 1m cadence while preserving upside and avoiding premature
exits. It is not implemented, started, or authorized by this record. No
numeric buffer—including no `+0.025` assumption—is frozen; no parameter sweep,
ratchet, or strategy-, symbol-, interval-, or regime-specific threshold is
authorized. `STEPWISE_RATCHET` remains later research.

`BOUNDARY_REFINEMENT_IMPLEMENTED=NO`;
`BOUNDARY_REFINEMENT_STARTED=NO`. It is no longer the automatic current/next
priority; existing boundary/recovery collection remains passive only.

## Economic Floor V2 active economic evaluation closure

The cumulative causally matched LOCAL PAPER versus untreated VPS PAPER review
contained 14 non-ambiguous Floor-triggered pairs: seven favored LOCAL and seven
favored VPS. Avoided loss totaled `0.211732391635 USDC`, lost upside totaled
`0.418828994501 USDC`, and the matched net effect was
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

LOCAL PAPER now runs V2 as evidence-only. Existing strategy exit authority and
entry semantics are unchanged. `BOUNDARY_EVIDENCE_COLLECTION=ACTIVE` and
`RECOVERY_EVIDENCE_COLLECTION=ACTIVE`; both have zero trading authority. No
positive boundary, recovery rule, ratchet, context-specific threshold, VPS
promotion, or LIVE authority is created. Any future Floor treatment requires a
separate recovery-aware evidence decision.

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
- `LEGACY_BINARY_MOVEMENT_CAPACITY_PREDICTABILITY=STRONG_HISTORICAL_DEFINITION`;
  later corrected-label generalization does not support a current `STRONG`
  claim.
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
- No Movement Capacity treatment is active or authorized. Its new forward
  holdout is active but deliberately uninspected and does not block trades.

## Read-only discovery conveyor #3–#7

`DISCOVERY_CONVEYOR_STATUS=COMPLETE`. These VPS PAPER results are read-only
discovery, not treatment authority. Economic Floor V2 is complete for the
cadence question; the conveyor changed no treatment authority.

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

### Experiment #5 — Same-Thesis Control, superseded by H11

- The initial conveyor result was partial and authorized no gate.
- H11 now confirms redundant episode exposure across historical replicated
  partitions; forward causal proof remains absent.
- `H11_P4_CANDIDATE=FROZEN_NOT_AUTHORIZED`; `LOCAL_TREATMENT=NO`.
- `INTERACTION_WITH_MOVEMENT_CAPACITY=INDEPENDENT_NON_BLOCKING_LABELS`.

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

## H11 closure and P4 forward holdout

`H11_STATUS=COMPLETE`;
`H11_PLAN_FINGERPRINT=c4286e14160d1d8467329d7a14636a035903b40ad87ea8d8c1480171e5e55826`.

- Corrected population: `971`; 15-minute episodes: `534`; multi-position
  episodes: `236`; maximum positions per episode: `8`.
- Position-count correlation with episode net: `-0.677527`; with episode loss:
  `+0.688393`.
- Two or more symbols lost together in `197` episodes; all four in `18`.
- `H11_REDUNDANT_EXPOSURE=CONFIRMED_HISTORICALLY_REPLICATED_NOT_FORWARD_PROVEN`.
- Marginal value was negative at every observed ordinal and fees multiplied
  with episode size.
- `H11_BTC_CAUSAL_VETO=REJECTED`: BTC leadership was unsupported; BTC
  co-movement is context only and grants no alt veto.

The frozen candidate is
`SAME_THESIS_EPISODE_REDUNDANCY_V1_FIRST_SIGNAL`, fingerprint
`951b7b6a41fb210fed6b4e37f6f63ff0a8d5e6083d1e8583a83890d7d80ee2fe`.
It marks a later LONG signal redundant only after an earlier accepted signal
in the same 15-minute UTC episode with identical symbol, side, and causal
regime; ordering is decision time then position ID, missing regime admits, and
later replacement is forbidden.

Historical TRAIN/VALIDATION/HISTORICAL_HOLDOUT net deltas were respectively
`+6.810305`, `+0.800396`, and `+2.376732 USDC`; winner retention was `93.75%`,
`100%`, and `100%`. Same-rate random outperformance has only a weak validation
margin, so forward confirmation is required.

`H11_P4_FORWARD_HOLDOUT=ACTIVE_UNINSPECTED`;
`P4_FORWARD_HOLDOUT_START_UTC=2026-09-06T17:24:04.625559215Z`.
The immutable contract is outside Git under
`~/waltrade-experiments/same-thesis-episode-redundancy-v1-forward-holdout/`.
It is non-blocking, derives `WOULD_BLOCK` from existing immutable evidence, and
does not inspect efficacy before its 25/100/conditional-200 checkpoints.
`P4_TREATMENT_ACTIVE=NO`; `P4_TRADES_BLOCKED=NO`.

## H12 probabilistic outcome surface

`H12_PLAN_FINGERPRINT=a2b33fa89149dfed6fb49744688d53bf9ae026da37e34ecebf94e84d3afeb5f2`;
`H12_POPULATION=971`; `H12_EPISODES=534`.

The pre-result frozen plan used only evidence strictly before
`2026-09-06T10:35:00Z` and inspected neither current forward holdout. Complete
paths were `971/971` at 15/30/60/120 minutes and `969/971` at 240 minutes; two
incomplete 240m paths were excluded without imputation. Longer 1d/7d/30d
paths were excluded for non-comparable coverage.

The deterministic strict walk-forward analysis generated `4,853` predictions
and `275` support-qualified hierarchical context rows. In validation, the
hierarchical cost-cover Brier score was `0.171277` versus global `0.167286`; in
historical holdout it was `0.136815` versus `0.134768`. Log loss agreed, and
episode-bootstrap aggregate binary interval coverage was `67.78%`.
`H12_PROBABILITY_CALIBRATION=NOT_SUPPORTED`;
`H12_CONTEXT_STABILITY=FAIL`.

`H12_DECISION=B_SIGNAL_EXISTS_BUT_CALIBRATION_OR_SUPPORT_INSUFFICIENT`.
The outcome surface is reproducible locally but not qualified for H13, and its
executable estimator is not promoted/reproducible on VPS. The untouched
forward calibration persistence is preserved uninspected but is not current
work.

`H12_CASH_BASELINE_AVAILABLE=YES`;
`H12_CONTINUE_CLOSE_REDEPLOY_IDENTIFIABLE=NO`;
`H12_NEW_TELEMETRY_REQUIRED=YES_FOR_CONTINUE_CLOSE_REDEPLOY_UTILITY_NOT_FOR_ENTRY_OUTCOME_SURFACE`;
`H12_FORWARD_CALIBRATION=PRESERVED_UNINSPECTED_BUT_NOT_CURRENT_WORK`.

### H12 forward calibration holdout

`H12_ARTIFACT_AUDIT=PASS`;
`PREDECLARED_FROZEN_ESTIMATOR_EXISTS=YES`;
`ESTIMATOR_VERSION=H12_HIERARCHICAL_EPISODE_BALANCED_SHRUNK_EMPIRICAL_V1`;
`ESTIMATOR_FINGERPRINT=a51b12b9f02eb7de0e7dbe4dfc6fa051ead3a60d7f40f3175411131a6ebfe2a2`.

`H12_FORWARD_CALIBRATION=PRESERVED_UNINSPECTED_BUT_NOT_CURRENT_WORK`;
`H12_FORWARD_START_UTC=2026-09-06T17:42:01.083408677Z`;
`H12_FORWARD_CONTRACT_SHA256=9ea37d62208ced970637efc74f9e31cde829a2e69dfbea12bdaa6eaddae566ea`.

The forward contract freezes the only predeclared historical estimator; no
validation/holdout-driven variant was selected. Its predictions are
reconstructable offline from immutable causal entry evidence and strictly
earlier completed outcomes. No runtime prediction write or new entry telemetry
is required, and unsupported estimates remain `UNKNOWN`.

Primary predictions are cost-cover, +0.5% net, +1% net, drawdown worse than
-1%, and expected terminal net across 15/30/60/120/240 minutes. Primary
calibration uses Brier, calibration-in-the-large, slope, fixed-band ECE and
reliability, coverage, episode support, and paired improvement versus the
causal unconditional base rate. Episode-block uncertainty bounds define
success; AUC cannot qualify the surface.

Integrity is checked after 25 complete 240m outcomes without efficacy review;
the first formal checkpoint is at least 100 plus adequate episode support, and
200 is conditional on unresolved uncertainty. Movement V1 and P4 are unchanged
and uninspected. `H12_TREATMENT_ACTIVE=NO`; `H12_TRADES_BLOCKED=NO`;
`H12_CAPITAL_AUTHORITY=NO`.

Cash/no-trade is available as the zero incremental baseline. Expected net,
supported tail risk, capital-hours, and full Fee V2 costs are available;
correlated heat is partial. Continue/close/redeploy utility is not identifiable
without action-time close value, remaining-horizon paths, contemporaneous
redeployment alternatives, and their causal capital/heat outcomes. That later
question requires a minimal forward evidence contract; the entry outcome
surface itself needs no new telemetry.

Cost-cover is diagnostic, not sufficient utility. D means no cost cover within
a fixed horizon, not permanently bad; C means bounded post-exit movement
capacity, not realizable profit. A/B/C/D are secondary diagnostics. The future
primary target is risk-adjusted net capital utility including all costs, time,
drawdown, correlation, and opportunity cost. NO TRADE/CASH is the baseline;
PAPER explores and LIVE requires positive expected net with uncertainty
control.

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
  also complete for the cadence question.

## NOW

- `OWNERSHIP_V1=COMPLETE`; do not tune V1 in place.
- Preserve Ownership V2 as a documentation-only frozen hypothesis; do not
  implement or activate it.
- `ECONOMIC_FLOOR_V1_STATUS=COMPLETE`; do not tune its boundary in place.
- `ECONOMIC_FLOOR_V2_STATUS=COMPLETE`; active V2 exit authority is OFF.
- Preserve `MOVEMENT_V1_FORWARD_HOLDOUT=ACTIVE_UNINSPECTED`.
- Preserve `H11_P4_FORWARD_HOLDOUT=ACTIVE_UNINSPECTED_NON_BLOCKING`.
- Preserve `H12_FORWARD_CALIBRATION=PRESERVED_UNINSPECTED_BUT_NOT_CURRENT_WORK`.
- Continue boundary/recovery evidence collection as passive evidence only.
- H12 is verdict B and grants no runtime or capital authority.
- Monitor forward freshness and the moderate pipeline risk; no pipeline change is required.

## NEXT

- `PRIOR_PRIMARY_RESEARCH=PROBABILISTIC_CAPITAL_DECISION_FOUNDATION`;
  `CURRENT_PRIMARY_RESEARCH=PAUSED_PENDING_SYSTEM_CONFORMANCE_REPAIR`.
- Perform no H12 checkpoint work during repair;
  `H12=PRESERVED_UNINSPECTED_BUT_NOT_CURRENT_WORK`.
- Resume no economic roadmap item until conformance exit gates and a separate
  Product Owner decision permit it.
- `LIVE_ELIGIBILITY=NO`; LIVE remains last with explicit Product Owner approval.
- Economic Floor V1/V2 is not qualified for VPS or LIVE; LOCAL PAPER V2 exit
  authority is OFF.

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

# WalTrade Daily Status

LAST_UPDATED=2026-08-31

VPS_PAPER_RUNTIME_SHA=d60c4517892c220b6450876c47f27d99e8bf4dc8

OWNERSHIP_CANDIDATE_INTRODUCED_SHA=d60c4517892c220b6450876c47f27d99e8bf4dc8

CURRENT_PHASE=STOP_LOSING

CURRENT_P0=ECONOMIC_FLOOR_AFTER_COST_COVER_V1_RESEARCH_DESIGN

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
| Exit Giveback | Proven secondary leak; immediate tiny-positive exit rejected |
| LOCAL PAPER | HEALTHY / OWNERSHIP EXPERIMENT TERMINAL |
| Harness Safety | Correct real global DB risk abort; blocked-session diagnostic capture COMPLETE; 40 tests passed |
| VPS PAPER | Ownership V1 bounded research decision `TERMINAL`; runtime unchanged by docs task |
| Full Opportunity throughput | VPS functional correctness, forward canonical freshness, and service-rate headroom PASS |
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
`NEXT_ECONOMIC_RESEARCH_PRIORITY=ECONOMIC_FLOOR_AFTER_COST_COVER_V1`;
`ECONOMIC_FLOOR_STATUS=NEXT_READY_FOR_RESEARCH_DESIGN`;
`ECONOMIC_FLOOR_START=NO`;
`ECONOMIC_FLOOR_IMPLEMENTED=NO`.

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
- `MOVEMENT_CAPACITY_PREDICTABILITY=STRONG`.
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
- No movement treatment is active or authorized. The frozen ownership candidate remains unchanged and first in causal order.
- After the ownership decision gate, ordered work is: economic floor after cost cover; context-specific movement capacity on new data; economic no-trade/new risk versus keep; same-thesis control; fee velocity; 1m/5m duplication.

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
- Movement Capacity remains `NEEDS_MORE_RESEARCH` and Economic Floor remains
  `FIRST_POST_OWNERSHIP_RESEARCH_PRIORITY`; no new treatment is authorized.

## NOW

- `OWNERSHIP_V1=COMPLETE`; do not tune V1 in place.
- Preserve V2 as a documentation-only frozen hypothesis; do not implement or activate it.
- Move Economic Floor from `WAITING_BEHIND_OWNERSHIP` to `NEXT_READY_FOR_RESEARCH_DESIGN` only.
- Monitor forward freshness and the moderate pipeline risk; no pipeline change is required.

## NEXT

- `CURRENT_STAGE=ECONOMIC_FLOOR_AFTER_COST_COVER_V1_RESEARCH_DESIGN_READY`.
- Design Economic Floor as the existing next post-ownership priority; do not implement it in this docs task.
- Any future V2 activation requires separate authorization and new natural evidence.
- Keep `LIVE_ELIGIBILITY=NO`.
- Movement capacity remains `NEEDS_MORE_RESEARCH`; `SECOND_CAUSAL_TREATMENT_AUTHORIZED=NO`.
- Economic Floor remains a future exit-only research candidate, not an authorized exit rule.

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

# STOP LOSING Master Checklist

This is the single living evidence tracker for the STOP LOSING phase. Git history is the history; do not create `v2`, `v3`, numbered, or timestamped checklist copies.

Status legend: `COMPLETE`, `IN_PROGRESS`, `EVIDENCE_NEEDS_CAUSAL_PROOF`, `NOT_COMPLETE`, `REJECTED`.

## A. Foundation and safety

- [x] `PAPER_ENTRY_ATOMICITY=PASS_2_2` — COMPLETE
- [x] Position 12599 classified `INCOMPLETE_EXECUTION / NOT_EXECUTED`; no fabricated economics — COMPLETE
- [x] Fee V2 canonical contract: 0.35% per side; roundtrip hurdle ~0.7024586051% — COMPLETE
- [x] Financial Truth forward health — COMPLETE
- [x] Full PAPER Opportunity Observation — COMPLETE
- [x] `RISK_BUDGET_CONTRACT_PARITY_4_4=PASS` — COMPLETE
- [x] Risk Budget immutable-event commit `f965a0b35f8be1b900cbd0e73332c653b003ca0a` — COMPLETE
- [x] Risk Budget influence OFF — COMPLETE
- [x] Learning auto-apply OFF — COMPLETE
- [x] Explicit environments: `PAPER=.env.okx.paper`, `LIVE=.env.okx.live` — COMPLETE
- [x] Deployment parity invariant: Git + contract + direct schema dependency + runtime semantics — COMPLETE
- [x] LOCAL/VPS authority model — COMPLETE: shared promoted contract; isolated runtime/data histories
- [x] LOCAL-only implementation, commit, push, and canonical-doc promotion authority — COMPLETE
- [x] VPS pull-only authority — COMPLETE: fetch, `pull --ff-only`, deploy, independently validate
- [x] VPS-only shared-code fixes, schema magic, pushes, and frozen-semantic changes — FORBIDDEN
- [x] Promotion path `LOCAL → GitHub → VPS pull --ff-only → independent validation` — COMPLETE
- [ ] `GIT_PARITY` — REQUIRED FOR EACH FUTURE RELEVANT SHARED ROLLOUT
- [ ] `CONTRACT_PARITY` — REQUIRED FOR EACH FUTURE RELEVANT SHARED ROLLOUT
- [ ] `DIRECT_SCHEMA_DEPENDENCY_PARITY` — REQUIRED FOR EACH FUTURE RELEVANT SHARED ROLLOUT
- [ ] `SCHEMA_OBJECT_PARITY` — REQUIRED FOR EACH FUTURE RELEVANT SHARED ROLLOUT
- [ ] `RUNTIME_SEMANTIC_PARITY` — REQUIRED FOR EACH FUTURE RELEVANT SHARED ROLLOUT
- [ ] `CONFIG_CONTRACT_PARITY` — REQUIRED WHERE SHARED/APPLICABLE
- [x] `SCHEMA_OBJECT_PARITY` means semantic equivalence of all active-contract tables, columns/types/nullability/defaults, keys, constraints, indexes, views/materialized views, functions/procedures, triggers, extensions, required migration state, and direct dependencies — INVARIANT
- [x] Required object missing in a corresponding environment fails parity even when `GIT_PARITY=PASS` — INVARIANT
- [x] Required active-contract migrations/objects apply by environment contract; blind all-environment migration and LOCAL/VPS-only schema magic — FORBIDDEN
- [x] `BUSINESS_DATA_PARITY=NOT_REQUIRED`; `BUSINESS_DATA_DIVERGENCE=EXPECTED` — INVARIANT
- [x] Natural row-count, trade, position, fill, order, balance, Financial Truth, observation, counterfactual, timestamp, market-outcome, learning/evidence, and audit-history differences are not parity failures — INVARIANT
- [x] Copying business data to manufacture LOCAL/VPS parity — FORBIDDEN
- [x] `SAME_SHARED_CONTRACT; DIFFERENT_NATURAL_DATA` — COMPLETE
- [x] PAPER research universe may intentionally remain aggregate-negative — COMPLETE
- [x] `TOTAL_PAPER_PNL > 0` is not a universal progress prerequisite — COMPLETE
- [x] Qualified universe requires frozen policy plus independent validation — COMPLETE
- [x] LIVE universe contains only explicitly approved qualified decisions — COMPLETE
- [x] Positive expected net after all costs with acceptable uncertainty is required for potential LIVE eligibility — COMPLETE
- [x] `EXPECTED_NET_AFTER_ALL_COSTS > 0` potential-LIVE gate — COMPLETE
- [x] Acceptable good-missed, drawdown/risk, execution, Financial Truth, auditability, and Product Owner approval gates — COMPLETE
- [x] Hindsight winner selection / cherry-picked LIVE cohort — FORBIDDEN
- [x] Attributed LOCAL experiment DB safety and background false-positive correction — COMPLETE
- [x] Read-only manual experiment observability and terminal artifacts — COMPLETE
- [x] LOCAL harness PostgreSQL client-disconnect RCA — COMPLETE: `HARNESS_FALSE_POSITIVE_CRITICAL_CLASSIFICATION`
- [x] LOCAL harness critical classifier correction — COMPLETE: expected attributed recreation disconnect is WARNING / CONTINUE
- [x] Terminal-condition catalog — COMPLETE
- [x] Recent LOCAL PAPER log replay against terminal catalog — COMPLETE / PASS
- [x] `FALSE_POSITIVE_TERMINAL_EVENTS=0` — COMPLETE
- [x] `UNKNOWN_TERMINAL_CLASSIFICATIONS=0` — COMPLETE
- [x] Harness validation — COMPLETE: 18 classifier tests + 14 attributed DB safety tests = 32 PASS
- [x] Equity UI canonical read authority LOCAL LIVE validation — COMPLETE / PASS
- [x] Equity UI RCA — `CANONICAL_EQUITY_AUTHORITY=HEALTHY`; `UI_API_READ_AUTHORITY=LEGACY_DEFECT`; `FRONTEND=VALID`
- [x] Equity UI minimum fix — `FIX_CLASS=REWIRE_EXISTING_AUTHORITY`; no new schema, engine, frontend model, or data repair
- [x] Equity UI shared promotion and VPS LIVE validation — COMPLETE
- [x] `EQUITY_UI_GIT_PARITY=PASS` — COMPLETE
- [x] `EQUITY_UI_CONTRACT_PARITY=PASS` — COMPLETE
- [x] `EQUITY_UI_DIRECT_SCHEMA_DEPENDENCY_PARITY=PASS` — COMPLETE
- [x] `EQUITY_UI_RUNTIME_SEMANTIC_PARITY=PASS` — COMPLETE
- [x] LOCAL LIVE Capital Preservation Mode — ACTIVE: new entries NO; exit/close YES
- [x] VPS LIVE Capital Preservation Mode — ACTIVE: new entries NO; exit/close YES
- [x] VPS PAPER direct schema dependency repair — COMPLETE
- [x] Full Opportunity projection lookup RCA — COMPLETE: expensive lookups plus FIFO single consumer
- [x] LOCAL Full Opportunity throughput optimization — PASS
- [x] VPS PAPER Full Opportunity throughput validation — COMPLETE / PASS
- [x] `FUNCTIONAL_CORRECTNESS=PASS` — COMPLETE
- [x] `FULL_PAPER_OPPORTUNITY_OBSERVATION_HEALTH=PASS` — COMPLETE
- [x] `FORWARD_CANONICAL_FRESHNESS=PASS` — COMPLETE
- [x] Cutoff missing logical / 1m / 5m keys `0 / 0 / 0` — COMPLETE
- [x] Not-exactly-one / duplicate observation / duplicate causal IDs `0 / 0 / 0` — COMPLETE
- [x] `ELIGIBLE_UNPROCESSED_THROUGH_CUTOFF=0` — COMPLETE
- [x] `DIRECT_SCHEMA_DEPENDENCY_PARITY=PASS` — COMPLETE
- [x] Service rate 25.400 > arrival 25.300 rows/minute — PASS
- [ ] Throughput headroom monitoring — IN_PROGRESS: narrow +0.100 rows/minute margin
- [ ] Periodic PAPER cost-model calibration against authoritative LIVE fees, maker/taker behavior, spread, slippage, partial fills, latency, and execution path — REQUIRED BEFORE MEANINGFUL LIVE RE-ENABLEMENT
- [x] Fee V2 remains canonical pending separately validated model change — COMPLETE

## B. Economic baseline and initial forensics

- [x] Full-cost hurdle — COMPLETE
- [x] Primary defect `INSUFFICIENT_MOVEMENT_RELATIVE_TO_FULL_COST` — COMPLETE
- [x] Movement Capacity initial forensic — COMPLETE
- [x] Fee Velocity forensic — COMPLETE
- [x] Marginal Portfolio Value forensic — COMPLETE
- [x] Cash / No-Trade diagnostic — COMPLETE
- [x] Winner Tail forensic — COMPLETE
- [x] Immediate tiny-positive exit — REJECTED
- [x] Post-cost-cover giveback — COMPLETE / PROVEN SECONDARY LEAK
- [ ] Economic No-Trade causal treatment — NOT COMPLETE

### Movement-capacity qualification

- [x] Canonical LOCAL cohort — COMPLETE: 1,517 mature trades
- [x] `240M_MFE_FULL_COST_COVER=828/1517` (54.581%) — COMPLETE
- [x] `INSUFFICIENT_MOVEMENT_240M=689/1517` (45.419%) — COMPLETE
- [x] `FINAL_NET_POSITIVE=172/1517`; final net -177.145833 USDC — COMPLETE
- [x] Metric reconciliation — COMPLETE: bounded 240m MFE viability is not final realized profitability and does not overwrite the differently defined VPS forensic
- [x] `MOVEMENT_CAPACITY_PREDICTABILITY=STRONG` — COMPLETE
- [x] `PRE_ENTRY_NON_VALUE_ADD_SEPARABILITY=WEAK` — COMPLETE
- [x] `ECONOMIC_NO_TRADE_REGION_SUPPORTED=PARTIAL` — COMPLETE
- [x] `MARGINAL_PORTFOLIO_VALUE=-103.994083_USDC_FOR_923_ADDITIONAL_ADMISSIONS` — COMPLETE
- [x] `NO_NEW_SCORE_REQUIRED=YES`; `NEW_ENGINE_REQUIRED=NO` — COMPLETE
- [x] Chronological DEVELOPMENT/HOLDOUT split — COMPLETE: 1,062 / 455
- [x] Three-rule maximum and DEVELOPMENT-only selection — PASS
- [x] Candidate frozen before HOLDOUT — `ATR_PCT <= 0.05275618624106265 AND REALTIME_SCORE <= 18.24315`; missing ATR allowed
- [x] HOLDOUT aggregate reproduction — 49 bad avoided; 19 good missed; 90.821% good retention; 19.758% bad rejection
- [x] Ownership ordering — 29 overlapping blocks; 39 incremental; +6.132816 USDC diagnostic incremental value
- [x] Major subgroup gate — FAIL: RSI good retention 46.67%; 5m bad rejection 1.41%
- [x] `HOLDOUT_GENERALIZATION=WEAK_GENERALIZATION`
- [x] `CAUSAL_ELIGIBILITY=NEEDS_MORE_RESEARCH`
- [x] `GLOBAL_MOVEMENT_GATE_READY=NO`
- [x] `CURRENT_HOLDOUT_BURNED=YES`; 455-trade holdout cannot tune a revised ATR/realtime rule
- [ ] `NEXT_MOVEMENT_RESEARCH=CONTEXT_SPECIFIC_ON_NEW_DATA_ONLY`
- [ ] Movement-capacity shadow counterfactual treatment — NOT AUTHORIZED

Ordered economic work after ownership reaches its decision gate:
`ECONOMIC_FLOOR_AFTER_COST_COVER_V1`, context-specific `MOVEMENT_CAPACITY` on
new data, `ECONOMIC_NO_TRADE / NEW_RISK_VS_KEEP`, `SAME_THESIS_CONTROL`,
`FEE_VELOCITY`, then `1m/5m_DUPLICATION`.

Latest VPS PAPER baseline:

| Metric | Value |
| --- | ---: |
| Trades | 1,515 |
| Net | -183.346579 USDC |
| Fees | 212.195019 USDC |
| Insufficient-movement rate | 78.2178% |
| Additional admissions | 965 |
| Non-value-add admissions | 868 |
| Non-value-add rate | 89.948% |
| Non-value-add fees | 121.513802 USDC |
| Non-value-add net | -122.770951 USDC |

Winner-tail result: `WINNER_TAIL_DEPENDENCE=LOW`; top 10% winners offset 1.875% of losses.

## C. Strategy ownership and ordered relationships

- [x] Ordered Strategy Pair Matrix — COMPLETE
- [x] RSI ↔ BBRANGE relationship — COMPLETE FORENSIC
- [x] BBRANGE-after-RSI marginal value — COMPLETE FORENSIC
- [x] TREND ↔ SUPERTREND ordered relationship matrix — COMPLETE FORENSIC
- [x] Same-Thesis Duplication — COMPLETE FORENSIC
- [x] Regime Transition vs Routing — COMPLETE FORENSIC
- [x] New Risk vs Keep Existing Risk diagnostic — COMPLETE
- [ ] Final ownership policy — NOT COMPLETE; causal evidence required
- [ ] VPS ownership treatment replication — IN_PROGRESS

Key ordered-pair evidence:

| Pair | N | Winners | Cost cover | Net |
| --- | ---: | ---: | ---: | ---: |
| BBRANGE → RSI | 134 | 1 | 0.746% | -18.591305 USDC |
| RSI → BBRANGE | 48 | 7 | 16.667% | -4.445463 USDC |
| BBRANGE → TREND | 212 | — | — | -26.227686 USDC |
| TREND → BBRANGE | 144 | — | — | -15.420940 USDC |

Supporting classifications:

- `SAME_THESIS_ADMISSIONS=572`
- `SAME_THESIS_NET=-69.028271 USDC`
- `REGIME_TRANSITION_ADMISSIONS=387`
- `REGIME_TRANSITION_NET=-45.423024 USDC`
- `OWNERSHIP_FAILURE_IS_TRANSITION_DRIVEN=NO`

## D. Current ownership causal experiment

- [x] RSI-after-BBRANGE LOCAL PAPER causal experiment — TERMINAL / EVIDENCE PRESERVED
- [x] Previous valid sequence exposure — COMPLETE: two CONTROL runs and one TREATMENT run
- [x] Previous treatment counterfactuals retained after harness abort — COMPLETE
- [x] Current restart CONTROL `CONTROL-118a765af6` — TERMINAL: `GLOBAL_DB_SAFETY_FAIL`
- [x] `GLOBAL_DB_RISK_ACTUALLY_PRESENT=YES` — COMPLETE
- [x] `SAFETY_ABORT_CLASSIFICATION=CORRECT_REAL_GLOBAL_DB_RISK` — COMPLETE
- [x] `TRADING_IMPACT=NONE` — COMPLETE
- [x] Blocked-session atomic diagnostic capture — COMPLETE: `TESTS=40_PASS`
- [x] `FAIL_CLOSED_BEHAVIOR_WEAKENED=NO` — COMPLETE
- [x] 240-minute blocked-opportunity maturity — COMPLETE: 4 MATURE / 0 PENDING
- [x] `LOCAL_MATURE_240M=4`; `LOCAL_BAD_AVOIDED=4`; `LOCAL_GOOD_MISSED=0` — COMPLETE
- [x] `BAD_AVOIDED > GOOD_MISSED` — PROMISING INITIAL CAUSAL EVIDENCE
- [x] Longer LOCAL discovery wait — NOT REQUIRED: `NO_ARBITRARY_SAMPLE_WAIT`
- [x] `RSI_AFTER_BBRANGE_OWNERSHIP_V1` candidate freeze — COMPLETE
- [x] VPS PAPER ownership acceptance start — COMPLETE: `2026-08-29T07:36:49.339989Z`
- [x] `OWNERSHIP_ACCEPTANCE_STARTED=YES`; `OWNERSHIP_TREATMENT_MODE=TREATMENT` — COMPLETE
- [x] `LONG_RUN_SAFETY_PREFLIGHT=PASS` — COMPLETE
- [ ] VPS PAPER treatment replication — IN_PROGRESS / `TREATMENT`
- [x] First mature independent VPS evidence — 2 affected; 2 blocked; 2 mature; 0 pending; 2 bad avoided; 0 good missed
- [x] VPS rates — `BAD_AVOIDED_RATE=100.000_PERCENT`; `GOOD_MISSED_RATE=0.000_PERCENT`; full-cost cover `0/2`
- [x] VPS avoided fees — `0.084000_USDC`
- [x] VPS qualifying evidence window — `2026-08-29T15:07:10.579096Z` through `2026-08-29T15:08:44.331574Z`
- [x] VPS PAPER runtime, Full PAPER Opportunity Observation, and forward canonical freshness — PASS
- [x] LOCAL 4/4 discovery and VPS 2/2 independent acceptance remain separate; no pooled formal sample
- [ ] First formal VPS review — NOT REACHED: `MATURE_240M=2`, gate `MATURE_240M>=5`
- [ ] Acceptance decision — `WAIT_FOR_MORE_MATURE_EVIDENCE`
- [ ] LIVE eligibility — NOT COMPLETE

Treatment semantics remain one variable: block only an RSI PAPER admission when the same symbol already has an OPEN BBRANGE position with positive remaining inventory. Do not infer success from fewer trades alone.

The latest restart terminated on a real blocked-session condition and correctly
failed closed. The diagnostic gap is closed without weakening genuine DB
protection or changing trading semantics. Four of four mature blocked
opportunities were `BAD_AVOIDED`, with zero `GOOD_MISSED`; this is promising
initial causal evidence, not proof of a final ownership policy.

`OWNERSHIP_STATUS=PROMISING_INITIAL_CAUSAL_EVIDENCE_NOT_PROVEN`;
`LOCAL_DISCOVERY=COMPLETE_FOR_CANDIDATE_FREEZE`;
`OWNERSHIP_CANDIDATE=FROZEN`; `CURRENT_STAGE=VPS_PAPER_INDEPENDENT_REPLICATION_IN_PROGRESS`;
`LIVE_ELIGIBILITY=NOT_COMPLETE`.

The first mature independent VPS evidence is promising, not proof. It remains
separate from LOCAL discovery evidence. The first formal economic review
occurs at `MATURE_240M >= 5`, unless `GOOD_MISSED` becomes materially
concerning earlier. No arbitrary N=20 or N=30 is required. The frozen
candidate is unchanged, throughput headroom remains monitored, and no queue
change or second causal treatment is authorized.

## E. Interval, crowding, and duplication

- [x] 1m/5m semantic relationship — COMPLETE FORENSIC: `MIXED`
- [x] `1M_AFTER_5M_ADMISSIONS=310`; net -35.277603 USDC — COMPLETE
- [x] `5M_AFTER_1M_ADMISSIONS=325`; net -35.998338 USDC — COMPLETE
- [x] Portfolio Crowding forensic — COMPLETE: `WEAK`
- [ ] 1m/5m causal suppression treatment — NOT COMPLETE / NOT AUTHORIZED
- [x] Blind global interval block — REJECTED
- [x] Blind one-strategy-per-symbol rule — REJECTED

## F. Exit and profit protection

- [x] Immediate tiny-positive exit — REJECTED
- [x] `112/115` tiny-positive-to-loss outcomes via `PROFIT_LOCK_TRAIL_DROP` — COMPLETE
- [x] `PROVEN_SECONDARY_LEAK=POST_COST_COVER_GIVEBACK` — COMPLETE
- [x] `ECONOMIC_FLOOR_AFTER_COST_COVER_V1` moved to first post-ownership economic priority — COMPLETE
- [ ] `ECONOMIC_FLOOR_AFTER_COST_COVER_V1` design — NOT COMPLETE
- [ ] Exit-only LOCAL PAPER treatment — NOT COMPLETE
- [ ] VPS PAPER exit replication — NOT COMPLETE

Future objective: after authoritative full economic cost is covered, test a
bounded floor that reduces giveback while leaving winner upside open. This
does not authorize a fixed take-profit, immediate tiny-positive exit, or
`TIME_EXIT` as an economic exit. Entry and exit treatments must not be mixed,
and no exit treatment starts before the current ownership decision gate.

## G. Future risk and allocation work

- [ ] Risk Contribution — NOT COMPLETE
- [ ] Volatility targeting — NOT COMPLETE
- [ ] Risk Budget numeric policy — NOT COMPLETE
- [ ] Drawdown modes and dynamic risk reduction — NOT COMPLETE
- [ ] Capital Allocation — NOT COMPLETE
- [ ] Edge-dependent sizing — NOT COMPLETE
- [ ] Portfolio-impact and correlation-aware sizing — NOT COMPLETE
- [ ] Limited Learning authority — NOT COMPLETE
- [ ] Read-only risk-normalization evidence — FUTURE: volatility-normalized risk, risk contribution, correlation concentration, MAE/exposure normalization
- [x] `RISK_NORMALIZATION_RESEARCH` is separate from `DYNAMIC_CAPITAL_ALLOCATION_AUTHORITY` — COMPLETE
- [x] Fixed 20 USDC sizing remains appropriate during current causal decision-quality research — COMPLETE
- [x] BTC/ETH/SOL/BNB are a correlated crypto complex, not institutional-quality diversification — STRATEGIC CONSTRAINT
- [ ] Broader markets / asset classes / independent return drivers — LONG-TERM ONLY; must not delay OKX-core proof

No sizing or capital-allocation change is authorized during STOP LOSING causal work.

## H. Decision gate

1. Preserve the frozen RSI-after-BBRANGE ownership candidate and semantics.
2. `CURRENT_STAGE=VPS_PAPER_INDEPENDENT_REPLICATION_IN_PROGRESS`.
3. Allow acceptance to run naturally and use read-only status checks.
4. Review `BAD_AVOIDED` versus `GOOD_MISSED` when `MATURE_240M >= 5`, or earlier if good-missed risk becomes concerning.
5. Treat `MATURE_240M >= 5` as first review, not proof or automatic LIVE eligibility; 5/5 alone cannot authorize LIVE.
6. Use VPS PAPER only for independent acceptance, not discovery or semantic expansion.
7. Keep `LIVE_ELIGIBILITY=NOT_COMPLETE` until VPS PAPER replication passes and positive expected-net plus all risk/authority gates pass.
8. Keep movement capacity at `NEEDS_MORE_RESEARCH`; no second causal treatment.

## I. Permanent long-run experiment gate

For every experiment longer than 30 minutes, require before launch:

- [x] `TERMINAL_CONDITION_CATALOG_COMPLETE=YES`
- [x] `RECENT_LOG_REPLAY_AGAINST_TERMINAL_CATALOG=PASS`
- [x] `KNOWN_BACKGROUND_ERRORS_CLASSIFIED=YES`
- [x] `TASK_ATTRIBUTION_RULES_TESTED=YES`
- [x] `GENUINE_DANGEROUS_CONDITIONS_FAIL_CLOSED=YES`
- [x] `RESTORE_PATH=PASS`
- [x] `MANUAL_READ_ONLY_STATUS_PATH=PASS`

Do not fix only the most recently observed error and restart blindly. Re-run this complete gate for each new long-running experiment context.

For every material producer/consumer pipeline before independent acceptance:

- [x] `FUNCTIONAL_CORRECTNESS=PASS`
- [x] `EFFECTIVE_SERVICE_RATE > OBSERVED_SOURCE_ARRIVAL_RATE`
- [x] after each completed consumer cycle, every eligible source event through the cycle-start cutoff has exactly one canonical observation
- [x] canonical watermark reaches the cycle-start cutoff with no eligible gaps

Healthy workers, fresh heartbeats, zero DB blockers, and individual successful
inserts alone are not sufficient forward-health proof.

## J. Permanent research trial discipline

Before final evaluation, every material candidate cycle records:

- [ ] `HYPOTHESIS_ID`
- [ ] `ECONOMIC_MECHANISM`
- [ ] `PRIMARY_METRIC` and `SECONDARY_METRICS`
- [ ] `CANDIDATE_FAMILY_COUNT`
- [ ] `PARAMETER_OR_RULE_VARIANTS_TRIED`
- [ ] `DEVELOPMENT_WINDOW` and `HOLDOUT_WINDOW`
- [ ] `CANDIDATE_FREEZE_TIMESTAMP_OR_STATE`
- [ ] `STOP_RULE` and `GOOD_MISSED_RULE`
- [ ] `RESULT=ACCEPTED|NEEDS_MORE_RESEARCH|REJECTED`

- [x] Methodology invariant only; no new DB table or application module required
- [x] Inspected holdout is burned for further tuning of the same candidate family
- [x] Holdout-driven redesign requires new natural data or another genuinely untouched set
- [x] Candidate/threshold/parameter alternative count must be reported
- [x] Best historical result is not treated as unbiased evidence
- [ ] For sufficiently broad future searches, consider Deflated Sharpe Ratio, Probability of Backtest Overfitting, or equivalent justified diagnostics
- [x] Multiple-testing diagnostics are not mechanically required for every small causal experiment

## K. Separate success ladders

Research quality:
`UNKNOWN → MEASURED → BAD MECHANISM IDENTIFIED → CANDIDATE → LOCAL CAUSAL EVIDENCE → FROZEN → VPS INDEPENDENT VALIDATION → QUALIFIED COHORT`.

Capital / LIVE:
`NO LIVE AUTHORITY → QUALIFIED POSITIVE EXPECTED-NET COHORT → BOUNDED LIVE ELIGIBILITY → POSITIVE DAYS → POSITIVE WEEKS → POSITIVE MONTHS → BETTER DRAWDOWN → BETTER CAPITAL EFFICIENCY`.

Total PAPER PnL may remain negative while the research universe remains broad.
The LIVE equity curve is the ultimate capital KPI.

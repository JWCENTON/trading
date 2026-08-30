# WalTrade Master Execution Roadmap

CURRENT_PHASE=STOP_LOSING

VPS_PAPER_RUNTIME_SHA=d60c4517892c220b6450876c47f27d99e8bf4dc8

OWNERSHIP_CANDIDATE_INTRODUCED_SHA=d60c4517892c220b6450876c47f27d99e8bf4dc8

Execution order:

`DATA → MECHANISM → MINIMUM CHANGE → LOCAL PAPER PROOF → VPS PAPER REPLICATION → LIVE LAST`

The North Star is frozen in [WALTRADE_CONSTITUTION.md](WALTRADE_CONSTITUTION.md). This roadmap is the living execution plan and records current verified truth rather than preserving superseded blockers.

## 1. Foundation state

| Foundation | Current verified state |
| --- | --- |
| PAPER entry atomicity | `PAPER_ENTRY_ATOMICITY=PASS_2_2` |
| Risk Budget immutable-event contract | `RISK_BUDGET_CONTRACT_PARITY_4_4=PASS` |
| Risk Budget promoted commit | `f965a0b35f8be1b900cbd0e73332c653b003ca0a` |
| Risk Budget influence | `OFF` |
| Learning auto-apply | `OFF` |
| Fee V2 | `0.35% per side` |
| Full roundtrip break-even movement | `~0.7024586051%` |
| LOCAL LIVE capital preservation | `ACTIVE`; new entries `NO`; exit/close `YES` |
| VPS LIVE capital preservation | `ACTIVE`; new entries `NO`; exit/close `YES` |
| LOCAL full-opportunity throughput fix | `PASS`; exact projection indexes |

PAPER entry order/commitment, canonical ENTRY fill, position linkage, and frozen Fee V2 contract are atomic across all four strategies. Missing or conflicting frozen ENTRY fee evidence fails closed before PAPER exit intent. The Risk Budget STATE_EVALUATION immutable-event contract is deployed with stable semantic equality, frozen upstream evidence, provenance normalization, idempotent same-cutoff retry, and true-conflict fail-closed behavior.

Git SHA alone is insufficient: relevant rollouts require Git, contract, direct schema dependency, and runtime/semantic parity.

### LOCAL / VPS governance

LOCAL and VPS consume the same promoted shared software contract, but they are
operationally isolated and retain independent runtime and data histories.
LOCAL is the only implementation and Git promotion authority: implementation,
tests, commits, pushes, and canonical documentation updates originate on
LOCAL. VPS is strictly pull-only. It may `git fetch`, `git pull --ff-only`,
deploy promoted artifacts, and validate them independently. VPS must never
commit, push, create VPS-only shared-code fixes or schema magic, or change
frozen candidate semantics.

Canonical promotion path:

`LOCAL → GitHub → VPS pull --ff-only → independent validation`

For corresponding environments—`LOCAL PAPER ↔ VPS PAPER` and, where
applicable, `LOCAL LIVE ↔ VPS LIVE`—the promoted shared contract requires
`GIT_PARITY`, `CONTRACT_PARITY`, `DIRECT_SCHEMA_DEPENDENCY_PARITY`,
`SCHEMA_OBJECT_PARITY`, `RUNTIME_SEMANTIC_PARITY`, and
`CONFIG_CONTRACT_PARITY` where shared and applicable. The rollout gate is:

`LOCAL validation → GitHub promotion → VPS pull --ff-only → GIT_PARITY → CONTRACT_PARITY → DIRECT_SCHEMA_DEPENDENCY_PARITY → SCHEMA_OBJECT_PARITY → RUNTIME_SEMANTIC_PARITY`

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
- `OWNERSHIP_CANDIDATE=FROZEN_UNCHANGED`.
- `FUNCTIONAL_CORRECTNESS=PASS`.
- `FULL_PAPER_OPPORTUNITY_OBSERVATION_HEALTH=PASS`.
- `FORWARD_CANONICAL_FRESHNESS=PASS`.
- `CUTOFF_MISSING_LOGICAL_KEYS=0`; missing 1m keys `0`; missing 5m keys `0`.
- `NOT_EXACTLY_ONE_KEYS=0`; duplicate observation keys `0`; duplicate causal event IDs `0`.
- `ELIGIBLE_UNPROCESSED_THROUGH_CUTOFF=0`.
- `DIRECT_SCHEMA_DEPENDENCY_PARITY=PASS`.
- `ROWS_PER_MINUTE_EFFECTIVE=25.400`; `FORWARD_SOURCE_ARRIVAL_RATE=25.300`.
- `SERVICE_RATE_ABOVE_ARRIVAL=YES`; headroom `+0.100_ROWS_PER_MINUTE`.
- `THROUGHPUT_HEADROOM_MONITORING_REQUIRED=YES` because the passing margin is narrow.

The forward-health contract is PASS and VPS PAPER ownership acceptance has
started. The narrow positive service-rate margin is a monitoring requirement,
not a failure. No queue change is authorized.

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

Existing pre-entry evidence predicts movement capacity strongly, led by
`ATR_PCT`, BBRANGE `BB_WIDTH_PCT`, and absolute EMA slope. It separates final
non-value-add admissions only weakly. For 923 admissions made while
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

Ordered economic work after the current frozen ownership acceptance reaches
its decision gate:

1. `ECONOMIC_FLOOR_AFTER_COST_COVER_V1` — exit-only causal research
2. `MOVEMENT_CAPACITY` — context-specific research on new, untouched data
3. `ECONOMIC_NO_TRADE / NEW_RISK_VS_KEEP_EXISTING_RISK`
4. `SAME_THESIS_CONTROL`
5. `FEE_VELOCITY`
6. `1m/5m MIXED_DUPLICATION`

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

Future exit candidate: `ECONOMIC_FLOOR_AFTER_COST_COVER_V1`.

Its intended semantics are upside-open with downside protection only after full economic cost has been covered. It must remain a separate exit-only experiment and must not be mixed with an entry treatment.

## 7. Current causal work

The frozen ownership candidate has one unchanged rule:

> If an RSI admission candidate has a same-symbol OPEN BBRANGE position with positive remaining inventory, block only that additional RSI PAPER admission.

Reason: `STOP_LOSING_OWNERSHIP_RSI_AFTER_BBRANGE`.

Sequence: `CONTROL → TREATMENT → CONTROL`, with an optional second treatment only if useful exposure justifies it. Primary outcome: `BAD_AVOIDED` versus `GOOD_MISSED`. Fewer trades alone is not success. Counterfactual observation must mature through 240-minute MFE, MAE, full-cost cover, time-to-cost-cover, and economic viability.

Current verified state:

- `LOCAL_OWNERSHIP_EXPERIMENT=TERMINAL`
- `VPS_OWNERSHIP_ACCEPTANCE=IN_PROGRESS`
- `OWNERSHIP_TREATMENT_MODE=TREATMENT`
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
`OWNERSHIP_STATUS=PROMISING_INITIAL_CAUSAL_EVIDENCE_NOT_PROVEN`, not proof of
a final ownership policy. `LOCAL_DISCOVERY=COMPLETE_FOR_CANDIDATE_FREEZE` and
the unchanged `RSI_AFTER_BBRANGE_OWNERSHIP_V1` candidate is `FROZEN`.
Independent VPS PAPER acceptance started at
`2026-08-29T07:36:49.339989Z` in `TREATMENT` mode. Independent VPS evidence is
now `AFFECTED_RSI_AFTER_BBRANGE=3`, `BLOCKED_RSI_ENTRIES=3`, `MATURE_240M=3`,
`PENDING_240M=0`, `BAD_AVOIDED=3`, and `GOOD_MISSED=0`.
`BAD_AVOIDED_RATE=100.00_PERCENT`, `GOOD_MISSED_RATE=0.00_PERCENT`,
`COUNTERFACTUAL_FULL_COST_COVER=0/3`, and
`AVOIDED_FEES=0.126000_USDC_ROUNDTRIP_EQUIVALENT`. The first qualifying
evidence arrived at `2026-08-29T15:07:10.579096Z`; no updated timestamp for the
latest qualifying observation is asserted here. PAPER runtime, Full PAPER
Opportunity Observation, and forward canonical freshness are PASS.

This VPS result is promising, not proof. It remains independent acceptance
evidence and must not be pooled with the LOCAL 4/4 discovery evidence as one
formal statistical sample. `FIRST_FORMAL_REVIEW_WHEN=MATURE_240M>=5`;
`FORMAL_REVIEW_GATE_REACHED=NO` and
`ACCEPTANCE_DECISION=WAIT_FOR_MORE_MATURE_EVIDENCE`. The gate is not reached,
`LIVE_ELIGIBILITY=NOT_COMPLETE`, no second treatment is authorized, and LIVE
Capital Preservation remains active.

`OWNERSHIP_ACCEPTANCE_STARTED=YES`;
`OWNERSHIP_TREATMENT_MODE=TREATMENT`;
`OWNERSHIP_CANDIDATE=RSI_AFTER_BBRANGE_OWNERSHIP_V1`.

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
`CAUSAL_ELIGIBILITY=NEEDS_MORE_RESEARCH`. No second treatment is authorized.

`GLOBAL_MOVEMENT_GATE_READY=NO`; `CURRENT_HOLDOUT_BURNED=YES`. The 455-trade
holdout is burned for further tuning of this candidate family. Any redesigned
or context-specific ATR/realtime hypothesis must use new natural data or
another genuinely untouched validation set. `NEXT_MOVEMENT_RESEARCH` is
`CONTEXT_SPECIFIC_ON_NEW_DATA_ONLY`.

## 8. Permanent research and LIVE authority gates

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

## 9. Ordered execution plan

### Now

1. Allow VPS PAPER ownership acceptance to run naturally.
2. Perform read-only status checks only.
3. Preserve the frozen RSI-after-BBRANGE candidate and its exact semantics.
4. Monitor forward canonical freshness and the narrow throughput headroom.
5. Wait for mature VPS blocked opportunities and review `BAD_AVOIDED` versus `GOOD_MISSED` first.

### Next

1. `CURRENT_STAGE=VPS_PAPER_INDEPENDENT_REPLICATION_IN_PROGRESS`.
2. Perform the first formal economic review when `MATURE_240M >= 5`, unless `GOOD_MISSED` becomes materially concerning earlier.
3. Do not impose an arbitrary N=20 or N=30 requirement.
4. Require independent VPS PAPER causal acceptance before any LIVE decision.
5. Keep `LIVE_ELIGIBILITY=NOT_COMPLETE` until replication and its safety/economic gates pass.
6. Keep movement capacity at `NEEDS_MORE_RESEARCH`; `SECOND_CAUSAL_TREATMENT_AUTHORIZED=NO`.
7. After the ownership decision gate, prioritize `ECONOMIC_FLOOR_AFTER_COST_COVER_V1` as separate exit-only causal research.
8. Then perform context-specific movement-capacity research on new, untouched data only.
9. Then evaluate economic no-trade / new-risk-versus-keep, same-thesis control, fee velocity, and finally 1m/5m semantic duplication.

The economic-floor priority follows a proven secondary leak: 115
tiny-positive-to-final-loss cases, of which 112 ended through
`PROFIT_LOCK_TRAIL_DROP`. Future research may test whether a bounded economic
floor, armed only after authoritative full cost is covered, reduces giveback
while keeping winner upside open. This does not authorize a final exit rule,
fixed take-profit, immediate tiny-positive exit, or `TIME_EXIT` as an economic
exit. Entry and exit treatments must not be mixed, and the exit candidate must
not start before current ownership acceptance reaches its decision gate.

### Only after economic proof

- Risk Contribution policy
- volatility targeting
- Risk Budget numeric policy
- drawdown modes and dynamic risk reduction
- Capital Allocation
- edge-dependent or portfolio-impact sizing
- limited Learning authority
- bounded LIVE eligibility with Product Owner approval

## 10. Permanent long-run experiment standard

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

## 11. Hard scope control

Do not add a new engine, brain, strategy family, ML classifier, portfolio framework, parameter sweep, DCA, campaign, averaging down, hold-until-green policy, global 1m/5m suppression, or blind one-strategy-per-symbol rule. Do not enable Risk Budget influence, Capital Allocation, or Learning auto-apply. Do not run discovery experiments on LIVE or use VPS PAPER to discover rather than independently accept a LOCAL candidate.

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

## 12. Success ladders

Research quality ladder:

`UNKNOWN → MEASURED → BAD MECHANISM IDENTIFIED → CANDIDATE → LOCAL CAUSAL EVIDENCE → FROZEN → VPS INDEPENDENT VALIDATION → QUALIFIED COHORT`

Capital / LIVE ladder:

`NO LIVE AUTHORITY → QUALIFIED POSITIVE EXPECTED-NET COHORT → BOUNDED LIVE ELIGIBILITY → POSITIVE DAYS → POSITIVE WEEKS → POSITIVE MONTHS → BETTER DRAWDOWN → BETTER CAPITAL EFFICIENCY`

Total PAPER PnL may remain negative while its research universe remains broad.
Capital Allocation follows economic proof; it does not create it. The LIVE
equity curve remains the ultimate capital KPI.

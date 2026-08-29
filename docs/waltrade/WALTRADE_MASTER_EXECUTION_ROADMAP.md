# WalTrade Master Execution Roadmap

CURRENT_PHASE=STOP_LOSING

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

## 3. Ranked STOP LOSING mechanisms

Ranked next mechanisms, separate from the already-frozen ownership candidate:

1. `MOVEMENT_CAPACITY`
2. `ECONOMIC_NO_TRADE`
3. `NEW_RISK_VS_KEEP_EXISTING_RISK`
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

- `CURRENT_OWNERSHIP_EXPERIMENT=TERMINAL`
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

All four preserved treatment counterfactuals have matured:
`MATURE_240M=4`, `PENDING_240M=0`, `BAD_AVOIDED=4`, and `GOOD_MISSED=0`.
This is
`OWNERSHIP_STATUS=PROMISING_INITIAL_CAUSAL_EVIDENCE_NOT_PROVEN`, not proof of
a final ownership policy. `LOCAL_DISCOVERY=COMPLETE_FOR_CANDIDATE_FREEZE` and
the unchanged `RSI_AFTER_BBRANGE_OWNERSHIP_V1` candidate is `FROZEN`.
Independent VPS PAPER acceptance started at
`2026-08-29T07:36:49.339989Z` in `TREATMENT` mode. Initial counters are
`AFFECTED_RSI_AFTER_BBRANGE=0`, `BLOCKED_RSI_ENTRIES=0`, `MATURE_240M=0`,
`PENDING_240M=0`, `BAD_AVOIDED=0`, and `GOOD_MISSED=0`. Zero initial exposure
does not imply economic success. `LIVE_ELIGIBILITY=NOT_COMPLETE`.

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

## 8. Ordered execution plan

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

### Only after economic proof

- Risk Contribution policy
- volatility targeting
- Risk Budget numeric policy
- drawdown modes and dynamic risk reduction
- Capital Allocation
- edge-dependent or portfolio-impact sizing
- limited Learning authority
- bounded LIVE eligibility with Product Owner approval

## 9. Permanent long-run experiment standard

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

## 10. Hard scope control

Do not add a new engine, brain, strategy family, ML classifier, portfolio framework, parameter sweep, DCA, campaign, averaging down, hold-until-green policy, global 1m/5m suppression, or blind one-strategy-per-symbol rule. Do not enable Risk Budget influence, Capital Allocation, or Learning auto-apply. Do not run discovery experiments on LIVE or use VPS PAPER to discover rather than independently accept a LOCAL candidate.

## 11. Success ladder

`CURRENT LARGE NEGATIVE → SMALLER NEGATIVE → NET ≈ 0 → +0.01 CAUSAL/REPEATABLE → POSITIVE DAYS → POSITIVE WEEKS → POSITIVE MONTHS → BETTER EXPECTANCY → BETTER DRAWDOWN → BETTER CAPITAL EFFICIENCY`

Capital Allocation follows economic proof; it does not create it.

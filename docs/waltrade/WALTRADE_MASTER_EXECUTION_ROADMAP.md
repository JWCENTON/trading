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

PAPER entry order/commitment, canonical ENTRY fill, position linkage, and frozen Fee V2 contract are atomic across all four strategies. Missing or conflicting frozen ENTRY fee evidence fails closed before PAPER exit intent. The Risk Budget STATE_EVALUATION immutable-event contract is deployed with stable semantic equality, frozen upstream evidence, provenance normalization, idempotent same-cutoff retry, and true-conflict fail-closed behavior.

Git SHA alone is insufficient: relevant rollouts require Git, contract, direct schema dependency, and runtime/semantic parity.

### Equity UI canonical read authority

LOCAL validation established `CANONICAL_EQUITY_AUTHORITY=HEALTHY`,
`UI_API_READ_AUTHORITY=LEGACY_DEFECT`, and `FRONTEND=VALID`. The minimum
shared fix is `FIX_CLASS=REWIRE_EXISTING_AUTHORITY`: LIVE `/ui/equity` reads
the accepted managed-capital baseline plus canonical managed-equity history,
without a new schema, engine, endpoint, frontend model, or data repair.

LOCAL LIVE validation is PASS. The change is not complete until it is promoted
to GitHub, rolled out to VPS LIVE, and Git/contract/direct-schema/runtime
semantic parity is verified. PAPER does not consume the LIVE managed-capital
authority and must not be rolled out for this change.

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

## 3. Ranked STOP LOSING mechanisms

Current strongest mechanisms:

1. `INSUFFICIENT_MOVEMENT_VS_COST`
2. `STRATEGY_PAIR_OWNERSHIP`
3. `NEW_RISK_VS_KEEP_EXISTING_RISK`
4. `HIGH_FEE_VELOCITY / SAME_THESIS_DUPLICATION`
5. `1m/5m MIXED DUPLICATION`

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

The active LOCAL PAPER experiment tests one unchanged rule:

> If an RSI admission candidate has a same-symbol OPEN BBRANGE position with positive remaining inventory, block only that additional RSI PAPER admission.

Reason: `STOP_LOSING_OWNERSHIP_RSI_AFTER_BBRANGE`.

Sequence: `CONTROL → TREATMENT → CONTROL`, with an optional second treatment only if useful exposure justifies it. Primary outcome: `BAD_AVOIDED` versus `GOOD_MISSED`. Fewer trades alone is not success. Counterfactual observation must mature through 240-minute MFE, MAE, full-cost cover, time-to-cost-cover, and economic viability.

Current verified state:

- `PREVIOUS_RUN=TERMINAL_FAIL_FROM_HARNESS_FALSE_POSITIVE`
- `ROOT_CAUSE=EXPECTED_POSTGRESQL_CLIENT_DISCONNECT_MISCLASSIFIED_AS_CRITICAL`
- `HARNESS_TERMINAL_CLASSIFIER=FIXED_AND_VALIDATED`
- `TERMINAL_CONDITION_PREFLIGHT=PASS`
- `CURRENT_OWNERSHIP_EXPERIMENT=IN_PROGRESS`
- `ECONOMIC_VERDICT=NOT_YET_PROVEN`
- `EXISTING_COUNTERFACTUALS=4_PENDING_240M`

The prior run's PostgreSQL event was `POSTGRESQL_FATAL_CLIENT_CONNECTION_LOST: connection to client lost` during an experiment-controlled bot-runner recreation. It interrupted a read-only BTCUSDC 1m candle query and had `TRADING_IMPACT=NONE`. It was not related to treatment semantics, ownership code, PAPER entry atomicity, Risk Budget, or Full PAPER Opportunity Observation, and it was not a shared WalTrade runtime defect.

The LOCAL experiment-only classifier now treats that narrowly attributed, bounded, read-only recreation disconnect as `WARNING / CONTINUE`. Genuine dangerous or unknown plausible critical conditions remain fail-closed. Validation: `TESTS=32_PASS`, `CLASSIFIER_TESTS=18_PASS`, `ATTRIBUTED_DB_SAFETY_TESTS=14_PASS`, `TERMINAL_CONDITION_CATALOG_COMPLETE=YES`, `RECENT_LOG_REPLAY_AGAINST_TERMINAL_CATALOG=PASS`, `FALSE_POSITIVE_TERMINAL_EVENTS=0`, and `UNKNOWN_TERMINAL_CLASSIFICATIONS=0`. No runtime, schema, or trading-semantic change was required; no VPS rollout is required.

The four valid blocked opportunities from the prior treatment remain preserved: `MATURE_240M=0`, `PENDING_240M=4`, `BAD_AVOIDED=0`, and `GOOD_MISSED=0`. Their maturity continues independently after the prior experiment termination. Do not classify them before their 240-minute evidence completes.

The current restart is healthy and active in CONTROL run `CONTROL-118a765af6`. Its independent runner/watchdog and read-only status path must remain undisturbed; its economic result must not be pre-judged.

## 8. Ordered execution plan

### Now

1. Allow the current ownership experiment to run.
2. Perform manual read-only checks every 30–60 minutes.
3. Allow the existing four counterfactuals to mature.
4. Collect additional causal treatment exposure.
5. Evaluate `BAD_AVOIDED` versus `GOOD_MISSED`.
6. Combine LOCAL causal evidence with VPS forensic evidence.
7. Rank mechanisms.
8. Select exactly one next intervention.

### Next

1. If ownership is promising with sufficient maturity, freeze a candidate and replicate it on VPS PAPER.
2. If ownership exposure is insufficient, repeat the same semantics later; do not broaden it.
3. After ownership, consider the small canonical regime-source rewire, then continue 1m/5m counterfactual work.
4. Run `ECONOMIC_FLOOR_AFTER_COST_COVER_V1` only as a separate future exit experiment.

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

## 10. Hard scope control

Do not add a new engine, brain, strategy family, ML classifier, portfolio framework, parameter sweep, DCA, campaign, averaging down, hold-until-green policy, global 1m/5m suppression, or blind one-strategy-per-symbol rule. Do not enable Risk Budget influence, Capital Allocation, or Learning auto-apply. Do not run discovery experiments on LIVE or use VPS PAPER to discover rather than independently accept a LOCAL candidate.

## 11. Success ladder

`CURRENT LARGE NEGATIVE → SMALLER NEGATIVE → NET ≈ 0 → +0.01 CAUSAL/REPEATABLE → POSITIVE DAYS → POSITIVE WEEKS → POSITIVE MONTHS → BETTER EXPECTANCY → BETTER DRAWDOWN → BETTER CAPITAL EFFICIENCY`

Capital Allocation follows economic proof; it does not create it.

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
- [ ] VPS PAPER Full Opportunity throughput validation — NOT COMPLETE / ACCEPTANCE BLOCKED

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
- [ ] VPS ownership treatment replication — NOT COMPLETE

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
- [x] `MATURE_240M=4`; `BAD_AVOIDED=4`; `GOOD_MISSED=0` — COMPLETE
- [x] `BAD_AVOIDED > GOOD_MISSED` — PROMISING INITIAL CAUSAL EVIDENCE
- [x] Longer LOCAL discovery wait — NOT REQUIRED: `NO_ARBITRARY_SAMPLE_WAIT`
- [x] `RSI_AFTER_BBRANGE_OWNERSHIP_V1` candidate freeze — COMPLETE
- [ ] VPS PAPER treatment replication — NOT COMPLETE
- [ ] LIVE eligibility — NOT COMPLETE

Treatment semantics remain one variable: block only an RSI PAPER admission when the same symbol already has an OPEN BBRANGE position with positive remaining inventory. Do not infer success from fewer trades alone.

The latest restart terminated on a real blocked-session condition and correctly
failed closed. The diagnostic gap is closed without weakening genuine DB
protection or changing trading semantics. Four of four mature blocked
opportunities were `BAD_AVOIDED`, with zero `GOOD_MISSED`; this is promising
initial causal evidence, not proof of a final ownership policy.

`OWNERSHIP_STATUS=PROMISING_INITIAL_CAUSAL_EVIDENCE_NOT_PROVEN`;
`LOCAL_DISCOVERY=COMPLETE_FOR_CANDIDATE_FREEZE`;
`OWNERSHIP_CANDIDATE=FROZEN`; `NEXT_STAGE=VPS_PAPER_INDEPENDENT_REPLICATION`;
`LIVE_ELIGIBILITY=NOT_COMPLETE`. VPS PAPER replication has not started.

The frozen candidate is unchanged. VPS PAPER acceptance remains
`BLOCKED_UNTIL_VPS_THROUGHPUT_VALIDATION`; no queue-lane architecture is
authorized while the proven exact-index optimization remains sufficient.

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
- [ ] `ECONOMIC_FLOOR_AFTER_COST_COVER_V1` design — NOT COMPLETE
- [ ] Exit-only LOCAL PAPER treatment — NOT COMPLETE
- [ ] VPS PAPER exit replication — NOT COMPLETE

Entry and exit treatments must not be mixed.

## G. Future risk and allocation work

- [ ] Risk Contribution — NOT COMPLETE
- [ ] Volatility targeting — NOT COMPLETE
- [ ] Risk Budget numeric policy — NOT COMPLETE
- [ ] Drawdown modes and dynamic risk reduction — NOT COMPLETE
- [ ] Capital Allocation — NOT COMPLETE
- [ ] Edge-dependent sizing — NOT COMPLETE
- [ ] Portfolio-impact and correlation-aware sizing — NOT COMPLETE
- [ ] Limited Learning authority — NOT COMPLETE

No sizing or capital-allocation change is authorized during STOP LOSING causal work.

## H. Decision gate

1. Preserve the frozen RSI-after-BBRANGE ownership candidate and semantics.
2. `NEXT_STAGE=VPS_PAPER_INDEPENDENT_REPLICATION`.
3. Use VPS PAPER only for independent acceptance, not discovery or semantic expansion.
4. Keep `LIVE_ELIGIBILITY=NOT_COMPLETE` until VPS PAPER replication passes.

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

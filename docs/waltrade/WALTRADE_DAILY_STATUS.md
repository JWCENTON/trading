# WalTrade Daily Status

LAST_UPDATED=2026-08-29

CURRENT_MAIN_SHA=62fe7d79993cf83040ad1724e2392be440c1d02f

CURRENT_VPS_PAPER_RUNTIME_SHA=d60c4517892c220b6450876c47f27d99e8bf4dc8

OWNERSHIP_CANDIDATE_INTRODUCED_SHA=d60c4517892c220b6450876c47f27d99e8bf4dc8

CURRENT_PHASE=STOP_LOSING

CURRENT_P0=VPS_PAPER_INDEPENDENT_REPLICATION_OF_FROZEN_RSI_AFTER_BBRANGE_OWNERSHIP_V1

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
| Strategy Ownership | `PROMISING_INITIAL_CAUSAL_EVIDENCE_NOT_PROVEN`; candidate `FROZEN` |
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
| VPS PAPER | Ownership independent acceptance `IN_PROGRESS`; frozen candidate in `TREATMENT` mode |
| Full Opportunity throughput | VPS functional correctness and forward canonical freshness PASS; narrow headroom monitored |
| LOCAL LIVE | Capital Preservation Mode `ACTIVE`; new entries `NO`; exit/close `YES` |
| Equity UI canonical authority | `VPS_LIVE_COMPLETE`; Git, contract, direct-schema, and runtime-semantic parity PASS |
| VPS LIVE | Capital Preservation Mode `ACTIVE`; new entries `NO`; exit/close `YES` |
| Economic Proof | Stable positive net expectancy after costs is NOT PROVEN |
| Capital Allocation | NOT NOW; requires economic and Risk Budget policy proof |

## Authority and research methodology

- LOCAL and VPS share promoted contracts but have independent runtime and data histories.
- `LOCAL_GIT_PROMOTION_AUTHORITY=ONLY`; implementation, tests, commits, pushes, and canonical doc updates originate on LOCAL.
- `VPS_GIT_AUTHORITY=PULL_ONLY`; VPS may fetch, pull `--ff-only`, deploy, and validate, but never commit, push, patch shared code/schema locally, or change frozen semantics.
- Promotion path: `LOCAL → GitHub → VPS pull --ff-only → independent validation`.
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

## Active experiment

`LOCAL_OWNERSHIP_EXPERIMENT=TERMINAL`;
`VPS_OWNERSHIP_ACCEPTANCE=IN_PROGRESS`;
`OWNERSHIP_ACCEPTANCE_STARTED=YES`;
`OWNERSHIP_TREATMENT_MODE=TREATMENT`;
`OWNERSHIP_CANDIDATE=RSI_AFTER_BBRANGE_OWNERSHIP_V1`.

| Field | Current artifact value |
| --- | --- |
| Environment | VPS PAPER |
| Candidate | `RSI_AFTER_BBRANGE_OWNERSHIP_V1` |
| Treatment mode | `TREATMENT` |
| Status | `IN_PROGRESS` |
| Acceptance started | `YES` |
| Started at | `2026-08-29T07:36:49.339989Z` |
| Affected RSI-after-BBRANGE | 0 |
| Blocked RSI entries | 0 |
| Mature / pending 240m | 0 / 0 |
| Bad avoided / good missed | 0 / 0 |
| Economic verdict | INSUFFICIENT EXPOSURE; zero initial exposure is not success |

Do not hardcode this phase elsewhere. Update this current-truth file from the experiment artifact when the series changes phase or becomes terminal.

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
- `MATURE_240M=4`; `PENDING_240M=0`; `BAD_AVOIDED=4`; `GOOD_MISSED=0`.
- `OWNERSHIP_ECONOMIC_VERDICT=PROMISING_INITIAL_CAUSAL_EVIDENCE`; ownership is not proven.
- `OWNERSHIP_STATUS=PROMISING_INITIAL_CAUSAL_EVIDENCE_NOT_PROVEN`.
- `LOCAL_DISCOVERY=COMPLETE_FOR_CANDIDATE_FREEZE`.
- `OWNERSHIP_CANDIDATE=FROZEN` (`RSI_AFTER_BBRANGE_OWNERSHIP_V1`).
- `LIVE_ELIGIBILITY=NOT_COMPLETE`.

## Capital preservation

- `LOCAL_LIVE_CAPITAL_PRESERVATION_MODE=ACTIVE`.
- `VPS_LIVE_CAPITAL_PRESERVATION_MODE=ACTIVE`.
- `NEW_LIVE_ENTRY_ALLOWED=NO`.
- `EXIT_CLOSE_CAPABILITY_AVAILABLE=YES`.

## Full PAPER Opportunity Observation throughput

- `VPS_PAPER_DIRECT_SCHEMA_DEPENDENCY_REPAIR=COMPLETE`.
- `ROOT_CAUSE=EXPENSIVE_PROJECTION_LOOKUPS_PLUS_FIFO_SINGLE_CONSUMER`.
- `LOCAL_THROUGHPUT_FIX=PASS`.
- The exact projection lookup indexes preserve FIFO, observation identity, Fee V2 evidence, and 240-minute outcome linkage.
- `OWNERSHIP_CANDIDATE=FROZEN_UNCHANGED`.
- `FUNCTIONAL_CORRECTNESS=PASS`.
- `FULL_PAPER_OPPORTUNITY_OBSERVATION_HEALTH=PASS`.
- `FORWARD_CANONICAL_FRESHNESS=PASS`.
- Cutoff missing logical, 1m, and 5m keys: `0 / 0 / 0`.
- Not-exactly-one keys, duplicate observation keys, and duplicate causal event IDs: `0 / 0 / 0`.
- `ELIGIBLE_UNPROCESSED_THROUGH_CUTOFF=0`.
- `DIRECT_SCHEMA_DEPENDENCY_PARITY=PASS`.
- `ROWS_PER_MINUTE_EFFECTIVE=25.400`; `FORWARD_SOURCE_ARRIVAL_RATE=25.300`.
- `SERVICE_RATE_ABOVE_ARRIVAL=YES`; headroom `+0.100_ROWS_PER_MINUTE`.
- `THROUGHPUT_HEADROOM_MONITORING_REQUIRED=YES`; narrow positive headroom is not a failure.
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

## NOW

- Allow VPS PAPER ownership acceptance to run naturally.
- Perform read-only status checks only.
- Preserve the frozen ownership candidate and its unchanged semantics.
- Monitor forward freshness and throughput headroom.
- Wait for mature VPS blocked opportunities and review `BAD_AVOIDED` versus `GOOD_MISSED` first.

## NEXT

- `CURRENT_STAGE=VPS_PAPER_INDEPENDENT_REPLICATION_IN_PROGRESS`.
- First formal economic review at `MATURE_240M >= 5`, unless `GOOD_MISSED` becomes materially concerning earlier.
- `MATURE_240M >= 5` is first review, not proof or automatic LIVE eligibility; 5/5 alone cannot authorize LIVE.
- No arbitrary N=20 or N=30 requirement.
- Require independent VPS PAPER acceptance before any LIVE eligibility decision.
- Movement capacity remains `NEEDS_MORE_RESEARCH`; `SECOND_CAUSAL_TREATMENT_AUTHORIZED=NO`.
- Do not start `ECONOMIC_FLOOR_AFTER_COST_COVER_V1` until ownership reaches its decision gate; it remains future exit-only research, not an authorized exit rule.

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

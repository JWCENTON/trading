# WalTrade Daily Status

LAST_UPDATED=2026-08-28

CURRENT_GIT_SHA=d60c4517892c220b6450876c47f27d99e8bf4dc8

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
| VPS PAPER | Independent acceptance / forensic environment; no active treatment authorized by this document |
| Full Opportunity throughput | LOCAL fix PASS; VPS validation still required |
| LOCAL LIVE | Capital Preservation Mode `ACTIVE`; new entries `NO`; exit/close `YES` |
| Equity UI canonical authority | `VPS_LIVE_COMPLETE`; Git, contract, direct-schema, and runtime-semantic parity PASS |
| VPS LIVE | Capital Preservation Mode `ACTIVE`; new entries `NO`; exit/close `YES` |
| Economic Proof | Stable positive net expectancy after costs is NOT PROVEN |
| Capital Allocation | NOT NOW; requires economic and Risk Budget policy proof |

## Active experiment

| Field | Current artifact value |
| --- | --- |
| Environment | LOCAL PAPER |
| Treatment | RSI-after-BBRANGE ownership admission |
| Status | TERMINAL |
| Phase | TERMINAL |
| Run ID | `CONTROL-118a765af6` |
| Completed CONTROL runs | 0 in the current restart |
| Completed TREATMENT runs | 0 |
| Terminal reason | `GLOBAL_DB_SAFETY_FAIL` |
| Safety classification | `CORRECT_REAL_GLOBAL_DB_RISK` |
| Trading impact | NONE |
| Baseline restore | PASS |
| Economic verdict | `PROMISING_INITIAL_CAUSAL_EVIDENCE`; NOT PROVEN |

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
- `VPS_PAPER_FULL_OPPORTUNITY_OBSERVATION_BLOCKER=THROUGHPUT_BELOW_ARRIVAL_RATE`.
- `ROOT_CAUSE=EXPENSIVE_PROJECTION_LOOKUPS_PLUS_FIFO_SINGLE_CONSUMER`.
- `LOCAL_THROUGHPUT_FIX=PASS`.
- The exact projection lookup indexes preserve FIFO, observation identity, Fee V2 evidence, and 240-minute outcome linkage.
- `OWNERSHIP_CANDIDATE=FROZEN_UNCHANGED`.
- `VPS_PAPER_ACCEPTANCE=BLOCKED_UNTIL_VPS_THROUGHPUT_VALIDATION`.
- VPS PAPER acceptance has not started.

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
- No movement treatment is active or authorized. The frozen ownership candidate remains unchanged and first in causal order.
- Ranked next mechanisms: movement capacity; economic no-trade; new risk vs keep; same-thesis control; fee velocity; 1m/5m duplication.

## NOW

- `LOCAL_DISCOVERY_NEXT_ACTION=NO_ARBITRARY_SAMPLE_WAIT`.
- Preserve the frozen ownership candidate and its unchanged semantics.
- Preserve the completed LOCAL evidence and safety artifacts.

## NEXT

- `NEXT_STAGE=VPS_PAPER_INDEPENDENT_REPLICATION`.
- Require independent VPS PAPER acceptance before any LIVE eligibility decision.

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

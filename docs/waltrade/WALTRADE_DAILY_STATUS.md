# WalTrade Daily Status

LAST_UPDATED=2026-08-28

CURRENT_GIT_SHA=2e8880140730ce1848f008b89a6200030f43402c

CURRENT_PHASE=STOP_LOSING

CURRENT_P0=FREEZE_AND_REPLICATE_RSI_AFTER_BBRANGE_OWNERSHIP_ON_VPS_PAPER

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
| Strategy Ownership | `PROMISING_INITIAL_CAUSAL_EVIDENCE`; final policy not proven |
| Movement vs Cost | Primary defect: insufficient movement relative to full cost |
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
| LOCAL LIVE | Non-experimental / frozen unless separately approved |
| Equity UI canonical authority | `VPS_LIVE_COMPLETE`; Git, contract, direct-schema, and runtime-semantic parity PASS |
| VPS LIVE | Production; no discovery experiment |
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
- `LIVE_ELIGIBILITY=NOT_COMPLETE`.

## Current economic baseline

- Fee V2: 0.35% per side.
- Full roundtrip break-even movement: approximately 0.7024586051%.
- Latest VPS PAPER forensic: 1,515 trades, net -183.346579 USDC, fees 212.195019 USDC, insufficient-movement rate 78.2178%.
- Additional admissions: 965; non-value-add admissions: 868 (89.948%); fees 121.513802 USDC; net -122.770951 USDC.

## NOW

- `LOCAL_DISCOVERY_NEXT_ACTION=NO_ARBITRARY_SAMPLE_WAIT`.
- Freeze the same ownership candidate and its unchanged semantics.
- Preserve the completed LOCAL evidence and safety artifacts.

## NEXT

- `NEXT_STAGE=FREEZE_SAME_OWNERSHIP_CANDIDATE_AND_REPLICATE_ON_VPS_PAPER`.
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

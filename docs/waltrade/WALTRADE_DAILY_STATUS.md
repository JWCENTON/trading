# WalTrade Daily Status

LAST_UPDATED=2026-08-28

CURRENT_GIT_SHA=ecc40102f6760c8f1257b4ac4be4ee248fc74171

CURRENT_PHASE=STOP_LOSING

CURRENT_P0=RSI_AFTER_BBRANGE_OWNERSHIP_CAUSAL_EXPERIMENT

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
| Strategy Ownership | Strong ordered-pair evidence; causal policy not yet proven |
| Movement vs Cost | Primary defect: insufficient movement relative to full cost |
| Fee Velocity | High fee velocity / same-thesis duplication is a leading mechanism |
| New Risk vs Keep Risk | Strong diagnostic candidate; policy not yet proven |
| Same-Thesis Duplication | 572 admissions; net -69.028271 USDC |
| 1m/5m | MIXED; no global block authorized |
| Portfolio Crowding | WEAK effect |
| Winner Tail | LOW dependence; top 10% winners offset 1.875% of losses |
| Exit Giveback | Proven secondary leak; immediate tiny-positive exit rejected |
| LOCAL PAPER | HEALTHY / EXPERIMENT ACTIVE |
| Harness Safety | `TERMINAL_CONDITION_PREFLIGHT_PASS`; 32 tests passed; recent-log replay passed |
| VPS PAPER | Independent acceptance / forensic environment; no active treatment authorized by this document |
| LOCAL LIVE | Non-experimental / frozen unless separately approved |
| VPS LIVE | Production; no discovery experiment |
| Economic Proof | Stable positive net expectancy after costs is NOT PROVEN |
| Capital Allocation | NOT NOW; requires economic and Risk Budget policy proof |

## Active experiment

| Field | Current artifact value |
| --- | --- |
| Environment | LOCAL PAPER |
| Treatment | RSI-after-BBRANGE ownership admission |
| Status | IN_PROGRESS |
| Phase | CONTROL |
| Run ID | `CONTROL-118a765af6` |
| Completed CONTROL runs | 0 in the current restart |
| Completed TREATMENT runs | 0 |
| Runner/watchdog | Independent and running |
| Manual checks | Read-only observability enabled |
| Experiment health | PASS |
| DB safety | WARNING_BACKGROUND; no task-attributable or global DB safety failure |
| Last progress | `2026-08-28T12:30:49.978151+00:00` |

Do not hardcode this phase elsewhere. Update this current-truth file from the experiment artifact when the series changes phase or becomes terminal.

## Harness closure and preserved counterfactuals

- Previous run: `TERMINAL_FAIL_FROM_HARNESS_FALSE_POSITIVE`.
- Root cause: an expected PostgreSQL client disconnect during experiment-controlled bot-runner recreation was misclassified as critical.
- Interrupted operation: read-only BTCUSDC 1m candle query.
- Trading impact: NONE; no treatment, ownership, entry-atomicity, Risk Budget, or Full Opportunity Observation defect.
- Harness closure: `TESTS=32_PASS`; terminal-condition catalog and recent real-log replay PASS; zero false-positive terminal events and zero unknown terminal classifications.
- `CURRENT_PENDING_COUNTERFACTUALS=4`; `MATURE_240M=0`; `BAD_AVOIDED=0`; `GOOD_MISSED=0`.
- The four prior valid treatment observations remain preserved and continue maturing independently.

## Current economic baseline

- Fee V2: 0.35% per side.
- Full roundtrip break-even movement: approximately 0.7024586051%.
- Latest VPS PAPER forensic: 1,515 trades, net -183.346579 USDC, fees 212.195019 USDC, insufficient-movement rate 78.2178%.
- Additional admissions: 965; non-value-add admissions: 868 (89.948%); fees 121.513802 USDC; net -122.770951 USDC.

## NOW

- Allow the current ownership experiment to run.
- Use manual read-only observation every 30–60 minutes.
- Wait for additional causal exposure and the four preserved 240-minute outcomes to mature.

## NEXT

- Combine LOCAL causal evidence with VPS independent forensics.
- Rank the strongest mechanisms.
- Select exactly one next intervention.

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

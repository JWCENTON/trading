# External Evidence Catalog

This catalog prevents WalTrade from reinventing established portfolio, risk, and execution concepts. It records known methods, not automatic implementation requirements. Exact external references have not yet been curated in repository documentation; no citation below is fabricated.

## TRANSACTION_COST_AWARENESS

MECHANISM=TRANSACTION_COST_AWARENESS
PROBLEM_SOLVED=Nominally profitable decisions that are negative after fees, spread, slippage, and execution cost.
EXTERNAL_SUPPORT=ESTABLISHED_STANDARD
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=Fee V2, full-cost hurdle, canonical net PnL, cost-cover evidence.
CURRENT_WALTRADE_SUPPORT=YES
OUR_EVIDENCE=0.35% per side; ~0.7024586051% roundtrip hurdle; 78.2178% insufficient-movement rate in the latest VPS PAPER trade-level forensic; separately, 689/1,517 canonical LOCAL opportunities did not reach the hurdle through 240-minute MFE, and only 172/1,517 finished net-positive.
NEXT_ACTION=KEEP

## ECONOMIC_NO_TRADE_REGION

MECHANISM=ECONOMIC_NO_TRADE_REGION
PROBLEM_SOLVED=Avoid changing portfolio state when expected incremental movement cannot cover full cost.
EXTERNAL_SUPPORT=STRONGLY_SUPPORTED
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=No-trade decisions, Full PAPER Opportunity Observation, movement-versus-cost evidence.
CURRENT_WALTRADE_SUPPORT=PARTIAL
OUR_EVIDENCE=Insufficient movement is the primary economic defect. Existing ATR, BBRANGE width, EMA slope, realtime, and momentum evidence predicts 240-minute movement capacity, but the frozen temporal-holdout rule has weak generalization because major strategy/interval behavior is asymmetric. A causal threshold/policy is not proven.
NEXT_ACTION=TEST

## THRESHOLD / BAND REBALANCING

MECHANISM=THRESHOLD / BAND REBALANCING
PROBLEM_SOLVED=Reduce unnecessary turnover by changing allocation only after a meaningful deviation.
EXTERNAL_SUPPORT=ESTABLISHED_STANDARD
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=Potential future evidence-based portfolio-state transition bands.
CURRENT_WALTRADE_SUPPORT=NO
OUR_EVIDENCE=High non-value-add admission and fee rates make the concept relevant; no WalTrade band is proven.
NEXT_ACTION=BACKLOG

## KEEP_EXISTING_RISK_vs_OPEN_NEW_RISK

MECHANISM=KEEP_EXISTING_RISK_vs_OPEN_NEW_RISK
PROBLEM_SOLVED=Distinguish a genuinely new opportunity from paying again for an already-owned thesis.
EXTERNAL_SUPPORT=STRONGLY_SUPPORTED
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=Ordered strategy ownership and current RSI-after-BBRANGE treatment.
CURRENT_WALTRADE_SUPPORT=PARTIAL
OUR_EVIDENCE=868/965 in the existing VPS forensic and 822/923 in the canonical LOCAL movement cohort were non-value-add; ordered strategy-pair asymmetry is material. Pre-entry separation of final non-value-add remains weak.
NEXT_ACTION=TEST

## MARGINAL_PORTFOLIO_CONTRIBUTION

MECHANISM=MARGINAL_PORTFOLIO_CONTRIBUTION
PROBLEM_SOLVED=Evaluate the incremental net value and risk of one additional admission.
EXTERNAL_SUPPORT=ESTABLISHED_STANDARD
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=Marginal Portfolio Value forensic and counterfactual opportunity outcomes.
CURRENT_WALTRADE_SUPPORT=PARTIAL
OUR_EVIDENCE=Non-value-add admissions produced -122.770951 USDC net and 121.513802 USDC fees in the existing forensic. In the canonical LOCAL movement cohort, all 923 additional admissions contributed -103.994083 USDC net; the temporal-holdout movement rule added +6.132816 USDC diagnostically after ownership but missed 11 cost-covering opportunities.
NEXT_ACTION=REUSE

## TURNOVER_CONTROL

MECHANISM=TURNOVER_CONTROL
PROBLEM_SOLVED=Limit fee leakage from excessive portfolio-state changes.
EXTERNAL_SUPPORT=ESTABLISHED_STANDARD
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=Fee Velocity / Thesis Turnover diagnostics.
CURRENT_WALTRADE_SUPPORT=PARTIAL
OUR_EVIDENCE=Same-thesis admissions and interval overlap are materially negative; no general turnover policy is authorized.
NEXT_ACTION=REWIRE

## REDUNDANT_EXPOSURE_CONTROL

MECHANISM=REDUNDANT_EXPOSURE_CONTROL
PROBLEM_SOLVED=Prevent multiple positions from repeatedly expressing substantially the same economic thesis.
EXTERNAL_SUPPORT=STRONGLY_SUPPORTED
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=Strategy-pair ownership, thesis classification, and interval relationship evidence.
CURRENT_WALTRADE_SUPPORT=PARTIAL
OUR_EVIDENCE=572 same-thesis admissions produced -69.028271 USDC net.
NEXT_ACTION=TEST

## MULTI_HORIZON_SIGNAL_AGGREGATION

MECHANISM=MULTI_HORIZON_SIGNAL_AGGREGATION
PROBLEM_SOLVED=Combine or distinguish signals across horizons without treating every interval as an independent bet.
EXTERNAL_SUPPORT=STRONGLY_SUPPORTED
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=1m/5m semantic relationship and multi-period Market Memory.
CURRENT_WALTRADE_SUPPORT=PARTIAL
OUR_EVIDENCE=The 1m/5m relationship is MIXED; both directions are negative, but a global block is unsupported.
NEXT_ACTION=TEST

## RISK_CONTRIBUTION

MECHANISM=RISK_CONTRIBUTION
PROBLEM_SOLVED=Measure how each position, symbol, strategy, interval, or regime contributes to portfolio risk.
EXTERNAL_SUPPORT=ESTABLISHED_STANDARD
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=Future risk contribution evidence feeding Risk Budget.
CURRENT_WALTRADE_SUPPORT=PARTIAL
OUR_EVIDENCE=Canonical risk evidence exists, but numeric portfolio contribution policy is not proven.
NEXT_ACTION=BACKLOG

## RISK_PARITY

MECHANISM=RISK_PARITY
PROBLEM_SOLVED=Allocate by risk contribution rather than equal nominal capital.
EXTERNAL_SUPPORT=ESTABLISHED_STANDARD
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=Possible future Capital Allocation method subordinate to global Risk Budget.
CURRENT_WALTRADE_SUPPORT=NO
OUR_EVIDENCE=Fixed nominal sizing may imply unequal real risk; WalTrade has not proven a risk-parity policy.
NEXT_ACTION=BACKLOG

## VOLATILITY_TARGETING

MECHANISM=VOLATILITY_TARGETING
PROBLEM_SOLVED=Adjust exposure to a target risk level as realized or forecast volatility changes.
EXTERNAL_SUPPORT=ESTABLISHED_STANDARD
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=Potential later ATR/volatility-aware sizing under Risk Budget.
CURRENT_WALTRADE_SUPPORT=NO
OUR_EVIDENCE=Movement, MAE, and cost-cover relationships require evidence before any sizing change.
NEXT_ACTION=BACKLOG

## DRAWDOWN_AWARE_EXPOSURE_CONTROL

MECHANISM=DRAWDOWN_AWARE_EXPOSURE_CONTROL
PROBLEM_SOLVED=Reduce risk during drawdown and protect the system's ability to continue operating.
EXTERNAL_SUPPORT=ESTABLISHED_STANDARD
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=Future NORMAL / DEFENSIVE / RECOVERY / PAUSED Risk Budget modes.
CURRENT_WALTRADE_SUPPORT=PARTIAL
OUR_EVIDENCE=Immutable Risk Budget evidence contract exists; influence and numeric policy remain OFF.
NEXT_ACTION=BACKLOG

## CASH_AS_VALID_ALLOCATION

MECHANISM=CASH_AS_VALID_ALLOCATION
PROBLEM_SOLVED=Avoid forcing capital into negative-expectancy opportunities.
EXTERNAL_SUPPORT=ESTABLISHED_STANDARD
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=NO TRADE decisions and cash/no-trade counterfactual diagnostics.
CURRENT_WALTRADE_SUPPORT=PARTIAL
OUR_EVIDENCE=Most current admissions do not cover full cost; no-trade is constitutionally valid.
NEXT_ACTION=REUSE

## OPPORTUNITY_COST_OF_CAPITAL

MECHANISM=OPPORTUNITY_COST_OF_CAPITAL
PROBLEM_SOLVED=Compare a candidate with alternative uses of capital, including waiting.
EXTERNAL_SUPPORT=STRONGLY_SUPPORTED
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=Opportunity Ranking, Remaining Opportunity, and marginal portfolio-value evidence.
CURRENT_WALTRADE_SUPPORT=PARTIAL
OUR_EVIDENCE=WalTrade records relevant context but has not proven full cross-slot allocation authority.
NEXT_ACTION=REWIRE

## POSITION_REPLACEMENT / REBALANCING

MECHANISM=POSITION_REPLACEMENT / REBALANCING
PROBLEM_SOLVED=Replace existing exposure only when a new opportunity offers sufficient incremental value after costs.
EXTERNAL_SUPPORT=ESTABLISHED_STANDARD
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=Future comparison of BUY, HOLD, NO TRADE, and replace-existing-risk decisions.
CURRENT_WALTRADE_SUPPORT=NO
OUR_EVIDENCE=New-risk-versus-keep-risk is relevant, but no replacement policy has causal proof.
NEXT_ACTION=BACKLOG

## WINNER_UPSIDE_PRESERVATION

MECHANISM=WINNER_UPSIDE_PRESERVATION
PROBLEM_SOLVED=Avoid truncating rare large winners while controlling loss and cost.
EXTERNAL_SUPPORT=STRONGLY_SUPPORTED
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=MFE, winner-tail, trailing, and exit-quality evidence.
CURRENT_WALTRADE_SUPPORT=PARTIAL
OUR_EVIDENCE=Immediate tiny-positive exit was rejected; current winner-tail dependence is low but upside must remain open in future exit tests.
NEXT_ACTION=KEEP

## GIVEBACK / PROFIT_PROTECTION

MECHANISM=GIVEBACK / PROFIT_PROTECTION
PROBLEM_SOLVED=Protect achieved economic viability from becoming a net loss without imposing a premature fixed take-profit.
EXTERNAL_SUPPORT=STRONGLY_SUPPORTED
SOURCE_REFERENCE=TO_BE_CURATED
WALTRADE_EQUIVALENT=Profit lock, MFE/giveback analytics, and future economic floor after cost cover.
CURRENT_WALTRADE_SUPPORT=PARTIAL
OUR_EVIDENCE=112/115 tiny-positive-to-loss outcomes ended through `PROFIT_LOCK_TRAIL_DROP`; leak is secondary but proven.
NEXT_ACTION=TEST

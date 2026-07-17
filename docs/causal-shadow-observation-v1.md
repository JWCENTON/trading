# Causal Shadow Observation V1

Shadow observation is not enabled by this change. When later wired and enabled,
only frozen `BLOCK_CANDIDATE` recommendations may be observed initially.
`SHADOW_OBSERVATION` never blocks or modifies action, direction, confidence,
sizing, prices, exits or execution.

An eligible observation must match deployment, environment, strategy, symbol,
interval, slot, regime policy, recommendation version, evidence cutoff and
activation window. Statuses distinguish NOT_ELIGIBLE,
ATTRIBUTED_SHADOW_OBSERVATION and future ATTRIBUTED_EXPERIMENT.

For BLOCK_CANDIDATE the actual decision remains authoritative. Would-trade
records show the actual action, hypothetical BLOCK action, with/without
recommendation projections and enforce `recommendation_effect_applied=false`.
Closed negative actual PnL is BENEFICIAL_DIRECTIONAL for a hypothetical block;
positive is HARMFUL_DIRECTIONAL; zero is NEUTRAL_DIRECTIONAL. These are not
exact execution-replay PnL claims.

Sample status is INSUFFICIENT_SAMPLE, COLLECTING or EVALUABLE. A future
configurable threshold must gate effectiveness reporting. Reports include
attributed, NO_TRADE and trade decisions; opened/closed/pending/not-evaluable
outcomes; net PnL; benefit/harm rates; median hold time and coverage. No automatic
promotion is authorized.

# Canonical Decision Observation Adapter V1

`common.decision_observation` defines the immutable event, semantic digest,
runtime flags, repository interface, error taxonomy and fail-open adapter.
Production bot wiring is deliberately absent.

Defaults are telemetry OFF, shadow observation OFF, auto-apply OFF and kill
switch active. Writes require valid `DEPLOYMENT_ID`, telemetry enabled,
auto-apply false and kill switch inactive. The kill switch covers observation,
attribution, would-trade, promotion consumption and outcomes without affecting
trading.

Skipped writes are distinguishable from failures: `TELEMETRY_DISABLED`,
`KILL_SWITCH_ACTIVE`, `SHADOW_OBSERVATION_DISABLED`, `AUTO_APPLY_NOT_ALLOWED`
and `DEPLOYMENT_MISMATCH` are structured informational skip reasons. Missing or
invalid deployment is instead `CONFIGURATION_INVALID` and remains fail-open.

Repository operations are keyed by deployment plus business identity. An
identical retry is idempotent; different content for the same identity raises
`IDEMPOTENCY_CONFLICT`. Required failures are structured and counted. An error
never escapes into the trading caller.

Outcome linkage uses `deployment_id + decision_key + position_id`. Without a
deterministic position link the status is `PENDING_OUTCOME` or `NOT_EVALUABLE`;
no symbol/timestamp heuristic is allowed.

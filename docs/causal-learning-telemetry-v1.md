# Causal Learning Telemetry V1

## 1. Problem statement

Learning effectiveness requires a deterministic forward-only chain:

`frozen recommendation → activation → future decision → actual/counterfactual outcome`.

V1 adds that chain as telemetry only. It does not apply recommendations or alter
trading decisions.

## 2. Why the effectiveness audit was blocked

The audit found no decision-level `recommendation_id` linkage in LOCAL LIVE or
LOCAL PAPER. Publication history alone cannot prove that a recommendation was
active, consumed by a decision, or responsible for a later outcome. Historical
links are not inferred.

## 3. Canonical IDs

- `recommendation_id`: deterministic SHA-256 of schema version, environment-aware
  slot, recommendation type/action, evidence cutoff and policy version.
- `recommendation_version`: immutable version within the environment/slot/regime.
- `activation_id`: UUID primary key of one activation-ledger entry.
- `experiment_id`: nullable identifier reserved for a future PAPER experiment.
- `decision_key`: stable key joining would-trade telemetry to its outcome.
- `slot_key`: uppercase
  `environment|strategy|symbol|interval|regime`, using `*` only for a deliberately
  regime-agnostic recommendation.
- `policy_version`: explicit producer/consumer policy version.

Text descriptions are not part of canonical identity.

## 4. Frozen recommendation contract

`learning_recommendation_snapshots_v1` stores evidence boundaries, validity,
expiry/reset, policy and payload hashes. A trigger prevents changes to identity,
evidence, validity and payload. Only lifecycle state/reset metadata may change.
`evidence_cutoff_at < decision_timestamp` is required for attribution.

## 5. Activation ledger

`learning_recommendation_activations_v1` is append-only for causal identity and
timing. Deactivation time/reason may be recorded without rewriting the activation.
Allowed modes are `SHADOW_OBSERVATION` and future PAPER-only
`PAPER_EXPERIMENT`; V1 creates no activation and enables no treatment.

## 6. Decision attribution

A shared `decision_registry_v1` insert trigger attributes only decisions inside
`effective_from <= timestamp < expires_at`, before deactivation, after the
evidence cutoff, and for the exact environment/slot/regime. No match is explicit
as `NO_ACTIVE_RECOMMENDATION`. A second trigger makes persisted attribution
immutable.

## 7. Baseline/treatment arms

The schema distinguishes `BASELINE`, `TREATMENT`, and `SHADOW_COUNTERFACTUAL`,
and records both the policy actually used and candidate policy. This is storage
capability only; V1 does not activate a treatment.

## 8. Would-trade telemetry

`learning_would_trade_decisions_v1` records blocked/shadow intent, side, entry,
quantity, notional, stop, take-profit, signal time and reason. Its idempotent
writer uses `decision_key`; it never inserts orders or positions.

## 9. Counterfactual outcomes

`learning_counterfactual_outcomes_v1` joins by `decision_key` and supports bounded
MFE, MAE and fixed-horizon outcomes with fees and method version. Where a
credible hypothetical exit is unavailable, status is `DIRECTIONAL_ONLY`.
Counterfactual results are never runtime inputs.

## 10. Promotion consumption

Activation records may reference the published promotion event, candidate,
payload hash and policy version. Future decision attribution persists these as
consumed promotion fields, separating publication from actual consumption.

## 11. Decision/outcome linkage

Executed decisions retain the existing deterministic `position_id` path into
actual outcomes, replay and warehouse. Non-executed decisions use `decision_key`
to join would-trade telemetry to a counterfactual outcome. Timestamp matching is
only a boundary condition, never the canonical join.

## 12. Legacy data policy

Existing rows receive `LEGACY_NOT_ATTRIBUTABLE`. No historical recommendation
backfill is attempted. Only future inserts pass through causal attribution.

## 13. Environment isolation

Recommendation identity, slot identity and activation lookup include
`trading_live` or `trading_paper`. PAPER experiments are rejected outside PAPER.
Regime-specific recommendations cannot collide with another regime.

## 14. Safety invariants

- auto-apply flag is forced to `0`;
- no migration statement changes `bot_control`, orders or positions;
- no strategy, ORC, Slot Brain, MME, sizing or threshold logic changes;
- attribution cannot alter a `FinalDecision` or its outcome;
- snapshots, activations and historical attribution cannot be causally rewritten;
- migration is transactional, additive and idempotent.

## 15. Experiment readiness

`v_learning_experiment_readiness_v1` requires frozen/active recommendation state,
evidence cutoff, activation, future attributed decision, baseline policy,
counterfactual telemetry, expiry, available kill switch, PAPER environment,
`SHADOW_OBSERVATION`, and auto-apply disabled.

## 16. Test coverage

Python contract tests cover deterministic/versioned identifiers, regime and
environment isolation, explicit no-recommendation state, strategy contracts,
schema fields, audit objects and no trading-state mutation. PostgreSQL tests
cover migration x2, before/after activation, expiry, reset, legacy preservation,
promotion consumption, attribution immutability, snapshot immutability,
idempotent would-trade recording, counterfactual linkage and auto-apply OFF.

### Scoped schema fingerprint

A global fingerprint over namespace patterns such as `learning_%`,
`decision_%`, or `v_learning_%` is not portable: it also incorporates unrelated
objects that legitimately differ between a minimal disposable database and an
existing WalTrade database.

The canonical migration fingerprint is therefore scoped to the explicit Causal
Learning Telemetry V1 manifest: its four tables; columns added to Decision
Registry, Replay and Feature Warehouse; two indexes; migration-owned
constraints; five triggers; seven functions; four views; and three automation
flags. Canonical rows include object type/schema/name, column type/nullability/
default, normalized constraint/index/trigger definitions, function identity
arguments plus normalized definition hashes, and normalized view-definition
hashes. Rows are whitespace-normalized, sorted, contain no OIDs, owners,
timestamps or row counts, and are then hashed with MD5.

Validated on 2026-07-16:

- disposable PostgreSQL after run 1: `9f09d167a7d3f42c4c9e4eea29a59608`;
- disposable PostgreSQL after run 2: `9f09d167a7d3f42c4c9e4eea29a59608`;
- LOCAL LIVE: `9f09d167a7d3f42c4c9e4eea29a59608`.

## 17. Rollout plan

Documentation only; not executed:

1. Validate schema and telemetry in LOCAL LIVE without creating activations.
2. Enable shadow observation in LOCAL PAPER.
3. Review coverage/readiness and explicitly approve commit/push from LOCAL.
4. Pull-only deployment to VPS LIVE after a separate decision.
5. Validate VPS PAPER before any separately approved experiment.

## 18. Rollback plan

Stop producers with the telemetry enable/kill-switch controls. Because V1 is
additive and has no trading consumer, leaving objects in place is safe. A later,
separately reviewed migration may drop triggers/views/functions and then tables
only after retention/export decisions. Never rewrite decision history during
rollback.

## 19. Explicit no-trading-change statement

This implementation is telemetry-only and shadow-only. It does not activate a
recommendation, execute an experiment, create an order/position, alter a strategy
decision, change LIVE behavior, or authorize auto-apply.

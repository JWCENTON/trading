# ADR: Causal Decision Observation V1

Status: proposed for review; no production wiring.

The canonical decision event is the immutable `DecisionObservationEvent` built
from a completed `FinalDecision`. The producer constructs it after trading
semantics are final. It never uses causal state to change that decision.

The event carries a producer-supplied stable `decision_key`; `event_id` identifies
delivery. Identity is unique by `(deployment_id, decision_key)`. Identical retry
is a no-op and different content is `IDEMPOTENCY_CONFLICT`. Both TRADE and
NO_TRADE, plus existing-logic blocks, EXIT and HOLD, are represented.

The bot/final-decision producer may emit the immutable value, but automation-runner
is the sole intended causal-state writer through one repository. No bot, API or
orchestrator writes attribution independently. Durable transport and production
wiring are explicitly deferred; wiring before choosing a durable handoff would
risk event loss.

`DEPLOYMENT_ID` is mandatory and must be one of `local-live`, `local-paper`,
`vps-live`, `vps-paper`. Hostname inference is forbidden. Invalid configuration
disables telemetry writes and reports `CONFIGURATION_INVALID`; trading continues.

The semantic SHA-256 digest covers only strategy, symbol, interval, action,
direction, confidence, quantity/entry/SL/TP/exit intents and execution eligibility.
UTF-8 compact JSON, sorted keys and explicit JSON null are canonical. The input
digest must equal the stored observation digest. Events and promotion consumption
evidence are append-only.

Replay and Warehouse consume `(deployment_id, decision_key)`, including NO_TRADE
and decisions without positions. A later outcome uses the same key plus a
deterministic `position_id`; symbol/time matching is forbidden. Actual promotion
consumption gets a separate append-only event and is never inferred from mere
eligibility. All telemetry failures are fail-open for trading.

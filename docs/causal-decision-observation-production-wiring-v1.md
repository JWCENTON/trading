# Causal Decision Observation Production Wiring Foundation V1

Status: **BLOCKED UNTIL CANONICAL FINALIZATION SINK EXISTS**. Producer bot wiring is absent.

## Decision-path audit

RSI, TREND, SUPERTREND and BBRANGE create `FinalDecision` in their respective `main.py` strategy
functions. Each covers entry gating, NO_TRADE/blocked decisions, position HOLD, exits, paper
simulation and live execution results. `EvaluationContext` supplies deployment, strategy, symbol,
interval and candle identity; `FinalDecision` may supply position/simulated-order ids after
execution. A canonical deterministic decision key is derivable before any position exists.

Persistence and execution remain bot-local. TREND, RSI and SUPERTREND return from many branches;
BBRANGE alone has a local `finish()`/`DecisionSink`, currently disabled. The bot processes run in
separate bot containers; automation-runner is a separate deployment-scoped process. There is no
single shared post-finalization adapter point through which every real decision passes.

Consequently this change provides the shared producer interface and real automation-runner
consumer, but does not wire producers into any bot. Adding four subtly different adapters would
give false coverage. The next wiring change must first introduce one shared `finish(FinalDecision)`
contract used by every return path, with the default-off producer invoked after finalization.

## Required transaction placement

NO_TRADE/HOLD/blocked events append immediately after finalization. For TRADE/EXIT the durable
append should share the DB transaction that persists the canonical order intent or position link
where feasible. The same `decision_key` must be stored additively on order intent/order/position.
Exchange side effects cannot be atomically coupled to PostgreSQL; reconciliation must retry the
same deterministic key. Observation never creates an order or position and never changes action,
direction, confidence, sizing, execution, exit logic, bot_control, ORC, Slot Brain or MME.

Until that shared transaction boundary exists, the sequence `FinalDecision finalized → process
crash → outbox insert not completed` is a **KNOWN PRODUCER ATOMICITY GAP**. At-least-once delivery
starts only after a successful outbox insert.

Promotion consumption is allowed only when a later producer actually receives and uses promotion
context. Lookup alone is not consumption. With shadow disabled its expected count is zero.

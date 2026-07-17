"""Causal Decision Observation V1 contracts; deliberately not production-wired."""

from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol

from common.decision_contract import FinalDecision

SCHEMA_VERSION = "CAUSAL_DECISION_OBSERVATION_V1"
VALID_DEPLOYMENTS = frozenset({"local-live", "local-paper", "vps-live", "vps-paper"})
SEMANTIC_FIELDS = (
    "strategy", "symbol", "interval", "action", "direction", "confidence",
    "quantity_intent", "entry_intent", "stop_loss_intent",
    "take_profit_intent", "exit_intent", "execution_eligible",
)


class FailureCode(str, Enum):
    CONFIGURATION_INVALID = "CONFIGURATION_INVALID"
    DECISION_OBSERVATION_WRITE_FAILED = "DECISION_OBSERVATION_WRITE_FAILED"
    SEMANTIC_DIGEST_MISMATCH = "SEMANTIC_DIGEST_MISMATCH"
    ACTIVATION_LOOKUP_FAILED = "ACTIVATION_LOOKUP_FAILED"
    ATTRIBUTION_WRITE_FAILED = "ATTRIBUTION_WRITE_FAILED"
    WOULD_TRADE_WRITE_FAILED = "WOULD_TRADE_WRITE_FAILED"
    PROMOTION_CONSUMPTION_FAILED = "PROMOTION_CONSUMPTION_FAILED"
    OUTCOME_LINK_FAILED = "OUTCOME_LINK_FAILED"
    COUNTERFACTUAL_UPDATE_FAILED = "COUNTERFACTUAL_UPDATE_FAILED"
    REPLAY_PROPAGATION_FAILED = "REPLAY_PROPAGATION_FAILED"
    WAREHOUSE_PROPAGATION_FAILED = "WAREHOUSE_PROPAGATION_FAILED"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"


class SkipReason(str, Enum):
    TELEMETRY_DISABLED = "TELEMETRY_DISABLED"
    KILL_SWITCH_ACTIVE = "KILL_SWITCH_ACTIVE"
    SHADOW_OBSERVATION_DISABLED = "SHADOW_OBSERVATION_DISABLED"
    AUTO_APPLY_NOT_ALLOWED = "AUTO_APPLY_NOT_ALLOWED"
    DEPLOYMENT_MISMATCH = "DEPLOYMENT_MISMATCH"


class IdempotencyConflict(ValueError):
    code = FailureCode.IDEMPOTENCY_CONFLICT


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def stable_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, default=_json_value, ensure_ascii=False,
                         sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class DecisionObservationEvent:
    event_id: str
    decision_key: str
    decision_created_at: datetime
    environment: str
    deployment_id: str
    strategy: str
    symbol: str
    interval: str
    slot_key: str
    regime: str | None
    regime_confidence: Decimal | None
    action: str
    direction: str | None
    confidence: Decimal | None
    quantity_intent: Decimal | None
    entry_intent: Decimal | None
    stop_loss_intent: Decimal | None
    take_profit_intent: Decimal | None
    exit_intent: str | None
    execution_eligible: bool
    decision_reason: str
    decision_payload_hash: str
    source_service: str
    source_instance: str
    decision_kind: str
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.deployment_id not in VALID_DEPLOYMENTS:
            raise ValueError("DEPLOYMENT_ID must be an explicit supported deployment")
        if self.decision_kind not in {"TRADE", "NO_TRADE", "BLOCKED_BY_EXISTING_LOGIC", "EXIT", "HOLD"}:
            raise ValueError("unsupported decision_kind")
        if not self.event_id or not self.decision_key or not self.decision_payload_hash:
            raise ValueError("event identity and payload hash are required")

    @property
    def semantic_digest(self) -> str:
        values = asdict(self)
        return stable_hash({name: values[name] for name in SEMANTIC_FIELDS})

    @property
    def event_digest(self) -> str:
        return stable_hash(asdict(self))


@dataclass(frozen=True)
class RuntimeFlags:
    telemetry_enabled: bool = False
    shadow_observation_enabled: bool = False
    auto_apply: bool = False
    kill_switch: bool = True
    deployment_id: str | None = None

    @property
    def writes_enabled(self) -> bool:
        return (self.telemetry_enabled and self.shadow_observation_enabled
                and not self.kill_switch and not self.auto_apply
                and self.deployment_id in VALID_DEPLOYMENTS)


class ObservationRepository(Protocol):
    def record_decision_observation(self, event: DecisionObservationEvent) -> str: ...
    def get_decision_observation(self, deployment_id: str, decision_key: str) -> DecisionObservationEvent: ...
    def record_attribution(self, value: dict[str, Any]) -> str: ...
    def record_would_trade(self, value: dict[str, Any]) -> str: ...
    def record_promotion_consumption(self, value: dict[str, Any]) -> str: ...
    def record_outcome(self, value: dict[str, Any]) -> str: ...


class InMemoryObservationRepository:
    """Deterministic repository contract fixture used before production wiring."""

    def __init__(self) -> None:
        self.observations: dict[tuple[str, str], tuple[str, DecisionObservationEvent]] = {}
        self.attributions: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
        self.would_trade: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
        self.promotions: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
        self.outcomes: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
        self.replay: dict[tuple[str, str], dict[str, Any]] = {}
        self.warehouse: dict[tuple[str, str], dict[str, Any]] = {}

    @staticmethod
    def _put(store, value, identity="decision_key") -> str:
        getter = (lambda name: value[name]) if isinstance(value, dict) else (lambda name: getattr(value, name))
        key = (getter("deployment_id"), getter(identity))
        digest = stable_hash(value if isinstance(value, dict) else asdict(value))
        existing = store.get(key)
        if existing and existing[0] != digest:
            raise IdempotencyConflict(f"different content for {key}")
        store.setdefault(key, (digest, value))
        return "idempotent" if existing else "inserted"

    def record_decision_observation(self, event):
        return self._put(self.observations, event, "decision_key")

    def get_decision_observation(self, deployment_id, decision_key):
        return self.observations[(deployment_id, decision_key)][1]

    def record_attribution(self, value): return self._put(self.attributions, value)
    def record_would_trade(self, value): return self._put(self.would_trade, value)
    def record_promotion_consumption(self, value):
        return self._put(self.promotions, value, "promotion_consumption_event_id")
    def record_outcome(self, value): return self._put(self.outcomes, value)

    def project(self, event: DecisionObservationEvent, attribution: dict[str, Any]) -> None:
        value = {"deployment_id": event.deployment_id, "decision_key": event.decision_key,
                 "decision_kind": event.decision_kind, **attribution}
        key = (event.deployment_id, event.decision_key)
        self.replay.setdefault(key, value)
        self.warehouse.setdefault(key, value)


class DecisionObservationAdapter:
    def __init__(self, repository: ObservationRepository, flags: RuntimeFlags,
                 logger: logging.Logger | None = None) -> None:
        self.repository, self.flags = repository, flags
        self.logger = logger or logging.getLogger(__name__)
        self.failures: Counter[str] = Counter()
        self.skips: Counter[str] = Counter()
        self.last_skip_reason: str | None = None

    def observe(self, event: DecisionObservationEvent) -> bool:
        """Persist without mutating or raising into the trading caller."""
        if not self.flags.telemetry_enabled:
            self._skip(SkipReason.TELEMETRY_DISABLED)
            return False
        if self.flags.kill_switch:
            self._skip(SkipReason.KILL_SWITCH_ACTIVE)
            return False
        if self.flags.deployment_id not in VALID_DEPLOYMENTS:
            self._fail(FailureCode.CONFIGURATION_INVALID)
            return False
        if not self.flags.shadow_observation_enabled:
            self._skip(SkipReason.SHADOW_OBSERVATION_DISABLED)
            return False
        if self.flags.auto_apply:
            self._skip(SkipReason.AUTO_APPLY_NOT_ALLOWED)
            return False
        if event.deployment_id != self.flags.deployment_id:
            self._skip(SkipReason.DEPLOYMENT_MISMATCH)
            return False
        digest = event.semantic_digest
        try:
            result = self.repository.record_decision_observation(event)
            persisted = self.repository.get_decision_observation(event.deployment_id, event.decision_key)
            if persisted.semantic_digest != digest:
                self._fail(FailureCode.SEMANTIC_DIGEST_MISMATCH)
                return False
            return result in {"inserted", "idempotent"}
        except IdempotencyConflict:
            self._fail(FailureCode.IDEMPOTENCY_CONFLICT)
        except Exception:
            self._fail(FailureCode.DECISION_OBSERVATION_WRITE_FAILED)
        return False

    def _fail(self, code: FailureCode) -> None:
        self.failures[code.value] += 1
        self.logger.error("causal_telemetry_failure", extra={"failure_code": code.value})

    def _skip(self, reason: SkipReason) -> None:
        self.last_skip_reason = reason.value
        self.skips[reason.value] += 1
        self.logger.info("causal_telemetry_write_skipped", extra={"skip_reason": reason.value})


def directional_status(net_pnl: Decimal | None, linked: bool = True, closed: bool = True) -> str:
    if not linked:
        return "NOT_EVALUABLE"
    if not closed:
        return "PENDING_OUTCOME"
    if net_pnl is None:
        return "NOT_EVALUABLE"
    if net_pnl > 0:
        return "HARMFUL_DIRECTIONAL"
    if net_pnl < 0:
        return "BENEFICIAL_DIRECTIONAL"
    return "NEUTRAL_DIRECTIONAL"


def event_from_final_decision(decision: FinalDecision, *, event_id: str,
                              decision_key: str, source_service: str,
                              source_instance: str, confidence: Decimal | None = None,
                              quantity_intent: Decimal | None = None,
                              entry_intent: Decimal | None = None,
                              stop_loss_intent: Decimal | None = None,
                              take_profit_intent: Decimal | None = None,
                              exit_intent: str | None = None) -> DecisionObservationEvent:
    """Pure adapter: construct an observation without changing FinalDecision."""
    ctx = decision.evaluation
    kind = ("TRADE" if decision.trade_executed else "EXIT" if decision.action == "EXIT"
            else "HOLD" if decision.action == "HOLD"
            else "BLOCKED_BY_EXISTING_LOGIC" if decision.action in {"BLOCK", "REJECT", "SUPPRESS"}
            else "NO_TRADE")
    payload_hash = stable_hash({"reason": decision.reason_code.value,
                                "details": dict(decision.details)})
    return DecisionObservationEvent(
        event_id=event_id, decision_key=decision_key,
        decision_created_at=decision.finished_at, environment=ctx.environment,
        deployment_id=ctx.deployment_id, strategy=ctx.strategy,
        symbol=ctx.symbol, interval=ctx.interval,
        slot_key="|".join((ctx.environment, ctx.strategy, ctx.symbol, ctx.interval,
                           ctx.market_regime or "*" )).upper(),
        regime=ctx.market_regime,
        regime_confidence=None if ctx.regime_confidence is None else Decimal(str(ctx.regime_confidence)),
        action=decision.action or "NO_ACTION", direction=decision.side,
        confidence=confidence, quantity_intent=quantity_intent,
        entry_intent=entry_intent or decision.reference_price,
        stop_loss_intent=stop_loss_intent, take_profit_intent=take_profit_intent,
        exit_intent=exit_intent, execution_eligible=decision.entry_attempted,
        decision_reason=decision.reason_code.value,
        decision_payload_hash=payload_hash, source_service=source_service,
        source_instance=source_instance, decision_kind=kind,
    )

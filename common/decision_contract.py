from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any, Callable, Mapping


class DecisionType(str, Enum):
    TRADE_EXECUTED = "TRADE_EXECUTED"
    NO_TRADE = "NO_TRADE"
    SIGNAL_REJECTED = "SIGNAL_REJECTED"
    ENTRY_BLOCKED = "ENTRY_BLOCKED"
    ENTRY_SUPPRESSED = "ENTRY_SUPPRESSED"
    PAPER_SIMULATION = "PAPER_SIMULATION"
    SYSTEM_NOT_EVALUATED = "SYSTEM_NOT_EVALUATED"
    TECHNICAL_FAILURE = "TECHNICAL_FAILURE"


class DecisionSubtype(str, Enum):
    NO_SIGNAL = "NO_SIGNAL"
    DATA_NOT_READY = "DATA_NOT_READY"
    NO_NEW_MARKET_DATA = "NO_NEW_MARKET_DATA"
    REGIME_BLOCKED = "REGIME_BLOCKED"
    ORC_BLOCKED = "ORC_BLOCKED"
    READINESS_BLOCKED = "READINESS_BLOCKED"
    RISK_BLOCKED = "RISK_BLOCKED"
    LIVE_DISABLED = "LIVE_DISABLED"
    EXECUTION_DISABLED = "EXECUTION_DISABLED"
    DUPLICATE_BLOCKED = "DUPLICATE_BLOCKED"
    ORDER_REJECTED = "ORDER_REJECTED"
    PAPER_ONLY = "PAPER_ONLY"
    EXECUTED = "EXECUTED"
    POSITION_MANAGEMENT = "POSITION_MANAGEMENT"
    EXIT_EXECUTED = "EXIT_EXECUTED"


class DecisionReason(str, Enum):
    UNKNOWN = "UNKNOWN"
    NO_SIGNAL = "NO_SIGNAL"
    NO_ROW = "NO_ROW"
    CANDLE_MISSING_CLOSE = "CANDLE_MISSING_CLOSE"
    INDICATORS_NOT_READY = "INDICATORS_NOT_READY"
    NO_NEW_CANDLE = "NO_NEW_CANDLE"
    DISABLE_HOURS = "DISABLE_HOURS"
    BOT_DISABLED = "BOT_DISABLED"
    BOT_MODE_HALT = "BOT_MODE_HALT"
    DAILY_MAX_LOSS_POSITIONS = "DAILY_MAX_LOSS_POSITIONS"
    NOT_ENOUGH_CANDLES = "NOT_ENOUGH_CANDLES"
    BB_NOT_READY = "BB_NOT_READY"
    BB_WIDTH_TOO_LOW = "BB_WIDTH_TOO_LOW"
    TREND_NOT_FLAT = "TREND_NOT_FLAT"
    RSI_EXTREME_BLOCK = "RSI_EXTREME_BLOCK"
    RSI_LONG_MAX_BLOCK = "RSI_LONG_MAX_BLOCK"
    SPOT_SHORT_BLOCK = "SPOT_SHORT_BLOCK"
    REGIME_BLOCK = "REGIME_BLOCK"
    SIZING_QTY_ZERO = "SIZING_QTY_ZERO"
    DB_GUARD_DUPLICATE = "DB_GUARD_DUPLICATE"
    LIVE_ENTRY_NOT_ATTEMPTED = "LIVE_ENTRY_NOT_ATTEMPTED"
    LIVE_ENTRY_NOT_FILLED = "LIVE_ENTRY_NOT_FILLED"
    LIVE_ORDER_FAILED = "LIVE_ORDER_FAILED"
    LIVE_ACK_MISSING_ORDER_ID = "LIVE_ACK_MISSING_ORDER_ID"
    SSOT_EXECUTE_AND_RECORD = "SSOT_EXECUTE_AND_RECORD"


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(k): _freeze(v) for k, v in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, tuple):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, set):
        return frozenset(_freeze(v) for v in value)
    return value


def _aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


@dataclass(frozen=True)
class EvaluationContext:
    deployment_id: str
    environment: str
    symbol: str
    interval: str
    strategy: str
    candle_open_time: datetime
    evaluation_started_at: datetime
    engine_name: str
    engine_version: str | None = None
    market_regime: str | None = None
    regime_confidence: Decimal | float | None = None
    runtime_enabled: bool | None = None
    live_orders_enabled: bool | None = None
    paper_mode: bool = False
    context: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("deployment_id", "environment", "symbol", "interval",
                     "strategy", "engine_name"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} must be explicit")
        _aware(self.candle_open_time, "candle_open_time")
        _aware(self.evaluation_started_at, "evaluation_started_at")
        object.__setattr__(self, "context", _freeze(self.context))

    @property
    def deployment_is_known(self) -> bool:
        return self.deployment_id.upper() != "UNKNOWN"

    def identity_components(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.deployment_id,
            self.environment,
            self.symbol,
            self.interval,
            self.strategy,
            self.candle_open_time.isoformat(),
        )


@dataclass(frozen=True)
class FinalDecision:
    evaluation: EvaluationContext
    decision_type: DecisionType
    decision_subtype: DecisionSubtype
    action: str | None
    reason_code: DecisionReason
    reason_text: str | None
    signal_detected: bool
    entry_attempted: bool
    order_submitted: bool
    trade_executed: bool
    position_id: int | None
    strategy_event_id: int | None
    simulated_order_id: int | None
    reference_price: Decimal | None
    side: str | None
    learning_eligible: bool
    replay_eligible: bool
    technical_failure: bool
    finished_at: datetime
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _aware(self.finished_at, "finished_at")
        object.__setattr__(self, "details", _freeze(self.details))
        if self.reference_price is not None and not isinstance(self.reference_price, Decimal):
            raise TypeError("reference_price must be Decimal or None")
        if self.trade_executed and not self.order_submitted:
            raise ValueError("trade_executed requires order_submitted")
        if self.order_submitted and not self.entry_attempted:
            raise ValueError("order_submitted requires entry_attempted")
        if self.decision_type is DecisionType.TRADE_EXECUTED and not self.trade_executed:
            raise ValueError("TRADE_EXECUTED requires trade_executed")
        if self.decision_type is DecisionType.NO_TRADE and self.trade_executed:
            raise ValueError("NO_TRADE cannot execute a trade")
        if (self.decision_type is DecisionType.SYSTEM_NOT_EVALUATED
                and (self.learning_eligible or self.replay_eligible)):
            raise ValueError("SYSTEM_NOT_EVALUATED cannot be learning/replay eligible")
        if self.decision_type is DecisionType.TECHNICAL_FAILURE and not self.technical_failure:
            raise ValueError("TECHNICAL_FAILURE requires technical_failure")
        if self.technical_failure and self.learning_eligible:
            raise ValueError("technical failures cannot be learning eligible")
        if self.decision_type is DecisionType.PAPER_SIMULATION and not self.evaluation.paper_mode:
            raise ValueError("PAPER_SIMULATION requires paper_mode")

    @classmethod
    def _make(cls, evaluation: EvaluationContext, decision_type: DecisionType,
              decision_subtype: DecisionSubtype, reason_code: DecisionReason, *,
              finished_at: datetime, action: str | None = None,
              reason_text: str | None = None, signal_detected: bool = False,
              entry_attempted: bool = False, order_submitted: bool = False,
              trade_executed: bool = False, position_id: int | None = None,
              strategy_event_id: int | None = None,
              simulated_order_id: int | None = None,
              reference_price: Decimal | None = None, side: str | None = None,
              learning_eligible: bool = False, replay_eligible: bool = False,
              technical_failure: bool = False,
              details: Mapping[str, Any] | None = None) -> "FinalDecision":
        return cls(
            evaluation, decision_type, decision_subtype, action, reason_code,
            reason_text, signal_detected, entry_attempted, order_submitted,
            trade_executed, position_id, strategy_event_id, simulated_order_id,
            reference_price, side, learning_eligible, replay_eligible,
            technical_failure, finished_at, details or {},
        )

    @classmethod
    def no_trade(cls, evaluation, reason_code, *, finished_at, reference_price=None,
                 reason_text=None, details=None):
        return cls._make(evaluation, DecisionType.NO_TRADE,
                         DecisionSubtype.NO_SIGNAL, reason_code,
                         finished_at=finished_at, reference_price=reference_price,
                         reason_text=reason_text, learning_eligible=True,
                         replay_eligible=True, details=details)

    @classmethod
    def system_not_evaluated(cls, evaluation, reason_code, *, finished_at,
                             reason_text=None, details=None):
        subtype = (DecisionSubtype.NO_NEW_MARKET_DATA
                   if reason_code is DecisionReason.NO_NEW_CANDLE
                   else DecisionSubtype.DATA_NOT_READY)
        return cls._make(evaluation, DecisionType.SYSTEM_NOT_EVALUATED, subtype,
                         reason_code, finished_at=finished_at,
                         reason_text=reason_text, details=details)

    @classmethod
    def signal_rejected(cls, evaluation, reason_code, *, finished_at,
                        reference_price=None, side=None, reason_text=None,
                        details=None):
        return cls._make(evaluation, DecisionType.SIGNAL_REJECTED,
                         DecisionSubtype.READINESS_BLOCKED, reason_code,
                         finished_at=finished_at, action="REJECT", side=side,
                         reference_price=reference_price, reason_text=reason_text,
                         signal_detected=True, learning_eligible=True,
                         replay_eligible=True, details=details)

    @classmethod
    def entry_blocked(cls, evaluation, reason_code, subtype, *, finished_at,
                      reference_price=None, side=None, reason_text=None,
                      signal_detected=True, details=None):
        return cls._make(evaluation, DecisionType.ENTRY_BLOCKED, subtype,
                         reason_code, finished_at=finished_at, action="BLOCK",
                         side=side, reference_price=reference_price,
                         reason_text=reason_text, signal_detected=signal_detected,
                         learning_eligible=True, replay_eligible=True,
                         details=details)

    @classmethod
    def entry_suppressed(cls, evaluation, reason_code, subtype, *, finished_at,
                         reference_price=None, side=None, reason_text=None,
                         signal_detected=False, entry_attempted=False,
                         details=None):
        return cls._make(evaluation, DecisionType.ENTRY_SUPPRESSED, subtype,
                         reason_code, finished_at=finished_at, action="SUPPRESS",
                         side=side, reference_price=reference_price,
                         reason_text=reason_text, signal_detected=signal_detected,
                         entry_attempted=entry_attempted, replay_eligible=True,
                         details=details)

    @classmethod
    def paper_simulation(cls, evaluation, reason_code, *, finished_at,
                         reference_price=None, side=None, reason_text=None,
                         simulated_order_id=None, details=None):
        return cls._make(evaluation, DecisionType.PAPER_SIMULATION,
                         DecisionSubtype.PAPER_ONLY, reason_code,
                         finished_at=finished_at, action="SIMULATE", side=side,
                         reference_price=reference_price, reason_text=reason_text,
                         signal_detected=True, entry_attempted=True,
                         simulated_order_id=simulated_order_id,
                         learning_eligible=True, replay_eligible=True,
                         details=details)

    @classmethod
    def trade_executed_result(cls, evaluation, reason_code, *, finished_at,
                              reference_price=None, side=None, reason_text=None,
                              position_id=None, details=None):
        return cls._make(evaluation, DecisionType.TRADE_EXECUTED,
                         DecisionSubtype.EXECUTED, reason_code,
                         finished_at=finished_at, action="EXECUTE", side=side,
                         reference_price=reference_price, reason_text=reason_text,
                         signal_detected=True, entry_attempted=True,
                         order_submitted=True, trade_executed=True,
                         position_id=position_id, learning_eligible=True,
                         replay_eligible=True, details=details)

    @classmethod
    def technical_failure_result(cls, evaluation, reason_code, subtype, *,
                                 finished_at, reference_price=None, side=None,
                                 reason_text=None, signal_detected=False,
                                 entry_attempted=False, order_submitted=False,
                                 details=None):
        return cls._make(evaluation, DecisionType.TECHNICAL_FAILURE, subtype,
                         reason_code, finished_at=finished_at, action="ERROR",
                         side=side, reference_price=reference_price,
                         reason_text=reason_text, signal_detected=signal_detected,
                         entry_attempted=entry_attempted,
                         order_submitted=order_submitted,
                         technical_failure=True, details=details)


DecisionSink = Callable[[FinalDecision], None]


_LEGACY_REASONS = {reason.value: reason for reason in DecisionReason
                   if reason is not DecisionReason.UNKNOWN}


def classify_legacy_reason(reason_text: str | None) -> DecisionReason:
    return _LEGACY_REASONS.get(str(reason_text or "").strip().upper(),
                               DecisionReason.UNKNOWN)

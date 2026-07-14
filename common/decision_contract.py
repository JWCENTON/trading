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
    ACTION_SUPPRESSED = "ACTION_SUPPRESSED"


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
    ORDER_ACCEPTED_NOT_FILLED = "ORDER_ACCEPTED_NOT_FILLED"
    PARTIAL_EXECUTION = "PARTIAL_EXECUTION"
    LEDGER_FAILURE = "LEDGER_FAILURE"
    PAPER_ONLY = "PAPER_ONLY"
    EXECUTED = "EXECUTED"
    POSITION_MANAGEMENT = "POSITION_MANAGEMENT"
    EXIT_EXECUTED = "EXIT_EXECUTED"
    EXECUTION_NOT_ATTEMPTED = "EXECUTION_NOT_ATTEMPTED"
    NO_POSITION = "NO_POSITION"


class DecisionReason(str, Enum):
    UNKNOWN = "UNKNOWN"
    NO_SIGNAL = "NO_SIGNAL"
    NO_ROW = "NO_ROW"
    CANDLE_MISSING_CLOSE = "CANDLE_MISSING_CLOSE"
    CANDLE_MISSING_FIELDS = "CANDLE_MISSING_FIELDS"
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
    POLICY_BLOCK = "POLICY_BLOCK"
    POSITION_HOLD = "POSITION_HOLD"
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"
    BREAK_EVEN_PROTECT = "BREAK_EVEN_PROTECT"
    PROFIT_LOCK = "PROFIT_LOCK"
    STRATEGY_EXIT = "STRATEGY_EXIT"
    TIME_EXIT = "TIME_EXIT"
    EXECUTION_FAILED = "EXECUTION_FAILED"
    EXECUTION_NOT_ATTEMPTED = "EXECUTION_NOT_ATTEMPTED"
    NO_OPEN_POSITION = "NO_OPEN_POSITION"


class ExecutionStage(str, Enum):
    NOT_ATTEMPTED = "NOT_ATTEMPTED"
    SUPPRESSED = "SUPPRESSED"
    REJECTED_BEFORE_ACK = "REJECTED_BEFORE_ACK"
    ACCEPTED_PENDING = "ACCEPTED_PENDING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    LEDGER_FAILURE = "LEDGER_FAILURE"


class ExecutionOutcomeInvariantError(ValueError):
    pass


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


@dataclass(frozen=True)
class ExecutionOutcome:
    attempted: bool
    order_accepted: bool
    executed: bool
    fully_executed: bool
    operation_succeeded: bool
    executed_qty: float | None
    requested_qty: float | None
    ledger_ok: bool
    suppressed: bool
    blocked_reason: str | None
    order_id: str | None
    client_order_id: str | None
    exchange_status: str | None
    error: str | None
    stage: ExecutionStage
    raw: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.order_accepted and not self.attempted:
            raise ExecutionOutcomeInvariantError(
                "order_accepted requires attempted"
            )
        if self.executed and not self.order_accepted:
            raise ExecutionOutcomeInvariantError(
                "executed requires order_accepted"
            )
        if self.fully_executed and not self.executed:
            raise ExecutionOutcomeInvariantError(
                "fully_executed requires executed"
            )
        if self.executed and (
            self.executed_qty is None or self.executed_qty <= 0
        ):
            raise ExecutionOutcomeInvariantError(
                "executed outcome requires positive executed_qty"
            )
        if self.operation_succeeded and not self.executed:
            raise ExecutionOutcomeInvariantError(
                "operation_succeeded requires executed"
            )
        if self.executed_qty is not None:
            if self.executed_qty < 0:
                raise ExecutionOutcomeInvariantError(
                    "executed_qty cannot be negative"
                )
            if self.executed_qty > 0 and not self.executed:
                raise ExecutionOutcomeInvariantError(
                    "positive executed_qty requires executed"
                )
            if not self.executed and self.executed_qty != 0:
                raise ExecutionOutcomeInvariantError(
                    "non-executed outcome requires zero executed_qty"
                )
        if self.suppressed and (
            self.attempted or self.order_accepted or self.executed
        ):
            raise ExecutionOutcomeInvariantError(
                "suppressed outcome cannot be attempted, accepted, or executed"
            )
        if not self.ledger_ok:
            expected_stage = ExecutionStage.LEDGER_FAILURE
        elif self.suppressed:
            expected_stage = ExecutionStage.SUPPRESSED
        elif self.fully_executed:
            expected_stage = ExecutionStage.FILLED
        elif self.executed:
            expected_stage = ExecutionStage.PARTIALLY_FILLED
        elif self.order_accepted:
            expected_stage = ExecutionStage.ACCEPTED_PENDING
        elif self.attempted:
            expected_stage = ExecutionStage.REJECTED_BEFORE_ACK
        else:
            expected_stage = ExecutionStage.NOT_ATTEMPTED
        if self.stage is not expected_stage:
            raise ExecutionOutcomeInvariantError(
                f"stage {self.stage.value} does not match {expected_stage.value}"
            )
        object.__setattr__(self, "raw", _freeze(self.raw))

    @classmethod
    def from_legacy(
        cls,
        result: Mapping[str, Any],
        *,
        paper_mode: bool = False,
    ) -> "ExecutionOutcome":
        attempted = bool(result.get("live_attempted", False))
        order_accepted = bool(result.get("order_accepted", False))
        raw_response = result.get("resp")
        response = raw_response if isinstance(raw_response, Mapping) else {}
        executed_qty_raw = result.get("executed_qty")
        if executed_qty_raw is None:
            executed_qty_raw = response.get("executedQty")
        try:
            executed_qty = (
                float(executed_qty_raw) if executed_qty_raw is not None else None
            )
        except (TypeError, ValueError):
            executed_qty = None
        requested_qty_raw = result.get("requested_qty")
        try:
            requested_qty = (
                float(requested_qty_raw) if requested_qty_raw is not None else None
            )
        except (TypeError, ValueError):
            requested_qty = None
        if paper_mode:
            executed = False
            fully_executed = False
            operation_succeeded = False
            executed_qty = 0.0
        else:
            executed = bool(
                result.get("executed", False)
                if "executed" in result
                else (executed_qty > 0 if executed_qty is not None else False)
            )
            fully_executed = bool(result.get("fully_executed", False))
            operation_succeeded = bool(result.get("live_ok", False))
            if not executed and executed_qty is None:
                executed_qty = 0.0
        ledger_ok = bool(result.get("ledger_ok", False))
        blocked_reason = str(result.get("blocked_reason") or "") or None
        explicitly_suppressed = result.get("suppressed")
        if explicitly_suppressed is None:
            suppressed = bool(
                not paper_mode
                and ledger_ok
                and not attempted
                and not order_accepted
                and not executed
                and blocked_reason
                and blocked_reason != "EXIT_NO_OPEN_POSITION"
            )
        else:
            suppressed = bool(explicitly_suppressed)

        if not ledger_ok:
            stage = ExecutionStage.LEDGER_FAILURE
        elif suppressed:
            stage = ExecutionStage.SUPPRESSED
        elif fully_executed:
            stage = ExecutionStage.FILLED
        elif executed:
            stage = ExecutionStage.PARTIALLY_FILLED
        elif order_accepted:
            stage = ExecutionStage.ACCEPTED_PENDING
        elif attempted:
            stage = ExecutionStage.REJECTED_BEFORE_ACK
        else:
            stage = ExecutionStage.NOT_ATTEMPTED

        return cls(
            attempted=attempted,
            order_accepted=order_accepted,
            executed=executed,
            fully_executed=fully_executed,
            operation_succeeded=operation_succeeded,
            executed_qty=executed_qty,
            requested_qty=requested_qty,
            ledger_ok=ledger_ok,
            suppressed=suppressed,
            blocked_reason=blocked_reason,
            order_id=(str(result.get("order_id") or response.get("orderId"))
                      if result.get("order_id") or response.get("orderId")
                      else None),
            client_order_id=(
                str(result["client_order_id"])
                if result.get("client_order_id") is not None else None
            ),
            exchange_status=(str(
                result.get("exchange_status")
                or result.get("status")
                or response.get("status")
            ) if (
                result.get("exchange_status")
                or result.get("status")
                or response.get("status")
            ) else None),
            error=(
                str(result.get("error") or blocked_reason)
                if result.get("error") or blocked_reason else None
            ),
            stage=stage,
            raw=result,
        )


def normalize_entry_execution_outcome(
    result: Mapping[str, Any],
    *,
    requested_qty: float,
    client_order_id: str | None,
    ledger_ok: bool = True,
) -> ExecutionOutcome:
    """Normalize a LIVE entry result without performing I/O or state mutation."""
    raw_response = result.get("resp")
    response = raw_response if isinstance(raw_response, Mapping) else {}
    # When the raw exchange response is available it is the quantity SSOT. This
    # deliberately rejects a legacy top-level requested-qty fallback for a
    # FILLED response whose exchange-reported executedQty is zero or absent.
    executed_qty_raw = (
        response.get("executedQty", 0.0)
        if response
        else result.get("executed_qty")
    )
    try:
        executed_qty = float(executed_qty_raw or 0.0)
    except (TypeError, ValueError):
        executed_qty = 0.0

    if "executed" in result:
        executed = bool(result.get("executed")) and executed_qty > 0.0
    else:
        executed = executed_qty > 0.0

    if "order_accepted" in result:
        order_accepted = bool(result.get("order_accepted"))
    else:
        # A confirmed fill necessarily passed exchange acceptance. An order ID or
        # NEW/ACCEPTED status alone deliberately does not infer acceptance.
        order_accepted = executed

    status = str(
        result.get("exchange_status")
        or result.get("status")
        or response.get("status")
        or ""
    ).upper()
    fully_executed = bool(
        executed
        and result.get("fully_executed", status == "FILLED")
    )

    if "live_attempted" in result or "attempted" in result:
        attempted = bool(
            result.get("live_attempted", result.get("attempted", False))
        )
    else:
        # A real fill proves that an exchange attempt occurred. Pending IDs and
        # statuses remain insufficient to infer an attempt or acceptance.
        attempted = executed

    normalized = {
        **result,
        "ledger_ok": bool(ledger_ok),
        "live_attempted": attempted,
        "order_accepted": order_accepted,
        "executed": executed,
        "fully_executed": fully_executed,
        "executed_qty": executed_qty,
        "requested_qty": result.get("requested_qty", requested_qty),
        "client_order_id": result.get("client_order_id") or client_order_id,
        # For entry semantics, operation success means a real positive fill.
        "live_ok": executed,
    }
    return ExecutionOutcome.from_legacy(normalized)


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
        if (self.decision_type is DecisionType.ACTION_SUPPRESSED
                and (self.order_submitted or self.trade_executed)):
            raise ValueError("ACTION_SUPPRESSED cannot submit or execute an order")

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
    def idle(cls, evaluation, reason_code, *, finished_at,
             reason_text=None, details=None):
        return cls._make(evaluation, DecisionType.SYSTEM_NOT_EVALUATED,
                         DecisionSubtype.NO_NEW_MARKET_DATA, reason_code,
                         finished_at=finished_at, action="IDLE",
                         reason_text=reason_text, details=details)

    @classmethod
    def position_hold(cls, evaluation, reason_code, *, finished_at,
                      reference_price=None, side=None, position_id=None,
                      reason_text=None, details=None):
        return cls._make(evaluation, DecisionType.NO_TRADE,
                         DecisionSubtype.POSITION_MANAGEMENT, reason_code,
                         finished_at=finished_at, action="HOLD", side=side,
                         position_id=position_id,
                         reference_price=reference_price,
                         reason_text=reason_text, details=details)

    @classmethod
    def exit_result(cls, evaluation, reason_code, *, finished_at,
                    reference_price=None, side=None, position_id=None,
                    reason_text=None, details=None):
        common = dict(
            finished_at=finished_at,
            action="EXIT",
            side=side,
            position_id=position_id,
            reference_price=reference_price,
            reason_text=reason_text,
            signal_detected=True,
            entry_attempted=True,
            learning_eligible=True,
            replay_eligible=True,
            details=details,
        )
        if evaluation.paper_mode:
            return cls._make(
                evaluation,
                DecisionType.PAPER_SIMULATION,
                DecisionSubtype.EXIT_EXECUTED,
                reason_code,
                **common,
            )
        return cls._make(
            evaluation,
            DecisionType.TRADE_EXECUTED,
            DecisionSubtype.EXIT_EXECUTED,
            reason_code,
            order_submitted=True,
            trade_executed=True,
            **common,
        )

    @classmethod
    def action_suppressed(cls, evaluation, reason_code, *, finished_at,
                          reference_price=None, side=None, reason_text=None,
                          details=None):
        return cls._make(
            evaluation,
            DecisionType.ACTION_SUPPRESSED,
            DecisionSubtype.EXECUTION_NOT_ATTEMPTED,
            reason_code,
            finished_at=finished_at,
            action="SUPPRESS",
            side=side,
            reference_price=reference_price,
            reason_text=reason_text,
            signal_detected=True,
            details=details,
        )

    @classmethod
    def no_position(cls, evaluation, reason_code, *, finished_at,
                    reference_price=None, side=None, reason_text=None,
                    details=None):
        return cls._make(
            evaluation,
            DecisionType.NO_TRADE,
            DecisionSubtype.NO_POSITION,
            reason_code,
            finished_at=finished_at,
            action="REJECT",
            side=side,
            reference_price=reference_price,
            reason_text=reason_text,
            signal_detected=True,
            details=details,
        )

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
                                 trade_executed=False,
                                 details=None):
        return cls._make(evaluation, DecisionType.TECHNICAL_FAILURE, subtype,
                         reason_code, finished_at=finished_at, action="ERROR",
                         side=side, reference_price=reference_price,
                         reason_text=reason_text, signal_detected=signal_detected,
                         entry_attempted=entry_attempted,
                         order_submitted=order_submitted,
                         trade_executed=trade_executed,
                         technical_failure=True, details=details)


DecisionSink = Callable[[FinalDecision], None]


_LEGACY_REASONS = {
    reason.value: reason
    for reason in (
        DecisionReason.NO_SIGNAL,
        DecisionReason.NO_ROW,
        DecisionReason.CANDLE_MISSING_CLOSE,
        DecisionReason.INDICATORS_NOT_READY,
        DecisionReason.NO_NEW_CANDLE,
        DecisionReason.DISABLE_HOURS,
        DecisionReason.BOT_DISABLED,
        DecisionReason.BOT_MODE_HALT,
        DecisionReason.DAILY_MAX_LOSS_POSITIONS,
        DecisionReason.NOT_ENOUGH_CANDLES,
        DecisionReason.BB_NOT_READY,
        DecisionReason.BB_WIDTH_TOO_LOW,
        DecisionReason.TREND_NOT_FLAT,
        DecisionReason.RSI_EXTREME_BLOCK,
        DecisionReason.RSI_LONG_MAX_BLOCK,
        DecisionReason.SPOT_SHORT_BLOCK,
        DecisionReason.REGIME_BLOCK,
        DecisionReason.SIZING_QTY_ZERO,
        DecisionReason.DB_GUARD_DUPLICATE,
        DecisionReason.LIVE_ENTRY_NOT_ATTEMPTED,
        DecisionReason.LIVE_ENTRY_NOT_FILLED,
        DecisionReason.LIVE_ORDER_FAILED,
        DecisionReason.LIVE_ACK_MISSING_ORDER_ID,
        DecisionReason.SSOT_EXECUTE_AND_RECORD,
    )
}


def classify_legacy_reason(reason_text: str | None) -> DecisionReason:
    return _LEGACY_REASONS.get(str(reason_text or "").strip().upper(),
                               DecisionReason.UNKNOWN)

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from decimal import Decimal

import pytest
import common.execution as execution
from common.exchange_client import ExchangeAPIException

from common.decision_contract import (
    DecisionReason,
    DecisionSubtype,
    DecisionType,
    ExecutionOutcome,
    ExecutionOutcomeInvariantError,
    ExecutionStage,
    EvaluationContext,
    FinalDecision,
    classify_legacy_reason,
)


NOW = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)


def context(**overrides):
    values = dict(
        deployment_id="LOCAL", environment="trading_paper",
        symbol="BTCUSDC", interval="1m", strategy="ANY_STRATEGY",
        candle_open_time=NOW, evaluation_started_at=NOW,
        engine_name="ANY_STRATEGY", paper_mode=True,
        context={"nested": {"values": [1, 2]}},
    )
    values.update(overrides)
    return EvaluationContext(**values)


def test_context_is_immutable_deep_and_strategy_neutral():
    item = context(strategy="FUTURE_STRATEGY")
    with pytest.raises(FrozenInstanceError):
        item.strategy = "changed"
    with pytest.raises(TypeError):
        item.context["nested"] = {}
    with pytest.raises(TypeError):
        item.context["nested"]["values"] = ()
    assert item.context["nested"]["values"] == (1, 2)


def test_identity_components_are_stable_and_unknown_is_visible():
    first = context(deployment_id="UNKNOWN", context={"a": 1})
    second = context(deployment_id="UNKNOWN", context={"a": 2})
    assert first.identity_components() == second.identity_components()
    assert first.deployment_is_known is False


@pytest.mark.parametrize("field", ["candle_open_time", "evaluation_started_at"])
def test_context_rejects_naive_time(field):
    with pytest.raises(ValueError, match="timezone-aware"):
        context(**{field: datetime(2026, 7, 12, 12, 0)})


def test_no_trade_factory_and_deep_immutability():
    decision = FinalDecision.no_trade(
        context(), DecisionReason.NO_SIGNAL, finished_at=NOW,
        reference_price=Decimal("100"), details={"levels": [1, 2]},
    )
    assert decision.decision_type is DecisionType.NO_TRADE
    assert decision.learning_eligible and decision.replay_eligible
    assert not decision.signal_detected and not decision.technical_failure
    assert decision.details["levels"] == (1, 2)
    with pytest.raises(FrozenInstanceError):
        decision.action = "BUY"


def test_all_factories_have_safe_defaults():
    ctx = context()
    results = [
        FinalDecision.system_not_evaluated(
            ctx, DecisionReason.INDICATORS_NOT_READY, finished_at=NOW),
        FinalDecision.signal_rejected(
            ctx, DecisionReason.BB_WIDTH_TOO_LOW, finished_at=NOW),
        FinalDecision.entry_blocked(
            ctx, DecisionReason.REGIME_BLOCK, DecisionSubtype.REGIME_BLOCKED,
            finished_at=NOW),
        FinalDecision.entry_suppressed(
            ctx, DecisionReason.BOT_DISABLED, DecisionSubtype.LIVE_DISABLED,
            finished_at=NOW),
        FinalDecision.paper_simulation(
            ctx, DecisionReason.SSOT_EXECUTE_AND_RECORD, finished_at=NOW),
        FinalDecision.technical_failure_result(
            ctx, DecisionReason.DB_GUARD_DUPLICATE,
            DecisionSubtype.DUPLICATE_BLOCKED, finished_at=NOW),
        FinalDecision.idle(
            ctx, DecisionReason.NO_NEW_CANDLE, finished_at=NOW),
        FinalDecision.position_hold(
            ctx, DecisionReason.POSITION_HOLD, finished_at=NOW,
            side="LONG", position_id=7),
        FinalDecision.exit_result(
            ctx, DecisionReason.STOP_LOSS, finished_at=NOW,
            side="SELL", position_id=7),
        FinalDecision.action_suppressed(
            ctx, DecisionReason.EXECUTION_NOT_ATTEMPTED,
            finished_at=NOW, side="SELL",
            details={"blocked_reason": "LIVE_ORDER_SUPPRESSED"}),
        FinalDecision.no_position(
            ctx, DecisionReason.NO_OPEN_POSITION,
            finished_at=NOW, side="SELL"),
    ]
    assert results[0].learning_eligible is False
    assert results[1].signal_detected is True
    assert results[2].replay_eligible is True
    assert results[3].trade_executed is False
    assert results[4].evaluation.paper_mode is True
    assert results[5].technical_failure is True
    assert results[6].action == "IDLE"
    assert results[6].decision_subtype is DecisionSubtype.NO_NEW_MARKET_DATA
    assert results[7].action == "HOLD"
    assert results[7].decision_subtype is DecisionSubtype.POSITION_MANAGEMENT
    assert results[8].action == "EXIT"
    assert results[8].decision_subtype is DecisionSubtype.EXIT_EXECUTED
    assert results[8].decision_type is DecisionType.PAPER_SIMULATION
    assert results[9].decision_type is DecisionType.ACTION_SUPPRESSED
    assert results[9].decision_subtype is DecisionSubtype.EXECUTION_NOT_ATTEMPTED
    assert results[9].action == "SUPPRESS"
    assert results[10].decision_type is DecisionType.NO_TRADE
    assert results[10].decision_subtype is DecisionSubtype.NO_POSITION
    assert results[10].action == "REJECT"


def test_live_exit_result_preserves_execution_invariants():
    decision = FinalDecision.exit_result(
        context(paper_mode=False), DecisionReason.TAKE_PROFIT,
        finished_at=NOW, side="SELL", position_id=9,
    )
    assert decision.decision_type is DecisionType.TRADE_EXECUTED
    assert decision.decision_subtype is DecisionSubtype.EXIT_EXECUTED
    assert decision.action == "EXIT"
    assert decision.entry_attempted
    assert decision.order_submitted
    assert decision.trade_executed


def test_trade_factory_for_live_context():
    decision = FinalDecision.trade_executed_result(
        context(paper_mode=False), DecisionReason.SSOT_EXECUTE_AND_RECORD,
        finished_at=NOW, side="BUY", reference_price=Decimal("90"))
    assert decision.trade_executed and decision.order_submitted
    assert decision.entry_attempted


@pytest.mark.parametrize(
    "changes, message",
    [
        ({"trade_executed": True, "order_submitted": False},
         "trade_executed requires"),
        ({"order_submitted": True, "entry_attempted": False},
         "order_submitted requires"),
        ({"decision_type": DecisionType.TRADE_EXECUTED,
          "trade_executed": False}, "TRADE_EXECUTED requires"),
        ({"decision_type": DecisionType.NO_TRADE,
          "trade_executed": True, "order_submitted": True,
          "entry_attempted": True}, "NO_TRADE cannot"),
        ({"decision_type": DecisionType.SYSTEM_NOT_EVALUATED,
          "learning_eligible": True}, "SYSTEM_NOT_EVALUATED"),
        ({"decision_type": DecisionType.TECHNICAL_FAILURE,
          "technical_failure": False}, "TECHNICAL_FAILURE requires"),
        ({"technical_failure": True, "learning_eligible": True},
         "technical failures cannot"),
    ],
)
def test_invalid_combinations(changes, message):
    values = dict(
        evaluation=context(), decision_type=DecisionType.ENTRY_BLOCKED,
        decision_subtype=DecisionSubtype.RISK_BLOCKED, action=None,
        reason_code=DecisionReason.DAILY_MAX_LOSS_POSITIONS,
        reason_text=None, signal_detected=False, entry_attempted=False,
        order_submitted=False, trade_executed=False, position_id=None,
        strategy_event_id=None, simulated_order_id=None,
        reference_price=None, side=None, learning_eligible=False,
        replay_eligible=False, technical_failure=False, finished_at=NOW,
        details={},
    )
    values.update(changes)
    with pytest.raises(ValueError, match=message):
        FinalDecision(**values)


def test_paper_simulation_rejects_live_context():
    with pytest.raises(ValueError, match="paper_mode"):
        FinalDecision.paper_simulation(
            context(paper_mode=False), DecisionReason.SSOT_EXECUTE_AND_RECORD,
            finished_at=NOW)


def test_reason_classification_is_scoped_and_unknown_visible():
    assert classify_legacy_reason("NO_SIGNAL") is DecisionReason.NO_SIGNAL
    assert classify_legacy_reason("future-unmapped") is DecisionReason.UNKNOWN
    assert classify_legacy_reason("STOP_LOSS") is DecisionReason.UNKNOWN
    assert classify_legacy_reason("CANDLE_MISSING_FIELDS") is DecisionReason.UNKNOWN


def test_reference_price_rejects_float_to_preserve_financial_precision():
    with pytest.raises(TypeError, match="Decimal"):
        FinalDecision.no_trade(
            context(), DecisionReason.NO_SIGNAL, finished_at=NOW,
            reference_price=100.0,
        )


@pytest.mark.parametrize(
    ("result", "stage"),
    [
        (
            {"ledger_ok": True, "live_attempted": False,
             "order_accepted": False, "live_ok": False,
             "blocked_reason": "LIVE_ORDER_SUPPRESSED"},
            ExecutionStage.SUPPRESSED,
        ),
        (
            {"ledger_ok": True, "live_attempted": True,
             "order_accepted": False, "live_ok": False},
            ExecutionStage.REJECTED_BEFORE_ACK,
        ),
        (
            {"ledger_ok": True, "live_attempted": True,
             "order_accepted": True, "live_ok": False},
            ExecutionStage.ACCEPTED_PENDING,
        ),
        (
            {"ledger_ok": True, "live_attempted": True,
                 "order_accepted": True, "live_ok": True,
                 "executed": True, "fully_executed": True,
                 "executed_qty": 1.0, "requested_qty": 1.0},
            ExecutionStage.FILLED,
        ),
        (
            {"ledger_ok": True, "live_attempted": True,
             "order_accepted": True, "live_ok": False,
             "executed": True, "fully_executed": False,
             "executed_qty": 0.4, "requested_qty": 1.0},
            ExecutionStage.PARTIALLY_FILLED,
        ),
        (
            {"ledger_ok": False, "live_attempted": False,
             "order_accepted": False, "live_ok": False},
            ExecutionStage.LEDGER_FAILURE,
        ),
    ],
)
def test_execution_outcome_stage_mapping(result, stage):
    outcome = ExecutionOutcome.from_legacy(result)
    assert outcome.stage is stage
    assert outcome.order_accepted is result["order_accepted"]
    assert outcome.executed is bool(result.get("executed", result["live_ok"]))


@pytest.mark.parametrize(
    "result",
    [
        {"ledger_ok": True, "live_attempted": False,
         "order_accepted": True, "live_ok": False},
        {"ledger_ok": True, "live_attempted": False,
         "order_accepted": False, "live_ok": True},
        {"ledger_ok": True, "live_attempted": True,
         "order_accepted": False, "live_ok": True},
        {"ledger_ok": True, "live_attempted": True,
         "order_accepted": False, "live_ok": False, "suppressed": True},
    ],
)
def test_execution_outcome_rejects_inconsistent_legacy_flags(result):
    with pytest.raises(ExecutionOutcomeInvariantError):
        ExecutionOutcome.from_legacy(result)


def test_execution_outcome_legacy_ack_default_is_false_and_raw_is_detached():
    raw = {
        "ledger_ok": True,
        "live_attempted": True,
        "live_ok": False,
        "details": {"stages": [{"name": "market"}]},
    }
    outcome = ExecutionOutcome.from_legacy(raw)
    raw["details"]["stages"][0]["name"] = "changed"

    assert outcome.order_accepted is False
    assert outcome.stage is ExecutionStage.REJECTED_BEFORE_ACK
    assert outcome.raw["details"]["stages"][0]["name"] == "market"


def test_execution_outcome_rejects_stage_mismatch():
    with pytest.raises(ExecutionOutcomeInvariantError, match="does not match"):
        ExecutionOutcome(
            attempted=False,
            order_accepted=False,
            executed=False,
            fully_executed=False,
            operation_succeeded=False,
            executed_qty=0.0,
            requested_qty=None,
            ledger_ok=True,
            suppressed=False,
            blocked_reason=None,
            order_id=None,
            client_order_id=None,
            exchange_status=None,
            error=None,
            stage=ExecutionStage.FILLED,
            raw={},
        )


@pytest.mark.parametrize(
    "result",
    [
        {"ledger_ok": True, "live_attempted": True,
         "order_accepted": True, "executed": False,
         "executed_qty": 0.1},
        {"ledger_ok": True, "live_attempted": True,
         "order_accepted": True, "executed": True,
         "executed_qty": 0.0},
        {"ledger_ok": True, "live_attempted": True,
         "order_accepted": True, "executed": False,
         "fully_executed": True, "executed_qty": 0.0},
    ],
)
def test_execution_outcome_rejects_inconsistent_fill_quantities(result):
    with pytest.raises(ExecutionOutcomeInvariantError):
        ExecutionOutcome.from_legacy(result)


@pytest.mark.parametrize(
    ("case", "expected_accepted", "expected_executed"),
    [
        ("maker_rejected", False, False),
        ("polling_failure", True, False),
        ("cancel_failure", True, False),
        ("fallback_rejected", True, False),
        ("fallback_accepted_no_fill", True, False),
        ("fallback_fill", True, True),
    ],
)
def test_maker_fallback_preserves_any_prior_ack(
    monkeypatch, case, expected_accepted, expected_executed
):
    class FakeClient:
        def __init__(self):
            self.calls = {"maker": 0, "poll": 0, "cancel": 0, "market": 0}

        def place_limit_maker_order(self, **_kwargs):
            self.calls["maker"] += 1
            if case == "maker_rejected":
                raise RuntimeError("maker rejected")
            return {"orderId": "maker-1", "status": "NEW", "executedQty": "0"}

        def get_order_status(self, **_kwargs):
            self.calls["poll"] += 1
            if case == "polling_failure":
                raise RuntimeError("poll failed")
            return {"orderId": "maker-1", "status": "NEW", "executedQty": "0"}

        def cancel_order_by_id(self, **_kwargs):
            self.calls["cancel"] += 1
            if case == "cancel_failure":
                raise RuntimeError("cancel failed")
            return {"orderId": "maker-1", "status": "CANCELED"}

        def place_market_order(self, **_kwargs):
            self.calls["market"] += 1
            if case in {"polling_failure", "cancel_failure", "fallback_rejected"}:
                raise RuntimeError("fallback rejected")
            if case == "fallback_accepted_no_fill":
                return {"orderId": "market-1", "status": "NEW", "executedQty": "0"}
            return {"orderId": "market-1", "status": "FILLED", "executedQty": "1"}

    client = FakeClient()
    monkeypatch.setattr(execution, "get_best_bid_ask", lambda *_args: (99.0, 100.0))
    monkeypatch.setattr(execution.time, "sleep", lambda *_args: None)
    if case == "polling_failure":
        ticks = iter((0.0, 0.0, 2.0))
        monkeypatch.setattr(execution.time, "time", lambda: next(ticks))

    result = execution.place_live_exit_maker_then_market(
        client,
        "BTCUSDC",
        "SELL",
        1.0,
        base_client_order_id="exit-1",
        timeout_sec=1 if case == "polling_failure" else 0,
        poll_sec=0.1,
    )

    assert result["attempted"] is True
    assert result["order_accepted"] is expected_accepted
    assert result["executed"] is expected_executed
    assert result["live_ok"] is expected_executed
    assert client.calls["maker"] == 1
    if case == "maker_rejected":
        assert client.calls["market"] == 0
    else:
        assert client.calls["market"] == 1
    if case == "polling_failure":
        assert client.calls["poll"] == 1
    if case == "cancel_failure":
        assert result["resp"]["cancel"]["error"] == "cancel failed"


@pytest.mark.parametrize(
    (
        "case", "response", "expected_status", "expected_order_id",
        "expected_accepted", "expected_executed_qty", "expected_full",
    ),
    [
        ("response_rejected", {"orderId": "rejected-1", "status": "REJECTED",
         "executedQty": "0"}, "REJECTED", "rejected-1", False, 0.0, False),
        ("new_ack", {"orderId": "new-1", "status": "NEW", "executedQty": "0"},
         "NEW", "new-1", True, 0.0, False),
        ("partial_fill", {"orderId": "partial-1", "status": "PARTIALLY_FILLED",
         "executedQty": "0.4"}, "PARTIALLY_FILLED", "partial-1",
         True, 0.4, False),
        ("filled", {"orderId": "filled-1", "status": "FILLED",
         "executedQty": "1"}, "FILLED", "filled-1", True, 1.0, True),
    ],
)
def test_place_live_order_success_metadata(
    monkeypatch, case, response, expected_status, expected_order_id,
    expected_accepted, expected_executed_qty, expected_full,
):
    class Client:
        def place_market_order(self, **_kwargs):
            return response

    monkeypatch.setattr(
        execution, "preflight_live_order",
        lambda *_args, **_kwargs: {"ok": True, "qty_adj": 1.0},
    )
    result = execution.place_live_order(
        Client(), "BTCUSDC", "BUY", 1.0,
        trading_mode="LIVE", live_orders_enabled=True, quote_asset="USDC",
        panic_disable_trading=False, live_max_notional=0.0,
        client_order_id=f"cid-{case}",
    )

    assert result["attempted"] is True
    assert result["order_accepted"] is expected_accepted
    assert result["order_id"] == expected_order_id
    assert result["client_order_id"] == f"cid-{case}"
    assert result["status"] == expected_status
    assert result["exchange_status"] == expected_status
    assert result["executed_qty"] == expected_executed_qty
    assert result["fully_executed"] is expected_full


def test_place_live_order_preflight_metadata(monkeypatch):
    monkeypatch.setattr(
        execution, "preflight_live_order",
        lambda *_args, **_kwargs: {"ok": False, "reason": "LIVE_ORDERS_DISABLED"},
    )
    result = execution.place_live_order(
        object(), "BTCUSDC", "BUY", 1.0,
        trading_mode="LIVE", live_orders_enabled=False, quote_asset="USDC",
        panic_disable_trading=False, live_max_notional=0.0,
        client_order_id="cid-preflight",
    )

    assert result["attempted"] is False
    assert result["order_accepted"] is False
    assert result["order_id"] is None
    assert result["client_order_id"] == "cid-preflight"
    assert result["status"] == "LIVE_ORDERS_DISABLED"
    assert result["executed_qty"] == 0.0


def test_place_live_order_exception_preserves_client_id(monkeypatch):
    class Client:
        def place_market_order(self, **_kwargs):
            raise ExchangeAPIException("rejected", code="51008")

    monkeypatch.setattr(
        execution, "preflight_live_order",
        lambda *_args, **_kwargs: {"ok": True, "qty_adj": 1.0},
    )
    result = execution.place_live_order(
        Client(), "BTCUSDC", "BUY", 1.0,
        trading_mode="LIVE", live_orders_enabled=True, quote_asset="USDC",
        panic_disable_trading=False, live_max_notional=0.0,
        client_order_id="cid-exception",
    )

    assert result["attempted"] is True
    assert result["order_accepted"] is False
    assert result["order_id"] is None
    assert result["client_order_id"] == "cid-exception"
    assert result["status"] == "EXCHANGE_REJECTED"
    assert result["executed_qty"] == 0.0


@pytest.mark.parametrize(
    ("case", "expected_qty", "expected_full", "expected_live_ok"),
    [
        ("partial_poll_failure", 0.4, False, False),
        ("partial_cancel_failure", 0.4, False, False),
        ("partial_fallback_rejected", 0.4, False, False),
        ("partial_fallback_no_fill", 0.4, False, False),
        ("partial_fallback_partial", 0.7, False, True),
        ("partial_fallback_completes", 1.0, True, True),
        ("no_fill_fallback_no_fill", 0.0, False, False),
        ("maker_full_fill", 1.0, True, True),
    ],
)
def test_maker_fallback_aggregates_partial_fill(
    monkeypatch, case, expected_qty, expected_full, expected_live_ok
):
    class Client:
        def __init__(self):
            self.polls = 0
            self.market_calls = 0

        def place_limit_maker_order(self, **_kwargs):
            return {"orderId": "maker-1", "status": "NEW", "executedQty": "0"}

        def get_order_status(self, **_kwargs):
            self.polls += 1
            if case == "partial_poll_failure" and self.polls > 1:
                raise RuntimeError("poll failed")
            if case == "maker_full_fill":
                return {"orderId": "maker-1", "status": "FILLED", "executedQty": "1"}
            if case == "no_fill_fallback_no_fill":
                return {"orderId": "maker-1", "status": "NEW", "executedQty": "0"}
            return {"orderId": "maker-1", "status": "PARTIALLY_FILLED",
                    "executedQty": "0.4"}

        def cancel_order_by_id(self, **_kwargs):
            if case == "partial_cancel_failure":
                raise RuntimeError("cancel failed")
            return {"orderId": "maker-1", "status": "CANCELED"}

        def place_market_order(self, **_kwargs):
            self.market_calls += 1
            if case in {
                "partial_poll_failure", "partial_cancel_failure",
                "partial_fallback_rejected",
            }:
                raise RuntimeError("fallback rejected")
            if case in {"partial_fallback_no_fill", "no_fill_fallback_no_fill"}:
                return {"orderId": "market-1", "status": "NEW", "executedQty": "0"}
            if case == "partial_fallback_partial":
                return {"orderId": "market-1", "status": "PARTIALLY_FILLED",
                        "executedQty": "0.3"}
            return {"orderId": "market-1", "status": "FILLED",
                    "executedQty": "0.6"}

    client = Client()
    monkeypatch.setattr(execution, "get_best_bid_ask", lambda *_args: (99.0, 100.0))
    monkeypatch.setattr(execution.time, "sleep", lambda *_args: None)
    ticks = (
        iter((0.0, 0.0, 0.5, 2.0))
        if case == "partial_poll_failure"
        else iter((0.0, 0.0, 2.0))
    )
    monkeypatch.setattr(execution.time, "time", lambda: next(ticks))

    result = execution.place_live_exit_maker_then_market(
        client, "BTCUSDC", "SELL", 1.0,
        base_client_order_id="exit-partial", timeout_sec=1, poll_sec=0.1,
    )

    assert result["attempted"] is True
    assert result["order_accepted"] is True
    assert result["executed"] is (expected_qty > 0.0)
    assert result["fully_executed"] is expected_full
    assert result["executed_qty"] == pytest.approx(expected_qty)
    assert result["remaining_qty"] == pytest.approx(max(0.0, 1.0 - expected_qty))
    assert result["live_ok"] is expected_live_ok
    if case == "maker_full_fill":
        assert client.market_calls == 0
    else:
        assert client.market_calls == 1

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from common.decision_contract import (
    DecisionReason,
    DecisionSubtype,
    DecisionType,
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
    ]
    assert results[0].learning_eligible is False
    assert results[1].signal_detected is True
    assert results[2].replay_eligible is True
    assert results[3].trade_executed is False
    assert results[4].evaluation.paper_mode is True
    assert results[5].technical_failure is True


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


def test_reference_price_rejects_float_to_preserve_financial_precision():
    with pytest.raises(TypeError, match="Decimal"):
        FinalDecision.no_trade(
            context(), DecisionReason.NO_SIGNAL, finished_at=NOW,
            reference_price=100.0,
        )

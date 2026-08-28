from datetime import datetime, timezone
from decimal import Decimal

import pytest

from common.ownership_rsi_after_bbrange_v1 import (
    BLOCK_REASON,
    CONTROL_REASON,
    configured_mode,
    evaluate_ownership_admission,
)
from common.decision_contract import (
    DecisionReason,
    DecisionSubtype,
    EvaluationContext,
    FinalDecision,
)
from common.paper_opportunity_observation import build_paper_opportunity_envelope
from common.paper_opportunity_observation import SUPPORTED_OUTCOME_HORIZONS


NOW = datetime(2026, 8, 27, tzinfo=timezone.utc)


class Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.sql = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return self.rows


class Connection:
    def __init__(self, rows):
        self.cursor_value = Cursor(rows)
        self.readonly = False
        self.closed = False

    def set_session(self, *, readonly):
        self.readonly = readonly

    def cursor(self):
        return self.cursor_value

    def rollback(self):
        return None

    def close(self):
        self.closed = True


def evaluate(*, mode="TREATMENT", rows=(), symbol="BTCUSDC", strategy="RSI", trading_mode="PAPER"):
    connection = Connection(rows)
    decision = evaluate_ownership_admission(
        lambda: connection,
        trading_mode=trading_mode,
        symbol=symbol,
        strategy=strategy,
        observed_at=NOW,
        market_regime="RANGE",
        environ={"OWNERSHIP_RSI_AFTER_BBRANGE_V1_MODE": mode},
    )
    return decision, connection


def test_rsi_same_symbol_open_bbrange_is_blocked_in_treatment():
    decision, connection = evaluate(rows=[(41, "5m", Decimal("0.125"))])
    assert decision.affected is True
    assert decision.blocked is True
    assert decision.reason == BLOCK_REASON
    assert decision.market_regime == "RANGE"
    assert connection.cursor_value.params == ("BTCUSDC",)
    assert "status='OPEN'" in connection.cursor_value.sql
    assert "remaining_inventory_qty>0" in connection.cursor_value.sql
    assert decision.details()["treatment_reason"] == BLOCK_REASON
    assert decision.details()["treatment_decision"] == "NO_TRADE"


def test_rsi_without_open_bbrange_is_allowed():
    decision, _ = evaluate(rows=[])
    assert decision.affected is False
    assert decision.blocked is False
    assert decision.reason == "NO_OPEN_BBRANGE"


def test_same_candidate_in_control_is_observed_but_not_blocked():
    decision, _ = evaluate(mode="CONTROL", rows=[(41, "1m", Decimal("1"))])
    assert decision.affected is True
    assert decision.blocked is False
    assert decision.effective is False
    assert decision.reason == CONTROL_REASON


def test_bbrange_on_different_symbol_is_not_selected():
    decision, connection = evaluate(symbol="ETHUSDC", rows=[])
    assert connection.cursor_value.params == ("ETHUSDC",)
    assert decision.affected is False
    assert decision.blocked is False


@pytest.mark.parametrize("excluded_state", ["CLOSED", "ZERO_REMAINING"])
def test_closed_or_zero_remaining_bbrange_is_not_selected(excluded_state):
    decision, connection = evaluate(rows=[])
    assert decision.blocked is False
    assert "status='OPEN'" in connection.cursor_value.sql
    assert "remaining_inventory_qty>0" in connection.cursor_value.sql
    assert excluded_state in {"CLOSED", "ZERO_REMAINING"}


@pytest.mark.parametrize("strategy", ["BBRANGE", "TREND", "SUPERTREND"])
def test_non_rsi_candidates_are_unchanged_without_database_read(strategy):
    decision = evaluate_ownership_admission(
        lambda: pytest.fail("non-RSI candidate must not query ownership"),
        trading_mode="PAPER",
        symbol="BTCUSDC",
        strategy=strategy,
        observed_at=NOW,
        market_regime="TREND",
        environ={"OWNERSHIP_RSI_AFTER_BBRANGE_V1_MODE": "TREATMENT"},
    )
    assert decision.affected is False
    assert decision.blocked is False


def test_admission_result_has_no_sizing_fee_or_exit_controls():
    decision, _ = evaluate(rows=[(41, "5m", Decimal("0.125"))])
    fields = set(vars(decision))
    assert not fields.intersection({"size", "qty", "fee", "exit", "stop_loss", "take_profit"})


def test_local_live_path_is_unchanged_without_database_read():
    decision = evaluate_ownership_admission(
        lambda: pytest.fail("LIVE path must not query ownership"),
        trading_mode="LIVE",
        symbol="BTCUSDC",
        strategy="RSI",
        observed_at=NOW,
        market_regime="RANGE",
        environ={"OWNERSHIP_RSI_AFTER_BBRANGE_V1_MODE": "TREATMENT"},
    )
    assert decision.effective is False
    assert decision.blocked is False


def test_off_is_exact_fail_open_default_and_invalid_mode_fails_closed():
    decision = evaluate_ownership_admission(
        lambda: pytest.fail("OFF must not query"),
        trading_mode="PAPER",
        symbol="BTCUSDC",
        strategy="RSI",
        observed_at=NOW,
        market_regime="RANGE",
        environ={},
    )
    assert decision.blocked is False
    assert decision.effective is False
    with pytest.raises(RuntimeError, match="INVALID_OWNERSHIP_RSI_AFTER_BBRANGE_V1_MODE"):
        configured_mode({"OWNERSHIP_RSI_AFTER_BBRANGE_V1_MODE": "INVALID"})


def test_treatment_block_remains_full_opportunity_outcome_eligible():
    ownership, _ = evaluate(rows=[(41, "5m", Decimal("0.125"))])
    context = EvaluationContext(
        deployment_id="local-paper",
        environment="trading_paper",
        symbol="BTCUSDC",
        interval="1m",
        strategy="RSI",
        candle_open_time=NOW,
        evaluation_started_at=NOW,
        engine_name="RSI",
        market_regime="RANGE",
        runtime_enabled=True,
        live_orders_enabled=False,
        paper_mode=True,
    )
    blocked = FinalDecision.entry_blocked(
        context,
        DecisionReason.POLICY_BLOCK,
        DecisionSubtype.READINESS_BLOCKED,
        finished_at=NOW,
        reference_price=Decimal("50000"),
        side="BUY",
        signal_detected=True,
        reason_text=BLOCK_REASON,
        details=ownership.details(),
    )
    envelope = build_paper_opportunity_envelope(
        blocked,
        environ={
            "FULL_PAPER_OPPORTUNITY_OBSERVATION_V1_ENABLED": "1",
            "TRADING_MODE": "PAPER",
            "PAPER_SIMULATION_FEE_RATE": "0.0035",
            "GIT_SHA": "a" * 40,
        },
    )
    assert envelope is not None
    assert envelope.outcome_eligible is True
    assert envelope.treatment_name == "OWNERSHIP_RSI_AFTER_BBRANGE_V1"
    assert envelope.treatment_decision == "NO_TRADE"
    assert envelope.treatment_reason == BLOCK_REASON
    assert envelope.fee_rate_entry == Decimal("0.0035")
    assert envelope.fee_rate_exit == Decimal("0.0035")
    assert envelope.fee_model_version == "PAPER_SIMULATOR_FINANCIAL_MODEL_V2"
    assert tuple(SUPPORTED_OUTCOME_HORIZONS) == (15, 30, 60, 240)

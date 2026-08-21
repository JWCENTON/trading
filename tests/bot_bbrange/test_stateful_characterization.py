from __future__ import annotations

import pytest
from common.decision_contract import DecisionType, FinalDecision

from tests.bot_bbrange.fixtures import band_frame, candle, runtime_snapshot


def reasons(cycle):
    return [event.payload.get("reason") for event in cycle.events]


def kinds(cycle):
    return [operation.kind for operation in cycle.operations]


def test_lifecycle_no_signal_and_invalid_data(stateful_bbrange):
    no_signal = stateful_bbrange.cycle(candle())
    assert isinstance(no_signal.returned_value, FinalDecision)
    assert no_signal.returned_value.decision_type is DecisionType.NO_TRADE
    assert [event.payload["event_type"] for event in no_signal.events][0] == "RUN_START"
    assert reasons(no_signal)[-2:] == ["NO_SIGNAL", "DONE"]
    invalid = stateful_bbrange.cycle(candle(ema=None))
    assert invalid.returned_value.reason_code.value == "INDICATORS_NOT_READY"
    assert reasons(invalid) == ["ENTER", "INDICATORS_NOT_READY", "DONE"]
    assert not [x for x in invalid.operations if x.kind == "execution"]


@pytest.mark.parametrize("control, reason", [("halt", "BOT_MODE_HALT"),
                                               ("disabled", "BOT_DISABLED"),
                                               ("regime", "REGIME_BLOCK")])
def test_control_and_regime_blocks(stateful_bbrange, control, reason):
    h = stateful_bbrange
    if control == "halt":
        h.mode = "HALT"
    elif control == "disabled":
        h.enabled = False
    else:
        h.regime_allow = False
    observed = h.cycle(candle(close=90.0, low=89.0))
    assert reason in reasons(observed)
    assert isinstance(observed.returned_value, FinalDecision)
    assert "execution" not in kinds(observed)


def test_insufficient_candles_and_invalid_bands(stateful_bbrange, monkeypatch):
    h = stateful_bbrange
    monkeypatch.setattr(h.module.pd, "read_sql_query",
                        lambda *_a, **_k: band_frame().iloc[:2])
    assert "NOT_ENOUGH_CANDLES" in reasons(h.cycle(candle(close=90.0, low=89.0)))
    frame = band_frame().copy()
    frame["close"] = float("nan")
    monkeypatch.setattr(h.module.pd, "read_sql_query", lambda *_a, **_k: frame)
    assert "BB_NOT_READY" in reasons(h.cycle(candle(close=90.0, low=89.0)))


@pytest.mark.parametrize(
    "mode,result,opens",
    [
        ("PAPER", None, True),
        ("LIVE", None, True),
        ("LIVE", {"ledger_ok": True, "live_attempted": False,
                  "live_ok": False, "blocked_reason": "LIVE_ORDER_SUPPRESSED"}, False),
        ("LIVE", {"ledger_ok": True, "live_attempted": True,
                  "live_ok": False, "blocked_reason": "REJECTED"}, False),
        ("PAPER", {"ledger_ok": False, "live_attempted": False,
                   "live_ok": False, "blocked_reason": "DB_GUARD_DUPLICATE"}, False),
    ],
)
def test_entry_outcomes_and_exactly_once(stateful_bbrange, mode, result, opens):
    h = stateful_bbrange
    h.trading_mode = mode
    h.execution_result = result
    observed = h.cycle(candle(close=90.0, low=89.0))
    assert kinds(observed).count("execution") == 1
    assert kinds(observed).count("position_open") == (1 if opens else 0)
    assert (observed.position is not None) is opens
    assert isinstance(observed.returned_value, FinalDecision)
    assert observed.returned_value.evaluation.context["contract_version"] == "FINAL_DECISION_V1"


def test_entry_hold_take_profit_stateful_sequence(stateful_bbrange):
    h = stateful_bbrange
    entry = h.cycle(candle(close=90.0, low=89.0))
    hold = h.cycle(candle(close=90.2, high=90.4, low=90.0))
    exited = h.cycle(candle(close=91.5, high=92.0, low=91.0))
    assert entry.position is not None
    assert entry.returned_value.decision_type is DecisionType.PAPER_SIMULATION
    assert hold.position is not None and "POSITION_OPEN_NO_EXIT" in reasons(hold)
    assert hold.returned_value.action == "HOLD"
    assert kinds(hold).count("execution") == 0
    assert exited.position is None
    assert exited.returned_value.reason_code.value == "TAKE_PROFIT"
    assert kinds(exited).count("execution") == 1
    assert kinds(exited).count("position_close") == 1


def test_stop_loss_confirmed_close_exactly_once(stateful_bbrange):
    h = stateful_bbrange
    h.set_position()
    observed = h.cycle(candle(close=99.0, high=100.0, low=98.0))
    assert observed.position is None
    assert observed.returned_value.reason_code.value == "STOP_LOSS"
    assert kinds(observed).count("execution") == 1
    assert kinds(observed).count("position_close") == 1
    assert "STOP LOSS" in next(
        x for x in observed.operations if x.kind == "position_close"
    ).payload["reason"]


def test_long_uses_frozen_boundary_before_legacy_stop(stateful_bbrange):
    h = stateful_bbrange
    h.set_position()
    h.monkeypatch.setattr(
        h.module, "load_frozen_boundary_price", lambda *_args, **_kwargs: "99.8",
    )
    observed = h.cycle(candle(close=99.7, high=100.0, low=99.7))
    assert observed.returned_value.reason_code.value == "STOP_LOSS"
    assert observed.position is None


def test_profit_lock_armed_state_persists_then_giveback_exits(stateful_bbrange):
    h = stateful_bbrange
    h.set_position()
    h.profit_lock_state = "ARMED"
    armed = h.cycle(candle(close=100.2, high=100.4, low=100.0))
    assert armed.position is not None
    assert armed.returned_value.action == "HOLD"
    assert next(x for x in armed.operations
                if x.kind == "profit_lock_event").payload["reason"] == "ARMED_WAITING"
    h.profit_lock_state = "TRIGGERED"
    exited = h.cycle(candle(close=100.1, high=100.3, low=99.9))
    assert exited.position is None
    assert exited.returned_value.reason_code.value == "PROFIT_LOCK"
    assert kinds(exited).count("position_close") == 1


def test_block_unblock_signal_and_suppressed_retry(stateful_bbrange):
    h = stateful_bbrange
    h.regime_allow = False
    blocked = h.cycle(candle(close=90.0, low=89.0))
    h.regime_allow = True
    h.trading_mode = "LIVE"
    h.execution_result = {"ledger_ok": True, "live_attempted": False,
                          "live_ok": False, "blocked_reason": "LIVE_ORDER_SUPPRESSED"}
    suppressed = h.cycle(candle(close=90.0, low=89.0))
    h.execution_result = None
    entered = h.cycle(candle(close=90.0, low=89.0))
    assert "REGIME_BLOCK" in reasons(blocked)
    assert blocked.returned_value.decision_type is DecisionType.ENTRY_BLOCKED
    assert suppressed.position is None
    assert suppressed.returned_value.decision_type is DecisionType.ENTRY_SUPPRESSED
    assert entered.position is not None
    assert entered.returned_value.decision_type is DecisionType.TRADE_EXECUTED


def test_panic_exit_and_error_then_recovery(stateful_bbrange, monkeypatch):
    h = stateful_bbrange
    h.set_position()
    h.mode = "PANIC"
    panic = h.cycle(candle())
    assert panic.position is None
    assert panic.returned_value.action == "EXIT"
    assert kinds(panic).count("execution") == 1
    h.mode = "NORMAL"
    original = h.module.get_runtime_snapshot
    monkeypatch.setattr(h.module, "get_runtime_snapshot",
                        lambda **_k: (_ for _ in ()).throw(RuntimeError("fixture")))
    with pytest.raises(RuntimeError, match="fixture"):
        h.cycle(candle())
    monkeypatch.setattr(h.module, "get_runtime_snapshot", original)
    recovered = h.cycle(candle())
    assert "NO_SIGNAL" in reasons(recovered)
    assert recovered.returned_value.decision_type is DecisionType.NO_TRADE


def test_final_decision_no_row_preserves_identity_and_reason(stateful_bbrange):
    decision = stateful_bbrange.cycle(None).returned_value
    assert decision.decision_type is DecisionType.SYSTEM_NOT_EVALUATED
    assert decision.reason_text == "NO_ROW"
    assert decision.evaluation.symbol == "BTCUSDC"
    assert decision.evaluation.interval == "1m"
    assert decision.evaluation.strategy == "BBRANGE"
    assert decision.details["has_row"] is False


@pytest.mark.parametrize(
    "result, expected_type, expected_subtype",
    [
        ({"ledger_ok": True, "live_attempted": False, "live_ok": False,
          "blocked_reason": "LIVE_ORDER_SUPPRESSED"},
         DecisionType.ACTION_SUPPRESSED, "EXECUTION_NOT_ATTEMPTED"),
        ({"ledger_ok": True, "live_attempted": True, "live_ok": False,
          "blocked_reason": "REJECTED"},
         DecisionType.TECHNICAL_FAILURE, "ORDER_REJECTED"),
        ({"ledger_ok": False, "live_attempted": False, "live_ok": False,
          "blocked_reason": "DB_GUARD_DUPLICATE"},
         DecisionType.TECHNICAL_FAILURE, "LEDGER_FAILURE"),
    ],
)
def test_final_decision_failed_exit_preserves_position_and_exactly_once(
    stateful_bbrange, result, expected_type, expected_subtype,
):
    h = stateful_bbrange
    h.trading_mode = "LIVE"
    h.execution_result = result
    h.set_position()
    observed = h.cycle(candle(close=102.0, high=102.0, low=101.0))
    assert observed.returned_value.decision_type is expected_type
    assert observed.returned_value.decision_subtype.value == expected_subtype
    assert observed.position is not None
    assert kinds(observed).count("execution") == 1
    assert kinds(observed).count("position_close") == 0
    assert reasons(observed)[-1] == "DONE"


def test_panic_without_position_returns_decision_without_new_terminal_event(
    stateful_bbrange,
):
    h = stateful_bbrange
    h.mode = "PANIC"
    observed = h.cycle(candle())
    assert observed.returned_value.reason_text == "PANIC_NO_POSITION"
    assert reasons(observed) == ["ENTER", "DONE"]
    assert kinds(observed).count("execution") == 0


def test_partial_take_profit_reduces_qty_without_position_closed_event(
    stateful_bbrange,
):
    h = stateful_bbrange
    h.trading_mode = "LIVE"
    h.set_position(price=100.0)
    h.execution_result = {
        "ledger_ok": True, "live_attempted": True, "order_accepted": True,
        "executed": True, "fully_executed": False,
        "executed_qty": 0.04, "requested_qty": 0.1,
        "live_ok": True, "blocked_reason": None,
    }
    observed = h.cycle(candle(close=102.0, high=102.0, low=101.0))
    assert observed.position is not None
    assert observed.position[2] == pytest.approx(0.06)
    assert observed.returned_value.decision_subtype.value == "PARTIAL_EXECUTION"
    assert kinds(observed).count("position_reduced") == 1
    assert kinds(observed).count("position_close") == 0

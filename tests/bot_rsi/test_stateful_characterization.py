from __future__ import annotations

import importlib
import importlib.util
import inspect
import socket
import sys
import threading
from pathlib import Path

import psycopg2
import pytest
import requests

from common.decision_contract import (
    DecisionReason,
    DecisionSubtype,
    DecisionType,
    FinalDecision,
)
from tests.bot_rsi.fixtures import (
    RsiStatefulHarness,
    StrictFakeExchange,
    candle,
    entry_candle,
    entry_previous_candle,
    runtime_snapshot,
)


@pytest.fixture
def rsi(monkeypatch):
    safe_env = {
        "SYMBOL": "BTCUSDC",
        "QUOTE_ASSET": "USDC",
        "STRATEGY_NAME": "RSI",
        "INTERVAL": "1m",
        "TRADING_MODE": "PAPER",
        "LIVE_ORDERS_ENABLED": "0",
        "DB_HOST": "invalid.test",
        "DB_NAME": "unit_test",
        "DB_USER": "unit_test",
        "DB_PASS": "unit_test",
        "EXCHANGE_PROVIDER": "OKX",
        "DAILY_MAX_LOSS_PCT": "0",
        "DISABLE_HOURS": "",
    }
    for key, value in safe_env.items():
        monkeypatch.setenv(key, value)
    for key in (
        "BINANCE_API_KEY",
        "BINANCE_API_SECRET",
        "OKX_API_KEY",
        "OKX_API_SECRET",
        "OKX_PASSPHRASE",
    ):
        monkeypatch.delenv(key, raising=False)

    blocked = lambda *_args, **_kwargs: pytest.fail("external I/O during import")
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(requests.sessions.Session, "request", blocked)
    monkeypatch.setattr(psycopg2, "connect", blocked)

    exchange_client = importlib.import_module("common.exchange_client")
    factory_calls = []
    monkeypatch.setattr(
        exchange_client,
        "get_market_data_client",
        lambda: factory_calls.append("called") or StrictFakeExchange(),
    )

    module_name = "waltrade_bot_rsi_main_characterized"
    sys.modules.pop(module_name, None)
    source = Path(__file__).resolve().parents[2] / "bot" / "main.py"
    spec = importlib.util.spec_from_file_location(module_name, source)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    assert factory_calls == []
    assert module._exchange_client is None
    return module


@pytest.fixture
def harness(monkeypatch, rsi):
    def no_thread_start(*_args, **_kwargs):
        pytest.fail("background thread started by RSI characterization")

    monkeypatch.setattr(threading.Thread, "start", no_thread_start)
    return RsiStatefulHarness(rsi, monkeypatch)


def event_types(observation):
    return tuple(
        event.payload.get("event_type")
        for event in observation.captured_events
        if event.channel == "strategy"
    )


def assert_isolated(observation):
    assert all(event.channel != "database" for event in observation.captured_events)
    assert all(event.channel != "decision_sink" for event in observation.captured_events)


def assert_operation_order(observation, *operations):
    indexes = [observation.operation_log.index(item) for item in operations]
    assert indexes == sorted(indexes)


def test_harness_is_test_only_and_rsi_has_no_decision_sink(rsi):
    signature = inspect.signature(rsi.run_strategy)
    source = inspect.getsource(rsi)
    assert tuple(signature.parameters) == ("row", "prev_row")
    assert "decision_sink" not in source
    assert "FinalDecision" in source


def test_no_closed_candle_is_explicit_no_action(harness):
    observed = harness.cycle(None)
    assert observed.candle_state == "NO_CLOSED_CANDLE"
    assert observed.action == "NO_ACTION"
    assert observed.reason is None
    assert observed.captured_events == ()
    assert observed.final_decision is None
    assert_isolated(observed)


def test_new_candle_is_processed_once_and_repeat_is_idle(harness):
    row = candle()
    first = harness.cycle(row, prev_row=candle(minute=-1))
    repeated = harness.cycle(row, prev_row=candle(minute=-1))

    assert first.candle_state == "NEW_CANDLE"
    assert first.reason == "NO_SIGNAL_REBOUND"
    assert "RUN_START" in event_types(first)
    assert repeated.candle_state == "ALREADY_PROCESSED"
    assert repeated.action == "NO_ACTION"
    assert repeated.reason == "NO_NEW_CANDLE"
    assert event_types(repeated) == ("TICK", "IDLE")
    assert repeated.order_attempts == ()
    assert repeated.final_decision.decision_type is DecisionType.SYSTEM_NOT_EVALUATED
    assert repeated.final_decision.decision_subtype is DecisionSubtype.NO_NEW_MARKET_DATA
    assert repeated.final_decision.action == "IDLE"
    assert repeated.final_decision.reason_code is DecisionReason.NO_NEW_CANDLE
    assert_isolated(first)
    assert_isolated(repeated)


def test_no_position_no_rsi_signal_is_observable_no_action(harness):
    observed = harness.cycle(candle(), prev_row=candle(minute=-1, rsi=40.0))
    assert observed.action == "NO_ACTION"
    assert observed.reason == "NO_SIGNAL_REBOUND"
    assert observed.position_before is None
    assert observed.position_after is None
    assert observed.order_attempts == ()
    assert observed.final_decision.decision_type is DecisionType.NO_TRADE
    assert observed.final_decision.action is None
    assert observed.final_decision.reason_code is DecisionReason.NO_SIGNAL
    assert_isolated(observed)


@pytest.mark.parametrize(
    ("missing", "expected_missing"),
    [
        ({"close": None}, ["close"]),
        ({"high": None}, ["high"]),
        ({"low": None}, ["low"]),
        ({"high": None, "low": None}, ["high", "low"]),
    ],
)
def test_missing_candle_fields_have_structured_reason(
    harness, missing, expected_missing
):
    observed = harness.cycle(candle(**missing))

    assert observed.final_decision.decision_type is DecisionType.SYSTEM_NOT_EVALUATED
    assert observed.final_decision.reason_code is DecisionReason.CANDLE_MISSING_FIELDS
    assert observed.final_decision.reason_text == "CANDLE_MISSING_FIELDS"
    assert observed.final_decision.details["missing_fields"] == tuple(expected_missing)
    blocked = next(
        event for event in observed.captured_events
        if event.channel == "strategy"
        and event.payload.get("event_type") == "BLOCKED"
    )
    assert blocked.payload["reason"] == "CANDLE_MISSING_FIELDS"
    assert observed.order_attempts == ()
    assert_isolated(observed)


def test_panic_without_position_uses_neutral_no_position_decision(harness):
    harness.runtime_mode = "PANIC"
    observed = harness.cycle(candle())

    decision = observed.final_decision
    assert decision.decision_type is DecisionType.NO_TRADE
    assert decision.decision_subtype is DecisionSubtype.NO_POSITION
    assert decision.action == "REJECT"
    assert decision.reason_code is DecisionReason.NO_OPEN_POSITION
    assert decision.reason_text == "PANIC_NO_POSITION"
    assert decision.entry_attempted is False
    assert decision.order_submitted is False
    assert decision.trade_executed is False
    assert "PANIC_NO_POSITION" in event_types(observed) or any(
        event.payload.get("reason") == "PANIC_NO_POSITION"
        for event in observed.captured_events if event.channel == "strategy"
    )
    assert observed.order_attempts == ()
    assert observed.position_after is None
    assert harness.runtime_mode == "HALT"
    assert_isolated(observed)


def test_valid_rebound_signal_attempts_entry_and_updates_fake_state(harness):
    observed = harness.cycle(entry_candle(), prev_row=entry_previous_candle())
    assert observed.action == "ENTRY_BUY"
    assert observed.signal == "BUY"
    assert observed.position_before is None
    assert observed.position_after[1] == "LONG"
    assert len(observed.order_attempts) == 1
    assert observed.order_attempts[0].intent == "ENTRY"
    assert [change.name for change in observed.state_changes] == ["position"]
    assert "POSITION_OPENED" in event_types(observed)
    assert observed.final_decision.decision_type is DecisionType.PAPER_SIMULATION
    assert observed.final_decision.action == "SIMULATE"
    assert observed.final_decision.side == "BUY"
    assert_isolated(observed)


def test_valid_signal_blocked_by_existing_regime_gate(harness):
    harness.regime_allow = False
    observed = harness.cycle(entry_candle(), prev_row=entry_previous_candle())
    assert observed.action == "NO_ACTION"
    assert observed.signal == "BUY"
    assert observed.reason == "REGIME_BLOCK"
    assert observed.order_attempts == ()
    assert observed.position_after is None
    assert observed.final_decision.decision_type is DecisionType.ENTRY_BLOCKED
    assert observed.final_decision.action == "BLOCK"
    assert observed.final_decision.reason_code is DecisionReason.REGIME_BLOCK
    assert_isolated(observed)


@pytest.mark.parametrize(
    ("mode", "enabled", "reason"),
    [("HALT", True, "BOT_MODE_HALT"), ("NORMAL", False, "BOT_DISABLED")],
)
def test_runtime_control_prevents_entry(harness, mode, enabled, reason):
    harness.runtime_mode = mode
    harness.runtime_enabled = enabled
    observed = harness.cycle(entry_candle(), prev_row=entry_previous_candle())
    assert observed.action == "NO_ACTION"
    assert observed.reason == reason
    assert observed.order_attempts == ()
    assert observed.position_after is None
    assert observed.final_decision.decision_type is DecisionType.ENTRY_SUPPRESSED
    assert observed.final_decision.action == "SUPPRESS"
    assert observed.final_decision.reason_text == reason
    assert_isolated(observed)


def test_open_position_hold(harness):
    harness.open_long()
    observed = harness.cycle(candle(close=100.1, high=100.2, low=99.9))
    assert observed.action == "NO_ACTION"
    assert observed.reason == "NO_EXIT_SIGNAL"
    assert observed.position_before == observed.position_after
    assert observed.order_attempts == ()
    assert "POSITION_HOLD" in event_types(observed)
    assert observed.final_decision.decision_type is DecisionType.NO_TRADE
    assert observed.final_decision.decision_subtype is DecisionSubtype.POSITION_MANAGEMENT
    assert observed.final_decision.action == "HOLD"
    assert observed.final_decision.reason_code is DecisionReason.POSITION_HOLD
    assert_isolated(observed)


def test_open_position_stop_loss(harness):
    harness.open_long()
    observed = harness.cycle(candle(close=99.1, high=100.0, low=99.0))
    assert observed.action == "EXIT_SELL"
    assert "STOP LOSS LONG" in observed.reason
    assert observed.order_attempts[0].exit_kind is None
    assert observed.position_after is None
    assert observed.final_decision.action == "EXIT"
    assert observed.final_decision.reason_code is DecisionReason.STOP_LOSS
    assert_isolated(observed)


def test_open_position_take_profit(harness):
    harness.open_long()
    observed = harness.cycle(candle(close=101.0, high=101.3, low=100.0))
    assert observed.action == "EXIT_SELL"
    assert observed.order_attempts[0].exit_kind == "TAKE_PROFIT"
    assert observed.position_after is None
    assert observed.final_decision.action == "EXIT"
    assert observed.final_decision.reason_code is DecisionReason.TAKE_PROFIT
    assert_isolated(observed)


def test_entry_execution_failure_is_observed_without_position(harness):
    harness.execution_ledger_ok = False
    observed = harness.cycle(entry_candle(), prev_row=entry_previous_candle())
    assert observed.action == "ENTRY_BUY"
    assert len(observed.order_attempts) == 1
    assert observed.position_after is None
    assert observed.final_decision.decision_type is DecisionType.TECHNICAL_FAILURE
    assert observed.final_decision.action == "ERROR"
    assert observed.final_decision.reason_code is DecisionReason.EXECUTION_FAILED
    assert_isolated(observed)


def test_open_position_profit_lock(harness):
    harness.open_long()
    harness.profit_lock_triggered = True
    observed = harness.cycle(candle(close=100.2, high=100.4, low=100.0))
    assert observed.action == "EXIT_SELL"
    assert observed.signal == "SELL"
    assert observed.order_attempts[0].exit_kind == "PROFIT_LOCK_LONG"
    assert any(
        event.channel == "profit_lock"
        and event.payload.get("event_type") == "PROFIT_LOCK_TRIGGERED"
        for event in observed.captured_events
    )
    assert observed.position_after is None
    assert observed.final_decision.action == "EXIT"
    assert observed.final_decision.reason_code is DecisionReason.PROFIT_LOCK
    assert_isolated(observed)


def test_open_position_soft_exit(harness):
    harness.open_long()
    observed = harness.cycle(candle(close=100.2, high=100.3, low=100.0, rsi=75.0))
    assert observed.action == "EXIT_SELL"
    assert observed.signal == "SELL"
    assert observed.order_attempts[0].exit_kind == "RSI_SOFT_EXIT"
    assert observed.position_after is None
    assert observed.final_decision.action == "EXIT"
    assert observed.final_decision.reason_code is DecisionReason.STRATEGY_EXIT
    assert_isolated(observed)


def test_open_position_time_exit_uses_fake_clock(harness):
    harness.time_exit_enabled = True
    harness.open_long(age_minutes=31)
    observed = harness.cycle(candle(close=100.0, high=100.2, low=99.9))
    assert observed.action == "EXIT_SELL"
    assert observed.signal == "SELL"
    assert observed.order_attempts[0].exit_kind == "TIME_EXIT"
    assert observed.position_after is None
    assert "EXIT_TIME" in event_types(observed)
    assert observed.final_decision.action == "EXIT"
    assert observed.final_decision.reason_code is DecisionReason.TIME_EXIT
    assert_isolated(observed)


def test_four_cycle_sequence_preserves_position_and_candle_state(harness):
    first = harness.cycle(
        candle(minute=0), prev_row=candle(minute=-1, rsi=40.0)
    )
    second = harness.cycle(
        entry_candle(minute=1), prev_row=entry_previous_candle(minute=0)
    )
    third = harness.cycle(
        entry_candle(minute=1), prev_row=entry_previous_candle(minute=0)
    )
    fourth = harness.cycle(candle(minute=2, close=99.9, high=100.0, low=99.7))

    assert [item.action for item in (first, second, third, fourth)] == [
        "NO_ACTION",
        "ENTRY_BUY",
        "NO_ACTION",
        "NO_ACTION",
    ]
    assert [item.candle_state for item in (first, second, third, fourth)] == [
        "NEW_CANDLE",
        "NEW_CANDLE",
        "ALREADY_PROCESSED",
        "NEW_CANDLE",
    ]
    assert second.position_after == third.position_after == fourth.position_after
    assert third.reason == "NO_NEW_CANDLE"
    assert fourth.reason == "NO_EXIT_SIGNAL"
    assert sum(len(item.order_attempts) for item in harness.observations) == 1
    assert [item.final_decision.action for item in (first, second, third, fourth)] == [
        None,
        "SIMULATE",
        "IDLE",
        "HOLD",
    ]
    for observed in harness.observations:
        assert_isolated(observed)


@pytest.mark.parametrize(
    ("path", "decision_type", "subtype", "action", "reason"),
    [
        ("duplicate", DecisionType.SYSTEM_NOT_EVALUATED,
         DecisionSubtype.NO_NEW_MARKET_DATA, "IDLE", DecisionReason.NO_NEW_CANDLE),
        ("no_signal", DecisionType.NO_TRADE,
         DecisionSubtype.NO_SIGNAL, None, DecisionReason.NO_SIGNAL),
        ("halt", DecisionType.ENTRY_SUPPRESSED,
         DecisionSubtype.EXECUTION_DISABLED, "SUPPRESS", DecisionReason.BOT_MODE_HALT),
        ("disabled", DecisionType.ENTRY_SUPPRESSED,
         DecisionSubtype.LIVE_DISABLED, "SUPPRESS", DecisionReason.BOT_DISABLED),
        ("regime", DecisionType.ENTRY_BLOCKED,
         DecisionSubtype.REGIME_BLOCKED, "BLOCK", DecisionReason.REGIME_BLOCK),
        ("entry", DecisionType.PAPER_SIMULATION,
         DecisionSubtype.PAPER_ONLY, "SIMULATE", DecisionReason.SSOT_EXECUTE_AND_RECORD),
        ("entry_buffer", DecisionType.SIGNAL_REJECTED,
         DecisionSubtype.READINESS_BLOCKED, "REJECT", DecisionReason.POLICY_BLOCK),
        ("execution_failure", DecisionType.TECHNICAL_FAILURE,
         DecisionSubtype.LEDGER_FAILURE, "ERROR", DecisionReason.EXECUTION_FAILED),
        ("hold", DecisionType.NO_TRADE,
         DecisionSubtype.POSITION_MANAGEMENT, "HOLD", DecisionReason.POSITION_HOLD),
        ("stop_loss", DecisionType.PAPER_SIMULATION,
         DecisionSubtype.EXIT_EXECUTED, "EXIT", DecisionReason.STOP_LOSS),
        ("take_profit", DecisionType.PAPER_SIMULATION,
         DecisionSubtype.EXIT_EXECUTED, "EXIT", DecisionReason.TAKE_PROFIT),
        ("profit_lock", DecisionType.PAPER_SIMULATION,
         DecisionSubtype.EXIT_EXECUTED, "EXIT", DecisionReason.PROFIT_LOCK),
        ("soft_exit", DecisionType.PAPER_SIMULATION,
         DecisionSubtype.EXIT_EXECUTED, "EXIT", DecisionReason.STRATEGY_EXIT),
        ("time_exit", DecisionType.PAPER_SIMULATION,
         DecisionSubtype.EXIT_EXECUTED, "EXIT", DecisionReason.TIME_EXIT),
    ],
)
def test_final_decision_path_mapping(
    harness, path, decision_type, subtype, action, reason
):
    if path == "duplicate":
        row = candle()
        harness.cycle(row, prev_row=candle(minute=-1))
        observed = harness.cycle(row, prev_row=candle(minute=-1))
    elif path == "no_signal":
        observed = harness.cycle(candle(), prev_row=candle(minute=-1, rsi=40.0))
    elif path == "halt":
        harness.runtime_mode = "HALT"
        observed = harness.cycle(entry_candle(), prev_row=entry_previous_candle())
    elif path == "disabled":
        harness.runtime_enabled = False
        observed = harness.cycle(entry_candle(), prev_row=entry_previous_candle())
    elif path == "regime":
        harness.regime_allow = False
        observed = harness.cycle(entry_candle(), prev_row=entry_previous_candle())
    elif path == "entry":
        observed = harness.cycle(entry_candle(), prev_row=entry_previous_candle())
    elif path == "entry_buffer":
        observed = harness.cycle(
            candle(close=99.9, high=100.0, low=99.7, rsi=35.0),
            prev_row=entry_previous_candle(),
        )
    elif path == "execution_failure":
        harness.execution_ledger_ok = False
        observed = harness.cycle(entry_candle(), prev_row=entry_previous_candle())
    else:
        harness.open_long(age_minutes=31 if path == "time_exit" else 10)
        if path == "hold":
            row = candle(close=100.1, high=100.2, low=99.9)
        elif path == "stop_loss":
            row = candle(close=99.1, high=100.0, low=99.0)
        elif path == "take_profit":
            row = candle(close=101.0, high=101.3, low=100.0)
        elif path == "profit_lock":
            harness.profit_lock_triggered = True
            row = candle(close=100.2, high=100.4, low=100.0)
        elif path == "soft_exit":
            row = candle(close=100.2, high=100.3, low=100.0, rsi=75.0)
        else:
            harness.time_exit_enabled = True
            row = candle(close=100.0, high=100.2, low=99.9)
        observed = harness.cycle(row)

    decision = observed.final_decision
    assert isinstance(decision, FinalDecision)
    assert decision.decision_type is decision_type
    assert decision.decision_subtype is subtype
    assert decision.action == action
    assert decision.reason_code is reason
    if action in {None, "IDLE", "HOLD", "BLOCK", "SUPPRESS"}:
        assert observed.order_attempts == ()


@pytest.mark.parametrize(
    (
        "case", "trading_mode", "result", "decision_type", "subtype",
        "action", "reason", "entry_attempted", "order_submitted",
        "trade_executed",
    ),
    [
        (
            "live_fill", "LIVE",
            {"ledger_ok": True, "live_attempted": True,
             "order_accepted": True, "live_ok": True, "executed": True,
             "fully_executed": True, "executed_qty": 0.1,
             "requested_qty": 0.1, "blocked_reason": None},
            DecisionType.TRADE_EXECUTED, DecisionSubtype.EXIT_EXECUTED,
            "EXIT", DecisionReason.STOP_LOSS, True, True, True,
        ),
        (
            "live_exchange_rejection", "LIVE",
            {"ledger_ok": True, "live_attempted": True,
             "order_accepted": False, "live_ok": False,
             "blocked_reason": "LIVE_ORDER_REJECTED"},
            DecisionType.TECHNICAL_FAILURE, DecisionSubtype.ORDER_REJECTED,
            "ERROR", DecisionReason.EXECUTION_FAILED, True, False, False,
        ),
        (
            "live_legacy_result_without_ack_flag", "LIVE",
            {"ledger_ok": True, "live_attempted": True, "live_ok": False,
             "blocked_reason": "LEGACY_LIVE_ORDER_FAILED"},
            DecisionType.TECHNICAL_FAILURE, DecisionSubtype.ORDER_REJECTED,
            "ERROR", DecisionReason.EXECUTION_FAILED, True, False, False,
        ),
        (
            "live_exchange_api_exception", "LIVE",
            {"ledger_ok": True, "live_attempted": True,
             "order_accepted": False, "live_ok": False,
             "blocked_reason": "EXCHANGE_API_EXCEPTION",
             "resp": {"meta": {"code": "51008"}}},
            DecisionType.TECHNICAL_FAILURE, DecisionSubtype.ORDER_REJECTED,
            "ERROR", DecisionReason.EXECUTION_FAILED, True, False, False,
        ),
        (
            "live_ack_no_fill", "LIVE",
            {"ledger_ok": True, "live_attempted": True,
             "order_accepted": True, "live_ok": False,
             "blocked_reason": "ACK_NO_FILL"},
            DecisionType.TECHNICAL_FAILURE,
            DecisionSubtype.ORDER_ACCEPTED_NOT_FILLED,
            "ERROR", DecisionReason.EXECUTION_FAILED, True, True, False,
        ),
        (
            "live_not_attempted", "LIVE",
            {"ledger_ok": True, "live_attempted": False,
             "order_accepted": False, "live_ok": False,
             "blocked_reason": "LIVE_ORDER_SUPPRESSED"},
            DecisionType.ACTION_SUPPRESSED,
            DecisionSubtype.EXECUTION_NOT_ATTEMPTED,
            "SUPPRESS", DecisionReason.EXECUTION_NOT_ATTEMPTED,
            False, False, False,
        ),
        (
            "live_preflight_block", "LIVE",
            {"ledger_ok": True, "live_attempted": False,
             "order_accepted": False, "live_ok": False,
             "blocked_reason": "LIVE_PREFLIGHT_FAILED"},
            DecisionType.ACTION_SUPPRESSED,
            DecisionSubtype.EXECUTION_NOT_ATTEMPTED,
            "SUPPRESS", DecisionReason.EXECUTION_NOT_ATTEMPTED,
            False, False, False,
        ),
        (
            "no_open_position", "LIVE",
            {"ledger_ok": True, "live_attempted": False,
             "order_accepted": False, "live_ok": False,
             "blocked_reason": "EXIT_NO_OPEN_POSITION"},
            DecisionType.NO_TRADE, DecisionSubtype.NO_POSITION,
            "REJECT", DecisionReason.NO_OPEN_POSITION, False, False, False,
        ),
        (
            "paper_exit", "PAPER",
            {"ledger_ok": True, "live_attempted": False,
             "order_accepted": False, "live_ok": False,
             "paper_executed": True, "blocked_reason": None},
            DecisionType.PAPER_SIMULATION, DecisionSubtype.EXIT_EXECUTED,
            "EXIT", DecisionReason.STOP_LOSS, True, False, False,
        ),
        (
            "ledger_failure", "LIVE",
            {"ledger_ok": False, "live_attempted": False,
             "order_accepted": False, "live_ok": False,
             "blocked_reason": "DB_GUARD_DUPLICATE"},
            DecisionType.TECHNICAL_FAILURE, DecisionSubtype.LEDGER_FAILURE,
            "ERROR", DecisionReason.EXECUTION_FAILED, False, False, False,
        ),
        (
            "ledger_failure_after_fill", "LIVE",
            {"ledger_ok": False, "live_attempted": True,
             "order_accepted": True, "live_ok": True, "executed": True,
             "fully_executed": True, "executed_qty": 0.1,
             "requested_qty": 0.1,
             "blocked_reason": "LEDGER_POST_FILL_FAILED"},
            DecisionType.TECHNICAL_FAILURE, DecisionSubtype.LEDGER_FAILURE,
            "ERROR", DecisionReason.EXECUTION_FAILED, True, True, True,
        ),
    ],
)
def test_rsi_exit_adapter_classification(
    harness, case, trading_mode, result, decision_type, subtype, action,
    reason, entry_attempted, order_submitted, trade_executed,
):
    snap = runtime_snapshot(trading_mode=trading_mode)
    evaluation = harness.module._rsi_evaluation_context(
        candle()[0], harness.now, snap=snap
    )
    decision = harness.module._rsi_exit_decision(
        evaluation,
        result,
        snap["cfg_effective"],
        reason_code=DecisionReason.STOP_LOSS,
        reason_text="fixture exit",
        side="SELL",
        price=99.0,
        position_id=17,
    )

    assert decision.decision_type is decision_type, case
    assert decision.decision_subtype is subtype
    assert decision.action == action
    assert decision.reason_code is reason
    assert decision.side == "SELL"
    assert decision.entry_attempted is entry_attempted
    assert decision.order_submitted is order_submitted
    assert decision.trade_executed is trade_executed
    assert decision.details["blocked_reason"] == result["blocked_reason"]
    assert decision.details["ledger_ok"] is result["ledger_ok"]
    assert decision.details["live_attempted"] is result["live_attempted"]
    assert decision.details["order_accepted"] is result.get("order_accepted", False)
    assert decision.details["live_ok"] is result["live_ok"]
    assert dict(decision.details["execution_result"])["blocked_reason"] == result["blocked_reason"]
    if decision.decision_type is DecisionType.TECHNICAL_FAILURE:
        assert decision.reason_text == result["blocked_reason"]
    if case == "no_open_position":
        assert decision.position_id is None


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
def test_rsi_exit_rejects_inconsistent_execution_outcome(harness, result):
    snap = runtime_snapshot(trading_mode="LIVE")
    evaluation = harness.module._rsi_evaluation_context(
        candle()[0], harness.now, snap=snap
    )
    with pytest.raises(ValueError):
        harness.module._rsi_exit_decision(
            evaluation, result, snap["cfg_effective"],
            reason_code=DecisionReason.STOP_LOSS,
            reason_text="fixture exit", side="SELL", price=99.0,
            position_id=17,
        )


def test_inconsistent_live_exit_is_rejected_before_position_mutation(harness):
    harness.trading_mode = "LIVE"
    harness.execution_live_attempted = True
    harness.execution_order_accepted = False
    harness.execution_live_ok = True
    harness.open_long()
    position_before = harness.position

    with pytest.raises(ValueError, match="executed requires order_accepted"):
        harness.cycle(candle(close=99.1, high=100.0, low=99.0))

    assert harness.position == position_before
    assert "execution:EXIT_SELL" in harness.operation_log
    assert "state_change:position" not in harness.operation_log
    assert "strategy_event:RUN_END" in harness.operation_log


@pytest.mark.parametrize(
    ("kind", "ledger_ok", "live_ok", "expected_subtype"),
    [
        ("entry", True, True, DecisionSubtype.PARTIAL_EXECUTION),
        ("exit", True, False, DecisionSubtype.PARTIAL_EXECUTION),
        ("entry_ledger_failure", False, False, DecisionSubtype.LEDGER_FAILURE),
        ("exit_ledger_failure", False, False, DecisionSubtype.LEDGER_FAILURE),
    ],
)
def test_partial_fill_final_decision_preserves_real_execution(
    harness, kind, ledger_ok, live_ok, expected_subtype
):
    snap = runtime_snapshot(trading_mode="LIVE")
    evaluation = harness.module._rsi_evaluation_context(
        candle()[0], harness.now, snap=snap
    )
    result = {
        "ledger_ok": ledger_ok,
        "live_attempted": True,
        "order_accepted": True,
        "executed": True,
        "fully_executed": False,
        "executed_qty": 0.4,
        "requested_qty": 1.0,
        "live_ok": live_ok,
        "blocked_reason": "FALLBACK_FAILED_AFTER_PARTIAL_FILL",
    }

    if kind.startswith("entry"):
        decision = harness.module._rsi_entry_decision(
            evaluation, result, snap["cfg_effective"],
            side="BUY", price=99.0, reason_text="fixture entry",
        )
    else:
        decision = harness.module._rsi_exit_decision(
            evaluation, result, snap["cfg_effective"],
            reason_code=DecisionReason.STOP_LOSS,
            reason_text="fixture exit", side="SELL", price=99.0,
            position_id=17,
        )

    assert decision.decision_type is DecisionType.TECHNICAL_FAILURE
    assert decision.decision_subtype is expected_subtype
    assert decision.action == "ERROR"
    assert decision.order_submitted is True
    assert decision.trade_executed is True
    assert decision.details["executed_qty"] == 0.4
    assert decision.details["fully_executed"] is False


@pytest.mark.parametrize(
    ("place_result", "expected_order_accepted", "expected_live_ok"),
    [
        (
            {"ok": False, "live_ok": False, "order_accepted": False,
             "reason": "EXCHANGE_API_EXCEPTION", "resp": None},
            False, False,
        ),
        (
            {"ok": True, "live_ok": False, "order_accepted": True,
             "reason": None,
             "resp": {"orderId": "ack-1", "status": "NEW",
                      "executedQty": "0"}},
            True, False,
        ),
        (
            {"ok": True, "live_ok": True, "order_accepted": True,
             "reason": None,
             "resp": {"orderId": "fill-1", "status": "FILLED",
                      "executedQty": "0.1"}},
            True, True,
        ),
    ],
)
def test_execute_and_record_propagates_order_accepted(
    rsi, monkeypatch, place_result, expected_order_accepted, expected_live_ok
):
    class FakeCursor:
        def close(self):
            return None

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    side_effects = {"place_live_order": 0}

    def place_once(*_args, **_kwargs):
        side_effects["place_live_order"] += 1
        return place_result

    monkeypatch.setattr(rsi, "insert_simulated_order", lambda **_kwargs: True)
    monkeypatch.setattr(
        rsi, "get_open_position",
        lambda: (17, "LONG", 0.1, 100.0, candle()[0]),
    )
    monkeypatch.setattr(rsi, "get_db_conn", lambda: FakeConnection())
    monkeypatch.setattr(rsi, "attach_exit_order_id_with_conn", lambda *_args: None)
    monkeypatch.setattr(rsi, "preflight_live_order", lambda *_args, **_kwargs: {"ok": True})
    monkeypatch.setattr(rsi, "get_exchange_client", lambda: object())
    monkeypatch.setattr(rsi, "place_live_order", place_once)
    monkeypatch.setattr(rsi, "emit_strategy_event", lambda **_kwargs: None)

    cfg_live = rsi.replace(
        rsi.cfg, trading_mode="LIVE", live_orders_enabled=True
    )
    result = rsi.execute_and_record(
        side="SELL", price=99.0, qty_btc=0.1, reason="STOP_LOSS",
        candle_open_time=candle()[0], is_exit=True, cfg_used=cfg_live,
        allow_live_orders=True, allow_meta={},
    )

    assert side_effects == {"place_live_order": 1}
    assert result["live_attempted"] is True
    assert result["order_accepted"] is expected_order_accepted
    assert result["live_ok"] is expected_live_ok


@pytest.mark.parametrize(
    (
        "case", "ledger_ok", "attempted", "accepted", "executed",
        "include_ack", "blocked_reason", "decision_type", "subtype",
        "action", "reason", "entry_attempted", "order_submitted",
        "trade_executed",
    ),
    [
        (
            "suppressed", True, False, False, False, True,
            "LIVE_ORDER_SUPPRESSED", DecisionType.ACTION_SUPPRESSED,
            DecisionSubtype.EXECUTION_NOT_ATTEMPTED, "SUPPRESS",
            DecisionReason.EXECUTION_NOT_ATTEMPTED, False, False, False,
        ),
        (
            "rejected_before_ack", True, True, False, False, True,
            "LIVE_ORDER_REJECTED", DecisionType.TECHNICAL_FAILURE,
            DecisionSubtype.ORDER_REJECTED, "ERROR",
            DecisionReason.EXECUTION_FAILED, True, False, False,
        ),
        (
            "exchange_api_exception", True, True, False, False, True,
            "EXCHANGE_API_EXCEPTION", DecisionType.TECHNICAL_FAILURE,
            DecisionSubtype.ORDER_REJECTED, "ERROR",
            DecisionReason.EXECUTION_FAILED, True, False, False,
        ),
        (
            "accepted_without_fill", True, True, True, False, True,
            "ACK_NO_FILL", DecisionType.TECHNICAL_FAILURE,
            DecisionSubtype.ORDER_ACCEPTED_NOT_FILLED, "ERROR",
            DecisionReason.EXECUTION_FAILED, True, True, False,
        ),
        (
            "accepted_fill", True, True, True, True, True,
            None, DecisionType.TRADE_EXECUTED,
            DecisionSubtype.EXECUTED, "EXECUTE",
            DecisionReason.SSOT_EXECUTE_AND_RECORD, True, True, True,
        ),
        (
            "ledger_failure_without_ack", False, False, False, False, True,
            "DB_GUARD_DUPLICATE", DecisionType.TECHNICAL_FAILURE,
            DecisionSubtype.LEDGER_FAILURE, "ERROR",
            DecisionReason.EXECUTION_FAILED, False, False, False,
        ),
        (
            "ledger_failure_after_ack", False, True, True, False, True,
            "LEDGER_POST_ACK_FAILED", DecisionType.TECHNICAL_FAILURE,
            DecisionSubtype.LEDGER_FAILURE, "ERROR",
            DecisionReason.EXECUTION_FAILED, True, True, False,
        ),
        (
            "ledger_failure_after_fill", False, True, True, True, True,
            "LEDGER_POST_FILL_FAILED", DecisionType.TECHNICAL_FAILURE,
            DecisionSubtype.LEDGER_FAILURE, "ERROR",
            DecisionReason.EXECUTION_FAILED, True, True, True,
        ),
        (
            "legacy_without_ack", True, True, False, False, False,
            "LEGACY_LIVE_ORDER_FAILED", DecisionType.TECHNICAL_FAILURE,
            DecisionSubtype.ORDER_REJECTED, "ERROR",
            DecisionReason.EXECUTION_FAILED, True, False, False,
        ),
    ],
)
def test_live_entry_execution_outcome_matrix(
    harness, case, ledger_ok, attempted, accepted, executed, include_ack,
    blocked_reason, decision_type, subtype, action, reason, entry_attempted,
    order_submitted, trade_executed,
):
    harness.trading_mode = "LIVE"
    harness.execution_ledger_ok = ledger_ok
    harness.execution_live_attempted = attempted
    harness.execution_order_accepted = accepted
    harness.execution_live_ok = executed
    harness.execution_include_order_accepted = include_ack
    harness.execution_blocked_reason = blocked_reason

    observed = harness.cycle(
        entry_candle(), prev_row=entry_previous_candle()
    )
    decision = observed.final_decision

    assert decision.decision_type is decision_type, case
    assert decision.decision_subtype is subtype
    assert decision.action == action
    assert decision.reason_code is reason
    assert decision.entry_attempted is entry_attempted
    assert decision.order_submitted is order_submitted
    assert decision.trade_executed is trade_executed
    assert decision.details["ledger_ok"] is ledger_ok
    assert decision.details["live_attempted"] is attempted
    assert decision.details["order_accepted"] is (accepted if include_ack else False)
    assert decision.details["live_ok"] is executed
    raw = decision.details["execution_result"]
    assert raw["ledger_ok"] is ledger_ok
    assert raw["live_attempted"] is attempted
    assert raw.get("order_accepted", False) is (accepted if include_ack else False)


@pytest.mark.parametrize(
    ("attempted", "accepted", "executed"),
    [
        (False, True, False),
        (True, False, True),
    ],
)
def test_live_entry_rejects_inconsistent_execution_outcome(
    harness, attempted, accepted, executed
):
    harness.trading_mode = "LIVE"
    harness.execution_live_attempted = attempted
    harness.execution_order_accepted = accepted
    harness.execution_live_ok = executed

    with pytest.raises(ValueError, match="requires"):
        harness.cycle(entry_candle(), prev_row=entry_previous_candle())
    assert harness.position is None
    assert "state_change:position" not in harness.operation_log
    assert "strategy_event:RUN_END" in harness.operation_log


@pytest.mark.parametrize(
    ("position", "expected_side"),
    [("LONG", "SELL"), ("SHORT", "BUY")],
)
def test_live_stop_exit_side_mapping_through_rsi_runtime(
    harness, position, expected_side
):
    harness.trading_mode = "LIVE"
    harness.execution_live_attempted = True
    harness.execution_order_accepted = True
    if position == "LONG":
        harness.open_long()
        row = candle(close=99.1, high=100.0, low=99.0)
    else:
        harness.open_short()
        row = candle(close=100.9, high=101.0, low=100.0)

    observed = harness.cycle(row)
    assert observed.order_attempts[0].side == expected_side
    assert observed.final_decision.side == expected_side
    assert observed.final_decision.decision_type is DecisionType.TRADE_EXECUTED
    assert observed.position_after is None


@pytest.mark.parametrize(
    "scenario",
    [
        "paper_entry",
        "live_entry",
        "paper_exit",
        "live_exit_success",
        "live_exit_suppressed",
        "live_exit_failed",
    ],
)
def test_side_effect_operation_order(harness, scenario):
    if scenario in {"paper_entry", "live_entry"}:
        if scenario == "live_entry":
            harness.trading_mode = "LIVE"
            harness.execution_live_attempted = True
            harness.execution_order_accepted = True
        observed = harness.cycle(entry_candle(), prev_row=entry_previous_candle())
        assert_operation_order(
            observed,
            "strategy_event:SIGNAL",
            "execution:ENTRY_BUY",
            "state_change:position",
            "strategy_event:POSITION_OPENED",
            "strategy_event:RUN_END",
        )
        return

    harness.open_long()
    if scenario.startswith("live_exit"):
        harness.trading_mode = "LIVE"
    if scenario == "live_exit_success":
        harness.execution_live_attempted = True
        harness.execution_order_accepted = True
    elif scenario == "live_exit_suppressed":
        harness.execution_live_ok = False
        harness.execution_live_attempted = False
        harness.execution_blocked_reason = "LIVE_ORDER_SUPPRESSED"
    elif scenario == "live_exit_failed":
        harness.execution_live_ok = False
        harness.execution_live_attempted = True
        harness.execution_order_accepted = False
        harness.execution_blocked_reason = "LIVE_ORDER_FAILED"

    observed = harness.cycle(
        candle(close=100.2, high=100.3, low=100.0, rsi=75.0)
    )
    assert_operation_order(
        observed,
        "strategy_event:EXIT_SIGNAL",
        "execution:EXIT_SELL",
        "strategy_event:RUN_END",
    )
    if scenario in {"paper_exit", "live_exit_success"}:
        assert_operation_order(
            observed,
            "execution:EXIT_SELL",
            "state_change:position",
            "strategy_event:RUN_END",
        )
    else:
        assert "state_change:position" not in observed.operation_log
        assert_operation_order(
            observed,
            "execution:EXIT_SELL",
            "strategy_event:BLOCKED",
            "strategy_event:RUN_END",
        )

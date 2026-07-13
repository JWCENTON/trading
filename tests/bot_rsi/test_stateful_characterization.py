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

from tests.bot_rsi.fixtures import (
    RsiStatefulHarness,
    StrictFakeExchange,
    candle,
    entry_candle,
    entry_previous_candle,
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


def test_harness_is_test_only_and_rsi_has_no_decision_sink_or_final_decision(rsi):
    signature = inspect.signature(rsi.run_strategy)
    source = inspect.getsource(rsi)
    assert tuple(signature.parameters) == ("row", "prev_row")
    assert "decision_sink" not in source
    assert "FinalDecision" not in source


def test_no_closed_candle_is_explicit_no_action(harness):
    observed = harness.cycle(None)
    assert observed.candle_state == "NO_CLOSED_CANDLE"
    assert observed.action == "NO_ACTION"
    assert observed.reason is None
    assert observed.captured_events == ()
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
    assert_isolated(first)
    assert_isolated(repeated)


def test_no_position_no_rsi_signal_is_observable_no_action(harness):
    observed = harness.cycle(candle(), prev_row=candle(minute=-1, rsi=40.0))
    assert observed.action == "NO_ACTION"
    assert observed.reason == "NO_SIGNAL_REBOUND"
    assert observed.position_before is None
    assert observed.position_after is None
    assert observed.order_attempts == ()
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
    assert_isolated(observed)


def test_valid_signal_blocked_by_existing_regime_gate(harness):
    harness.regime_allow = False
    observed = harness.cycle(entry_candle(), prev_row=entry_previous_candle())
    assert observed.action == "NO_ACTION"
    assert observed.signal == "BUY"
    assert observed.reason == "REGIME_BLOCK"
    assert observed.order_attempts == ()
    assert observed.position_after is None
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
    assert_isolated(observed)


def test_open_position_hold(harness):
    harness.open_long()
    observed = harness.cycle(candle(close=100.1, high=100.2, low=99.9))
    assert observed.action == "NO_ACTION"
    assert observed.reason == "NO_EXIT_SIGNAL"
    assert observed.position_before == observed.position_after
    assert observed.order_attempts == ()
    assert "POSITION_HOLD" in event_types(observed)
    assert_isolated(observed)


def test_open_position_stop_loss(harness):
    harness.open_long()
    observed = harness.cycle(candle(close=99.1, high=100.0, low=99.0))
    assert observed.action == "EXIT_SELL"
    assert "STOP LOSS LONG" in observed.reason
    assert observed.order_attempts[0].exit_kind is None
    assert observed.position_after is None
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
    assert_isolated(observed)


def test_open_position_soft_exit(harness):
    harness.open_long()
    observed = harness.cycle(candle(close=100.2, high=100.3, low=100.0, rsi=75.0))
    assert observed.action == "EXIT_SELL"
    assert observed.signal == "SELL"
    assert observed.order_attempts[0].exit_kind == "RSI_SOFT_EXIT"
    assert observed.position_after is None
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
    for observed in harness.observations:
        assert_isolated(observed)

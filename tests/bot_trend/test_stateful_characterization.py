from __future__ import annotations

import importlib
import importlib.util
import inspect
import socket
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import psycopg2
import pytest
import requests

from tests.bot_trend.fixtures import (
    StrictFakeExchange,
    TrendStatefulHarness,
    downtrend_rows,
    flat_rows,
    trend_rows,
)


@pytest.fixture
def trend(monkeypatch):
    safe_env = {
        "SYMBOL": "BTCUSDC", "QUOTE_ASSET": "USDC", "STRATEGY_NAME": "TREND",
        "INTERVAL": "1m", "TRADING_MODE": "PAPER", "LIVE_ORDERS_ENABLED": "0",
        "DB_HOST": "invalid.test", "DB_NAME": "unit_test", "DB_USER": "unit_test",
        "DB_PASS": "unit_test", "DAILY_MAX_LOSS_PCT": "0",
        "ADAPTIVE_EARLY_CUT_SHADOW_ENABLED": "0",
    }
    for key, value in safe_env.items():
        monkeypatch.setenv(key, value)
    for key in ("BINANCE_API_KEY", "BINANCE_API_SECRET", "OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE"):
        monkeypatch.delenv(key, raising=False)
    blocked = lambda *_args, **_kwargs: pytest.fail("external I/O during TREND import")
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(requests.sessions.Session, "request", blocked)
    monkeypatch.setattr(psycopg2, "connect", blocked)
    exchange_client = importlib.import_module("common.exchange_client")
    calls = []
    monkeypatch.setattr(exchange_client, "get_market_data_client", lambda: calls.append("called") or StrictFakeExchange())
    name = "waltrade_bot_trend_main_characterized"
    sys.modules.pop(name, None)
    source = Path(__file__).resolve().parents[2] / "bot_trend" / "main.py"
    spec = importlib.util.spec_from_file_location(name, source)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    assert calls == []
    return module


@pytest.fixture
def harness(monkeypatch, trend):
    monkeypatch.setattr(threading.Thread, "start", lambda *_a, **_k: pytest.fail("background thread started"))
    return TrendStatefulHarness(trend, monkeypatch)


def event_types(observation):
    return tuple(event.get("event_type") for event in observation.strategy_events)


def reasons(observation):
    return tuple(event.get("reason") for event in observation.strategy_events)


def test_harness_is_test_only_and_has_no_final_decision_or_sink(trend):
    source = inspect.getsource(trend)
    assert tuple(inspect.signature(trend.run_trend_strategy).parameters) == ()
    assert "FinalDecision" not in source
    assert "decision_sink" not in source


def test_insufficient_and_duplicate_candle_paths(harness):
    no_candle = harness.cycle([])
    assert no_candle.candle_state == "NO_CLOSED_CANDLE"
    assert no_candle.strategy_events == ()
    insufficient = harness.cycle(flat_rows(count=20))
    assert insufficient.observed_action == "NO_ACTION"
    assert insufficient.strategy_events == ()
    duplicate = harness.cycle(flat_rows(count=20))
    assert duplicate.observed_action == "IDLE"
    assert event_types(duplicate) == ("IDLE",)
    assert duplicate.execution_attempts == ()


def test_first_same_and_next_candle_deduplication(harness):
    first = harness.cycle(trend_rows(minute=0, previous=102.0, current=102.0))
    same = harness.cycle(trend_rows(minute=0, previous=102.0, current=102.0))
    next_one = harness.cycle(trend_rows(minute=1, previous=102.0, current=102.0))
    assert first.candle_state == "NEW_CANDLE"
    assert same.candle_state == "ALREADY_PROCESSED"
    assert next_one.candle_state == "NEW_CANDLE"
    assert same.execution_attempts == ()
    assert "heartbeat:update" in first.operation_log


@pytest.mark.parametrize(
    ("setup", "expected_reason"),
    [
        (lambda h: setattr(h, "runtime_mode", "HALT"), "BOT_MODE_HALT"),
        (lambda h: setattr(h, "runtime_enabled", False), "BOT_DISABLED"),
        (lambda h: setattr(h, "regime_allow", False), "REGIME_BLOCK"),
        (lambda h: setattr(h, "sizing_qty", 0.0), "SIZING_QTY_ZERO"),
    ],
)
def test_entry_control_and_readiness_blocks(harness, setup, expected_reason):
    setup(harness)
    observed = harness.cycle(trend_rows())
    assert expected_reason in reasons(observed)
    assert observed.position_after is None
    assert observed.execution_attempts == ()


def test_no_signal_and_entry_quality_filter(harness):
    no_signal = harness.cycle(trend_rows(previous=102.0, current=102.0))
    assert no_signal.observed_action == "NO_ACTION"
    assert "NO_SIGNAL" in reasons(no_signal)
    harness.module.MAX_DIST_FROM_EMA_FAST_PCT = 0.01
    rejected = harness.cycle(trend_rows(minute=1))
    assert "MAX_DIST_FROM_EMA" in reasons(rejected)
    assert rejected.execution_attempts == ()


def test_downtrend_is_long_only_even_when_allow_short_is_enabled(harness):
    harness.module.ALLOW_SHORT = True
    observed = harness.cycle(downtrend_rows())
    assert observed.observed_action == "NO_ACTION"
    assert "TREND_DOWN_LONG_ONLY" in reasons(observed)
    assert observed.execution_attempts == ()


def test_missing_optional_indicator_values_do_not_block_current_runtime(harness):
    rows = [tuple(list(row[:4]) + [None, None]) for row in trend_rows()]
    observed = harness.cycle(rows)
    assert observed.observed_action == "ENTRY_ATTEMPT"
    assert observed.execution_attempts


def test_paper_entry_and_live_boundaries(harness):
    paper = harness.cycle(trend_rows())
    assert paper.observed_action == "ENTRY_ATTEMPT"
    assert paper.position_after is not None
    assert paper.execution_attempts[0].side == "BUY"

    harness.position = None
    harness.trading_mode = "LIVE"
    harness.allow_entry = False
    suppressed = harness.cycle(trend_rows(minute=1))
    assert suppressed.observed_action == "ENTRY_ATTEMPT"
    assert suppressed.position_after is None
    assert suppressed.execution_attempts[0].is_exit is False

    harness.allow_entry = True
    filled = harness.cycle(trend_rows(minute=2))
    assert filled.position_after is not None
    assert filled.execution_attempts[0].side == "BUY"


def test_current_live_new_ack_is_promoted_to_live_ok_and_opens_position(trend, monkeypatch):
    """Freeze TREND's legacy ACK semantics without using FinalDecision/ExecutionOutcome."""
    class Conn:
        def commit(self):
            pass

        def close(self):
            pass

    opened = []
    monkeypatch.setattr(trend, "insert_simulated_order", lambda **_kwargs: True)
    monkeypatch.setattr(trend, "get_db_conn", lambda: Conn())
    monkeypatch.setattr(trend, "get_exchange_client", lambda: StrictFakeExchange())
    monkeypatch.setattr(trend, "get_open_position", lambda: None)
    monkeypatch.setattr(trend, "emit_strategy_event", lambda **_kwargs: None)
    monkeypatch.setattr(trend, "preflight_live_order", lambda *_a, **_k: {"ok": True})
    monkeypatch.setattr(
        trend,
        "place_live_order",
        lambda *_a, **_k: {
            "ok": True, "live_ok": False, "order_accepted": True,
            "resp": {"orderId": "order-new", "status": "NEW", "executedQty": "0"},
        },
    )
    monkeypatch.setattr(
        trend,
        "open_position_from_live_ack",
        lambda **kwargs: opened.append(kwargs) or 77,
    )
    result = trend.execute_and_record(
        side="BUY", price=102.0, qty_btc=0.1, reason="fixture",
        candle_open_time=trend_rows()[0][2], is_exit=False,
        cfg_used=SimpleNamespace(
            symbol="BTCUSDC", interval="1m", trading_mode="LIVE",
            live_orders_enabled=True, quote_asset="USDC",
        ),
        allow_live_orders=True, allow_meta={}, rsi_14=50.0, ema_21=101.0,
    )
    assert result["live_attempted"] is True, result
    assert result["live_ok"] is True
    assert opened[0]["qty"] == 0.1
    assert opened[0]["entry_order_id"] == "order-new"


@pytest.mark.parametrize(
    ("side", "entry", "price", "expected_side", "reason"),
    [
        ("LONG", 100.0, 102.0, "SELL", "TAKE_PROFIT_LONG"),
        ("LONG", 100.0, 99.0, "SELL", "STOP_LOSS_LONG"),
        ("SHORT", 100.0, 98.0, "BUY", "TAKE_PROFIT_SHORT"),
        ("SHORT", 100.0, 101.0, "BUY", "STOP_LOSS_SHORT"),
    ],
)
def test_take_profit_stop_loss_and_side_mapping(harness, side, entry, price, expected_side, reason):
    harness.set_position(side=side, entry_price=entry)
    observed = harness.cycle(trend_rows(current=price))
    assert observed.observed_action == "EXIT"
    assert observed.execution_attempts[0].side == expected_side
    assert observed.position_mutations[-1].reason == reason


def test_hold_has_heartbeat_but_no_explicit_hold_event(harness):
    harness.set_position(entry_price=102.0, age_minutes=5)
    observed = harness.cycle(trend_rows(current=102.1))
    assert observed.observed_action == "HOLD"
    assert observed.execution_attempts == ()
    assert event_types(observed) == ("RUN_START", "REGIME_GATE", "RUN_END")
    assert observed.heartbeat_calls
    assert observed.operation_log.index("heartbeat:update") < observed.operation_log.index("strategy_event:RUN_END")


def test_early_cut_long_and_live_exit_failure(harness):
    harness.module.STOP_LOSS_PCT = 5.0
    harness.set_position(entry_price=102.0, age_minutes=31)
    cut = harness.cycle(trend_rows(current=101.6))
    assert cut.observed_action == "EXIT"
    assert cut.position_mutations[-1].reason == "EARLY_CUT_LONG"

    harness.set_position(entry_price=100.0)
    harness.module.STOP_LOSS_PCT = 0.8
    harness.trading_mode = "LIVE"
    harness.execution_live_ok = False
    failed = harness.cycle(trend_rows(minute=1, current=99.0))
    assert failed.observed_action == "EXIT_ATTEMPT"
    assert failed.position_after is not None


def test_profit_lock_and_time_exit(harness):
    harness.set_position(entry_price=102.0, age_minutes=10)
    harness.profit_lock_state = "TRIGGERED"
    profit = harness.cycle(trend_rows(current=102.5))
    assert profit.observed_action == "EXIT"
    assert "PROFIT_LOCK_TRIGGERED" in event_types(profit)
    assert profit.position_mutations[-1].reason == "TRAIL_DROP"

    harness.set_position(entry_price=102.0, age_minutes=100)
    harness.profit_lock_state = "NONE"
    harness.time_exit_enabled = True
    timed = harness.cycle(trend_rows(minute=1, current=102.0))
    assert timed.observed_action == "EXIT"
    assert timed.position_mutations[-1].reason == "TIME_EXIT_PROFIT_FADED"


def test_guarded_profit_and_remaining_time_exit_branches(harness):
    harness.module.GUARDED_PROFIT_ENABLED = True
    harness.guarded_profit_triggered = True
    harness.set_position(entry_price=102.0, age_minutes=10)
    guarded = harness.cycle(trend_rows(current=102.5))
    assert guarded.observed_action == "EXIT"
    assert "GUARDED_PROFIT_LONG" in reasons(guarded)
    assert guarded.position_mutations[-1].reason == "GUARDED_PROFIT_LONG"

    harness.module.GUARDED_PROFIT_ENABLED = False
    harness.guarded_profit_triggered = False
    harness.time_exit_enabled = True
    harness.set_position(entry_price=100.0, age_minutes=100)
    kept = harness.cycle(trend_rows(minute=1, current=100.1))
    assert kept.observed_action == "HOLD"
    assert "TIME_EXIT_SKIPPED_KEEP_PROFIT_WINDOW" in reasons(kept)

    harness.set_position(entry_price=100.0, age_minutes=151)
    hard = harness.cycle(trend_rows(minute=2, current=100.3))
    assert hard.observed_action == "EXIT"
    assert hard.position_mutations[-1].reason == "TIME_EXIT_HARD"


def test_panic_exit_and_mode_transition(harness):
    harness.set_position(entry_price=102.0)
    harness.runtime_mode = "PANIC"
    observed = harness.cycle(trend_rows())
    assert observed.observed_action == "EXIT"
    assert harness.runtime_mode == "HALT"
    assert any(m.operation == "MODE" for m in observed.position_mutations)


def test_execution_ledger_failure_does_not_mutate_position(harness):
    harness.set_position(entry_price=100.0)
    harness.execution_ledger_ok = False
    observed = harness.cycle(trend_rows(current=99.0))
    assert observed.observed_action == "EXIT_ATTEMPT"
    assert observed.position_after is not None
    assert observed.position_mutations == ()


def test_sequence_a_no_position_entry_dedupe_and_hold(harness):
    observations = [
        harness.cycle(trend_rows(minute=0, previous=102.0, current=102.0)),
        harness.cycle(trend_rows(minute=1)),
        harness.cycle(trend_rows(minute=1)),
        harness.cycle(trend_rows(minute=2, current=102.1)),
        harness.cycle(trend_rows(minute=3, current=102.1)),
    ]
    assert [o.observed_action for o in observations] == [
        "NO_ACTION", "ENTRY_ATTEMPT", "IDLE", "HOLD", "HOLD"
    ]
    assert observations[-1].position_after is not None


def test_sequence_b_entry_profit_management_exit(harness):
    entry = harness.cycle(trend_rows())
    hold = harness.cycle(trend_rows(minute=1, current=102.5))
    harness.profit_lock_state = "ARMED"
    armed = harness.cycle(trend_rows(minute=2, current=102.6))
    harness.profit_lock_state = "TRIGGERED"
    exited = harness.cycle(trend_rows(minute=3, current=102.5))
    assert [entry.observed_action, hold.observed_action, armed.observed_action, exited.observed_action] == [
        "ENTRY_ATTEMPT", "HOLD", "HOLD", "EXIT"
    ]
    assert "PROFIT_LOCK_ARMED" in event_types(armed)


def test_sequence_c_entry_deterioration_early_cut(harness):
    harness.module.STOP_LOSS_PCT = 5.0
    entry = harness.cycle(trend_rows())
    weak = harness.cycle(trend_rows(minute=1, current=101.9))
    harness.advance(31)
    cut = harness.cycle(trend_rows(minute=2, current=101.6))
    assert [entry.observed_action, weak.observed_action, cut.observed_action] == [
        "ENTRY_ATTEMPT", "HOLD", "EXIT"
    ]
    assert cut.position_mutations[-1].reason == "EARLY_CUT_LONG"


def test_operation_log_freezes_boundary_chronology(harness):
    entry = harness.cycle(trend_rows())
    assert entry.operation_log.index("strategy_event:RUN_START") < entry.operation_log.index("heartbeat:update")
    assert entry.operation_log.index("heartbeat:update") < entry.operation_log.index("execution:entry")
    assert entry.operation_log.index("execution:entry") < entry.operation_log.index("state_change:open")
    assert entry.operation_log.index("state_change:open") < entry.operation_log.index("strategy_event:RUN_END")

    harness.module.TAKE_PROFIT_PCT = 0.1
    exited = harness.cycle(trend_rows(minute=1, current=102.5))
    assert exited.operation_log.index("execution:exit") < exited.operation_log.index("state_change:close")
    assert exited.operation_log.index("state_change:close") < exited.operation_log.index("strategy_event:RUN_END")

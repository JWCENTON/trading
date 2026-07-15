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
    ExecutionAttempt,
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


def test_trend_returns_final_decision_without_sink(trend):
    source = inspect.getsource(trend)
    assert tuple(inspect.signature(trend.run_trend_strategy).parameters) == ()
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


def run_live_entry_boundary(
    trend, monkeypatch, place_result, *, ledger_ok=True, open_failure=False
):
    class Conn:
        def commit(self):
            operations.append("db:commit")

        def close(self):
            operations.append("db:close")

    operations = []
    events = []
    opened = []

    def insert(**_kwargs):
        operations.append("ledger:reserve")
        return ledger_ok

    def place(*_args, **_kwargs):
        operations.append("execution:place_live_order")
        return place_result

    def open_position(**kwargs):
        operations.append("state_change:open_attempt")
        if open_failure:
            raise RuntimeError("position ledger write failed")
        opened.append(kwargs)
        operations.append("state_change:open")
        return 77

    monkeypatch.setattr(trend, "insert_simulated_order", insert)
    monkeypatch.setattr(
        trend, "get_db_conn",
        lambda: operations.append("db:get_connection") or Conn(),
    )
    monkeypatch.setattr(trend, "get_exchange_client", lambda: StrictFakeExchange())
    monkeypatch.setattr(
        trend, "get_open_position",
        lambda: operations.append("state:read_position") or None,
    )
    monkeypatch.setattr(
        trend, "emit_strategy_event",
        lambda **payload: (
            events.append(payload),
            operations.append(f"strategy_event:{payload['event_type']}")
        ),
    )
    monkeypatch.setattr(
        trend, "preflight_live_order",
        lambda *_a, **_k: operations.append("execution:preflight") or {"ok": True},
    )
    monkeypatch.setattr(trend, "place_live_order", place)
    monkeypatch.setattr(trend, "open_position_from_live_ack", open_position)

    try:
        result = trend.execute_and_record(
            side="BUY", price=102.0, qty_btc=0.1, reason="fixture",
            candle_open_time=trend_rows()[0][2], is_exit=False,
            cfg_used=SimpleNamespace(
                symbol="BTCUSDC", interval="1m", trading_mode="LIVE",
                live_orders_enabled=True, quote_asset="USDC",
            ),
            allow_live_orders=True, allow_meta={}, rsi_14=50.0, ema_21=101.0,
        )
        error = None
    except RuntimeError as exc:
        result = None
        error = exc
    return SimpleNamespace(
        result=result, error=error, opened=opened, events=events,
        operation_log=operations,
    )


def run_live_exit_boundary(
    trend, monkeypatch, place_result, *, ledger_ok=True, preflight_result=None
):
    class Conn:
        def commit(self):
            operations.append("db:commit")

        def close(self):
            operations.append("db:close")

    operations = []
    events = []
    monkeypatch.setattr(
        trend, "insert_simulated_order",
        lambda **_kwargs: operations.append("ledger:reserve") or ledger_ok,
    )
    monkeypatch.setattr(
        trend, "get_db_conn",
        lambda: operations.append("db:get_connection") or Conn(),
    )
    monkeypatch.setattr(trend, "get_exchange_client", lambda: StrictFakeExchange())
    monkeypatch.setattr(
        trend, "set_exit_client_order_id",
        lambda *_a, **_k: operations.append("state:attach_exit_identity"),
    )
    monkeypatch.setattr(
        trend, "emit_strategy_event",
        lambda **payload: (
            events.append(payload),
            operations.append(f"strategy_event:{payload['event_type']}")
        ),
    )
    monkeypatch.setattr(
        trend, "preflight_live_order",
        lambda *_a, **_k: operations.append("execution:preflight") or (
            preflight_result if preflight_result is not None else {"ok": True}
        ),
    )
    monkeypatch.setattr(
        trend, "place_live_order",
        lambda *_a, **_k: operations.append("execution:place_live_order") or place_result,
    )
    monkeypatch.setattr(
        trend, "apply_partial_exit_result",
        lambda *_a, **_k: operations.append("state:apply_partial_exit") or None,
    )
    result = trend.execute_and_record(
        side="SELL", price=102.0, qty_btc=0.1, reason="fixture-exit",
        candle_open_time=trend_rows()[0][2], is_exit=True, pos_id=7,
        cfg_used=SimpleNamespace(
            symbol="BTCUSDC", interval="1m", trading_mode="LIVE",
            live_orders_enabled=True, quote_asset="USDC",
        ),
        allow_live_orders=True, allow_meta={}, rsi_14=50.0, ema_21=101.0,
    )
    return SimpleNamespace(result=result, events=events, operation_log=operations)


def install_real_exit_runtime_boundary(
    harness, monkeypatch, place_result, *, ledger_ok=True,
    preflight_result=None,
):
    """Run the production exit adapter underneath the stateful runtime caller."""
    module = harness.module
    production_execute = harness.production_execute_and_record

    class Conn:
        def commit(self):
            harness.operation_log.append("db:commit")

        def close(self):
            harness.operation_log.append("db:close")

    monkeypatch.setattr(
        module, "insert_simulated_order",
        lambda **_kwargs: harness.operation_log.append("ledger:reserve") or ledger_ok,
    )
    monkeypatch.setattr(
        module, "get_db_conn",
        lambda: harness.operation_log.append("db:get_connection") or Conn(),
    )
    monkeypatch.setattr(
        module, "set_exit_client_order_id",
        lambda *_a, **_k: harness.operation_log.append("state:attach_exit_identity"),
    )
    monkeypatch.setattr(
        module, "preflight_live_order",
        lambda *_a, **_k: harness.operation_log.append("execution:preflight") or (
            preflight_result if preflight_result is not None else {"ok": True}
        ),
    )
    monkeypatch.setattr(
        module, "place_live_order",
        lambda *_a, **_k: harness.operation_log.append("execution:place_live_order") or place_result,
    )

    def execute(**kwargs):
        attempt = ExecutionAttempt(
            side=str(kwargs["side"]), is_exit=bool(kwargs["is_exit"]),
            price=float(kwargs["price"]), quantity=float(kwargs["qty_btc"]),
            reason=str(kwargs["reason"]),
        )
        harness.attempts.append(attempt)
        harness.operation_log.append("execution:exit")
        return production_execute(**kwargs)

    monkeypatch.setattr(module, "execute_and_record", execute)


@pytest.mark.parametrize("status", ["NEW", "ACCEPTED"])
def test_live_accepted_without_fill_stays_pending(trend, monkeypatch, status):
    observed = run_live_entry_boundary(
        trend,
        monkeypatch,
        {
            "ok": True, "attempted": True, "live_ok": False,
            "order_accepted": True, "executed": False,
            "fully_executed": False, "executed_qty": 0.0,
            "requested_qty": 0.1, "order_id": f"order-{status.lower()}",
            "client_order_id": "cid-entry", "exchange_status": status,
            "resp": {"orderId": f"order-{status.lower()}", "status": status,
                     "executedQty": "0"},
        },
    )
    assert observed.error is None
    assert observed.result["live_attempted"] is True
    assert observed.result["order_accepted"] is True
    assert observed.result["executed"] is False
    assert observed.result["live_ok"] is False
    assert observed.result["blocked_reason"] == "ORDER_ACCEPTED_PENDING_FILL"
    assert observed.opened == []
    assert "state_change:open_attempt" not in observed.operation_log
    sent = next(e for e in observed.events if e["event_type"] == "LIVE_ORDER_SENT")
    assert sent["reason"] == "ORDER_ACCEPTED_PENDING_FILL"
    assert sent["info"]["pending_fill"] is True
    assert observed.operation_log.count("execution:place_live_order") == 1


@pytest.mark.parametrize(
    ("status", "executed_qty", "fully_executed", "expected_qty"),
    [
        ("PARTIALLY_FILLED", 0.04, False, 0.04),
        ("FILLED", 0.1, True, 0.1),
    ],
)
def test_live_partial_and_full_fill_use_actual_quantity(
    trend, monkeypatch, status, executed_qty, fully_executed, expected_qty
):
    observed = run_live_entry_boundary(
        trend,
        monkeypatch,
        {
            "ok": True, "attempted": True, "live_ok": True,
            "order_accepted": True, "executed": True,
            "fully_executed": fully_executed, "executed_qty": executed_qty,
            "requested_qty": 0.1, "order_id": "order-filled",
            "client_order_id": "cid-entry", "exchange_status": status,
            "resp": {"orderId": "order-filled", "status": status,
                     "executedQty": str(executed_qty)},
        },
    )
    assert observed.error is None
    assert observed.result["live_ok"] is True
    assert observed.result["executed"] is True
    assert observed.result["fully_executed"] is fully_executed
    assert observed.opened[0]["qty"] == expected_qty
    if not fully_executed:
        assert observed.opened[0]["qty"] != 0.1
    assert observed.operation_log.index("execution:place_live_order") < observed.operation_log.index("state_change:open")


def test_live_rejection_before_ack_and_exchange_exception_do_not_open(trend, monkeypatch):
    rejected = run_live_entry_boundary(
        trend,
        monkeypatch,
        {
            "ok": True, "attempted": True, "live_ok": False,
            "order_accepted": False, "executed": False,
            "fully_executed": False, "executed_qty": 0.0,
            "requested_qty": 0.1, "order_id": None,
            "client_order_id": "cid-entry", "exchange_status": "REJECTED",
            "resp": {"status": "REJECTED", "executedQty": "0"},
        },
    )
    assert rejected.result["live_ok"] is False
    assert rejected.result["order_accepted"] is False
    assert rejected.opened == []

    failed = run_live_entry_boundary(
        trend,
        monkeypatch,
        {
            "ok": False, "attempted": True, "live_ok": False,
            "order_accepted": False, "executed": False,
            "fully_executed": False, "executed_qty": 0.0,
            "requested_qty": 0.1, "order_id": None,
            "client_order_id": "cid-entry", "exchange_status": "EXECUTION_EXCEPTION",
            "reason": "LIVE_ORDER_EXCEPTION", "resp": None,
        },
    )
    assert failed.result["live_ok"] is False
    assert failed.result["blocked_reason"] == "LIVE_ORDER_FAILED"
    assert failed.opened == []
    assert failed.operation_log.count("execution:place_live_order") == 1


def test_live_ledger_failure_before_and_after_fill(trend, monkeypatch):
    before = run_live_entry_boundary(trend, monkeypatch, {}, ledger_ok=False)
    assert before.result["ledger_ok"] is False
    assert before.opened == []
    assert "execution:place_live_order" not in before.operation_log

    after = run_live_entry_boundary(
        trend,
        monkeypatch,
        {
            "ok": True, "attempted": True, "live_ok": True,
            "order_accepted": True, "executed": True,
            "fully_executed": True, "executed_qty": 0.1,
            "requested_qty": 0.1, "order_id": "order-filled",
            "client_order_id": "cid-entry", "exchange_status": "FILLED",
            "resp": {"orderId": "order-filled", "status": "FILLED",
                     "executedQty": "0.1"},
        },
        open_failure=True,
    )
    assert after.error is None
    assert after.result["ledger_ok"] is False
    assert after.result["executed"] is True
    assert after.result["executed_qty"] == 0.1
    assert after.result["blocked_reason"] == "LIVE_ENTRY_FILL_POSITION_WRITE_FAILED"
    assert after.opened == []
    assert "state_change:open_attempt" in after.operation_log
    assert "state_change:open" not in after.operation_log
    assert next(e for e in after.events if e["event_type"] == "LIVE_ORDER_SENT")["reason"] == "OK"
    blocked = next(e for e in after.events if e["event_type"] == "BLOCKED")
    assert blocked["reason"] == "LIVE_ENTRY_FILL_BUT_POSITION_NOT_OPENED"

    partial = run_live_entry_boundary(
        trend,
        monkeypatch,
        {
            "ok": True, "attempted": True, "live_ok": True,
            "order_accepted": True, "executed": True,
            "fully_executed": False, "executed_qty": 0.04,
            "requested_qty": 0.1, "order_id": "order-partial",
            "client_order_id": "cid-entry", "exchange_status": "PARTIALLY_FILLED",
            "resp": {"orderId": "order-partial", "status": "PARTIALLY_FILLED",
                     "executedQty": "0.04"},
        },
        open_failure=True,
    )
    decision = trend._trend_execution_decision(
        trend._trend_evaluation_context(
            trend_rows()[0][2], trend.datetime.now(trend.timezone.utc),
            {"cfg_effective": SimpleNamespace(trading_mode="LIVE"),
             "bc": SimpleNamespace(enabled=True), "allowed_orders_entry": True},
        ),
        partial.result, SimpleNamespace(trading_mode="LIVE"), is_exit=False,
        reason_code=trend.DecisionReason.SSOT_EXECUTE_AND_RECORD,
        reason_text="ENTRY", side="BUY", price=102.0,
    )
    assert decision.decision_type.value == "TECHNICAL_FAILURE"
    assert decision.decision_subtype.value == "LEDGER_FAILURE"
    assert decision.order_submitted is True
    assert decision.trade_executed is True
    assert decision.details["executed_qty"] == 0.04
    assert decision.details["requested_qty"] == 0.1
    assert decision.details["fully_executed"] is False
    assert partial.opened == []
    assert partial.operation_log.count("execution:place_live_order") == 1


@pytest.mark.parametrize(
    "status, accepted, executed_qty, fully",
    [
        ("REJECTED", False, 0.0, False),
        ("NEW", True, 0.0, False),
        ("PARTIALLY_FILLED", True, 0.04, False),
        ("FILLED", True, 0.1, True),
    ],
)
def test_real_live_exit_adapter_preserves_canonical_matrix(
    trend, monkeypatch, status, accepted, executed_qty, fully
):
    observed = run_live_exit_boundary(
        trend, monkeypatch,
        {
            "ok": True, "attempted": True, "live_ok": executed_qty > 0,
            "order_accepted": accepted, "executed": executed_qty > 0,
            "fully_executed": fully, "executed_qty": executed_qty,
            "requested_qty": 0.1, "order_id": "exit-1",
            "client_order_id": "cid-exit", "exchange_status": status,
            "resp": {"orderId": "exit-1", "status": status,
                     "executedQty": str(executed_qty)},
        },
    )
    result = observed.result
    assert result["order_accepted"] is accepted
    assert result["executed"] is (executed_qty > 0)
    assert result["fully_executed"] is fully
    assert result["executed_qty"] == executed_qty
    assert result["live_ok"] is (executed_qty > 0)
    assert observed.operation_log.count("execution:place_live_order") == 1
    sent = next(e for e in observed.events if e["event_type"] == "LIVE_ORDER_SENT")
    expected_event_reason = (
        "OK" if executed_qty > 0 else
        "ORDER_ACCEPTED_PENDING_FILL" if accepted else "ACK_NO_FILL"
    )
    assert sent["reason"] == expected_event_reason
    if not accepted:
        assert result["blocked_reason"] == "ACK_NO_FILL"
        decision = trend._trend_execution_decision(
            trend._trend_evaluation_context(
                trend_rows()[0][2], trend.datetime.now(trend.timezone.utc),
                {"cfg_effective": SimpleNamespace(trading_mode="LIVE"),
                 "bc": SimpleNamespace(enabled=True), "allowed_orders_entry": True},
            ),
            result, SimpleNamespace(trading_mode="LIVE"), is_exit=True,
            reason_code=trend.DecisionReason.STRATEGY_EXIT,
            reason_text="EXIT", side="SELL", price=102.0, position_id=7,
        )
        assert decision.decision_subtype.value == "ORDER_REJECTED"


def test_real_live_exit_preflight_suppression_does_not_submit(trend, monkeypatch):
    observed = run_live_exit_boundary(
        trend, monkeypatch, {},
        preflight_result={"ok": False, "reason": "LIVE_DISABLED"},
    )
    assert observed.result["live_attempted"] is False
    assert observed.result["live_ok"] is False
    assert observed.result["blocked_reason"] == "LIVE_DISABLED"
    assert "execution:place_live_order" not in observed.operation_log


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


@pytest.mark.parametrize(
    "result, expected_type, expected_subtype, submitted, executed",
    [
        ({"ledger_ok": True, "live_attempted": False, "suppressed": True,
          "blocked_reason": "LIVE_DISABLED"}, "ACTION_SUPPRESSED", "EXECUTION_NOT_ATTEMPTED", False, False),
        ({"ledger_ok": True, "live_attempted": True, "order_accepted": False,
          "executed": False, "executed_qty": 0.0}, "TECHNICAL_FAILURE", "ORDER_REJECTED", False, False),
        ({"ledger_ok": True, "live_attempted": True, "order_accepted": True,
          "executed": False, "executed_qty": 0.0}, "TECHNICAL_FAILURE", "ORDER_ACCEPTED_NOT_FILLED", True, False),
        ({"ledger_ok": True, "live_attempted": True, "order_accepted": True,
          "executed": True, "fully_executed": False, "executed_qty": 0.4,
          "requested_qty": 1.0}, "TECHNICAL_FAILURE", "PARTIAL_EXECUTION", True, True),
        ({"ledger_ok": True, "live_attempted": True, "order_accepted": True,
          "executed": True, "fully_executed": True, "executed_qty": 1.0,
          "requested_qty": 1.0}, "TRADE_EXECUTED", "EXECUTED", True, True),
        ({"ledger_ok": False, "live_attempted": True, "order_accepted": True,
          "executed": True, "fully_executed": True, "executed_qty": 1.0,
          "requested_qty": 1.0}, "TECHNICAL_FAILURE", "LEDGER_FAILURE", True, True),
    ],
)
def test_entry_execution_outcome_classification(
    trend, result, expected_type, expected_subtype, submitted, executed
):
    now = trend.datetime.now(trend.timezone.utc)
    evaluation = trend._trend_evaluation_context(
        trend_rows()[0][2], now,
        {"cfg_effective": SimpleNamespace(trading_mode="LIVE"),
         "bc": SimpleNamespace(enabled=True), "allowed_orders_entry": True},
    )
    decision = trend._trend_execution_decision(
        evaluation, result, SimpleNamespace(trading_mode="LIVE"),
        is_exit=False, reason_code=trend.DecisionReason.SSOT_EXECUTE_AND_RECORD,
        reason_text="ENTRY", side="BUY", price=102.0,
    )
    assert decision.decision_type.value == expected_type
    assert decision.decision_subtype.value == expected_subtype
    assert decision.order_submitted is submitted
    assert decision.trade_executed is executed
    assert decision.details["order_accepted"] is submitted
    assert decision.details["executed"] is executed


@pytest.mark.parametrize("position_side, order_side", [("LONG", "SELL"), ("SHORT", "BUY")])
def test_exit_full_fill_side_and_classification(trend, position_side, order_side):
    now = trend.datetime.now(trend.timezone.utc)
    evaluation = trend._trend_evaluation_context(
        trend_rows()[0][2], now,
        {"cfg_effective": SimpleNamespace(trading_mode="LIVE"),
         "bc": SimpleNamespace(enabled=True), "allowed_orders_entry": True},
    )
    decision = trend._trend_execution_decision(
        evaluation,
        {"ledger_ok": True, "live_attempted": True, "order_accepted": True,
         "executed": True, "fully_executed": True, "executed_qty": 1.0,
         "requested_qty": 1.0},
        SimpleNamespace(trading_mode="LIVE"), is_exit=True,
        reason_code=trend.DecisionReason.STRATEGY_EXIT,
        reason_text=position_side, side=order_side, price=102.0, position_id=7,
    )
    assert decision.decision_subtype.value == "EXIT_EXECUTED"
    assert decision.side == order_side


def test_trend_decision_recursively_freezes_execution_raw(trend):
    now = trend.datetime.now(trend.timezone.utc)
    evaluation = trend._trend_evaluation_context(
        trend_rows()[0][2], now,
        {"cfg_effective": SimpleNamespace(trading_mode="LIVE"),
         "bc": SimpleNamespace(enabled=True), "allowed_orders_entry": True},
    )
    raw = {"ledger_ok": True, "live_attempted": True,
           "order_accepted": True, "executed": False, "executed_qty": 0.0,
           "resp": {"fills": [{"qty": "0"}], "tags": {"pending"},
                    "route": ({"venue": "primary"},)}}
    decision = trend._trend_execution_decision(
        evaluation, raw, SimpleNamespace(trading_mode="LIVE"), is_exit=False,
        reason_code=trend.DecisionReason.SSOT_EXECUTE_AND_RECORD,
        reason_text="ENTRY", side="BUY", price=102.0,
    )
    raw["resp"]["fills"][0]["qty"] = "99"
    assert decision.details["raw"]["resp"]["fills"][0]["qty"] == "0"
    assert isinstance(decision.details["raw"]["resp"]["fills"], tuple)
    assert isinstance(decision.details["raw"]["resp"]["tags"], frozenset)
    assert isinstance(decision.details["raw"]["resp"]["route"], tuple)


@pytest.mark.parametrize(
    "case, attempted, accepted, executed_qty, fully, expected_subtype, event_reason",
    [
        ("not_attempted", False, False, 0.0, False, "EXECUTION_NOT_ATTEMPTED", None),
        ("rejected", True, False, 0.0, False, "ORDER_REJECTED", "ACK_NO_FILL"),
        ("ack_only", True, True, 0.0, False, "ORDER_ACCEPTED_NOT_FILLED", "ORDER_ACCEPTED_PENDING_FILL"),
        ("partial", True, True, 0.04, False, "PARTIAL_EXECUTION", "OK"),
        ("full", True, True, 0.1, True, "EXIT_EXECUTED", "OK"),
    ],
)
def test_live_standard_exit_runtime_matrix(
    harness, case, attempted, accepted, executed_qty, fully, expected_subtype,
    event_reason,
):
    harness.trading_mode = "LIVE"
    harness.set_position(entry_price=100.0, qty=0.1)
    harness.execution_live_attempted = attempted
    harness.execution_order_accepted = accepted
    harness.execution_executed_qty = executed_qty
    harness.execution_fully_executed = fully
    harness.execution_live_ok = executed_qty > 0
    observed = harness.cycle(trend_rows(current=102.0))

    assert observed.final_decision.decision_subtype.value == expected_subtype
    assert observed.final_decision.order_submitted is accepted
    assert observed.final_decision.trade_executed is (executed_qty > 0)
    assert len(observed.execution_attempts) == 1
    assert observed.operation_log[-1] == "strategy_event:RUN_END"
    sent = [e for e in observed.strategy_events if e.get("event_type") == "LIVE_ORDER_SENT"]
    assert ([e["reason"] for e in sent] or [None])[-1] == event_reason
    if case == "full":
        assert observed.position_after is None
        assert len(observed.position_mutations) == 1
    elif case == "partial":
        assert observed.position_after[2] == pytest.approx(0.06)
        assert "REDUCE" in [m.operation for m in observed.position_mutations]
        assert observed.final_decision.details["position_mutation_semantics"] == "QUANTITY_REDUCED"
    else:
        assert observed.position_after[2] == pytest.approx(0.1)
        assert observed.position_mutations == ()


def test_real_exit_adapter_db_guard_failure_runtime_chronology(
    harness, monkeypatch,
):
    harness.trading_mode = "LIVE"
    harness.set_position(entry_price=102.0, qty=0.1, age_minutes=10)
    harness.profit_lock_state = "TRIGGERED"
    install_real_exit_runtime_boundary(
        harness, monkeypatch, {}, ledger_ok=False,
    )
    observed = harness.cycle(trend_rows(current=102.5))
    decision = observed.final_decision
    assert decision.decision_type.value == "TECHNICAL_FAILURE"
    assert decision.decision_subtype.value == "LEDGER_FAILURE"
    assert decision.entry_attempted is False
    assert decision.order_submitted is False
    assert decision.trade_executed is False
    assert decision.details["blocked_reason"] == "DB_GUARD_DUPLICATE"
    assert observed.position_after is not None
    assert observed.position_mutations == ()
    assert len(observed.execution_attempts) == 1
    assert "execution:place_live_order" not in observed.operation_log
    blocked = next(
        event for event in observed.strategy_events
        if event.get("event_type") == "BLOCKED"
    )
    assert blocked["reason"] == "DB_GUARD_DUPLICATE"
    log = observed.operation_log
    assert log.index("strategy_event:EXIT_SIGNAL") < log.index("execution:exit")
    assert log.index("execution:exit") < log.index("strategy_event:BLOCKED")
    assert log.index("strategy_event:BLOCKED") < log.index("strategy_event:RUN_END")
    assert observed.operation_log[-1] == "strategy_event:RUN_END"


def test_real_panic_preflight_suppression_preserves_chronology(
    harness, monkeypatch,
):
    harness.runtime_mode = "PANIC"
    harness.trading_mode = "LIVE"
    harness.set_position(side="LONG", entry_price=100.0, qty=0.1)
    install_real_exit_runtime_boundary(
        harness, monkeypatch, {},
        preflight_result={"ok": False, "reason": "PANIC_PREFLIGHT_BLOCK"},
    )
    lookups = []
    monkeypatch.setattr(
        harness.module, "get_open_position",
        lambda: lookups.append("lookup") or harness.position,
    )
    monkeypatch.setattr(
        harness.module, "safe_close_if_open",
        harness.production_safe_close_if_open,
    )
    monkeypatch.setattr(
        harness.module, "can_trade",
        lambda *_a, **_k: (True, {"why": "panic-boundary-test"}),
    )
    observed = harness.cycle(trend_rows(current=100.0))
    decision = observed.final_decision
    assert decision.decision_type.value == "ACTION_SUPPRESSED"
    assert decision.decision_subtype.value == "EXECUTION_NOT_ATTEMPTED"
    assert decision.action == "SUPPRESS"
    assert decision.entry_attempted is False
    assert decision.order_submitted is False
    assert decision.trade_executed is False
    assert decision.details["blocked_reason"] == "PANIC_PREFLIGHT_BLOCK"
    assert decision.details["raw"]["resp"]["reason"] == "PANIC_PREFLIGHT_BLOCK"
    assert len(lookups) == 1
    assert len(observed.execution_attempts) == 1
    assert "execution:place_live_order" not in observed.operation_log
    assert observed.position_after is not None
    assert not any(m.operation == "CLOSE" for m in observed.position_mutations)
    blocked_index = observed.operation_log.index("strategy_event:BLOCKED")
    blocked_event = next(
        event for event in observed.strategy_events
        if event.get("event_type") == "BLOCKED"
    )
    assert blocked_event["reason"] == "PANIC_PREFLIGHT_BLOCK"
    config_index = observed.operation_log.index("strategy_event:CONFIG_APPLIED")
    mode_index = observed.operation_log.index("state_change:mode")
    assert blocked_index < config_index < mode_index
    assert observed.operation_log[-1] == "strategy_event:RUN_END"


@pytest.mark.parametrize("position_side, exit_side", [("LONG", "SELL"), ("SHORT", "BUY")])
@pytest.mark.parametrize(
    "accepted, executed_qty, fully, expected_subtype",
    [
        (False, 0.0, False, "ORDER_REJECTED"),
        (True, 0.0, False, "ORDER_ACCEPTED_NOT_FILLED"),
        (True, 0.04, False, "PARTIAL_EXECUTION"),
        (True, 0.1, True, "EXIT_EXECUTED"),
    ],
)
def test_panic_live_runtime_preserves_canonical_outcome(
    harness, position_side, exit_side, accepted, executed_qty, fully,
    expected_subtype,
):
    harness.runtime_mode = "PANIC"
    harness.trading_mode = "LIVE"
    harness.set_position(side=position_side, entry_price=100.0, qty=0.1)
    harness.execution_order_accepted = accepted
    harness.execution_executed_qty = executed_qty
    harness.execution_fully_executed = fully
    harness.execution_live_ok = executed_qty > 0
    observed = harness.cycle(trend_rows(current=100.0))
    decision = observed.final_decision
    assert decision.decision_subtype.value == expected_subtype
    assert decision.side == exit_side
    assert decision.order_submitted is accepted
    assert decision.trade_executed is (executed_qty > 0)
    assert len(observed.execution_attempts) == 1
    assert observed.operation_log[-1] == "strategy_event:RUN_END"
    assert observed.operation_log.index("execution:exit") < observed.operation_log.index("strategy_event:LIVE_ORDER_SENT")
    assert observed.operation_log.index("strategy_event:LIVE_ORDER_SENT") < observed.operation_log.index("strategy_event:CONFIG_APPLIED")
    assert observed.operation_log.index("strategy_event:CONFIG_APPLIED") < observed.operation_log.index("state_change:mode")
    if fully:
        assert observed.position_after is None
    elif executed_qty > 0:
        assert observed.position_after[2] == pytest.approx(0.06)
        assert "REDUCE" in [m.operation for m in observed.position_mutations]
    else:
        assert observed.position_after is not None


def test_profit_lock_partial_exit_chronology_reduces_quantity(harness):
    harness.runtime_mode = "NORMAL"
    harness.trading_mode = "LIVE"
    harness.set_position(entry_price=102.0, qty=0.1, age_minutes=10)
    harness.profit_lock_state = "TRIGGERED"
    harness.execution_order_accepted = True
    harness.execution_executed_qty = 0.04
    harness.execution_fully_executed = False
    harness.execution_live_ok = True
    observed = harness.cycle(trend_rows(current=102.5))
    log = observed.operation_log
    assert log.index("strategy_event:EXIT_SIGNAL") < log.index("execution:exit")
    assert log.index("execution:exit") < log.index("strategy_event:LIVE_ORDER_SENT")
    live_sent = next(
        event for event in observed.strategy_events
        if event.get("event_type") == "LIVE_ORDER_SENT"
    )
    assert live_sent["reason"] == "OK"
    assert log.index("strategy_event:LIVE_ORDER_SENT") < log.index("state_change:reduce")
    assert log.index("state_change:reduce") < log.index("strategy_event:RUN_END")
    assert log.index("strategy_event:LIVE_ORDER_SENT") < log.index("strategy_event:RUN_END")
    assert len(observed.position_mutations) == 1
    assert observed.position_mutations[0].operation == "REDUCE"
    assert observed.position_after[2] == pytest.approx(0.06)
    assert len(observed.execution_attempts) == 1
    assert observed.final_decision.decision_subtype.value == "PARTIAL_EXECUTION"


def test_real_panic_db_guard_failure_preserves_chronology(harness, monkeypatch):
    harness.runtime_mode = "PANIC"
    harness.trading_mode = "LIVE"
    harness.set_position(side="SHORT", entry_price=100.0, qty=0.1)
    install_real_exit_runtime_boundary(
        harness, monkeypatch, {}, ledger_ok=False,
    )
    lookups = []
    monkeypatch.setattr(
        harness.module, "get_open_position",
        lambda: lookups.append("lookup") or harness.position,
    )
    monkeypatch.setattr(
        harness.module, "safe_close_if_open",
        harness.production_safe_close_if_open,
    )
    monkeypatch.setattr(
        harness.module, "can_trade",
        lambda *_a, **_k: (True, {"why": "panic-db-guard-test"}),
    )
    observed = harness.cycle(trend_rows(current=100.0))
    decision = observed.final_decision
    assert decision.decision_subtype.value == "LEDGER_FAILURE"
    assert decision.side == "BUY"
    assert decision.entry_attempted is False
    assert decision.order_submitted is False
    assert decision.trade_executed is False
    assert decision.details["blocked_reason"] == "DB_GUARD_DUPLICATE"
    assert len(lookups) == 1
    assert observed.position_after is not None
    assert len(observed.execution_attempts) == 1
    assert "execution:place_live_order" not in observed.operation_log
    blocked = next(
        event for event in observed.strategy_events
        if event.get("event_type") == "BLOCKED"
    )
    assert blocked["reason"] == "DB_GUARD_DUPLICATE"
    log = observed.operation_log
    assert log.index("execution:exit") < log.index("strategy_event:BLOCKED")
    assert log.index("strategy_event:BLOCKED") < log.index("strategy_event:CONFIG_APPLIED")
    assert log.index("strategy_event:CONFIG_APPLIED") < log.index("state_change:mode")
    assert log[-1] == "strategy_event:RUN_END"

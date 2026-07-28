from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from common.decision_contract import (
    DecisionReason,
    DecisionType,
    EvaluationContext,
    FinalDecision,
)

from tests.bot_bbrange.fixtures import (
    FakeConnection,
    Recorder,
    StrictFakeExchange,
    band_frame,
    candle,
    runtime_snapshot,
)


@pytest.fixture
def bbrange(monkeypatch):
    # Synthetic import environment only. No production env file is loaded.
    safe_env = {
        "SYMBOL": "BTCUSDC",
        "QUOTE_ASSET": "USDC",
        "STRATEGY_NAME": "BBRANGE",
        "INTERVAL": "1m",
        "TRADING_MODE": "PAPER",
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
    monkeypatch.delenv("BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
    exchange_client = importlib.import_module("common.exchange_client")
    fake = StrictFakeExchange()
    monkeypatch.setattr(exchange_client, "get_market_data_client", lambda: fake)
    module_name = "waltrade_bot_bbrange_main_characterized"
    sys.modules.pop(module_name, None)
    source = Path(__file__).resolve().parents[2] / "bot_bbrange" / "main.py"
    spec = importlib.util.spec_from_file_location(module_name, source)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    assert module._exchange_client is None
    assert module.get_exchange_client() is fake
    return module


@pytest.fixture
def harness(monkeypatch, bbrange):
    rec = Recorder()

    def event(**kwargs):
        rec.add("strategy_event", **kwargs)

    monkeypatch.setattr(bbrange, "emit_strategy_event", event)
    monkeypatch.setattr(bbrange, "emit_regime_gate_event",
                        lambda **kwargs: rec.add("regime_gate", **kwargs))
    monkeypatch.setattr(bbrange, "heartbeat",
                        lambda info: rec.add("heartbeat", info=info))
    monkeypatch.setattr(bbrange, "get_runtime_snapshot",
                        lambda **_kwargs: runtime_snapshot())
    monkeypatch.setattr(bbrange, "get_open_position",
                        lambda: rec.add("position_lookup") or None)
    monkeypatch.setattr(bbrange, "hard_time_exit_enabled", lambda: False)
    monkeypatch.setattr(bbrange, "load_position_path_snapshot",
                        lambda **_kwargs: SimpleNamespace(
                            bars_seen=1, max_high=100.2, min_low=99.8))
    monkeypatch.setattr(bbrange, "evaluate_profit_lock",
                        lambda **_kwargs: SimpleNamespace(
                            triggered=False, reason_code="NOT_ARMED",
                            trigger_type=None, peak_move_pct=0.0,
                            current_move_pct=0.0, floor_pct=0.0,
                            trail_drop_pct=0.0, age_minutes=0.0))
    monkeypatch.setattr(bbrange, "emit_profit_lock_event_once",
                        lambda **kwargs: rec.add("profit_lock_event", **kwargs))
    monkeypatch.setattr(bbrange, "decide_regime_gate",
                        lambda **_kwargs: SimpleNamespace(
                            allow=True, why="fixture", regime="FLAT", meta={}))
    monkeypatch.setattr(bbrange, "get_trend", lambda *_args: "FLAT")
    monkeypatch.setattr(bbrange.pd, "read_sql_query",
                        lambda *_args, **_kwargs: band_frame())
    monkeypatch.setattr(bbrange, "get_db_conn",
                        lambda: FakeConnection(rec))
    monkeypatch.setattr(bbrange, "compute_qty_from_notional",
                        lambda *_args, **_kwargs: (0.1, {"fixture": True}))
    monkeypatch.setattr(bbrange, "get_user_settings_snapshot", lambda: {})
    monkeypatch.setattr(bbrange, "get_recent_win_streak",
                        lambda **_kwargs: SimpleNamespace(
                            eligible=False, checked=3, required=3, streak=0,
                            source="fixture", error=None, boost_candidate=False,
                            boost_allowed=False, boost_block_reason=None,
                            prev_net_1=None, prev_net_2=None, prev_net_3=None,
                            last_exit_reason=None, last_boost_exit_reason=None,
                            last_trade_gross_pct=None,
                            rolling_5_gross_pct_avg=None))
    monkeypatch.setattr(bbrange, "DAILY_MAX_LOSS_PCT", 0.0)
    monkeypatch.setattr(bbrange, "DISABLE_HOURS_SET", set())
    monkeypatch.setattr(bbrange, "MIN_BB_WIDTH_PCT", 0.0001)
    monkeypatch.setattr(bbrange, "BBRANGE_EXPLORE_ENABLED", False)
    return bbrange, rec


def run_observed(module, rec, row):
    return rec.observe(module.run_strategy(row))


def test_import_is_offline_and_no_new_candle_is_outside_run_strategy(bbrange):
    assert isinstance(bbrange.get_exchange_client(), StrictFakeExchange)
    assert bbrange.LAST_PROCESSED_OPEN_TIME is None


@pytest.mark.parametrize(
    ("row", "expected_reason", "expected_event"),
    [
        (None, "NO_ROW", "SKIP"),
        (candle(ema=None), "INDICATORS_NOT_READY", "SKIP"),
    ],
)
def test_pre_entry_data_paths(harness, row, expected_reason, expected_event):
    module, rec = harness
    observed = run_observed(module, rec, row)
    assert isinstance(observed.returned_value, FinalDecision)
    assert observed.returned_value.reason_text == expected_reason
    assert observed.returned_value.evaluation.strategy == "BBRANGE"
    assert observed.terminal_reason == expected_reason
    assert observed.event_types == ("RUN_START", expected_event, "RUN_END")
    assert not [x for x in observed.operations if x.kind == "exchange_order"]


def test_halt_path(harness, monkeypatch):
    module, rec = harness
    monkeypatch.setattr(module, "get_runtime_snapshot",
                        lambda **_kwargs: runtime_snapshot(mode="HALT"))
    observed = run_observed(module, rec, candle())
    assert observed.terminal_reason == "BOT_MODE_HALT"
    assert observed.event_types == ("RUN_START", "BLOCKED", "RUN_END")


def test_existing_position_neutral_hold(harness, monkeypatch):
    module, rec = harness
    monkeypatch.setattr(module, "get_open_position",
                        lambda: rec.add("position_lookup") or
                        (17, "LONG", 0.1, 100.0, candle()[0]))
    observed = run_observed(module, rec, candle(high=100.2, low=99.8))
    assert observed.terminal_reason == "POSITION_OPEN_NO_EXIT"
    assert "BLOCKED" in observed.event_types
    assert not [x for x in observed.operations if x.kind == "entry_execution"]


@pytest.mark.parametrize(
    ("high", "low", "expected_reason"),
    [(102.0, 100.0, "TAKE_PROFIT"), (100.0, 98.0, "STOP_LOSS")],
)
def test_take_profit_and_stop_loss(harness, monkeypatch, high, low, expected_reason):
    module, rec = harness
    monkeypatch.setattr(module, "get_open_position",
                        lambda: (17, "LONG", 0.1, 100.0, candle()[0]))
    monkeypatch.setattr(module, "execute_and_record",
                        lambda **kwargs: rec.add("exit_execution", **kwargs) or
                        {"ledger_ok": True, "live_ok": True})
    monkeypatch.setattr(module, "close_position",
                        lambda **kwargs: rec.add("position_close", **kwargs) or True)
    run_observed(module, rec, candle(high=high, low=low))
    closes = [x for x in rec.items if x.kind == "position_close"]
    assert closes[0].payload["reason"] == expected_reason
    assert len([x for x in rec.items if x.kind == "exit_execution"]) == 1


def test_no_signal(harness):
    module, rec = harness
    observed = run_observed(module, rec, candle(close=100.0))
    assert observed.terminal_reason == "NO_SIGNAL"
    assert observed.event_types[-2:] == ("SKIP", "RUN_END")


def test_strategy_filter_rejected(harness, monkeypatch):
    module, rec = harness
    monkeypatch.setattr(module, "MIN_BB_WIDTH_PCT", 1.0)
    observed = run_observed(module, rec, candle())
    assert observed.terminal_reason == "BB_WIDTH_TOO_LOW"


def test_regime_block(harness, monkeypatch):
    module, rec = harness
    monkeypatch.setattr(module, "decide_regime_gate",
                        lambda **_kwargs: SimpleNamespace(
                            allow=False, why="fixture-block", regime="TREND", meta={}))
    observed = run_observed(module, rec, candle(close=90.0, low=89.0))
    assert observed.terminal_reason == "REGIME_BLOCK"
    assert observed.event_types[-2:] == ("BLOCKED", "RUN_END")


def test_runtime_disabled(harness, monkeypatch):
    module, rec = harness
    monkeypatch.setattr(module, "get_runtime_snapshot",
                        lambda **_kwargs: runtime_snapshot(enabled=False))
    observed = run_observed(module, rec, candle())
    assert observed.terminal_reason == "BOT_DISABLED"


def test_panic_without_position_preserves_halt_side_effects(harness, monkeypatch):
    module, rec = harness
    monkeypatch.setattr(module, "get_runtime_snapshot",
                        lambda **_kwargs: runtime_snapshot(mode="PANIC"))
    monkeypatch.setattr(module, "set_mode",
                        lambda mode, reason=None: rec.add("set_mode", mode=mode, reason=reason))
    observed = run_observed(module, rec, candle())
    # Characterized behavior: PANIC with no position emits no terminal reason.
    assert observed.terminal_reason is None
    assert [x.payload["mode"] for x in rec.items if x.kind == "set_mode"] == ["HALT"]


@pytest.mark.parametrize(
    ("setup", "reason"),
    [
        ("disable_hour", "DISABLE_HOURS"),
        ("not_enough", "NOT_ENOUGH_CANDLES"),
        ("bb_not_ready", "BB_NOT_READY"),
        ("trend", "TREND_NOT_FLAT"),
        ("rsi_extreme", "RSI_EXTREME_BLOCK"),
        ("rsi_max", "RSI_LONG_MAX_BLOCK"),
        ("qty_zero", "SIZING_QTY_ZERO"),
    ],
)
def test_additional_entry_terminal_gates(harness, monkeypatch, setup, reason):
    module, rec = harness
    row = candle(close=90.0, low=89.0)
    if setup == "disable_hour":
        monkeypatch.setattr(module, "DISABLE_HOURS_SET", {row[0].hour})
    elif setup == "not_enough":
        monkeypatch.setattr(module.pd, "read_sql_query",
                            lambda *_args, **_kwargs: band_frame().iloc[:2])
    elif setup == "bb_not_ready":
        frame = band_frame().copy()
        frame["close"] = float("nan")
        monkeypatch.setattr(module.pd, "read_sql_query",
                            lambda *_args, **_kwargs: frame)
    elif setup == "trend":
        monkeypatch.setattr(module, "get_trend", lambda *_args: "UP")
    elif setup == "rsi_extreme":
        row = candle(close=90.0, low=89.0, rsi=5.0)
    elif setup == "rsi_max":
        row = candle(close=90.0, low=89.0, rsi=50.0)
    elif setup == "qty_zero":
        monkeypatch.setattr(module, "compute_qty_from_notional",
                            lambda *_args, **_kwargs: (0.0, {"fixture": True}))
    observed = run_observed(module, rec, row)
    assert observed.terminal_reason == reason


def test_daily_loss_gate(harness, monkeypatch):
    module, rec = harness
    monkeypatch.setattr(module, "DAILY_MAX_LOSS_PCT", 1.0)
    monkeypatch.setattr(module, "get_runtime_snapshot",
                        lambda **_kwargs: runtime_snapshot(trading_mode="LIVE"))
    monkeypatch.setattr(module, "compute_daily_loss_pct_positions",
                        lambda *_args, **_kwargs: {"daily_pct": -2.0})
    monkeypatch.setattr(module, "should_emit_daily_loss_shadow", lambda **_kwargs: False)
    monkeypatch.setattr(module, "should_block_daily_loss_positions",
                        lambda **_kwargs: True)
    observed = run_observed(module, rec, candle())
    assert observed.terminal_reason == "DAILY_MAX_LOSS_POSITIONS"


@pytest.mark.parametrize("path", ["profit_lock", "time_exit"])
def test_profit_lock_and_time_exit_categories(harness, monkeypatch, path):
    module, rec = harness
    old_time = candle()[0].replace(year=2025)
    monkeypatch.setattr(module, "get_open_position",
                        lambda: (17, "LONG", 0.1, 100.0, old_time))
    monkeypatch.setattr(module, "execute_and_record",
                        lambda **kwargs: rec.add("exit_execution", **kwargs) or
                        {"ledger_ok": True, "live_ok": True})
    monkeypatch.setattr(module, "close_position",
                        lambda **kwargs: rec.add("position_close", **kwargs) or True)
    if path == "profit_lock":
        monkeypatch.setattr(module, "evaluate_profit_lock",
                            lambda **_kwargs: SimpleNamespace(
                                triggered=True, reason_code="PROFIT_LOCK_LONG",
                                trigger_type="FLOOR", peak_move_pct=2.0,
                                current_move_pct=1.0, floor_pct=0.8,
                                trail_drop_pct=1.0, age_minutes=10.0))
    else:
        monkeypatch.setattr(module, "hard_time_exit_enabled", lambda: True)
        monkeypatch.setattr(module, "MAX_POSITION_MINUTES", 1)
    run_observed(module, rec, candle(high=100.2, low=99.8))
    assert len([x for x in rec.items if x.kind == "exit_execution"]) == 1
    assert len([x for x in rec.items if x.kind == "position_close"]) == 1


@pytest.mark.parametrize(
    ("result", "terminal", "position_event"),
    [
        ({"ledger_ok": False, "live_attempted": False, "live_ok": False,
          "blocked_reason": "DB_GUARD_DUPLICATE"}, "DB_GUARD_DUPLICATE", None),
        ({"ledger_ok": True, "live_attempted": False, "live_ok": True,
          "blocked_reason": None}, "SSOT_EXECUTE_AND_RECORD", "POSITION_OPENED"),
        ({"ledger_ok": True, "live_attempted": False, "live_ok": False,
          "blocked_reason": "LIVE_ORDER_SUPPRESSED"},
         "LIVE_ENTRY_NOT_ATTEMPTED", None),
        ({"ledger_ok": True, "live_attempted": True, "live_ok": False,
          "blocked_reason": "LIVE_ORDER_FAILED"}, "LIVE_ENTRY_NOT_FILLED", None),
    ],
)
def test_entry_execution_outer_contract(harness, monkeypatch, result, terminal,
                                        position_event):
    module, rec = harness
    trading_mode = "LIVE" if terminal.startswith("LIVE_") else "PAPER"
    monkeypatch.setattr(module, "get_runtime_snapshot",
                        lambda **_kwargs: runtime_snapshot(trading_mode=trading_mode))

    def execute(**kwargs):
        rec.add("entry_execution", **kwargs)
        if result.get("blocked_reason"):
            module.emit_strategy_event(
                event_type="BLOCKED", decision="BUY",
                reason=result["blocked_reason"] == "LIVE_ORDER_SUPPRESSED"
                and "LIVE_ENTRY_NOT_ATTEMPTED" or result["blocked_reason"],
                price=kwargs["price"], candle_open_time=kwargs["candle_open_time"],
                info={},
            )
        return result

    monkeypatch.setattr(module, "execute_and_record", execute)
    observed = run_observed(module, rec, candle(close=90.0, low=89.0))
    assert len([x for x in rec.items if x.kind == "entry_execution"]) == 1
    assert observed.terminal_reason == terminal
    assert (position_event in observed.event_types) if position_event else True


def test_execute_and_record_duplicate_paper_and_live_suppressed(bbrange, monkeypatch):
    rec = Recorder()
    monkeypatch.setattr(bbrange, "emit_strategy_event",
                        lambda **kwargs: rec.add("strategy_event", **kwargs))
    cfg = runtime_snapshot()["cfg_effective"]

    monkeypatch.setattr(bbrange, "insert_simulated_order", lambda **_kwargs: False)
    duplicate = bbrange.execute_and_record(
        "BUY", 90.0, 0.1, "fixture", candle()[0], is_exit=False,
        cfg_used=cfg, allow_live_orders=False, allow_meta={}, rsi_14=40, ema_21=100)
    assert duplicate["blocked_reason"] == "DB_GUARD_DUPLICATE"
    assert [x.payload["reason"] for x in rec.items] == ["DB_GUARD_DUPLICATE"]

    rec.items.clear()
    monkeypatch.setattr(bbrange, "insert_simulated_order", lambda **_kwargs: True)
    monkeypatch.setattr(bbrange, "open_position",
                        lambda *_args, **_kwargs: rec.add("position_insert") or 44)
    paper = bbrange.execute_and_record(
        "BUY", 90.0, 0.1, "fixture", candle()[0], is_exit=False,
        cfg_used=cfg, allow_live_orders=False, allow_meta={}, rsi_14=40, ema_21=100)
    assert paper["live_ok"] is True
    assert [x.payload["event_type"] for x in rec.items
            if x.kind == "strategy_event"] == ["SIM_ORDER_CREATED", "PAPER_POSITION_OPENED"]

    rec.items.clear()
    live_cfg = runtime_snapshot(trading_mode="LIVE", allow_entry=False)["cfg_effective"]
    suppressed = bbrange.execute_and_record(
        "BUY", 90.0, 0.1, "fixture", candle()[0], is_exit=False,
        cfg_used=live_cfg, allow_live_orders=False,
        allow_meta={"why": "fixture"}, rsi_14=40, ema_21=100)
    assert suppressed["blocked_reason"] == "LIVE_ORDER_SUPPRESSED"
    assert [x.payload["reason"] for x in rec.items
            if x.kind == "strategy_event"] == ["LEDGER_OK", "LIVE_ENTRY_NOT_ATTEMPTED"]


def test_execute_and_record_live_failure_and_fill(bbrange, monkeypatch):
    rec = Recorder()
    monkeypatch.setattr(bbrange, "emit_strategy_event",
                        lambda **kwargs: rec.add("strategy_event", **kwargs))
    monkeypatch.setattr(bbrange, "insert_simulated_order", lambda **_kwargs: True)
    monkeypatch.setattr(bbrange, "get_open_position", lambda: None)
    monkeypatch.setattr(bbrange, "build_live_entry_intent_client_order_id",
                        lambda *_args: "fixture-entry-id")
    monkeypatch.setattr(bbrange, "get_db_conn", lambda: FakeConnection(rec))
    cfg = runtime_snapshot(trading_mode="LIVE")["cfg_effective"]

    monkeypatch.setattr(bbrange, "place_live_order",
                        lambda *_args, **_kwargs: {"ok": False, "resp": {"code": "X"}})
    failed = bbrange.execute_and_record(
        "BUY", 90.0, 0.1, "fixture", candle()[0], is_exit=False,
        cfg_used=cfg, allow_live_orders=True, allow_meta={}, rsi_14=40, ema_21=100)
    assert failed["blocked_reason"] == "LIVE_ORDER_FAILED"

    rec.items.clear()
    monkeypatch.setattr(bbrange, "place_live_order",
                        lambda *_args, **_kwargs: {
                            "ok": True, "live_ok": True,
                            "resp": {"status": "FILLED", "executedQty": "0.1",
                                     "orderId": "order-7"}})
    monkeypatch.setattr(bbrange, "open_position_from_live_ack",
                        lambda **kwargs: rec.add("position_insert", **kwargs) or 77)
    monkeypatch.setattr(bbrange, "attach_entry_order_id_with_conn",
                        lambda *_args, **_kwargs: rec.add("position_attach"))
    filled = bbrange.execute_and_record(
        "BUY", 90.0, 0.1, "fixture", candle()[0], is_exit=False,
        cfg_used=cfg, allow_live_orders=True, allow_meta={}, rsi_14=40, ema_21=100)
    assert filled["live_ok"] is True
    assert len([x for x in rec.items if x.kind == "position_insert"]) == 1
    assert len([x for x in rec.items if x.kind == "position_attach"]) == 1


def exit_evaluation(module):
    now = candle()[0]
    return EvaluationContext(
        deployment_id="local-paper",
        environment="trading_paper",
        symbol=module.SYMBOL,
        interval=module.INTERVAL,
        strategy=module.STRATEGY_NAME,
        candle_open_time=now,
        evaluation_started_at=now,
        engine_name=module.STRATEGY_NAME,
        paper_mode=True,
    )


def test_paper_close_exception_fails_closed(bbrange, monkeypatch):
    events = []
    monkeypatch.setattr(bbrange, "insert_simulated_order", lambda **_kwargs: 501)
    monkeypatch.setattr(
        bbrange, "get_open_position",
        lambda: (77, "LONG", 0.1, 100.0, candle()[0]),
    )
    monkeypatch.setattr(
        bbrange, "close_position",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("db unavailable")
        ),
    )
    monkeypatch.setattr(
        bbrange, "emit_strategy_event",
        lambda **event: events.append(event),
    )
    cfg = runtime_snapshot()["cfg_effective"]

    result = bbrange.execute_and_record(
        "SELL", 101.0, 0.1, "test-close", candle()[0], is_exit=True,
        cfg_used=cfg, allow_live_orders=False, allow_meta={},
        rsi_14=50.0, ema_21=100.0,
    )
    decision = bbrange._bbrange_exit_decision(
        exit_evaluation(bbrange), result, cfg,
        reason_code=DecisionReason.TAKE_PROFIT,
        reason_text="test-close", price=101.0, position_id=77,
    )

    assert result["ledger_ok"] is False
    assert result["position_close_succeeded"] is False
    assert result["blocked_reason"] == "POSITION_CLOSE_FAILED"
    assert decision.action != "EXIT"
    assert not any(
        event["event_type"] == "PAPER_POSITION_CLOSED" for event in events
    )
    failure = next(event for event in events if event["reason"] == "POSITION_CLOSE_FAILED")
    assert failure["info"]["position_id"] == 77
    assert failure["info"]["simulated_order_id"] == 501


@pytest.mark.parametrize("closed_ok", [True, False])
def test_paper_close_event_type_matches_mutation(
    bbrange, monkeypatch, closed_ok
):
    events = []
    monkeypatch.setattr(bbrange, "insert_simulated_order", lambda **_kwargs: 501)
    monkeypatch.setattr(
        bbrange, "get_open_position",
        lambda: (77, "LONG", 0.1, 100.0, candle()[0]),
    )

    def close(*_args, **_kwargs):
        if closed_ok:
            bbrange.emit_strategy_event(
                event_type="POSITION_CLOSED", decision=None,
                reason="test-close", price=101.0,
                candle_open_time=candle()[0], info={"position_id": 77},
            )
        return closed_ok

    monkeypatch.setattr(bbrange, "close_position", close)
    monkeypatch.setattr(
        bbrange, "record_simulated_fill_evidence",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(bbrange, "get_exchange_client", lambda: object())
    monkeypatch.setattr(
        bbrange, "emit_strategy_event", lambda **event: events.append(event)
    )
    cfg = runtime_snapshot()["cfg_effective"]

    result = bbrange.execute_and_record(
        "SELL", 101.0, 0.1, "test-close", candle()[0], is_exit=True,
        cfg_used=cfg, allow_live_orders=False, allow_meta={},
        rsi_14=50.0, ema_21=100.0,
    )

    closed = [
        event for event in events if "CLOSED" in event["event_type"]
    ]
    failed = [
        event for event in events
        if event["event_type"] == "POSITION_CLOSE_FAILED"
    ]
    assert len(closed) == (1 if closed_ok else 0)
    assert len(failed) == (0 if closed_ok else 1)
    assert result["ledger_ok"] is closed_ok
    assert result["position_close_succeeded"] is closed_ok


@pytest.mark.parametrize("position_id", [44, None])
def test_paper_open_event_type_matches_mutation(
    bbrange, monkeypatch, position_id
):
    events = []
    monkeypatch.setattr(bbrange, "insert_simulated_order", lambda **_kwargs: 501)
    monkeypatch.setattr(
        bbrange, "open_position",
        lambda *_args, **_kwargs: position_id,
    )
    monkeypatch.setattr(
        bbrange, "record_simulated_fill_evidence",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(bbrange, "get_exchange_client", lambda: object())
    monkeypatch.setattr(
        bbrange, "emit_strategy_event", lambda **event: events.append(event)
    )
    cfg = runtime_snapshot()["cfg_effective"]

    bbrange.execute_and_record(
        "BUY", 100.0, 0.1, "test-open", candle()[0], is_exit=False,
        cfg_used=cfg, allow_live_orders=False, allow_meta={},
        rsi_14=50.0, ema_21=100.0,
    )

    opened = [
        event for event in events if "OPENED" in event["event_type"]
    ]
    assert len(opened) == (1 if position_id else 0)
    if not position_id:
        assert any(
            event["event_type"] == "BLOCKED"
            and event["reason"] == "POSITIONS_OPEN_SKIPPED"
            for event in events
        )


@pytest.mark.parametrize(
    ("result", "successful"),
    [
        (
            {
                "ledger_ok": True, "live_ok": True,
                "blocked_reason": None,
            },
            False,
        ),
        (
            {
                "ledger_ok": True, "live_ok": True,
                "blocked_reason": None,
                "position_close_succeeded": True,
            },
            True,
        ),
        (
            {
                "ledger_ok": False, "live_ok": False,
                "blocked_reason": "POSITION_CLOSE_FAILED",
                "position_close_succeeded": False,
            },
            False,
        ),
    ],
)
def test_paper_exit_decision_requires_explicit_close_success(
    bbrange, result, successful
):
    cfg = runtime_snapshot()["cfg_effective"]
    decision = bbrange._bbrange_exit_decision(
        exit_evaluation(bbrange), result, cfg,
        reason_code=DecisionReason.TAKE_PROFIT,
        reason_text="test-close", price=101.0, position_id=77,
    )
    assert (decision.action == "EXIT") is successful


@pytest.mark.parametrize(
    ("scenario", "expected_type"),
    [
        ("no_signal", DecisionType.NO_TRADE),
        ("regime", DecisionType.ENTRY_BLOCKED),
        ("paper", DecisionType.PAPER_SIMULATION),
        ("duplicate", DecisionType.TECHNICAL_FAILURE),
        ("live_suppressed", DecisionType.ENTRY_SUPPRESSED),
    ],
)
def test_final_decision_sink_exactly_once_for_entry_paths(
        harness, monkeypatch, scenario, expected_type):
    module, rec = harness
    row = candle(close=100.0) if scenario == "no_signal" else candle(close=90.0, low=89.0)
    if scenario == "regime":
        monkeypatch.setattr(module, "decide_regime_gate",
                            lambda **_kwargs: SimpleNamespace(
                                allow=False, why="fixture-block", regime="TREND", meta={}))
    elif scenario in {"paper", "duplicate", "live_suppressed"}:
        result = {
            "ledger_ok": scenario != "duplicate",
            "live_attempted": False,
            "live_ok": scenario == "paper",
            "blocked_reason": (
                "DB_GUARD_DUPLICATE" if scenario == "duplicate"
                else "LIVE_ORDER_SUPPRESSED" if scenario == "live_suppressed"
                else None
            ),
        }
        if scenario == "live_suppressed":
            monkeypatch.setattr(module, "get_runtime_snapshot",
                                lambda **_kwargs: runtime_snapshot(trading_mode="LIVE"))
        monkeypatch.setattr(module, "execute_and_record", lambda **_kwargs: result)

    decisions = []
    returned = module.run_strategy(row, decision_sink=decisions.append)
    assert isinstance(returned, FinalDecision)
    assert returned.decision_type is expected_type
    assert returned.evaluation.symbol == module.SYMBOL
    assert returned.evaluation.interval == module.INTERVAL
    assert returned.evaluation.strategy == module.STRATEGY_NAME
    assert len(decisions) == 1
    assert decisions == [returned]
    assert rec.items[-1].payload["event_type"] == "RUN_END"


@pytest.mark.parametrize("path", ["no_row", "halt", "position"])
def test_final_decision_sink_not_called_outside_full_entry(
        harness, monkeypatch, path):
    module, _rec = harness
    row = None if path == "no_row" else candle()
    if path == "halt":
        monkeypatch.setattr(module, "get_runtime_snapshot",
                            lambda **_kwargs: runtime_snapshot(mode="HALT"))
    elif path == "position":
        monkeypatch.setattr(module, "get_open_position",
                            lambda: (17, "LONG", 0.1, 100.0, candle()[0]))
    decisions = []
    returned = module.run_strategy(row, decision_sink=decisions.append)
    assert isinstance(returned, FinalDecision)
    assert returned.evaluation.strategy == module.STRATEGY_NAME
    assert decisions == []


def test_final_decision_sink_failure_is_fail_open(harness, caplog):
    module, rec = harness

    def broken_sink(_decision):
        raise RuntimeError("fixture sink failure")

    returned = module.run_strategy(candle(), decision_sink=broken_sink)
    assert isinstance(returned, FinalDecision)
    assert returned.reason_code.value == "NO_SIGNAL"
    assert rec.items[-1].payload["event_type"] == "RUN_END"
    assert "trading result unchanged" in caplog.text

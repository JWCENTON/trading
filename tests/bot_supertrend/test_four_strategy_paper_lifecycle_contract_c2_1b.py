from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATHS = {
    "RSI": ROOT / "bot/main.py",
    "TREND": ROOT / "bot_trend/main.py",
    "BBRANGE": ROOT / "bot_bbrange/main.py",
    "SUPERTREND": ROOT / "bot_supertrend/main.py",
}
NOW = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)


def load_strategy(monkeypatch, strategy):
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    monkeypatch.setenv("STRATEGY_NAME", strategy)
    monkeypatch.setenv("SYMBOL", "BTCUSDC")
    monkeypatch.setenv("INTERVAL", "1m")
    name = f"waltrade_paper_parity_{strategy.lower()}"
    sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(name, MODULE_PATHS[strategy])
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    monkeypatch.setattr(
        module,
        "execute_paper_exit_after_preflight",
        lambda *_args, action, **_kwargs: action(
            SimpleNamespace(position_id=77)
        ),
    )
    if strategy == "SUPERTREND":
        monkeypatch.setattr(
            module, "paper_supertrend_entries_enabled", lambda *_a, **_k: (True, None)
        )
        monkeypatch.setattr(
            module, "expire_paper_supertrend_slot_canaries", lambda *_a, **_k: 0
        )
        monkeypatch.setattr(module, "persist_exit_intent", lambda *_a, **_k: "fixture")
        monkeypatch.setattr(
            module, "reconcile_terminal_compatibility_outcome",
            lambda *_a, **_k: SimpleNamespace(
                applied=False, reason="INVENTORY_NOT_TERMINAL"
            ),
        )
    return module


@pytest.mark.parametrize("strategy", tuple(MODULE_PATHS))
def test_four_strategy_paper_entry_exit_has_direct_position_evidence(
    monkeypatch, strategy
):
    module = load_strategy(monkeypatch, strategy)
    state = {"position_id": None, "next_order": 500}
    evidence = []

    def insert_order(*_args, **_kwargs):
        state["next_order"] += 1
        return state["next_order"]

    def record(*_args, **kwargs):
        evidence.append(dict(kwargs))
        if (
            strategy in {"RSI", "TREND", "BBRANGE"}
            and kwargs.get("require_terminal_close")
        ):
            assert state["position_id"] == 77
            state["position_id"] = None
        return True

    def open_position(*_args, **_kwargs):
        assert state["position_id"] is None
        state["position_id"] = 77
        return 77

    def close_position(*_args, **_kwargs):
        assert state["position_id"] == 77
        state["position_id"] = None
        return True

    def get_open_position():
        if state["position_id"] is None:
            return None
        return (77, "LONG", 0.1, 100.0, NOW)

    monkeypatch.setattr(module, "insert_simulated_order", insert_order)
    monkeypatch.setattr(module, "record_simulated_fill_evidence", record)
    monkeypatch.setattr(module, "get_exchange_client", lambda: object())
    monkeypatch.setattr(module, "emit_strategy_event", lambda **_kwargs: None)
    if strategy == "SUPERTREND":
        monkeypatch.setattr(
            module, "paper_supertrend_entries_enabled", lambda *_a, **_k: (True, None)
        )
        monkeypatch.setattr(module, "persist_exit_intent", lambda *_a, **_k: "fixture")
        monkeypatch.setattr(
            module, "reconcile_terminal_compatibility_outcome",
            lambda *_a, **_k: SimpleNamespace(
                applied=False, reason="INVENTORY_NOT_TERMINAL"
            ),
        )
    if hasattr(module, "open_position"):
        monkeypatch.setattr(module, "open_position", open_position)
    if hasattr(module, "close_position"):
        monkeypatch.setattr(module, "close_position", close_position)
    if hasattr(module, "get_open_position"):
        monkeypatch.setattr(module, "get_open_position", get_open_position)

    if strategy == "RSI":
        def apply_rsi(*, is_exit, **_kwargs):
            position_id = state["position_id"]
            if is_exit:
                action = "EXIT_PENDING_CANONICAL_PERSISTENCE"
            else:
                position_id = open_position()
                action = "ENTRY_OPENED"
            return {
                "ok": True, "blocked_reason": None, "pos_id": position_id,
                "client_order_id": None,
                "paper_pos_action": action,
            }

        monkeypatch.setattr(module, "ssot_apply_positions_paper", apply_rsi)
    elif strategy == "TREND":
        def apply_trend(*, is_exit, **_kwargs):
            position_id = state["position_id"]
            if is_exit:
                action = "EXIT_PENDING_CANONICAL_PERSISTENCE"
            else:
                position_id = open_position()
                action = "ENTRY_OPENED"
            return {"paper_pos_action": action, "paper_pos_id": position_id}

        monkeypatch.setattr(module, "ssot_apply_positions_paper", apply_trend)

    config = SimpleNamespace(
        symbol="BTCUSDC", interval="1m", trading_mode="PAPER",
        live_orders_enabled=False, quote_asset="USDC",
    )

    def invoke(is_exit):
        common = dict(
            side="SELL" if is_exit else "BUY", price=101.0 if is_exit else 100.0,
            qty_btc=0.1, reason="parity", candle_open_time=NOW,
            is_exit=is_exit, cfg_used=config, allow_live_orders=False,
            allow_meta={},
        )
        if strategy in {"TREND", "BBRANGE"}:
            common.update(rsi_14=50.0, ema_21=100.0)
        return module.execute_and_record(**common)

    entry = invoke(False)
    assert state["position_id"] == 77
    assert evidence[-1]["position_id"] == 77
    assert evidence[-1]["simulated_order_id"] == 501

    exit_result = invoke(True)
    if strategy == "SUPERTREND":
        assert exit_result["position_id"] == 77
        close_position()
    assert state["position_id"] is None
    assert evidence[-1]["position_id"] == 77
    assert evidence[-1]["simulated_order_id"] == 502
    assert [item["position_id"] for item in evidence] == [77, 77]
    assert [item["environment"] for item in evidence] == ["paper", "paper"]
    assert entry["ledger_ok"] is True
    assert exit_result["ledger_ok"] is True


@pytest.mark.parametrize("strategy", tuple(MODULE_PATHS))
@pytest.mark.parametrize("close_result", [True, False])
def test_four_strategy_exit_result_requires_successful_close(
    monkeypatch, strategy, close_result
):
    module = load_strategy(monkeypatch, strategy)
    close_calls = []
    events = []
    monkeypatch.setattr(module, "insert_simulated_order", lambda **_kwargs: 601)
    monkeypatch.setattr(
        module, "record_simulated_fill_evidence",
        lambda *_args, **_kwargs: close_result,
    )
    monkeypatch.setattr(module, "get_exchange_client", lambda: object())
    monkeypatch.setattr(
        module, "emit_strategy_event", lambda **event: events.append(event)
    )
    monkeypatch.setattr(
        module, "get_open_position", lambda: (77, "LONG", 0.1, 100.0, NOW)
    )

    def close(*_args, **_kwargs):
        close_calls.append(77)
        if close_result:
            module.emit_strategy_event(
                event_type="POSITION_CLOSED", decision=None,
                reason="contract", price=101.0, candle_open_time=NOW,
                info={"position_id": 77},
            )
        return close_result

    monkeypatch.setattr(module, "close_position", close)
    if strategy == "RSI":
        monkeypatch.setattr(
            module,
            "ssot_apply_positions_paper",
            lambda **_kwargs: {
                "ok": True,
                "blocked_reason": None,
                "pos_id": 77,
                "client_order_id": None,
                "paper_pos_action": "EXIT_PENDING_CANONICAL_PERSISTENCE",
            },
        )
    elif strategy == "TREND":
        def apply_trend(**_kwargs):
            return {
                "paper_pos_action": "EXIT_PENDING_CANONICAL_PERSISTENCE",
                "paper_pos_id": 77,
                "position_close_succeeded": None,
            }

        monkeypatch.setattr(
            module,
            "ssot_apply_positions_paper",
            apply_trend,
        )

    config = SimpleNamespace(
        symbol="BTCUSDC", interval="1m", trading_mode="PAPER",
        live_orders_enabled=False, quote_asset="USDC",
    )
    kwargs = dict(
        side="SELL", price=101.0, qty_btc=0.1, reason="contract",
        candle_open_time=NOW, is_exit=True, cfg_used=config,
        allow_live_orders=False, allow_meta={},
    )
    if strategy in {"TREND", "BBRANGE"}:
        kwargs.update(rsi_14=50.0, ema_21=100.0)

    result = module.execute_and_record(**kwargs)
    if strategy == "SUPERTREND":
        result = module._close_supertrend_exit(
            result, exit_price=101.0, reason="contract", candle_open_time=NOW
        )

    assert result.get("position_close_succeeded", close_result) is close_result
    if close_result:
        assert result["blocked_reason"] is None
        assert sum(
            event["event_type"] == "POSITION_CLOSED" for event in events
        ) == 1
    else:
        assert result["blocked_reason"] == "POSITION_CLOSE_FAILED"
        assert result.get("ledger_ok", False) is False or strategy == "SUPERTREND"
        assert not any(
            "CLOSED" in event["event_type"] for event in events
        )
        assert sum(
            event["event_type"] in {
                "POSITION_CLOSE_FAILED", "BLOCKED", "ERROR"
            }
            and event["reason"] == "POSITION_CLOSE_FAILED"
            for event in events
        ) == 1
    if strategy in {"RSI", "TREND", "BBRANGE"}:
        assert close_calls == []
    else:
        assert len(close_calls) <= 1


@pytest.mark.parametrize("strategy", tuple(MODULE_PATHS))
def test_four_strategy_close_exception_never_reports_success(
    monkeypatch, strategy
):
    module = load_strategy(monkeypatch, strategy)
    monkeypatch.setattr(module, "insert_simulated_order", lambda **_kwargs: 701)
    monkeypatch.setattr(module, "emit_strategy_event", lambda **_kwargs: None)
    monkeypatch.setattr(module, "get_exchange_client", lambda: object())
    error = RuntimeError("close unavailable")
    monkeypatch.setattr(
        module,
        "record_simulated_fill_evidence",
        (
            (lambda *_args, **_kwargs: (_ for _ in ()).throw(error))
            if strategy in {"RSI", "TREND", "BBRANGE"}
            else (lambda *_args, **_kwargs: True)
        ),
    )
    monkeypatch.setattr(
        module, "get_open_position", lambda: (77, "LONG", 0.1, 100.0, NOW)
    )

    if strategy == "RSI":
        monkeypatch.setattr(
            module,
            "ssot_apply_positions_paper",
            lambda **_kwargs: {
                "ok": True, "blocked_reason": None, "pos_id": 77,
                "client_order_id": None,
                "paper_pos_action": "EXIT_PENDING_CANONICAL_PERSISTENCE",
            },
        )
    elif strategy == "TREND":
        monkeypatch.setattr(
            module,
            "ssot_apply_positions_paper",
            lambda **_kwargs: {
                "paper_pos_action": "EXIT_PENDING_CANONICAL_PERSISTENCE",
                "paper_pos_id": 77,
                "position_close_succeeded": None,
            },
        )
    elif strategy == "SUPERTREND":
        monkeypatch.setattr(
            module, "close_position",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(error),
        )

    config = SimpleNamespace(
        symbol="BTCUSDC", interval="1m", trading_mode="PAPER",
        live_orders_enabled=False, quote_asset="USDC",
    )
    kwargs = dict(
        side="SELL", price=101.0, qty_btc=0.1, reason="exception-contract",
        candle_open_time=NOW, is_exit=True, cfg_used=config,
        allow_live_orders=False, allow_meta={},
    )
    if strategy in {"TREND", "BBRANGE"}:
        kwargs.update(rsi_14=50.0, ema_21=100.0)

    result = module.execute_and_record(**kwargs)
    if strategy == "SUPERTREND":
        result = module._close_supertrend_exit(
            result, exit_price=101.0, reason="exception-contract",
            candle_open_time=NOW,
        )
    assert result["position_close_succeeded"] is False
    assert result["blocked_reason"] == "POSITION_CLOSE_FAILED"


@pytest.mark.parametrize("strategy", tuple(MODULE_PATHS))
def test_four_strategy_paper_exit_denial_precedes_simulated_order(
    monkeypatch, strategy
):
    module = load_strategy(monkeypatch, strategy)
    calls = {"preflight": 0, "insert": 0}

    def deny(*_args, **_kwargs):
        calls["preflight"] += 1
        return {
            "ledger_ok": False,
            "blocked_reason": "PAPER_EXIT_PREFLIGHT_BLOCKED",
            "preflight_reason_code": "ENTRY_BEFORE_ACTIVE_ADOPTION",
        }

    monkeypatch.setattr(module, "execute_paper_exit_after_preflight", deny)
    monkeypatch.setattr(
        module, "insert_simulated_order",
        lambda **_kwargs: calls.__setitem__("insert", calls["insert"] + 1),
    )
    config = SimpleNamespace(
        symbol="BTCUSDC", interval="1m", trading_mode="PAPER",
        live_orders_enabled=False, quote_asset="USDC",
    )
    kwargs = dict(
        side="SELL", price=99.0, qty_btc=0.1, reason="STOP_LOSS",
        candle_open_time=NOW, is_exit=True, cfg_used=config,
        allow_live_orders=False, allow_meta={},
    )
    if strategy in {"TREND", "BBRANGE"}:
        kwargs.update(rsi_14=50.0, ema_21=100.0)
    result = module.execute_and_record(**kwargs)
    assert result["blocked_reason"] == "PAPER_EXIT_PREFLIGHT_BLOCKED"
    assert result["preflight_reason_code"] == "ENTRY_BEFORE_ACTIVE_ADOPTION"
    assert calls == {"preflight": 1, "insert": 0}


@pytest.mark.parametrize("strategy", tuple(MODULE_PATHS))
def test_four_strategy_live_exit_does_not_enter_paper_preflight(
    monkeypatch, strategy
):
    module = load_strategy(monkeypatch, strategy)
    monkeypatch.setattr(
        module, "execute_paper_exit_after_preflight",
        lambda *_args, **_kwargs: pytest.fail("PAPER preflight reached by LIVE"),
    )
    monkeypatch.setattr(module, "insert_simulated_order", lambda **_kwargs: 801)
    monkeypatch.setattr(module, "emit_strategy_event", lambda **_kwargs: None)
    config = SimpleNamespace(
        symbol="BTCUSDC", interval="1m", trading_mode="LIVE",
        live_orders_enabled=False, quote_asset="USDC",
    )
    kwargs = dict(
        side="SELL", price=99.0, qty_btc=0.1, reason="STOP_LOSS",
        candle_open_time=NOW, is_exit=True, cfg_used=config,
        allow_live_orders=False, allow_meta={},
    )
    if strategy in {"TREND", "BBRANGE"}:
        kwargs.update(rsi_14=50.0, ema_21=100.0)
    result = module.execute_and_record(**kwargs)
    assert result["ledger_ok"] is True
    assert result["live_attempted"] is False

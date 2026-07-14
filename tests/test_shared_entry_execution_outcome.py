from __future__ import annotations

import importlib
import importlib.util
import inspect
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from common.decision_contract import (
    ExecutionStage,
    normalize_entry_execution_outcome,
)


ROOT = Path(__file__).resolve().parents[1]
STRATEGIES = {
    "RSI": ROOT / "bot" / "main.py",
    "TREND": ROOT / "bot_trend" / "main.py",
    "SUPERTREND": ROOT / "bot_supertrend" / "main.py",
    "BBRANGE": ROOT / "bot_bbrange" / "main.py",
}
OPEN_TIME = datetime(2026, 7, 14, 12, 0, tzinfo=timezone.utc)


class FakeCursor:
    def __init__(self, operations):
        self.operations = operations

    def execute(self, sql, params=None):
        self.operations.append("db:execute")

    def fetchall(self):
        return []

    def close(self):
        self.operations.append("db:cursor_close")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class FakeConnection:
    def __init__(self, operations):
        self.operations = operations

    def cursor(self):
        return FakeCursor(self.operations)

    def commit(self):
        self.operations.append("db:commit")

    def rollback(self):
        self.operations.append("db:rollback")

    def close(self):
        self.operations.append("db:close")


def load_strategy(label):
    module_name = f"waltrade_shared_entry_{label.lower()}"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, STRATEGIES[label])
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def execution_result(case):
    common = {
        "attempted": True,
        "requested_qty": 0.1,
        "client_order_id": "cid-entry",
    }
    if case == "rejected":
        return {
            **common, "ok": True, "live_ok": False, "order_accepted": False,
            "executed": False, "fully_executed": False, "executed_qty": 0.0,
            "order_id": None, "exchange_status": "REJECTED",
            "resp": {"status": "REJECTED", "executedQty": "0"},
        }
    if case in {"new", "accepted"}:
        status = case.upper()
        return {
            **common, "ok": True, "live_ok": False, "order_accepted": True,
            "executed": False, "fully_executed": False, "executed_qty": 0.0,
            "order_id": f"order-{case}", "exchange_status": status,
            "resp": {"orderId": f"order-{case}", "status": status,
                     "executedQty": "0"},
        }
    if case == "partial":
        return {
            **common, "ok": True, "live_ok": True, "order_accepted": True,
            "executed": True, "fully_executed": False, "executed_qty": 0.04,
            "order_id": "order-partial", "exchange_status": "PARTIALLY_FILLED",
            "resp": {"orderId": "order-partial", "status": "PARTIALLY_FILLED",
                     "executedQty": "0.04"},
        }
    if case == "filled":
        return {
            **common, "ok": True, "live_ok": True, "order_accepted": True,
            "executed": True, "fully_executed": True, "executed_qty": 0.1,
            "order_id": "order-filled", "exchange_status": "FILLED",
            "resp": {"orderId": "order-filled", "status": "FILLED",
                     "executedQty": "0.1"},
        }
    if case == "filled_with_requested_qty_fallback":
        return {
            **common, "ok": True, "live_ok": True, "order_accepted": True,
            "executed": True, "fully_executed": True, "executed_qty": 0.1,
            "order_id": "order-filled-no-qty", "exchange_status": "FILLED",
            "resp": {"orderId": "order-filled-no-qty", "status": "FILLED",
                     "executedQty": "0"},
        }
    if case == "exception":
        return {
            **common, "ok": False, "live_ok": False, "order_accepted": False,
            "executed": False, "fully_executed": False, "executed_qty": 0.0,
            "order_id": None, "exchange_status": "EXECUTION_EXCEPTION",
            "reason": "LIVE_ORDER_EXCEPTION", "resp": None,
        }
    if case == "legacy_fill_without_ack":
        return {
            **common, "ok": True, "live_ok": True, "executed": True,
            "fully_executed": False, "executed_qty": 0.06,
            "order_id": "order-legacy", "exchange_status": "PARTIALLY_FILLED",
            "resp": {"orderId": "order-legacy", "status": "PARTIALLY_FILLED",
                     "executedQty": "0.06"},
        }
    raise AssertionError(case)


def run_entry(module, monkeypatch, result, *, ledger_ok=True, position_failure=False):
    operations = []
    events = []
    positions = []

    def reserve(**_kwargs):
        operations.append("ledger:reserve")
        return ledger_ok

    def place(*_args, **_kwargs):
        operations.append("execution:place_live_order")
        return result

    def open_position(**kwargs):
        operations.append("state_change:open_attempt")
        if position_failure:
            raise RuntimeError("position write failed")
        positions.append(kwargs)
        operations.append("state_change:open")
        return 77

    monkeypatch.setattr(module, "insert_simulated_order", reserve)
    monkeypatch.setattr(module, "get_open_position", lambda: None)
    monkeypatch.setattr(module, "get_exchange_client", lambda: object())
    monkeypatch.setattr(module, "place_live_order", place)
    monkeypatch.setattr(module, "open_position_from_live_ack", open_position)
    monkeypatch.setattr(
        module, "emit_strategy_event",
        lambda **payload: (
            events.append(payload),
            operations.append(f"strategy_event:{payload['event_type']}"),
        ),
    )
    monkeypatch.setattr(
        module, "get_db_conn",
        lambda: operations.append("db:get_connection") or FakeConnection(operations),
    )
    if hasattr(module, "preflight_live_order"):
        monkeypatch.setattr(
            module, "preflight_live_order",
            lambda *_a, **_k: operations.append("execution:preflight") or {"ok": True},
        )
    if hasattr(module, "build_live_entry_intent_client_order_id"):
        monkeypatch.setattr(
            module, "build_live_entry_intent_client_order_id",
            lambda *_a, **_k: "cid-entry",
        )
    if hasattr(module, "attach_entry_order_id_with_conn"):
        monkeypatch.setattr(
            module, "attach_entry_order_id_with_conn",
            lambda *_a, **_k: operations.append("ledger:attach_entry_order"),
        )

    cfg = SimpleNamespace(
        symbol="BTCUSDC", interval="1m", trading_mode="LIVE",
        live_orders_enabled=True, quote_asset="USDC",
    )
    kwargs = {
        "side": "BUY", "price": 102.0, "qty_btc": 0.1,
        "reason": "fixture", "candle_open_time": OPEN_TIME,
        "is_exit": False, "cfg_used": cfg, "allow_live_orders": True,
        "allow_meta": {},
    }
    signature = inspect.signature(module.execute_and_record)
    if "rsi_14" in signature.parameters and signature.parameters["rsi_14"].default is inspect.Parameter.empty:
        kwargs["rsi_14"] = 50.0
    if "ema_21" in signature.parameters and signature.parameters["ema_21"].default is inspect.Parameter.empty:
        kwargs["ema_21"] = 101.0
    returned = module.execute_and_record(**kwargs)
    return SimpleNamespace(
        result=returned, positions=positions, events=events, operation_log=operations,
    )


@pytest.mark.parametrize("strategy", STRATEGIES)
@pytest.mark.parametrize(
    ("case", "expected_position_qty", "expected_reason"),
    [
        ("rejected", None, "ORDER_REJECTED"),
        ("new", None, "ORDER_ACCEPTED_PENDING_FILL"),
        ("accepted", None, "ORDER_ACCEPTED_PENDING_FILL"),
        ("partial", 0.04, "OK"),
        ("filled", 0.1, "OK"),
        (
            "filled_with_requested_qty_fallback", None,
            "ORDER_ACCEPTED_PENDING_FILL",
        ),
        ("exception", None, "LIVE_ORDER_FAILED"),
        ("legacy_fill_without_ack", 0.06, "OK"),
    ],
)
def test_strategy_live_entry_matrix(
    monkeypatch, strategy, case, expected_position_qty, expected_reason
):
    monkeypatch.setenv("TRADING_MODE", "LIVE")
    module = load_strategy(strategy)
    observed = run_entry(module, monkeypatch, execution_result(case))
    assert observed.operation_log.count("execution:place_live_order") == 1
    if expected_position_qty is None:
        assert observed.positions == []
        assert "state_change:open" not in observed.operation_log
    else:
        assert observed.positions[0]["qty"] == expected_position_qty
        assert observed.positions[0]["qty"] <= 0.1
        assert observed.operation_log.index("execution:place_live_order") < observed.operation_log.index("state_change:open")

    if case == "exception":
        event = next(e for e in observed.events if e["event_type"] == "BLOCKED")
    else:
        event = next(e for e in observed.events if e["event_type"] == "LIVE_ORDER_SENT")
    if case == "exception" and strategy == "RSI":
        expected_reason = "LIVE_ORDER_EXCEPTION"
    assert event["reason"] == expected_reason

    expected_accepted = case in {
        "new", "accepted", "partial", "filled",
        "filled_with_requested_qty_fallback", "legacy_fill_without_ack",
    }
    expected_executed_qty = expected_position_qty or 0.0
    assert observed.result["order_accepted"] is expected_accepted
    assert observed.result["executed"] is (expected_executed_qty > 0.0)
    assert observed.result["executed_qty"] == expected_executed_qty
    assert observed.result["requested_qty"] == 0.1
    assert observed.result["live_ok"] is (expected_executed_qty > 0.0)

    if case in {"new", "accepted", "filled_with_requested_qty_fallback"}:
        assert observed.result["blocked_reason"] == "ORDER_ACCEPTED_PENDING_FILL"
    if case in {"partial", "legacy_fill_without_ack"}:
        assert observed.result["fully_executed"] is False


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_strategy_ledger_failure_before_fill_sends_no_order(monkeypatch, strategy):
    monkeypatch.setenv("TRADING_MODE", "LIVE")
    module = load_strategy(strategy)
    observed = run_entry(
        module, monkeypatch, execution_result("filled"), ledger_ok=False
    )
    assert observed.result["ledger_ok"] is False
    assert observed.positions == []
    assert "execution:place_live_order" not in observed.operation_log


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_strategy_ledger_failure_after_fill_preserves_execution(monkeypatch, strategy):
    monkeypatch.setenv("TRADING_MODE", "LIVE")
    module = load_strategy(strategy)
    observed = run_entry(
        module, monkeypatch, execution_result("filled"), position_failure=True
    )
    assert observed.result["ledger_ok"] is False
    assert observed.result["executed"] is True
    assert observed.result["executed_qty"] == 0.1
    assert observed.result["blocked_reason"] == "LIVE_ENTRY_FILL_POSITION_WRITE_FAILED"
    assert observed.positions == []
    assert observed.operation_log.count("execution:place_live_order") == 1
    assert "state_change:open_attempt" in observed.operation_log
    assert "state_change:open" not in observed.operation_log
    blocked = next(e for e in observed.events if e["event_type"] == "BLOCKED")
    assert blocked["reason"] == "LIVE_ENTRY_FILL_BUT_POSITION_NOT_OPENED"


def test_shared_normalizer_is_pure_and_legacy_fill_infers_only_required_ack():
    source = inspect.getsource(normalize_entry_execution_outcome)
    assert "get_db_conn" not in source
    assert "place_live_order" not in source
    assert "emit_strategy_event" not in source
    pending = normalize_entry_execution_outcome(
        execution_result("new"), requested_qty=0.1, client_order_id="cid-entry"
    )
    assert pending.stage is ExecutionStage.ACCEPTED_PENDING
    legacy_fill = normalize_entry_execution_outcome(
        execution_result("legacy_fill_without_ack"),
        requested_qty=0.1,
        client_order_id="cid-entry",
    )
    assert legacy_fill.order_accepted is True
    assert legacy_fill.executed_qty == 0.06


def test_pending_entry_recovery_uses_ingested_fills_without_hidden_client_call():
    reconcile = importlib.import_module("common.reconcile_positions")
    ingest = importlib.import_module("common.exchange_ingest_trades")
    entry_reconcile = importlib.import_module(
        "common.entry_fill_reconciliation"
    )
    operations = []

    class StrictClient:
        def __getattr__(self, name):
            raise AssertionError(f"unexpected recovery call: {name}")

    reconcile.reconcile_positions(
        FakeConnection(operations), StrictClient(), min_age_s=0
    )
    assert operations.count("db:execute") == 2
    source = inspect.getsource(reconcile.reconcile_positions)
    assert "FROM positions" in source
    assert "INSERT INTO positions" not in source
    ingest_source = inspect.getsource(ingest.ingest_my_trades)
    assert "run_pending_entry_reconciliation_if_due" in ingest_source
    assert "reconcile_okx_exit_fills" in ingest_source
    assert ingest_source.index("run_pending_entry_reconciliation_if_due") < ingest_source.index(
        "reconcile_okx_exit_fills"
    )
    entry_source = inspect.getsource(entry_reconcile)
    assert "get_my_trades" not in entry_source
    assert "place_live_order" not in entry_source

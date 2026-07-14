from __future__ import annotations

import ast
from pathlib import Path

from common import execution


ROOT = Path(__file__).resolve().parents[1]
RSI = ROOT / "bot/main.py"
EXECUTION = ROOT / "common/execution.py"
MIGRATION = ROOT / "db/migrations/20260714_pending_entry_fill_reconciliation_v1.sql"


def _call_keywords(path: Path, function_name: str):
    tree = ast.parse(path.read_text())
    calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == function_name
    ]
    assert len(calls) == 1
    return {keyword.arg for keyword in calls[0].keywords}


def test_rsi_order_call_preserves_identity_without_another_exchange_request():
    keywords = _call_keywords(RSI, "place_live_order")
    assert {
        "strategy", "interval", "client_order_id", "exchange_source",
    } <= keywords
    assert "db_conn" not in keywords
    assert RSI.read_text().count("resp = place_live_order(") == 1


def test_rsi_live_order_event_contains_reconciliation_metadata():
    source = RSI.read_text()
    live_event = source[source.index('event_type="LIVE_ORDER_SENT"'):]
    live_event = live_event[:live_event.index("if entry_outcome is not None:")]
    assert '"order_accepted"' in live_event
    assert '"requested_qty"' in live_event
    assert '"order_purpose"' in live_event
    assert '"client_order_id"' in live_event


def test_order_writer_and_mirror_preserve_canonical_metadata():
    execution = EXECUTION.read_text()
    migration = MIGRATION.read_text()
    insert = execution[execution.index("INSERT INTO binance_orders"):]
    insert = insert[:insert.index("ON CONFLICT (exchange_source, symbol, order_id) DO NOTHING")]
    for field in (
        "strategy", "interval", "order_purpose", "requested_qty",
        "order_accepted", "exchange_source",
    ):
        assert field in insert
    assert "NEW.strategy" in migration
    assert 'NEW."interval"' in migration
    assert "CASE WHEN v_is_exit THEN 'EXIT' ELSE 'ENTRY' END" in migration
    assert "ON CONFLICT (exchange_source, symbol, order_id) DO UPDATE" in migration


def test_rejection_metadata_cannot_be_mirrored_as_accepted():
    migration = MIGRATION.read_text()
    assert "NULLIF(NEW.info->>'order_accepted', '')::boolean" in migration
    assert '"order_accepted": bool(' in RSI.read_text()


def test_ack_persistence_never_creates_position():
    execution = EXECUTION.read_text()
    order_insert = execution[execution.index("INSERT INTO binance_orders"):]
    order_insert = order_insert[:order_insert.index("return {")]
    assert "INSERT INTO positions" not in order_insert
    migration = MIGRATION.read_text()
    mirror = migration[
        migration.index("CREATE OR REPLACE FUNCTION public.mirror_live_order_sent"):
    ]
    mirror = mirror[:mirror.index("CREATE OR REPLACE VIEW")]
    assert "INSERT INTO public.positions" not in mirror


def test_direct_order_writer_persists_pending_identity(monkeypatch):
    recorded = []

    class Cursor:
        def execute(self, sql, params=None):
            recorded.append((" ".join(str(sql).split()), params))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Connection:
        def cursor(self):
            return Cursor()

    class Client:
        def place_market_order(self, **_kwargs):
            return {
                "orderId": "order-pending", "clientOrderId": "cid-pending",
                "status": "NEW", "executedQty": "0",
            }

    monkeypatch.setattr(
        execution,
        "preflight_live_order",
        lambda *_args, **_kwargs: {"ok": True, "qty_adj": 0.1},
    )
    result = execution.place_live_order(
        Client(), "BTCUSDC", "BUY", 0.1,
        trading_mode="LIVE", live_orders_enabled=True, quote_asset="USDC",
        panic_disable_trading=False, live_max_notional=0,
        client_order_id="cid-pending", db_conn=Connection(), leg="ENTRY",
        strategy="RSI", interval="1m", exchange_source=" OKX ",
    )

    assert result["order_accepted"] is True
    assert result["executed"] is False
    assert len(recorded) == 1
    sql, params = recorded[0]
    assert "INSERT INTO binance_orders" in sql
    assert params[-6:] == ("RSI", "1m", "ENTRY", 0.1, True, "okx")


def test_direct_order_writer_preserves_rejection_as_not_accepted(monkeypatch):
    recorded = []

    class Cursor:
        def execute(self, _sql, params=None):
            recorded.append(params)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Connection:
        def cursor(self):
            return Cursor()

    class Client:
        def place_market_order(self, **_kwargs):
            return {
                "orderId": "order-rejected", "clientOrderId": "cid-rejected",
                "status": "REJECTED", "executedQty": "0",
            }

    monkeypatch.setattr(
        execution,
        "preflight_live_order",
        lambda *_args, **_kwargs: {"ok": True, "qty_adj": 0.1},
    )
    result = execution.place_live_order(
        Client(), "BTCUSDC", "BUY", 0.1,
        trading_mode="LIVE", live_orders_enabled=True, quote_asset="USDC",
        panic_disable_trading=False, live_max_notional=0,
        client_order_id="cid-rejected", db_conn=Connection(), leg="ENTRY",
        strategy="RSI", interval="1m", exchange_source="okx",
    )
    assert result["order_accepted"] is False
    assert recorded[0][-2] is False

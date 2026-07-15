from decimal import Decimal

from common.partial_exit import (
    ExitQuantityStatus,
    apply_confirmed_exit_quantity,
    apply_partial_exit_result,
)
from common.exchange_ingest_trades import RECONCILE_OKX_EXIT_FILLS_SQL


class FakeDB:
    def __init__(self, qty="1"):
        self.position = {"id": 7, "qty": Decimal(qty), "status": "OPEN"}
        self.orders = {}
        self.commits = 0
        self.rollbacks = 0

    def connect(self):
        return FakeConnection(self)


class FakeConnection:
    def __init__(self, db):
        self.db = db

    def cursor(self):
        return FakeCursor(self.db)

    def commit(self):
        self.db.commits += 1

    def rollback(self):
        self.db.rollbacks += 1

    def close(self):
        pass


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self.rowcount = 0
        self.result = None

    def execute(self, sql, params=None):
        compact = " ".join(sql.split()).lower()
        self.rowcount = 0
        self.result = None
        if compact.startswith("select qty from positions"):
            position_id = int(params[0])
            p = self.db.position
            self.result = (p["qty"],) if p["id"] == position_id and p["status"] == "OPEN" else None
        elif compact.startswith("insert into binance_orders"):
            symbol, side, client_id, order_id, position_id, strategy, interval, requested, source = params
            self.db.orders.setdefault((source, symbol, order_id), {
                "id": len(self.db.orders) + 1,
                "position_id": int(position_id), "purpose": "EXIT",
                "reconciled": Decimal("0"), "client_id": client_id,
            })
        elif compact.startswith("select id, position_id, order_purpose"):
            source, symbol, order_id = params
            order = self.db.orders.get((source, symbol, order_id))
            if order:
                self.result = (order["id"], order["position_id"],
                               order["purpose"], order["reconciled"])
        elif compact.startswith("update binance_orders"):
            cumulative, _position_id, _action, order_row_id = params
            order = next(o for o in self.db.orders.values() if o["id"] == order_row_id)
            order["reconciled"] = Decimal(str(cumulative))
            self.rowcount = 1
        elif compact.startswith("update positions"):
            qty, closed = params[0], bool(params[1])
            position_id = int(params[-1])
            if self.db.position["id"] == position_id and self.db.position["status"] == "OPEN":
                self.db.position["qty"] = Decimal(str(qty))
                self.db.position["status"] = "CLOSED" if closed else "OPEN"
                self.rowcount = 1
        else:
            raise AssertionError(compact)

    def fetchone(self):
        return self.result

    def close(self):
        pass


def apply(db, evidences):
    return apply_confirmed_exit_quantity(
        db.connect, position_id=7, exchange_source="okx", symbol="BTCUSDC",
        strategy="TREND", interval="1m", side="SELL", requested_qty=1,
        evidences=evidences, exit_price=101, exit_reason="TAKE_PROFIT",
    )


def evidence(order_id, qty):
    return {"order_id": order_id, "client_order_id": f"cid-{order_id}",
            "cumulative_qty": Decimal(str(qty))}


def test_partial_duplicate_cumulative_distinct_and_full_sequence():
    db = FakeDB()
    first = apply(db, [evidence("a", ".2")])
    assert first.status is ExitQuantityStatus.PARTIALLY_REDUCED
    assert db.position == {"id": 7, "qty": Decimal("0.8"), "status": "OPEN"}

    duplicate = apply(db, [evidence("a", ".2")])
    assert duplicate.status is ExitQuantityStatus.ALREADY_APPLIED
    assert db.position["qty"] == Decimal("0.8")

    cumulative = apply(db, [evidence("a", ".5")])
    assert cumulative.applied_qty == Decimal("0.3")
    assert db.position["qty"] == Decimal("0.5")

    distinct = apply(db, [evidence("b", ".2")])
    assert distinct.applied_qty == Decimal("0.2")
    assert db.position["qty"] == Decimal("0.3")

    full = apply(db, [evidence("c", ".3")])
    assert full.status is ExitQuantityStatus.FULLY_CLOSED
    assert db.position["qty"] == Decimal("0")
    assert db.position["status"] == "CLOSED"


def test_multi_leg_evidence_is_applied_once_and_never_negative():
    db = FakeDB("0.1")
    result = apply(db, [evidence("maker", ".04"), evidence("market", ".08")])
    assert result.status is ExitQuantityStatus.FULLY_CLOSED
    assert result.applied_qty == Decimal("0.1")
    assert result.remaining_qty == Decimal("0")
    assert db.position["qty"] >= 0


def test_strategy_then_reconciliation_same_fill_is_a_noop():
    db = FakeDB()
    strategy = apply(db, [evidence("same", ".2")])
    reconciliation = apply(db, [evidence("same", ".2")])
    assert strategy.applied_qty == Decimal("0.2")
    assert reconciliation.status is ExitQuantityStatus.ALREADY_APPLIED
    assert db.position["qty"] == Decimal("0.8")


def test_reconciliation_then_strategy_retry_same_fill_is_a_noop():
    db = FakeDB()
    reconciliation = apply(db, [evidence("same", ".2")])
    strategy_retry = apply(db, [evidence("same", ".2")])
    assert reconciliation.applied_qty == Decimal("0.2")
    assert strategy_retry.status is ExitQuantityStatus.ALREADY_APPLIED
    assert db.position["qty"] == Decimal("0.8")


def test_reconciler_uses_the_same_order_high_water_delta_contract():
    sql = " ".join(RECONCILE_OKX_EXIT_FILLS_SQL.split())
    assert "executed_qty - COALESCE(bo.reconciled_executed_qty, 0) AS delta_qty" in sql
    assert "FOR UPDATE OF bo" in sql
    assert "SET reconciled_executed_qty = po.executed_qty" in sql
    assert "GREATEST(0, p.qty - c.delta_qty)" in sql


def test_confirmed_partial_without_order_identity_fails_closed():
    db = FakeDB()
    result = {"executed": True, "fully_executed": False, "executed_qty": 0.2}
    try:
        apply_partial_exit_result(
            db.connect, result=result, position_id=7, exchange_source="okx",
            symbol="BTCUSDC", strategy="RSI", interval="1m", side="SELL",
            exit_price=101, exit_reason="STOP_LOSS",
        )
    except RuntimeError as exc:
        assert "durable order identity" in str(exc)
    else:
        raise AssertionError("missing fill identity must not be silently applied")
    assert db.position["qty"] == Decimal("1")

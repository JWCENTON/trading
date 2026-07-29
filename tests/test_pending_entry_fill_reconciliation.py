from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.entry_fill_reconciliation import (
    reconcile_pending_entry_fills as _reconcile_pending_entry_fills,
    run_pending_entry_reconciliation_if_due as _run_pending_entry_reconciliation_if_due,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "db/migrations/20260714_pending_entry_fill_reconciliation_v1.sql"


@pytest.fixture(autouse=True)
def immutable_runtime_revision(monkeypatch):
    monkeypatch.setenv("GIT_SHA", "2" * 40)


def reconcile_pending_entry_fills(conn, **kwargs):
    kwargs.setdefault("trading_mode", "LIVE")
    return _reconcile_pending_entry_fills(conn, **kwargs)


def run_pending_entry_reconciliation_if_due(conn, **kwargs):
    kwargs.setdefault("trading_mode", "LIVE")
    return _run_pending_entry_reconciliation_if_due(conn, **kwargs)


def candidate(
    *,
    strategy="RSI",
    interval="1m",
    side="BUY",
    fill_side="BUY",
    fill_side_count=1,
    fill_count=1,
    qty="0.04",
    avg="100.5",
    fees="0.02",
    order_id="order-1",
    client_order_id="cid-1",
):
    first = datetime(2026, 7, 14, 12, 0, tzinfo=timezone.utc)
    last = datetime(2026, 7, 14, 12, 1, tzinfo=timezone.utc)
    return (
        11, "okx", "BTCUSDC", strategy, interval, side, order_id,
        client_order_id, None, fill_count, Decimal(qty), Decimal(avg),
        Decimal(fees) if fees is not None else None,
        first, last, fill_side_count, fill_side,
    )


class MemoryCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rows = []
        self.one = None
        self.rowcount = 0

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split())
        self.conn.operations.append(normalized)
        self.rows = []
        self.one = None
        self.rowcount = 0

        if "pending-entry:due-gate" in normalized:
            keys = set(params[0])
            self.rows = sorted(
                (key, value) for key, value in self.conn.kv.items() if key in keys
            )
            return
        if "pending-entry:kv-upsert" in normalized:
            self.conn.kv[str(params[0])] = str(params[1])
            self.rowcount = 1
            return

        if "pending-entry:candidates" in normalized:
            limit = int(params[0])
            eligible = []
            for row in self.conn.candidates:
                mark = self.conn.marks.get(row[0])
                linked = next(
                    (
                        p for p in self.conn.positions
                        if mark and mark[1] is not None and p["id"] == mark[1]
                    ),
                    None,
                )
                truth_changed = bool(
                    linked and linked["status"] == "OPEN"
                    and (
                        linked["entry_price"] != row[11]
                        or (
                            row[12] is not None
                            and linked.get("fees_usdc") != row[12]
                        )
                    )
                )
                terminal = bool(
                    mark and mark[0] in {
                        "AMBIGUOUS_ENTRY_FILL",
                        "OPEN_POSITION_ORDER_MISMATCH",
                        "LATE_ENTRY_FILL_AFTER_POSITION_CLOSED",
                    }
                )
                if not terminal and (
                    not mark or row[9] > mark[2] or row[10] != mark[3]
                    or truth_changed
                ):
                    eligible.append(row)
            self.rows = eligible[:limit]
            return
        if normalized.startswith("SAVEPOINT"):
            self.conn.savepoint = (
                deepcopy(self.conn.positions), deepcopy(self.conn.marks)
            )
            return
        if normalized.startswith("ROLLBACK TO SAVEPOINT"):
            self.conn.positions, self.conn.marks = self.conn.savepoint
            return
        if normalized.startswith("RELEASE SAVEPOINT"):
            self.conn.savepoint = None
            return
        if "pending-entry:exact-position" in normalized:
            (
                symbol, strategy, interval, order_id, client_id, _,
                reconciled_position_id, _,
            ) = params
            self.rows = [
                self.conn.position_row(p)
                for p in self.conn.positions
                if p["symbol"] == symbol and p["strategy"] == strategy
                and p["interval"] == interval
                and (
                    p["entry_order_id"] == order_id
                    or (
                        client_id is not None
                        and p["entry_client_order_id"] == client_id
                    )
                    or (
                        reconciled_position_id is not None
                        and p["id"] == reconciled_position_id
                    )
                )
            ]
            return
        if "pending-entry:open-slot" in normalized:
            symbol, strategy, interval = params
            self.rows = [
                self.conn.position_row(p)
                for p in self.conn.positions
                if p["symbol"] == symbol and p["strategy"] == strategy
                and p["interval"] == interval and p["status"] == "OPEN"
            ]
            return
        if "pending-entry:insert-position" in normalized:
            if self.conn.fail_insert:
                self.conn.fail_insert = False
                raise RuntimeError("ledger insert failed")
            if self.conn.race_position is not None:
                self.conn.positions.append(self.conn.race_position)
                self.conn.race_position = None
                self.one = None
                return
            symbol, strategy, interval, position_side, qty, avg = params[:6]
            if any(
                p["status"] == "OPEN" and p["symbol"] == symbol
                and p["strategy"] == strategy and p["interval"] == interval
                for p in self.conn.positions
            ):
                self.one = None
                return
            position = {
                "id": self.conn.next_position_id,
                "symbol": symbol,
                "strategy": strategy,
                "interval": interval,
                "status": "OPEN",
                "side": position_side,
                "qty": Decimal(str(qty)),
                "entry_price": Decimal(str(avg)),
                "fees_usdc": params[11],
                "entry_order_id": params[7],
                "entry_client_order_id": params[8],
            }
            self.conn.next_position_id += 1
            self.conn.positions.append(position)
            self.one = (position["id"],)
            return
        if "pending-entry:update-position" in normalized:
            position_id = int(params[10])
            for position in self.conn.positions:
                if position["id"] == position_id and position["status"] == "OPEN":
                    position["qty"] = Decimal(str(params[0]))
                    position["entry_price"] = Decimal(str(params[1]))
                    if params[5] is not None:
                        position["fees_usdc"] = params[5]
                    self.rowcount = 1
                    return
            return
        if "pending-entry:mark-order" in normalized:
            (
                status, link_position, position_id, clear_position, fill_count, qty,
                unreconciled_qty, error, action, order_row_id,
            ) = params
            previous = self.conn.marks.get(int(order_row_id))
            self.conn.marks[int(order_row_id)] = (
                status,
                (
                    position_id if link_position
                    else None if clear_position
                    else (previous[1] if previous else None)
                ),
                int(fill_count) if fill_count is not None else (previous[2] if previous else 0),
                Decimal(str(qty)) if qty is not None else (previous[3] if previous else Decimal("0")),
                Decimal(str(unreconciled_qty)),
                error,
                action,
            )
            self.rowcount = 1
            return
        raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.one

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class MemoryConnection:
    def __init__(self, candidates=()):
        self.candidates = list(candidates)
        self.positions = []
        self.marks = {}
        self.operations = []
        self.next_position_id = 101
        self.fail_insert = False
        self.race_position = None
        self.savepoint = None
        self.kv = {
            "pending_entry_reconciliation_schema_version": "1",
            "pending_entry_reconciliation_enabled": "1",
            "pending_entry_reconciliation_interval_seconds": "30",
            "pending_entry_reconciliation_last_run": "1970-01-01T00:00:00+00:00",
        }

    def cursor(self):
        return MemoryCursor(self)

    @staticmethod
    def position_row(position):
        return (
            position["id"], position["status"], position["qty"],
            position["entry_price"], position.get("fees_usdc"),
            position["entry_order_id"], position["entry_client_order_id"],
        )


class TrapConnection:
    def cursor(self):
        raise AssertionError("cursor called")

    def commit(self):
        raise AssertionError("commit called")

    def rollback(self):
        raise AssertionError("rollback called")


def existing_position(row, *, status="OPEN", qty=None, order_id=None):
    return {
        "id": 77,
        "symbol": row[2],
        "strategy": row[3],
        "interval": row[4],
        "status": status,
        "side": "LONG" if row[5] == "BUY" else "SHORT",
        "qty": Decimal(str(qty if qty is not None else row[10])),
        "entry_price": row[11],
        "fees_usdc": row[12],
        "entry_order_id": order_id if order_id is not None else row[6],
        "entry_client_order_id": row[7],
    }


def test_pending_ack_without_fill_creates_nothing():
    conn = MemoryConnection()
    stats = reconcile_pending_entry_fills(conn)
    assert stats.scanned == 0
    assert conn.positions == []


def test_paper_reconcile_is_no_query_no_write_no_op():
    stats = reconcile_pending_entry_fills(
        TrapConnection(),
        trading_mode="PAPER",
    )

    assert stats.status == "NOT_APPLICABLE"
    assert stats.ran is False
    assert stats.applicable is False
    assert stats.scanned == 0
    assert stats.processed == 0
    assert stats.created == 0
    assert stats.updated == 0
    assert stats.failed == 0
    assert stats.has_more is False


def test_empty_live_reconcile_is_explicit_success():
    conn = MemoryConnection()

    stats = reconcile_pending_entry_fills(conn, trading_mode="LIVE")

    assert stats.status == "OK"
    assert stats.ran is True
    assert stats.applicable is True
    assert stats.scanned == 0
    assert stats.processed == 0
    assert stats.has_more is False


def test_paper_due_runner_is_not_applicable_before_due_gate():
    result = run_pending_entry_reconciliation_if_due(
        TrapConnection(),
        force=True,
        trading_mode="PAPER",
    )

    assert result.ran is False
    assert result.status == "NOT_APPLICABLE"
    assert result.applicable is False
    assert result.stats.status == "NOT_APPLICABLE"
    assert result.stats.ran is False
    assert result.stats.applicable is False
    assert result.stats.scanned == 0
    assert result.stats.processed == 0
    assert result.stats.created == 0
    assert result.stats.updated == 0
    assert result.stats.failed == 0
    assert result.stats.has_more is False


def test_automation_passes_environment_to_lightweight_reconciliation_gate():
    source = (ROOT / "automation_runner/main.py").read_text()
    call = source[source.index("run_pending_entry_reconciliation_if_due(") :]
    call = call[:call.index(")") + 1]
    assert "trading_mode=cfg.trading_mode" in call


@pytest.mark.parametrize("strategy", ["RSI", "TREND", "SUPERTREND", "BBRANGE"])
def test_late_fill_creates_one_position_for_each_strategy(strategy):
    row = candidate(strategy=strategy)
    conn = MemoryConnection([row])
    stats = reconcile_pending_entry_fills(conn)
    assert stats.created == 1
    assert len(conn.positions) == 1
    assert conn.positions[0]["strategy"] == strategy
    assert conn.positions[0]["qty"] == Decimal("0.04")
    assert conn.positions[0]["entry_price"] == Decimal("100.5")
    assert conn.positions[0]["fees_usdc"] == Decimal("0.02")
    assert conn.marks[11][0] == "ENTRY_FILL_POSITION_CREATED"


def test_requested_qty_is_never_used_for_partial_fill():
    row = candidate(qty="0.03")
    conn = MemoryConnection([row])
    reconcile_pending_entry_fills(conn)
    assert conn.positions[0]["qty"] == Decimal("0.03")
    assert conn.positions[0]["qty"] != Decimal("0.1")


def test_second_partial_updates_aggregate_without_duplicate():
    first = candidate(fill_count=1, qty="0.04", avg="100", fees="0.01")
    conn = MemoryConnection([first])
    assert reconcile_pending_entry_fills(conn).created == 1
    second = candidate(fill_count=2, qty="0.07", avg="101", fees="0.02")
    conn.candidates = [second]
    stats = reconcile_pending_entry_fills(conn)
    assert stats.updated == 1
    assert len(conn.positions) == 1
    assert conn.positions[0]["qty"] == Decimal("0.07")
    assert conn.positions[0]["entry_price"] == Decimal("101")
    assert conn.positions[0]["fees_usdc"] == Decimal("0.02")

    completed = candidate(
        fill_count=3, qty="0.1", avg="101.5", fees="0.03"
    )
    conn.candidates = [completed]
    completed_stats = reconcile_pending_entry_fills(conn)
    assert completed_stats.updated == 1
    assert len(conn.positions) == 1
    assert conn.positions[0]["qty"] == Decimal("0.1")
    assert conn.positions[0]["entry_price"] == Decimal("101.5")


def test_repeat_run_is_idempotent():
    row = candidate()
    conn = MemoryConnection([row])
    assert reconcile_pending_entry_fills(conn).created == 1
    second = reconcile_pending_entry_fills(conn)
    assert second.scanned == 0
    assert len(conn.positions) == 1


def test_existing_exact_position_is_not_duplicated():
    row = candidate()
    conn = MemoryConnection([row])
    conn.positions.append(existing_position(row))
    stats = reconcile_pending_entry_fills(conn)
    assert stats.already_reconciled == 1
    assert len(conn.positions) == 1


def test_existing_same_qty_position_updates_fill_price_and_fees():
    row = candidate(qty="0.04", avg="101.25", fees="0.03")
    conn = MemoryConnection([row])
    position = existing_position(row)
    position["entry_price"] = Decimal("100")
    position["fees_usdc"] = None
    conn.positions.append(position)
    stats = reconcile_pending_entry_fills(conn)
    assert stats.updated == 1
    assert len(conn.positions) == 1
    assert conn.positions[0]["entry_price"] == Decimal("101.25")
    assert conn.positions[0]["fees_usdc"] == Decimal("0.03")


def test_late_fee_pricing_revisits_unchanged_fill_aggregate():
    initial = candidate(qty="0.04", avg="101", fees=None)
    conn = MemoryConnection([initial])
    assert reconcile_pending_entry_fills(conn).created == 1
    assert conn.positions[0]["fees_usdc"] is None

    priced = candidate(qty="0.04", avg="101", fees="0.025")
    conn.candidates = [priced]
    stats = reconcile_pending_entry_fills(conn)
    assert stats.updated == 1
    assert conn.positions[0]["fees_usdc"] == Decimal("0.025")


def test_unrelated_open_slot_is_ambiguous_and_not_overwritten():
    row = candidate()
    conn = MemoryConnection([row])
    unrelated = existing_position(row, order_id="other-order")
    unrelated["entry_client_order_id"] = "other-client"
    conn.positions.append(unrelated)
    stats = reconcile_pending_entry_fills(conn)
    assert stats.ambiguous == 1
    assert len(conn.positions) == 1
    assert conn.positions[0]["entry_order_id"] == "other-order"
    rerun = reconcile_pending_entry_fills(conn)
    assert rerun.scanned == 0
    assert rerun.has_more is False


@pytest.mark.parametrize(
    ("side", "fill_side", "fill_side_count"),
    [("BUY", "SELL", 1), ("BUY", "BUY", 2), ("HOLD", "HOLD", 1)],
)
def test_side_mismatch_or_multiple_fill_sides_is_ambiguous(
    side, fill_side, fill_side_count
):
    conn = MemoryConnection([
        candidate(side=side, fill_side=fill_side, fill_side_count=fill_side_count)
    ])
    stats = reconcile_pending_entry_fills(conn)
    assert stats.ambiguous == 1
    assert conn.positions == []


def test_closed_position_equal_aggregate_is_historically_reconciled():
    row = candidate(fill_count=1, qty="0.04")
    conn = MemoryConnection([row])
    conn.positions.append(existing_position(row, status="CLOSED", qty="0.04"))
    stats = reconcile_pending_entry_fills(conn)
    assert stats.already_reconciled == 1
    assert len(conn.positions) == 1
    assert conn.positions[0]["status"] == "CLOSED"
    assert conn.positions[0]["qty"] == Decimal("0.04")


def test_closed_position_late_fill_is_alarm_with_unreconciled_qty_on_rerun():
    row = list(candidate(fill_count=2, qty="0.07"))
    row[8] = 77
    row = tuple(row)
    conn = MemoryConnection([row])
    conn.positions.append(existing_position(row, status="CLOSED", qty="0.04"))
    conn.marks[11] = (
        "ENTRY_FILL_POSITION_CREATED", 77, 1, Decimal("0.04"),
        Decimal("0"), None, "ENTRY_FILL_POSITION_CREATED",
    )

    first = reconcile_pending_entry_fills(conn)
    assert first.alarms == 1
    assert conn.positions[0]["qty"] == Decimal("0.04")
    assert conn.marks[11][0] == "LATE_ENTRY_FILL_AFTER_POSITION_CLOSED"
    assert conn.marks[11][1] is None
    assert conn.marks[11][3] == Decimal("0.04")
    assert conn.marks[11][4] == Decimal("0.03")

    second = reconcile_pending_entry_fills(conn)
    assert second.scanned == 0
    assert second.alarms == 0
    assert conn.marks[11][0] == "LATE_ENTRY_FILL_AFTER_POSITION_CLOSED"
    assert conn.marks[11][4] == Decimal("0.03")


def test_ledger_insert_failure_preserves_retry_eligibility():
    row = candidate()
    conn = MemoryConnection([row])
    conn.fail_insert = True
    failed = reconcile_pending_entry_fills(conn)
    assert failed.failed == 1
    assert conn.positions == []
    assert conn.marks[11][0] == "ENTRY_FILL_RECONCILIATION_ERROR"
    assert conn.marks[11][3] == Decimal("0")
    retried = reconcile_pending_entry_fills(conn)
    assert retried.created == 1


def test_concurrent_insert_winner_is_linked_without_duplicate():
    row = candidate()
    conn = MemoryConnection([row])
    conn.race_position = existing_position(row)
    stats = reconcile_pending_entry_fills(conn)
    assert stats.already_reconciled == 1
    assert len(conn.positions) == 1


def test_concurrent_partial_winner_is_updated_to_current_aggregate():
    row = candidate(fill_count=2, qty="0.07", avg="101", fees="0.02")
    conn = MemoryConnection([row])
    conn.race_position = existing_position(
        candidate(qty="0.04", avg="100", fees="0.01"), qty="0.04"
    )
    stats = reconcile_pending_entry_fills(conn)
    assert stats.updated == 1
    assert conn.positions[0]["qty"] == Decimal("0.07")
    assert conn.positions[0]["entry_price"] == Decimal("101")


def test_concurrent_winner_for_different_order_is_ambiguous_without_link():
    row = candidate(order_id="order-b", client_order_id="cid-b")
    conn = MemoryConnection([row])
    winner = existing_position(row, order_id="order-a")
    winner["entry_client_order_id"] = "cid-a"
    conn.race_position = winner
    stats = reconcile_pending_entry_fills(conn)
    assert stats.ambiguous == 1
    assert conn.positions[0]["qty"] == Decimal("0.04")
    assert conn.marks[11][0] == "OPEN_POSITION_ORDER_MISMATCH"
    assert conn.marks[11][1] is None


def test_second_pending_order_never_links_or_updates_first_order_position():
    row_b = candidate(order_id="order-b", client_order_id="cid-b")
    conn = MemoryConnection([row_b])
    position_a = existing_position(row_b, order_id="order-a")
    position_a["entry_client_order_id"] = "cid-a"
    conn.positions.append(position_a)
    stats = reconcile_pending_entry_fills(conn)
    assert stats.ambiguous == 1
    assert conn.positions[0]["entry_order_id"] == "order-a"
    assert conn.positions[0]["qty"] == Decimal("0.04")
    assert conn.marks[11][0] == "OPEN_POSITION_ORDER_MISMATCH"
    assert conn.marks[11][1] is None


def test_sticky_foreign_reconciled_position_id_is_cleared_on_identity_conflict():
    row = list(candidate(order_id="order-b", client_order_id="cid-b"))
    row[8] = 77
    conn = MemoryConnection([tuple(row)])
    foreign = existing_position(tuple(row), order_id="order-a")
    foreign["entry_client_order_id"] = "cid-a"
    conn.positions.append(foreign)
    conn.marks[11] = (
        "OLD_AMBIGUOUS", 77, 0, Decimal("0"), Decimal("0"), None, None
    )
    stats = reconcile_pending_entry_fills(conn)
    assert stats.ambiguous == 1
    assert conn.marks[11][1] is None
    assert conn.positions[0]["entry_order_id"] == "order-a"


def test_batch_is_bounded_and_candidate_rows_are_locked():
    rows = [candidate(order_id=f"order-{i}") for i in range(3)]
    rows = [tuple([i + 1, *row[1:]]) for i, row in enumerate(rows)]
    conn = MemoryConnection(rows)
    stats = reconcile_pending_entry_fills(conn, batch_size=2)
    assert stats.scanned == 2
    candidate_sql = conn.operations[0]
    assert "FOR UPDATE OF bo SKIP LOCKED" in candidate_sql
    assert "LIMIT %s" in candidate_sql


def test_due_runner_drains_150_rows_in_two_bounded_runs_without_new_fills(monkeypatch):
    monkeypatch.setattr(
        "common.schema_readiness.validate_pending_entry_reconciliation_schema",
        lambda _conn, **_kwargs: None,
    )
    rows = []
    for i in range(150):
        row = list(candidate(interval=f"slot-{i}", order_id=f"order-{i}"))
        row[0] = i + 1
        rows.append(tuple(row))
    conn = MemoryConnection(rows)

    first = run_pending_entry_reconciliation_if_due(conn, force=True)
    assert first.ran is True
    assert first.stats.scanned == 100
    assert first.stats.created == 100
    assert first.stats.has_more is True

    second = run_pending_entry_reconciliation_if_due(conn, force=True)
    assert second.stats.scanned == 50
    assert second.stats.created == 50
    assert second.stats.has_more is False
    assert len(conn.positions) == 150


def test_due_runner_retries_failed_order_on_next_cycle(monkeypatch):
    monkeypatch.setattr(
        "common.schema_readiness.validate_pending_entry_reconciliation_schema",
        lambda _conn, **_kwargs: None,
    )
    conn = MemoryConnection([candidate()])
    conn.fail_insert = True
    failed = run_pending_entry_reconciliation_if_due(conn, force=True)
    assert failed.stats.failed == 1
    assert failed.stats.has_more is True
    assert conn.marks[11][0] == "ENTRY_FILL_RECONCILIATION_ERROR"
    assert conn.marks[11][1] is None

    retried = run_pending_entry_reconciliation_if_due(conn, force=True)
    assert retried.stats.created == 1
    assert conn.marks[11][0] == "ENTRY_FILL_POSITION_CREATED"


def test_due_runner_fails_closed_when_schema_contract_is_missing(monkeypatch):
    monkeypatch.setattr(
        "common.schema_readiness.validate_pending_entry_reconciliation_schema",
        lambda _conn, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("missing trigger")
        ),
    )
    conn = MemoryConnection([candidate()])
    result = run_pending_entry_reconciliation_if_due(conn, force=True)
    assert result.ran is False
    assert result.status == "SCHEMA_NOT_READY"
    assert conn.positions == []
    assert conn.kv["pending_entry_reconciliation_last_status"] == "SCHEMA_NOT_READY"


def test_migration_has_explicit_identity_purpose_and_audit_statuses():
    sql = MIGRATION.read_text()
    for field in (
        "strategy", '"interval"', "order_purpose", "requested_qty",
        "order_accepted", "reconciliation_status", "reconciled_position_id",
        "reconciled_fill_count", "reconciled_executed_qty", "exchange_source",
    ):
        assert field in sql
    for status in (
        "MATCHED_ENTRY_FILL", "PENDING_ENTRY_ACK",
        "ENTRY_FILL_POSITION_CREATED", "ENTRY_FILL_POSITION_UPDATED",
        "ENTRY_FILL_ALREADY_RECONCILED", "ORPHAN_ENTRY_FILL",
        "AMBIGUOUS_ENTRY_FILL", "EXIT_FILL", "MANUAL_OR_EXTERNAL_FILL",
        "CANARY_IGNORE", "EXIT_ACK_PENDING",
        "LATE_ENTRY_FILL_AFTER_POSITION_CLOSED",
        "ENTRY_FILL_RECONCILIATION_ERROR", "OPEN_POSITION_ORDER_MISMATCH",
    ):
        assert status in sql
    assert "CREATE OR REPLACE VIEW public.v_pending_entry_fill_reconciliation_audit" in sql
    assert "ON CONFLICT (exchange_source, symbol, order_id) DO UPDATE" in sql
    assert "ux_binance_orders_source_symbol_order_id" in sql
    assert "ux_binance_orders_legacy_null_source_symbol_order_id" in sql


def test_audit_case_is_alarm_first_and_exit_ack_is_not_a_fill():
    sql = " ".join(MIGRATION.read_text().split())
    case = sql[sql.index("CASE WHEN COALESCE(ft.is_canary") :]
    case = case[:case.index("END AS audit_status")]
    ordered = [
        "CANARY_IGNORE",
        "EXIT_ACK_PENDING",
        "EXIT_FILL",
        "AMBIGUOUS_ENTRY_FILL",
        "PENDING_ENTRY_ACK",
        "LATE_ENTRY_FILL_AFTER_POSITION_CLOSED",
        "ENTRY_FILL_RECONCILIATION_ERROR",
        "OPEN_POSITION_ORDER_MISMATCH",
        "ENTRY_FILL_ALREADY_RECONCILED",
        "ENTRY_FILL_POSITION_CREATED",
        "MATCHED_ENTRY_FILL",
        "ORPHAN_ENTRY_FILL",
    ]
    offsets = [case.index(status) for status in ordered]
    assert offsets == sorted(offsets)
    assert "bo.order_purpose = 'EXIT' AND ft.order_id IS NULL" in case
    assert "WHEN rp.id IS NOT NULL OR ep.id IS NOT NULL THEN 'MATCHED_ENTRY_FILL'" in case
    assert "ft.executed_qty - COALESCE(bo.reconciled_executed_qty, 0)" in sql


def test_exit_fills_are_excluded_from_entry_reconciliation():
    source = (ROOT / "common/entry_fill_reconciliation.py").read_text()
    assert "bo.order_purpose = 'ENTRY'" in source
    sql = MIGRATION.read_text()
    assert "WHEN bo.order_purpose = 'EXIT' THEN 'EXIT_FILL'" in sql


def test_orphan_manual_and_canary_fills_remain_audit_only():
    sql = MIGRATION.read_text()
    assert "WHERE NOT EXISTS" in sql
    assert "WHEN ft.is_canary THEN 'CANARY_IGNORE'" in sql
    assert (
        "WHEN COALESCE(ft.fill_client_order_id, '') = '' "
        "THEN 'MANUAL_OR_EXTERNAL_FILL'"
    ) in " ".join(sql.split())
    assert "ELSE 'ORPHAN_ENTRY_FILL'" in sql


def test_reconciliation_contains_no_exchange_or_order_calls():
    source = (ROOT / "common/entry_fill_reconciliation.py").read_text()
    assert "get_my_trades" not in source
    assert "place_live_order" not in source
    assert "get_order(" not in source
    assert "create_order" not in source
    assert "runtime DDL" not in source

"""Real PostgreSQL rollout-gate tests for pending entry fill reconciliation.

These tests intentionally use the production reconciliation implementation.
They are skipped unless WALTRADE_TEST_PG_DSN is explicit, and every connection
is guarded by both an ``_test`` database name and an operator-created marker.
"""

from __future__ import annotations

import os
import threading
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest


TEST_DSN = os.getenv("WALTRADE_TEST_PG_DSN", "").strip()
pytestmark = pytest.mark.skipif(
    not TEST_DSN,
    reason="WALTRADE_TEST_PG_DSN is not set; real disposable PostgreSQL required",
)

import psycopg2  # noqa: E402
from psycopg2 import sql  # noqa: E402
from psycopg2.extensions import parse_dsn  # noqa: E402

from common.entry_fill_reconciliation import (  # noqa: E402
    _CANDIDATES_SQL,
    reconcile_pending_entry_fills,
)


def _dsn_database_name() -> str:
    try:
        database = str(parse_dsn(TEST_DSN).get("dbname") or "")
    except Exception as exc:
        raise RuntimeError("invalid WALTRADE_TEST_PG_DSN") from exc
    if not database.lower().endswith("_test"):
        raise RuntimeError(
            "refusing PostgreSQL integration test: DSN database must end in _test"
        )
    return database


def _guarded_connect():
    expected_database = _dsn_database_name()  # Guard before opening a session.
    conn = psycopg2.connect(TEST_DSN, connect_timeout=5)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            actual_database = str(cur.fetchone()[0])
            if actual_database != expected_database or not actual_database.lower().endswith(
                "_test"
            ):
                raise RuntimeError(
                    "refusing PostgreSQL integration test: connected database is not *_test"
                )
            cur.execute(
                "SELECT value FROM automation_kv "
                "WHERE key='waltrade_disposable_test_db'"
            )
            row = cur.fetchone()
            if row is None or str(row[0]).strip().lower() != "true":
                raise RuntimeError(
                    "refusing PostgreSQL integration test: "
                    "waltrade_disposable_test_db=true is required"
                )
            cur.execute(
                "SELECT value FROM automation_kv "
                "WHERE key='pending_entry_reconciliation_schema_version'"
            )
            schema_row = cur.fetchone()
            if schema_row is None or str(schema_row[0]) != "1":
                raise RuntimeError(
                    "pending-entry migration must be validated before integration tests"
                )
            cur.execute("SET lock_timeout = '2s'")
            cur.execute("SET statement_timeout = '10s'")
            cur.execute("SET idle_in_transaction_session_timeout = '15s'")
        return conn
    except Exception:
        conn.close()
        raise


def _identity(label: str) -> tuple[str, str]:
    token = uuid.uuid4().hex[:10]
    prefix = f"WTPG{label.upper()}{token}"
    return prefix, f"waltrade_test_{label.lower()}_{token}"


def _seed_entries(conn, *, prefix: str, source: str, count: int, same_slot=False):
    first_time = datetime.now(timezone.utc) - timedelta(hours=1)
    trade_base = uuid.uuid4().int % 8_000_000_000_000_000
    identities = []
    with conn.cursor() as cur:
        for index in range(count):
            symbol = f"{prefix}{0 if same_slot else index}USDC"
            order_id = f"{prefix}-order-{index}"
            client_id = f"{prefix}-client-{index}"
            event_time = first_time + timedelta(microseconds=index)
            cur.execute(
                """
                INSERT INTO binance_orders(
                  created_at, symbol, side, order_type, client_order_id,
                  order_id, status, raw, is_exit, strategy, "interval",
                  order_purpose, requested_qty, order_accepted, exchange_source
                ) VALUES (
                  %s,%s,'BUY','MARKET',%s,%s,'FILLED','{}'::jsonb,false,
                  'RSI','1m','ENTRY',0.10,true,%s
                )
                """,
                (event_time, symbol, client_id, order_id, source),
            )
            cur.execute(
                """
                INSERT INTO binance_order_fills(
                  source, trade_id, order_id, symbol, side, role,
                  executed_qty, avg_price, quote_notional_usdc,
                  commission_amount, commission_asset, commission_usdc,
                  event_time, fill_idx, raw
                ) VALUES (
                  %s,%s,%s,%s,'BUY','TAKER',0.10,100,10,
                  0.01,'USDC',0.01,%s,0,
                  jsonb_build_object('raw',jsonb_build_object('clOrdId',%s))
                )
                """,
                (
                    source,
                    trade_base + index,
                    order_id,
                    symbol,
                    event_time,
                    client_id,
                ),
            )
            identities.append((symbol, order_id, client_id))
    return identities


def _cleanup(*, prefix: str, source: str):
    conn = _guarded_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE binance_orders SET reconciled_position_id=NULL "
                "WHERE exchange_source=%s",
                (source,),
            )
            cur.execute("DELETE FROM binance_order_fills WHERE source=%s", (source,))
            cur.execute("DELETE FROM binance_orders WHERE exchange_source=%s", (source,))
            cur.execute("DELETE FROM positions WHERE symbol LIKE %s", (prefix + "%",))
            cur.execute("DELETE FROM strategy_events WHERE symbol LIKE %s", (prefix + "%",))
        conn.commit()
    finally:
        conn.close()


def _plan_index_names(value):
    names = set()
    if isinstance(value, dict):
        index_name = value.get("Index Name")
        if index_name:
            names.add(str(index_name))
        for child in value.values():
            names.update(_plan_index_names(child))
    elif isinstance(value, list):
        for child in value:
            names.update(_plan_index_names(child))
    return names


def _explain_production_candidate_query(conn):
    with conn.cursor() as cur:
        cur.execute("ANALYZE binance_orders")
        cur.execute("ANALYZE binance_order_fills")
        cur.execute("ANALYZE positions")
        cur.execute("SET LOCAL enable_seqscan = off")
        cur.execute("EXPLAIN (FORMAT JSON) " + _CANDIDATES_SQL, (100,))
        plan = cur.fetchone()[0]
    index_names = _plan_index_names(plan)
    assert "ix_binance_orders_pending_entry_reconcile" in index_names, index_names
    return plan


def _run_two_reconciliation_workers(*, batch_size: int):
    barrier = threading.Barrier(2, timeout=5)
    states = [{}, {}]

    def target(state):
        conn = None
        committed = False
        try:
            conn = _guarded_connect()
            state["connection"] = conn
            barrier.wait()
            state["stats"] = reconcile_pending_entry_fills(
                conn, batch_size=batch_size
            )
            conn.commit()
            committed = True
        except BaseException as exc:  # propagate worker failures to the test
            state["error"] = exc
            if conn is not None and not conn.closed:
                conn.rollback()
        finally:
            if conn is not None and not conn.closed:
                if not committed:
                    conn.rollback()
                conn.close()
            state["connection"] = None

    threads = [
        threading.Thread(
            target=target,
            args=(state,),
            name=f"pending-entry-pg-worker-{index}",
        )
        for index, state in enumerate(states)
    ]
    for thread in threads:
        thread.start()
    return threads, states


def _finish_reconciliation_workers(threads, states):
    for thread in threads:
        thread.join(timeout=12)
    alive = [thread for thread in threads if thread.is_alive()]
    if alive:
        for state in states:
            conn = state.get("connection")
            if conn is not None and not conn.closed:
                conn.cancel()
        for thread in alive:
            thread.join(timeout=5)
    if any(thread.is_alive() for thread in threads):
        raise AssertionError("PostgreSQL reconciliation worker remained blocked")
    for state in states:
        if "error" in state:
            raise state["error"]
    return [state["stats"] for state in states]


def test_real_drain_100_then_50_then_zero_without_new_fills():
    prefix, source = _identity("drain")
    conn = _guarded_connect()
    try:
        _seed_entries(conn, prefix=prefix, source=source, count=150)

        # EXPLAIN imports the exact production constant; it cannot drift into a
        # hand-maintained approximation and must use the dedicated pending index.
        _explain_production_candidate_query(conn)

        runs = []
        first = reconcile_pending_entry_fills(conn, batch_size=100)
        runs.append(first)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT order_id,reconciliation_status FROM binance_orders "
                "WHERE exchange_source=%s ORDER BY created_at,id",
                (source,),
            )
            after_first = cur.fetchall()
        assert [row[0] for row in after_first[:100]] == [
            f"{prefix}-order-{index}" for index in range(100)
        ]
        assert all(row[1] == "ENTRY_FILL_POSITION_CREATED" for row in after_first[:100])
        assert all(row[1] is None for row in after_first[100:])

        second = reconcile_pending_entry_fills(conn, batch_size=100)
        runs.append(second)
        third = reconcile_pending_entry_fills(conn, batch_size=100)
        runs.append(third)

        assert len(runs) == 3
        assert (first.scanned, first.created, first.has_more) == (100, 100, True)
        assert (second.scanned, second.created, second.has_more) == (50, 50, False)
        assert (third.scanned, third.created, third.has_more) == (0, 0, False)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM positions WHERE symbol LIKE %s", (prefix + "%",))
            assert cur.fetchone()[0] == 150
            cur.execute(_CANDIDATES_SQL, (100,))
            assert cur.fetchall() == []
    finally:
        conn.rollback()
        conn.close()


def test_real_skip_locked_processes_other_row_then_locked_row():
    prefix, source = _identity("skip")
    setup = _guarded_connect()
    identities = _seed_entries(setup, prefix=prefix, source=source, count=2)
    setup.commit()
    setup.close()

    locker = worker = None
    try:
        locker = _guarded_connect()
        with locker.cursor() as cur:
            cur.execute(
                "SELECT id FROM binance_orders "
                "WHERE exchange_source=%s AND order_id=%s FOR UPDATE",
                (source, identities[0][1]),
            )
            assert cur.fetchone() is not None

        worker = _guarded_connect()
        first = reconcile_pending_entry_fills(worker, batch_size=100)
        worker.commit()
        worker.close()
        worker = None
        assert (first.scanned, first.created) == (1, 1)

        locker.commit()
        locker.close()
        locker = None

        worker = _guarded_connect()
        second = reconcile_pending_entry_fills(worker, batch_size=100)
        worker.commit()
        assert (second.scanned, second.created) == (1, 1)
        with worker.cursor() as cur:
            cur.execute("SELECT count(*) FROM positions WHERE symbol LIKE %s", (prefix + "%",))
            assert cur.fetchone()[0] == 2
            cur.execute(
                "SELECT count(DISTINCT reconciled_position_id), count(*) "
                "FROM binance_orders WHERE exchange_source=%s",
                (source,),
            )
            assert cur.fetchone() == (2, 2)
    finally:
        for conn in (locker, worker):
            if conn is not None:
                conn.rollback()
                conn.close()
        _cleanup(prefix=prefix, source=source)


def test_real_savepoint_failure_preserves_other_order_and_retries():
    prefix, source = _identity("savepoint")
    conn = _guarded_connect()
    identities = _seed_entries(conn, prefix=prefix, source=source, count=2)
    constraint_name = f"ck_{prefix.lower()}_controlled_failure"
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("ALTER TABLE positions ADD CONSTRAINT {} CHECK (symbol <> %s) NOT VALID").format(
                    sql.Identifier(constraint_name)
                ),
                (identities[0][0],),
            )

        failed_run = reconcile_pending_entry_fills(conn, batch_size=100)
        assert (failed_run.failed, failed_run.created, failed_run.has_more) == (1, 1, True)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT reconciliation_status FROM binance_orders "
                "WHERE exchange_source=%s AND order_id=%s",
                (source, identities[0][1]),
            )
            assert cur.fetchone()[0] == "ENTRY_FILL_RECONCILIATION_ERROR"
            cur.execute("SELECT count(*) FROM positions WHERE symbol LIKE %s", (prefix + "%",))
            assert cur.fetchone()[0] == 1

        observer = _guarded_connect()
        try:
            with observer.cursor() as cur:
                cur.execute(
                    "SELECT id FROM binance_orders WHERE exchange_source=%s "
                    "FOR UPDATE SKIP LOCKED",
                    (source,),
                )
                assert cur.fetchall() == []
        finally:
            observer.rollback()
            observer.close()

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("ALTER TABLE positions DROP CONSTRAINT {}").format(
                    sql.Identifier(constraint_name)
                )
            )
        conn.commit()
        conn.close()
        conn = None

        retry = _guarded_connect()
        try:
            retried = reconcile_pending_entry_fills(retry, batch_size=100)
            retry.commit()
            assert (retried.scanned, retried.created, retried.failed) == (1, 1, 0)
            with retry.cursor() as cur:
                cur.execute(
                    "SELECT reconciliation_status, reconciled_position_id, "
                    "reconciliation_error, unreconciled_qty, "
                    "last_reconciliation_action "
                    "FROM binance_orders WHERE exchange_source=%s AND order_id=%s",
                    (source, identities[0][1]),
                )
                status, position_id, error, unreconciled_qty, action = cur.fetchone()
                assert status == "ENTRY_FILL_POSITION_CREATED"
                assert position_id is not None
                assert error is None
                assert unreconciled_qty == Decimal("0")
                assert action == "ENTRY_FILL_POSITION_CREATED"
                cur.execute("SELECT count(*) FROM positions WHERE symbol LIKE %s", (prefix + "%",))
                assert cur.fetchone()[0] == 2
        finally:
            retry.close()
    finally:
        if conn is not None:
            conn.rollback()
            conn.close()
        _cleanup(prefix=prefix, source=source)


def test_two_transaction_exact_order_insert_conflict_rereads_winner():
    prefix, source = _identity("exactrace")
    setup = _guarded_connect()
    identity = _seed_entries(setup, prefix=prefix, source=source, count=1)[0]
    setup.commit()
    setup.close()

    try:
        threads, states = _run_two_reconciliation_workers(batch_size=1)
        stats = _finish_reconciliation_workers(threads, states)
        assert len(stats) == 2
        assert sum(item.scanned for item in stats) == 1
        assert sum(item.created for item in stats) == 1
        assert sum(item.failed for item in stats) == 0

        drain = _guarded_connect()
        try:
            final_run = reconcile_pending_entry_fills(drain, batch_size=100)
            drain.commit()
            assert final_run.scanned == 0
        finally:
            drain.close()

        verify = _guarded_connect()
        try:
            with verify.cursor() as cur:
                cur.execute(
                    "SELECT id,qty,entry_price,fees_usdc FROM positions "
                    "WHERE symbol=%s AND strategy='RSI' AND \"interval\"='1m'",
                    (identity[0],),
                )
                rows = cur.fetchall()
                assert len(rows) == 1
                position_id, qty, price, fees = rows[0]
                assert qty == Decimal("0.10")
                assert price == Decimal("100")
                assert fees == Decimal("0.01000000")
                cur.execute(
                    "SELECT reconciliation_status,reconciled_position_id "
                    "FROM binance_orders WHERE exchange_source=%s AND order_id=%s",
                    (source, identity[1]),
                )
                assert cur.fetchone() == (
                    "ENTRY_FILL_POSITION_CREATED",
                    position_id,
                )
        finally:
            verify.rollback()
            verify.close()
    finally:
        _cleanup(prefix=prefix, source=source)


def test_two_transaction_foreign_order_conflict_becomes_mismatch_without_link():
    prefix, source = _identity("foreignrace")
    setup = _guarded_connect()
    identities = _seed_entries(
        setup, prefix=prefix, source=source, count=2, same_slot=True
    )
    setup.commit()
    setup.close()

    try:
        threads, states = _run_two_reconciliation_workers(batch_size=1)
        stats = _finish_reconciliation_workers(threads, states)
        assert len(stats) == 2
        assert sum(item.scanned for item in stats) == 2
        assert sum(item.created for item in stats) == 1
        assert sum(item.ambiguous for item in stats) == 1
        assert sum(item.failed for item in stats) == 0

        drain = _guarded_connect()
        try:
            final_run = reconcile_pending_entry_fills(drain, batch_size=100)
            drain.commit()
            assert final_run.scanned == 0
        finally:
            drain.close()

        verify = _guarded_connect()
        try:
            with verify.cursor() as cur:
                cur.execute("SELECT id,entry_order_id FROM positions WHERE symbol=%s", (identities[0][0],))
                positions = cur.fetchall()
                assert len(positions) == 1
                winner_position_id, winner_order_id = positions[0]
                cur.execute(
                    "SELECT order_id,reconciliation_status,reconciled_position_id "
                    "FROM binance_orders WHERE exchange_source=%s ORDER BY order_id",
                    (source,),
                )
                rows = {row[0]: row[1:] for row in cur.fetchall()}
                loser_order_id = next(
                    item[1] for item in identities if item[1] != winner_order_id
                )
                assert rows[winner_order_id] == (
                    "ENTRY_FILL_POSITION_CREATED",
                    winner_position_id,
                )
                assert rows[loser_order_id] == (
                    "OPEN_POSITION_ORDER_MISMATCH",
                    None,
                )
        finally:
            verify.rollback()
            verify.close()
    finally:
        _cleanup(prefix=prefix, source=source)

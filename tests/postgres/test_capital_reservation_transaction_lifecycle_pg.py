from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import uuid

import pytest
from psycopg2.extensions import TRANSACTION_STATUS_INTRANS

from common.capital_reservation import reservation_schema_available_cursor
from common.db import db_write_conn


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT / "db/migrations/20260821_capital_reservation_authority_v1.sql"
).read_text()


def _database(disposable_postgres_v16):
    name = "waltrade_baseline_test_reservation_tx_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    return name


def _apply(disposable_postgres_v16, database):
    conn = disposable_postgres_v16.connect(database)
    try:
        with conn.cursor() as cur:
            cur.execute(MIGRATION)
        conn.commit()
    finally:
        conn.close()


def _create_marker_table(disposable_postgres_v16, database):
    conn = disposable_postgres_v16.connect(database)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE tx_lifecycle_marker (id integer PRIMARY KEY)")
        conn.commit()
    finally:
        conn.close()


def _session_count(disposable_postgres_v16, database, application_name):
    conn = disposable_postgres_v16.connect(database)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE datname=current_database() AND application_name=%s",
                (application_name,),
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def _set_application_name(cur, name):
    cur.execute("SELECT set_config('application_name', %s, false)", (name,))


def test_probe_present_does_not_leave_idle_transaction(disposable_postgres_v16):
    database = _database(disposable_postgres_v16)
    _apply(disposable_postgres_v16, database)
    application = "reservation_probe_present"

    with db_write_conn(lambda: disposable_postgres_v16.connect(database)) as (_conn, cur):
        _set_application_name(cur, application)
        assert reservation_schema_available_cursor(cur) is True

    assert _session_count(disposable_postgres_v16, database, application) == 0


def test_probe_absent_does_not_leave_idle_transaction(disposable_postgres_v16):
    database = _database(disposable_postgres_v16)
    application = "reservation_probe_absent"

    with db_write_conn(lambda: disposable_postgres_v16.connect(database)) as (_conn, cur):
        _set_application_name(cur, application)
        assert reservation_schema_available_cursor(cur) is False

    assert _session_count(disposable_postgres_v16, database, application) == 0


def test_exception_after_probe_rolls_back_and_closes(disposable_postgres_v16):
    database = _database(disposable_postgres_v16)
    _apply(disposable_postgres_v16, database)
    _create_marker_table(disposable_postgres_v16, database)
    application = "reservation_probe_exception"

    with pytest.raises(RuntimeError, match="downstream failure"):
        with db_write_conn(
            lambda: disposable_postgres_v16.connect(database)
        ) as (_conn, cur):
            _set_application_name(cur, application)
            assert reservation_schema_available_cursor(cur) is True
            cur.execute("INSERT INTO tx_lifecycle_marker VALUES (1)")
            raise RuntimeError("downstream failure")

    assert _session_count(disposable_postgres_v16, database, application) == 0
    conn = disposable_postgres_v16.connect(database)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM tx_lifecycle_marker")
            assert cur.fetchone()[0] == 0
    finally:
        conn.close()


def test_early_return_after_probe_closes_connection(disposable_postgres_v16):
    database = _database(disposable_postgres_v16)
    _apply(disposable_postgres_v16, database)
    application = "reservation_probe_early_return"

    def run():
        with db_write_conn(
            lambda: disposable_postgres_v16.connect(database)
        ) as (_conn, cur):
            _set_application_name(cur, application)
            assert reservation_schema_available_cursor(cur) is True
            return "EARLY"

    assert run() == "EARLY"
    assert _session_count(disposable_postgres_v16, database, application) == 0


def test_repeated_probe_cycles_leave_no_sessions(disposable_postgres_v16):
    database = _database(disposable_postgres_v16)
    _apply(disposable_postgres_v16, database)
    application = "reservation_probe_repeated"

    for _ in range(8):
        with db_write_conn(
            lambda: disposable_postgres_v16.connect(database)
        ) as (_conn, cur):
            _set_application_name(cur, application)
            assert reservation_schema_available_cursor(cur) is True

    assert _session_count(disposable_postgres_v16, database, application) == 0


def test_failed_worker_cannot_self_block_subsequent_insert(disposable_postgres_v16):
    database = _database(disposable_postgres_v16)
    _apply(disposable_postgres_v16, database)
    _create_marker_table(disposable_postgres_v16, database)

    with pytest.raises(RuntimeError, match="after insert"):
        with db_write_conn(
            lambda: disposable_postgres_v16.connect(database)
        ) as (_conn, cur):
            assert reservation_schema_available_cursor(cur) is True
            cur.execute("INSERT INTO tx_lifecycle_marker VALUES (7)")
            raise RuntimeError("after insert")

    conn = disposable_postgres_v16.connect(database)
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout='1s'")
            cur.execute("INSERT INTO tx_lifecycle_marker VALUES (7)")
        conn.commit()
    finally:
        conn.close()


def test_multiple_workers_probe_and_insert_without_blocking(disposable_postgres_v16):
    database = _database(disposable_postgres_v16)
    _apply(disposable_postgres_v16, database)
    _create_marker_table(disposable_postgres_v16, database)

    def worker(marker):
        with db_write_conn(
            lambda: disposable_postgres_v16.connect(database)
        ) as (conn, cur):
            cur.execute("SET LOCAL statement_timeout='2s'")
            assert reservation_schema_available_cursor(cur) is True
            cur.execute("INSERT INTO tx_lifecycle_marker VALUES (%s)", (marker,))
            conn.commit()
        return marker

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert sorted(pool.map(worker, range(1, 9))) == list(range(1, 9))


def test_probe_preserves_caller_owned_transaction_boundary(disposable_postgres_v16):
    database = _database(disposable_postgres_v16)
    _apply(disposable_postgres_v16, database)
    _create_marker_table(disposable_postgres_v16, database)
    conn = disposable_postgres_v16.connect(database)
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO tx_lifecycle_marker VALUES (99)")
            assert reservation_schema_available_cursor(cur) is True
            assert conn.get_transaction_status() == TRANSACTION_STATUS_INTRANS
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM tx_lifecycle_marker WHERE id=99")
            assert cur.fetchone()[0] == 0
    finally:
        conn.close()

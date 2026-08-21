from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import uuid

import pytest

from common.position_risk_boundary import (
    _COLUMNS,
    _event_from_row,
    accept_boundary_policy_cursor,
    activate_boundary_for_position_cursor,
    append_boundary_event_cursor,
    load_boundary_projections_cursor,
)


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "db/migrations/20260821_position_risk_boundary_authority_v1.sql").read_text()
NOW = datetime(2026, 8, 21, 12, tzinfo=timezone.utc)


def database(disposable_postgres_v16):
    name = "waltrade_baseline_test_risk_boundary_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    return disposable_postgres_v16.connect(name)


def apply(conn):
    with conn.cursor() as cur:
        cur.execute(MIGRATION)
    conn.commit()


def setup_sources(cur):
    cur.execute(
        "CREATE TABLE strategy_params(symbol text,strategy text,interval text,"
        "param_name text,param_value numeric)"
    )
    cur.execute(
        "INSERT INTO strategy_params VALUES "
        "('BTCUSDC','RSI','1m','STOP_LOSS_PCT',0.8)"
    )
    cur.execute(
        "CREATE TABLE simulated_execution_fills_v1("
        "id bigint PRIMARY KEY,simulated_order_id bigint,position_id bigint,"
        "order_purpose text,fill_qty numeric,fill_price numeric,execution_at timestamptz)"
    )


def accept(cur, *, account="a" * 64, reservation=None):
    return accept_boundary_policy_cursor(
        cur, environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint=account,
        reservation_id=reservation or uuid.UUID("22222222-2222-4222-8222-222222222222"),
        decision_id="decision-1", intent_id=None, order_identity="101",
        symbol="BTCUSDC", strategy="RSI", interval="1m",
        effective_at=NOW, source_authority="TEST_ACCEPTED_COMMITMENT",
        provenance={"test": True},
    )


def test_migration_is_idempotent_empty_and_append_only(disposable_postgres_v16):
    conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        apply(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM position_risk_boundary_event_v1")
            assert cur.fetchone()[0] == 0
            setup_sources(cur)
            _, boundary_id = accept(cur)
        conn.commit()
        with conn.cursor() as cur:
            with pytest.raises(Exception, match="POSITION_RISK_BOUNDARY_APPEND_ONLY"):
                cur.execute(
                    "UPDATE position_risk_boundary_event_v1 SET source_authority='X' "
                    "WHERE boundary_id=%s", (str(boundary_id),),
                )
        conn.rollback()
    finally:
        conn.close()


def test_policy_freezes_before_fill_and_mutable_param_does_not_change_it(disposable_postgres_v16):
    conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        with conn.cursor() as cur:
            setup_sources(cur)
            status, boundary_id = accept(cur)
            assert status == "INSERTED"
            assert accept(cur)[0] == "IDEMPOTENT"
            cur.execute(
                "UPDATE strategy_params SET param_value=5 WHERE param_name='STOP_LOSS_PCT'"
            )
            assert accept(
                cur, reservation=uuid.UUID("22222222-2222-4222-8222-222222222222")
            )[0] == "IDEMPOTENT"
            cur.execute(
                "INSERT INTO simulated_execution_fills_v1 VALUES "
                "(1,101,501,'ENTRY',0.25,100.123456789123456789,%s)", (NOW,),
            )
            assert activate_boundary_for_position_cursor(
                cur, position_id=501, environment="PAPER",
                deployment_id="local-paper", effective_at=NOW,
                source_authority="TEST_FILL",
            ) == "INSERTED"
            cur.execute(
                "SELECT state,boundary_distance_pct,entry_basis_price,boundary_price,"
                "policy_fingerprint FROM v_position_risk_boundary_current_v1 "
                "WHERE boundary_id=%s", (str(boundary_id),),
            )
            row = cur.fetchone()
            assert row[0] == "BOUNDARY_ACTIVATED"
            assert row[1] == Decimal("0.800000000000")
            assert row[2] == Decimal("100.123456789123456789")
            assert row[3] == Decimal("99.322469134810469135")
            assert len(row[4]) == 64
        conn.rollback()
    finally:
        conn.close()


def test_additional_fill_appends_weighted_basis_revision(disposable_postgres_v16):
    conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        with conn.cursor() as cur:
            setup_sources(cur)
            _, boundary_id = accept(cur)
            cur.execute(
                "INSERT INTO simulated_execution_fills_v1 VALUES "
                "(1,101,501,'ENTRY',1,100,%s)", (NOW,),
            )
            activate_boundary_for_position_cursor(
                cur, position_id=501, environment="PAPER",
                deployment_id="local-paper", effective_at=NOW,
                source_authority="TEST_FILL",
            )
            cur.execute(
                "INSERT INTO simulated_execution_fills_v1 VALUES "
                "(2,101,501,'ENTRY',1,102,%s)", (NOW + timedelta(seconds=1),),
            )
            activate_boundary_for_position_cursor(
                cur, position_id=501, environment="PAPER",
                deployment_id="local-paper",
                effective_at=NOW + timedelta(seconds=1),
                source_authority="TEST_FILL",
            )
            cur.execute(
                "SELECT event_sequence,state,entry_basis_price,boundary_price "
                "FROM position_risk_boundary_event_v1 WHERE boundary_id=%s "
                "ORDER BY event_sequence", (str(boundary_id),),
            )
            rows = cur.fetchall()
            assert [row[0] for row in rows] == [1, 2, 3]
            assert rows[-1][1:] == (
                "BOUNDARY_REVISED_ENTRY_BASIS", Decimal("101.000000000000000000"),
                Decimal("100.192000000000000000"),
            )
        conn.rollback()
    finally:
        conn.close()


def test_event_sequence_and_account_scope_fail_closed(disposable_postgres_v16):
    conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        with conn.cursor() as cur:
            setup_sources(cur)
            _, boundary_id = accept(cur)
            cur.execute(
                f"SELECT {_COLUMNS} FROM v_position_risk_boundary_current_v1 "
                "WHERE boundary_id=%s", (str(boundary_id),),
            )
            event = _event_from_row(cur.fetchone())
            invalid = replace(
                event, event_id=uuid.uuid4(), event_sequence=3,
                state="BOUNDARY_ACTIVATED", position_id=501,
                entry_basis_price=Decimal("100"),
                entry_basis_authority="TEST", boundary_price=Decimal("99.2"),
                event_fingerprint="e" * 64,
            )
            with pytest.raises(Exception, match="EVENT_SEQUENCE_INVALID"):
                append_boundary_event_cursor(cur, invalid)
        conn.rollback()

        with conn.cursor() as cur:
            projections, status = load_boundary_projections_cursor(
                cur, environment="PAPER", deployment_id="local-paper",
                account_identity_fingerprint="b" * 64,
            )
            assert status == "CANONICAL"
            assert projections == {}
        conn.rollback()
    finally:
        conn.close()

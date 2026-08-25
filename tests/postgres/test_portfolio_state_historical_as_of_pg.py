"""Exact PAPER Portfolio State declared-as-of replay regression."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import uuid

from common.capital_reservation import paper_account_identity_fingerprint
from common.portfolio_state import read_portfolio_state
from common.position_risk_boundary import (
    accept_boundary_policy_cursor,
    activate_boundary_for_position_cursor,
    load_boundary_projections_cursor,
)


ROOT = Path(__file__).resolve().parents[2]
RESERVATION_MIGRATION = (
    ROOT / "db/migrations/20260821_capital_reservation_authority_v1.sql"
).read_text()
BOUNDARY_MIGRATION = (
    ROOT / "db/migrations/20260821_position_risk_boundary_authority_v1.sql"
).read_text()
T = datetime(2026, 8, 25, 14, 0, tzinfo=timezone.utc)
CLOSED_AT = datetime(2026, 8, 25, 14, 0, 28, 741911, tzinfo=timezone.utc)
ACCOUNT = paper_account_identity_fingerprint("local-paper")
SNAPSHOT = uuid.UUID("44444444-4444-4444-8444-444444444444")


SCHEMA = """
CREATE TABLE paper_equity_baseline_v2(
  deployment_id text,baseline_version text,baseline_timestamp timestamptz,
  baseline_managed_equity numeric,baseline_unrealized_pnl numeric,
  source_authority text,activation_fingerprint text,evidence_status text
);
CREATE TABLE positions(
  id bigint primary key,symbol text,strategy text,interval text,side text,
  entry_price numeric,remaining_inventory_qty numeric,
  inventory_evidence_status text,status text,exit_time timestamptz,
  entry_opportunity_snapshot_id uuid
);
CREATE TABLE simulated_execution_fills_v1(
  id bigint primary key,simulated_order_id bigint,position_id bigint,
  order_purpose text,fill_qty numeric,fill_price numeric,
  execution_at timestamptz,environment text,deployment_id text,
  source_authority text
);
CREATE TABLE canonical_financial_truth_v1(
  position_id bigint primary key,financial_truth_status text,
  authoritative_net_pnl numeric,evidence_observed_at timestamptz
);
CREATE TABLE candles(symbol text,interval text,close numeric,open_time timestamptz);
CREATE TABLE market_regime(symbol text,interval text,regime text,ts timestamptz);
CREATE TABLE equity_daily_snapshot_v1(
  deployment_id text,evidence_status text,source_timestamp timestamptz,
  waltrade_managed_equity_usdc numeric
);
CREATE TABLE entry_opportunity_evidence_v1(
  snapshot_id uuid primary key,fee_rate_exit_assumption numeric,
  fee_model_version text,decision_created_at timestamptz,captured_at timestamptz
);
"""


def _database(disposable_postgres_v16):
    name = "waltrade_baseline_test_portfolio_asof_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
        cur.execute(RESERVATION_MIGRATION)
        cur.execute(BOUNDARY_MIGRATION)
    conn.commit()
    return conn


def _accept_and_activate(cur, *, position_id: int, order_id: int, at: datetime):
    status, _ = accept_boundary_policy_cursor(
        cur, environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint=ACCOUNT, reservation_id=uuid.uuid4(),
        decision_id=f"decision-{position_id}", intent_id=None,
        order_identity=str(order_id), symbol="BTCUSDC", strategy="RSI",
        interval="1m", effective_at=at - timedelta(seconds=1),
        source_authority="TEST_ACCEPT", provenance={"test": True},
        boundary_distance_pct=Decimal("0.8"),
    )
    assert status == "INSERTED"
    assert activate_boundary_for_position_cursor(
        cur, position_id=position_id, environment="PAPER",
        deployment_id="local-paper", effective_at=at,
        source_authority="TEST_FILL",
    ) == "INSERTED"


def _prepare(cur):
    cur.execute(
        "INSERT INTO paper_equity_baseline_v2 VALUES "
        "('local-paper','PAPER_EQUITY_BASELINE_V2',%s,100,0,"
        "'CANONICAL',%s,'COMPLETE')",
        (T - timedelta(days=1), "a" * 64),
    )
    cur.execute(
        "INSERT INTO entry_opportunity_evidence_v1 VALUES "
        "(%s,0.0035,'PAPER_SIMULATOR_FINANCIAL_MODEL_V2',%s,%s)",
        (str(SNAPSHOT), T - timedelta(minutes=3), T - timedelta(minutes=2)),
    )
    cur.execute(
        "INSERT INTO candles VALUES ('BTCUSDC','1m',101,%s)",
        (T - timedelta(seconds=5),),
    )
    cur.execute(
        "INSERT INTO market_regime VALUES ('BTCUSDC','1m','TREND',%s)",
        (T - timedelta(seconds=5),),
    )

    # Confirmed incident: current row is CLOSED, but 1.5 units existed at T.
    cur.execute(
        "INSERT INTO positions VALUES "
        "(8309,'BTCUSDC','RSI','1m','BUY',100,0,'COMPLETE','CLOSED',%s,%s)",
        (CLOSED_AT, str(SNAPSHOT)),
    )
    cur.execute(
        "INSERT INTO simulated_execution_fills_v1 VALUES "
        "(1,9001,8309,'ENTRY',2,100,%s,'PAPER','local-paper','SIMULATED_EXECUTION'),"
        "(2,9002,8309,'EXIT',0.5,101,%s,'PAPER','local-paper','SIMULATED_EXECUTION')",
        (T - timedelta(minutes=1), T - timedelta(seconds=10)),
    )
    _accept_and_activate(
        cur, position_id=8309, order_id=9001, at=T - timedelta(seconds=30)
    )
    # This exit exists when the query runs, but is not eligible at declared T.
    cur.execute(
        "INSERT INTO simulated_execution_fills_v1 VALUES "
        "(3,9003,8309,'EXIT',1.5,102,%s,'PAPER','local-paper','SIMULATED_EXECUTION')",
        (CLOSED_AT,),
    )
    cur.execute(
        "INSERT INTO canonical_financial_truth_v1 VALUES "
        "(8309,'COMPLETE',1,%s)", (CLOSED_AT,),
    )

    # Future position and fill must not leak backward.
    cur.execute(
        "INSERT INTO positions VALUES "
        "(8310,'BTCUSDC','RSI','1m','BUY',110,0.25,'COMPLETE','OPEN',NULL,%s)",
        (str(SNAPSHOT),),
    )
    cur.execute(
        "INSERT INTO simulated_execution_fills_v1 VALUES "
        "(4,9010,8310,'ENTRY',0.25,110,%s,'PAPER','local-paper','SIMULATED_EXECUTION')",
        (T + timedelta(seconds=10),),
    )
    _accept_and_activate(
        cur, position_id=8310, order_id=9010, at=T + timedelta(seconds=11)
    )

    # Administrative evidence is outside the canonical execution authority.
    cur.execute(
        "INSERT INTO positions VALUES "
        "(8311,'BTCUSDC','RSI','1m','BUY',90,9,'COMPLETE','OPEN',NULL,%s)",
        (str(SNAPSHOT),),
    )
    cur.execute(
        "INSERT INTO simulated_execution_fills_v1 VALUES "
        "(5,9011,8311,'ENTRY',9,90,%s,'PAPER','local-paper','ADMIN_REPAIR')",
        (T - timedelta(minutes=1),),
    )


def test_exact_position_8309_declared_as_of_and_post_close(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            _prepare(cur)
            at_t = read_portfolio_state(
                cur, environment="PAPER", deployment_id="local-paper", as_of=T,
            )
            assert at_t.open_positions_count == 1
            assert at_t.position_risk[0].position_id == 8309
            assert at_t.exposure_by_symbol[0].quantity == Decimal("1.5")
            assert at_t.open_risk == Decimal("3.2208000000000000000")
            assert at_t.open_risk_status == "CANONICAL"
            assert "ENTRY_MINUS_EXIT_THROUGH_DECLARED_AS_OF" in (
                at_t.source_authorities["inventory_quantity"]
            )

            post_close = read_portfolio_state(
                cur, environment="PAPER", deployment_id="local-paper",
                as_of=CLOSED_AT + timedelta(seconds=1),
            )
            assert {item.position_id for item in post_close.position_risk} == {8310}
            assert post_close.realized_pnl == Decimal("1")

            current = read_portfolio_state(
                cur, environment="PAPER", deployment_id="local-paper", as_of=None,
            )
            assert current.open_positions_count == 2
            assert current.source_authorities["inventory_quantity"] == (
                "positions.remaining_inventory_qty"
            )
        conn.rollback()
    finally:
        conn.close()


def test_boundary_projection_never_forward_fills_future_event(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            _prepare(cur)
            before, status = load_boundary_projections_cursor(
                cur, environment="PAPER", deployment_id="local-paper",
                account_identity_fingerprint=ACCOUNT, as_of=T,
            )
            after, _ = load_boundary_projections_cursor(
                cur, environment="PAPER", deployment_id="local-paper",
                account_identity_fingerprint=ACCOUNT,
                as_of=T + timedelta(seconds=12),
            )
            assert status == "CANONICAL"
            assert set(before) == {8309}
            assert set(after) == {8309, 8310}
            assert before[8309].effective_at < T
        conn.rollback()
    finally:
        conn.close()

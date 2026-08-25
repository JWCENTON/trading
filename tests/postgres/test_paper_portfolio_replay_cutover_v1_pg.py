"""PAPER Portfolio Replay Cutover V1 authority and immutability regressions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import uuid

import pytest

from common.capital_reservation import paper_account_identity_fingerprint
from common.paper_portfolio_replay_cutover import (
    calibration_replay_eligibility_cursor,
    create_replay_cutover_cursor,
)
from common.position_risk_boundary import (
    accept_boundary_policy_cursor,
    activate_boundary_for_position_cursor,
)


ROOT = Path(__file__).resolve().parents[2]
RESERVATION = (ROOT / "db/migrations/20260821_capital_reservation_authority_v1.sql").read_text()
BOUNDARY = (ROOT / "db/migrations/20260821_position_risk_boundary_authority_v1.sql").read_text()
CUTOVER = (ROOT / "db/migrations/20260825_paper_portfolio_replay_cutover_v1.sql").read_text()
NOW = datetime.now(timezone.utc).replace(microsecond=0)
ACCOUNT = paper_account_identity_fingerprint("local-paper")


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
  entry_opportunity_snapshot_id uuid,entry_order_id text,
  inventory_contract_adoption_id bigint,inventory_contract_generation bigint
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
    name = "waltrade_baseline_test_replay_cutover_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
        cur.execute(RESERVATION)
        cur.execute(BOUNDARY)
        cur.execute(CUTOVER)
    conn.commit()
    return conn


def _canonical_current(cur, *, complete: bool = True):
    snapshot = uuid.uuid4()
    cur.execute(
        "INSERT INTO paper_equity_baseline_v2 VALUES "
        "('local-paper','PAPER_EQUITY_BASELINE_V2',%s,100,0,'CANONICAL',%s,'COMPLETE')",
        (NOW - timedelta(days=10), "a" * 64),
    )
    cur.execute(
        "INSERT INTO positions VALUES "
        "(1,'BTCUSDC','RSI','1m','BUY',100,2,%s,'OPEN',NULL,%s,'9001',9,1)",
        ("COMPLETE" if complete else "INCOMPLETE", str(snapshot)),
    )
    cur.execute(
        "INSERT INTO simulated_execution_fills_v1 VALUES "
        "(1,9001,1,'ENTRY',2,100,%s,'PAPER','local-paper','SIMULATED_EXECUTION')",
        (NOW - timedelta(minutes=1, seconds=30),),
    )
    cur.execute(
        "INSERT INTO candles VALUES ('BTCUSDC','1m',110,%s)",
        (NOW - timedelta(seconds=1),),
    )
    cur.execute(
        "INSERT INTO market_regime VALUES ('BTCUSDC','1m','TREND',%s)",
        (NOW - timedelta(seconds=1),),
    )
    cur.execute(
        "INSERT INTO entry_opportunity_evidence_v1 VALUES "
        "(%s,0.0035,'PAPER_SIMULATOR_FINANCIAL_MODEL_V2',%s,%s)",
        (str(snapshot), NOW - timedelta(minutes=2), NOW - timedelta(minutes=1)),
    )
    status, _ = accept_boundary_policy_cursor(
        cur, environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint=ACCOUNT, reservation_id=uuid.uuid4(),
        decision_id="decision-1", intent_id=None, order_identity="9001",
        symbol="BTCUSDC", strategy="RSI", interval="1m",
        effective_at=NOW - timedelta(minutes=2), source_authority="TEST_ACCEPT",
        provenance={"test": True}, boundary_distance_pct=Decimal("1"),
    )
    assert status == "INSERTED"
    assert activate_boundary_for_position_cursor(
        cur, position_id=1, environment="PAPER", deployment_id="local-paper",
        effective_at=NOW - timedelta(minutes=1), source_authority="TEST_FILL",
    ) == "INSERTED"


def test_canonical_current_inventory_creates_exact_immutable_cutover(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            _canonical_current(cur)
            cur.execute(CUTOVER)
            cur.execute("BEGIN")
            cutover, created = create_replay_cutover_cursor(
                cur, deployment_id="local-paper", git_revision="b" * 40,
            )
            assert created is True
            assert cutover.inventory_position_count == 1
            cur.execute(
                "SELECT remaining_inventory_qty,entry_basis_price,risk_owner,"
                "entry_lineage->>'entry_order_id' "
                "FROM paper_portfolio_replay_cutover_position_v1"
            )
            assert cur.fetchone() == (
                Decimal("2"), Decimal("100"), "POSITION_OPEN_RISK", "9001"
            )
            same, created_again = create_replay_cutover_cursor(
                cur, deployment_id="local-paper", git_revision="b" * 40,
            )
            assert created_again is False
            assert same.cutover_id == cutover.cutover_id
            with pytest.raises(Exception, match="APPEND_ONLY"):
                cur.execute(
                    "UPDATE paper_portfolio_replay_cutover_v1 SET cutover_at=%s",
                    (NOW + timedelta(seconds=1),),
                )
        conn.rollback()
    finally:
        conn.close()


def test_incomplete_current_inventory_fails_before_cutover_rows(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            _canonical_current(cur, complete=False)
            with pytest.raises(RuntimeError, match="CURRENT_PORTFOLIO_INCOMPLETE"):
                create_replay_cutover_cursor(
                    cur, deployment_id="local-paper", git_revision="b" * 40,
                )
            cur.execute("SELECT count(*) FROM paper_portfolio_replay_cutover_v1")
            assert cur.fetchone()[0] == 0
        conn.rollback()
    finally:
        conn.close()


def test_calibration_eligibility_starts_at_cutover(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            _canonical_current(cur)
            cutover, _ = create_replay_cutover_cursor(
                cur, deployment_id="local-paper", git_revision="b" * 40,
            )
            assert calibration_replay_eligibility_cursor(
                cur, deployment_id="local-paper",
                evaluation_as_of=cutover.cutover_at - timedelta(microseconds=1),
            ) == (False, "PRE_CUTOVER_NON_CAUSAL_CALIBRATION_EVIDENCE")
            assert calibration_replay_eligibility_cursor(
                cur, deployment_id="local-paper", evaluation_as_of=cutover.cutover_at,
            ) == (True, "CANONICAL_REPLAY_INTERVAL_REQUIRED")
        conn.rollback()
    finally:
        conn.close()

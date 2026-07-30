from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from common.closed_outcome_read_model import (
    CLOSED_OUTCOME_ROWS_SQL,
    fetch_closed_outcome_summary,
)


START = datetime(2026, 7, 29, 0, 0, tzinfo=timezone.utc)
END = datetime(2026, 7, 30, 0, 0, tzinfo=timezone.utc)


def _database(disposable_postgres_v16, purpose):
    name = f"waltrade_baseline_test_closed_outcome_{purpose}"
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE positions (
              id BIGINT PRIMARY KEY,
              strategy TEXT NOT NULL DEFAULT 'RSI',
              status TEXT NOT NULL,
              side TEXT,
              qty NUMERIC,
              exit_time TIMESTAMPTZ,
              gross_pnl_usdc NUMERIC,
              fees_usdc NUMERIC,
              net_pnl_usdc NUMERIC,
              inventory_evidence_status TEXT,
              inventory_contract_generation BIGINT,
              exit_context_json JSONB
            );
            CREATE INDEX ix_positions_closed_exit
              ON positions(exit_time) WHERE status='CLOSED';

            CREATE TABLE canonical_financial_truth_v1 (
              position_id BIGINT PRIMARY KEY,
              financial_truth_status TEXT,
              authoritative_gross_pnl NUMERIC,
              authoritative_net_pnl NUMERIC,
              authoritative_fees_usdc NUMERIC,
              authoritative_entry_fees_usdc NUMERIC,
              authoritative_exit_fees_usdc NUMERIC
            );

            CREATE TABLE simulated_execution_fills_v1 (
              id BIGSERIAL PRIMARY KEY,
              simulated_order_id BIGINT,
              position_id BIGINT NOT NULL,
              fill_index INTEGER NOT NULL,
              order_purpose TEXT NOT NULL,
              fill_qty NUMERIC NOT NULL,
              fill_price NUMERIC NOT NULL,
              fill_notional NUMERIC NOT NULL,
              authoritative_fee_usdc NUMERIC,
              estimated_fee_usdc NUMERIC
            );
            CREATE INDEX ix_sim_fills_position
              ON simulated_execution_fills_v1(position_id, order_purpose);
            """
        )
    conn.commit()
    return conn


def _position(cur, position_id, **values):
    defaults = {
        "strategy": "RSI",
        "status": "CLOSED",
        "side": "LONG",
        "qty": "0",
        "exit_time": "2026-07-29 12:00:00+00",
        "gross": None,
        "fees": None,
        "net": None,
        "evidence": None,
        "generation": None,
        "provenance": None,
    }
    defaults.update(values)
    cur.execute(
        """
        INSERT INTO positions(
          id,strategy,status,side,qty,exit_time,gross_pnl_usdc,
          fees_usdc,net_pnl_usdc,inventory_evidence_status,
          inventory_contract_generation,exit_context_json
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            position_id,
            defaults["strategy"],
            defaults["status"],
            defaults["side"],
            defaults["qty"],
            defaults["exit_time"],
            defaults["gross"],
            defaults["fees"],
            defaults["net"],
            defaults["evidence"],
            defaults["generation"],
            (
                '{"outcome_provenance":"CLOSED_OUTCOME_V1"}'
                if defaults["provenance"] == "CLOSED_OUTCOME_V1"
                else None
            ),
        ),
    )


def _fill(cur, position_id, purpose, notional, fee="0.10", index=0, qty="1"):
    cur.execute(
        """
        INSERT INTO simulated_execution_fills_v1(
          simulated_order_id,position_id,fill_index,order_purpose,
          fill_qty,fill_price,fill_notional,authoritative_fee_usdc,
          estimated_fee_usdc
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NULL)
        """,
        (
            position_id * 10 + index,
            position_id,
            index,
            purpose,
            qty,
            notional,
            notional,
            fee,
        ),
    )


def test_canonical_contract_and_boundaries(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16, "contract")
    try:
        with conn.cursor() as cur:
            _position(
                cur, 1, gross="2", fees="0.2", net="1.8",
                evidence="COMPLETE", generation=5, exit_time=START,
                provenance="CLOSED_OUTCOME_V1",
            )
            _position(
                cur, 2, gross="-2", fees="0.2", net="-2.2",
                evidence="COMPLETE", generation=4, exit_time=END,
                provenance="CLOSED_OUTCOME_V1",
            )
            _position(cur, 3)
            _fill(cur, 3, "ENTRY", "10")
            _fill(cur, 3, "EXIT", "12", index=1)
            _position(cur, 4, side="SHORT")
            _fill(cur, 4, "ENTRY", "10")
            _fill(cur, 4, "EXIT", "12", index=1)
            _position(cur, 5)
            _fill(cur, 5, "ENTRY", "10", fee="0")
            _fill(cur, 5, "EXIT", "10", fee="0", index=1)
            _position(cur, 6)
            _fill(cur, 6, "EXIT", "12")
            _position(cur, 7)
            _fill(cur, 7, "ENTRY", "10")
            _position(cur, 8)
            _fill(cur, 8, "ENTRY", "10", fee=None)
            _fill(cur, 8, "EXIT", "12", index=1)
            _position(
                cur, 9, strategy="SUPERTREND", gross="0", fees="0",
                net="0", evidence="COMPLETE", generation=2,
            )
            _fill(cur, 9, "ENTRY", "10")
            _fill(cur, 9, "EXIT", "11", index=1)
            _position(cur, 10)
            cur.execute(
                """
                INSERT INTO canonical_financial_truth_v1 VALUES
                  (10,'COMPLETE',3,2.5,0.5,NULL,NULL)
                """
            )
            _position(cur, 11, exit_time="2026-07-28 23:59:59+00")
            _fill(cur, 11, "ENTRY", "10")
            _fill(cur, 11, "EXIT", "20", index=1)
            _position(cur, 12, exit_time="2026-07-30 00:00:01+00")
            _fill(cur, 12, "ENTRY", "10")
            _fill(cur, 12, "EXIT", "20", index=1)
            _position(cur, 13, status="OPEN")
            _fill(cur, 13, "ENTRY", "10")
            _fill(cur, 13, "EXIT", "20", index=1)
            _position(cur, 14)
            _fill(cur, 14, "ENTRY", "10", qty="2")
            _fill(cur, 14, "EXIT", "12", index=1, qty="1")
            conn.commit()

            params = {"window_start": START, "window_end": END}
            cur.execute("SELECT count(*) FROM positions")
            positions_before = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM simulated_execution_fills_v1")
            fills_before = cur.fetchone()[0]
            cur.execute(CLOSED_OUTCOME_ROWS_SQL, params)
            rows = {row[0]: row for row in cur.fetchall()}

            assert set(rows) == set(range(1, 11)) | {14}
            assert rows[1][2:7] == (
                "STORED_PROVEN", Decimal("2"), Decimal("0.2"),
                Decimal("1.8"), "WIN",
            )
            assert rows[2][2] == "STORED_PROVEN"
            assert rows[2][6] == "LOSS"
            assert rows[3][2] == "SIMULATED_FILLS"
            assert rows[3][5] == Decimal("1.80")
            assert rows[4][5] == Decimal("-2.20")
            assert rows[5][6] == "FLAT"
            assert rows[6][6] == "UNRESOLVED"
            assert rows[7][6] == "UNRESOLVED"
            assert rows[8][6] == "UNRESOLVED"
            assert rows[9][2] == "SIMULATED_FILLS"
            assert rows[9][5] == Decimal("0.80")
            assert rows[10][2] == "FINANCIAL_TRUTH"
            assert rows[10][5] == Decimal("2.5")
            assert rows[14][6] == "UNRESOLVED"
            assert all(row[7] is (row[6] != "UNRESOLVED") for row in rows.values())

            summary = fetch_closed_outcome_summary(
                cur, window_start=START, window_end=END
            )
            assert summary["trades"] == 11
            assert summary["resolved_trades"] == 7
            assert summary["unresolved_trades"] == 4
            assert summary["wins"] == 4
            assert summary["losses"] == 2
            assert summary["flats"] == 1
            assert summary["outcome_source_counts"] == {
                "FINANCIAL_TRUTH": 1,
                "SIMULATED_FILLS": 4,
                "STORED_PROVEN": 2,
                "UNRESOLVED": 4,
            }
            cur.execute("SELECT count(*) FROM positions")
            assert cur.fetchone()[0] == positions_before
            cur.execute("SELECT count(*) FROM simulated_execution_fills_v1")
            assert cur.fetchone()[0] == fills_before
    finally:
        conn.close()


def test_multiple_fills_do_not_duplicate_position(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16, "dedupe")
    try:
        with conn.cursor() as cur:
            _position(cur, 20, qty="0", generation=1)
            _fill(cur, 20, "ENTRY", "5", fee="0.05", index=0)
            _fill(cur, 20, "ENTRY", "5", fee="0.05", index=1)
            _fill(cur, 20, "EXIT", "6", fee="0.05", index=2)
            _fill(cur, 20, "EXIT", "6", fee="0.05", index=3)
            conn.commit()
            summary = fetch_closed_outcome_summary(
                cur, window_start=START, window_end=END
            )
        assert summary["trades"] == 1
        assert summary["wins"] == 1
        assert summary["net_pnl"] == Decimal("1.80")
    finally:
        conn.close()

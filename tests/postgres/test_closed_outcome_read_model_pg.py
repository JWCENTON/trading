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
              symbol TEXT NOT NULL DEFAULT 'BTCUSDC',
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
              ,entry_order_id TEXT
              ,exit_order_id TEXT
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

            CREATE TABLE binance_order_fills (
              id BIGSERIAL PRIMARY KEY, source TEXT NOT NULL DEFAULT 'okx',
              trade_id TEXT NOT NULL, order_id TEXT NOT NULL, symbol TEXT NOT NULL,
              side TEXT NOT NULL, executed_qty NUMERIC NOT NULL,
              quote_notional_usdc NUMERIC NOT NULL, commission_amount NUMERIC,
              commission_asset TEXT, commission_usdc NUMERIC
            );
            CREATE TABLE exchange_fill_ingestion_state_v2 (
              order_id TEXT, application_status TEXT
            );
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


def _legacy_fill(
    cur, position_id, purpose, qty, notional, fee, *, symbol="BTCUSDC",
):
    order_id = f"legacy-{position_id}-{purpose.lower()}"
    side = "BUY" if purpose == "ENTRY" else "SELL"
    commission_asset = symbol.removesuffix("USDC") if purpose == "ENTRY" else "USDC"
    commission_amount = (
        Decimal(str(fee)) / (Decimal(str(notional)) / Decimal(str(qty)))
        if purpose == "ENTRY" else Decimal(str(fee))
    )
    cur.execute(
        "UPDATE positions SET symbol=%s,entry_order_id=CASE WHEN %s='ENTRY' "
        "THEN %s ELSE entry_order_id END,exit_order_id=CASE WHEN %s='EXIT' "
        "THEN %s ELSE exit_order_id END WHERE id=%s",
        (symbol, purpose, order_id, purpose, order_id, position_id),
    )
    cur.execute(
        """
        INSERT INTO binance_order_fills(
          source,trade_id,order_id,symbol,side,executed_qty,
          quote_notional_usdc,commission_amount,commission_asset,commission_usdc
        ) VALUES ('okx',%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            f"trade-{position_id}-{purpose.lower()}", order_id, symbol, side,
            qty, notional, commission_amount, commission_asset, fee,
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
            assert rows[3][2] == "PAPER_SIMULATED_FILLS"
            assert rows[3][5] == Decimal("1.80")
            assert rows[4][5] == Decimal("-2.20")
            assert rows[5][6] == "FLAT"
            assert rows[6][6] == "UNRESOLVED"
            assert rows[7][6] == "UNRESOLVED"
            assert rows[8][6] == "UNRESOLVED"
            assert rows[9][2] == "PAPER_SIMULATED_FILLS"
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
                "PAPER_SIMULATED_FILLS": 4,
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


def test_live_legacy_execution_proven_golden_cohort(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16, "legacy_live")
    cases = (
        (3069, "0.00032361", "19.999874664", "0.0699995613240", "0.00032248", "19.966058968", "0.069881206388", "-0.103615"),
        (3070, "0.00032298", "19.999858242", "0.069999503847", "0.00032185", "19.955214960", "0.06984325236", "-0.114268"),
        (3071, "0.00032385", "19.999972065", "0.0699999022275", "0.00032271", "20.005728759", "0.0700200506565", "-0.063614"),
        (3072, "0.00032283", "19.99996416", "0.069999874560", "0.0003217", "19.96515238", "0.06987803333", "-0.104439"),
        (3073, "0.00032341", "19.999803764", "0.0699993131740", "0.00032228", "19.973818648", "0.069908365268", "-0.095769"),
        (3074, "0.00031878", "19.999555884", "0.069998445594", "0.00031767", "19.941607182", "0.069795625137", "-0.127860"),
        (3075, "0.011318", "19.98883298", "0.06996091543", "0.011279", "19.95063357", "0.069827217495", "-0.108868"),
        (3076, "0.00031248", "19.999813680", "0.069999347880", "0.00031138", "19.917234872", "0.069710322052", "-0.151638"),
        (3077, "0.034257", "19.8994996", "0.06964824860", "0.034138", "19.8587522", "0.0695056327", "-0.110534"),
        (3078, "0.25796", "19.9893204", "0.0699626214", "0.25705", "19.8673945", "0.06953588075", "-0.190662"),
    )
    try:
        with conn.cursor() as cur:
            for case in cases:
                position_id, eq, en, ef, xq, xn, xf, _expected = case
                _position(cur, position_id)
                _legacy_fill(cur, position_id, "ENTRY", eq, en, ef)
                _legacy_fill(cur, position_id, "EXIT", xq, xn, xf)
            _position(cur, 3090)
            _legacy_fill(
                cur, 3090, "ENTRY", "0.001", "20", "0.07",
            )
            cur.execute(
                "UPDATE positions SET exit_order_id='legacy-3090-exit' WHERE id=3090"
            )
            conn.commit()
            cur.execute(CLOSED_OUTCOME_ROWS_SQL, {
                "window_start": START, "window_end": END,
            })
            rows = {row[0]: row for row in cur.fetchall()}
        assert len(rows) == 11
        for case in cases:
            row = rows[case[0]]
            assert row[2] == "LEGACY_EXECUTION_PROVEN"
            assert row[6] == "LOSS"
            assert row[5].quantize(Decimal("0.000001")) == Decimal(case[-1])
            assert row[8] == "COMPLETE"
        assert rows[3090][2] == "UNRESOLVED"
        assert rows[3090][8] == "INCOMPLETE"
        assert "MISSING_EXIT_FILLS" in rows[3090][9]
    finally:
        conn.close()


def test_vps_paper_37_qty_zero_stored_outcomes(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16, "vps_37_qty_zero")
    nets = [Decimal("0.04")] * 28 + [Decimal("-0.04")] * 8 + [Decimal("-0.06620976")]
    fees = [Decimal("0.01")] * 36 + [Decimal("0.23250238")]
    try:
        with conn.cursor() as cur:
            for offset, (net, fee) in enumerate(zip(nets, fees), start=1):
                _position(
                    cur, 4000 + offset, qty="0", gross=net + fee,
                    fees=fee, net=net, evidence="COMPLETE", generation=1,
                    provenance="CLOSED_OUTCOME_V1",
                )
            conn.commit()
            summary = fetch_closed_outcome_summary(
                cur, window_start=START, window_end=END,
            )
        assert summary["trades"] == 37
        assert summary["resolved_trades"] == 37
        assert summary["wins"] == 28
        assert summary["losses"] == 9
        assert summary["flats"] == 0
        assert summary["gross_pnl"] == Decimal("1.32629262")
        assert summary["fees"] == Decimal("0.59250238")
        assert summary["net_pnl"] == Decimal("0.73379024")
        assert summary["win_rate"].quantize(Decimal("0.0000000001")) == Decimal("75.6756756757")
    finally:
        conn.close()

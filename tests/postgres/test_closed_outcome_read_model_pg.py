from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from time import monotonic

from common.closed_outcome_read_model import (
    build_closed_outcome_rows_sql,
    fetch_closed_outcomes,
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
              entry_price NUMERIC,
              exit_price NUMERIC,
              exit_time TIMESTAMPTZ,
              exit_reason TEXT,
              gross_pnl_usdc NUMERIC,
              fees_usdc NUMERIC,
              net_pnl_usdc NUMERIC,
              inventory_evidence_status TEXT,
              remaining_inventory_qty NUMERIC,
              gross_entry_executed_qty NUMERIC,
              net_entry_inventory_qty NUMERIC,
              cumulative_exit_executed_qty NUMERIC,
              exit_inventory_reduction_qty NUMERIC,
              inventory_contract_adoption_id BIGINT,
              inventory_contract_generation BIGINT,
              entry_context_json JSONB,
              exit_context_json JSONB
              ,entry_order_id TEXT
              ,exit_order_id TEXT
            );
            CREATE INDEX ix_positions_closed_exit
              ON positions(exit_time) WHERE status='CLOSED';

            CREATE TABLE runtime_contract_adoption_v2 (
              adoption_id BIGINT PRIMARY KEY,
              contract_name TEXT NOT NULL,
              environment TEXT NOT NULL,
              deployment_id TEXT NOT NULL,
              generation BIGINT NOT NULL,
              status TEXT NOT NULL DEFAULT 'ACTIVE'
            );

            CREATE TABLE canonical_financial_truth_v1 (
              position_id BIGINT PRIMARY KEY,
              financial_truth_status TEXT,
              authoritative_gross_pnl NUMERIC,
              authoritative_net_pnl NUMERIC,
              authoritative_fees_usdc NUMERIC,
              authoritative_entry_fees_usdc NUMERIC,
              authoritative_exit_fees_usdc NUMERIC,
              entry_fill_count INTEGER,
              exit_fill_count INTEGER,
              executed_entry_qty NUMERIC,
              executed_exit_qty NUMERIC,
              remaining_inventory_qty NUMERIC,
              remaining_qty NUMERIC,
              source_fingerprint TEXT,
              source_order_ids JSONB,
              source_fill_ids JSONB,
              calculation_version TEXT,
              failure_reason TEXT,
              failure_code TEXT,
              failure_detail TEXT
            );

            CREATE TABLE simulated_execution_fills_v1 (
              id BIGSERIAL PRIMARY KEY,
              simulated_order_id BIGINT,
              position_id BIGINT NOT NULL,
              fill_index INTEGER NOT NULL,
              order_purpose TEXT NOT NULL,
              side TEXT NOT NULL,
              symbol TEXT NOT NULL,
              fill_qty NUMERIC NOT NULL,
              fill_price NUMERIC NOT NULL,
              fill_notional NUMERIC NOT NULL,
              fee_qty NUMERIC,
              fee_asset TEXT,
              authoritative_fee_usdc NUMERIC,
              estimated_fee_usdc NUMERIC,
              source_authority TEXT,
              environment TEXT,
              deployment_id TEXT,
              simulation_model_version TEXT,
              source_fingerprint TEXT
            );
            CREATE INDEX ix_sim_fills_position
              ON simulated_execution_fills_v1(position_id, order_purpose);
            CREATE INDEX ix_sim_fills_order_position
              ON simulated_execution_fills_v1(simulated_order_id, position_id);

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
            CREATE TABLE position_lifecycle_events_c2_2 (
              event_id BIGSERIAL PRIMARY KEY,
              position_id BIGINT NOT NULL,
              order_id TEXT NOT NULL,
              mutation_kind TEXT NOT NULL,
              mutation_high_water NUMERIC NOT NULL,
              payload JSONB NOT NULL DEFAULT '{}'::jsonb
            );
            CREATE INDEX ix_lifecycle_position
              ON position_lifecycle_events_c2_2(position_id, mutation_kind);
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
        "entry_price": "10",
        "exit_price": "11",
        "exit_time": "2026-07-29 12:00:00+00",
        "gross": None,
        "fees": None,
        "net": None,
        "evidence": None,
        "remaining": None,
        "gross_entry_qty": None,
        "net_entry_qty": None,
        "cumulative_exit_qty": None,
        "exit_reduction_qty": None,
        "adoption_id": None,
        "generation": None,
        "provenance": None,
        "entry_context": None,
        "exit_context": None,
        "entry_order_id": None,
        "exit_order_id": None,
        "exit_reason": None,
    }
    defaults.update(values)
    cur.execute(
        """
        INSERT INTO positions(
          id,strategy,status,side,qty,entry_price,exit_price,exit_time,gross_pnl_usdc,
          fees_usdc,net_pnl_usdc,inventory_evidence_status,
          remaining_inventory_qty,gross_entry_executed_qty,
          net_entry_inventory_qty,cumulative_exit_executed_qty,
          exit_inventory_reduction_qty,inventory_contract_adoption_id,
          inventory_contract_generation,
          entry_context_json,exit_context_json,entry_order_id,exit_order_id,
          exit_reason
        ) VALUES (
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
          %s,%s,%s,%s,%s,%s
        )
        """,
        (
            position_id,
            defaults["strategy"],
            defaults["status"],
            defaults["side"],
            defaults["qty"],
            defaults["entry_price"],
            defaults["exit_price"],
            defaults["exit_time"],
            defaults["gross"],
            defaults["fees"],
            defaults["net"],
            defaults["evidence"],
            defaults["remaining"],
            defaults["gross_entry_qty"],
            defaults["net_entry_qty"],
            defaults["cumulative_exit_qty"],
            defaults["exit_reduction_qty"],
            defaults["adoption_id"],
            defaults["generation"],
            defaults["entry_context"],
            defaults["exit_context"] or (
                '{"outcome_provenance":"CLOSED_OUTCOME_V1"}'
                if defaults["provenance"] == "CLOSED_OUTCOME_V1"
                else None
            ),
            defaults["entry_order_id"],
            defaults["exit_order_id"],
            defaults["exit_reason"],
        ),
    )


def _fill(
    cur, position_id, purpose, notional, fee="0.10", index=0, qty="1",
    side=None, *, price=None, fill_id=None, simulated_order_id=None,
    symbol="BTCUSDC", source_fingerprint=None,
    source_authority="SIMULATED_EXECUTION", environment="paper",
    deployment_id="local-paper",
    simulation_model_version="PAPER_SIMULATOR_FINANCIAL_MODEL_V1",
):
    cur.execute(
        """
        INSERT INTO simulated_execution_fills_v1(
          id,simulated_order_id,position_id,fill_index,order_purpose,side,symbol,
          fill_qty,fill_price,fill_notional,fee_qty,fee_asset,
          authoritative_fee_usdc,estimated_fee_usdc,source_authority,
          environment,deployment_id,simulation_model_version,source_fingerprint
        ) VALUES (
          COALESCE(%s,nextval(pg_get_serial_sequence(
            'simulated_execution_fills_v1','id'
          ))),
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'USDC',%s,NULL,%s,%s,%s,%s,%s
        )
        """,
        (
            fill_id,
            simulated_order_id or (position_id * 10 + index),
            position_id,
            index,
            purpose,
            side or ("BUY" if purpose == "ENTRY" else "SELL"),
            symbol,
            qty,
            (
                price if price is not None
                else Decimal(str(notional)) / Decimal(str(qty))
            ),
            notional,
            fee,
            fee,
            source_authority,
            environment,
            deployment_id,
            simulation_model_version,
            source_fingerprint or f"fill-{position_id}-{purpose}-{index}",
        ),
    )


def _terminal_close(
    cur, position_id, order_id, *, kind="POSITION_CLOSED",
    execution_source=None,
):
    cur.execute(
        """
        INSERT INTO position_lifecycle_events_c2_2(
          position_id,order_id,mutation_kind,mutation_high_water,payload
        ) VALUES (
          %s,%s,%s,0,
          jsonb_build_object('execution_source', %s::text)
        )
        """,
        (position_id, str(order_id), kind, execution_source),
    )


def _paper_adoption(
    cur, adoption_id, *, generation=2, deployment_id="local-paper",
):
    cur.execute(
        """
        INSERT INTO runtime_contract_adoption_v2(
          adoption_id,contract_name,environment,deployment_id,generation,status
        ) VALUES (
          %s,'FEE_AWARE_INVENTORY_C2_2','paper',%s,%s,'ACTIVE'
        )
        ON CONFLICT (adoption_id) DO NOTHING
        """,
        (adoption_id, deployment_id, generation),
    )


def _vps_6456(cur):
    _paper_adoption(cur, 2, deployment_id="vps-paper")
    _position(
        cur,
        6456,
        strategy="SUPERTREND",
        qty="0",
        entry_price="62676.7",
        exit_price="62625.4",
        gross="0.00000000",
        fees="0.00000000",
        net="0.00000000",
        evidence="COMPLETE",
        remaining="0",
        gross_entry_qty="0.00031909",
        net_entry_qty="0.00031909",
        cumulative_exit_qty="0.00031909",
        exit_reduction_qty="0.00031909",
        adoption_id=2,
        generation=2,
        entry_context=None,
        exit_context=None,
    )
    _fill(
        cur,
        6456,
        "ENTRY",
        "19.999508203",
        fee="0.0079998032812",
        qty="0.00031909",
        price="62676.7",
        fill_id=361,
        simulated_order_id=25688,
        deployment_id="vps-paper",
        source_fingerprint="vps-6456-entry-fill-361",
    )
    _fill(
        cur,
        6456,
        "EXIT",
        "19.983138886",
        fee="0.0079932555544",
        index=1,
        qty="0.00031909",
        price="62625.4",
        fill_id=363,
        simulated_order_id=25786,
        deployment_id="vps-paper",
        source_fingerprint="vps-6456-exit-fill-363",
    )
    _terminal_close(
        cur, 6456, 25786, execution_source="PAPER_SIMULATED",
    )


def _financial_truth(
    cur, position_id, *, gross, fees, net, remaining="0", status="COMPLETE",
):
    cur.execute(
        """
        INSERT INTO canonical_financial_truth_v1(
          position_id,financial_truth_status,authoritative_gross_pnl,
          authoritative_net_pnl,authoritative_fees_usdc,
          entry_fill_count,exit_fill_count,executed_entry_qty,executed_exit_qty,
          remaining_inventory_qty,remaining_qty,source_fingerprint,
          source_order_ids,source_fill_ids,calculation_version
        ) VALUES (%s,%s,%s,%s,%s,1,1,1,1,%s,%s,%s,%s::jsonb,%s::jsonb,
          'FINANCIAL_TRUTH_CALCULATION_V1')
        """,
        (
            position_id, status, gross, net, fees, remaining, remaining,
            f"fingerprint-{position_id}",
            f'["entry-{position_id}","exit-{position_id}"]',
            f'["fill-entry-{position_id}","fill-exit-{position_id}"]',
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
            _fill(cur, 4, "ENTRY", "10", side="SELL")
            _fill(cur, 4, "EXIT", "12", index=1, side="BUY")
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
            _financial_truth(cur, 10, gross="3", fees="0.5", net="2.5")
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
            cur.execute(build_closed_outcome_rows_sql("PAPER"), params)
            rows = {row[0]: row for row in cur.fetchall()}

            assert set(rows) == set(range(1, 11)) | {14}
            assert rows[1][2:7] == (
                "VERIFIED_LEGACY_STORED", Decimal("2"), Decimal("0.2"),
                Decimal("1.8"), "WIN",
            )
            assert rows[2][2] == "VERIFIED_LEGACY_STORED"
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
                cur, environment="PAPER", window_start=START, window_end=END
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
                "VERIFIED_LEGACY_STORED": 2,
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
                cur, environment="PAPER", window_start=START, window_end=END
            )
        assert summary["trades"] == 1
        assert summary["wins"] == 1
        assert summary["net_pnl"] == Decimal("1.80")
    finally:
        conn.close()


def test_paper_administrative_retirement_is_performance_excluded_but_account_visible(
    disposable_postgres_v16,
):
    conn = _database(disposable_postgres_v16, "administrative_retirement")
    try:
        with conn.cursor() as cur:
            _position(
                cur, 21, qty="0", evidence="COMPLETE", remaining="0",
                entry_order_id="210", exit_order_id="211",
                exit_reason="LEGACY_ADMINISTRATIVE_CLOSE",
            )
            _fill(
                cur, 21, "ENTRY", "10", fee="0.10", index=0,
                simulated_order_id=210,
            )
            _fill(
                cur, 21, "EXIT", "11", fee="0.10", index=1,
                simulated_order_id=211,
            )
            _terminal_close(
                cur, 21, 211, execution_source="PAPER_SIMULATED"
            )
            conn.commit()
            performance = fetch_closed_outcome_summary(
                cur, environment="PAPER", window_start=START, window_end=END
            )
            account = fetch_closed_outcome_summary(
                cur, environment="PAPER", window_start=START, window_end=END,
                include_administrative_retirements=True,
            )
        assert performance["trades"] == 0
        assert performance["wins"] == 0
        assert performance["net_pnl"] is None
        assert account["trades"] == 1
        assert account["wins"] == 1
        assert account["net_pnl"] == Decimal("0.80")
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
            cur.execute(build_closed_outcome_rows_sql("LIVE"), {
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
                cur, environment="PAPER", window_start=START, window_end=END,
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


def test_environment_isolation_and_paper_complexity_gate(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16, "environment_isolation")
    try:
        with conn.cursor() as cur:
            _position(cur, 5000)
            _fill(cur, 5000, "ENTRY", "10")
            _fill(cur, 5000, "EXIT", "12", index=1)
            _legacy_fill(cur, 5000, "ENTRY", "1", "10", "0.10")
            _legacy_fill(cur, 5000, "EXIT", "1", "9", "0.10")
            cur.execute(
                """
                INSERT INTO positions(id,status,side,qty,exit_time)
                SELECT 10000 + n, 'CLOSED', 'LONG', 0, %s
                FROM generate_series(1, 10000) n
                """,
                (START,),
            )
            cur.execute(
                """
                INSERT INTO binance_order_fills(
                  source,trade_id,order_id,symbol,side,executed_qty,
                  quote_notional_usdc,commission_amount,commission_asset,commission_usdc
                )
                SELECT 'okx', 'noise-' || n, 'unrelated-' || n,
                  'BTCUSDC', 'BUY', 1, 10, 0.1, 'USDC', 0.1
                FROM generate_series(1, 20000) n
                """
            )
            conn.commit()

            params = {"window_start": START, "window_end": END}
            cur.execute("SET LOCAL jit = off")
            started = monotonic()
            cur.execute(build_closed_outcome_rows_sql("PAPER"), params)
            paper_rows = {row[0]: row for row in cur.fetchall()}
            assert monotonic() - started < 2
            cur.execute(build_closed_outcome_rows_sql("LIVE"), params)
            live_rows = {row[0]: row for row in cur.fetchall()}

            assert paper_rows[5000][2] == "PAPER_SIMULATED_FILLS"
            assert paper_rows[5000][6] == "WIN"
            assert live_rows[5000][2] == "LEGACY_EXECUTION_PROVEN"
            assert live_rows[5000][6] == "LOSS"

            cur.execute(
                "EXPLAIN (FORMAT JSON) " + build_closed_outcome_rows_sql("PAPER"),
                params,
            )
            assert "binance_order_fills" not in str(cur.fetchone()[0])
    finally:
        conn.close()


def _paper_fixture_with_components(
    cur, position_ids, stored_nets, resolved_nets, stored_fees, resolved_fees,
    *, exit_context=None,
):
    for position_id, stored_net, resolved_net, stored_fee, resolved_fee in zip(
        position_ids, stored_nets, resolved_nets, stored_fees, resolved_fees,
    ):
        _position(
            cur, position_id, gross=str(stored_net + stored_fee),
            fees=str(stored_fee), net=str(stored_net),
            provenance="CLOSED_OUTCOME_V1", exit_context=exit_context,
        )
        half_fee = resolved_fee / Decimal("2")
        _fill(cur, position_id, "ENTRY", "20", fee=str(half_fee))
        _fill(
            cur, position_id, "EXIT",
            str(Decimal("20") + resolved_net + resolved_fee),
            fee=str(half_fee), index=1,
        )


def test_vps_21_component_rounding_shapes_are_non_material(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16, "vps_21_component_rounding")
    position_ids = [
        6286, 6292, 6300, 6304, 6307, 6329, 6330, 6331, 6346, 6354,
        6366, 6370, 6376, 6377, 6380, 6400, 6435, 6439, 6443, 6446, 6449,
    ]
    try:
        with conn.cursor() as cur:
            _paper_fixture_with_components(
                cur,
                position_ids,
                [Decimal("0.90000000")] * 21,
                [Decimal("0.899999992")] * 21,
                [Decimal("0.10000000")] * 21,
                [Decimal("0.100000004")] * 21,
                exit_context=(
                    '{"outcome_provenance":"UNKNOWN",'
                    '"fee_model":"UNKNOWN",'
                    '"rounding_policy_hypothesis":"LIKELY_PER_FILL_ROUNDING"}'
                ),
            )
            conn.commit()
            rows = fetch_closed_outcomes(
                cur, environment="PAPER", window_start=START, window_end=END,
            )
            summary = fetch_closed_outcome_summary(
                cur, environment="PAPER", window_start=START, window_end=END,
            )
        assert set(rows) == set(position_ids)
        assert all(row["outcome_source"] == "PAPER_SIMULATED_FILLS" for row in rows.values())
        assert all(row["evidence_complete"] for row in rows.values())
        assert all(row["normalization_status"] == "COMPONENT_ROUNDING_ACCUMULATION" for row in rows.values())
        assert all(row["gross_delta"] == Decimal("0.000000004") for row in rows.values())
        assert all(row["fee_delta"] == Decimal("-0.000000004") for row in rows.values())
        assert all(row["net_delta"] == Decimal("0.000000008") for row in rows.values())
        assert all(row["reconstructed_net_delta"] == row["net_delta"] for row in rows.values())
        assert all(row["legacy_stored_provenance"] == "UNKNOWN" for row in rows.values())
        assert all(row["legacy_fee_model"] == "UNKNOWN" for row in rows.values())
        assert summary["component_rounding_accumulation_count"] == 21
        assert summary["material_conflict_count"] == 0
        assert summary["aggregate_normalization_status"] == "NON_MATERIAL_NORMALIZATION"
        assert all(row["selected_source_confidence"] == "HIGH_ASSURANCE" for row in rows.values())
        assert all(row["rollout_impact"] == "NON_BLOCKING_COMPONENT_ROUNDING" for row in rows.values())
        assert summary["blocking_conflict_count"] == 0
        assert summary["rollout_gate_status"] == "PASS"
    finally:
        conn.close()


def test_vps_37_production_shape_prefers_fill_precision(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16, "vps_37_production")
    stored_nets = [Decimal("0.04000000")] * 28 + [Decimal("-0.04000000")] * 8 + [Decimal("-0.06620976")]
    deltas = [Decimal("-0.000000008")] * 3 + [Decimal("0")]
    deltas += [Decimal("0.000000000293793939")] * 32
    deltas += [Decimal("0.000000000293793952")]
    resolved_nets = [stored + delta for stored, delta in zip(stored_nets, deltas)]
    stored_fees = [Decimal("0.01601358")] * 37
    resolved_fees = [Decimal("0.016013584")] * 3 + [Decimal("0.01601358")]
    regular_fee = Decimal("0.016013577403175")
    resolved_fees += [regular_fee] * 32
    resolved_fees += [Decimal("0.5925023863048") - sum(resolved_fees)]
    try:
        with conn.cursor() as cur:
            _paper_fixture_with_components(
                cur, range(50000, 50037), stored_nets, resolved_nets,
                stored_fees, resolved_fees,
            )
            conn.commit()
            rows = fetch_closed_outcomes(
                cur, environment="PAPER", window_start=START, window_end=END,
            )
            summary = fetch_closed_outcome_summary(
                cur, environment="PAPER", window_start=START, window_end=END,
            )
        assert sum(stored_nets) == Decimal("0.7337902400000")
        assert summary["net_pnl"] == Decimal("0.7337902256952")
        assert summary["gross_pnl"] == Decimal("1.326292612")
        assert summary["fees"] == Decimal("0.5925023863048")
        assert summary["normalization_delta"] == Decimal("-0.0000000143048")
        assert summary["aggregate_normalization_status"] == "NON_MATERIAL_NORMALIZATION"
        assert summary["normalization_status_counts"] == {
            "COMPONENT_ROUNDING_ACCUMULATION": 3,
            "EXACT_MATCH": 1,
            "ROUNDING_ONLY": 33,
        }
        assert summary["material_conflict_count"] == 0
        assert summary["blocking_conflict_count"] == 0
        assert summary["rollout_gate_status"] == "PASS"
        assert summary["outcome_source_counts"] == {"PAPER_SIMULATED_FILLS": 37}
        assert (summary["wins"], summary["losses"], summary["flats"]) == (28, 9, 0)
        assert all(row["normalization_status"] != "MATERIAL_CONFLICT" for row in rows.values())
        assert all(row["calculation_version"] == "CLOSED_OUTCOME_PAPER_V2" for row in rows.values())
        assert all(row["stored_scale"] is not None for row in rows.values())
        assert all(row["fill_scale"] is not None for row in rows.values())
        assert all(row["legacy_stored_provenance"] == "CLOSED_OUTCOME_V1" for row in rows.values())
        assert sum(row["normalization_stored_value"] for row in rows.values()) == Decimal("0.7337902400000")
        assert sum(row["normalization_resolved_value"] for row in rows.values()) == Decimal("0.7337902256952")
    finally:
        conn.close()


def test_vps_current_33_production_shape(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16, "vps_33_current")
    stored_nets = [Decimal("0.01000000")] * 23 + [Decimal("-0.02000000")] * 9 + [Decimal("-0.06722983")]
    deltas = [Decimal("-0.000000008")] * 5 + [Decimal("0")]
    regular_delta = Decimal("0.000000000723798557580")
    deltas += [regular_delta] * 26
    deltas += [Decimal("-0.00000002045743894532204") - sum(deltas)]
    resolved_nets = [stored + delta for stored, delta in zip(stored_nets, deltas)]
    stored_fees = [Decimal("0.01000000")] * 33
    resolved_fees = [Decimal("0.010000004")] * 5 + [Decimal("0.01000000")]
    resolved_fees += [Decimal("0.009999998")] * 27
    try:
        with conn.cursor() as cur:
            _paper_fixture_with_components(
                cur, range(51000, 51033), stored_nets, resolved_nets,
                stored_fees, resolved_fees,
            )
            conn.commit()
            summary = fetch_closed_outcome_summary(
                cur, environment="PAPER", window_start=START, window_end=END,
            )
        assert sum(stored_nets) == Decimal("-0.01722983")
        assert summary["net_pnl"] == Decimal("-0.01722985045743894532204")
        assert summary["aggregate_normalization_status"] == "NON_MATERIAL_NORMALIZATION"
        assert summary["component_rounding_accumulation_count"] == 5
        assert summary["material_conflict_count"] == 0
        assert summary["blocking_conflict_count"] == 0
        assert summary["rollout_gate_status"] == "PASS"
        assert (summary["wins"], summary["losses"], summary["flats"]) == (23, 10, 0)
    finally:
        conn.close()


def test_simulated_material_conflict_wins_with_diagnostic(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16, "paper_material_conflict")
    try:
        with conn.cursor() as cur:
            _position(cur, 52000, gross="1.1", fees="0.1", net="1.0")
            _fill(cur, 52000, "ENTRY", "10", fee="0.1")
            _fill(cur, 52000, "EXIT", "12", fee="0.1", index=1)
            _position(cur, 52001, gross="1.1", fees="0.1", net="1.0")
            _fill(cur, 52001, "ENTRY", "10", fee="0.1")
            conn.commit()
            cur.execute(build_closed_outcome_rows_sql("PAPER"), {
                "window_start": START, "window_end": END,
            })
            rows = {row[0]: row for row in cur.fetchall()}
        assert rows[52000][2] == "PAPER_SIMULATED_FILLS"
        assert rows[52000][12] == "MATERIAL_CONFLICT"
        assert rows[52000][11] == "LEGACY_STORED_CONFLICT"
        assert rows[52000][31] == "BLOCKING_AUTHORITATIVE_CONFLICT"
        assert rows[52001][2] == "UNRESOLVED"
        assert "SIMULATED_EVIDENCE_INCOMPLETE" in rows[52001][9]
        assert rows[52001][12] == "SOURCE_NOT_COMPARABLE"
    finally:
        conn.close()


def test_vps_6456_zero_placeholder_is_superseded_by_complete_simulated_fills(
    disposable_postgres_v16,
):
    conn = _database(disposable_postgres_v16, "vps_6456_supersession")
    try:
        with conn.cursor() as cur:
            _vps_6456(cur)
            conn.commit()
            rows = fetch_closed_outcomes(
                cur, environment="PAPER", window_start=START, window_end=END,
            )
            summary = fetch_closed_outcome_summary(
                cur, environment="PAPER", window_start=START, window_end=END,
            )

        row = rows[6456]
        assert row["position_order_linkage_status"] == (
            "DERIVED_UNIQUE_FILL_LIFECYCLE_LINKAGE"
        )
        assert row["derived_entry_order_id"] == 25688
        assert row["derived_exit_order_id"] == 25786
        assert row["outcome_source"] == "PAPER_SIMULATED_FILLS"
        assert row["selected_source_confidence"] == "HIGH_ASSURANCE"
        assert row["normalization_status"] == "MATERIAL_CONFLICT"
        assert row["rollout_impact"] == "NON_BLOCKING_SOURCE_SUPERSEDED"
        assert row["source_superseded_reason"] == (
            "HIGH_ASSURANCE_SIMULATED_FILLS_SUPERSEDE_"
            "UNTRUSTED_STORED_ZERO_PLACEHOLDER_WITH_DERIVED_ORDER_LINKAGE"
        )
        assert row["gross_pnl_usdc"] == Decimal("-0.016369317")
        assert row["fees_usdc"] == Decimal("0.0159930588356")
        assert row["net_pnl_usdc"] == Decimal("-0.0323623758356")
        assert row["result_class"] == "LOSS"

        assert summary["trades"] == summary["resolved_trades"] == 1
        assert summary["coverage_ratio"] == Decimal("1")
        assert summary["outcome_source_counts"] == {
            "PAPER_SIMULATED_FILLS": 1,
        }
        assert summary["normalization_status_counts"] == {
            "MATERIAL_CONFLICT": 1,
        }
        assert summary["blocking_conflict_count"] == 0
        assert summary["superseded_conflict_count"] == 1
        assert summary["not_evaluable_count"] == 0
        assert summary["rollout_gate_status"] == "PASS"
        assert summary["rollout_impact_counts"] == {
            "NON_BLOCKING_SOURCE_SUPERSEDED": 1,
        }
    finally:
        conn.close()


def test_order_linkage_classification_and_ambiguity_matrix(
    disposable_postgres_v16,
):
    conn = _database(disposable_postgres_v16, "order_linkage_matrix")

    def seed(
        cur, position_id, *, entry_orders, exit_orders,
        position_entry_id=None, position_exit_id=None, lifecycle_order=None,
    ):
        _position(
            cur, position_id, gross="0", fees="0", net="0",
            evidence="COMPLETE", remaining="0", adoption_id=20,
            generation=2, entry_order_id=position_entry_id,
            exit_order_id=position_exit_id,
        )
        entry_count = Decimal(len(entry_orders))
        exit_count = Decimal(len(exit_orders))
        for index, order_id in enumerate(entry_orders):
            _fill(
                cur, position_id, "ENTRY", str(Decimal("10") / entry_count),
                fee=str(Decimal("0.1") / entry_count),
                qty=str(Decimal("1") / entry_count), index=index,
                simulated_order_id=order_id,
            )
        for offset, order_id in enumerate(exit_orders, start=100):
            _fill(
                cur, position_id, "EXIT", str(Decimal("12") / exit_count),
                fee=str(Decimal("0.1") / exit_count),
                qty=str(Decimal("1") / exit_count), index=offset,
                simulated_order_id=order_id,
            )
        _terminal_close(
            cur, position_id,
            lifecycle_order if lifecycle_order is not None else exit_orders[0],
            execution_source="PAPER_SIMULATED",
        )

    try:
        with conn.cursor() as cur:
            _paper_adoption(cur, 20)
            seed(cur, 55000, entry_orders=[550000], exit_orders=[550001])
            seed(
                cur, 55001, entry_orders=[550010, 550012],
                exit_orders=[550011],
            )
            seed(
                cur, 55002, entry_orders=[550020],
                exit_orders=[550021, 550022],
            )
            seed(
                cur, 55003, entry_orders=[550030], exit_orders=[550031],
                lifecycle_order=559999,
            )
            seed(
                cur, 55004, entry_orders=[550040], exit_orders=[550041],
                position_entry_id="550040", position_exit_id="550041",
            )
            seed(
                cur, 55005, entry_orders=[550050], exit_orders=[550051],
                position_entry_id="550059", position_exit_id="550051",
            )
            seed(
                cur, 55006, entry_orders=[550060], exit_orders=[550061],
                position_exit_id="550061",
            )
            seed(
                cur, 55007, entry_orders=[550070], exit_orders=[550071],
                position_entry_id="550070",
            )
            seed(
                cur, 55008, entry_orders=[550080], exit_orders=[550081],
            )
            _position(cur, 55009)
            _fill(
                cur, 55009, "ENTRY", "10", simulated_order_id=550080,
                index=9,
            )
            conn.commit()
            rows = fetch_closed_outcomes(
                cur, environment="PAPER", window_start=START, window_end=END,
            )

        assert rows[55000]["position_order_linkage_status"] == (
            "DERIVED_UNIQUE_FILL_LIFECYCLE_LINKAGE"
        )
        assert rows[55000]["rollout_impact"] == (
            "NON_BLOCKING_SOURCE_SUPERSEDED"
        )
        for position_id in (55001, 55002):
            assert rows[position_id]["position_order_linkage_status"] == (
                "AMBIGUOUS_ORDER_LINKAGE"
            )
            assert rows[position_id]["rollout_impact"] == (
                "BLOCKING_AUTHORITATIVE_CONFLICT"
            )
        assert rows[55003]["position_order_linkage_status"] == (
            "CONFLICTING_ORDER_LINKAGE"
        )
        assert rows[55003]["rollout_impact"] == (
            "BLOCKING_AUTHORITATIVE_CONFLICT"
        )
        assert rows[55004]["position_order_linkage_status"] == (
            "EXPLICIT_POSITION_ORDER_LINKAGE"
        )
        assert rows[55004]["rollout_impact"] == (
            "NON_BLOCKING_SOURCE_SUPERSEDED"
        )
        assert rows[55005]["position_order_linkage_status"] == (
            "CONFLICTING_ORDER_LINKAGE"
        )
        assert rows[55005]["rollout_impact"] == (
            "BLOCKING_AUTHORITATIVE_CONFLICT"
        )
        for position_id in (55006, 55007):
            assert rows[position_id]["position_order_linkage_status"] == (
                "MISSING_ORDER_LINKAGE"
            )
            assert rows[position_id]["rollout_impact"] == (
                "BLOCKING_AUTHORITATIVE_CONFLICT"
            )
            assert rows[position_id]["source_superseded_reason"] is None
        assert rows[55008]["position_order_linkage_status"] == (
            "CONFLICTING_ORDER_LINKAGE"
        )
        assert rows[55008]["rollout_impact"] == (
            "BLOCKING_EVIDENCE_INCONSISTENT"
        )
        assert rows[55008]["source_superseded_reason"] is None
    finally:
        conn.close()


def test_simulated_zero_supersession_negative_authority_matrix(
    disposable_postgres_v16,
):
    conn = _database(disposable_postgres_v16, "zero_supersession_negative")
    trusted_context = (
        '{"outcome_provenance":"FINANCIAL_TRUTH",'
        '"calculation_version":"TRUSTED_V1",'
        '"source_fingerprint":"trusted-stored-evidence"}'
    )
    try:
        with conn.cursor() as cur:
            for position_id, stored, context in (
                (54000, ("0", "0", "0"), trusted_context),
                (54003, ("1", "0.1", "0.9"), trusted_context),
                (54004, ("1", "0.1", "0.9"), None),
            ):
                _position(
                    cur, position_id, gross=stored[0], fees=stored[1],
                    net=stored[2], evidence="COMPLETE", remaining="0",
                    exit_context=context,
                    entry_order_id=str(position_id * 10),
                    exit_order_id=str(position_id * 10 + 1),
                )
                _fill(cur, position_id, "ENTRY", "10", fee="0.1")
                _fill(
                    cur, position_id, "EXIT", "12", fee="0.1", index=1,
                )
                _terminal_close(cur, position_id, position_id * 10 + 1)

            _position(
                cur, 54001, gross="0", fees="0", net="0",
                evidence="COMPLETE", remaining="0",
                entry_order_id="540010", exit_order_id="540011",
            )
            _fill(cur, 54001, "ENTRY", "10", fee="0.1")
            _terminal_close(cur, 54001, 540011)

            _position(
                cur, 54002, gross="0", fees="0", net="0",
                evidence="COMPLETE", remaining="0",
                entry_order_id="540020", exit_order_id="540021",
            )
            _fill(cur, 54002, "ENTRY", "10", fee="0.1")
            _fill(cur, 54002, "EXIT", "12", fee="0.1", index=1)
            _terminal_close(cur, 54002, "540021")
            cur.execute(
                "INSERT INTO exchange_fill_ingestion_state_v2 VALUES "
                "('540020','CORRECTION_PENDING')"
            )

            for position_id, evidence, remaining, add_lifecycle in (
                (54005, "COMPLETE", "0", False),
                (54006, "COMPLETE", "0.0001", True),
                (54007, "INCOMPLETE", "0", True),
            ):
                _position(
                    cur, position_id, gross="0", fees="0", net="0",
                    evidence=evidence, remaining=remaining,
                    entry_order_id=str(position_id * 10),
                    exit_order_id=str(position_id * 10 + 1),
                )
                _fill(cur, position_id, "ENTRY", "10", fee="0.1")
                _fill(
                    cur, position_id, "EXIT", "12", fee="0.1", index=1,
                )
                if add_lifecycle:
                    _terminal_close(cur, position_id, position_id * 10 + 1)

            _position(
                cur, 54008, gross="0", fees="0", net="0",
                evidence="COMPLETE", remaining="0",
                entry_order_id="540080", exit_order_id="540081",
            )
            _fill(
                cur, 54008, "ENTRY", "10", fee="0.1",
                source_fingerprint="duplicate-source-fingerprint",
            )
            _fill(
                cur, 54008, "EXIT", "12", fee="0.1", index=1,
                source_fingerprint="duplicate-source-fingerprint",
            )
            _terminal_close(cur, 54008, 540081)

            _position(
                cur, 54009, gross="0", fees="0", net="0",
                evidence="COMPLETE", remaining="0",
                entry_order_id="540090", exit_order_id="540091",
            )
            _fill(cur, 54009, "ENTRY", "10", fee="0.1")
            _fill(
                cur, 54009, "EXIT", "12", fee="0.1", index=1,
                symbol="ETHUSDC",
            )
            _terminal_close(cur, 54009, 540091)

            _position(
                cur, 54010, gross="0", fees="0", net="0",
                evidence="COMPLETE", remaining="0",
                entry_order_id="540100", exit_order_id="540101",
            )
            _fill(cur, 54010, "ENTRY", "10", fee=None)
            _fill(cur, 54010, "EXIT", "12", fee="0.1", index=1)
            _terminal_close(cur, 54010, 540101)

            _position(
                cur, 54011, gross="0", fees="0", net="0",
                evidence="COMPLETE", remaining="0",
                entry_order_id="540110", exit_order_id="540111",
            )
            _fill(cur, 54011, "ENTRY", "10", fee="0.1", qty="2")
            _fill(cur, 54011, "EXIT", "12", fee="0.1", index=1, qty="1")
            _terminal_close(cur, 54011, 540111)

            _position(
                cur, 54012, gross="0", fees="0", net="0",
                evidence="COMPLETE", remaining="0",
                entry_context=trusted_context,
                exit_context='{"outcome_provenance":"UNKNOWN"}',
                entry_order_id="540120", exit_order_id="540121",
            )
            _fill(cur, 54012, "ENTRY", "10", fee="0.1")
            _fill(cur, 54012, "EXIT", "12", fee="0.1", index=1)
            _terminal_close(cur, 54012, 540121)

            _position(
                cur, 54013, gross="0", fees="0", net="0",
                evidence="COMPLETE", remaining="0",
                entry_order_id="540130", exit_order_id="540131",
            )
            _fill(cur, 54013, "ENTRY", "10", fee="0.1")
            _fill(cur, 54013, "EXIT", "12", fee="0.1", index=1)
            _terminal_close(cur, 54013, 540131)
            cur.execute(
                "INSERT INTO exchange_fill_ingestion_state_v2 VALUES "
                "('540130','IDEMPOTENCY_CONFLICT')"
            )

            conn.commit()
            rows = fetch_closed_outcomes(
                cur, environment="PAPER", window_start=START, window_end=END,
            )

        assert rows[54000]["rollout_impact"] == (
            "BLOCKING_AUTHORITATIVE_CONFLICT"
        )
        assert rows[54001]["rollout_impact"] == "NOT_EVALUABLE"
        assert rows[54002]["rollout_impact"] == (
            "BLOCKING_EVIDENCE_INCONSISTENT"
        )
        assert rows[54003]["rollout_impact"] == (
            "BLOCKING_AUTHORITATIVE_CONFLICT"
        )
        assert rows[54004]["rollout_impact"] == (
            "BLOCKING_AUTHORITATIVE_CONFLICT"
        )
        assert rows[54012]["rollout_impact"] == (
            "BLOCKING_AUTHORITATIVE_CONFLICT"
        )
        assert rows[54013]["rollout_impact"] == (
            "BLOCKING_EVIDENCE_INCONSISTENT"
        )
        for position_id in (54005, 54006, 54007, 54010, 54011):
            assert rows[position_id]["rollout_impact"] in {
                "BLOCKING_AUTHORITATIVE_CONFLICT",
                "BLOCKING_EVIDENCE_INCONSISTENT",
                "NOT_EVALUABLE",
            }
        for position_id in (54008, 54009):
            assert rows[position_id]["rollout_impact"] == (
                "BLOCKING_EVIDENCE_INCONSISTENT"
            )
        assert all(
            row["source_superseded_reason"] is None for row in rows.values()
        )
    finally:
        conn.close()


def test_ft_rollout_impact_separates_superseded_and_blocking_conflicts(
    disposable_postgres_v16,
):
    conn = _database(disposable_postgres_v16, "ft_rollout_impact")
    golden_nets = [
        Decimal("-0.00548696512"), Decimal("-0.007031578824"),
        Decimal("0.0098272733136"), Decimal("-0.02408621660"),
        Decimal("-0.12287960864"), Decimal("-0.1496637480680"),
        Decimal("0.149969550352"), Decimal("0.15402325488"),
        Decimal("0.08185227072"), Decimal("0.095667336348"),
        Decimal("-0.095007485628"), Decimal("-0.0939109786728"),
    ]
    ids = [10355, 10356, 10357, 10358, 10372, 10374,
           10376, 10377, 10391, 10392, 10418, 10420]
    try:
        with conn.cursor() as cur:
            for position_id, net in zip(ids, golden_nets):
                _position(
                    cur, position_id, strategy="SUPERTREND",
                    gross="0.00000000", fees="0.00000000", net="0.00000000",
                )
                _financial_truth(
                    cur, position_id, gross=str(net + Decimal("0.01")),
                    fees="0.01", net=str(net),
                )
                _fill(cur, position_id, "ENTRY", "20", fee="0.005")
                _fill(
                    cur, position_id, "EXIT", str(Decimal("20") + net + Decimal("0.01")),
                    fee="0.005", index=1,
                )

            trusted_context = (
                '{"outcome_provenance":"FINANCIAL_TRUTH",'
                '"calculation_version":"TRUSTED_V1",'
                '"source_fingerprint":"stored-frozen"}'
            )
            _position(
                cur, 53000, gross="0", fees="0", net="0",
                exit_context=trusted_context,
            )
            _financial_truth(cur, 53000, gross="0", fees="0", net="0")

            _position(
                cur, 53001, gross="0", fees="0", net="0",
                exit_context=trusted_context,
            )
            _financial_truth(cur, 53001, gross="1", fees="0.1", net="0.9")

            _position(cur, 53002, gross="0", fees="0", net="0")
            _financial_truth(
                cur, 53002, gross="1", fees="0.1", net="0.9", remaining="0.1",
            )
            conn.commit()
            rows = fetch_closed_outcomes(
                cur, environment="PAPER", window_start=START, window_end=END,
            )
            superseded_summary = fetch_closed_outcome_summary(
                cur, environment="PAPER", window_start=START, window_end=END,
            )

        golden = [rows[position_id] for position_id in ids]
        assert all(row["outcome_source"] == "FINANCIAL_TRUTH" for row in golden)
        assert all(row["selected_source_confidence"] == "AUTHORITATIVE" for row in golden)
        assert all(row["normalization_status"] == "MATERIAL_CONFLICT" for row in golden)
        assert all(row["rollout_impact"] == "NON_BLOCKING_SOURCE_SUPERSEDED" for row in golden)
        assert all(row["source_superseded_reason"] for row in golden)
        assert (sum(row["result_class"] == "WIN" for row in golden),
                sum(row["result_class"] == "LOSS" for row in golden)) == (5, 7)
        assert sum(row["net_pnl_usdc"] for row in golden) == Decimal("-0.0067268959392")

        assert rows[53000]["rollout_impact"] == "NON_BLOCKING_EXACT"
        assert rows[53000]["source_superseded_reason"] is None
        assert rows[53001]["comparison_source_confidence"] == "AUTHORITATIVE"
        assert rows[53001]["rollout_impact"] == "BLOCKING_AUTHORITATIVE_CONFLICT"
        assert rows[53002]["rollout_impact"] == "BLOCKING_EVIDENCE_INCONSISTENT"
        assert superseded_summary["superseded_conflict_count"] == 12
        assert superseded_summary["authoritative_conflict_count"] == 1
        assert superseded_summary["evidence_inconsistent_count"] == 1
        assert superseded_summary["blocking_conflict_count"] == 2
        assert superseded_summary["rollout_gate_status"] == "BLOCKED"
    finally:
        conn.close()


def test_vps_mixed_quality_all_history_coverage(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16, "vps_mixed_coverage")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO positions(
                  id,status,side,qty,entry_price,exit_price,exit_time,
                  gross_pnl_usdc,fees_usdc,net_pnl_usdc
                )
                SELECT 60000+n,'CLOSED','LONG',0,10,11,%s,0.02,0.01,0.01
                FROM generate_series(1,6271) n
                """, (START,),
            )
            for position_id in range(70001, 70166):
                _position(cur, position_id)
                _fill(cur, position_id, "ENTRY", "10", fee="0.1")
                _fill(cur, position_id, "EXIT", "11", fee="0.1", index=1)
            for position_id in range(80001, 80004):
                _position(cur, position_id, entry_price=None, exit_price=None)
            conn.commit()
            summary = fetch_closed_outcome_summary(
                cur, environment="PAPER", window_start=START, window_end=END,
            )
        assert summary["trades"] == 6439
        assert summary["resolved_trades"] == 6436
        assert summary["unresolved_trades"] == 3
        assert summary["high_assurance_count"] == 165
        assert summary["legacy_compatible_count"] == 6271
        assert summary["component_rounding_accumulation_count"] == 0
        assert summary["material_conflict_count"] == 0
        assert summary["outcome_source_counts"] == {
            "PAPER_SIMULATED_FILLS": 165,
            "UNRESOLVED": 3,
            "VERIFIED_LEGACY_STORED": 6271,
        }
    finally:
        conn.close()

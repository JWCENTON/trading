from __future__ import annotations

from pathlib import Path

import pytest
from psycopg2.extras import execute_values


ROOT = Path(__file__).resolve().parents[2]
API_SOURCE = (ROOT / "api/main.py").read_text()
HANDLER = API_SOURCE[
    API_SOURCE.index('@app.get("/ui/recent-closed")'):
    API_SOURCE.index('@app.post("/ui/control/panic"')
]


def _production_sql() -> str:
    marker = 'cur.execute("""'
    start = HANDLER.index(marker) + len(marker)
    end = HANDLER.index('""", (TRADING_MODE, limit))', start)
    return HANDLER[start:end]


PRODUCTION_SQL = _production_sql()
GLOBAL_REAL_CTE = """bounded_real_orders AS MATERIALIZED (
                SELECT
                  f.order_id,
                  f.symbol,
                  f.side,
                  SUM(f.executed_qty) AS executed_qty,
                  SUM(f.quote_notional_usdc) AS quote_notional_usdc,
                  CASE
                    WHEN SUM(f.executed_qty) = 0 THEN NULL
                    ELSE SUM(f.quote_notional_usdc) / SUM(f.executed_qty)
                  END AS avg_price,
                  SUM(
                    COALESCE(
                      f.commission_usdc,
                      CASE
                        WHEN f.commission_asset = 'USDC'
                          THEN f.commission_amount
                        WHEN f.commission_asset = 'BNB'
                          AND f.bnbusdc_price IS NOT NULL
                          THEN f.commission_amount * f.bnbusdc_price
                        ELSE 0
                      END
                    )
                  ) AS fee_usdc
                FROM binance_order_fills f
                CROSS JOIN runtime_scope scope
                WHERE scope.trading_mode = 'LIVE'
                GROUP BY f.order_id, f.symbol, f.side
              ),
              """
GLOBAL_PAPER_CTE = """bounded_paper_orders AS MATERIALIZED (
                SELECT
                  o.id::text AS order_id,
                  o.symbol,
                  o.side,
                  o.quantity_btc AS executed_qty,
                  o.price * o.quantity_btc AS quote_notional_usdc,
                  o.price AS avg_price,
                  0::numeric AS fee_usdc
                FROM simulated_orders o
                CROSS JOIN runtime_scope scope
                WHERE scope.trading_mode = 'PAPER'
              ),
              """


def _replace_cte(
    query: str, start_name: str, next_name: str, replacement: str
) -> str:
    start = query.index(f"{start_name} AS MATERIALIZED (")
    end = query.index(f"{next_name} AS MATERIALIZED (", start)
    return query[:start] + replacement + query[end:]


GLOBAL_REFERENCE_SQL = _replace_cte(
    PRODUCTION_SQL,
    "bounded_real_orders",
    "bounded_paper_orders",
    GLOBAL_REAL_CTE,
)
GLOBAL_REFERENCE_SQL = _replace_cte(
    GLOBAL_REFERENCE_SQL,
    "bounded_paper_orders",
    "bounded_execution_orders",
    GLOBAL_PAPER_CTE,
)


def _create_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE positions (
              id BIGINT PRIMARY KEY,
              symbol TEXT NOT NULL,
              interval TEXT NOT NULL,
              strategy TEXT NOT NULL,
              side TEXT,
              status TEXT NOT NULL,
              qty NUMERIC,
              entry_time TIMESTAMPTZ,
              exit_time TIMESTAMPTZ,
              entry_price NUMERIC,
              exit_price NUMERIC,
              entry_order_id TEXT,
              exit_order_id TEXT,
              net_pnl_usdc NUMERIC,
              exit_reason TEXT
            );
            CREATE INDEX ix_positions_closed_exit_time_mfe_snapshot
              ON positions(exit_time DESC) WHERE status='CLOSED';

            CREATE TABLE binance_order_fills (
              id BIGSERIAL PRIMARY KEY,
              order_id TEXT NOT NULL,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              executed_qty NUMERIC NOT NULL,
              quote_notional_usdc NUMERIC NOT NULL,
              commission_usdc NUMERIC,
              commission_asset TEXT,
              commission_amount NUMERIC,
              bnbusdc_price NUMERIC
            );
            CREATE INDEX ix_binance_order_fills_order_id
              ON binance_order_fills(order_id);

            CREATE TABLE simulated_orders (
              id BIGINT PRIMARY KEY,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              quantity_btc NUMERIC NOT NULL,
              price NUMERIC NOT NULL
            );

            CREATE TABLE simulated_execution_fills_v1 (
              id BIGSERIAL PRIMARY KEY,
              position_id BIGINT NOT NULL,
              order_purpose TEXT NOT NULL,
              fill_qty NUMERIC NOT NULL,
              fill_price NUMERIC NOT NULL
            );
            CREATE INDEX ix_simulated_execution_fills_position_v1
              ON simulated_execution_fills_v1(
                position_id, order_purpose, id
              );

            CREATE TABLE automation_kv (
              key TEXT PRIMARY KEY,
              value TEXT
            );
            INSERT INTO automation_kv(key,value)
            VALUES ('FEE_PER_SIDE_PCT','0.00075');
            """
        )
    conn.commit()


def _seed(conn, mode: str):
    sides = ("LONG", "SHORT", "BUY", "SELL")
    rows = []
    for position_id in range(1, 1501):
        side = sides[position_id % len(sides)]
        qty = "0" if position_id == 1500 else "2"
        entry_price = "0" if position_id == 1499 else "10"
        exit_price = None if position_id == 1498 else "12"
        rows.append(
            (
                position_id,
                "BTCUSDC",
                "1m",
                "RSI",
                side,
                "CLOSED",
                qty,
                f"2026-01-01 00:00:00+00",
                f"2026-01-01 00:{position_id // 60:02d}:{position_id % 60:02d}+00",
                entry_price,
                exit_price,
                str(position_id * 2),
                str(position_id * 2 + 1),
                "1.25" if position_id == 1497 else None,
                f"fixture-{position_id}",
            )
        )
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO positions(
              id,symbol,interval,strategy,side,status,qty,
              entry_time,exit_time,entry_price,exit_price,
              entry_order_id,exit_order_id,net_pnl_usdc,exit_reason
            ) VALUES %s
            """,
            rows,
        )

        execute_values(
            cur,
            """
            INSERT INTO binance_order_fills(
              order_id,symbol,side,executed_qty,quote_notional_usdc,
              commission_usdc,commission_asset,commission_amount,bnbusdc_price
            ) VALUES %s
            """,
            [
                (
                    f"noise-{index}",
                    "ETHUSDC",
                    "BUY",
                    "1",
                    "10",
                    "0.01",
                    "USDC",
                    "0.01",
                    None,
                )
                for index in range(5000)
            ],
        )
        execute_values(
            cur,
            """
            INSERT INTO simulated_orders(id,symbol,side,quantity_btc,price)
            VALUES %s
            """,
            [
                (100_000 + index, "ETHUSDC", "BUY", "1", "10")
                for index in range(5000)
            ],
        )
        execute_values(
            cur,
            """
            INSERT INTO simulated_execution_fills_v1(
              position_id,order_purpose,fill_qty,fill_price
            ) VALUES %s
            """,
            [
                (100_000 + index, "ENTRY", "1", "10")
                for index in range(5000)
            ],
        )

        for position_id in range(1488, 1501):
            entry_order = str(position_id * 2)
            exit_order = str(position_id * 2 + 1)
            if mode == "LIVE":
                if position_id != 1496:
                    cur.execute(
                        """
                        INSERT INTO binance_order_fills(
                          order_id,symbol,side,executed_qty,
                          quote_notional_usdc,commission_usdc,
                          commission_asset,commission_amount,bnbusdc_price
                        ) VALUES
                          (%s,'BTCUSDC','BUY',1,10,NULL,'USDC',0.01,NULL),
                          (%s,'BTCUSDC','BUY',1,11,NULL,'BNB',0.001,300)
                        """,
                        (entry_order, entry_order),
                    )
                if position_id not in {1495, 1498}:
                    cur.execute(
                        """
                        INSERT INTO binance_order_fills(
                          order_id,symbol,side,executed_qty,
                          quote_notional_usdc,commission_usdc,
                          commission_asset,commission_amount,bnbusdc_price
                        ) VALUES
                          (%s,'BTCUSDC','SELL',1,12,0.01,'USDC',0.01,NULL),
                          (%s,'BTCUSDC','SELL',1,13,NULL,'BNB',0.001,300)
                        """,
                        (exit_order, exit_order),
                    )
            else:
                if position_id != 1496:
                    cur.execute(
                        """
                        INSERT INTO simulated_orders(
                          id,symbol,side,quantity_btc,price
                        ) VALUES (%s,'BTCUSDC','BUY',2,10.5)
                        """,
                        (int(entry_order),),
                    )
                if position_id not in {1495, 1498}:
                    cur.execute(
                        """
                        INSERT INTO simulated_orders(
                          id,symbol,side,quantity_btc,price
                        ) VALUES (%s,'BTCUSDC','SELL',2,12.5)
                        """,
                        (int(exit_order),),
                    )

            if position_id % 2 == 0:
                cur.execute(
                    """
                    INSERT INTO simulated_execution_fills_v1(
                      position_id,order_purpose,fill_qty,fill_price
                    ) VALUES
                      (%s,'ENTRY',1,10.2),(%s,'ENTRY',1,10.3),
                      (%s,'EXIT',1,12.2),(%s,'EXIT',1,12.3)
                    """,
                    (position_id, position_id, position_id, position_id),
                )
    conn.commit()


def _database(disposable_postgres_v16, mode: str, purpose: str):
    name = (
        f"waltrade_baseline_test_recent_closed_"
        f"{mode.lower()}_{purpose}"
    )
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    _create_schema(conn)
    _seed(conn, mode)
    return conn


def _plan_nodes(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _plan_nodes(child)
    elif isinstance(value, list):
        for child in value:
            yield from _plan_nodes(child)


@pytest.mark.parametrize("mode", ["PAPER", "LIVE"])
def test_bounded_query_matches_global_reference(disposable_postgres_v16, mode):
    conn = _database(disposable_postgres_v16, mode, "equivalence")
    try:
        with conn.cursor() as cur:
            cur.execute(GLOBAL_REFERENCE_SQL, (mode, 10))
            expected = cur.fetchall()
            cur.execute(PRODUCTION_SQL, (mode, 10))
            actual = cur.fetchall()
        assert actual == expected
        assert len(actual) == 10
        assert [row[0] for row in actual] == list(range(1500, 1490, -1))
        assert all(len(row) == 15 for row in actual)
        assert {row[5] for row in actual} == {"LONG", "SHORT", "BUY", "SELL"}
        assert {row[14] for row in actual} == {
            f"fixture-{position_id}" for position_id in range(1491, 1501)
        }
    finally:
        conn.close()


@pytest.mark.parametrize("mode", ["PAPER", "LIVE"])
def test_fewer_rows_and_exit_time_tie_remain_equivalent(
    disposable_postgres_v16, mode
):
    conn = _database(disposable_postgres_v16, mode, "boundaries")
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM positions WHERE id < 1494")
            cur.execute(
                "UPDATE positions SET exit_time=%s WHERE id IN (1499,1500)",
                ("2026-01-02 00:00:00+00",),
            )
            cur.execute(GLOBAL_REFERENCE_SQL, (mode, 10))
            expected = cur.fetchall()
            cur.execute(PRODUCTION_SQL, (mode, 10))
            actual = cur.fetchall()
        assert sorted(actual, key=lambda row: row[0]) == sorted(
            expected, key=lambda row: row[0]
        )
        assert len(actual) == 7
        assert set(row[0] for row in actual[:2]) == {1499, 1500}
    finally:
        conn.close()


@pytest.mark.parametrize("mode", ["PAPER", "LIVE"])
def test_plan_bounds_execution_evidence(disposable_postgres_v16, mode):
    conn = _database(disposable_postgres_v16, mode, "plan")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "ANALYZE positions,binance_order_fills,"
                "simulated_orders,simulated_execution_fills_v1"
            )
            cur.execute(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + PRODUCTION_SQL,
                (mode, 10),
            )
            explained = cur.fetchone()[0][0]
            cur.execute("SELECT count(*) FROM binance_order_fills")
            total_real_fills = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM simulated_execution_fills_v1")
            total_simulated_fills = cur.fetchone()[0]
        nodes = list(_plan_nodes(explained["Plan"]))
        index_names = {
            str(node["Index Name"])
            for node in nodes
            if node.get("Index Name")
        }
        real_nodes = [
            node for node in nodes
            if node.get("Relation Name") == "binance_order_fills"
        ]
        simulated_nodes = [
            node for node in nodes
            if node.get("Relation Name") == "simulated_execution_fills_v1"
        ]
        scanned_real = sum(
            int(node.get("Actual Rows", 0)) * int(node.get("Actual Loops", 0))
            for node in real_nodes
        )
        scanned_simulated = sum(
            int(node.get("Actual Rows", 0)) * int(node.get("Actual Loops", 0))
            for node in simulated_nodes
        )

        assert "ix_simulated_execution_fills_position_v1" in index_names
        assert scanned_simulated < total_simulated_fills
        if mode == "LIVE":
            assert "ix_binance_order_fills_order_id" in index_names
            assert scanned_real < total_real_fills
        else:
            assert scanned_real == 0
        assert not any(
            int(node.get("Temp Read Blocks", 0))
            or int(node.get("Temp Written Blocks", 0))
            for node in nodes
        )
        assert int(explained["Plan"]["Actual Rows"]) <= 10
    finally:
        conn.close()


def test_production_sql_has_structural_top_n_boundary():
    assert PRODUCTION_SQL.index("LIMIT %s") < PRODUCTION_SQL.index(
        "relevant_order_ids AS MATERIALIZED"
    )
    assert "v_positions_pnl_net_real_ssot" not in PRODUCTION_SQL
    assert "v_positions_pnl_net_est" not in PRODUCTION_SQL
    assert PRODUCTION_SQL.count("FROM relevant_order_ids relevant") == 2
    assert "WHERE f.order_id = relevant.order_id" in PRODUCTION_SQL
    assert "o.id = relevant.order_id::bigint" in PRODUCTION_SQL

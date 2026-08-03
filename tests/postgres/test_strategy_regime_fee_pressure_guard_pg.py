from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LEDGER = (
    ROOT / "db/migrations/20260801_schema_migration_ledger_v1_baseline.sql"
).read_text()
MIGRATION = (
    ROOT / "db/migrations/20260803_strategy_regime_fee_pressure_guard_v1.sql"
).read_text()


SCHEMA = """
CREATE TABLE positions (
  id BIGINT PRIMARY KEY,
  strategy TEXT NOT NULL,
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  market_regime TEXT,
  status TEXT NOT NULL,
  exit_time TIMESTAMPTZ,
  net_pnl_usdc NUMERIC,
  gross_pnl_usdc NUMERIC,
  fees_usdc NUMERIC
);
CREATE TABLE strategy_regime_stats (
  strategy TEXT NOT NULL,
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  market_regime TEXT NOT NULL,
  window_days INTEGER NOT NULL,
  trades INTEGER NOT NULL,
  wins INTEGER NOT NULL,
  losses INTEGER NOT NULL,
  net_pnl_usdc NUMERIC(18,8),
  gross_pnl_usdc NUMERIC(18,8),
  fees_usdc NUMERIC(18,8),
  avg_net_usdc NUMERIC(18,8),
  profit_factor NUMERIC(18,6),
  win_rate_pct NUMERIC(10,4),
  fee_pressure_pct NUMERIC(10,4),
  calculated_at TIMESTAMPTZ NOT NULL
);
CREATE OR REPLACE FUNCTION refresh_strategy_regime_stats()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM strategy_regime_stats WHERE window_days IN (14,30);
  INSERT INTO strategy_regime_stats
  SELECT strategy,symbol,interval,market_regime,14,trades,wins,losses,
         net_pnl_usdc,gross_pnl_usdc,fees_usdc,avg_net_usdc,
         profit_factor,win_rate_pct,fee_pressure_pct,now()
  FROM v_strategy_regime_14d;
  INSERT INTO strategy_regime_stats
  SELECT strategy,symbol,interval,market_regime,30,trades,wins,losses,
         net_pnl_usdc,gross_pnl_usdc,fees_usdc,avg_net_usdc,
         profit_factor,win_rate_pct,fee_pressure_pct,now()
  FROM v_strategy_regime_30d;
END;
$$;
"""


def test_fee_pressure_overflow_is_null_and_next_refresh_jobs_remain_usable(
    disposable_postgres_v16,
):
    database = "waltrade_baseline_test_strategy_regime_fee_pressure"
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(LEDGER)
            cur.execute(SCHEMA)
            cur.execute(MIGRATION)
            cur.execute(MIGRATION)
            cur.execute(
                """
                INSERT INTO positions VALUES
                  (10307,'BBRANGE','ETHUSDC','1m','TREND_UP','CLOSED',
                   now(),-2.77607850,0.00022383,2.77630233),
                  (10308,'TREND','BTCUSDC','5m','TREND_UP','CLOSED',
                   now(),1.9,2.0,0.1)
                """
            )
            cur.execute("SELECT refresh_strategy_regime_stats()")
            cur.execute(
                """
                SELECT window_days,fee_pressure_pct
                FROM strategy_regime_stats
                WHERE strategy='BBRANGE' AND symbol='ETHUSDC'
                ORDER BY window_days
                """
            )
            assert cur.fetchall() == [(14, None), (30, None)]
            cur.execute(
                """
                SELECT window_days,fee_pressure_pct
                FROM strategy_regime_stats
                WHERE strategy='TREND' AND symbol='BTCUSDC'
                ORDER BY window_days
                """
            )
            assert [str(row[1]) for row in cur.fetchall()] == ["5.0000", "5.0000"]
            cur.execute(
                """
                SELECT count(*) FROM schema_migration_ledger_v1
                WHERE migration_id=
                  '20260803_strategy_regime_fee_pressure_guard_v1.sql'
                  AND success=true
                """
            )
            assert cur.fetchone() == (1,)
    finally:
        conn.close()

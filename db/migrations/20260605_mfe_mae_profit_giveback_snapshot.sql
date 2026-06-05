BEGIN;

-- MFE / MAE / Profit Giveback analytics - snapshot version.
-- This replaces heavy live views with a cached table refreshed on demand.
-- It does not change trading, ORC picks, bot_control, positions or runtime params.

DROP VIEW IF EXISTS v_profit_lock_giveback_14d;
DROP VIEW IF EXISTS v_trade_mfe_mae_exit_reason_14d;
DROP VIEW IF EXISTS v_trade_mfe_mae_strategy_7d;
DROP VIEW IF EXISTS v_trade_mfe_mae;

CREATE INDEX IF NOT EXISTS ix_positions_closed_exit_time_mfe_snapshot
ON positions(exit_time DESC)
WHERE status = 'CLOSED';

CREATE INDEX IF NOT EXISTS ix_positions_closed_id_mfe_snapshot
ON positions(id)
WHERE status = 'CLOSED';

CREATE INDEX IF NOT EXISTS ix_candles_symbol_interval_open_time_mfe_snapshot
ON candles(symbol, interval, open_time);

CREATE TABLE IF NOT EXISTS trade_mfe_mae_snapshot (
  id BIGINT PRIMARY KEY,
  strategy TEXT,
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  side TEXT,
  entry_time TIMESTAMPTZ NOT NULL,
  exit_time TIMESTAMPTZ NOT NULL,
  entry_price NUMERIC,
  exit_price NUMERIC,
  qty NUMERIC,
  exit_reason TEXT,
  market_regime TEXT,
  gross_pnl_usdc NUMERIC,
  fees_usdc NUMERIC,
  net_pnl_usdc NUMERIC,
  hold_minutes NUMERIC,
  max_high NUMERIC,
  min_low NUMERIC,
  first_candle_open_time TIMESTAMPTZ,
  last_candle_open_time TIMESTAMPTZ,
  bars_seen INTEGER,
  mfe_pct NUMERIC,
  mae_pct NUMERIC,
  exit_pct NUMERIC,
  giveback_pct NUMERIC,
  mfe_usdc NUMERIC,
  mae_usdc NUMERIC,
  fee_pressure_pct NUMERIC,
  mfe_capture_pct NUMERIC,
  refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_trade_mfe_snapshot_exit_time
ON trade_mfe_mae_snapshot(exit_time DESC);

CREATE INDEX IF NOT EXISTS ix_trade_mfe_snapshot_strategy_symbol_interval
ON trade_mfe_mae_snapshot(strategy, symbol, interval, exit_time DESC);

CREATE INDEX IF NOT EXISTS ix_trade_mfe_snapshot_exit_reason
ON trade_mfe_mae_snapshot(exit_reason, exit_time DESC);

CREATE INDEX IF NOT EXISTS ix_trade_mfe_snapshot_regime
ON trade_mfe_mae_snapshot(market_regime, exit_time DESC);

CREATE OR REPLACE FUNCTION refresh_trade_mfe_mae_snapshot(days_back INTEGER DEFAULT 30)
RETURNS TABLE(refreshed_rows INTEGER, min_exit_time TIMESTAMPTZ, max_exit_time TIMESTAMPTZ)
LANGUAGE plpgsql
AS $$
DECLARE
  v_days INTEGER := GREATEST(COALESCE(days_back, 30), 1);
BEGIN
  DELETE FROM trade_mfe_mae_snapshot
  WHERE exit_time >= now() - make_interval(days => v_days);

  INSERT INTO trade_mfe_mae_snapshot (
    id, strategy, symbol, interval, side, entry_time, exit_time,
    entry_price, exit_price, qty, exit_reason, market_regime,
    gross_pnl_usdc, fees_usdc, net_pnl_usdc, hold_minutes,
    max_high, min_low, first_candle_open_time, last_candle_open_time, bars_seen,
    mfe_pct, mae_pct, exit_pct, giveback_pct, mfe_usdc, mae_usdc,
    fee_pressure_pct, mfe_capture_pct, refreshed_at
  )
  WITH trade AS (
    SELECT
      p.id,
      p.strategy,
      p.symbol,
      p.interval,
      p.side,
      p.entry_time,
      p.exit_time,
      p.entry_price,
      p.exit_price,
      p.qty,
      p.exit_reason,
      p.market_regime,
      p.gross_pnl_usdc,
      p.fees_usdc,
      p.net_pnl_usdc,
      p.hold_minutes,
      upper(COALESCE(p.side, 'LONG')) AS side_norm
    FROM positions p
    WHERE p.status = 'CLOSED'
      AND p.exit_time >= now() - make_interval(days => v_days)
      AND p.entry_time IS NOT NULL
      AND p.exit_time IS NOT NULL
      AND p.entry_price IS NOT NULL
      AND p.exit_price IS NOT NULL
      AND p.qty IS NOT NULL
  ), enriched AS (
    SELECT
      t.*,
      COALESCE(e.max_high, t.entry_price) AS max_high,
      COALESCE(e.min_low, t.entry_price) AS min_low,
      e.first_candle_open_time,
      e.last_candle_open_time,
      COALESCE(e.bars_seen, 0)::INTEGER AS bars_seen
    FROM trade t
    LEFT JOIN LATERAL (
      SELECT
        MAX(c.high) AS max_high,
        MIN(c.low) AS min_low,
        MIN(c.open_time) AS first_candle_open_time,
        MAX(c.open_time) AS last_candle_open_time,
        COUNT(*) AS bars_seen
      FROM candles c
      WHERE c.symbol = t.symbol
        AND c.interval = t.interval
        AND c.open_time >= t.entry_time
        AND c.open_time <= t.exit_time
    ) e ON true
  ), calc AS (
    SELECT
      e.*,
      CASE
        WHEN e.side_norm IN ('LONG','BUY')
          THEN ((e.max_high - e.entry_price) / NULLIF(e.entry_price, 0)) * 100
        WHEN e.side_norm IN ('SHORT','SELL')
          THEN ((e.entry_price - e.min_low) / NULLIF(e.entry_price, 0)) * 100
      END AS mfe_pct,
      CASE
        WHEN e.side_norm IN ('LONG','BUY')
          THEN ((e.min_low - e.entry_price) / NULLIF(e.entry_price, 0)) * 100
        WHEN e.side_norm IN ('SHORT','SELL')
          THEN ((e.entry_price - e.max_high) / NULLIF(e.entry_price, 0)) * 100
      END AS mae_pct,
      CASE
        WHEN e.side_norm IN ('LONG','BUY')
          THEN ((e.exit_price - e.entry_price) / NULLIF(e.entry_price, 0)) * 100
        WHEN e.side_norm IN ('SHORT','SELL')
          THEN ((e.entry_price - e.exit_price) / NULLIF(e.entry_price, 0)) * 100
      END AS exit_pct,
      CASE
        WHEN e.side_norm IN ('LONG','BUY')
          THEN (e.max_high - e.entry_price) * e.qty
        WHEN e.side_norm IN ('SHORT','SELL')
          THEN (e.entry_price - e.min_low) * e.qty
      END AS mfe_usdc,
      CASE
        WHEN e.side_norm IN ('LONG','BUY')
          THEN (e.min_low - e.entry_price) * e.qty
        WHEN e.side_norm IN ('SHORT','SELL')
          THEN (e.entry_price - e.max_high) * e.qty
      END AS mae_usdc
    FROM enriched e
  )
  SELECT
    c.id, c.strategy, c.symbol, c.interval, c.side, c.entry_time, c.exit_time,
    c.entry_price, c.exit_price, c.qty, c.exit_reason, c.market_regime,
    c.gross_pnl_usdc, c.fees_usdc, c.net_pnl_usdc, c.hold_minutes,
    c.max_high, c.min_low, c.first_candle_open_time, c.last_candle_open_time, c.bars_seen,
    c.mfe_pct,
    c.mae_pct,
    c.exit_pct,
    c.mfe_pct - c.exit_pct AS giveback_pct,
    c.mfe_usdc,
    c.mae_usdc,
    CASE
      WHEN ABS(COALESCE(c.gross_pnl_usdc, 0)) > 0
        THEN (c.fees_usdc / ABS(c.gross_pnl_usdc)) * 100
    END AS fee_pressure_pct,
    CASE
      WHEN c.mfe_pct > 0
        THEN (c.exit_pct / NULLIF(c.mfe_pct, 0)) * 100
    END AS mfe_capture_pct,
    now() AS refreshed_at
  FROM calc c
  ON CONFLICT (id) DO UPDATE SET
    strategy = EXCLUDED.strategy,
    symbol = EXCLUDED.symbol,
    interval = EXCLUDED.interval,
    side = EXCLUDED.side,
    entry_time = EXCLUDED.entry_time,
    exit_time = EXCLUDED.exit_time,
    entry_price = EXCLUDED.entry_price,
    exit_price = EXCLUDED.exit_price,
    qty = EXCLUDED.qty,
    exit_reason = EXCLUDED.exit_reason,
    market_regime = EXCLUDED.market_regime,
    gross_pnl_usdc = EXCLUDED.gross_pnl_usdc,
    fees_usdc = EXCLUDED.fees_usdc,
    net_pnl_usdc = EXCLUDED.net_pnl_usdc,
    hold_minutes = EXCLUDED.hold_minutes,
    max_high = EXCLUDED.max_high,
    min_low = EXCLUDED.min_low,
    first_candle_open_time = EXCLUDED.first_candle_open_time,
    last_candle_open_time = EXCLUDED.last_candle_open_time,
    bars_seen = EXCLUDED.bars_seen,
    mfe_pct = EXCLUDED.mfe_pct,
    mae_pct = EXCLUDED.mae_pct,
    exit_pct = EXCLUDED.exit_pct,
    giveback_pct = EXCLUDED.giveback_pct,
    mfe_usdc = EXCLUDED.mfe_usdc,
    mae_usdc = EXCLUDED.mae_usdc,
    fee_pressure_pct = EXCLUDED.fee_pressure_pct,
    mfe_capture_pct = EXCLUDED.mfe_capture_pct,
    refreshed_at = EXCLUDED.refreshed_at;

  RETURN QUERY
  SELECT
    COUNT(*)::INTEGER AS refreshed_rows,
    MIN(s.exit_time) AS min_exit_time,
    MAX(s.exit_time) AS max_exit_time
  FROM trade_mfe_mae_snapshot s
  WHERE s.exit_time >= now() - make_interval(days => v_days);
END;
$$;

CREATE OR REPLACE VIEW v_trade_mfe_mae AS
SELECT *
FROM trade_mfe_mae_snapshot;

CREATE OR REPLACE VIEW v_trade_mfe_mae_strategy_7d AS
SELECT
  strategy,
  symbol,
  interval,
  COUNT(*) AS trades,
  COUNT(*) FILTER (WHERE net_pnl_usdc > 0) AS wins,
  COUNT(*) FILTER (WHERE net_pnl_usdc <= 0) AS losses,
  ROUND(((COUNT(*) FILTER (WHERE net_pnl_usdc > 0))::numeric / NULLIF(COUNT(*),0) * 100), 2) AS win_rate_pct,
  ROUND(SUM(gross_pnl_usdc), 6) AS gross,
  ROUND(SUM(fees_usdc), 6) AS fees,
  ROUND(SUM(net_pnl_usdc), 6) AS net,
  ROUND(AVG(mfe_pct), 4) AS avg_mfe_pct,
  ROUND(AVG(mae_pct), 4) AS avg_mae_pct,
  ROUND(AVG(exit_pct), 4) AS avg_exit_pct,
  ROUND(AVG(giveback_pct), 4) AS avg_giveback_pct,
  ROUND(AVG(fee_pressure_pct), 2) AS avg_fee_pressure_pct,
  ROUND(AVG(mfe_capture_pct), 2) AS avg_mfe_capture_pct
FROM trade_mfe_mae_snapshot
WHERE exit_time >= now() - interval '7 days'
GROUP BY strategy, symbol, interval;

CREATE OR REPLACE VIEW v_trade_mfe_mae_exit_reason_14d AS
SELECT
  strategy,
  symbol,
  interval,
  exit_reason,
  COUNT(*) AS trades,
  ROUND(SUM(gross_pnl_usdc), 6) AS gross,
  ROUND(SUM(fees_usdc), 6) AS fees,
  ROUND(SUM(net_pnl_usdc), 6) AS net,
  ROUND(AVG(mfe_pct), 4) AS avg_mfe_pct,
  ROUND(AVG(mae_pct), 4) AS avg_mae_pct,
  ROUND(AVG(exit_pct), 4) AS avg_exit_pct,
  ROUND(AVG(giveback_pct), 4) AS avg_giveback_pct,
  ROUND(AVG(fee_pressure_pct), 2) AS avg_fee_pressure_pct,
  ROUND(AVG(mfe_capture_pct), 2) AS avg_mfe_capture_pct
FROM trade_mfe_mae_snapshot
WHERE exit_time >= now() - interval '14 days'
GROUP BY strategy, symbol, interval, exit_reason;

CREATE OR REPLACE VIEW v_profit_lock_giveback_14d AS
SELECT
  id,
  strategy,
  symbol,
  interval,
  entry_time,
  exit_time,
  hold_minutes,
  gross_pnl_usdc,
  fees_usdc,
  net_pnl_usdc,
  mfe_pct,
  exit_pct,
  giveback_pct,
  fee_pressure_pct,
  mfe_capture_pct,
  exit_reason
FROM trade_mfe_mae_snapshot
WHERE exit_time >= now() - interval '14 days'
  AND exit_reason ILIKE '%PROFIT_LOCK%';

ANALYZE positions;
ANALYZE candles;
ANALYZE trade_mfe_mae_snapshot;

COMMIT;

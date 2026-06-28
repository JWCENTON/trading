BEGIN;

CREATE TABLE IF NOT EXISTS slot_intelligence_snapshot (
  window_days INTEGER NOT NULL,
  strategy TEXT NOT NULL,
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  market_regime TEXT NOT NULL,
  confidence_bucket TEXT NOT NULL,

  trades INTEGER NOT NULL,
  wins INTEGER NOT NULL,
  losses INTEGER NOT NULL,
  win_rate_pct NUMERIC,
  gross_pnl NUMERIC,
  fees NUMERIC,
  net_pnl NUMERIC,
  avg_net_trade NUMERIC,
  profit_factor_net NUMERIC,
  fee_pressure_pct NUMERIC,

  avg_mfe_pct NUMERIC,
  avg_mae_pct NUMERIC,
  avg_exit_pct NUMERIC,
  avg_giveback_pct NUMERIC,
  avg_mfe_capture_pct NUMERIC,
  avg_hold_minutes NUMERIC,

  current_regime TEXT,
  current_confidence NUMERIC,
  runs_24h INTEGER,
  buy_decisions_24h INTEGER,
  hard_blocks_24h INTEGER,
  last_runtime_reason TEXT,
  last_runtime_at TIMESTAMPTZ,
  last_exit_at TIMESTAMPTZ,

  edge_score NUMERIC,
  edge_status TEXT NOT NULL,
  status_reason TEXT NOT NULL,

  refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (window_days, strategy, symbol, interval, market_regime, confidence_bucket)
);

CREATE INDEX IF NOT EXISTS ix_slot_intel_status_score
ON slot_intelligence_snapshot(edge_status, edge_score DESC);

CREATE INDEX IF NOT EXISTS ix_slot_intel_slot
ON slot_intelligence_snapshot(strategy, symbol, interval, refreshed_at DESC);

CREATE OR REPLACE FUNCTION refresh_slot_intelligence_snapshot(days_back INTEGER DEFAULT 30)
RETURNS TABLE(refreshed_rows INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
  v_days INTEGER := GREATEST(COALESCE(days_back, 30), 1);
BEGIN
  DELETE FROM slot_intelligence_snapshot WHERE window_days = v_days;

  INSERT INTO slot_intelligence_snapshot (
    window_days, strategy, symbol, interval, market_regime, confidence_bucket,
    trades, wins, losses, win_rate_pct, gross_pnl, fees, net_pnl, avg_net_trade,
    profit_factor_net, fee_pressure_pct,
    avg_mfe_pct, avg_mae_pct, avg_exit_pct, avg_giveback_pct, avg_mfe_capture_pct,
    avg_hold_minutes,
    current_regime, current_confidence,
    runs_24h, buy_decisions_24h, hard_blocks_24h,
    last_runtime_reason, last_runtime_at, last_exit_at,
    edge_score, edge_status, status_reason, refreshed_at
  )
  WITH trades AS (
    SELECT
      m.*,
      COALESCE(m.market_regime, 'UNKNOWN') AS regime_norm,
      CASE
        WHEN rc.confidence IS NULL THEN 'UNKNOWN'
        WHEN rc.confidence >= 0.80 THEN 'HIGH'
        WHEN rc.confidence >= 0.60 THEN 'MID'
        ELSE 'LOW'
      END AS confidence_bucket
    FROM v_trade_mfe_mae m
    LEFT JOIN LATERAL (
      SELECT mr.confidence
      FROM market_regime mr
      WHERE mr.symbol = m.symbol
        AND mr.interval = m.interval
        AND mr.ts <= m.exit_time
      ORDER BY mr.ts DESC
      LIMIT 1
    ) rc ON TRUE
    WHERE m.exit_time >= now() - make_interval(days => v_days)
      AND m.net_pnl_usdc IS NOT NULL
  ),
  agg AS (
    SELECT
      strategy,
      symbol,
      interval,
      regime_norm AS market_regime,
      confidence_bucket,
      COUNT(*)::INTEGER AS trades,
      COUNT(*) FILTER (WHERE net_pnl_usdc > 0)::INTEGER AS wins,
      COUNT(*) FILTER (WHERE net_pnl_usdc <= 0)::INTEGER AS losses,
      100.0 * COUNT(*) FILTER (WHERE net_pnl_usdc > 0) / NULLIF(COUNT(*),0) AS win_rate_pct,
      SUM(gross_pnl_usdc) AS gross_pnl,
      SUM(fees_usdc) AS fees,
      SUM(net_pnl_usdc) AS net_pnl,
      AVG(net_pnl_usdc) AS avg_net_trade,
      CASE
        WHEN ABS(SUM(net_pnl_usdc) FILTER (WHERE net_pnl_usdc < 0)) > 0
        THEN SUM(net_pnl_usdc) FILTER (WHERE net_pnl_usdc > 0)
             / ABS(SUM(net_pnl_usdc) FILTER (WHERE net_pnl_usdc < 0))
      END AS profit_factor_net,
      CASE
        WHEN ABS(SUM(gross_pnl_usdc)) > 0
        THEN SUM(fees_usdc) / ABS(SUM(gross_pnl_usdc)) * 100
      END AS fee_pressure_pct,
      AVG(mfe_pct) AS avg_mfe_pct,
      AVG(mae_pct) AS avg_mae_pct,
      AVG(exit_pct) AS avg_exit_pct,
      AVG(giveback_pct) AS avg_giveback_pct,
      AVG(mfe_capture_pct) AS avg_mfe_capture_pct,
      AVG(hold_minutes) AS avg_hold_minutes,
      MAX(exit_time) AS last_exit_at
    FROM trades
    GROUP BY strategy, symbol, interval, regime_norm, confidence_bucket
  ),
  runtime AS (
    SELECT
      se.strategy,
      se.symbol,
      se.interval,
      COUNT(*) FILTER (WHERE se.event_type = 'RUN_START')::INTEGER AS runs_24h,
      COUNT(*) FILTER (
        WHERE COALESCE(se.decision,'') ILIKE '%BUY%'
           OR COALESCE(se.reason,'') ILIKE '%BUY%'
           OR se.event_type ILIKE '%SIGNAL%'
      )::INTEGER AS buy_decisions_24h,
      COUNT(*) FILTER (
        WHERE se.event_type IN ('BLOCKED','SKIP')
          AND (
            COALESCE(se.reason,'') ILIKE '%BLOCK%'
            OR COALESCE(se.reason,'') ILIKE '%HARD%'
            OR COALESCE(se.reason,'') ILIKE '%LONG_ONLY%'
            OR COALESCE(se.reason,'') ILIKE '%REGIME%'
            OR COALESCE(se.reason,'') ILIKE '%STALE%'
          )
      )::INTEGER AS hard_blocks_24h
    FROM strategy_events se
    WHERE se.created_at >= now() - interval '24 hours'
    GROUP BY se.strategy, se.symbol, se.interval
  ),
  last_event AS (
    SELECT DISTINCT ON (strategy, symbol, interval)
      strategy, symbol, interval, reason AS last_runtime_reason, created_at AS last_runtime_at
    FROM strategy_events
    ORDER BY strategy, symbol, interval, created_at DESC
  ),
  current_regime AS (
    SELECT DISTINCT ON (symbol, interval)
      symbol, interval, regime AS current_regime, confidence AS current_confidence
    FROM market_regime
    ORDER BY symbol, interval, ts DESC
  ),
  scored AS (
    SELECT
      a.*,
      cr.current_regime,
      cr.current_confidence,
      COALESCE(r.runs_24h,0) AS runs_24h,
      COALESCE(r.buy_decisions_24h,0) AS buy_decisions_24h,
      COALESCE(r.hard_blocks_24h,0) AS hard_blocks_24h,
      le.last_runtime_reason,
      le.last_runtime_at,

      (
        CASE WHEN a.trades >= 50 THEN 20 WHEN a.trades >= 30 THEN 15 WHEN a.trades >= 20 THEN 10 ELSE 0 END
        + CASE WHEN a.net_pnl > 0 THEN 20 ELSE -20 END
        + CASE WHEN COALESCE(a.profit_factor_net,0) >= 1.30 THEN 25
               WHEN COALESCE(a.profit_factor_net,0) >= 1.15 THEN 15
               WHEN COALESCE(a.profit_factor_net,0) >= 1.00 THEN 5
               ELSE -20 END
        + CASE WHEN a.avg_net_trade > 0 THEN 10 ELSE -10 END
        + CASE WHEN a.win_rate_pct >= 55 THEN 10 WHEN a.win_rate_pct >= 50 THEN 5 ELSE 0 END
        + CASE WHEN COALESCE(a.fee_pressure_pct,999) <= 65 THEN 10 ELSE -10 END
        + CASE WHEN COALESCE(a.avg_mfe_capture_pct,0) >= 35 THEN 5 ELSE 0 END
      )::NUMERIC AS edge_score
    FROM agg a
    LEFT JOIN runtime r USING(strategy, symbol, interval)
    LEFT JOIN last_event le USING(strategy, symbol, interval)
    LEFT JOIN current_regime cr USING(symbol, interval)
  )
  SELECT
    v_days,
    strategy, symbol, interval, market_regime, confidence_bucket,
    trades, wins, losses,
    ROUND(win_rate_pct,2),
    ROUND(gross_pnl,8),
    ROUND(fees,8),
    ROUND(net_pnl,8),
    ROUND(avg_net_trade,8),
    ROUND(profit_factor_net,6),
    ROUND(fee_pressure_pct,4),
    ROUND(avg_mfe_pct,4),
    ROUND(avg_mae_pct,4),
    ROUND(avg_exit_pct,4),
    ROUND(avg_giveback_pct,4),
    ROUND(avg_mfe_capture_pct,2),
    ROUND(avg_hold_minutes,2),
    current_regime,
    ROUND(current_confidence,6),
    runs_24h,
    buy_decisions_24h,
    hard_blocks_24h,
    last_runtime_reason,
    last_runtime_at,
    last_exit_at,
    ROUND(edge_score,2),

    CASE
      WHEN strategy IN ('RSI','BBRANGE')
       AND market_regime NOT IN ('RANGE_LOWVOL','RANGE_HIGHVOL')
        THEN 'OBSERVE'

      WHEN strategy IN ('TREND','SUPERTREND')
       AND market_regime NOT IN ('TREND_UP','TREND_DOWN')
        THEN 'OBSERVE'

      WHEN trades >= 30
       AND net_pnl > 0
       AND avg_net_trade > 0
       AND COALESCE(profit_factor_net,0) >= 1.15
       AND COALESCE(fee_pressure_pct,999) <= 70
       AND win_rate_pct >= 50
        THEN 'ALLOW_LIVE'

      WHEN trades >= 30
       AND (
          net_pnl < 0
          OR avg_net_trade < 0
          OR COALESCE(profit_factor_net,0) < 0.85
       )
        THEN 'BLOCK_LIVE'

      ELSE 'OBSERVE'
    END AS edge_status,

    CASE
      WHEN trades < 30 THEN 'SAMPLE_TOO_SMALL'
      WHEN strategy IN ('RSI','BBRANGE') AND market_regime NOT IN ('RANGE_LOWVOL','RANGE_HIGHVOL') THEN 'RANGE_STRATEGY_OUTSIDE_RANGE'
      WHEN strategy IN ('TREND','SUPERTREND') AND market_regime NOT IN ('TREND_UP','TREND_DOWN') THEN 'TREND_STRATEGY_OUTSIDE_TREND'
      WHEN net_pnl > 0 AND COALESCE(profit_factor_net,0) >= 1.15 THEN 'POSITIVE_NET_EDGE'
      WHEN net_pnl < 0 THEN 'NEGATIVE_NET_EDGE'
      ELSE 'MIXED_OR_WEAK_EDGE'
    END AS status_reason,

    now()
  FROM scored;

  RETURN QUERY
  SELECT COUNT(*)::INTEGER
  FROM slot_intelligence_snapshot
  WHERE window_days = v_days;
END;
$$;

CREATE OR REPLACE VIEW v_slot_intelligence_v1 AS
SELECT *
FROM slot_intelligence_snapshot;

CREATE OR REPLACE VIEW v_slot_intelligence_latest_30d AS
SELECT *
FROM slot_intelligence_snapshot
WHERE window_days = 30;

INSERT INTO automation_kv(key, value)
VALUES
  ('slot_intelligence_v1_enabled', '1'),
  ('slot_intelligence_v1_version', '20260628')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

COMMIT;

BEGIN;

CREATE OR REPLACE VIEW v_strategy_regime_edge AS
WITH base AS (
  SELECT
    strategy,
    symbol,
    interval,
    market_regime,
    COUNT(*) AS trades,
    COUNT(*) FILTER (WHERE net_pnl_usdc > 0) AS wins,
    COUNT(*) FILTER (WHERE net_pnl_usdc <= 0) AS losses,
    SUM(net_pnl_usdc)::numeric AS net_pnl_usdc,
    SUM(gross_pnl_usdc)::numeric AS gross_pnl_usdc,
    SUM(fees_usdc)::numeric AS fees_usdc,
    AVG(net_pnl_usdc)::numeric AS avg_net_usdc,
    CASE
      WHEN COUNT(*) > 0
      THEN (100.0 * COUNT(*) FILTER (WHERE net_pnl_usdc > 0) / COUNT(*))::numeric
      ELSE NULL
    END AS win_rate_pct,
    CASE
      WHEN ABS(SUM(net_pnl_usdc) FILTER (WHERE net_pnl_usdc < 0)) > 0
      THEN (
        SUM(net_pnl_usdc) FILTER (WHERE net_pnl_usdc > 0)
        / ABS(SUM(net_pnl_usdc) FILTER (WHERE net_pnl_usdc < 0))
      )::numeric
      ELSE NULL
    END AS profit_factor,
    CASE
      WHEN ABS(SUM(gross_pnl_usdc)) > 0
      THEN (100.0 * SUM(fees_usdc) / ABS(SUM(gross_pnl_usdc)))::numeric
      ELSE NULL
    END AS fee_pressure_pct
  FROM positions
  WHERE status='CLOSED'
    AND exit_time >= now() - interval '30 days'
    AND market_regime IS NOT NULL
    AND net_pnl_usdc IS NOT NULL
  GROUP BY strategy, symbol, interval, market_regime
)
SELECT
  strategy,
  symbol,
  interval,
  market_regime,
  trades,
  wins,
  losses,
  ROUND(net_pnl_usdc, 8) AS net_pnl_usdc,
  ROUND(gross_pnl_usdc, 8) AS gross_pnl_usdc,
  ROUND(fees_usdc, 8) AS fees_usdc,
  ROUND(avg_net_usdc, 8) AS avg_net_usdc,
  ROUND(win_rate_pct, 2) AS win_rate_pct,
  ROUND(profit_factor, 6) AS profit_factor,
  ROUND(fee_pressure_pct, 4) AS fee_pressure_pct,
  CASE
    WHEN trades >= 10 AND net_pnl_usdc > 0 AND profit_factor >= 1.20 THEN 'STRONG_EDGE'
    WHEN trades >= 5  AND net_pnl_usdc > 0 AND profit_factor >= 1.05 THEN 'WEAK_EDGE'
    WHEN trades >= 10 AND net_pnl_usdc < 0 AND COALESCE(profit_factor, 0) < 0.80 THEN 'AVOID'
    ELSE 'OBSERVE'
  END AS edge_class
FROM base;

INSERT INTO automation_kv(key, value)
VALUES ('strategy_regime_edge_source_window', '30d_fallback')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

COMMIT;

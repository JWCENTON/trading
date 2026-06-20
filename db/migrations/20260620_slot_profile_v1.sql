CREATE OR REPLACE VIEW v_slot_profile_v1_14d AS
SELECT
  p.strategy,
  p.symbol,
  p.interval,
  COALESCE(p.market_regime, 'UNKNOWN') AS market_regime,

  COUNT(*) AS trades,
  COUNT(*) FILTER (WHERE p.net_pnl_usdc > 0) AS wins,
  COUNT(*) FILTER (WHERE p.net_pnl_usdc <= 0) AS losses,

  ROUND(SUM(p.net_pnl_usdc)::numeric, 8) AS net_pnl,
  ROUND(AVG(p.net_pnl_usdc)::numeric, 8) AS avg_net_trade,

  ROUND(
    (100.0 * COUNT(*) FILTER (WHERE p.net_pnl_usdc > 0) / NULLIF(COUNT(*), 0))::numeric,
    2
  ) AS win_rate_pct,

  ROUND(SUM(p.gross_pnl_usdc)::numeric, 8) AS gross_pnl,
  ROUND(SUM(p.fees_usdc)::numeric, 8) AS fees,

  CASE
    WHEN ABS(SUM(p.gross_pnl_usdc)) > 0
      THEN ROUND((SUM(p.fees_usdc) / ABS(SUM(p.gross_pnl_usdc)) * 100)::numeric, 4)
    ELSE NULL
  END AS fee_pressure_pct,

  CASE
    WHEN ABS(SUM(p.net_pnl_usdc) FILTER (WHERE p.net_pnl_usdc < 0)) > 0
      THEN ROUND(
        (
          SUM(p.net_pnl_usdc) FILTER (WHERE p.net_pnl_usdc > 0)
          / ABS(SUM(p.net_pnl_usdc) FILTER (WHERE p.net_pnl_usdc < 0))
        )::numeric,
        6
      )
    ELSE NULL
  END AS profit_factor_net,

  CASE
    WHEN COUNT(*) >= 20
      AND SUM(p.net_pnl_usdc) > 0
      AND AVG(p.net_pnl_usdc) > 0
      AND (
        ABS(SUM(p.net_pnl_usdc) FILTER (WHERE p.net_pnl_usdc < 0)) = 0
        OR (
          SUM(p.net_pnl_usdc) FILTER (WHERE p.net_pnl_usdc > 0)
          / NULLIF(ABS(SUM(p.net_pnl_usdc) FILTER (WHERE p.net_pnl_usdc < 0)), 0)
        ) >= 1.15
      )
      THEN 'ALLOW_LIVE_CANDIDATE'

    WHEN COUNT(*) >= 20
      AND SUM(p.net_pnl_usdc) < 0
      THEN 'BLOCK_LIVE'

    ELSE 'OBSERVE'
  END AS edge_status

FROM positions p
WHERE p.status = 'CLOSED'
  AND p.exit_time >= now() - interval '14 days'
  AND p.net_pnl_usdc IS NOT NULL
GROUP BY
  p.strategy,
  p.symbol,
  p.interval,
  COALESCE(p.market_regime, 'UNKNOWN');

BEGIN;

-- ORC V6.2 Compatibility Gate
-- Keeps public.v_orc_picks_v5 output shape for backward compatibility.
-- Adds strategy x regime x confidence analytics and uses it as boost/penalty in ORC ranking.

CREATE OR REPLACE VIEW public.v_strategy_regime_confidence_14d AS
WITH trades AS (
  SELECT
    p.id,
    p.strategy,
    p.symbol,
    p.interval,
    COALESCE(p.market_regime, mr.regime, 'UNKNOWN') AS market_regime,
    CASE
      WHEN mr.confidence >= 0.85 THEN 'HIGH_CONF'
      WHEN mr.confidence >= 0.65 THEN 'MID_CONF'
      WHEN mr.confidence IS NOT NULL THEN 'LOW_CONF'
      ELSE 'NO_CONF'
    END AS confidence_bucket,
    p.gross_pnl_usdc,
    p.fees_usdc,
    p.net_pnl_usdc,
    mr.confidence
  FROM public.positions p
  LEFT JOIN LATERAL (
    SELECT m.regime, m.confidence
    FROM public.market_regime m
    WHERE m.symbol = p.symbol
      AND m.interval = p.interval
      AND m.ts <= COALESCE(p.entry_time, p.exit_time)
    ORDER BY m.ts DESC
    LIMIT 1
  ) mr ON true
  WHERE p.status = 'CLOSED'
    AND p.exit_time >= now() - interval '14 days'
    AND p.net_pnl_usdc IS NOT NULL
)
SELECT
  strategy,
  symbol,
  interval,
  market_regime,
  confidence_bucket,
  COUNT(*) AS trades,
  COUNT(*) FILTER (WHERE net_pnl_usdc > 0) AS wins,
  COUNT(*) FILTER (WHERE net_pnl_usdc <= 0) AS losses,
  ROUND((100.0 * COUNT(*) FILTER (WHERE net_pnl_usdc > 0) / NULLIF(COUNT(*),0))::numeric, 2) AS win_rate_pct,
  ROUND(SUM(gross_pnl_usdc)::numeric, 8) AS gross_pnl,
  ROUND(SUM(fees_usdc)::numeric, 8) AS fees,
  ROUND(SUM(net_pnl_usdc)::numeric, 8) AS net_pnl,
  ROUND(AVG(net_pnl_usdc)::numeric, 8) AS avg_net_trade,
  ROUND(AVG(confidence)::numeric, 4) AS avg_confidence,
  CASE
    WHEN ABS(SUM(gross_pnl_usdc)) > 0
      THEN ROUND((SUM(fees_usdc) / ABS(SUM(gross_pnl_usdc)))::numeric * 100, 4)
    ELSE NULL
  END AS fee_pressure_pct,
  CASE
    WHEN ABS(SUM(net_pnl_usdc) FILTER (WHERE net_pnl_usdc < 0)) > 0
      THEN ROUND(
        (SUM(net_pnl_usdc) FILTER (WHERE net_pnl_usdc > 0)
         / ABS(SUM(net_pnl_usdc) FILTER (WHERE net_pnl_usdc < 0)))::numeric,
        6
      )
    ELSE NULL
  END AS profit_factor_net
FROM trades
GROUP BY strategy, symbol, interval, market_regime, confidence_bucket;

CREATE OR REPLACE VIEW public.v_orc_v62_candidates AS
WITH latest_regime AS (
  SELECT DISTINCT ON (symbol, interval)
    symbol,
    interval,
    regime AS current_regime,
    confidence AS current_confidence,
    ts AS current_regime_ts,
    CASE
      WHEN confidence >= 0.85 THEN 'HIGH_CONF'
      WHEN confidence >= 0.65 THEN 'MID_CONF'
      WHEN confidence IS NOT NULL THEN 'LOW_CONF'
      ELSE 'NO_CONF'
    END AS current_confidence_bucket
  FROM public.market_regime
  ORDER BY symbol, interval, ts DESC
),
base AS (
  SELECT
    c.*,
    lr.current_regime,
    lr.current_confidence,
    lr.current_regime_ts,
    lr.current_confidence_bucket,
    sr.trades AS regime_trades_14d,
    sr.net_pnl AS regime_net_14d,
    sr.avg_net_trade AS regime_avg_net_14d,
    sr.profit_factor_net AS regime_pf_14d,
    sr.win_rate_pct AS regime_win_rate_14d,
    sr.fee_pressure_pct AS regime_fee_pressure_14d,
    sr.avg_confidence AS regime_avg_confidence_14d
  FROM public.v_orc_candidates_v5c c
  LEFT JOIN latest_regime lr
    ON lr.symbol = c.symbol
   AND lr.interval = c.interval
  LEFT JOIN public.v_strategy_regime_confidence_14d sr
    ON sr.symbol = c.symbol
   AND sr.interval = c.interval
   AND sr.strategy = c.strategy
   AND sr.market_regime = lr.current_regime
   AND sr.confidence_bucket = lr.current_confidence_bucket
),
scored AS (
  SELECT
    b.*,
    (b.n_trades_3d >= 5 AND b.net_sum_3d > 0 AND b.profit_factor_3d >= 1.05) AS passes_v62_core,
    CASE
      WHEN b.strategy = 'RSI'
       AND b.current_regime IN ('TREND_UP','TREND_DOWN')
       AND COALESCE(b.current_confidence,0) >= 0.65
        THEN true
      ELSE false
    END AS rsi_trend_penalty,
    CASE
      WHEN COALESCE(b.regime_trades_14d,0) >= 5
       AND COALESCE(b.regime_net_14d,0) > 0
       AND COALESCE(b.regime_pf_14d,0) >= 1.05
        THEN true
      ELSE false
    END AS positive_regime_compatibility,
    CASE
      WHEN COALESCE(b.regime_trades_14d,0) >= 5
       AND COALESCE(b.regime_net_14d,0) < 0
        THEN true
      ELSE false
    END AS negative_regime_compatibility,
    CASE
      WHEN b.strategy = 'TREND'
       AND b.current_regime = 'TREND_UP'
       AND COALESCE(b.current_confidence,0) >= 0.85
       AND COALESCE(b.regime_net_14d,0) > 0 THEN 0.35
      WHEN b.strategy = 'SUPERTREND'
       AND b.current_regime IN ('TREND_UP','TREND_DOWN')
       AND COALESCE(b.current_confidence,0) >= 0.85
       AND COALESCE(b.regime_net_14d,0) > 0 THEN 0.35
      WHEN b.strategy = 'RSI'
       AND b.current_regime IN ('TREND_UP','TREND_DOWN')
       AND COALESCE(b.current_confidence,0) >= 0.65 THEN -0.50
      WHEN COALESCE(b.regime_trades_14d,0) >= 5
       AND COALESCE(b.regime_net_14d,0) < 0 THEN -0.35
      ELSE 0.00
    END::numeric AS compatibility_score
  FROM base b
)
SELECT
  s.*,
  (
    s.passes_v62_core
    AND NOT s.rsi_trend_penalty
    AND NOT (
      s.negative_regime_compatibility
      AND COALESCE(s.regime_trades_14d,0) >= 10
      AND COALESCE(s.regime_pf_14d,0) < 1.0
    )
  ) AS eligible_v62,
  CASE
    WHEN NOT s.passes_v62_core THEN 'REJECT_CORE_NET_OR_PF'
    WHEN s.rsi_trend_penalty THEN 'REJECT_RSI_TREND_CONFIDENCE_PENALTY'
    WHEN s.negative_regime_compatibility AND COALESCE(s.regime_trades_14d,0) >= 10 AND COALESCE(s.regime_pf_14d,0) < 1.0 THEN 'REJECT_NEGATIVE_REGIME_COMPATIBILITY'
    WHEN s.positive_regime_compatibility THEN 'PICK_POSITIVE_REGIME_COMPATIBILITY'
    ELSE 'PICK_CORE_NET_AWARE'
  END AS v62_reason
FROM scored s;

CREATE OR REPLACE VIEW public.v_orc_picks_v5 AS
WITH candidates AS (
  SELECT
    1 AS prio,
    c.symbol,
    c.interval,
    c.strategy,
    c.n_trades_3d,
    c.net_sum_3d,
    c.profit_factor_3d,
    c.last_exit_ts_3d,
    c.n_buy_24h,
    c.n_runs_24h,
    c.n_filter_block_24h,
    c.filter_block_rate_24h,
    c.last_ts_24h,
    c.n_signal_15m,
    c.last_signal_ts,
    c.eligible_pick_v5,
    c.eligible_bootstrap_v5,
    c.eligible_signal_v5,
    c.eligible_activity_v5,
    c.eligible_softfill_v5,
    c.current_regime,
    c.current_confidence,
    c.current_confidence_bucket,
    c.regime_trades_14d,
    c.regime_net_14d,
    c.regime_pf_14d,
    c.compatibility_score,
    c.v62_reason,
    GREATEST(
      COALESCE(c.last_exit_ts_3d, '1970-01-01'::timestamptz),
      COALESCE(c.last_ts_24h, '1970-01-01'::timestamptz),
      COALESCE(c.last_signal_ts, '1970-01-01'::timestamptz)
    ) AS rank_last_ts
  FROM public.v_orc_v62_candidates c
  WHERE c.eligible_v62
),
dedup_symbol_interval AS (
  SELECT
    c.*,
    row_number() OVER (
      PARTITION BY c.symbol, c.interval
      ORDER BY
        c.compatibility_score DESC,
        c.net_sum_3d DESC,
        c.profit_factor_3d DESC,
        c.n_trades_3d DESC,
        c.regime_net_14d DESC NULLS LAST,
        c.regime_pf_14d DESC NULLS LAST,
        c.n_signal_15m DESC,
        c.n_buy_24h DESC,
        c.rank_last_ts DESC,
        CASE
          WHEN c.strategy='SUPERTREND' THEN 0
          WHEN c.strategy='TREND' THEN 1
          WHEN c.strategy='RSI' THEN 2
          WHEN c.strategy='BBRANGE' THEN 3
          ELSE 4
        END,
        c.strategy
    ) AS si_rn
  FROM candidates c
),
ranked AS (
  SELECT
    d.*,
    row_number() OVER (
      ORDER BY
        d.compatibility_score DESC,
        d.net_sum_3d DESC,
        d.profit_factor_3d DESC,
        d.n_trades_3d DESC,
        d.regime_net_14d DESC NULLS LAST,
        d.regime_pf_14d DESC NULLS LAST,
        d.n_signal_15m DESC,
        d.n_buy_24h DESC,
        d.rank_last_ts DESC,
        CASE
          WHEN d.strategy='SUPERTREND' THEN 0
          WHEN d.strategy='TREND' THEN 1
          WHEN d.strategy='RSI' THEN 2
          WHEN d.strategy='BBRANGE' THEN 3
          ELSE 4
        END,
        d.symbol,
        d.interval,
        d.strategy
    ) AS final_rn
  FROM dedup_symbol_interval d
  WHERE d.si_rn = 1
),
policy AS (
  SELECT COALESCE(
    (SELECT NULLIF(to_jsonb(ap)->>'max_picks','')::int
     FROM public.allocation_policy ap
     WHERE COALESCE((to_jsonb(ap)->>'enabled')::boolean, true)
     LIMIT 1),
    8
  ) AS max_picks
)
SELECT
  r.prio,
  r.symbol,
  r.interval,
  r.strategy,
  r.n_trades_3d,
  r.net_sum_3d,
  r.profit_factor_3d,
  r.last_exit_ts_3d,
  r.n_buy_24h,
  r.n_runs_24h,
  r.n_filter_block_24h,
  r.filter_block_rate_24h,
  r.last_ts_24h,
  r.n_signal_15m,
  r.last_signal_ts,
  r.eligible_pick_v5,
  r.eligible_bootstrap_v5,
  r.eligible_signal_v5,
  r.eligible_activity_v5,
  r.eligible_softfill_v5,
  r.final_rn AS rn,
  r.final_rn
FROM ranked r
CROSS JOIN policy p
WHERE r.final_rn <= p.max_picks;

CREATE OR REPLACE VIEW public.v_orc_v62_explain AS
SELECT
  c.symbol,
  c.interval,
  c.strategy,
  c.n_trades_3d,
  c.net_sum_3d,
  c.profit_factor_3d,
  c.current_regime,
  c.current_confidence,
  c.current_confidence_bucket,
  c.regime_trades_14d,
  c.regime_net_14d,
  c.regime_pf_14d,
  c.compatibility_score,
  c.passes_v62_core,
  c.rsi_trend_penalty,
  c.positive_regime_compatibility,
  c.negative_regime_compatibility,
  c.eligible_v62,
  c.v62_reason,
  CASE WHEN p.symbol IS NOT NULL THEN true ELSE false END AS picked_now,
  p.final_rn,
  bc.live_orders_enabled,
  bc.reason AS bot_control_reason
FROM public.v_orc_v62_candidates c
LEFT JOIN public.v_orc_picks_v5 p
  ON p.symbol = c.symbol
 AND p.interval = c.interval
 AND p.strategy = c.strategy
LEFT JOIN public.bot_control bc
  ON bc.symbol = c.symbol
 AND bc.interval = c.interval
 AND bc.strategy = c.strategy;

INSERT INTO public.automation_kv(key, value, updated_at)
VALUES
  ('orc_active_version', 'ORC_V6_2', now()),
  ('orc_active_mode', 'COMPATIBILITY_GATE', now())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;

COMMIT;

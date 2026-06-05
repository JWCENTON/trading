BEGIN;

-- ORC V6.3: Cooldown + Promote + Regime Hysteresis
-- Keeps public.v_orc_picks_v5 output shape for backward compatibility.
-- Does not mutate raw market_regime; ORC uses hysteresis_regime with fallback to raw regime.

CREATE OR REPLACE VIEW public.v_market_regime_hysteresis_latest AS
WITH ranked AS (
  SELECT
    mr.symbol,
    mr.interval,
    mr.ts,
    mr.regime,
    mr.confidence,
    row_number() OVER (PARTITION BY mr.symbol, mr.interval ORDER BY mr.ts DESC) AS rn
  FROM public.market_regime mr
), pivot AS (
  SELECT
    symbol,
    interval,
    MAX(ts) FILTER (WHERE rn=1) AS raw_ts,
    MAX(regime) FILTER (WHERE rn=1) AS raw_regime,
    MAX(confidence) FILTER (WHERE rn=1) AS raw_confidence,
    MAX(regime) FILTER (WHERE rn=2) AS prev_regime,
    MAX(confidence) FILTER (WHERE rn=2) AS prev_confidence,
    MAX(regime) FILTER (WHERE rn=3) AS prev2_regime,
    COUNT(*) AS rows_seen
  FROM ranked
  WHERE rn <= 3
  GROUP BY symbol, interval
)
SELECT
  symbol,
  interval,
  raw_ts,
  raw_regime,
  raw_confidence,
  prev_regime,
  prev_confidence,
  prev2_regime,
  CASE
    WHEN raw_regime IS NULL THEN NULL
    WHEN prev_regime IS NULL THEN raw_regime
    WHEN raw_regime = prev_regime THEN raw_regime
    WHEN prev_regime = prev2_regime THEN prev_regime
    ELSE raw_regime
  END AS hysteresis_regime,
  CASE
    WHEN raw_regime IS NULL THEN raw_confidence
    WHEN prev_regime IS NULL THEN raw_confidence
    WHEN raw_regime = prev_regime THEN raw_confidence
    WHEN prev_regime = prev2_regime THEN prev_confidence
    ELSE raw_confidence
  END AS hysteresis_confidence,
  CASE
    WHEN raw_regime IS NULL THEN 'NO_RAW_REGIME'
    WHEN prev_regime IS NULL THEN 'RAW_ONLY_FIRST_SAMPLE'
    WHEN raw_regime = prev_regime THEN 'CONFIRMED_RAW_2_BARS'
    WHEN prev_regime = prev2_regime THEN 'HOLD_PREVIOUS_UNTIL_2_CONFIRMATIONS'
    ELSE 'ACCEPT_RAW_AFTER_MIXED_HISTORY'
  END AS hysteresis_reason,
  (raw_regime IS DISTINCT FROM (
    CASE
      WHEN raw_regime IS NULL THEN NULL
      WHEN prev_regime IS NULL THEN raw_regime
      WHEN raw_regime = prev_regime THEN raw_regime
      WHEN prev_regime = prev2_regime THEN prev_regime
      ELSE raw_regime
    END
  )) AS hysteresis_holding_previous
FROM pivot;

CREATE OR REPLACE VIEW public.v_strategy_regime_confidence_30d AS
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
    AND p.exit_time >= now() - interval '30 days'
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

CREATE OR REPLACE VIEW public.v_orc_cooldown_candidates_v63 AS
SELECT
  strategy,
  symbol,
  interval,
  market_regime,
  confidence_bucket,
  trades,
  net_pnl,
  avg_net_trade,
  profit_factor_net,
  fee_pressure_pct,
  avg_confidence,
  (
    trades >= 30
    AND COALESCE(net_pnl,0) < 0
    AND COALESCE(profit_factor_net,0) < 0.80
    AND COALESCE(avg_net_trade,0) < 0
  ) AS cooldown_strong,
  CASE
    WHEN trades >= 30 AND COALESCE(net_pnl,0) < 0 AND COALESCE(profit_factor_net,0) < 0.80 AND COALESCE(avg_net_trade,0) < 0
      THEN 'COOLDOWN_STRONG_NET_NEGATIVE_PF_LT_0_80_SAMPLE_30'
    WHEN trades >= 15 AND COALESCE(net_pnl,0) < 0 AND COALESCE(avg_net_trade,0) < 0
      THEN 'COOLDOWN_WEAK_OBSERVE'
    ELSE 'OBSERVE'
  END AS cooldown_reason
FROM public.v_strategy_regime_confidence_30d;

CREATE OR REPLACE VIEW public.v_orc_promote_candidates_v63 AS
SELECT
  strategy,
  symbol,
  interval,
  market_regime,
  confidence_bucket,
  trades,
  net_pnl,
  avg_net_trade,
  profit_factor_net,
  fee_pressure_pct,
  avg_confidence,
  (
    trades >= 20
    AND COALESCE(net_pnl,0) > 0
    AND COALESCE(profit_factor_net,0) >= 1.20
    AND COALESCE(avg_confidence,0) >= 0.65
    AND COALESCE(fee_pressure_pct,999) < 50
  ) AS promote_candidate,
  CASE
    WHEN trades >= 20 AND COALESCE(net_pnl,0) > 0 AND COALESCE(profit_factor_net,0) >= 1.20 AND COALESCE(avg_confidence,0) >= 0.65 AND COALESCE(fee_pressure_pct,999) < 50
      THEN 'PROMOTE_CONFIRMED_NET_POSITIVE_PF_GTE_1_20'
    WHEN trades >= 10 AND COALESCE(net_pnl,0) > 0 AND COALESCE(avg_net_trade,0) > 0
      THEN 'PROMOTE_WEAK_OBSERVE'
    ELSE 'OBSERVE'
  END AS promote_reason,
  CASE
    WHEN trades >= 20 AND COALESCE(net_pnl,0) > 0 AND COALESCE(profit_factor_net,0) >= 1.20 AND COALESCE(avg_confidence,0) >= 0.65 AND COALESCE(fee_pressure_pct,999) < 50
      THEN CASE
        WHEN strategy = 'SUPERTREND' THEN 0.15
        WHEN strategy = 'TREND' THEN 0.05
        ELSE 0.03
      END
    ELSE 0.00
  END::numeric AS promote_bonus
FROM public.v_strategy_regime_confidence_30d;

CREATE OR REPLACE VIEW public.v_orc_v63_candidates AS
WITH latest_regime AS (
  SELECT
    symbol,
    interval,
    raw_regime,
    raw_confidence,
    hysteresis_regime,
    hysteresis_confidence,
    raw_ts AS current_regime_ts,
    hysteresis_reason,
    hysteresis_holding_previous,
    CASE
      WHEN hysteresis_confidence >= 0.85 THEN 'HIGH_CONF'
      WHEN hysteresis_confidence >= 0.65 THEN 'MID_CONF'
      WHEN hysteresis_confidence IS NOT NULL THEN 'LOW_CONF'
      ELSE 'NO_CONF'
    END AS hysteresis_confidence_bucket
  FROM public.v_market_regime_hysteresis_latest
), base AS (
  SELECT
    c.*,
    lr.raw_regime AS current_raw_regime,
    lr.raw_confidence AS current_raw_confidence,
    COALESCE(lr.hysteresis_regime, c.current_regime) AS current_hysteresis_regime,
    COALESCE(lr.hysteresis_confidence, c.current_confidence) AS current_hysteresis_confidence,
    COALESCE(lr.hysteresis_confidence_bucket, c.current_confidence_bucket) AS current_hysteresis_confidence_bucket,
    lr.current_regime_ts AS hysteresis_regime_ts,
    lr.hysteresis_reason,
    COALESCE(lr.hysteresis_holding_previous, false) AS hysteresis_holding_previous,
    cd.trades AS cooldown_trades_30d,
    cd.net_pnl AS cooldown_net_30d,
    cd.profit_factor_net AS cooldown_pf_30d,
    cd.cooldown_strong,
    cd.cooldown_reason,
    pr.trades AS promote_trades_30d,
    pr.net_pnl AS promote_net_30d,
    pr.profit_factor_net AS promote_pf_30d,
    pr.promote_candidate,
    pr.promote_reason,
    COALESCE(pr.promote_bonus,0.00)::numeric AS promote_bonus
  FROM public.v_orc_v62_candidates c
  LEFT JOIN latest_regime lr
    ON lr.symbol = c.symbol
   AND lr.interval = c.interval
  LEFT JOIN public.v_orc_cooldown_candidates_v63 cd
    ON cd.symbol = c.symbol
   AND cd.interval = c.interval
   AND cd.strategy = c.strategy
   AND cd.market_regime = COALESCE(lr.hysteresis_regime, c.current_regime)
   AND cd.confidence_bucket = COALESCE(lr.hysteresis_confidence_bucket, c.current_confidence_bucket)
  LEFT JOIN public.v_orc_promote_candidates_v63 pr
    ON pr.symbol = c.symbol
   AND pr.interval = c.interval
   AND pr.strategy = c.strategy
   AND pr.market_regime = COALESCE(lr.hysteresis_regime, c.current_regime)
   AND pr.confidence_bucket = COALESCE(lr.hysteresis_confidence_bucket, c.current_confidence_bucket)
), scored AS (
  SELECT
    b.*,
    (COALESCE(b.compatibility_score,0.00) + COALESCE(b.promote_bonus,0.00))::numeric AS v63_score,
    (
      b.eligible_v62
      AND NOT COALESCE(b.cooldown_strong,false)
    ) AS eligible_v63,
    CASE
      WHEN COALESCE(b.cooldown_strong,false) THEN 'REJECT_COOLDOWN_STRONG'
      WHEN NOT b.eligible_v62 THEN b.v62_reason
      WHEN COALESCE(b.promote_candidate,false) THEN 'PICK_PROMOTE_BONUS_' || b.promote_reason
      WHEN b.hysteresis_holding_previous THEN 'PICK_CORE_WITH_HYSTERESIS_' || COALESCE(b.hysteresis_reason,'HYSTERESIS')
      ELSE b.v62_reason
    END AS v63_reason
  FROM base b
)
SELECT *
FROM scored;

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
    c.current_hysteresis_regime AS current_regime,
    c.current_hysteresis_confidence AS current_confidence,
    c.current_hysteresis_confidence_bucket AS current_confidence_bucket,
    c.regime_trades_14d,
    c.regime_net_14d,
    c.regime_pf_14d,
    c.v63_score AS compatibility_score,
    c.v63_reason,
    c.promote_bonus,
    GREATEST(
      COALESCE(c.last_exit_ts_3d, '1970-01-01'::timestamptz),
      COALESCE(c.last_ts_24h, '1970-01-01'::timestamptz),
      COALESCE(c.last_signal_ts, '1970-01-01'::timestamptz)
    ) AS rank_last_ts
  FROM public.v_orc_v63_candidates c
  WHERE c.eligible_v63
), dedup_symbol_interval AS (
  SELECT
    c.*,
    row_number() OVER (
      PARTITION BY c.symbol, c.interval
      ORDER BY
        c.compatibility_score DESC,
        c.promote_bonus DESC,
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
), ranked AS (
  SELECT
    d.*,
    row_number() OVER (
      ORDER BY
        d.compatibility_score DESC,
        d.promote_bonus DESC,
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
), policy AS (
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

CREATE OR REPLACE VIEW public.v_orc_v63_explain AS
SELECT
  c.symbol,
  c.interval,
  c.strategy,
  c.n_trades_3d,
  c.net_sum_3d,
  c.profit_factor_3d,
  c.current_raw_regime,
  c.current_raw_confidence,
  c.current_hysteresis_regime,
  c.current_hysteresis_confidence,
  c.current_hysteresis_confidence_bucket,
  c.hysteresis_reason,
  c.hysteresis_holding_previous,
  c.regime_trades_14d,
  c.regime_net_14d,
  c.regime_pf_14d,
  c.compatibility_score AS v62_compatibility_score,
  c.promote_bonus,
  c.v63_score,
  c.passes_v62_core,
  c.rsi_trend_penalty,
  c.positive_regime_compatibility,
  c.negative_regime_compatibility,
  c.cooldown_trades_30d,
  c.cooldown_net_30d,
  c.cooldown_pf_30d,
  c.cooldown_strong,
  c.cooldown_reason,
  c.promote_trades_30d,
  c.promote_net_30d,
  c.promote_pf_30d,
  c.promote_candidate,
  c.promote_reason,
  c.eligible_v63,
  c.v63_reason,
  CASE WHEN p.symbol IS NOT NULL THEN true ELSE false END AS picked_now,
  p.final_rn,
  bc.live_orders_enabled,
  bc.reason AS bot_control_reason
FROM public.v_orc_v63_candidates c
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
  ('orc_active_version', 'ORC_V6_3', now()),
  ('orc_active_mode', 'COOLDOWN_PROMOTE_HYSTERESIS', now()),
  ('orc_v63_explore_enabled', '0', now())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;

COMMIT;

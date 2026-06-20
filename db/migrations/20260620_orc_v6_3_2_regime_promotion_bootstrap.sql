BEGIN;

DROP VIEW IF EXISTS v_orc_v63_candidates_core;

CREATE OR REPLACE VIEW v_orc_v632_regime_bootstrap_candidates AS
SELECT
  1 AS prio,
  c.symbol,
  c.interval,
  c.strategy,
  COALESCE(pr.n_trades, c.n_trades_3d, 0)::bigint AS n_trades_3d,
  COALESCE(pr.net_sum, c.net_sum_3d, 0)::numeric AS net_sum_3d,
  COALESCE(pr.profit_factor, c.profit_factor_3d, 0)::numeric AS profit_factor_3d,
  c.last_exit_ts_3d,
  c.n_buy_24h,
  c.n_runs_24h,
  c.n_filter_block_24h,
  c.filter_block_rate_24h,
  c.last_ts_24h,
  c.n_signal_15m,
  c.last_signal_ts,
  true AS eligible_pick_v5,
  true AS eligible_bootstrap_v5,
  c.eligible_signal_v5,
  c.eligible_activity_v5,
  false AS eligible_softfill_v5,
  COALESCE(pr.paper_score, 0) + 0.25 AS rank_score,
  0.25::numeric AS promote_bonus,
  GREATEST(
    COALESCE(c.last_exit_ts_3d, '1970-01-01'::timestamptz),
    COALESCE(c.last_ts_24h, '1970-01-01'::timestamptz),
    COALESCE(c.last_signal_ts, '1970-01-01'::timestamptz),
    COALESCE(pr.published_at, '1970-01-01'::timestamptz)
  ) AS rank_last_ts
FROM v_orc_v63_candidates c
JOIN promoted_regime_candidates pr
  ON pr.symbol = c.symbol
 AND pr.interval = c.interval
 AND pr.strategy = c.strategy
 AND pr.market_regime = c.current_hysteresis_regime
WHERE pr.eligible_live = true
  AND COALESCE(c.current_hysteresis_confidence, c.current_confidence, 0) >= 0.80
  AND COALESCE(pr.n_trades, 0) >= 20
  AND COALESCE(pr.net_sum, 0) > 0
  AND COALESCE(pr.profit_factor, 0) >= 1.20
  AND COALESCE(pr.fee_pressure_pct, 999) <= 65
  AND NOT COALESCE(c.cooldown_strong, false);

CREATE OR REPLACE VIEW v_orc_picks_v5 AS
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
    c.v63_score AS rank_score,
    c.promote_bonus,
    GREATEST(
      COALESCE(c.last_exit_ts_3d, '1970-01-01'::timestamptz),
      COALESCE(c.last_ts_24h, '1970-01-01'::timestamptz),
      COALESCE(c.last_signal_ts, '1970-01-01'::timestamptz)
    ) AS rank_last_ts
  FROM v_orc_v63_candidates c
  WHERE c.eligible_v63 = true

  UNION ALL

  SELECT
    prio,
    symbol,
    interval,
    strategy,
    n_trades_3d,
    net_sum_3d,
    profit_factor_3d,
    last_exit_ts_3d,
    n_buy_24h,
    n_runs_24h,
    n_filter_block_24h,
    filter_block_rate_24h,
    last_ts_24h,
    n_signal_15m,
    last_signal_ts,
    eligible_pick_v5,
    eligible_bootstrap_v5,
    eligible_signal_v5,
    eligible_activity_v5,
    eligible_softfill_v5,
    rank_score,
    promote_bonus,
    rank_last_ts
  FROM v_orc_v632_regime_bootstrap_candidates
),
dedup_symbol_interval AS (
  SELECT
    c.*,
    row_number() OVER (
      PARTITION BY c.symbol, c.interval
      ORDER BY
        c.rank_score DESC,
        c.promote_bonus DESC,
        c.net_sum_3d DESC,
        c.profit_factor_3d DESC,
        c.n_trades_3d DESC,
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
        d.rank_score DESC,
        d.promote_bonus DESC,
        d.net_sum_3d DESC,
        d.profit_factor_3d DESC,
        d.n_trades_3d DESC,
        d.n_signal_15m DESC,
        d.n_buy_24h DESC,
        d.rank_last_ts DESC,
        d.symbol,
        d.interval,
        d.strategy
    ) AS final_rn
  FROM dedup_symbol_interval d
  WHERE d.si_rn = 1
),
policy AS (
  SELECT COALESCE((
    SELECT NULLIF(to_jsonb(ap.*)->>'max_picks','')::integer
    FROM allocation_policy ap
    WHERE COALESCE((to_jsonb(ap.*)->>'enabled')::boolean, true)
    LIMIT 1
  ), 8) AS max_picks
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

INSERT INTO automation_kv(key, value)
VALUES
  ('orc_active_version', 'ORC_V6_3_2'),
  ('orc_active_mode', 'REGIME_PROMOTION_BOOTSTRAP_PICKS'),
  ('orc_v632_regime_promotion_bootstrap_enabled', '1')
ON CONFLICT (key)
DO UPDATE SET value = EXCLUDED.value;

COMMIT;

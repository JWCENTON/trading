BEGIN;

CREATE VIEW IF NOT EXISTS v_orc_v63_candidates_core AS
SELECT *
FROM v_orc_v63_candidates;

CREATE OR REPLACE VIEW v_orc_v63_candidates AS
WITH rp AS (
  SELECT
    symbol,
    interval,
    strategy,
    market_regime,
    paper_score,
    n_trades,
    win_rate,
    net_sum,
    profit_factor,
    fee_pressure_pct,
    published_at
  FROM promoted_regime_candidates
  WHERE eligible_live = true
),
joined AS (
  SELECT
    c.*,
    rp.paper_score AS rp_paper_score,
    rp.n_trades AS rp_n_trades,
    rp.win_rate AS rp_win_rate,
    rp.net_sum AS rp_net_sum,
    rp.profit_factor AS rp_profit_factor,
    rp.fee_pressure_pct AS rp_fee_pressure_pct,
    rp.published_at AS rp_published_at,
    (
      rp.symbol IS NOT NULL
      AND c.current_hysteresis_regime = rp.market_regime
      AND COALESCE(c.current_hysteresis_confidence, c.current_confidence, 0) >= 0.80
      AND COALESCE(rp.n_trades, 0) >= 20
      AND COALESCE(rp.net_sum, 0) > 0
      AND COALESCE(rp.profit_factor, 0) >= 1.20
      AND COALESCE(rp.fee_pressure_pct, 999) <= 65
    ) AS regime_promotion_bootstrap
  FROM v_orc_v63_candidates_core c
  LEFT JOIN rp
    ON rp.symbol = c.symbol
   AND rp.interval = c.interval
   AND rp.strategy = c.strategy
   AND rp.market_regime = c.current_hysteresis_regime
)
SELECT
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
  current_regime,
  current_confidence,
  current_regime_ts,
  current_confidence_bucket,
  regime_trades_14d,
  regime_net_14d,
  regime_avg_net_14d,
  regime_pf_14d,
  regime_win_rate_14d,
  regime_fee_pressure_14d,
  regime_avg_confidence_14d,
  CASE
    WHEN regime_promotion_bootstrap THEN true
    ELSE passes_v62_core
  END AS passes_v62_core,
  rsi_trend_penalty,
  CASE
    WHEN regime_promotion_bootstrap THEN true
    ELSE positive_regime_compatibility
  END AS positive_regime_compatibility,
  negative_regime_compatibility,
  CASE
    WHEN regime_promotion_bootstrap
      THEN GREATEST(COALESCE(compatibility_score, 0), COALESCE(rp_paper_score, 0) + 0.25)
    ELSE compatibility_score
  END AS compatibility_score,
  CASE
    WHEN regime_promotion_bootstrap THEN true
    ELSE eligible_v62
  END AS eligible_v62,
  CASE
    WHEN regime_promotion_bootstrap THEN 'PICK_REGIME_PROMOTION_BOOTSTRAP'
    ELSE v62_reason
  END AS v62_reason,
  current_raw_regime,
  current_raw_confidence,
  current_hysteresis_regime,
  current_hysteresis_confidence,
  current_hysteresis_confidence_bucket,
  hysteresis_regime_ts,
  hysteresis_reason,
  hysteresis_holding_previous,
  cooldown_trades_30d,
  cooldown_net_30d,
  cooldown_pf_30d,
  cooldown_strong,
  cooldown_reason,
  promote_trades_30d,
  promote_net_30d,
  promote_pf_30d,
  promote_candidate,
  CASE
    WHEN regime_promotion_bootstrap THEN
      concat(
        'REGIME_PROMOTION_BOOTSTRAP',
        '_paper_score=', COALESCE(rp_paper_score::text, '0'),
        '_paper_pf=', COALESCE(rp_profit_factor::text, '0'),
        '_paper_trades=', COALESCE(rp_n_trades::text, '0')
      )
    ELSE promote_reason
  END AS promote_reason,
  CASE
    WHEN regime_promotion_bootstrap
      THEN GREATEST(COALESCE(promote_bonus, 0), 0.25)
    ELSE promote_bonus
  END AS promote_bonus,
  CASE
    WHEN regime_promotion_bootstrap
      THEN GREATEST(COALESCE(v63_score, 0), COALESCE(rp_paper_score, 0) + 0.25)
    ELSE v63_score
  END AS v63_score,
  CASE
    WHEN regime_promotion_bootstrap AND NOT COALESCE(cooldown_strong, false) THEN true
    ELSE eligible_v63
  END AS eligible_v63,
  CASE
    WHEN regime_promotion_bootstrap AND COALESCE(cooldown_strong, false) THEN 'REJECT_COOLDOWN_STRONG'
    WHEN regime_promotion_bootstrap THEN 'PICK_REGIME_PROMOTION_BOOTSTRAP'
    ELSE v63_reason
  END AS v63_reason
FROM joined;

INSERT INTO automation_kv(key, value)
VALUES
  ('orc_active_version', 'ORC_V6_3_2'),
  ('orc_active_mode', 'REGIME_PROMOTION_BOOTSTRAP'),
  ('orc_v632_regime_promotion_bootstrap_enabled', '1'),
  ('orc_v632_regime_promotion_min_confidence', '0.80')
ON CONFLICT (key)
DO UPDATE SET value = EXCLUDED.value;

COMMIT;

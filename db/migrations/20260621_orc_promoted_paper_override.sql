BEGIN;

DO $$
BEGIN
  IF to_regclass('public.promoted_regime_candidates') IS NULL THEN
    RAISE NOTICE 'promoted_regime_candidates missing; skipping ORC promoted paper override';
    RETURN;
  END IF;
END $$;

CREATE OR REPLACE VIEW v_orc_v632_regime_bootstrap_candidates AS
WITH base AS (
  SELECT
    c.*,
    pr.paper_score,
    pr.n_trades,
    pr.net_sum,
    pr.profit_factor,
    pr.fee_pressure_pct,
    pr.published_at,
    COALESCE(
      c.current_hysteresis_confidence,
      c.current_confidence,
      conf.last_non_null_confidence,
      0
    ) AS effective_confidence,
    conf.last_non_null_confidence,
    conf.last_non_null_confidence_ts
  FROM v_orc_v63_candidates c
  JOIN promoted_regime_candidates pr
    ON pr.symbol = c.symbol
   AND pr.interval = c.interval
   AND pr.strategy = c.strategy
   AND pr.market_regime = c.current_hysteresis_regime
  LEFT JOIN LATERAL (
    SELECT
      mr.confidence AS last_non_null_confidence,
      mr.ts AS last_non_null_confidence_ts
    FROM market_regime mr
    WHERE mr.symbol = c.symbol
      AND mr.interval = c.interval
      AND mr.regime = c.current_hysteresis_regime
      AND mr.confidence IS NOT NULL
      AND mr.ts >= now() - interval '60 minutes'
    ORDER BY mr.ts DESC
    LIMIT 1
  ) conf ON true
  WHERE pr.eligible_live = true
    AND COALESCE(pr.n_trades, 0) >= 20
    AND COALESCE(pr.net_sum, 0) > 0
    AND COALESCE(pr.profit_factor, 0) >= 1.20
    AND COALESCE(pr.fee_pressure_pct, 999) <= 65
    AND NOT COALESCE(c.cooldown_strong, false)
    AND COALESCE(
      c.current_hysteresis_confidence,
      c.current_confidence,
      conf.last_non_null_confidence,
      0
    ) >= 0.70
    AND NOT (
      c.strategy = 'BBRANGE'
      AND c.current_hysteresis_regime NOT IN ('RANGE_LOWVOL', 'RANGE_HIGHVOL')
    )
)
SELECT
  1 AS prio,
  symbol,
  interval,
  strategy,
  COALESCE(n_trades::bigint, n_trades_3d, 0::bigint) AS n_trades_3d,
  COALESCE(net_sum, net_sum_3d, 0::numeric) AS net_sum_3d,
  COALESCE(profit_factor, profit_factor_3d, 0::numeric) AS profit_factor_3d,
  last_exit_ts_3d,
  n_buy_24h,
  n_runs_24h,
  n_filter_block_24h,
  filter_block_rate_24h,
  last_ts_24h,
  n_signal_15m,
  last_signal_ts,
  true AS eligible_pick_v5,
  true AS eligible_bootstrap_v5,
  eligible_signal_v5,
  eligible_activity_v5,
  false AS eligible_softfill_v5,
  COALESCE(paper_score, 0::numeric) + 0.25 AS rank_score,
  0.25 AS promote_bonus,
  GREATEST(
    COALESCE(last_exit_ts_3d, '1970-01-01'::timestamptz),
    COALESCE(last_ts_24h, '1970-01-01'::timestamptz),
    COALESCE(last_signal_ts, '1970-01-01'::timestamptz),
    COALESCE(published_at, '1970-01-01'::timestamptz),
    COALESCE(last_non_null_confidence_ts, '1970-01-01'::timestamptz)
  ) AS rank_last_ts
FROM base;

INSERT INTO automation_kv(key, value)
VALUES
  ('orc_promoted_paper_override_enabled', '1'),
  ('orc_promoted_paper_override_min_confidence', '0.70'),
  ('orc_promoted_paper_override_version', '20260621')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

COMMIT;

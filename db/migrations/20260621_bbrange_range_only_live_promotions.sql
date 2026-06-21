BEGIN;

-- WALTRADE ORC safety patch:
-- BBRANGE is a range/flat strategy. Do not allow PAPER promotions or ORC
-- bootstrap picks to turn BBRANGE LIVE in TREND_UP/TREND_DOWN.
-- This fixes the E2E conflict:
--   ORC promotes BBRANGE + TREND_UP
--   bot_bbrange runtime blocks it as TREND_NOT_FLAT

CREATE OR REPLACE VIEW v_slot_profile_v1_14d AS
WITH base AS (
  SELECT
    p.strategy,
    p.symbol,
    p.interval,
    COALESCE(p.market_regime, 'UNKNOWN') AS market_regime,

    COUNT(*) AS trades,
    COUNT(*) FILTER (WHERE p.net_pnl_usdc > 0) AS wins,
    COUNT(*) FILTER (WHERE p.net_pnl_usdc <= 0) AS losses,

    SUM(p.net_pnl_usdc) AS net_pnl_raw,
    AVG(p.net_pnl_usdc) AS avg_net_trade_raw,
    SUM(p.gross_pnl_usdc) AS gross_pnl_raw,
    SUM(p.fees_usdc) AS fees_raw,

    CASE
      WHEN ABS(SUM(p.net_pnl_usdc) FILTER (WHERE p.net_pnl_usdc < 0)) > 0
        THEN
          SUM(p.net_pnl_usdc) FILTER (WHERE p.net_pnl_usdc > 0)
          / ABS(SUM(p.net_pnl_usdc) FILTER (WHERE p.net_pnl_usdc < 0))
      ELSE NULL
    END AS pf_raw,

    CASE
      WHEN ABS(SUM(p.gross_pnl_usdc)) > 0
        THEN SUM(p.fees_usdc) / ABS(SUM(p.gross_pnl_usdc)) * 100
      ELSE NULL
    END AS fee_pressure_raw,

    100.0 * COUNT(*) FILTER (WHERE p.net_pnl_usdc > 0) / NULLIF(COUNT(*), 0) AS win_rate_raw

  FROM positions p
  WHERE p.status = 'CLOSED'
    AND p.exit_time >= now() - interval '14 days'
    AND p.net_pnl_usdc IS NOT NULL
  GROUP BY
    p.strategy,
    p.symbol,
    p.interval,
    COALESCE(p.market_regime, 'UNKNOWN')
)
SELECT
  strategy,
  symbol,
  interval,
  market_regime,

  trades,
  wins,
  losses,

  ROUND(net_pnl_raw::numeric, 8) AS net_pnl,
  ROUND(avg_net_trade_raw::numeric, 8) AS avg_net_trade,
  ROUND(win_rate_raw::numeric, 2) AS win_rate_pct,
  ROUND(gross_pnl_raw::numeric, 8) AS gross_pnl,
  ROUND(fees_raw::numeric, 8) AS fees,
  ROUND(fee_pressure_raw::numeric, 4) AS fee_pressure_pct,
  ROUND(pf_raw::numeric, 6) AS profit_factor_net,

  CASE
    -- BBRANGE can be promoted LIVE only in RANGE regimes.
    -- Even if PAPER showed profit in TREND_UP, LIVE runtime blocks BBRANGE in trend,
    -- so allowing it here creates ORC/runtime disagreement.
    WHEN strategy = 'BBRANGE'
      AND market_regime NOT IN ('RANGE_LOWVOL', 'RANGE_HIGHVOL')
      THEN 'OBSERVE'

    WHEN strategy = 'BBRANGE'
      AND market_regime IN ('RANGE_LOWVOL', 'RANGE_HIGHVOL')
      AND trades >= 20
      AND net_pnl_raw > 0
      AND avg_net_trade_raw > 0
      AND COALESCE(pf_raw, 999) >= 1.20
      AND COALESCE(fee_pressure_raw, 999) <= 65
      AND win_rate_raw >= 55
      THEN 'ALLOW_LIVE_CANDIDATE'

    WHEN trades >= 20
      AND net_pnl_raw < 0
      THEN 'BLOCK_LIVE'

    ELSE 'OBSERVE'
  END AS edge_status

FROM base;

-- Defense-in-depth: only apply promotion hardening when promoted_regime_candidates exists.
DO $$
BEGIN
  IF to_regclass('public.promoted_regime_candidates') IS NOT NULL THEN
    UPDATE promoted_regime_candidates
    SET
      eligible_live = false,
      elig_reason = 'BLOCK_BBRANGE_NOT_RANGE',
      meta = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
        'blocked_by', '20260621_bbrange_range_only_live_promotions',
        'blocked_reason', 'BBRANGE runtime is range/flat-only'
      )
    WHERE strategy = 'BBRANGE'
      AND market_regime NOT IN ('RANGE_LOWVOL', 'RANGE_HIGHVOL')
      AND eligible_live = true;
  ELSE
    RAISE NOTICE 'promoted_regime_candidates missing; skipping published-promotion update';
  END IF;
END $$;

INSERT INTO automation_kv(key, value)
VALUES
  ('orc_bbrange_range_only_live_promotions', '1'),
  ('orc_bbrange_range_only_live_promotions_version', '20260621')
ON CONFLICT (key)
DO UPDATE SET value = EXCLUDED.value;

COMMIT;

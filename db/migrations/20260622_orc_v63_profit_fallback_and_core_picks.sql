-- ORC V6.3 profit fallback + core pick cleanup
-- Date: 2026-06-22
--
-- Fixes:
-- 1. v_orc_profit_metrics_3d uses v_strategy_regime_edge fallback when 3d live sample is too small.
-- 2. v_orc_candidates_v5c consumes v_orc_profit_metrics_3d instead of stale v_slot_profitability_3d_v5 values.
-- 3. v_orc_v62_candidates lowers core sample threshold from 5 to 3 trades.
-- 4. v_orc_picks_v5 disables legacy bootstrap union branches and picks only eligible_v63=true.

CREATE OR REPLACE VIEW v_orc_profit_metrics_3d AS
WITH
x3 AS (
  SELECT
    symbol,
    "interval",
    strategy,
    realized_pnl_quote,
    CASE WHEN realized_pnl_quote > 0 THEN 1 ELSE 0 END AS is_win
  FROM v_orc_closed_trades_3d
),
agg3 AS (
  SELECT
    symbol,
    "interval",
    strategy,
    COUNT(*) AS n_trades_3d,
    COALESCE(SUM(realized_pnl_quote), 0::numeric) AS net_sum_3d,
    COALESCE(SUM(CASE WHEN realized_pnl_quote > 0 THEN realized_pnl_quote ELSE 0::numeric END), 0::numeric) AS sum_profit_3d,
    COALESCE(SUM(CASE WHEN realized_pnl_quote < 0 THEN realized_pnl_quote ELSE 0::numeric END), 0::numeric) AS sum_loss_3d,
    COALESCE(SUM(is_win), 0)::numeric / NULLIF(COUNT(*)::numeric, 0::numeric) AS win_rate_3d
  FROM x3
  GROUP BY 1,2,3
),
edge AS (
  SELECT
    symbol,
    "interval",
    strategy,
    SUM(trades)::bigint AS n_trades_edge,
    COALESCE(SUM(net_pnl_usdc), 0::numeric) AS net_sum_edge,
    COALESCE(SUM(wins), 0)::numeric / NULLIF(SUM(trades)::numeric, 0::numeric) AS win_rate_edge,
    COALESCE(SUM(CASE WHEN net_pnl_usdc > 0 THEN net_pnl_usdc ELSE 0::numeric END), 0::numeric) AS sum_profit_edge,
    COALESCE(SUM(CASE WHEN net_pnl_usdc < 0 THEN net_pnl_usdc ELSE 0::numeric END), 0::numeric) AS sum_loss_edge
  FROM v_strategy_regime_edge
  GROUP BY 1,2,3
),
merged AS (
  SELECT
    COALESCE(a.symbol, e.symbol) AS symbol,
    COALESCE(a."interval", e."interval") AS "interval",
    COALESCE(a.strategy, e.strategy) AS strategy,

    CASE
      WHEN COALESCE(a.n_trades_3d, 0) >= 3 THEN a.n_trades_3d
      ELSE COALESCE(e.n_trades_edge, a.n_trades_3d, 0)
    END AS n_trades_3d,

    CASE
      WHEN COALESCE(a.n_trades_3d, 0) >= 3 THEN a.net_sum_3d
      ELSE COALESCE(e.net_sum_edge, a.net_sum_3d, 0::numeric)
    END AS net_sum_3d,

    CASE
      WHEN COALESCE(a.n_trades_3d, 0) >= 3 THEN COALESCE(a.win_rate_3d, 0::numeric)
      ELSE COALESCE(e.win_rate_edge, a.win_rate_3d, 0::numeric)
    END AS win_rate_3d,

    CASE
      WHEN COALESCE(a.n_trades_3d, 0) >= 3 THEN a.sum_profit_3d
      ELSE COALESCE(e.sum_profit_edge, a.sum_profit_3d, 0::numeric)
    END AS sum_profit_3d,

    CASE
      WHEN COALESCE(a.n_trades_3d, 0) >= 3 THEN a.sum_loss_3d
      ELSE COALESCE(e.sum_loss_edge, a.sum_loss_3d, 0::numeric)
    END AS sum_loss_3d
  FROM agg3 a
  FULL OUTER JOIN edge e
    ON e.symbol = a.symbol
   AND e."interval" = a."interval"
   AND e.strategy = a.strategy
)
SELECT
  symbol,
  "interval",
  strategy,
  n_trades_3d,
  net_sum_3d,
  COALESCE(win_rate_3d, 0::numeric) AS win_rate_3d,
  CASE
    WHEN n_trades_3d = 0 THEN 0::numeric
    WHEN sum_loss_3d = 0::numeric AND sum_profit_3d > 0::numeric THEN 999::numeric
    WHEN sum_loss_3d = 0::numeric AND sum_profit_3d = 0::numeric THEN 0::numeric
    ELSE sum_profit_3d / ABS(sum_loss_3d)
  END AS profit_factor_3d
FROM merged;



CREATE OR REPLACE VIEW v_orc_candidates_v5c AS
WITH p AS (
  SELECT
    old.symbol,
    old."interval",
    old.strategy,
    COALESCE(m.n_trades_3d, old.n_trades_3d, 0) AS n_trades_3d,
    COALESCE(m.net_sum_3d, old.net_sum_3d, 0::numeric) AS net_sum_3d,
    COALESCE(m.profit_factor_3d, old.profit_factor_3d, 0::numeric) AS profit_factor_3d,
    old.last_exit_ts_3d,
    (
      COALESCE(m.n_trades_3d, old.n_trades_3d, 0) >= 1
      AND COALESCE(m.net_sum_3d, old.net_sum_3d, 0::numeric) > 0
      AND COALESCE(m.profit_factor_3d, old.profit_factor_3d, 0::numeric) >= 1.05
    ) AS is_pick_profitable_3d
  FROM v_slot_profitability_3d_v5 old
  LEFT JOIN v_orc_profit_metrics_3d m
    ON m.symbol = old.symbol
   AND m."interval" = old."interval"
   AND m.strategy = old.strategy
)
SELECT
  p.symbol,
  p."interval",
  p.strategy,
  p.n_trades_3d,
  p.net_sum_3d,
  p.profit_factor_3d,
  p.last_exit_ts_3d,
  COALESCE(a.n_buy_24h, 0::bigint) AS n_buy_24h,
  COALESCE(a.n_runs_24h, 0::bigint) AS n_runs_24h,
  COALESCE(a.n_filter_block_24h, 0::bigint) AS n_filter_block_24h,
  COALESCE(a.filter_block_rate_24h, 0::numeric) AS filter_block_rate_24h,
  a.last_ts_24h,
  COALESCE(s.n_signal_15m, 0::bigint) AS n_signal_15m,
  s.last_signal_ts,

  (
    p.is_pick_profitable_3d
    AND NOT (
      p.strategy = 'RSI'
      AND p.symbol = 'BTCUSDC'
      AND p."interval" = '1m'
      AND (
        COALESCE(p.net_sum_3d, 0::numeric) < 0
        OR COALESCE(p.profit_factor_3d, 0::numeric) < 1
        OR COALESCE(a.filter_block_rate_24h, 0::numeric) > 0.50
      )
    )
  ) AS eligible_pick_v5,

  (
    (
      p.strategy <> 'TREND'
      AND COALESCE(a.n_buy_24h, 0::bigint) >= 8
      AND COALESCE(a.filter_block_rate_24h, 0::numeric) <= 0.70
      AND (
        COALESCE(p.n_trades_3d, 0::bigint) >= 1
        OR COALESCE(s.n_signal_15m, 0::bigint) > 0
      )
    )
    OR (
      p.strategy = ANY (ARRAY['BBRANGE','SUPERTREND'])
      AND COALESCE(p.n_trades_3d, 0::bigint) = 0
      AND (
        COALESCE(a.n_buy_24h, 0::bigint) >= 20
        OR COALESCE(s.n_signal_15m, 0::bigint) > 0
      )
      AND COALESCE(a.filter_block_rate_24h, 0::numeric) <= 0.55
    )
  ) AS eligible_bootstrap_v5,

  (
    COALESCE(s.n_signal_15m, 0::bigint) > 0
    AND COALESCE(a.n_buy_24h, 0::bigint) >= 4
    AND COALESCE(a.filter_block_rate_24h, 0::numeric) <= 0.70
  ) AS eligible_signal_v5,

  (
    COALESCE(a.n_buy_24h, 0::bigint) >= 12
    AND COALESCE(a.filter_block_rate_24h, 0::numeric) <= 0.50
    AND COALESCE(p.n_trades_3d, 0::bigint) >= 1
    AND COALESCE(p.net_sum_3d, 0::numeric) >= 0
  ) AS eligible_activity_v5,

  (
    (
      COALESCE(a.n_buy_24h, 0::bigint) >= 12
      AND COALESCE(a.filter_block_rate_24h, 0::numeric) <= 0.50
      AND (
        COALESCE(p.n_trades_3d, 0::bigint) >= 1
        OR COALESCE(s.n_signal_15m, 0::bigint) > 0
      )
    )
    OR (
      p.strategy = ANY (ARRAY['BBRANGE','SUPERTREND'])
      AND COALESCE(p.n_trades_3d, 0::bigint) = 0
      AND COALESCE(a.n_buy_24h, 0::bigint) >= 18
      AND COALESCE(a.filter_block_rate_24h, 0::numeric) <= 0.55
    )
  ) AS eligible_softfill_v5
FROM p
LEFT JOIN v_orc_activity_24h a
  ON a.symbol = p.symbol
 AND a."interval" = p."interval"
 AND a.strategy = p.strategy
LEFT JOIN v_orc_signal_15m s
  ON s.symbol = p.symbol
 AND s."interval" = p."interval"
 AND s.strategy = p.strategy;
 
 
 
 CREATE OR REPLACE VIEW v_orc_v62_candidates AS
WITH latest_regime AS (
  SELECT DISTINCT ON (symbol, "interval")
    symbol,
    "interval",
    regime AS current_regime,
    confidence AS current_confidence,
    ts AS current_regime_ts,
    CASE
      WHEN confidence >= 0.85 THEN 'HIGH_CONF'
      WHEN confidence >= 0.65 THEN 'MID_CONF'
      WHEN confidence IS NOT NULL THEN 'LOW_CONF'
      ELSE 'NO_CONF'
    END AS current_confidence_bucket
  FROM market_regime
  ORDER BY symbol, "interval", ts DESC
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
  FROM v_orc_candidates_v5c c
  LEFT JOIN latest_regime lr
    ON lr.symbol = c.symbol
   AND lr."interval" = c."interval"
  LEFT JOIN v_strategy_regime_confidence_14d sr
    ON sr.symbol = c.symbol
   AND sr."interval" = c."interval"
   AND sr.strategy = c.strategy
   AND sr.market_regime = lr.current_regime
   AND sr.confidence_bucket = lr.current_confidence_bucket
),
scored AS (
  SELECT
    b.*,

    (
      b.n_trades_3d >= 3
      AND b.net_sum_3d > 0::numeric
      AND b.profit_factor_3d >= 1.05
    ) AS passes_v62_core,

    (
      b.strategy = 'RSI'
      AND b.current_regime = ANY (ARRAY['TREND_UP','TREND_DOWN'])
      AND COALESCE(b.current_confidence, 0::numeric) >= 0.65
    ) AS rsi_trend_penalty,

    (
      COALESCE(b.regime_trades_14d, 0) >= 5
      AND COALESCE(b.regime_net_14d, 0::numeric) > 0
      AND COALESCE(b.regime_pf_14d, 0::numeric) >= 1.05
    ) AS positive_regime_compatibility,

    (
      COALESCE(b.regime_trades_14d, 0) >= 5
      AND COALESCE(b.regime_net_14d, 0::numeric) < 0
    ) AS negative_regime_compatibility,

    CASE
      WHEN b.strategy = 'TREND'
       AND b.current_regime = 'TREND_UP'
       AND COALESCE(b.current_confidence, 0::numeric) >= 0.85
       AND COALESCE(b.regime_net_14d, 0::numeric) > 0 THEN 0.35

      WHEN b.strategy = 'SUPERTREND'
       AND b.current_regime = ANY (ARRAY['TREND_UP','TREND_DOWN'])
       AND COALESCE(b.current_confidence, 0::numeric) >= 0.85
       AND COALESCE(b.regime_net_14d, 0::numeric) > 0 THEN 0.35

      WHEN b.strategy = 'RSI'
       AND b.current_regime = ANY (ARRAY['TREND_UP','TREND_DOWN'])
       AND COALESCE(b.current_confidence, 0::numeric) >= 0.65 THEN -0.50

      WHEN COALESCE(b.regime_trades_14d, 0) >= 5
       AND COALESCE(b.regime_net_14d, 0::numeric) < 0 THEN -0.35

      ELSE 0.00
    END AS compatibility_score
  FROM base b
)
SELECT
  symbol,
  "interval",
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
  passes_v62_core,
  rsi_trend_penalty,
  positive_regime_compatibility,
  negative_regime_compatibility,
  compatibility_score,
  (
    passes_v62_core
    AND NOT rsi_trend_penalty
    AND NOT (
      negative_regime_compatibility
      AND COALESCE(regime_trades_14d, 0) >= 10
      AND COALESCE(regime_pf_14d, 0::numeric) < 1.0
    )
  ) AS eligible_v62,
  CASE
    WHEN NOT passes_v62_core THEN 'REJECT_CORE_NET_OR_PF'
    WHEN rsi_trend_penalty THEN 'REJECT_RSI_TREND_CONFIDENCE_PENALTY'
    WHEN negative_regime_compatibility
      AND COALESCE(regime_trades_14d, 0) >= 10
      AND COALESCE(regime_pf_14d, 0::numeric) < 1.0
      THEN 'REJECT_NEGATIVE_REGIME_COMPATIBILITY'
    WHEN positive_regime_compatibility THEN 'PICK_POSITIVE_REGIME_COMPATIBILITY'
    ELSE 'PICK_CORE_NET_AWARE'
  END AS v62_reason
FROM scored;

-- 4) Pick layer: use only eligible V6.3 candidates, no legacy bootstrap union branches.
CREATE OR REPLACE VIEW v_orc_picks_v5 AS
WITH candidates AS (
  SELECT
    1 AS prio,
    c.symbol,
    c."interval",
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
),
dedup_symbol_interval AS (
  SELECT
    c.*,
    row_number() OVER (
      PARTITION BY c.symbol, c."interval"
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
          WHEN c.strategy = 'SUPERTREND' THEN 0
          WHEN c.strategy = 'TREND' THEN 1
          WHEN c.strategy = 'RSI' THEN 2
          WHEN c.strategy = 'BBRANGE' THEN 3
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
        d."interval",
        d.strategy
    ) AS final_rn
  FROM dedup_symbol_interval d
  WHERE d.si_rn = 1
),
policy AS (
  SELECT COALESCE((
    SELECT NULLIF(to_jsonb(ap.*) ->> 'max_picks', '')::integer
    FROM allocation_policy ap
    WHERE COALESCE((to_jsonb(ap.*) ->> 'enabled')::boolean, true)
    LIMIT 1
  ), 8) AS max_picks
)
SELECT
  r.prio,
  r.symbol,
  r."interval",
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

BEGIN;

CREATE OR REPLACE VIEW v_slot_profitability_3d_v5 AS
WITH universe AS (
  SELECT DISTINCT
    b.symbol,
    b.interval,
    b.strategy
  FROM bot_control b
),
closed_3d AS (
  SELECT
    p.symbol,
    p.interval,
    p.strategy,
    p.exit_time,
    COALESCE(
      p.net_pnl_usdc,
      CASE
        WHEN p.side ILIKE 'LONG' OR p.side ILIKE 'BUY'
          THEN (p.exit_price - p.entry_price) * p.qty
        WHEN p.side ILIKE 'SHORT' OR p.side ILIKE 'SELL'
          THEN (p.entry_price - p.exit_price) * p.qty
        ELSE NULL::numeric
      END
    ) AS pnl
  FROM positions p
  WHERE p.status='CLOSED'
    AND p.exit_time IS NOT NULL
    AND p.exit_time >= now() - interval '3 days'
),
agg AS (
  SELECT
    c.symbol,
    c.interval,
    c.strategy,
    COUNT(*) AS n_trades_3d,
    COALESCE(SUM(c.pnl),0::numeric) AS net_sum_3d,
    COALESCE(AVG(c.pnl),0::numeric) AS net_avg_3d,
    COALESCE(AVG(CASE WHEN c.pnl > 0 THEN 1.0 ELSE 0.0 END),0::numeric) AS win_rate_3d,
    COALESCE(SUM(CASE WHEN c.pnl > 0 THEN c.pnl ELSE 0::numeric END),0::numeric) AS gross_profit_3d,
    COALESCE(SUM(CASE WHEN c.pnl < 0 THEN -c.pnl ELSE 0::numeric END),0::numeric) AS gross_loss_3d,
    MAX(c.exit_time) AS last_exit_ts_3d
  FROM closed_3d c
  GROUP BY c.symbol,c.interval,c.strategy
)
SELECT
  u.symbol,
  u.interval,
  u.strategy,
  COALESCE(a.n_trades_3d,0::bigint) AS n_trades_3d,
  COALESCE(a.net_sum_3d,0::numeric) AS net_sum_3d,
  COALESCE(a.net_avg_3d,0::numeric) AS net_avg_3d,
  COALESCE(a.win_rate_3d,0::numeric) AS win_rate_3d,
  CASE
    WHEN COALESCE(a.gross_loss_3d,0::numeric) > 0
      THEN COALESCE(a.gross_profit_3d,0::numeric) / a.gross_loss_3d
    WHEN COALESCE(a.gross_profit_3d,0::numeric) > 0
      THEN 999999::numeric
    ELSE 0::numeric
  END AS profit_factor_3d,
  a.last_exit_ts_3d,
  COALESCE(a.n_trades_3d,0::bigint) >= 5
    AND COALESCE(a.net_sum_3d,0::numeric) > 0
    AND (
      CASE
        WHEN COALESCE(a.gross_loss_3d,0::numeric) > 0
          THEN COALESCE(a.gross_profit_3d,0::numeric) / a.gross_loss_3d
        WHEN COALESCE(a.gross_profit_3d,0::numeric) > 0
          THEN 999999::numeric
        ELSE 0::numeric
      END
    ) >= 1.05 AS is_pick_profitable_3d,
  COALESCE(a.n_trades_3d,0::bigint) >= 30
    AND COALESCE(a.net_sum_3d,0::numeric) >= 0.15
    AND (
      CASE
        WHEN COALESCE(a.gross_loss_3d,0::numeric) > 0
          THEN COALESCE(a.gross_profit_3d,0::numeric) / a.gross_loss_3d
        WHEN COALESCE(a.gross_profit_3d,0::numeric) > 0
          THEN 999999::numeric
        ELSE 0::numeric
      END
    ) >= 1.05 AS is_step_profitable_3d
FROM universe u
LEFT JOIN agg a
  ON a.symbol=u.symbol
 AND a.interval=u.interval
 AND a.strategy=u.strategy;

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
    GREATEST(
      COALESCE(c.last_exit_ts_3d, '1970-01-01'::timestamptz),
      COALESCE(c.last_ts_24h, '1970-01-01'::timestamptz),
      COALESCE(c.last_signal_ts, '1970-01-01'::timestamptz)
    ) AS rank_last_ts
  FROM v_orc_candidates_v5c c
  WHERE c.eligible_pick_v5
    AND c.n_trades_3d >= 5
    AND c.net_sum_3d > 0
    AND c.profit_factor_3d >= 1.05
),
dedup_symbol_interval AS (
  SELECT
    c.*,
    row_number() OVER (
      PARTITION BY c.symbol, c.interval
      ORDER BY
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
        d.net_sum_3d DESC,
        d.profit_factor_3d DESC,
        d.n_trades_3d DESC,
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
     FROM allocation_policy ap
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

COMMIT;

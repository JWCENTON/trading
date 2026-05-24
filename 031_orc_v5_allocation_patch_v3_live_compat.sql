-- 031_orc_v5_allocation_patch_v3_live_compat.sql
-- ORC/F0 allocation patch v3
-- Built only against verified local PAPER schema:
--   public.v_orc_candidates_v5c
--   public.v_orc_picks_v5
-- Existing candidate dependencies:
--   public.v_slot_profitability_3d_v5
--   public.v_orc_activity_24h
--   public.v_orc_signal_15m
--
-- Goals:
--   1) Keep patch limited to orchestration/allocation layer.
--   2) Do not change core strategies.
--   3) Do not change exit engine.
--   4) Do not manually disable any strategy.
--   5) Add soft RSI penalty through ranking only.
--   6) Add controlled cold-start exploration candidates for BBRANGE/SUPERTREND using existing columns only.
--
-- Safety:
--   - Single transaction.
--   - No table mutations.
--   - CREATE OR REPLACE VIEW only.
--   - No dependency on allocation_policy.
--   - LIVE-compatible v_orc_picks_v5 output shape is preserved, including rn and final_rn.

BEGIN;

CREATE OR REPLACE VIEW public.v_orc_candidates_v5c AS
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

    -- Tier 1: original profitability pick preserved, but RSI/BTCUSDC/1m is softened when
    -- it has weak 3d performance or recent blocked/noisy behavior. This is not a hard disable.
    (
      p.is_pick_profitable_3d
      AND NOT (
        p.strategy = 'RSI'::text
        AND p.symbol = 'BTCUSDC'::text
        AND p."interval" = '1m'::text
        AND (
          COALESCE(p.net_sum_3d, 0::numeric) < 0::numeric
          OR COALESCE(p.profit_factor_3d, 0::numeric) < 1::numeric
          OR COALESCE(a.filter_block_rate_24h, 0::numeric) > 0.50
        )
      )
    ) AS eligible_pick_v5,

    -- Tier 2: original bootstrap preserved, with small cold-start path for BBRANGE/SUPERTREND
    -- when the strategy has recent buy activity/signals and acceptable filter-block rate.
    (
      (
        p.strategy <> 'TREND'::text
        AND COALESCE(a.n_buy_24h, 0::bigint) >= 8
        AND COALESCE(a.filter_block_rate_24h, 0::numeric) <= 0.70
        AND (
          COALESCE(p.n_trades_3d, 0::bigint) >= 1
          OR COALESCE(s.n_signal_15m, 0::bigint) > 0
        )
      )
      OR (
        p.strategy IN ('BBRANGE'::text, 'SUPERTREND'::text)
        AND COALESCE(p.n_trades_3d, 0::bigint) = 0
        AND (
          COALESCE(a.n_buy_24h, 0::bigint) >= 20
          OR COALESCE(s.n_signal_15m, 0::bigint) > 0
        )
        AND COALESCE(a.filter_block_rate_24h, 0::numeric) <= 0.55
      )
    ) AS eligible_bootstrap_v5,

    -- Tier 3: original signal rule preserved.
    (
      COALESCE(s.n_signal_15m, 0::bigint) > 0
      AND COALESCE(a.n_buy_24h, 0::bigint) >= 4
      AND COALESCE(a.filter_block_rate_24h, 0::numeric) <= 0.70
    ) AS eligible_signal_v5,

    -- Tier 4: original activity rule preserved.
    (
      COALESCE(a.n_buy_24h, 0::bigint) >= 12
      AND COALESCE(a.filter_block_rate_24h, 0::numeric) <= 0.50
      AND COALESCE(p.n_trades_3d, 0::bigint) >= 1
      AND COALESCE(p.net_sum_3d, 0::numeric) >= 0::numeric
    ) AS eligible_activity_v5,

    -- Tier 5: original softfill rule preserved, with cautious BBRANGE/SUPERTREND cold-start path.
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
        p.strategy IN ('BBRANGE'::text, 'SUPERTREND'::text)
        AND COALESCE(p.n_trades_3d, 0::bigint) = 0
        AND COALESCE(a.n_buy_24h, 0::bigint) >= 18
        AND COALESCE(a.filter_block_rate_24h, 0::numeric) <= 0.55
      )
    ) AS eligible_softfill_v5
FROM public.v_slot_profitability_3d_v5 p
LEFT JOIN public.v_orc_activity_24h a
  ON a.symbol = p.symbol
 AND a."interval" = p."interval"
 AND a.strategy = p.strategy
LEFT JOIN public.v_orc_signal_15m s
  ON s.symbol = p.symbol
 AND s."interval" = p."interval"
 AND s.strategy = p.strategy;


CREATE OR REPLACE VIEW public.v_orc_picks_v5 AS
WITH tier1 AS (
    SELECT
        1 AS prio,
        c.*,
        GREATEST(
          COALESCE(c.last_exit_ts_3d, '1970-01-01 00:00:00+00'::timestamptz),
          COALESCE(c.last_ts_24h, '1970-01-01 00:00:00+00'::timestamptz),
          COALESCE(c.last_signal_ts, '1970-01-01 00:00:00+00'::timestamptz)
        ) AS rank_last_ts
    FROM public.v_orc_candidates_v5c c
    WHERE c.eligible_pick_v5
),
tier2 AS (
    SELECT
        2 AS prio,
        c.*,
        GREATEST(
          COALESCE(c.last_exit_ts_3d, '1970-01-01 00:00:00+00'::timestamptz),
          COALESCE(c.last_ts_24h, '1970-01-01 00:00:00+00'::timestamptz),
          COALESCE(c.last_signal_ts, '1970-01-01 00:00:00+00'::timestamptz)
        ) AS rank_last_ts
    FROM public.v_orc_candidates_v5c c
    WHERE c.eligible_bootstrap_v5
),
tier3 AS (
    SELECT
        3 AS prio,
        c.*,
        GREATEST(
          COALESCE(c.last_exit_ts_3d, '1970-01-01 00:00:00+00'::timestamptz),
          COALESCE(c.last_ts_24h, '1970-01-01 00:00:00+00'::timestamptz),
          COALESCE(c.last_signal_ts, '1970-01-01 00:00:00+00'::timestamptz)
        ) AS rank_last_ts
    FROM public.v_orc_candidates_v5c c
    WHERE c.eligible_signal_v5
),
tier4 AS (
    SELECT
        4 AS prio,
        c.*,
        GREATEST(
          COALESCE(c.last_exit_ts_3d, '1970-01-01 00:00:00+00'::timestamptz),
          COALESCE(c.last_ts_24h, '1970-01-01 00:00:00+00'::timestamptz),
          COALESCE(c.last_signal_ts, '1970-01-01 00:00:00+00'::timestamptz)
        ) AS rank_last_ts
    FROM public.v_orc_candidates_v5c c
    WHERE c.eligible_activity_v5
),
tier5 AS (
    SELECT
        5 AS prio,
        c.*,
        GREATEST(
          COALESCE(c.last_exit_ts_3d, '1970-01-01 00:00:00+00'::timestamptz),
          COALESCE(c.last_ts_24h, '1970-01-01 00:00:00+00'::timestamptz),
          COALESCE(c.last_signal_ts, '1970-01-01 00:00:00+00'::timestamptz)
        ) AS rank_last_ts
    FROM public.v_orc_candidates_v5c c
    WHERE c.eligible_softfill_v5
),
unioned AS (
    SELECT * FROM tier1
    UNION ALL SELECT * FROM tier2
    UNION ALL SELECT * FROM tier3
    UNION ALL SELECT * FROM tier4
    UNION ALL SELECT * FROM tier5
),
deduped AS (
    SELECT *
    FROM (
        SELECT
            u.*,
            row_number() OVER (
              PARTITION BY u.symbol, u."interval", u.strategy
              ORDER BY u.prio
            ) AS dup_rn
        FROM unioned u
    ) x
    WHERE x.dup_rn = 1
),
ranked AS (
    SELECT
        d.*,
        row_number() OVER (
            ORDER BY
              -- tier priority preserved
              d.prio,

              -- mild strategy preference after tier: TREND gets priority in directional conditions,
              -- BBRANGE/SUPERTREND are not buried behind RSI cold-start forever,
              -- RSI is still allowed but slightly lower when competing within same tier.
              CASE
                WHEN d.strategy = 'TREND' THEN 0
                WHEN d.strategy IN ('BBRANGE', 'SUPERTREND') THEN 1
                WHEN d.strategy = 'RSI' THEN 2
                ELSE 3
              END,

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
    FROM deduped d
)
SELECT
    prio,
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
    final_rn AS rn,
    final_rn
FROM ranked
WHERE final_rn <= 8;


-- Optional exploration view for inspection / future compatibility.
-- It depends only on public.v_orc_candidates_v5c and is safe even if automation ignores it.
CREATE OR REPLACE VIEW public.v_orc_exploration_picks_v1 AS
WITH candidates AS (
    SELECT
        c.symbol,
        c."interval",
        c.strategy,
        true AS eligible_exploration_v1,
        CASE
          WHEN c.strategy = 'BBRANGE' THEN 'ORC_EXPLORE_V1: BBRANGE cold-start activity, no-sample'
          WHEN c.strategy = 'SUPERTREND' THEN 'ORC_EXPLORE_V1: SUPERTREND cold-start activity/signal, no-sample'
          ELSE 'ORC_EXPLORE_V1: other'
        END::text AS reason,
        row_number() OVER (
          ORDER BY
            CASE
              WHEN c.strategy = 'BBRANGE' THEN 1
              WHEN c.strategy = 'SUPERTREND' THEN 2
              ELSE 9
            END,
            c.n_signal_15m DESC,
            c.n_buy_24h DESC,
            GREATEST(
              COALESCE(c.last_ts_24h, '1970-01-01 00:00:00+00'::timestamptz),
              COALESCE(c.last_signal_ts, '1970-01-01 00:00:00+00'::timestamptz)
            ) DESC,
            c.symbol,
            c."interval",
            c.strategy
        ) AS rn,
        c.n_trades_3d,
        c.net_sum_3d,
        c.profit_factor_3d
    FROM public.v_orc_candidates_v5c c
    WHERE c.strategy IN ('BBRANGE', 'SUPERTREND')
      AND COALESCE(c.n_trades_3d, 0::bigint) = 0
      AND (
        c.eligible_bootstrap_v5
        OR c.eligible_softfill_v5
        OR (
          c.strategy = 'SUPERTREND'
          AND COALESCE(c.n_signal_15m, 0::bigint) > 0
          AND COALESCE(c.filter_block_rate_24h, 0::numeric) <= 0.70
        )
      )
)
SELECT
    symbol,
    "interval",
    strategy,
    eligible_exploration_v1,
    reason,
    rn,
    n_trades_3d,
    net_sum_3d,
    profit_factor_3d
FROM candidates
WHERE rn <= 2;

COMMIT;

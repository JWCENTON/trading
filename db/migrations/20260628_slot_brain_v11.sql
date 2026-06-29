BEGIN;

CREATE TABLE IF NOT EXISTS slot_brain_snapshot (
  id BIGSERIAL PRIMARY KEY,
  calculated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  window_label TEXT NOT NULL,
  window_minutes INTEGER NOT NULL,

  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  strategy TEXT NOT NULL,

  trades INTEGER NOT NULL DEFAULT 0,
  wins INTEGER NOT NULL DEFAULT 0,
  losses INTEGER NOT NULL DEFAULT 0,

  gross_pnl_usdc NUMERIC(18,8),
  fees_usdc NUMERIC(18,8),
  net_pnl_usdc NUMERIC(18,8),

  avg_net_trade NUMERIC(18,8),
  win_rate NUMERIC(10,6),
  profit_factor_net NUMERIC(18,8),

  avg_mfe_pct NUMERIC(18,8),
  avg_mae_pct NUMERIC(18,8),
  avg_giveback_pct NUMERIC(18,8),
  mfe_capture_pct NUMERIC(18,8),

  avg_hold_minutes NUMERIC(18,8),

  fee_per_trade_usdc NUMERIC(18,8),
  fee_to_gross_pct NUMERIC(18,8),
  fee_to_turnover_pct NUMERIC(18,8),

  avg_regime_confidence NUMERIC(10,6),
  min_regime_confidence NUMERIC(10,6),
  max_regime_confidence NUMERIC(10,6),
  confidence_stddev NUMERIC(10,6),

  sample_quality TEXT NOT NULL DEFAULT 'NO_SAMPLE',
  stability_score NUMERIC(10,6),
  edge_score NUMERIC(10,6),

  edge_status TEXT NOT NULL DEFAULT 'OBSERVE',
  status_reason TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE(window_label, symbol, interval, strategy)
);

CREATE INDEX IF NOT EXISTS ix_slot_brain_snapshot_slot
ON slot_brain_snapshot(symbol, interval, strategy);

CREATE INDEX IF NOT EXISTS ix_slot_brain_snapshot_window
ON slot_brain_snapshot(window_label, calculated_at DESC);

CREATE INDEX IF NOT EXISTS ix_slot_brain_snapshot_edge
ON slot_brain_snapshot(edge_status, edge_score DESC);

CREATE OR REPLACE FUNCTION refresh_slot_brain_snapshot(
  p_window_label TEXT,
  p_window_minutes INTEGER
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO slot_brain_snapshot (
    calculated_at,
    window_label,
    window_minutes,
    symbol,
    interval,
    strategy,
    trades,
    wins,
    losses,
    gross_pnl_usdc,
    fees_usdc,
    net_pnl_usdc,
    avg_net_trade,
    win_rate,
    profit_factor_net,
    avg_mfe_pct,
    avg_mae_pct,
    avg_giveback_pct,
    mfe_capture_pct,
    avg_hold_minutes,
    fee_per_trade_usdc,
    fee_to_gross_pct,
    fee_to_turnover_pct,
    avg_regime_confidence,
    min_regime_confidence,
    max_regime_confidence,
    confidence_stddev,
    sample_quality,
    stability_score,
    edge_score,
    edge_status,
    status_reason
  )
  WITH base AS (
    SELECT
      p.symbol,
      p.interval,
      p.strategy,
      p.id,
      p.entry_time,
      p.exit_time,
      p.entry_price,
      p.exit_price,
      p.qty,
      p.gross_pnl_usdc,
      p.fees_usdc,
      p.net_pnl_usdc,
      p.hold_minutes,
      p.market_regime,
      COALESCE(mr.confidence, NULL) AS regime_confidence
    FROM positions p
    LEFT JOIN LATERAL (
      SELECT confidence
      FROM market_regime mr
      WHERE mr.symbol = p.symbol
        AND mr.interval = p.interval
        AND mr.ts <= p.entry_time
      ORDER BY mr.ts DESC
      LIMIT 1
    ) mr ON true
    WHERE p.status = 'CLOSED'
      AND p.exit_time >= now() - make_interval(mins => p_window_minutes)
      AND p.net_pnl_usdc IS NOT NULL
  ),
  mfe AS (
    SELECT
      v.id,
      v.mfe_pct,
      v.mae_pct,
      v.giveback_pct
    FROM v_trade_mfe_mae v
  ),
  agg AS (
    SELECT
      b.symbol,
      b.interval,
      b.strategy,
      COUNT(*)::INTEGER AS trades,
      COUNT(*) FILTER (WHERE b.net_pnl_usdc > 0)::INTEGER AS wins,
      COUNT(*) FILTER (WHERE b.net_pnl_usdc < 0)::INTEGER AS losses,

      SUM(b.gross_pnl_usdc) AS gross_pnl_usdc,
      SUM(b.fees_usdc) AS fees_usdc,
      SUM(b.net_pnl_usdc) AS net_pnl_usdc,
      AVG(b.net_pnl_usdc) AS avg_net_trade,

      CASE WHEN COUNT(*) > 0
        THEN COUNT(*) FILTER (WHERE b.net_pnl_usdc > 0)::NUMERIC / COUNT(*)::NUMERIC
      END AS win_rate,

      CASE
        WHEN ABS(SUM(b.net_pnl_usdc) FILTER (WHERE b.net_pnl_usdc < 0)) > 0
          THEN (SUM(b.net_pnl_usdc) FILTER (WHERE b.net_pnl_usdc > 0))
             / ABS(SUM(b.net_pnl_usdc) FILTER (WHERE b.net_pnl_usdc < 0))
        WHEN SUM(b.net_pnl_usdc) FILTER (WHERE b.net_pnl_usdc > 0) > 0
          THEN 99
        ELSE NULL
      END AS profit_factor_net,

      AVG(m.mfe_pct) AS avg_mfe_pct,
      AVG(m.mae_pct) AS avg_mae_pct,
      AVG(m.giveback_pct) AS avg_giveback_pct,

      CASE
        WHEN AVG(m.mfe_pct) > 0
          THEN GREATEST(0, LEAST(1, (AVG(m.mfe_pct) - AVG(m.giveback_pct)) / NULLIF(AVG(m.mfe_pct), 0)))
      END AS mfe_capture_pct,

      AVG(b.hold_minutes) AS avg_hold_minutes,

      CASE WHEN COUNT(*) > 0 THEN SUM(b.fees_usdc) / COUNT(*) END AS fee_per_trade_usdc,

      CASE
        WHEN ABS(SUM(b.gross_pnl_usdc)) > 0
          THEN (SUM(b.fees_usdc) / ABS(SUM(b.gross_pnl_usdc))) * 100
      END AS fee_to_gross_pct,

      CASE
        WHEN SUM(ABS(b.entry_price * b.qty)) > 0
          THEN (SUM(b.fees_usdc) / SUM(ABS(b.entry_price * b.qty))) * 100
      END AS fee_to_turnover_pct,

      AVG(b.regime_confidence) AS avg_regime_confidence,
      MIN(b.regime_confidence) AS min_regime_confidence,
      MAX(b.regime_confidence) AS max_regime_confidence,
      STDDEV_POP(b.regime_confidence) AS confidence_stddev
    FROM base b
    LEFT JOIN mfe m ON m.id = b.id
    GROUP BY b.symbol, b.interval, b.strategy
  ),
  scored AS (
    SELECT
      a.*,

      CASE
        WHEN a.trades = 0 THEN 'NO_SAMPLE'
        WHEN a.trades < 10 THEN 'LOW'
        WHEN a.trades < 30 THEN 'MEDIUM'
        ELSE 'GOOD'
      END AS sample_quality,

      LEAST(1.0, a.trades::NUMERIC / 50.0)
        * COALESCE(GREATEST(0, LEAST(1, 1 - COALESCE(a.confidence_stddev, 0))), 0.5)
        AS stability_score,

      (
        0.30 * COALESCE(GREATEST(0, LEAST(1, a.win_rate)), 0)
        +
        0.25 * COALESCE(GREATEST(0, LEAST(1, a.profit_factor_net / 2.0)), 0)
        +
        0.20 * COALESCE(GREATEST(0, LEAST(1, (a.avg_net_trade + 0.05) / 0.10)), 0)
        +
        0.15 * COALESCE(GREATEST(0, LEAST(1, a.mfe_capture_pct)), 0)
        +
        0.10 * COALESCE(GREATEST(0, LEAST(1, 1 - COALESCE(a.fee_to_turnover_pct, 0) / 0.20)), 0)
      ) AS edge_score
    FROM agg a
  )
  SELECT
    now(),
    p_window_label,
    p_window_minutes,
    s.symbol,
    s.interval,
    s.strategy,
    s.trades,
    s.wins,
    s.losses,
    ROUND(s.gross_pnl_usdc, 8),
    ROUND(s.fees_usdc, 8),
    ROUND(s.net_pnl_usdc, 8),
    ROUND(s.avg_net_trade, 8),
    ROUND(s.win_rate, 6),
    ROUND(s.profit_factor_net, 8),
    ROUND(s.avg_mfe_pct, 8),
    ROUND(s.avg_mae_pct, 8),
    ROUND(s.avg_giveback_pct, 8),
    ROUND(s.mfe_capture_pct, 8),
    ROUND(s.avg_hold_minutes, 8),
    ROUND(s.fee_per_trade_usdc, 8),
    ROUND(s.fee_to_gross_pct, 8),
    ROUND(s.fee_to_turnover_pct, 8),
    ROUND(s.avg_regime_confidence, 6),
    ROUND(s.min_regime_confidence, 6),
    ROUND(s.max_regime_confidence, 6),
    ROUND(s.confidence_stddev, 6),
    s.sample_quality,
    ROUND(s.stability_score, 6),
    ROUND(s.edge_score, 6),
    CASE
      WHEN s.trades >= 30
       AND s.net_pnl_usdc > 0
       AND COALESCE(s.profit_factor_net, 0) >= 1.15
       AND COALESCE(s.edge_score, 0) >= 0.65
        THEN 'ALLOW_LIVE'
      WHEN s.trades >= 10
       AND (
            s.net_pnl_usdc < 0
         OR COALESCE(s.profit_factor_net, 0) < 0.90
         OR COALESCE(s.edge_score, 0) < 0.45
       )
        THEN 'BLOCK_LIVE'
      ELSE 'OBSERVE'
    END AS edge_status,
    CASE
      WHEN s.trades < 10 THEN 'INSUFFICIENT_SAMPLE'
      WHEN s.trades >= 30
       AND s.net_pnl_usdc > 0
       AND COALESCE(s.profit_factor_net, 0) >= 1.15
       AND COALESCE(s.edge_score, 0) >= 0.65
        THEN 'POSITIVE_EDGE_AFTER_FEES'
      WHEN s.net_pnl_usdc < 0 THEN 'NEGATIVE_NET_PNL'
      WHEN COALESCE(s.profit_factor_net, 0) < 0.90 THEN 'LOW_PROFIT_FACTOR'
      WHEN COALESCE(s.edge_score, 0) < 0.45 THEN 'LOW_EDGE_SCORE'
      ELSE 'OBSERVE_MORE_DATA'
    END AS status_reason
  FROM scored s
  ON CONFLICT (window_label, symbol, interval, strategy)
  DO UPDATE SET
    calculated_at = EXCLUDED.calculated_at,
    window_minutes = EXCLUDED.window_minutes,
    trades = EXCLUDED.trades,
    wins = EXCLUDED.wins,
    losses = EXCLUDED.losses,
    gross_pnl_usdc = EXCLUDED.gross_pnl_usdc,
    fees_usdc = EXCLUDED.fees_usdc,
    net_pnl_usdc = EXCLUDED.net_pnl_usdc,
    avg_net_trade = EXCLUDED.avg_net_trade,
    win_rate = EXCLUDED.win_rate,
    profit_factor_net = EXCLUDED.profit_factor_net,
    avg_mfe_pct = EXCLUDED.avg_mfe_pct,
    avg_mae_pct = EXCLUDED.avg_mae_pct,
    avg_giveback_pct = EXCLUDED.avg_giveback_pct,
    mfe_capture_pct = EXCLUDED.mfe_capture_pct,
    avg_hold_minutes = EXCLUDED.avg_hold_minutes,
    fee_per_trade_usdc = EXCLUDED.fee_per_trade_usdc,
    fee_to_gross_pct = EXCLUDED.fee_to_gross_pct,
    fee_to_turnover_pct = EXCLUDED.fee_to_turnover_pct,
    avg_regime_confidence = EXCLUDED.avg_regime_confidence,
    min_regime_confidence = EXCLUDED.min_regime_confidence,
    max_regime_confidence = EXCLUDED.max_regime_confidence,
    confidence_stddev = EXCLUDED.confidence_stddev,
    sample_quality = EXCLUDED.sample_quality,
    stability_score = EXCLUDED.stability_score,
    edge_score = EXCLUDED.edge_score,
    edge_status = EXCLUDED.edge_status,
    status_reason = EXCLUDED.status_reason;
END;
$$;

COMMIT;

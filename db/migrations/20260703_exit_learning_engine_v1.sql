BEGIN;

CREATE TABLE IF NOT EXISTS exit_learning_v1 (
  id BIGSERIAL PRIMARY KEY,
  strategy TEXT NOT NULL,
  symbol TEXT NOT NULL,
  "interval" TEXT NOT NULL,
  exit_family TEXT,
  exit_decision_class_v2 TEXT,
  trades INTEGER NOT NULL,
  net_sum NUMERIC,
  avg_net NUMERIC,
  avg_mfe_pct NUMERIC,
  avg_exit_pct NUMERIC,
  avg_giveback_pct NUMERIC,
  avg_mfe_capture_ratio NUMERIC,
  avg_giveback_ratio NUMERIC,
  learning_priority TEXT NOT NULL,
  learning_reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(strategy, symbol, "interval", exit_family, exit_decision_class_v2)
);

CREATE OR REPLACE FUNCTION refresh_exit_learning_v1(
  p_since TIMESTAMPTZ DEFAULT now() - interval '30 days'
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_count INTEGER := 0;
BEGIN
  INSERT INTO exit_learning_v1 (
    strategy, symbol, "interval", exit_family, exit_decision_class_v2,
    trades, net_sum, avg_net,
    avg_mfe_pct, avg_exit_pct, avg_giveback_pct,
    avg_mfe_capture_ratio, avg_giveback_ratio,
    learning_priority, learning_reason,
    updated_at
  )
  SELECT
    strategy,
    symbol,
    "interval",
    exit_family,
    exit_decision_class_v2,
    COUNT(*)::INTEGER AS trades,
    SUM(net_pnl_usdc) AS net_sum,
    AVG(net_pnl_usdc) AS avg_net,
    AVG(mfe_pct) AS avg_mfe_pct,
    AVG(exit_pct) AS avg_exit_pct,
    AVG(giveback_pct) AS avg_giveback_pct,
    AVG(mfe_capture_ratio) AS avg_mfe_capture_ratio,
    AVG(giveback_ratio) AS avg_giveback_ratio,
    CASE
      WHEN COUNT(*) >= 10 AND SUM(net_pnl_usdc) < -1 THEN 'P0_FIX_FIRST'
      WHEN COUNT(*) >= 5 AND SUM(net_pnl_usdc) < 0 THEN 'P1_REVIEW'
      WHEN AVG(giveback_ratio) >= 0.60 THEN 'P1_HIGH_GIVEBACK'
      WHEN AVG(mfe_capture_ratio) < 0.40 AND AVG(mfe_pct) > 0 THEN 'P2_LOW_CAPTURE'
      ELSE 'P3_OBSERVE'
    END AS learning_priority,
    CASE
      WHEN COUNT(*) >= 10 AND SUM(net_pnl_usdc) < -1 THEN 'High sample negative net exit class'
      WHEN COUNT(*) >= 5 AND SUM(net_pnl_usdc) < 0 THEN 'Negative net exit class'
      WHEN AVG(giveback_ratio) >= 0.60 THEN 'High giveback ratio'
      WHEN AVG(mfe_capture_ratio) < 0.40 AND AVG(mfe_pct) > 0 THEN 'Low MFE capture'
      ELSE 'Observe only'
    END AS learning_reason,
    now()
  FROM exit_trace_v2
  WHERE exit_time >= p_since
  GROUP BY strategy, symbol, "interval", exit_family, exit_decision_class_v2
  ON CONFLICT (strategy, symbol, "interval", exit_family, exit_decision_class_v2)
  DO UPDATE SET
    trades = EXCLUDED.trades,
    net_sum = EXCLUDED.net_sum,
    avg_net = EXCLUDED.avg_net,
    avg_mfe_pct = EXCLUDED.avg_mfe_pct,
    avg_exit_pct = EXCLUDED.avg_exit_pct,
    avg_giveback_pct = EXCLUDED.avg_giveback_pct,
    avg_mfe_capture_ratio = EXCLUDED.avg_mfe_capture_ratio,
    avg_giveback_ratio = EXCLUDED.avg_giveback_ratio,
    learning_priority = EXCLUDED.learning_priority,
    learning_reason = EXCLUDED.learning_reason,
    updated_at = now();

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;

CREATE OR REPLACE VIEW v_exit_learning_priority_v1 AS
SELECT
  strategy,
  symbol,
  "interval",
  exit_family,
  exit_decision_class_v2,
  trades,
  ROUND(net_sum, 6) AS net_sum,
  ROUND(avg_net, 6) AS avg_net,
  ROUND(avg_mfe_pct, 4) AS avg_mfe_pct,
  ROUND(avg_exit_pct, 4) AS avg_exit_pct,
  ROUND(avg_giveback_pct, 4) AS avg_giveback_pct,
  ROUND(avg_mfe_capture_ratio, 4) AS avg_mfe_capture_ratio,
  ROUND(avg_giveback_ratio, 4) AS avg_giveback_ratio,
  learning_priority,
  learning_reason,
  updated_at
FROM exit_learning_v1
ORDER BY
  CASE learning_priority
    WHEN 'P0_FIX_FIRST' THEN 0
    WHEN 'P1_REVIEW' THEN 1
    WHEN 'P1_HIGH_GIVEBACK' THEN 2
    WHEN 'P2_LOW_CAPTURE' THEN 3
    ELSE 4
  END,
  net_sum ASC NULLS LAST,
  trades DESC;

COMMIT;

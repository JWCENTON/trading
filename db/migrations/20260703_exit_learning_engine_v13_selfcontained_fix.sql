BEGIN;

ALTER TABLE exit_learning_v1
  ADD COLUMN IF NOT EXISTS sample_confidence TEXT,
  ADD COLUMN IF NOT EXISTS recoverable_net_usdc NUMERIC,
  ADD COLUMN IF NOT EXISTS recoverable_reason TEXT,
  ADD COLUMN IF NOT EXISTS learning_score NUMERIC;

DROP VIEW IF EXISTS v_exit_learning_top_recoverable_v1;
DROP VIEW IF EXISTS v_exit_learning_priority_v1;

CREATE OR REPLACE FUNCTION refresh_exit_learning_v1(
  p_since TIMESTAMPTZ DEFAULT now() - interval '3 days'
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
    sample_confidence, recoverable_net_usdc, recoverable_reason, learning_score,
    updated_at
  )
  WITH base AS (
    SELECT
      strategy, symbol, "interval", exit_family, exit_decision_class_v2,
      COUNT(*)::INTEGER AS trades,
      SUM(net_pnl_usdc) AS net_sum,
      AVG(net_pnl_usdc) AS avg_net,
      AVG(mfe_pct) AS avg_mfe_pct,
      AVG(exit_pct) AS avg_exit_pct,
      AVG(giveback_pct) AS avg_giveback_pct,
      AVG(CASE WHEN mfe_pct > 0 AND exit_pct > 0 THEN LEAST(GREATEST(exit_pct / NULLIF(mfe_pct,0),0),1) END) AS avg_mfe_capture_ratio,
      AVG(CASE WHEN mfe_pct > 0 AND giveback_pct >= 0 THEN LEAST(GREATEST(giveback_pct / NULLIF(mfe_pct,0),0),1) END) AS avg_giveback_ratio
    FROM exit_trace_v2
    WHERE exit_time >= p_since
    GROUP BY strategy, symbol, "interval", exit_family, exit_decision_class_v2
  ),
  scored AS (
    SELECT *,
      CASE WHEN trades >= 30 THEN 'HIGH'
           WHEN trades >= 10 THEN 'MEDIUM'
           WHEN trades >= 5 THEN 'LOW'
           ELSE 'VERY_LOW' END AS sample_confidence,
      CASE
        WHEN exit_decision_class_v2 IN (
          'PROFIT_LOCK_TO_LOSS_AFTER_FEES_OR_GIVEBACK',
          'PROFIT_LOCK_HIGH_GIVEBACK_REVIEW',
          'WIN_WITH_LOW_MFE_CAPTURE_REVIEW',
          'LOSS_AFTER_AVAILABLE_PROFIT_REVIEW',
          'STOP_AFTER_PROFIT_AVAILABLE_REVIEW'
        )
        THEN GREATEST(0, COALESCE(avg_giveback_pct,0) * trades * 0.01)
        ELSE 0
      END AS recoverable_net_usdc
    FROM base
  )
  SELECT
    strategy, symbol, "interval", exit_family, exit_decision_class_v2,
    trades, net_sum, avg_net,
    avg_mfe_pct, avg_exit_pct, avg_giveback_pct,
    avg_mfe_capture_ratio, avg_giveback_ratio,
    CASE
      WHEN trades >= 10 AND net_sum < -1 THEN 'P0_FIX_FIRST'
      WHEN trades >= 5 AND net_sum < 0 THEN 'P1_REVIEW'
      WHEN trades >= 5 AND COALESCE(avg_giveback_ratio,0) >= 0.60 THEN 'P1_HIGH_GIVEBACK'
      WHEN trades >= 5 AND COALESCE(avg_mfe_capture_ratio,1) < 0.40 AND COALESCE(avg_mfe_pct,0) > 0 THEN 'P2_LOW_CAPTURE'
      ELSE 'P3_OBSERVE'
    END,
    CASE
      WHEN trades >= 10 AND net_sum < -1 THEN 'High sample negative net exit class'
      WHEN trades >= 5 AND net_sum < 0 THEN 'Negative net exit class'
      WHEN trades >= 5 AND COALESCE(avg_giveback_ratio,0) >= 0.60 THEN 'High giveback ratio with sufficient sample'
      WHEN trades >= 5 AND COALESCE(avg_mfe_capture_ratio,1) < 0.40 AND COALESCE(avg_mfe_pct,0) > 0 THEN 'Low MFE capture with sufficient sample'
      ELSE 'Observe only'
    END,
    sample_confidence,
    recoverable_net_usdc,
    CASE
      WHEN exit_decision_class_v2 ILIKE '%PROFIT_LOCK%' THEN 'Potential recovery from reducing giveback after profit lock'
      WHEN exit_decision_class_v2 = 'WIN_WITH_LOW_MFE_CAPTURE_REVIEW' THEN 'Potential recovery from increasing MFE capture'
      WHEN exit_decision_class_v2 IN ('LOSS_AFTER_AVAILABLE_PROFIT_REVIEW','STOP_AFTER_PROFIT_AVAILABLE_REVIEW') THEN 'Potential recovery from protecting available profit earlier'
      ELSE 'No recoverable estimate for this class'
    END,
    ROUND(
      GREATEST(0, -COALESCE(net_sum,0)) * 10
      + COALESCE(recoverable_net_usdc,0) * 5
      + CASE sample_confidence WHEN 'HIGH' THEN 5 WHEN 'MEDIUM' THEN 3 WHEN 'LOW' THEN 1 ELSE 0 END,
      6
    ),
    now()
  FROM scored
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
    sample_confidence = EXCLUDED.sample_confidence,
    recoverable_net_usdc = EXCLUDED.recoverable_net_usdc,
    recoverable_reason = EXCLUDED.recoverable_reason,
    learning_score = EXCLUDED.learning_score,
    updated_at = now();

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;

CREATE VIEW v_exit_learning_priority_v1 AS
SELECT
  strategy, symbol, "interval", exit_family, exit_decision_class_v2,
  trades, sample_confidence,
  ROUND(net_sum,6) AS net_sum,
  ROUND(avg_net,6) AS avg_net,
  ROUND(avg_mfe_pct,4) AS avg_mfe_pct,
  ROUND(avg_exit_pct,4) AS avg_exit_pct,
  ROUND(avg_giveback_pct,4) AS avg_giveback_pct,
  ROUND(avg_mfe_capture_ratio,4) AS avg_mfe_capture_ratio,
  ROUND(avg_giveback_ratio,4) AS avg_giveback_ratio,
  ROUND(recoverable_net_usdc,6) AS recoverable_net_usdc,
  recoverable_reason,
  learning_priority,
  learning_reason,
  ROUND(learning_score,6) AS learning_score,
  updated_at
FROM exit_learning_v1
ORDER BY learning_score DESC NULLS LAST, net_sum ASC NULLS LAST, trades DESC;

CREATE VIEW v_exit_learning_top_recoverable_v1 AS
SELECT *
FROM v_exit_learning_priority_v1
WHERE recoverable_net_usdc > 0
ORDER BY recoverable_net_usdc DESC, learning_score DESC
LIMIT 50;

COMMIT;

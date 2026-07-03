BEGIN;

CREATE OR REPLACE VIEW v_exit_trace_context_coverage_v1 AS
SELECT
  strategy,
  COUNT(*) AS closed_trades,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%entry=%') AS with_entry,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%peak=%') AS with_peak,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%current=%') AS with_current,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%mfe=%') AS with_mfe,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%mae=%') AS with_mae,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%giveback=%') AS with_giveback,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%age=%') AS with_age,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%bars=%') AS with_bars,
  ROUND(
    100.0 * COUNT(*) FILTER (
      WHERE exit_reason LIKE '%entry=%'
        AND exit_reason LIKE '%peak=%'
        AND exit_reason LIKE '%current=%'
        AND exit_reason LIKE '%giveback=%'
    ) / NULLIF(COUNT(*), 0),
    2
  ) AS full_context_pct,
  MAX(exit_time) AS last_exit_time
FROM positions
WHERE status='CLOSED'
GROUP BY strategy;

CREATE OR REPLACE VIEW v_exit_trace_context_coverage_6h_v1 AS
SELECT
  strategy,
  COUNT(*) AS closed_trades,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%entry=%') AS with_entry,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%peak=%') AS with_peak,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%current=%') AS with_current,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%mfe=%') AS with_mfe,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%mae=%') AS with_mae,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%giveback=%') AS with_giveback,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%age=%') AS with_age,
  COUNT(*) FILTER (WHERE exit_reason LIKE '%bars=%') AS with_bars,
  ROUND(
    100.0 * COUNT(*) FILTER (
      WHERE exit_reason LIKE '%entry=%'
        AND exit_reason LIKE '%peak=%'
        AND exit_reason LIKE '%current=%'
        AND exit_reason LIKE '%giveback=%'
    ) / NULLIF(COUNT(*), 0),
    2
  ) AS full_context_pct,
  MAX(exit_time) AS last_exit_time
FROM positions
WHERE status='CLOSED'
  AND exit_time >= now() - interval '6 hours'
GROUP BY strategy;

CREATE OR REPLACE VIEW v_exit_trace_missing_context_v1 AS
SELECT
  exit_time,
  symbol,
  "interval",
  strategy,
  exit_reason,
  CASE WHEN exit_reason NOT LIKE '%entry=%' THEN true ELSE false END AS missing_entry,
  CASE WHEN exit_reason NOT LIKE '%peak=%' THEN true ELSE false END AS missing_peak,
  CASE WHEN exit_reason NOT LIKE '%current=%' THEN true ELSE false END AS missing_current,
  CASE WHEN exit_reason NOT LIKE '%giveback=%' THEN true ELSE false END AS missing_giveback,
  CASE WHEN exit_reason NOT LIKE '%age=%' THEN true ELSE false END AS missing_age
FROM positions
WHERE status='CLOSED'
  AND exit_time >= now() - interval '24 hours'
  AND (
    exit_reason NOT LIKE '%entry=%'
    OR exit_reason NOT LIKE '%peak=%'
    OR exit_reason NOT LIKE '%current=%'
    OR exit_reason NOT LIKE '%giveback=%'
    OR exit_reason NOT LIKE '%age=%'
  )
ORDER BY exit_time DESC;

CREATE OR REPLACE VIEW v_exit_trace_learning_priority_v1 AS
SELECT
  strategy,
  symbol,
  "interval",
  exit_family,
  exit_decision_class_v2,
  COUNT(*) AS trades,
  ROUND(SUM(net_pnl_usdc), 6) AS net_sum,
  ROUND(AVG(net_pnl_usdc), 6) AS avg_net,
  ROUND(AVG(mfe_pct), 4) AS avg_mfe_pct,
  ROUND(AVG(exit_pct), 4) AS avg_exit_pct,
  ROUND(AVG(giveback_pct), 4) AS avg_giveback_pct,
  ROUND(AVG(mfe_capture_ratio), 4) AS avg_mfe_capture_ratio,
  ROUND(AVG(giveback_ratio), 4) AS avg_giveback_ratio,
  MAX(exit_time) AS last_exit_time
FROM exit_trace_v2
WHERE exit_time >= now() - interval '30 days'
GROUP BY strategy, symbol, "interval", exit_family, exit_decision_class_v2
HAVING COUNT(*) >= 2
ORDER BY
  SUM(net_pnl_usdc) ASC,
  AVG(giveback_pct) DESC NULLS LAST,
  COUNT(*) DESC;

CREATE OR REPLACE VIEW v_exit_trace_north_star_status_v1 AS
SELECT
  'exit_context_coverage_6h' AS metric,
  ROUND(
    100.0 * COUNT(*) FILTER (
      WHERE exit_reason LIKE '%entry=%'
        AND exit_reason LIKE '%peak=%'
        AND exit_reason LIKE '%current=%'
        AND exit_reason LIKE '%giveback=%'
    ) / NULLIF(COUNT(*), 0),
    2
  ) AS value_pct,
  COUNT(*) AS sample_size,
  MAX(exit_time) AS last_seen
FROM positions
WHERE status='CLOSED'
  AND exit_time >= now() - interval '6 hours'

UNION ALL

SELECT
  'parsed_exit_trace_v2_6h' AS metric,
  ROUND(
    100.0 * COUNT(*) FILTER (
      WHERE parsed_peak_pct IS NOT NULL
        AND parsed_current_pct IS NOT NULL
    ) / NULLIF(COUNT(*), 0),
    2
  ) AS value_pct,
  COUNT(*) AS sample_size,
  MAX(exit_time) AS last_seen
FROM exit_trace_v2
WHERE exit_time >= now() - interval '6 hours';

COMMIT;

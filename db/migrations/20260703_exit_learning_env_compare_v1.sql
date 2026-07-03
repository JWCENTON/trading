BEGIN;

CREATE OR REPLACE VIEW v_exit_learning_env_compare_v1 AS
WITH src AS (
  SELECT
    current_database() AS db_name,
    CASE
      WHEN current_database() ILIKE '%live%' THEN 'LIVE'
      WHEN current_database() ILIKE '%paper%' THEN 'PAPER'
      ELSE 'UNKNOWN'
    END AS environment,
    strategy,
    symbol,
    "interval",
    exit_family,
    exit_decision_class_v2,
    trades,
    sample_confidence,
    net_sum,
    avg_net,
    avg_mfe_pct,
    avg_exit_pct,
    avg_giveback_pct,
    avg_mfe_capture_ratio,
    avg_giveback_ratio,
    recoverable_net_usdc,
    learning_priority,
    learning_reason,
    learning_score,
    updated_at
  FROM exit_learning_v1
)
SELECT *
FROM src
ORDER BY learning_score DESC NULLS LAST, net_sum ASC NULLS LAST, trades DESC;

CREATE OR REPLACE VIEW v_exit_learning_top_problem_slots_v1 AS
SELECT
  environment,
  strategy,
  symbol,
  "interval",
  exit_family,
  exit_decision_class_v2,
  trades,
  sample_confidence,
  ROUND(net_sum, 6) AS net_sum,
  ROUND(avg_giveback_pct, 4) AS avg_giveback_pct,
  ROUND(recoverable_net_usdc, 6) AS recoverable_net_usdc,
  learning_priority,
  ROUND(learning_score, 6) AS learning_score,
  updated_at
FROM v_exit_learning_env_compare_v1
WHERE learning_priority IN ('P0_FIX_FIRST', 'P1_REVIEW', 'P1_HIGH_GIVEBACK', 'P2_LOW_CAPTURE')
ORDER BY learning_score DESC NULLS LAST, net_sum ASC NULLS LAST, trades DESC;

CREATE OR REPLACE VIEW v_exit_learning_strategy_summary_v1 AS
SELECT
  environment,
  strategy,
  COUNT(*) AS classes,
  SUM(trades) AS trades,
  ROUND(SUM(net_sum), 6) AS net_sum,
  ROUND(SUM(recoverable_net_usdc), 6) AS recoverable_net_usdc,
  COUNT(*) FILTER (WHERE learning_priority='P0_FIX_FIRST') AS p0_classes,
  COUNT(*) FILTER (WHERE learning_priority LIKE 'P1%') AS p1_classes,
  MAX(updated_at) AS updated_at
FROM v_exit_learning_env_compare_v1
GROUP BY environment, strategy
ORDER BY net_sum ASC, recoverable_net_usdc DESC;

COMMIT;

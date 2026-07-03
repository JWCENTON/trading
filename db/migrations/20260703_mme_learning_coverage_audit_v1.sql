BEGIN;

DROP VIEW IF EXISTS v_mme_learning_missing_recent_v1;
DROP VIEW IF EXISTS v_mme_learning_slot_coverage_v1;
DROP VIEW IF EXISTS v_mme_learning_available_context_v1;

CREATE OR REPLACE VIEW v_mme_learning_available_context_v1 AS
SELECT
  symbol,
  "interval",
  COUNT(*) AS context_rows,
  MAX(orc_readiness_score) AS max_orc_readiness_score,
  MAX(remaining_score) AS max_remaining_score,
  MAX(sequence_stage) AS any_sequence_stage,
  MAX(mme_orc_status) AS any_mme_orc_status
FROM v_market_memory_orc_context_best_v17
GROUP BY symbol, "interval";

CREATE OR REPLACE VIEW v_mme_learning_missing_recent_v1 AS
SELECT
  a.position_id,
  a.exit_time,
  a.symbol,
  a."interval",
  a.strategy,
  a.coverage_pct,
  a.learning_status,
  a.missing_fields,
  c.context_rows,
  c.max_orc_readiness_score,
  c.max_remaining_score,
  c.any_sequence_stage,
  c.any_mme_orc_status,
  CASE
    WHEN c.symbol IS NULL THEN 'NO_MME_CONTEXT_FOR_SYMBOL_INTERVAL'
    WHEN 'MME' = ANY(a.missing_fields) THEN 'MME_CONTEXT_EXISTS_BUT_AUDIT_NOT_LINKED'
    ELSE 'MME_OK'
  END AS mme_gap_reason
FROM v_learning_feedback_audit_v1 a
LEFT JOIN v_mme_learning_available_context_v1 c
  ON c.symbol = a.symbol
 AND c."interval" = a."interval"
WHERE a.exit_time >= now() - interval '72 hours'
ORDER BY a.exit_time DESC;

CREATE OR REPLACE VIEW v_mme_learning_slot_coverage_v1 AS
SELECT
  symbol,
  "interval",
  strategy,
  COUNT(*) AS trades,
  COUNT(*) FILTER (WHERE 'MME' = ANY(missing_fields)) AS missing_mme_trades,
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE NOT ('MME' = ANY(missing_fields)))::numeric
    / NULLIF(COUNT(*), 0),
    2
  ) AS mme_coverage_pct,
  MIN(exit_time) AS oldest_exit,
  MAX(exit_time) AS newest_exit
FROM v_learning_feedback_audit_v1
WHERE exit_time >= now() - interval '72 hours'
GROUP BY symbol, "interval", strategy
ORDER BY missing_mme_trades DESC, trades DESC;

COMMIT;

BEGIN;

DROP VIEW IF EXISTS v_decision_intelligence_v1;

CREATE OR REPLACE VIEW v_decision_intelligence_v1 AS
SELECT
  lf.environment,
  lf.decision_key,
  lf.position_id,

  lf.symbol,
  lf.interval,
  lf.strategy,

  lf.entry_time,
  lf.exit_time,

  CASE
    WHEN lf.exit_time IS NOT NULL THEN 'CLOSED'
    WHEN lf.entry_time IS NOT NULL THEN 'ENTRY_ONLY'
    ELSE 'CONTEXT_ONLY'
  END AS decision_lifecycle_status,

  lf.priority,
  lf.recommendation_type,
  lf.recommendation_action,

  lf.net_pnl_usdc,
  lf.gross_pnl_usdc,
  lf.fees_usdc,
  lf.hold_minutes,
  lf.market_regime,
  lf.exit_reason,

  lf.snapshot_status_model_b,
  lf.missing_context_model_b,
  lf.model_b_baseline,
  lf.mme_status,
  lf.mme_readiness_score,

  lf.pnl_class,
  lf.fee_pressure_pct,
  lf.evidence_status,
  lf.raw_evidence,

  CASE
    WHEN lf.missing_context_model_b IS NULL THEN 0
    WHEN jsonb_typeof(lf.missing_context_model_b) = 'array'
      THEN jsonb_array_length(lf.missing_context_model_b)
    ELSE NULL
  END AS missing_context_count,

  CASE
    WHEN lf.net_pnl_usdc IS NOT NULL THEN true
    ELSE false
  END AS has_pnl,

  CASE
    WHEN lf.exit_reason IS NOT NULL THEN true
    ELSE false
  END AS has_exit_reason,

  CASE
    WHEN lf.mme_status IS NOT NULL THEN true
    ELSE false
  END AS has_mme_status,

  lf.created_at,
  lf.refreshed_at

FROM learning_feature_warehouse_v1 lf;

COMMIT;

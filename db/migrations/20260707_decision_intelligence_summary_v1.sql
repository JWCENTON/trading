BEGIN;

DROP VIEW IF EXISTS v_decision_intelligence_summary_v1;

CREATE OR REPLACE VIEW v_decision_intelligence_summary_v1 AS
SELECT
  environment,
  decision_key,
  position_id,
  symbol,
  interval,
  strategy,

  MIN(entry_time) AS entry_time,
  MAX(exit_time) AS exit_time,

  CASE
    WHEN COUNT(*) FILTER (WHERE decision_lifecycle_status = 'CLOSED') > 0 THEN 'CLOSED'
    WHEN COUNT(*) FILTER (WHERE decision_lifecycle_status = 'ENTRY_ONLY') > 0 THEN 'ENTRY_ONLY'
    ELSE 'CONTEXT_ONLY'
  END AS lifecycle_status,

  MAX(net_pnl_usdc) AS net_pnl_usdc,
  MAX(gross_pnl_usdc) AS gross_pnl_usdc,
  MAX(fees_usdc) AS fees_usdc,
  MAX(hold_minutes) AS hold_minutes,
  MAX(market_regime) AS market_regime,
  MAX(exit_reason) AS exit_reason,

  COUNT(*) AS recommendation_rows,
  COUNT(DISTINCT recommendation_type) AS recommendation_types,
  COUNT(DISTINCT recommendation_action) AS recommendation_actions,

  BOOL_OR(priority = 'P0') AS has_p0,
  BOOL_OR(priority = 'P1') AS has_p1,
  BOOL_OR(priority = 'P2') AS has_p2,
  BOOL_OR(priority = 'P3') AS has_p3,

  CASE
    WHEN BOOL_OR(priority = 'P0') THEN 'P0'
    WHEN BOOL_OR(priority = 'P1') THEN 'P1'
    WHEN BOOL_OR(priority = 'P2') THEN 'P2'
    WHEN BOOL_OR(priority = 'P3') THEN 'P3'
    ELSE NULL
  END AS highest_priority,

  BOOL_OR(recommendation_type = 'AVOID_CONTEXT_REVIEW') AS has_avoid_context_review,
  BOOL_OR(recommendation_type = 'ENTRY_QUALITY_REVIEW') AS has_entry_quality_review,
  BOOL_OR(recommendation_type = 'POSITIVE_CONTEXT_CONFIRMATION') AS has_positive_context_confirmation,
  BOOL_OR(recommendation_type = 'WEAK_CONTEXT_WIN_REVIEW') AS has_weak_context_win_review,
  BOOL_OR(recommendation_type = 'OBSERVE_INCOMPLETE_PNL') AS has_observe_incomplete_pnl,

  BOOL_OR(recommendation_action = 'SHADOW_REVIEW_AVOID_GATE') AS has_shadow_review_avoid_gate,
  BOOL_OR(recommendation_action = 'SHADOW_REVIEW_ENTRY_FILTER') AS has_shadow_review_entry_filter,
  BOOL_OR(recommendation_action = 'SHADOW_CONFIRM_CONTEXT_EDGE') AS has_shadow_confirm_context_edge,
  BOOL_OR(recommendation_action = 'SHADOW_REVIEW_MISSED_CONTEXT_EDGE') AS has_shadow_review_missed_context_edge,

  MIN(missing_context_count) AS min_missing_context_count,
  MAX(missing_context_count) AS max_missing_context_count,

  BOOL_AND(COALESCE(missing_context_count, 0) = 0) AS full_context,
  BOOL_OR(has_pnl) AS has_pnl,
  BOOL_OR(has_exit_reason) AS has_exit_reason,
  BOOL_OR(has_mme_status) AS has_mme_status,

  MIN(mme_readiness_score) AS min_mme_readiness_score,
  MAX(mme_readiness_score) AS max_mme_readiness_score,
  AVG(mme_readiness_score) AS avg_mme_readiness_score,

  ARRAY_AGG(DISTINCT recommendation_type ORDER BY recommendation_type) AS recommendation_type_list,
  ARRAY_AGG(DISTINCT recommendation_action ORDER BY recommendation_action) AS recommendation_action_list,
  ARRAY_AGG(DISTINCT mme_status ORDER BY mme_status) FILTER (WHERE mme_status IS NOT NULL) AS mme_status_list,

  MIN(created_at) AS first_created_at,
  MAX(refreshed_at) AS last_refreshed_at

FROM v_decision_intelligence_v1
GROUP BY
  environment,
  decision_key,
  position_id,
  symbol,
  interval,
  strategy;

COMMIT;

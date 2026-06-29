BEGIN;

CREATE OR REPLACE VIEW v_orc_candidate_context_v1 AS
SELECT
  o.*,

  m.mme_orc_status,
  m.orc_hint AS mme_orc_hint,
  m.orc_readiness_score AS mme_orc_readiness_score,
  m.sequence_type AS mme_sequence_type,
  m.sequence_stage AS mme_sequence_stage,
  m.sequence_quality AS mme_sequence_quality,
  m.continuation_score AS mme_continuation_score,
  m.reversal_score AS mme_reversal_score,
  m.late_entry_risk AS mme_late_entry_risk,
  m.rank_score AS mme_rank_score,
  m.global_rank AS mme_global_rank,
  m.ranking_status AS mme_ranking_status,
  m.opportunity_score AS mme_opportunity_score,
  m.confidence_score AS mme_confidence_score,
  m.urgency_score AS mme_urgency_score,
  m.exhaustion_risk AS mme_exhaustion_risk,
  m.remaining_score AS mme_remaining_score,
  m.timing_score AS mme_timing_score,
  m.long_context AS mme_long_context,
  m.short_context AS mme_short_context,
  m.direction AS mme_direction,
  m.orc_context_reason AS mme_reason,
  m.mme_orc_priority,
  m.mme_orc_watch,
  m.mme_orc_avoid,

  CASE
    WHEN m.mme_orc_priority THEN 18
    WHEN m.mme_orc_watch THEN 8
    WHEN m.mme_orc_avoid THEN -25
    ELSE 0
  END AS mme_score_bonus,

  CASE
    WHEN m.mme_orc_priority THEN 'MME_PRIORITY_CONTEXT'
    WHEN m.mme_orc_watch THEN 'MME_WATCH_CONTEXT'
    WHEN m.mme_orc_avoid THEN 'MME_AVOID_CONTEXT'
    WHEN m.symbol IS NULL THEN 'NO_MME_CONTEXT'
    ELSE 'MME_OBSERVE_CONTEXT'
  END AS mme_context_status,

  COALESCE(o.v63_score, 0)
  + CASE
      WHEN m.mme_orc_priority THEN 18
      WHEN m.mme_orc_watch THEN 8
      WHEN m.mme_orc_avoid THEN -25
      ELSE 0
    END AS orc_context_score,

  CASE
    WHEN COALESCE(o.picked_v63_now, false) = true
     AND COALESCE(o.orc_v7_ready, false) = true
     AND COALESCE(m.mme_orc_avoid, false) = false
      THEN true
    ELSE false
  END AS context_ready_now,

  jsonb_build_object(
    'symbol', o.symbol,
    'interval', o.interval,
    'strategy', o.strategy,
    'picked_v63_now', COALESCE(o.picked_v63_now, false),
    'orc_v7_ready', COALESCE(o.orc_v7_ready, false),
    'readiness_reason', o.readiness_reason,
    'v7_reason', o.v7_reason,
    'mme_status', m.mme_orc_status,
    'mme_hint', m.orc_hint,
    'mme_readiness', m.orc_readiness_score,
    'mme_sequence', m.sequence_type,
    'mme_stage', m.sequence_stage,
    'mme_long_context', m.long_context,
    'mme_short_context', m.short_context,
    'mme_reason', m.orc_context_reason,
    'mme_bonus', CASE
      WHEN m.mme_orc_priority THEN 18
      WHEN m.mme_orc_watch THEN 8
      WHEN m.mme_orc_avoid THEN -25
      ELSE 0
    END
  ) AS orc_context_payload

FROM v_orc_v7_shadow_picks o
LEFT JOIN v_market_memory_orc_context_v17 m
  ON m.symbol = o.symbol
 AND m.interval = o.interval;

CREATE OR REPLACE VIEW v_orc_candidate_context_best_v1 AS
SELECT *
FROM v_orc_candidate_context_v1
ORDER BY
  context_ready_now DESC,
  orc_context_score DESC NULLS LAST,
  mme_orc_readiness_score DESC NULLS LAST,
  v7_rn ASC NULLS LAST
LIMIT 20;

COMMIT;

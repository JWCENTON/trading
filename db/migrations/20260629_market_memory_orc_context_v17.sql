BEGIN;

CREATE OR REPLACE VIEW v_market_memory_orc_context_v17 AS
SELECT
  s.symbol,
  s.interval,

  s.orc_hint,
  s.orc_readiness_score,
  s.reason AS orc_context_reason,

  s.sequence_type,
  s.sequence_stage,
  s.sequence_quality,
  s.continuation_score,
  s.reversal_score,
  s.late_entry_risk,

  s.rank_score,
  s.global_rank,
  s.ranking_status,

  s.opportunity_score,
  s.confidence_score,
  s.urgency_score,
  s.exhaustion_risk,
  s.remaining_score,
  s.timing_score,

  s.opportunity_type,
  s.opportunity_stage,
  s.action_hint,
  s.timeline_type,
  s.chain_length,
  s.chain_age_minutes,
  s.long_context,
  s.short_context,
  s.direction,

  s.first_event_at,
  s.last_event_at,
  s.expires_at,
  s.refreshed_at,

  CASE
    WHEN s.orc_hint = 'ORC_PRIORITY_CANDIDATE'
     AND s.orc_readiness_score >= 80
     AND s.late_entry_risk < 35
      THEN true
    ELSE false
  END AS mme_orc_priority,

  CASE
    WHEN s.orc_hint IN ('ORC_PRIORITY_CANDIDATE','ORC_WATCH_CANDIDATE')
     AND s.orc_readiness_score >= 65
     AND s.late_entry_risk < 50
      THEN true
    ELSE false
  END AS mme_orc_watch,

  CASE
    WHEN s.orc_hint = 'ORC_AVOID_LATE_ENTRY'
      OR s.late_entry_risk >= 65
      OR s.exhaustion_risk >= 65
      THEN true
    ELSE false
  END AS mme_orc_avoid,

  CASE
    WHEN s.orc_hint = 'ORC_PRIORITY_CANDIDATE'
     AND s.orc_readiness_score >= 80
     AND s.late_entry_risk < 35
      THEN 'MME_PRIORITY_READY'

    WHEN s.orc_hint = 'ORC_WATCH_CANDIDATE'
     AND s.orc_readiness_score >= 65
     AND s.late_entry_risk < 50
      THEN 'MME_WATCH_READY'

    WHEN s.orc_hint = 'ORC_AVOID_LATE_ENTRY'
      OR s.late_entry_risk >= 65
      OR s.exhaustion_risk >= 65
      THEN 'MME_AVOID_LATE_OR_EXHAUSTED'

    ELSE 'MME_OBSERVE'
  END AS mme_orc_status,

  jsonb_build_object(
    'symbol', s.symbol,
    'interval', s.interval,
    'orc_hint', s.orc_hint,
    'orc_readiness_score', s.orc_readiness_score,
    'sequence_type', s.sequence_type,
    'sequence_stage', s.sequence_stage,
    'sequence_quality', s.sequence_quality,
    'continuation_score', s.continuation_score,
    'reversal_score', s.reversal_score,
    'late_entry_risk', s.late_entry_risk,
    'rank_score', s.rank_score,
    'global_rank', s.global_rank,
    'ranking_status', s.ranking_status,
    'opportunity_score', s.opportunity_score,
    'confidence_score', s.confidence_score,
    'urgency_score', s.urgency_score,
    'exhaustion_risk', s.exhaustion_risk,
    'remaining_score', s.remaining_score,
    'timing_score', s.timing_score,
    'long_context', s.long_context,
    'short_context', s.short_context,
    'direction', s.direction,
    'status', CASE
      WHEN s.orc_hint = 'ORC_PRIORITY_CANDIDATE'
       AND s.orc_readiness_score >= 80
       AND s.late_entry_risk < 35
        THEN 'MME_PRIORITY_READY'

      WHEN s.orc_hint = 'ORC_WATCH_CANDIDATE'
       AND s.orc_readiness_score >= 65
       AND s.late_entry_risk < 50
        THEN 'MME_WATCH_READY'

      WHEN s.orc_hint = 'ORC_AVOID_LATE_ENTRY'
        OR s.late_entry_risk >= 65
        OR s.exhaustion_risk >= 65
        THEN 'MME_AVOID_LATE_OR_EXHAUSTED'

      ELSE 'MME_OBSERVE'
    END,
    'reason', s.reason
  ) AS payload

FROM v_market_memory_sequence_current s;

CREATE OR REPLACE VIEW v_market_memory_orc_context_best_v17 AS
SELECT *
FROM v_market_memory_orc_context_v17
ORDER BY
  mme_orc_priority DESC,
  orc_readiness_score DESC NULLS LAST,
  sequence_quality DESC NULLS LAST,
  continuation_score DESC NULLS LAST
LIMIT 1;

COMMIT;

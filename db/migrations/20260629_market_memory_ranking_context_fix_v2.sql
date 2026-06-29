BEGIN;

ALTER TABLE market_memory_ranking
  ADD COLUMN IF NOT EXISTS long_context TEXT,
  ADD COLUMN IF NOT EXISTS short_context TEXT;

DROP VIEW IF EXISTS v_market_memory_best_sequence CASCADE;
DROP VIEW IF EXISTS v_market_memory_sequence_current CASCADE;
DROP VIEW IF EXISTS v_market_memory_best_opportunity CASCADE;
DROP VIEW IF EXISTS v_market_memory_ranking_current CASCADE;

CREATE OR REPLACE VIEW v_market_memory_ranking_current AS
SELECT
  symbol,
  interval,
  rank_key,
  opportunity_score,
  confidence_score,
  urgency_score,
  exhaustion_risk,
  remaining_score,
  timing_score,
  rank_score,
  global_rank,
  ranking_status,
  reason,
  opportunity_type,
  stage,
  direction,
  action_hint,
  timeline_type,
  chain_length,
  chain_age_minutes,
  long_context,
  short_context,
  first_event_at,
  last_event_at,
  expires_at,
  payload,
  refreshed_at
FROM market_memory_ranking
WHERE expires_at > now();

CREATE OR REPLACE VIEW v_market_memory_best_opportunity AS
SELECT *
FROM v_market_memory_ranking_current
ORDER BY global_rank ASC NULLS LAST, rank_score DESC NULLS LAST
LIMIT 1;

CREATE OR REPLACE VIEW v_market_memory_sequence_current AS
SELECT *
FROM market_memory_sequence
WHERE expires_at > now();

CREATE OR REPLACE VIEW v_market_memory_best_sequence AS
SELECT *
FROM v_market_memory_sequence_current
ORDER BY orc_readiness_score DESC NULLS LAST,
         sequence_quality DESC NULLS LAST,
         continuation_score DESC NULLS LAST
LIMIT 1;

CREATE OR REPLACE FUNCTION refresh_market_memory_ranking_v15()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM market_memory_ranking
  WHERE expires_at <= now();

  INSERT INTO market_memory_ranking (
    symbol, interval, rank_key,
    opportunity_score, confidence_score, urgency_score, exhaustion_risk,
    remaining_score, timing_score, rank_score, global_rank,
    ranking_status, reason,
    opportunity_type, stage, direction, action_hint, timeline_type,
    chain_length, chain_age_minutes,
    long_context, short_context,
    first_event_at, last_event_at, expires_at, payload, refreshed_at
  )
  WITH src AS (
    SELECT *
    FROM v_market_memory_opportunity_active
  ),
  scored AS (
    SELECT
      s.*,

      LEAST(100, GREATEST(0,
        100
        - COALESCE(exhaustion_risk,0) * 0.75
        - CASE
            WHEN chain_age_minutes <= 10 THEN 0
            WHEN chain_age_minutes <= 30 THEN 10
            WHEN chain_age_minutes <= 60 THEN 25
            ELSE 45
          END
        + CASE WHEN stage IN ('TRIGGER','EXPANSION') THEN 12 ELSE 0 END
      )) AS remaining_score_calc,

      LEAST(100, GREATEST(0,
        COALESCE(urgency_score,0) * 0.45
        + CASE WHEN stage='TRIGGER' THEN 30
               WHEN stage='EXPANSION' THEN 22
               WHEN stage='LATE_EXPANSION' THEN 8
               ELSE -10 END
        + CASE WHEN chain_age_minutes <= 10 THEN 18
               WHEN chain_age_minutes <= 30 THEN 10
               ELSE 0 END
      )) AS timing_score_calc
    FROM src s
  ),
  ranked AS (
    SELECT
      *,
      LEAST(100, GREATEST(0,
        COALESCE(opportunity_score,0) * 0.35
        + COALESCE(confidence_score,0) * 0.20
        + COALESCE(urgency_score,0) * 0.15
        + remaining_score_calc * 0.20
        + timing_score_calc * 0.10
        - COALESCE(exhaustion_risk,0) * 0.20
      )) AS rank_score_calc
    FROM scored
  ),
  final AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        ORDER BY rank_score_calc DESC NULLS LAST,
                 remaining_score_calc DESC NULLS LAST,
                 urgency_score DESC NULLS LAST
      ) AS rn
    FROM ranked
  ),
  labeled AS (
    SELECT
      *,
      CASE
        WHEN rank_score_calc >= 80 AND remaining_score_calc >= 60 AND exhaustion_risk < 45 THEN 'PRIORITY'
        WHEN rank_score_calc >= 65 AND remaining_score_calc >= 45 AND exhaustion_risk < 60 THEN 'WATCH'
        WHEN exhaustion_risk >= 60 OR remaining_score_calc < 30 THEN 'LATE_OR_EXHAUSTED'
        ELSE 'OBSERVE'
      END AS ranking_status_calc,

      CASE
        WHEN rank_score_calc >= 80 AND remaining_score_calc >= 60 AND exhaustion_risk < 45 THEN 'BEST_ACTIVE_OPPORTUNITY'
        WHEN rank_score_calc >= 65 AND remaining_score_calc >= 45 AND exhaustion_risk < 60 THEN 'GOOD_BUT_NOT_TOP_PRIORITY'
        WHEN exhaustion_risk >= 60 OR remaining_score_calc < 30 THEN 'MOVE_TOO_LATE_OR_EXHAUSTED'
        ELSE 'LOW_RANK_OR_INSUFFICIENT_TIMING'
      END AS reason_calc
    FROM final
  )
  SELECT
    symbol,
    interval,
    md5(symbol || '|' || interval || '|MME_RANKING_V15') AS rank_key,

    opportunity_score,
    confidence_score,
    urgency_score,
    exhaustion_risk,
    ROUND(remaining_score_calc,6),
    ROUND(timing_score_calc,6),
    ROUND(rank_score_calc,6),
    rn::INTEGER,

    ranking_status_calc,
    reason_calc,

    opportunity_type,
    stage,
    direction,
    action_hint,
    timeline_type,
    chain_length,
    chain_age_minutes,
    long_context,
    short_context,
    first_event_at,
    last_event_at,
    expires_at,

    jsonb_build_object(
      'opportunity_score', opportunity_score,
      'confidence_score', confidence_score,
      'urgency_score', urgency_score,
      'exhaustion_risk', exhaustion_risk,
      'remaining_score', ROUND(remaining_score_calc,6),
      'timing_score', ROUND(timing_score_calc,6),
      'rank_score', ROUND(rank_score_calc,6),
      'global_rank', rn,
      'ranking_status', ranking_status_calc,
      'stage', stage,
      'action_hint', action_hint,
      'long_context', long_context,
      'short_context', short_context,
      'reason', reason_calc
    ),
    now()
  FROM labeled
  ON CONFLICT (symbol, interval) DO UPDATE SET
    rank_key = EXCLUDED.rank_key,
    opportunity_score = EXCLUDED.opportunity_score,
    confidence_score = EXCLUDED.confidence_score,
    urgency_score = EXCLUDED.urgency_score,
    exhaustion_risk = EXCLUDED.exhaustion_risk,
    remaining_score = EXCLUDED.remaining_score,
    timing_score = EXCLUDED.timing_score,
    rank_score = EXCLUDED.rank_score,
    global_rank = EXCLUDED.global_rank,
    ranking_status = EXCLUDED.ranking_status,
    reason = EXCLUDED.reason,
    opportunity_type = EXCLUDED.opportunity_type,
    stage = EXCLUDED.stage,
    direction = EXCLUDED.direction,
    action_hint = EXCLUDED.action_hint,
    timeline_type = EXCLUDED.timeline_type,
    chain_length = EXCLUDED.chain_length,
    chain_age_minutes = EXCLUDED.chain_age_minutes,
    long_context = EXCLUDED.long_context,
    short_context = EXCLUDED.short_context,
    first_event_at = EXCLUDED.first_event_at,
    last_event_at = EXCLUDED.last_event_at,
    expires_at = EXCLUDED.expires_at,
    payload = EXCLUDED.payload,
    refreshed_at = now();
END;
$$;

COMMIT;

BEGIN;

CREATE TABLE IF NOT EXISTS market_memory_sequence (
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  sequence_key TEXT NOT NULL,

  sequence_type TEXT NOT NULL,
  sequence_stage TEXT NOT NULL,
  direction TEXT,

  sequence_quality NUMERIC(10,6),
  continuation_score NUMERIC(10,6),
  reversal_score NUMERIC(10,6),
  late_entry_risk NUMERIC(10,6),
  orc_readiness_score NUMERIC(10,6),

  orc_hint TEXT NOT NULL DEFAULT 'OBSERVE',
  reason TEXT,

  opportunity_score NUMERIC(10,6),
  confidence_score NUMERIC(10,6),
  urgency_score NUMERIC(10,6),
  exhaustion_risk NUMERIC(10,6),
  remaining_score NUMERIC(10,6),
  timing_score NUMERIC(10,6),
  rank_score NUMERIC(10,6),
  global_rank INTEGER,
  ranking_status TEXT,

  opportunity_type TEXT,
  opportunity_stage TEXT,
  action_hint TEXT,
  timeline_type TEXT,
  chain_length INTEGER,
  chain_age_minutes NUMERIC(10,2),
  long_context TEXT,
  short_context TEXT,

  first_event_at TIMESTAMPTZ,
  last_event_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ NOT NULL,

  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (symbol, interval)
);

CREATE INDEX IF NOT EXISTS ix_market_memory_sequence_orc
ON market_memory_sequence(orc_hint, orc_readiness_score DESC, expires_at DESC);

CREATE INDEX IF NOT EXISTS ix_market_memory_sequence_quality
ON market_memory_sequence(sequence_quality DESC, continuation_score DESC, expires_at DESC);

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

CREATE OR REPLACE FUNCTION refresh_market_memory_sequence_v16()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM market_memory_sequence
  WHERE expires_at <= now();

  INSERT INTO market_memory_sequence (
    symbol,
    interval,
    sequence_key,
    sequence_type,
    sequence_stage,
    direction,
    sequence_quality,
    continuation_score,
    reversal_score,
    late_entry_risk,
    orc_readiness_score,
    orc_hint,
    reason,
    opportunity_score,
    confidence_score,
    urgency_score,
    exhaustion_risk,
    remaining_score,
    timing_score,
    rank_score,
    global_rank,
    ranking_status,
    opportunity_type,
    opportunity_stage,
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
  )
  WITH r AS (
    SELECT *
    FROM v_market_memory_ranking_current
  ),
  scored AS (
    SELECT
      r.*,

      CASE
        WHEN timeline_type='EARLY_REVERSAL_UP'
         AND stage IN ('TRIGGER','EXPANSION')
         AND COALESCE(remaining_score,0) >= 60
          THEN 'EARLY_REVERSAL_SEQUENCE'

        WHEN timeline_type='EARLY_REVERSAL_UP'
         AND stage='LATE_EXPANSION'
          THEN 'LATE_REVERSAL_SEQUENCE'

        WHEN stage='EXHAUSTION_RISK'
         OR COALESCE(exhaustion_risk,0) >= 60
          THEN 'EXHAUSTION_SEQUENCE'

        WHEN COALESCE(rank_score,0) >= 65
          THEN 'ACTIVE_IMPULSE_SEQUENCE'

        ELSE 'WEAK_SEQUENCE'
      END AS sequence_type_calc,

      CASE
        WHEN stage='TRIGGER' THEN 'SETUP_TRIGGER'
        WHEN stage='EXPANSION' THEN 'EXPANSION'
        WHEN stage='LATE_EXPANSION' THEN 'LATE_EXPANSION'
        WHEN stage='EXHAUSTION_RISK' THEN 'EXHAUSTION'
        ELSE 'OBSERVE'
      END AS sequence_stage_calc,

      LEAST(100, GREATEST(0,
        COALESCE(opportunity_score,0) * 0.20
        + COALESCE(confidence_score,0) * 0.20
        + COALESCE(remaining_score,0) * 0.25
        + COALESCE(timing_score,0) * 0.20
        + CASE WHEN timeline_type='EARLY_REVERSAL_UP' THEN 12 ELSE 0 END
        + CASE WHEN stage IN ('TRIGGER','EXPANSION') THEN 10 ELSE 0 END
        - COALESCE(exhaustion_risk,0) * 0.20
      )) AS sequence_quality_calc,

      LEAST(100, GREATEST(0,
        COALESCE(remaining_score,0) * 0.35
        + COALESCE(urgency_score,0) * 0.20
        + COALESCE(confidence_score,0) * 0.15
        + COALESCE(rank_score,0) * 0.20
        + CASE WHEN stage='EXPANSION' THEN 12
               WHEN stage='TRIGGER' THEN 8
               WHEN stage='LATE_EXPANSION' THEN -8
               ELSE -15 END
        - COALESCE(exhaustion_risk,0) * 0.20
      )) AS continuation_score_calc,

      LEAST(100, GREATEST(0,
        CASE WHEN timeline_type='EARLY_REVERSAL_UP' THEN 35 ELSE 0 END
        + CASE WHEN long_context LIKE 'BEAR_LONG%' AND short_context LIKE 'BULL_SHORT%' THEN 25 ELSE 0 END
        + COALESCE(confidence_score,0) * 0.20
        + COALESCE(urgency_score,0) * 0.15
        - COALESCE(exhaustion_risk,0) * 0.15
      )) AS reversal_score_calc,

      LEAST(100, GREATEST(0,
        COALESCE(exhaustion_risk,0) * 0.55
        + CASE WHEN chain_age_minutes > 60 THEN 35
               WHEN chain_age_minutes > 30 THEN 22
               WHEN chain_age_minutes > 15 THEN 10
               ELSE 2 END
        + CASE WHEN stage='LATE_EXPANSION' THEN 15 ELSE 0 END
        + CASE WHEN stage='EXHAUSTION_RISK' THEN 30 ELSE 0 END
        - COALESCE(remaining_score,0) * 0.15
      )) AS late_entry_risk_calc
    FROM r
  ),
  final AS (
    SELECT
      *,
      LEAST(100, GREATEST(0,
        sequence_quality_calc * 0.30
        + continuation_score_calc * 0.25
        + reversal_score_calc * 0.15
        + COALESCE(rank_score,0) * 0.20
        + COALESCE(remaining_score,0) * 0.10
        - late_entry_risk_calc * 0.25
      )) AS orc_readiness_score_calc
    FROM scored
  ),
  labeled AS (
    SELECT
      *,
      CASE
        WHEN orc_readiness_score_calc >= 80
         AND late_entry_risk_calc < 35
         AND continuation_score_calc >= 65
          THEN 'ORC_PRIORITY_CANDIDATE'

        WHEN orc_readiness_score_calc >= 65
         AND late_entry_risk_calc < 50
          THEN 'ORC_WATCH_CANDIDATE'

        WHEN late_entry_risk_calc >= 65
          THEN 'ORC_AVOID_LATE_ENTRY'

        ELSE 'ORC_OBSERVE'
      END AS orc_hint_calc,

      CASE
        WHEN orc_readiness_score_calc >= 80
         AND late_entry_risk_calc < 35
         AND continuation_score_calc >= 65
          THEN 'SEQUENCE_STRONG_EARLY_OR_CONTINUATION'

        WHEN orc_readiness_score_calc >= 65
         AND late_entry_risk_calc < 50
          THEN 'SEQUENCE_VALID_BUT_NEEDS_CONFIRMATION'

        WHEN late_entry_risk_calc >= 65
          THEN 'SEQUENCE_TOO_LATE_OR_EXHAUSTED'

        ELSE 'SEQUENCE_WEAK_OR_INCOMPLETE'
      END AS reason_calc
    FROM final
  )
  SELECT
    symbol,
    interval,
    md5(symbol || '|' || interval || '|MME_SEQUENCE_V16') AS sequence_key,
    sequence_type_calc,
    sequence_stage_calc,
    direction,

    ROUND(sequence_quality_calc,6),
    ROUND(continuation_score_calc,6),
    ROUND(reversal_score_calc,6),
    ROUND(late_entry_risk_calc,6),
    ROUND(orc_readiness_score_calc,6),

    orc_hint_calc,
    reason_calc,

    opportunity_score,
    confidence_score,
    urgency_score,
    exhaustion_risk,
    remaining_score,
    timing_score,
    rank_score,
    global_rank,
    ranking_status,

    opportunity_type,
    stage,
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
      'sequence_type', sequence_type_calc,
      'sequence_stage', sequence_stage_calc,
      'sequence_quality', ROUND(sequence_quality_calc,6),
      'continuation_score', ROUND(continuation_score_calc,6),
      'reversal_score', ROUND(reversal_score_calc,6),
      'late_entry_risk', ROUND(late_entry_risk_calc,6),
      'orc_readiness_score', ROUND(orc_readiness_score_calc,6),
      'orc_hint', orc_hint_calc,
      'reason', reason_calc,
      'rank_score', rank_score,
      'remaining_score', remaining_score,
      'timing_score', timing_score,
      'opportunity_score', opportunity_score,
      'confidence_score', confidence_score,
      'urgency_score', urgency_score,
      'exhaustion_risk', exhaustion_risk,
      'stage', stage,
      'timeline_type', timeline_type,
      'chain_age_minutes', chain_age_minutes
    ),
    now()
  FROM labeled
  ON CONFLICT (symbol, interval) DO UPDATE SET
    sequence_key = EXCLUDED.sequence_key,
    sequence_type = EXCLUDED.sequence_type,
    sequence_stage = EXCLUDED.sequence_stage,
    direction = EXCLUDED.direction,
    sequence_quality = EXCLUDED.sequence_quality,
    continuation_score = EXCLUDED.continuation_score,
    reversal_score = EXCLUDED.reversal_score,
    late_entry_risk = EXCLUDED.late_entry_risk,
    orc_readiness_score = EXCLUDED.orc_readiness_score,
    orc_hint = EXCLUDED.orc_hint,
    reason = EXCLUDED.reason,
    opportunity_score = EXCLUDED.opportunity_score,
    confidence_score = EXCLUDED.confidence_score,
    urgency_score = EXCLUDED.urgency_score,
    exhaustion_risk = EXCLUDED.exhaustion_risk,
    remaining_score = EXCLUDED.remaining_score,
    timing_score = EXCLUDED.timing_score,
    rank_score = EXCLUDED.rank_score,
    global_rank = EXCLUDED.global_rank,
    ranking_status = EXCLUDED.ranking_status,
    opportunity_type = EXCLUDED.opportunity_type,
    opportunity_stage = EXCLUDED.opportunity_stage,
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

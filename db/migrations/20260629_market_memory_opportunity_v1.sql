BEGIN;

CREATE TABLE IF NOT EXISTS market_memory_opportunity (
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  opportunity_key TEXT NOT NULL,
  opportunity_type TEXT NOT NULL,

  stage TEXT NOT NULL,
  direction TEXT,
  opportunity_score NUMERIC(10,6),
  confidence_score NUMERIC(10,6),
  urgency_score NUMERIC(10,6),
  exhaustion_risk NUMERIC(10,6),

  action_hint TEXT NOT NULL DEFAULT 'OBSERVE',
  reason TEXT,

  timeline_type TEXT,
  chain_score NUMERIC(10,6),
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

CREATE INDEX IF NOT EXISTS ix_market_memory_opportunity_score
ON market_memory_opportunity(opportunity_score DESC, action_hint, expires_at DESC);

CREATE OR REPLACE VIEW v_market_memory_opportunity_active AS
SELECT *
FROM market_memory_opportunity
WHERE expires_at > now();

CREATE OR REPLACE FUNCTION refresh_market_memory_opportunity_v1()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM market_memory_opportunity
  WHERE expires_at <= now();

  INSERT INTO market_memory_opportunity (
    symbol,
    interval,
    opportunity_key,
    opportunity_type,
    stage,
    direction,
    opportunity_score,
    confidence_score,
    urgency_score,
    exhaustion_risk,
    action_hint,
    reason,
    timeline_type,
    chain_score,
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
  WITH t AS (
    SELECT *
    FROM v_market_memory_timeline_active
  ),
  scored AS (
    SELECT
      t.*,

      CASE
        WHEN chain_age_minutes <= 10 AND has_breakout_up AND (has_volume_spike OR has_atr_expansion)
          THEN 'TRIGGER'
        WHEN chain_age_minutes <= 30 AND has_breakout_up AND has_atr_expansion
          THEN 'EXPANSION'
        WHEN chain_age_minutes <= 60 AND has_breakout_up
          THEN 'LATE_EXPANSION'
        ELSE 'EXHAUSTION_RISK'
      END AS stage_calc,

      LEAST(100, GREATEST(0,
        COALESCE(chain_score,0) * 0.45
        + CASE WHEN timeline_type='EARLY_REVERSAL_UP' THEN 20 ELSE 0 END
        + CASE WHEN has_volume_spike THEN 10 ELSE 0 END
        + CASE WHEN has_atr_expansion THEN 8 ELSE 0 END
        + CASE WHEN has_breakout_up THEN 10 ELSE 0 END
        + CASE WHEN has_momentum_up THEN 8 ELSE 0 END
        + CASE WHEN chain_age_minutes <= 10 THEN 12
               WHEN chain_age_minutes <= 30 THEN 7
               WHEN chain_age_minutes <= 60 THEN 2
               ELSE -10 END
      )) AS opportunity_score_calc,

      LEAST(100, GREATEST(0,
        COALESCE(chain_score,0) * 0.50
        + LEAST(chain_length, 6) * 6
        + CASE WHEN long_context LIKE 'BEAR_LONG%' AND short_context LIKE 'BULL_SHORT%' THEN 15 ELSE 0 END
        + CASE WHEN has_breakout_up THEN 8 ELSE 0 END
        + CASE WHEN has_atr_expansion THEN 6 ELSE 0 END
        + CASE WHEN has_volume_spike THEN 6 ELSE 0 END
      )) AS confidence_score_calc,

      LEAST(100, GREATEST(0,
        CASE WHEN timeline_type='EARLY_REVERSAL_UP' THEN 25 ELSE 0 END
        + CASE WHEN chain_age_minutes <= 10 THEN 35
               WHEN chain_age_minutes <= 30 THEN 25
               WHEN chain_age_minutes <= 60 THEN 12
               ELSE 2 END
        + CASE WHEN has_volume_spike THEN 15 ELSE 0 END
        + CASE WHEN has_breakout_up THEN 15 ELSE 0 END
      )) AS urgency_score_calc,

      LEAST(100, GREATEST(0,
        CASE WHEN chain_age_minutes > 60 THEN 45
             WHEN chain_age_minutes > 30 THEN 25
             WHEN chain_age_minutes > 15 THEN 12
             ELSE 3 END
        + CASE WHEN has_momentum_up THEN 15 ELSE 0 END
        + CASE WHEN direction IN ('DOWN','MIXED') THEN 10 ELSE 0 END
      )) AS exhaustion_risk_calc
    FROM t
  ),
  final AS (
    SELECT
      *,
      CASE
        WHEN opportunity_score_calc >= 85
         AND confidence_score_calc >= 75
         AND exhaustion_risk_calc < 35
          THEN 'PRIORITY_WATCH'

        WHEN opportunity_score_calc >= 70
         AND confidence_score_calc >= 60
         AND exhaustion_risk_calc < 50
          THEN 'WATCH'

        WHEN exhaustion_risk_calc >= 60
          THEN 'LATE_OR_RISKY'

        ELSE 'OBSERVE'
      END AS action_hint_calc,

      CASE
        WHEN opportunity_score_calc >= 85
         AND confidence_score_calc >= 75
         AND exhaustion_risk_calc < 35
          THEN 'HIGH_OPPORTUNITY_EARLY_STAGE'

        WHEN opportunity_score_calc >= 70
         AND confidence_score_calc >= 60
         AND exhaustion_risk_calc < 50
          THEN 'VALID_OPPORTUNITY_BUT_MONITOR_TIMING'

        WHEN exhaustion_risk_calc >= 60
          THEN 'MOVE_MAY_BE_LATE_OR_REVERSING'

        ELSE 'INSUFFICIENT_EDGE_FOR_ACTION'
      END AS reason_calc
    FROM scored
  )
  SELECT
    symbol,
    interval,
    md5(symbol || '|' || interval || '|OPPORTUNITY') AS opportunity_key,
    CASE
      WHEN timeline_type='EARLY_REVERSAL_UP' THEN 'EARLY_REVERSAL_OPPORTUNITY'
      WHEN timeline_type LIKE '%BREAKOUT%' THEN 'BREAKOUT_OPPORTUNITY'
      ELSE 'MARKET_ACTIVITY_OPPORTUNITY'
    END AS opportunity_type,
    stage_calc,
    direction,
    ROUND(opportunity_score_calc, 6),
    ROUND(confidence_score_calc, 6),
    ROUND(urgency_score_calc, 6),
    ROUND(exhaustion_risk_calc, 6),
    action_hint_calc,
    reason_calc,
    timeline_type,
    chain_score,
    chain_length,
    chain_age_minutes,
    long_context,
    short_context,
    first_event_at,
    last_event_at,
    expires_at,
    jsonb_build_object(
      'timeline_type', timeline_type,
      'stage', stage_calc,
      'direction', direction,
      'chain_score', chain_score,
      'chain_length', chain_length,
      'chain_age_minutes', chain_age_minutes,
      'long_context', long_context,
      'short_context', short_context,
      'has_volume_spike', has_volume_spike,
      'has_atr_expansion', has_atr_expansion,
      'has_breakout_up', has_breakout_up,
      'has_momentum_up', has_momentum_up,
      'opportunity_score', ROUND(opportunity_score_calc, 6),
      'confidence_score', ROUND(confidence_score_calc, 6),
      'urgency_score', ROUND(urgency_score_calc, 6),
      'exhaustion_risk', ROUND(exhaustion_risk_calc, 6)
    ),
    now()
  FROM final
  ON CONFLICT (symbol, interval) DO UPDATE SET
    opportunity_key = EXCLUDED.opportunity_key,
    opportunity_type = EXCLUDED.opportunity_type,
    stage = EXCLUDED.stage,
    direction = EXCLUDED.direction,
    opportunity_score = EXCLUDED.opportunity_score,
    confidence_score = EXCLUDED.confidence_score,
    urgency_score = EXCLUDED.urgency_score,
    exhaustion_risk = EXCLUDED.exhaustion_risk,
    action_hint = EXCLUDED.action_hint,
    reason = EXCLUDED.reason,
    timeline_type = EXCLUDED.timeline_type,
    chain_score = EXCLUDED.chain_score,
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

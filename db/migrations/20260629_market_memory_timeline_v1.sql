BEGIN;

CREATE TABLE IF NOT EXISTS market_memory_timeline (
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  timeline_key TEXT NOT NULL,
  timeline_type TEXT NOT NULL,
  direction TEXT,

  chain_length INTEGER NOT NULL DEFAULT 0,
  chain_score NUMERIC(10,6),
  chain_importance TEXT,

  long_context TEXT,
  short_context TEXT,

  has_volume_spike BOOLEAN NOT NULL DEFAULT false,
  has_atr_expansion BOOLEAN NOT NULL DEFAULT false,
  has_breakout_up BOOLEAN NOT NULL DEFAULT false,
  has_momentum_up BOOLEAN NOT NULL DEFAULT false,
  has_reversal_candidate BOOLEAN NOT NULL DEFAULT false,

  first_event_at TIMESTAMPTZ,
  last_event_at TIMESTAMPTZ,
  chain_age_minutes NUMERIC(10,2),
  expires_at TIMESTAMPTZ NOT NULL,

  status TEXT NOT NULL DEFAULT 'ACTIVE',
  reason TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (symbol, interval)
);

CREATE INDEX IF NOT EXISTS ix_market_memory_timeline_score
ON market_memory_timeline(chain_score DESC, chain_importance, expires_at DESC);

CREATE OR REPLACE VIEW v_market_memory_timeline_active AS
SELECT *
FROM market_memory_timeline
WHERE expires_at > now()
  AND status = 'ACTIVE';

CREATE OR REPLACE FUNCTION refresh_market_memory_timeline_v1()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM market_memory_timeline
  WHERE expires_at <= now();

  INSERT INTO market_memory_timeline (
    symbol,
    interval,
    timeline_key,
    timeline_type,
    direction,
    chain_length,
    chain_score,
    chain_importance,
    long_context,
    short_context,
    has_volume_spike,
    has_atr_expansion,
    has_breakout_up,
    has_momentum_up,
    has_reversal_candidate,
    first_event_at,
    last_event_at,
    chain_age_minutes,
    expires_at,
    status,
    reason,
    payload,
    refreshed_at
  )
  WITH e AS (
    SELECT *
    FROM v_market_memory_events_active
  ),
  c AS (
    SELECT *
    FROM v_market_memory_event_clusters_active
  ),
  mm AS (
    SELECT *
    FROM market_memory_snapshot
    WHERE window_label IN ('15m','1h','6h','24h','7d','30d','90d')
  ),
  ctx AS (
    SELECT
      symbol,
      interval,

      MIN(return_pct) FILTER (WHERE window_label IN ('7d','30d','90d')) AS worst_long_return_pct,
      MAX(return_pct) FILTER (WHERE window_label IN ('15m','1h','6h')) AS best_short_return_pct,
      MAX(volume_ratio) FILTER (WHERE window_label IN ('15m','1h')) AS max_short_volume_ratio,
      MAX(range_pct) FILTER (WHERE window_label IN ('15m','1h')) AS max_short_range_pct,

      MAX(realtime_score) FILTER (WHERE window_label IN ('15m','1h','6h')) AS max_short_realtime_score
    FROM mm
    GROUP BY symbol, interval
  ),
  chain AS (
    SELECT
      e.symbol,
      e.interval,

      COUNT(*) AS chain_length,
      MIN(COALESCE(e.first_observed_at, e.observed_at)) AS first_event_at,
      MAX(COALESCE(e.last_observed_at, e.observed_at)) AS last_event_at,
      MAX(e.expires_at) AS expires_at,
      MAX(e.score) AS max_event_score,
      AVG(e.score) AS avg_event_score,

      BOOL_OR(e.event_type='VOLUME_SPIKE') AS has_volume_spike,
      BOOL_OR(e.event_type='ATR_EXPANSION') AS has_atr_expansion,
      BOOL_OR(e.event_type='BREAKOUT_UP') AS has_breakout_up,
      BOOL_OR(e.event_type='MOMENTUM_UP') AS has_momentum_up,
      BOOL_OR(e.event_type='REVERSAL_UP_CANDIDATE') AS has_reversal_candidate,

      COUNT(*) FILTER (WHERE e.direction='UP') AS up_events,
      COUNT(*) FILTER (WHERE e.direction='DOWN') AS down_events,

      jsonb_agg(
        jsonb_build_object(
          'event_type', e.event_type,
          'importance', e.importance,
          'score', e.score,
          'direction', e.direction,
          'window_label', e.window_label,
          'first_observed_at', e.first_observed_at,
          'last_observed_at', e.last_observed_at,
          'reason', e.reason
        )
        ORDER BY COALESCE(e.first_observed_at, e.observed_at), e.score DESC NULLS LAST
      ) AS events_json
    FROM e
    GROUP BY e.symbol, e.interval
  ),
  scored AS (
    SELECT
      ch.*,
      ctx.worst_long_return_pct,
      ctx.best_short_return_pct,
      ctx.max_short_volume_ratio,
      ctx.max_short_range_pct,
      ctx.max_short_realtime_score,
      c.cluster_type,
      c.cluster_score,
      c.cluster_importance,

      EXTRACT(EPOCH FROM (ch.last_event_at - ch.first_event_at)) / 60.0 AS chain_age_minutes,

      CASE
        WHEN ctx.worst_long_return_pct <= -8 THEN 'BEAR_LONG_STRONG'
        WHEN ctx.worst_long_return_pct <= -3 THEN 'BEAR_LONG'
        ELSE 'NEUTRAL_LONG'
      END AS long_context,

      CASE
        WHEN ctx.best_short_return_pct >= 1.2 THEN 'BULL_SHORT_STRONG'
        WHEN ctx.best_short_return_pct >= 0.35 THEN 'BULL_SHORT'
        ELSE 'NEUTRAL_SHORT'
      END AS short_context
    FROM chain ch
    LEFT JOIN ctx
      ON ctx.symbol = ch.symbol
     AND ctx.interval = ch.interval
    LEFT JOIN c
      ON c.symbol = ch.symbol
     AND c.interval = ch.interval
  ),
  final AS (
    SELECT
      *,
      LEAST(100, GREATEST(0,
        COALESCE(max_event_score,0) * 0.25
        + COALESCE(avg_event_score,0) * 0.15
        + COALESCE(cluster_score,0) * 0.25
        + LEAST(chain_length, 6) * 5
        + CASE WHEN has_volume_spike THEN 8 ELSE 0 END
        + CASE WHEN has_atr_expansion THEN 6 ELSE 0 END
        + CASE WHEN has_breakout_up THEN 10 ELSE 0 END
        + CASE WHEN has_momentum_up THEN 12 ELSE 0 END
        + CASE WHEN has_reversal_candidate THEN 18 ELSE 0 END
        + CASE WHEN worst_long_return_pct <= -3 AND best_short_return_pct >= 0.35 THEN 10 ELSE 0 END
        + CASE WHEN worst_long_return_pct <= -8 AND best_short_return_pct >= 0.70 THEN 12 ELSE 0 END
      )) AS chain_score
    FROM scored
  )
  SELECT
    symbol,
    interval,
    md5(symbol || '|' || interval || '|TIMELINE') AS timeline_key,

    CASE
      WHEN worst_long_return_pct <= -3
       AND best_short_return_pct >= 0.35
       AND has_breakout_up
       AND (has_volume_spike OR has_atr_expansion)
        THEN 'EARLY_REVERSAL_UP'

      WHEN has_volume_spike AND has_atr_expansion AND has_breakout_up AND has_momentum_up
        THEN 'FULL_IMPULSE_UP_CHAIN'

      WHEN has_atr_expansion AND has_breakout_up
        THEN 'VOLATILITY_BREAKOUT_CHAIN'

      WHEN has_breakout_up AND has_momentum_up
        THEN 'MOMENTUM_BREAKOUT_CHAIN'

      ELSE 'MARKET_ACTIVITY_CHAIN'
    END AS timeline_type,

    CASE
      WHEN up_events > down_events THEN 'UP'
      WHEN down_events > up_events THEN 'DOWN'
      ELSE 'MIXED'
    END AS direction,

    chain_length,
    ROUND(chain_score, 6),

    CASE
      WHEN chain_score >= 85 THEN 'EXTREME'
      WHEN chain_score >= 70 THEN 'HIGH'
      WHEN chain_score >= 45 THEN 'MEDIUM'
      ELSE 'LOW'
    END AS chain_importance,

    long_context,
    short_context,

    has_volume_spike,
    has_atr_expansion,
    has_breakout_up,
    has_momentum_up,
    has_reversal_candidate,

    first_event_at,
    last_event_at,
    ROUND(chain_age_minutes, 2),
    expires_at,
    'ACTIVE' AS status,

    CASE
      WHEN worst_long_return_pct <= -3
       AND best_short_return_pct >= 0.35
       AND has_breakout_up
       AND (has_volume_spike OR has_atr_expansion)
        THEN 'LONG_BEARISH_CONTEXT_WITH_SHORT_UP_IMPULSE'

      WHEN has_volume_spike AND has_atr_expansion AND has_breakout_up AND has_momentum_up
        THEN 'ORDERED_FULL_IMPULSE_COMPONENTS_PRESENT'

      WHEN has_atr_expansion AND has_breakout_up
        THEN 'VOLATILITY_EXPANSION_WITH_BREAKOUT'

      WHEN has_breakout_up AND has_momentum_up
        THEN 'BREAKOUT_WITH_MOMENTUM'

      ELSE 'MULTIPLE_MARKET_EVENTS_IN_SEQUENCE'
    END AS reason,

    jsonb_build_object(
      'events', events_json,
      'cluster_type', cluster_type,
      'cluster_score', cluster_score,
      'cluster_importance', cluster_importance,
      'worst_long_return_pct', worst_long_return_pct,
      'best_short_return_pct', best_short_return_pct,
      'max_short_volume_ratio', max_short_volume_ratio,
      'max_short_range_pct', max_short_range_pct,
      'max_short_realtime_score', max_short_realtime_score,
      'chain_length', chain_length,
      'chain_age_minutes', ROUND(chain_age_minutes, 2)
    ) AS payload,

    now()
  FROM final
  WHERE chain_length >= 2
  ON CONFLICT (symbol, interval) DO UPDATE SET
    timeline_key = EXCLUDED.timeline_key,
    timeline_type = EXCLUDED.timeline_type,
    direction = EXCLUDED.direction,
    chain_length = EXCLUDED.chain_length,
    chain_score = EXCLUDED.chain_score,
    chain_importance = EXCLUDED.chain_importance,
    long_context = EXCLUDED.long_context,
    short_context = EXCLUDED.short_context,
    has_volume_spike = EXCLUDED.has_volume_spike,
    has_atr_expansion = EXCLUDED.has_atr_expansion,
    has_breakout_up = EXCLUDED.has_breakout_up,
    has_momentum_up = EXCLUDED.has_momentum_up,
    has_reversal_candidate = EXCLUDED.has_reversal_candidate,
    first_event_at = LEAST(
      COALESCE(market_memory_timeline.first_event_at, EXCLUDED.first_event_at),
      EXCLUDED.first_event_at
    ),
    last_event_at = EXCLUDED.last_event_at,
    chain_age_minutes = EXCLUDED.chain_age_minutes,
    expires_at = GREATEST(market_memory_timeline.expires_at, EXCLUDED.expires_at),
    status = EXCLUDED.status,
    reason = EXCLUDED.reason,
    payload = EXCLUDED.payload,
    refreshed_at = now();
END;
$$;

COMMIT;

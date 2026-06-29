BEGIN;

CREATE TABLE IF NOT EXISTS market_memory_event_clusters (
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,

  cluster_key TEXT NOT NULL,
  cluster_type TEXT NOT NULL,
  direction TEXT,

  event_count INTEGER NOT NULL DEFAULT 0,
  extreme_events INTEGER NOT NULL DEFAULT 0,
  high_events INTEGER NOT NULL DEFAULT 0,
  medium_events INTEGER NOT NULL DEFAULT 0,
  low_events INTEGER NOT NULL DEFAULT 0,

  volume_spike INTEGER NOT NULL DEFAULT 0,
  atr_expansion INTEGER NOT NULL DEFAULT 0,
  breakout_up INTEGER NOT NULL DEFAULT 0,
  breakout_down INTEGER NOT NULL DEFAULT 0,
  momentum_up INTEGER NOT NULL DEFAULT 0,
  momentum_down INTEGER NOT NULL DEFAULT 0,
  reversal_up_candidate INTEGER NOT NULL DEFAULT 0,

  max_score NUMERIC(10,6),
  avg_score NUMERIC(10,6),
  cluster_score NUMERIC(10,6),
  cluster_importance TEXT,

  first_observed_at TIMESTAMPTZ,
  last_observed_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ NOT NULL,

  status TEXT NOT NULL DEFAULT 'ACTIVE',
  reason TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (symbol, interval)
);

CREATE INDEX IF NOT EXISTS ix_market_memory_event_clusters_score
ON market_memory_event_clusters(cluster_score DESC, cluster_importance, expires_at DESC);

CREATE OR REPLACE VIEW v_market_memory_event_clusters_active AS
SELECT *
FROM market_memory_event_clusters
WHERE expires_at > now()
  AND status = 'ACTIVE';

CREATE OR REPLACE FUNCTION refresh_market_memory_event_clusters_v1()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM market_memory_event_clusters
  WHERE expires_at <= now();

  INSERT INTO market_memory_event_clusters (
    symbol,
    interval,
    cluster_key,
    cluster_type,
    direction,
    event_count,
    extreme_events,
    high_events,
    medium_events,
    low_events,
    volume_spike,
    atr_expansion,
    breakout_up,
    breakout_down,
    momentum_up,
    momentum_down,
    reversal_up_candidate,
    max_score,
    avg_score,
    cluster_score,
    cluster_importance,
    first_observed_at,
    last_observed_at,
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
  agg AS (
    SELECT
      symbol,
      interval,

      COUNT(*) AS event_count,
      COUNT(*) FILTER (WHERE importance='EXTREME') AS extreme_events,
      COUNT(*) FILTER (WHERE importance='HIGH') AS high_events,
      COUNT(*) FILTER (WHERE importance='MEDIUM') AS medium_events,
      COUNT(*) FILTER (WHERE importance='LOW') AS low_events,

      COUNT(*) FILTER (WHERE event_type='VOLUME_SPIKE') AS volume_spike,
      COUNT(*) FILTER (WHERE event_type='ATR_EXPANSION') AS atr_expansion,
      COUNT(*) FILTER (WHERE event_type='BREAKOUT_UP') AS breakout_up,
      COUNT(*) FILTER (WHERE event_type='BREAKOUT_DOWN') AS breakout_down,
      COUNT(*) FILTER (WHERE event_type='MOMENTUM_UP') AS momentum_up,
      COUNT(*) FILTER (WHERE event_type='MOMENTUM_DOWN') AS momentum_down,
      COUNT(*) FILTER (WHERE event_type='REVERSAL_UP_CANDIDATE') AS reversal_up_candidate,

      MAX(score) AS max_score,
      AVG(score) AS avg_score,
      MIN(first_observed_at) AS first_observed_at,
      MAX(last_observed_at) AS last_observed_at,
      MAX(expires_at) AS expires_at,

      COUNT(*) FILTER (WHERE direction='UP') AS up_events,
      COUNT(*) FILTER (WHERE direction='DOWN') AS down_events,

      jsonb_agg(
        jsonb_build_object(
          'event_type', event_type,
          'importance', importance,
          'score', score,
          'direction', direction,
          'regime', regime,
          'window_label', window_label,
          'first_observed_at', first_observed_at,
          'last_observed_at', last_observed_at,
          'refresh_count', refresh_count,
          'reason', reason
        )
        ORDER BY score DESC NULLS LAST
      ) AS events_json
    FROM e
    GROUP BY symbol, interval
  ),
  scored AS (
    SELECT
      *,
      LEAST(100, GREATEST(0,
        COALESCE(max_score,0) * 0.35
        + COALESCE(avg_score,0) * 0.20
        + LEAST(event_count, 6) * 5
        + volume_spike * 10
        + breakout_up * 12
        + momentum_up * 12
        + atr_expansion * 6
        + reversal_up_candidate * 18
        + extreme_events * 10
        + high_events * 7
        + medium_events * 4
      )) AS cluster_score
    FROM agg
  )
  SELECT
    symbol,
    interval,
    md5(symbol || '|' || interval || '|CLUSTER') AS cluster_key,

    CASE
      WHEN reversal_up_candidate > 0 THEN 'REVERSAL_CLUSTER'
      WHEN volume_spike > 0 AND breakout_up > 0 AND momentum_up > 0 THEN 'IMPULSE_UP_CLUSTER'
      WHEN breakout_up > 0 AND momentum_up > 0 THEN 'MOMENTUM_BREAKOUT_CLUSTER'
      WHEN atr_expansion > 0 AND (breakout_up > 0 OR momentum_up > 0) THEN 'VOLATILITY_IMPULSE_CLUSTER'
      ELSE 'MIXED_ACTIVITY_CLUSTER'
    END AS cluster_type,

    CASE
      WHEN up_events > down_events THEN 'UP'
      WHEN down_events > up_events THEN 'DOWN'
      ELSE 'MIXED'
    END AS direction,

    event_count,
    extreme_events,
    high_events,
    medium_events,
    low_events,
    volume_spike,
    atr_expansion,
    breakout_up,
    breakout_down,
    momentum_up,
    momentum_down,
    reversal_up_candidate,
    ROUND(max_score, 6),
    ROUND(avg_score, 6),
    ROUND(cluster_score, 6),

    CASE
      WHEN cluster_score >= 85 THEN 'EXTREME'
      WHEN cluster_score >= 70 THEN 'HIGH'
      WHEN cluster_score >= 45 THEN 'MEDIUM'
      ELSE 'LOW'
    END AS cluster_importance,

    first_observed_at,
    last_observed_at,
    expires_at,
    'ACTIVE' AS status,

    CASE
      WHEN reversal_up_candidate > 0 THEN 'REVERSAL_UP_CANDIDATE_PRESENT'
      WHEN volume_spike > 0 AND breakout_up > 0 AND momentum_up > 0 THEN 'VOLUME_BREAKOUT_MOMENTUM_CONFLUENCE'
      WHEN breakout_up > 0 AND momentum_up > 0 THEN 'BREAKOUT_AND_MOMENTUM_CONFLUENCE'
      WHEN atr_expansion > 0 AND (breakout_up > 0 OR momentum_up > 0) THEN 'VOLATILITY_WITH_DIRECTIONAL_IMPULSE'
      ELSE 'MULTIPLE_MARKET_EVENTS_ACTIVE'
    END AS reason,

    jsonb_build_object(
      'events', events_json,
      'event_count', event_count,
      'up_events', up_events,
      'down_events', down_events,
      'volume_spike', volume_spike,
      'atr_expansion', atr_expansion,
      'breakout_up', breakout_up,
      'momentum_up', momentum_up,
      'reversal_up_candidate', reversal_up_candidate
    ) AS payload,

    now()
  FROM scored
  WHERE event_count >= 2
  ON CONFLICT (symbol, interval) DO UPDATE SET
    cluster_key = EXCLUDED.cluster_key,
    cluster_type = EXCLUDED.cluster_type,
    direction = EXCLUDED.direction,
    event_count = EXCLUDED.event_count,
    extreme_events = EXCLUDED.extreme_events,
    high_events = EXCLUDED.high_events,
    medium_events = EXCLUDED.medium_events,
    low_events = EXCLUDED.low_events,
    volume_spike = EXCLUDED.volume_spike,
    atr_expansion = EXCLUDED.atr_expansion,
    breakout_up = EXCLUDED.breakout_up,
    breakout_down = EXCLUDED.breakout_down,
    momentum_up = EXCLUDED.momentum_up,
    momentum_down = EXCLUDED.momentum_down,
    reversal_up_candidate = EXCLUDED.reversal_up_candidate,
    max_score = EXCLUDED.max_score,
    avg_score = EXCLUDED.avg_score,
    cluster_score = EXCLUDED.cluster_score,
    cluster_importance = EXCLUDED.cluster_importance,
    first_observed_at = LEAST(
      COALESCE(market_memory_event_clusters.first_observed_at, EXCLUDED.first_observed_at),
      EXCLUDED.first_observed_at
    ),
    last_observed_at = EXCLUDED.last_observed_at,
    expires_at = GREATEST(market_memory_event_clusters.expires_at, EXCLUDED.expires_at),
    status = EXCLUDED.status,
    reason = EXCLUDED.reason,
    payload = EXCLUDED.payload,
    refreshed_at = now();
END;
$$;

COMMIT;

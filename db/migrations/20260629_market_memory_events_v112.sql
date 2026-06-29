BEGIN;

-- Drop old uniqueness first, because canonical rewrite can temporarily create duplicate event_key.
ALTER TABLE market_memory_events
  DROP CONSTRAINT IF EXISTS market_memory_events_event_key_key;

DROP INDEX IF EXISTS ux_market_memory_events_logical_event;

ALTER TABLE market_memory_events
  ADD COLUMN IF NOT EXISTS importance TEXT,
  ADD COLUMN IF NOT EXISTS first_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS refresh_count INTEGER NOT NULL DEFAULT 1;

-- Canonicalize existing rows.
UPDATE market_memory_events
SET
  event_key = md5(symbol || '|' || interval || '|' || event_type || '|' || window_label),
  first_observed_at = COALESCE(first_observed_at, observed_at),
  last_observed_at = COALESCE(last_observed_at, observed_at),
  importance = COALESCE(
    importance,
    CASE
      WHEN score >= 85 THEN 'EXTREME'
      WHEN score >= 70 THEN 'HIGH'
      WHEN score >= 45 THEN 'MEDIUM'
      ELSE 'LOW'
    END
  );

-- Merge duplicate logical events into the best row.
WITH grouped AS (
  SELECT
    symbol,
    interval,
    event_type,
    window_label,
    MAX(score) AS max_score,
    MIN(COALESCE(first_observed_at, observed_at)) AS min_first_observed_at,
    MAX(COALESCE(last_observed_at, observed_at)) AS max_last_observed_at,
    MAX(expires_at) AS max_expires_at,
    SUM(COALESCE(refresh_count, 1)) AS sum_refresh_count
  FROM market_memory_events
  GROUP BY symbol, interval, event_type, window_label
),
winner AS (
  SELECT DISTINCT ON (e.symbol, e.interval, e.event_type, e.window_label)
    e.id,
    e.symbol,
    e.interval,
    e.event_type,
    e.window_label,
    g.max_score,
    g.min_first_observed_at,
    g.max_last_observed_at,
    g.max_expires_at,
    g.sum_refresh_count
  FROM market_memory_events e
  JOIN grouped g
    ON g.symbol = e.symbol
   AND g.interval = e.interval
   AND g.event_type = e.event_type
   AND g.window_label = e.window_label
  ORDER BY
    e.symbol,
    e.interval,
    e.event_type,
    e.window_label,
    e.score DESC NULLS LAST,
    e.expires_at DESC,
    e.observed_at DESC,
    e.id DESC
)
UPDATE market_memory_events e
SET
  event_key = md5(e.symbol || '|' || e.interval || '|' || e.event_type || '|' || e.window_label),
  score = w.max_score,
  importance = CASE
    WHEN w.max_score >= 85 THEN 'EXTREME'
    WHEN w.max_score >= 70 THEN 'HIGH'
    WHEN w.max_score >= 45 THEN 'MEDIUM'
    ELSE 'LOW'
  END,
  first_observed_at = w.min_first_observed_at,
  last_observed_at = w.max_last_observed_at,
  observed_at = w.max_last_observed_at,
  expires_at = w.max_expires_at,
  refresh_count = GREATEST(w.sum_refresh_count, 1)
FROM winner w
WHERE e.id = w.id;

WITH winner AS (
  SELECT DISTINCT ON (symbol, interval, event_type, window_label)
    id,
    symbol,
    interval,
    event_type,
    window_label
  FROM market_memory_events
  ORDER BY
    symbol,
    interval,
    event_type,
    window_label,
    score DESC NULLS LAST,
    expires_at DESC,
    observed_at DESC,
    id DESC
)
DELETE FROM market_memory_events e
WHERE NOT EXISTS (
  SELECT 1
  FROM winner w
  WHERE w.id = e.id
);

-- Recreate uniqueness after cleanup.
ALTER TABLE market_memory_events
  ADD CONSTRAINT market_memory_events_event_key_key UNIQUE (event_key);

CREATE UNIQUE INDEX IF NOT EXISTS ux_market_memory_events_logical_event
ON market_memory_events(symbol, interval, event_type, window_label);

CREATE INDEX IF NOT EXISTS ix_market_memory_events_importance
ON market_memory_events(importance, score DESC, observed_at DESC);

DROP VIEW IF EXISTS v_market_memory_event_summary;
DROP VIEW IF EXISTS v_market_memory_events_active;

CREATE OR REPLACE VIEW v_market_memory_events_active AS
SELECT *
FROM market_memory_events
WHERE expires_at > now();

CREATE OR REPLACE VIEW v_market_memory_event_summary AS
SELECT
  symbol,
  interval,
  COUNT(*) FILTER (WHERE expires_at > now()) AS active_events,
  COUNT(*) FILTER (WHERE event_type='VOLUME_SPIKE' AND expires_at > now()) AS volume_spike,
  COUNT(*) FILTER (WHERE event_type='ATR_EXPANSION' AND expires_at > now()) AS atr_expansion,
  COUNT(*) FILTER (WHERE event_type='BREAKOUT_UP' AND expires_at > now()) AS breakout_up,
  COUNT(*) FILTER (WHERE event_type='BREAKOUT_DOWN' AND expires_at > now()) AS breakout_down,
  COUNT(*) FILTER (WHERE event_type='MOMENTUM_UP' AND expires_at > now()) AS momentum_up,
  COUNT(*) FILTER (WHERE event_type='MOMENTUM_DOWN' AND expires_at > now()) AS momentum_down,
  COUNT(*) FILTER (WHERE event_type='REVERSAL_UP_CANDIDATE' AND expires_at > now()) AS reversal_up_candidate,
  COUNT(*) FILTER (WHERE importance='EXTREME' AND expires_at > now()) AS extreme_events,
  COUNT(*) FILTER (WHERE importance='HIGH' AND expires_at > now()) AS high_events,
  COUNT(*) FILTER (WHERE importance='MEDIUM' AND expires_at > now()) AS medium_events,
  COUNT(*) FILTER (WHERE importance='LOW' AND expires_at > now()) AS low_events,
  MAX(score) FILTER (WHERE expires_at > now()) AS max_score,
  MAX(observed_at) FILTER (WHERE expires_at > now()) AS last_event_at
FROM market_memory_events
GROUP BY symbol, interval;

CREATE OR REPLACE FUNCTION refresh_market_memory_events_v1()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM market_memory_events
  WHERE expires_at <= now();

  INSERT INTO market_memory_events (
    symbol,
    interval,
    event_type,
    event_key,
    score,
    importance,
    direction,
    regime,
    confidence,
    window_label,
    observed_at,
    first_observed_at,
    last_observed_at,
    expires_at,
    refresh_count,
    reason,
    payload
  )
  WITH mm AS (
    SELECT *
    FROM market_memory_snapshot
    WHERE window_label IN ('15m','1h','6h','24h','7d','30d','90d')
  ),
  base AS (
    SELECT
      mm.*,
      CASE
        WHEN return_pct > 0 THEN 'UP'
        WHEN return_pct < 0 THEN 'DOWN'
        ELSE 'FLAT'
      END AS direction_calc
    FROM mm
  ),
  events AS (
    SELECT
      symbol,
      interval,
      'MOMENTUM_UP'::TEXT AS event_type,
      ROUND(LEAST(100, GREATEST(0, ABS(return_pct) * 35 + COALESCE(volume_ratio,0) * 10)), 6) AS score,
      'UP'::TEXT AS direction,
      last_regime AS regime,
      last_confidence AS confidence,
      window_label,
      last_open_time AS observed_at,
      CASE window_label
        WHEN '15m' THEN now() + interval '20 minutes'
        WHEN '1h' THEN now() + interval '45 minutes'
        WHEN '6h' THEN now() + interval '90 minutes'
        ELSE now() + interval '120 minutes'
      END AS expires_at,
      'POSITIVE_RETURN_SHORT_WINDOW'::TEXT AS reason,
      jsonb_build_object(
        'return_pct', return_pct,
        'range_pct', range_pct,
        'volume_ratio', volume_ratio,
        'realtime_score', realtime_score,
        'candles_n', candles_n
      ) AS payload
    FROM base
    WHERE window_label IN ('15m','1h','6h')
      AND return_pct >= CASE window_label
        WHEN '15m' THEN 0.35
        WHEN '1h' THEN 0.75
        WHEN '6h' THEN 1.25
        ELSE 999
      END

    UNION ALL

    SELECT
      s.symbol,
      s.interval,
      'REVERSAL_UP_CANDIDATE'::TEXT AS event_type,
      ROUND(LEAST(100, GREATEST(0,
        ABS(s.return_pct) * 35
        + COALESCE(s.volume_ratio,0) * 12
        + ABS(COALESCE(l.return_pct,0)) * 1.5
      )), 6) AS score,
      'UP'::TEXT AS direction,
      s.last_regime AS regime,
      s.last_confidence AS confidence,
      s.window_label,
      s.last_open_time AS observed_at,
      now() + interval '60 minutes' AS expires_at,
      'SHORT_UP_IMPULSE_AFTER_LONG_NEGATIVE_CONTEXT'::TEXT AS reason,
      jsonb_build_object(
        'short_window', s.window_label,
        'short_return_pct', s.return_pct,
        'short_range_pct', s.range_pct,
        'short_volume_ratio', s.volume_ratio,
        'long_window', l.window_label,
        'long_return_pct', l.return_pct,
        'long_range_pct', l.range_pct,
        'short_realtime_score', s.realtime_score,
        'long_realtime_score', l.realtime_score
      ) AS payload
    FROM base s
    JOIN base l
      ON l.symbol = s.symbol
     AND l.interval = s.interval
     AND l.window_label IN ('7d','30d','90d')
    WHERE s.window_label IN ('15m','1h','6h')
      AND s.return_pct >= CASE s.window_label
        WHEN '15m' THEN 0.30
        WHEN '1h' THEN 0.70
        WHEN '6h' THEN 1.20
        ELSE 999
      END
      AND l.return_pct <= -3.0

    UNION ALL

    SELECT
      symbol,
      interval,
      'BREAKOUT_UP'::TEXT AS event_type,
      ROUND(LEAST(100, GREATEST(0, range_pct * 18 + return_pct * 20 + COALESCE(volume_ratio,0) * 10)), 6) AS score,
      'UP'::TEXT AS direction,
      last_regime AS regime,
      last_confidence AS confidence,
      window_label,
      last_open_time AS observed_at,
      now() + interval '45 minutes' AS expires_at,
      'RANGE_EXPANSION_WITH_POSITIVE_RETURN'::TEXT AS reason,
      jsonb_build_object(
        'return_pct', return_pct,
        'range_pct', range_pct,
        'volume_ratio', volume_ratio,
        'realtime_score', realtime_score,
        'candles_n', candles_n
      ) AS payload
    FROM base
    WHERE window_label IN ('15m','1h')
      AND return_pct > 0
      AND range_pct >= CASE window_label
        WHEN '15m' THEN 0.55
        WHEN '1h' THEN 1.25
        ELSE 999
      END

    UNION ALL

    SELECT
      symbol,
      interval,
      'VOLUME_SPIKE'::TEXT AS event_type,
      ROUND(LEAST(100, GREATEST(0, COALESCE(volume_ratio,0) * 25)), 6) AS score,
      direction_calc AS direction,
      last_regime AS regime,
      last_confidence AS confidence,
      window_label,
      last_open_time AS observed_at,
      now() + interval '30 minutes' AS expires_at,
      'LAST_VOLUME_ABOVE_WINDOW_AVERAGE'::TEXT AS reason,
      jsonb_build_object(
        'return_pct', return_pct,
        'range_pct', range_pct,
        'volume_ratio', volume_ratio,
        'realtime_score', realtime_score,
        'candles_n', candles_n
      ) AS payload
    FROM base
    WHERE window_label IN ('15m','1h')
      AND COALESCE(volume_ratio,0) >= 2.5

    UNION ALL

    SELECT
      symbol,
      interval,
      'ATR_EXPANSION'::TEXT AS event_type,
      ROUND(LEAST(100, GREATEST(0, range_pct * 25)), 6) AS score,
      direction_calc AS direction,
      last_regime AS regime,
      last_confidence AS confidence,
      window_label,
      last_open_time AS observed_at,
      now() + interval '45 minutes' AS expires_at,
      'RANGE_EXPANSION_PROXY'::TEXT AS reason,
      jsonb_build_object(
        'return_pct', return_pct,
        'range_pct', range_pct,
        'volume_ratio', volume_ratio,
        'realtime_score', realtime_score,
        'candles_n', candles_n
      ) AS payload
    FROM base
    WHERE window_label IN ('15m','1h','6h')
      AND range_pct >= CASE window_label
        WHEN '15m' THEN 0.75
        WHEN '1h' THEN 1.75
        WHEN '6h' THEN 3.50
        ELSE 999
      END
  ),
  prepared AS (
    SELECT
      symbol,
      interval,
      event_type,
      md5(symbol || '|' || interval || '|' || event_type || '|' || window_label) AS event_key,
      score,
      CASE
        WHEN score >= 85 THEN 'EXTREME'
        WHEN score >= 70 THEN 'HIGH'
        WHEN score >= 45 THEN 'MEDIUM'
        ELSE 'LOW'
      END AS importance,
      direction,
      regime,
      confidence,
      window_label,
      observed_at,
      observed_at AS first_observed_at,
      observed_at AS last_observed_at,
      expires_at,
      1 AS refresh_count,
      reason,
      payload
    FROM events
    WHERE observed_at IS NOT NULL
      AND score >= 20
  )
  SELECT *
  FROM prepared
  ON CONFLICT (symbol, interval, event_type, window_label) DO UPDATE SET
    event_key = EXCLUDED.event_key,
    score = GREATEST(market_memory_events.score, EXCLUDED.score),
    importance = CASE
      WHEN GREATEST(market_memory_events.score, EXCLUDED.score) >= 85 THEN 'EXTREME'
      WHEN GREATEST(market_memory_events.score, EXCLUDED.score) >= 70 THEN 'HIGH'
      WHEN GREATEST(market_memory_events.score, EXCLUDED.score) >= 45 THEN 'MEDIUM'
      ELSE 'LOW'
    END,
    direction = EXCLUDED.direction,
    regime = EXCLUDED.regime,
    confidence = EXCLUDED.confidence,
    observed_at = EXCLUDED.observed_at,
    first_observed_at = COALESCE(market_memory_events.first_observed_at, market_memory_events.observed_at, EXCLUDED.observed_at),
    last_observed_at = EXCLUDED.observed_at,
    expires_at = GREATEST(market_memory_events.expires_at, EXCLUDED.expires_at),
    refresh_count = COALESCE(market_memory_events.refresh_count, 1) + 1,
    reason = EXCLUDED.reason,
    payload = EXCLUDED.payload || jsonb_build_object(
      'refresh_count', COALESCE(market_memory_events.refresh_count, 1) + 1,
      'first_observed_at', COALESCE(market_memory_events.first_observed_at, market_memory_events.observed_at, EXCLUDED.observed_at),
      'last_observed_at', EXCLUDED.observed_at
    );
END;
$$;

COMMIT;

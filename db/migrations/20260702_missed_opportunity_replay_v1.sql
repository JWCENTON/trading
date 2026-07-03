CREATE TABLE IF NOT EXISTS missed_opportunity_replay (
  entry_trace_event_id BIGINT PRIMARY KEY REFERENCES entry_trace_events(id) ON DELETE CASCADE,

  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  strategy TEXT NOT NULL,
  reason TEXT,
  event_time TIMESTAMPTZ NOT NULL,
  event_price NUMERIC(18,8),

  realtime_score NUMERIC(10,4),
  realtime_status TEXT,

  future_5m_return_pct NUMERIC(18,8),
  future_15m_return_pct NUMERIC(18,8),
  future_30m_return_pct NUMERIC(18,8),
  future_60m_return_pct NUMERIC(18,8),

  max_up_30m_pct NUMERIC(18,8),
  max_down_30m_pct NUMERIC(18,8),
  max_up_60m_pct NUMERIC(18,8),
  max_down_60m_pct NUMERIC(18,8),

  missed_move_pct NUMERIC(18,8),
  adverse_move_pct NUMERIC(18,8),
  missed_opportunity BOOLEAN NOT NULL DEFAULT false,
  missed_direction TEXT,
  replay_status TEXT NOT NULL DEFAULT 'OK',

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_missed_replay_symbol_interval_time
ON missed_opportunity_replay(symbol, interval, event_time DESC);

CREATE INDEX IF NOT EXISTS ix_missed_replay_status_time
ON missed_opportunity_replay(replay_status, event_time DESC);

CREATE INDEX IF NOT EXISTS ix_missed_replay_missed
ON missed_opportunity_replay(missed_opportunity, missed_move_pct DESC);

CREATE OR REPLACE FUNCTION refresh_missed_opportunity_replay_v1(
  p_since INTERVAL DEFAULT interval '24 hours',
  p_min_realtime NUMERIC DEFAULT 50.0,
  p_min_move_pct NUMERIC DEFAULT 0.35
)
RETURNS TABLE(processed INT, inserted_or_updated INT)
LANGUAGE plpgsql
AS $$
DECLARE
  v_processed INT := 0;
  v_upserted INT := 0;
BEGIN
  WITH candidates AS (
    SELECT
      e.id,
      e.symbol,
      e.interval,
      e.strategy,
      e.reason,
      e.created_at AS event_time,
      e.price AS event_price,
      e.realtime_score,
      e.realtime_status
    FROM entry_trace_events e
    WHERE e.created_at >= now() - p_since
      AND e.price IS NOT NULL
      AND e.realtime_score IS NOT NULL
      AND e.realtime_score >= p_min_realtime
      AND (
        e.reason ILIKE '%NO_SIGNAL%'
        OR e.reason ILIKE '%LIVE_ENTRY_NOT_ATTEMPTED%'
        OR e.reason ILIKE '%ATR_TOO_LOW%'
        OR e.reason ILIKE '%MAX_DIST_FROM_EMA%'
        OR e.reason ILIKE '%EMA_SLOPE_DOWN%'
      )
  ),
  replay AS (
    SELECT
      c.*,

      c5.close AS close_5m,
      c15.close AS close_15m,
      c30.close AS close_30m,
      c60.close AS close_60m,

      x30.max_high AS max_high_30m,
      x30.min_low AS min_low_30m,
      x60.max_high AS max_high_60m,
      x60.min_low AS min_low_60m

    FROM candidates c

    LEFT JOIN LATERAL (
      SELECT close
      FROM candles
      WHERE symbol = c.symbol
        AND interval = c.interval
        AND open_time >= c.event_time + interval '5 minutes'
      ORDER BY open_time ASC
      LIMIT 1
    ) c5 ON true

    LEFT JOIN LATERAL (
      SELECT close
      FROM candles
      WHERE symbol = c.symbol
        AND interval = c.interval
        AND open_time >= c.event_time + interval '15 minutes'
      ORDER BY open_time ASC
      LIMIT 1
    ) c15 ON true

    LEFT JOIN LATERAL (
      SELECT close
      FROM candles
      WHERE symbol = c.symbol
        AND interval = c.interval
        AND open_time >= c.event_time + interval '30 minutes'
      ORDER BY open_time ASC
      LIMIT 1
    ) c30 ON true

    LEFT JOIN LATERAL (
      SELECT close
      FROM candles
      WHERE symbol = c.symbol
        AND interval = c.interval
        AND open_time >= c.event_time + interval '60 minutes'
      ORDER BY open_time ASC
      LIMIT 1
    ) c60 ON true

    LEFT JOIN LATERAL (
      SELECT
        MAX(high) AS max_high,
        MIN(low) AS min_low
      FROM candles
      WHERE symbol = c.symbol
        AND interval = c.interval
        AND open_time >= c.event_time
        AND open_time <= c.event_time + interval '30 minutes'
    ) x30 ON true

    LEFT JOIN LATERAL (
      SELECT
        MAX(high) AS max_high,
        MIN(low) AS min_low
      FROM candles
      WHERE symbol = c.symbol
        AND interval = c.interval
        AND open_time >= c.event_time
        AND open_time <= c.event_time + interval '60 minutes'
    ) x60 ON true
  ),
  calc AS (
    SELECT
      r.id,
      r.symbol,
      r.interval,
      r.strategy,
      r.reason,
      r.event_time,
      r.event_price,
      r.realtime_score,
      r.realtime_status,

      CASE WHEN r.close_5m IS NOT NULL THEN ((r.close_5m - r.event_price) / r.event_price) * 100 END AS future_5m_return_pct,
      CASE WHEN r.close_15m IS NOT NULL THEN ((r.close_15m - r.event_price) / r.event_price) * 100 END AS future_15m_return_pct,
      CASE WHEN r.close_30m IS NOT NULL THEN ((r.close_30m - r.event_price) / r.event_price) * 100 END AS future_30m_return_pct,
      CASE WHEN r.close_60m IS NOT NULL THEN ((r.close_60m - r.event_price) / r.event_price) * 100 END AS future_60m_return_pct,

      CASE WHEN r.max_high_30m IS NOT NULL THEN ((r.max_high_30m - r.event_price) / r.event_price) * 100 END AS max_up_30m_pct,
      CASE WHEN r.min_low_30m IS NOT NULL THEN ((r.min_low_30m - r.event_price) / r.event_price) * 100 END AS max_down_30m_pct,
      CASE WHEN r.max_high_60m IS NOT NULL THEN ((r.max_high_60m - r.event_price) / r.event_price) * 100 END AS max_up_60m_pct,
      CASE WHEN r.min_low_60m IS NOT NULL THEN ((r.min_low_60m - r.event_price) / r.event_price) * 100 END AS max_down_60m_pct,

      GREATEST(
        COALESCE(((r.max_high_30m - r.event_price) / r.event_price) * 100, -999999),
        COALESCE(((r.max_high_60m - r.event_price) / r.event_price) * 100, -999999)
      ) AS missed_move_pct,

      LEAST(
        COALESCE(((r.min_low_30m - r.event_price) / r.event_price) * 100, 999999),
        COALESCE(((r.min_low_60m - r.event_price) / r.event_price) * 100, 999999)
      ) AS adverse_move_pct,

      CASE
        WHEN r.close_60m IS NULL THEN 'WAITING_FOR_60M_CANDLE'
        ELSE 'OK'
      END AS replay_status

    FROM replay r
  ),
  upserted AS (
    INSERT INTO missed_opportunity_replay (
      entry_trace_event_id,
      symbol,
      interval,
      strategy,
      reason,
      event_time,
      event_price,
      realtime_score,
      realtime_status,
      future_5m_return_pct,
      future_15m_return_pct,
      future_30m_return_pct,
      future_60m_return_pct,
      max_up_30m_pct,
      max_down_30m_pct,
      max_up_60m_pct,
      max_down_60m_pct,
      missed_move_pct,
      adverse_move_pct,
      missed_opportunity,
      missed_direction,
      replay_status,
      updated_at
    )
    SELECT
      id,
      symbol,
      interval,
      strategy,
      reason,
      event_time,
      event_price,
      realtime_score,
      realtime_status,
      future_5m_return_pct,
      future_15m_return_pct,
      future_30m_return_pct,
      future_60m_return_pct,
      max_up_30m_pct,
      max_down_30m_pct,
      max_up_60m_pct,
      max_down_60m_pct,
      NULLIF(missed_move_pct, -999999),
      NULLIF(adverse_move_pct, 999999),
      CASE
        WHEN replay_status='OK' AND missed_move_pct >= p_min_move_pct THEN true
        ELSE false
      END,
      CASE
        WHEN replay_status='OK' AND missed_move_pct >= p_min_move_pct THEN 'UP'
        ELSE NULL
      END,
      replay_status,
      now()
    FROM calc
    ON CONFLICT (entry_trace_event_id)
    DO UPDATE SET
      future_5m_return_pct = EXCLUDED.future_5m_return_pct,
      future_15m_return_pct = EXCLUDED.future_15m_return_pct,
      future_30m_return_pct = EXCLUDED.future_30m_return_pct,
      future_60m_return_pct = EXCLUDED.future_60m_return_pct,
      max_up_30m_pct = EXCLUDED.max_up_30m_pct,
      max_down_30m_pct = EXCLUDED.max_down_30m_pct,
      max_up_60m_pct = EXCLUDED.max_up_60m_pct,
      max_down_60m_pct = EXCLUDED.max_down_60m_pct,
      missed_move_pct = EXCLUDED.missed_move_pct,
      adverse_move_pct = EXCLUDED.adverse_move_pct,
      missed_opportunity = EXCLUDED.missed_opportunity,
      missed_direction = EXCLUDED.missed_direction,
      replay_status = EXCLUDED.replay_status,
      updated_at = now()
    RETURNING 1
  )
  SELECT
    (SELECT COUNT(*) FROM candidates),
    (SELECT COUNT(*) FROM upserted)
  INTO v_processed, v_upserted;

  processed := v_processed;
  inserted_or_updated := v_upserted;
  RETURN NEXT;
END;
$$;

CREATE OR REPLACE VIEW v_missed_opportunity_recent AS
SELECT *
FROM missed_opportunity_replay
ORDER BY event_time DESC;

CREATE OR REPLACE VIEW v_missed_opportunity_top AS
SELECT
  symbol,
  interval,
  strategy,
  reason,
  event_time,
  event_price,
  realtime_score,
  realtime_status,
  future_5m_return_pct,
  future_15m_return_pct,
  future_30m_return_pct,
  future_60m_return_pct,
  max_up_30m_pct,
  max_down_30m_pct,
  max_up_60m_pct,
  max_down_60m_pct,
  missed_move_pct,
  adverse_move_pct,
  missed_opportunity,
  replay_status
FROM missed_opportunity_replay
WHERE replay_status = 'OK'
ORDER BY missed_opportunity DESC, missed_move_pct DESC NULLS LAST;

CREATE OR REPLACE VIEW v_missed_opportunity_summary_24h AS
SELECT
  symbol,
  interval,
  strategy,
  reason,
  COUNT(*) AS n,
  COUNT(*) FILTER (WHERE missed_opportunity) AS missed_n,
  ROUND(AVG(realtime_score),4) AS avg_realtime_score,
  ROUND(MAX(realtime_score),4) AS max_realtime_score,
  ROUND(AVG(missed_move_pct),4) AS avg_missed_move_pct,
  ROUND(MAX(missed_move_pct),4) AS max_missed_move_pct,
  ROUND(AVG(adverse_move_pct),4) AS avg_adverse_move_pct,
  MAX(event_time) AS last_event_time
FROM missed_opportunity_replay
WHERE event_time >= now() - interval '24 hours'
GROUP BY symbol, interval, strategy, reason
ORDER BY missed_n DESC, max_missed_move_pct DESC NULLS LAST;

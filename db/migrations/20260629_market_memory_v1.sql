BEGIN;

CREATE TABLE IF NOT EXISTS market_memory_snapshot (
  window_label TEXT NOT NULL,
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,

  candles_n INTEGER NOT NULL DEFAULT 0,
  first_open_time TIMESTAMPTZ,
  last_open_time TIMESTAMPTZ,

  first_close NUMERIC(18,8),
  last_close NUMERIC(18,8),
  high_price NUMERIC(18,8),
  low_price NUMERIC(18,8),

  return_pct NUMERIC(18,8),
  range_pct NUMERIC(18,8),
  avg_body_pct NUMERIC(18,8),
  avg_wick_pct NUMERIC(18,8),
  avg_volume NUMERIC(30,8),
  volume_ratio NUMERIC(18,8),

  last_regime TEXT,
  last_confidence NUMERIC(10,6),

  momentum_score NUMERIC(10,6),
  volatility_score NUMERIC(10,6),
  volume_score NUMERIC(10,6),
  realtime_score NUMERIC(10,6),

  status TEXT NOT NULL DEFAULT 'OBSERVE',
  status_reason TEXT,

  refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (window_label, symbol, interval)
);

CREATE INDEX IF NOT EXISTS ix_market_memory_snapshot_status
ON market_memory_snapshot(status, realtime_score DESC);

CREATE INDEX IF NOT EXISTS ix_market_memory_snapshot_symbol_interval
ON market_memory_snapshot(symbol, interval);

CREATE OR REPLACE FUNCTION refresh_market_memory_snapshot(
  p_window_label TEXT,
  p_minutes INTEGER
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM market_memory_snapshot
  WHERE window_label = p_window_label;

  INSERT INTO market_memory_snapshot (
    window_label,
    symbol,
    interval,
    candles_n,
    first_open_time,
    last_open_time,
    first_close,
    last_close,
    high_price,
    low_price,
    return_pct,
    range_pct,
    avg_body_pct,
    avg_wick_pct,
    avg_volume,
    volume_ratio,
    last_regime,
    last_confidence,
    momentum_score,
    volatility_score,
    volume_score,
    realtime_score,
    status,
    status_reason,
    refreshed_at
  )
  WITH c AS (
    SELECT
      symbol,
      interval,
      open_time,
      open::NUMERIC AS open_price,
      high::NUMERIC AS high_price,
      low::NUMERIC AS low_price,
      close::NUMERIC AS close_price,
      COALESCE(volume::NUMERIC, 0) AS volume
    FROM candles
    WHERE open_time >= now() - make_interval(mins => p_minutes)
      AND close IS NOT NULL
      AND open IS NOT NULL
      AND high IS NOT NULL
      AND low IS NOT NULL
  ),
  ranked AS (
    SELECT
      c.*,
      ROW_NUMBER() OVER (PARTITION BY symbol, interval ORDER BY open_time ASC) AS rn_asc,
      ROW_NUMBER() OVER (PARTITION BY symbol, interval ORDER BY open_time DESC) AS rn_desc
    FROM c
  ),
  agg AS (
    SELECT
      symbol,
      interval,
      COUNT(*) AS candles_n,
      MIN(open_time) AS first_open_time,
      MAX(open_time) AS last_open_time,
      MAX(high_price) AS high_price,
      MIN(low_price) AS low_price,
      AVG(volume) AS avg_volume,
      AVG(
        CASE WHEN open_price > 0
          THEN ABS(close_price - open_price) / open_price * 100
          ELSE NULL
        END
      ) AS avg_body_pct,
      AVG(
        CASE WHEN open_price > 0
          THEN ((high_price - low_price) - ABS(close_price - open_price)) / open_price * 100
          ELSE NULL
        END
      ) AS avg_wick_pct
    FROM ranked
    GROUP BY symbol, interval
  ),
  first_last AS (
    SELECT
      a.*,
      f.close_price AS first_close,
      l.close_price AS last_close,
      l.volume AS last_volume
    FROM agg a
    JOIN ranked f
      ON f.symbol = a.symbol
     AND f.interval = a.interval
     AND f.rn_asc = 1
    JOIN ranked l
      ON l.symbol = a.symbol
     AND l.interval = a.interval
     AND l.rn_desc = 1
  ),
  regime_last AS (
    SELECT DISTINCT ON (symbol, interval)
      symbol,
      interval,
      regime AS last_regime,
      confidence AS last_confidence
    FROM market_regime
    ORDER BY symbol, interval, ts DESC
  ),
  scored AS (
    SELECT
      fl.*,
      rl.last_regime,
      rl.last_confidence,

      CASE WHEN fl.first_close > 0
        THEN ((fl.last_close - fl.first_close) / fl.first_close) * 100
        ELSE NULL
      END AS return_pct,

      CASE WHEN fl.first_close > 0
        THEN ((fl.high_price - fl.low_price) / fl.first_close) * 100
        ELSE NULL
      END AS range_pct,

      CASE WHEN fl.avg_volume > 0
        THEN fl.last_volume / fl.avg_volume
        ELSE NULL
      END AS volume_ratio
    FROM first_last fl
    LEFT JOIN regime_last rl
      ON rl.symbol = fl.symbol
     AND rl.interval = fl.interval
  ),
  final AS (
    SELECT
      s.*,

      LEAST(100, GREATEST(0, COALESCE(ABS(s.return_pct), 0) * 20)) AS momentum_score,
      LEAST(100, GREATEST(0, COALESCE(s.range_pct, 0) * 10)) AS volatility_score,
      LEAST(100, GREATEST(0, COALESCE(s.volume_ratio, 0) * 25)) AS volume_score
    FROM scored s
  )
  SELECT
    p_window_label,
    symbol,
    interval,
    candles_n,
    first_open_time,
    last_open_time,
    first_close,
    last_close,
    high_price,
    low_price,
    ROUND(return_pct, 8),
    ROUND(range_pct, 8),
    ROUND(avg_body_pct, 8),
    ROUND(avg_wick_pct, 8),
    ROUND(avg_volume, 8),
    ROUND(volume_ratio, 8),
    last_regime,
    last_confidence,
    ROUND(momentum_score, 6),
    ROUND(volatility_score, 6),
    ROUND(volume_score, 6),
    ROUND(((momentum_score * 0.40) + (volatility_score * 0.30) + (volume_score * 0.30)), 6) AS realtime_score,
    CASE
      WHEN candles_n < 3 THEN 'NO_DATA'
      WHEN ((momentum_score * 0.40) + (volatility_score * 0.30) + (volume_score * 0.30)) >= 70 THEN 'HOT'
      WHEN ((momentum_score * 0.40) + (volatility_score * 0.30) + (volume_score * 0.30)) >= 45 THEN 'ACTIVE'
      ELSE 'OBSERVE'
    END AS status,
    CASE
      WHEN candles_n < 3 THEN 'INSUFFICIENT_CANDLES'
      WHEN ((momentum_score * 0.40) + (volatility_score * 0.30) + (volume_score * 0.30)) >= 70 THEN 'STRONG_REALTIME_MOVE'
      WHEN ((momentum_score * 0.40) + (volatility_score * 0.30) + (volume_score * 0.30)) >= 45 THEN 'MODERATE_REALTIME_MOVE'
      ELSE 'LOW_REALTIME_MOVE'
    END AS status_reason,
    now()
  FROM final;
END;
$$;

COMMIT;

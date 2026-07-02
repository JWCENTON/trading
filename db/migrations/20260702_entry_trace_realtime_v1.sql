CREATE TABLE IF NOT EXISTS entry_trace_events (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  strategy TEXT NOT NULL,

  candle_open_time TIMESTAMPTZ,
  event_type TEXT,
  decision TEXT,
  reason TEXT,
  price NUMERIC(18,8),

  realtime_score NUMERIC(10,4),
  realtime_status TEXT,
  primary_driver TEXT,

  atr_pct NUMERIC(18,8),
  ema_slope_pct NUMERIC(18,8),
  volume_ratio NUMERIC(18,8),
  momentum_3_pct NUMERIC(18,8),
  momentum_5_pct NUMERIC(18,8),
  range_pct NUMERIC(18,8),
  breakout_up BOOLEAN,
  breakout_down BOOLEAN,

  input_info JSONB NOT NULL DEFAULT '{}'::jsonb,
  realtime_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_entry_trace_events_sym_int_strat_created
ON entry_trace_events(symbol, interval, strategy, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_entry_trace_events_reason_created
ON entry_trace_events(reason, created_at DESC);

CREATE OR REPLACE VIEW v_entry_trace_recent AS
SELECT *
FROM entry_trace_events
ORDER BY created_at DESC;

CREATE OR REPLACE VIEW v_entry_trace_summary_24h AS
SELECT
  symbol,
  interval,
  strategy,
  reason,
  COUNT(*) AS n,
  ROUND(AVG(realtime_score), 4) AS avg_realtime_score,
  ROUND(MAX(realtime_score), 4) AS max_realtime_score,
  ROUND(AVG(atr_pct), 4) AS avg_atr_pct,
  ROUND(AVG(volume_ratio), 4) AS avg_volume_ratio,
  ROUND(AVG(momentum_3_pct), 4) AS avg_momentum_3_pct,
  MAX(created_at) AS last_seen
FROM entry_trace_events
WHERE created_at >= now() - interval '24 hours'
GROUP BY symbol, interval, strategy, reason
ORDER BY n DESC;

CREATE OR REPLACE VIEW v_realtime_score_latest AS
SELECT DISTINCT ON (symbol, interval, strategy)
  symbol,
  interval,
  strategy,
  created_at,
  reason,
  realtime_score,
  realtime_status,
  primary_driver,
  atr_pct,
  ema_slope_pct,
  volume_ratio,
  momentum_3_pct,
  momentum_5_pct,
  breakout_up,
  breakout_down,
  realtime_json
FROM entry_trace_events
ORDER BY symbol, interval, strategy, created_at DESC;

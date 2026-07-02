ALTER TABLE entry_trace_events
  ADD COLUMN IF NOT EXISTS atr_component NUMERIC(10,4),
  ADD COLUMN IF NOT EXISTS volume_component NUMERIC(10,4),
  ADD COLUMN IF NOT EXISTS ema_component NUMERIC(10,4),
  ADD COLUMN IF NOT EXISTS momentum_component NUMERIC(10,4),
  ADD COLUMN IF NOT EXISTS breakout_component NUMERIC(10,4),
  ADD COLUMN IF NOT EXISTS realtime_weights_json JSONB NOT NULL DEFAULT '{}'::jsonb;

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
  atr_component,
  volume_component,
  ema_component,
  momentum_component,
  breakout_component,
  realtime_weights_json,
  realtime_json
FROM entry_trace_events
ORDER BY symbol, interval, strategy, created_at DESC;

CREATE OR REPLACE VIEW v_realtime_component_summary_24h AS
SELECT
  symbol,
  interval,
  strategy,
  reason,
  COUNT(*) AS n,
  ROUND(AVG(realtime_score),4) AS avg_realtime_score,
  ROUND(MAX(realtime_score),4) AS max_realtime_score,
  ROUND(AVG(atr_component),4) AS avg_atr_component,
  ROUND(AVG(volume_component),4) AS avg_volume_component,
  ROUND(AVG(ema_component),4) AS avg_ema_component,
  ROUND(AVG(momentum_component),4) AS avg_momentum_component,
  ROUND(AVG(breakout_component),4) AS avg_breakout_component,
  MAX(created_at) AS last_seen
FROM entry_trace_events
WHERE created_at >= now() - interval '24 hours'
GROUP BY symbol, interval, strategy, reason
ORDER BY n DESC, avg_realtime_score DESC;

CREATE OR REPLACE VIEW v_realtime_calibration_24h AS
SELECT
  interval,
  COUNT(*) AS n,
  ROUND(AVG(realtime_score),4) AS avg_score,
  ROUND(MAX(realtime_score),4) AS max_score,
  COUNT(*) FILTER (WHERE realtime_status='REALTIME_READY') AS ready_n,
  COUNT(*) FILTER (WHERE realtime_status='REALTIME_WATCH') AS watch_n,
  COUNT(*) FILTER (WHERE realtime_status='REALTIME_OBSERVE') AS observe_n,
  COUNT(*) FILTER (WHERE realtime_status='REALTIME_WEAK') AS weak_n,
  ROUND(AVG(atr_component),4) AS avg_atr_component,
  ROUND(AVG(volume_component),4) AS avg_volume_component,
  ROUND(AVG(ema_component),4) AS avg_ema_component,
  ROUND(AVG(momentum_component),4) AS avg_momentum_component,
  ROUND(AVG(breakout_component),4) AS avg_breakout_component
FROM entry_trace_events
WHERE created_at >= now() - interval '24 hours'
GROUP BY interval
ORDER BY interval;

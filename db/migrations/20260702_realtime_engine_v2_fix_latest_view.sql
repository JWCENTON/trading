DROP VIEW IF EXISTS v_realtime_score_latest;

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

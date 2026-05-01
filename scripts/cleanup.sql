SET maintenance_work_mem = '16MB';
SET max_parallel_maintenance_workers = 0;

DELETE FROM candles
WHERE open_time < now() - interval '30 days';

DELETE FROM strategy_events
WHERE created_at < now() - interval '30 days';

DELETE FROM watchdog_events
WHERE created_at < now() - interval '30 days';

DELETE FROM regime_gate_events
WHERE created_at < now() - interval '30 days';

DELETE FROM market_regime
WHERE created_at < now() - interval '30 days';

DELETE FROM bot_heartbeat_samples
WHERE seen_at < now() - interval '30 days';

VACUUM ANALYZE candles;
VACUUM ANALYZE strategy_events;
VACUUM ANALYZE watchdog_events;
VACUUM ANALYZE regime_gate_events;
VACUUM ANALYZE market_regime;
VACUUM ANALYZE bot_heartbeat_samples;

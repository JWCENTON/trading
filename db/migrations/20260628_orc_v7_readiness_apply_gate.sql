CREATE OR REPLACE VIEW v_strategy_readiness_15m AS
WITH recent AS (
  SELECT symbol, interval, strategy, event_type, reason, decision, created_at, info
  FROM strategy_events
  WHERE created_at >= now() - interval '15 minutes'
),
agg AS (
  SELECT
    symbol,
    interval,
    strategy,
    COUNT(*) AS events_15m,
    COUNT(*) FILTER (WHERE event_type='RUN_START') AS runs_15m,
    COUNT(*) FILTER (WHERE decision ILIKE '%BUY%') AS buy_decisions_15m,
    COUNT(*) FILTER (WHERE event_type='SIGNAL') AS signals_15m,
    COUNT(*) FILTER (WHERE event_type='SKIP') AS skips_15m,
    COUNT(*) FILTER (
      WHERE reason IN (
        'TREND_DOWN_LONG_ONLY',
        'TREND_NOT_ACTIVE_FLAT',
        'REGIME_BLOCK',
        'POLICY_BLOCK',
        'POLICY_WOULD_BLOCK'
      )
    ) AS hard_blocks_15m,
    MAX(created_at) AS last_event_at,
    MAX(created_at) FILTER (WHERE event_type='RUN_START') AS last_run_at,
    MAX(created_at) FILTER (WHERE decision ILIKE '%BUY%' OR event_type='SIGNAL') AS last_signal_at
  FROM recent
  GROUP BY symbol, interval, strategy
)
SELECT
  symbol,
  interval,
  strategy,
  events_15m,
  runs_15m,
  buy_decisions_15m,
  signals_15m,
  skips_15m,
  hard_blocks_15m,
  last_event_at,
  last_run_at,
  last_signal_at,
  CASE
    WHEN runs_15m = 0 THEN false
    WHEN hard_blocks_15m >= runs_15m THEN false
    WHEN buy_decisions_15m > 0 OR signals_15m > 0 THEN true
    ELSE false
  END AS ready_now,
  CASE
    WHEN runs_15m = 0 THEN 'NO_RUNTIME'
    WHEN hard_blocks_15m >= runs_15m THEN 'HARD_BLOCKED'
    WHEN buy_decisions_15m > 0 THEN 'BUY_READY'
    WHEN signals_15m > 0 THEN 'SIGNAL_READY'
    ELSE 'NO_SIGNAL_YET'
  END AS readiness_reason
FROM agg;

CREATE OR REPLACE VIEW v_strategy_readiness_current AS
WITH r AS (
  SELECT DISTINCT ON (symbol, interval)
    symbol, interval, regime, confidence, ts
  FROM market_regime
  ORDER BY symbol, interval, ts DESC
)
SELECT
  bc.symbol,
  bc.interval,
  bc.strategy,
  bc.enabled,
  bc.live_orders_enabled,
  bc.regime_mode,
  bc.reason AS bot_control_reason,
  r.regime,
  r.confidence,
  sr.ready_now,
  sr.readiness_reason,
  sr.runs_15m,
  sr.buy_decisions_15m,
  sr.signals_15m,
  sr.hard_blocks_15m,
  sr.last_event_at,
  CASE
    WHEN bc.enabled IS NOT TRUE THEN false
    WHEN r.regime IS NULL THEN false
    WHEN sr.ready_now IS TRUE THEN true
    ELSE false
  END AS orc_v7_ready
FROM bot_control bc
LEFT JOIN r
  ON r.symbol = bc.symbol
 AND r.interval = bc.interval
LEFT JOIN v_strategy_readiness_15m sr
  ON sr.symbol = bc.symbol
 AND sr.interval = bc.interval
 AND sr.strategy = bc.strategy;

CREATE OR REPLACE VIEW v_orc_v7_shadow_picks AS
SELECT
  e.symbol,
  e.interval,
  e.strategy,
  e.n_trades_3d,
  e.net_sum_3d,
  e.profit_factor_3d,
  e.current_hysteresis_regime,
  e.current_hysteresis_confidence,
  e.regime_net_14d,
  e.regime_pf_14d,
  e.v63_score,
  e.eligible_v63,
  e.v63_reason,
  e.picked_now AS picked_v63_now,
  r.orc_v7_ready,
  r.readiness_reason,
  r.runs_15m,
  r.buy_decisions_15m,
  r.signals_15m,
  r.hard_blocks_15m,
  CASE
    WHEN e.eligible_v63 IS TRUE
     AND r.orc_v7_ready IS TRUE
    THEN true
    ELSE false
  END AS eligible_v7_shadow,
  CASE
    WHEN e.eligible_v63 IS NOT TRUE THEN e.v63_reason
    WHEN r.orc_v7_ready IS NOT TRUE THEN COALESCE(r.readiness_reason, 'NO_READINESS')
    ELSE 'V7_READY'
  END AS v7_reason,
  ROW_NUMBER() OVER (
    ORDER BY
      CASE WHEN e.eligible_v63 IS TRUE AND r.orc_v7_ready IS TRUE THEN 0 ELSE 1 END,
      e.v63_score DESC NULLS LAST,
      e.net_sum_3d DESC NULLS LAST
  ) AS v7_rn
FROM v_orc_v63_explain e
LEFT JOIN v_strategy_readiness_current r
  ON r.symbol = e.symbol
 AND r.interval = e.interval
 AND r.strategy = e.strategy;

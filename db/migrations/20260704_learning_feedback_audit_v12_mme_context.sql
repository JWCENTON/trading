BEGIN;

DROP VIEW IF EXISTS v_learning_feedback_missing_components_v1;
DROP VIEW IF EXISTS v_learning_feedback_summary_v1;
DROP VIEW IF EXISTS v_learning_feedback_audit_v1;

CREATE OR REPLACE VIEW v_learning_feedback_audit_v1 AS
WITH closed_positions AS (
  SELECT *
  FROM positions
  WHERE status = 'CLOSED'
    AND entry_time IS NOT NULL
    AND exit_time IS NOT NULL
    AND exit_time >= now() - interval '30 days'
),
base AS (
  SELECT
    p.id AS position_id,
    p.symbol,
    p."interval",
    p.strategy,
    p.side,
    p.entry_time,
    p.exit_time,
    p.entry_price,
    p.exit_price,
    p.exit_reason,
    p.market_regime,
    p.gross_pnl_usdc,
    p.fees_usdc,
    p.net_pnl_usdc,

    et.id AS entry_trace_id,
    et.realtime_score AS entry_realtime_score,
    et.realtime_status AS entry_realtime_status,
    et.reason AS entry_trace_reason,

    rt.realtime_score AS latest_realtime_score,
    rt.realtime_status AS latest_realtime_status,

    replay.id AS replay_position_id,
    replay.mfe_pct AS replay_mfe_pct,
    replay.mae_pct AS replay_mae_pct,
    replay.giveback_pct AS replay_giveback_pct,
    replay.exit_replay_label,

    xv3.id AS exit_trace_id,
    xv3.exit_family,
    xv3.exit_decision_class_v2,
    xv3.exit_thesis_state,

    el.id AS exit_learning_id,
    el.sample_confidence,
    el.learning_priority,
    el.learning_score,
    el.recoverable_net_usdc,

    sb.id AS slot_brain_id,
    sb.edge_score,
    sb.edge_status,
    sb.sample_quality,
    sb.stability_score,

    orc.symbol AS orc_symbol,
    orc.orc_final_score_v2,
    orc.eligible_v63,
    orc.orc_v7_ready,
    orc.context_v2_ready_now,
    orc.v63_reason,
    orc.readiness_reason,

    mme.symbol AS mme_symbol,
    mme.mme_orc_status,
    mme.orc_readiness_score AS mme_orc_readiness_score,
    mme.sequence_stage AS mme_sequence_stage,
    mme.remaining_score AS mme_remaining_score,

    se.id AS strategy_event_id,
    se.decision AS strategy_decision,
    se.reason AS strategy_reason

  FROM closed_positions p

  LEFT JOIN LATERAL (
    SELECT e.*
    FROM entry_trace_events e
    WHERE e.symbol = p.symbol
      AND e."interval" = p."interval"
      AND e.strategy = p.strategy
      AND e.created_at BETWEEN p.entry_time - interval '30 minutes'
                           AND p.entry_time + interval '10 minutes'
    ORDER BY abs(extract(epoch FROM (e.created_at - p.entry_time))) ASC
    LIMIT 1
  ) et ON true

  LEFT JOIN v_realtime_score_latest rt
    ON rt.symbol = p.symbol
   AND rt."interval" = p."interval"
   AND rt.strategy = p.strategy

  LEFT JOIN v_trade_entry_exit_replay_v1 replay
    ON replay.id = p.id

  LEFT JOIN exit_trace_v3 xv3
    ON xv3.position_id = p.id

  LEFT JOIN exit_learning_v1 el
    ON el.symbol = p.symbol
   AND el."interval" = p."interval"
   AND el.strategy = p.strategy
   AND COALESCE(el.exit_family, '') = COALESCE(xv3.exit_family, '')
   AND COALESCE(el.exit_decision_class_v2, '') = COALESCE(xv3.exit_decision_class_v2, '')

  LEFT JOIN LATERAL (
    SELECT s.*
    FROM slot_brain_snapshot s
    WHERE s.symbol = p.symbol
      AND s."interval" = p."interval"
      AND s.strategy = p.strategy
      AND s.window_label IN ('30d', '7d', '90d')
    ORDER BY
      CASE s.window_label
        WHEN '30d' THEN 1
        WHEN '7d' THEN 2
        WHEN '90d' THEN 3
        ELSE 9
      END,
      s.calculated_at DESC
    LIMIT 1
  ) sb ON true

  LEFT JOIN v_learning_orc_context_compat_v1 orc
    ON orc.symbol = p.symbol
   AND orc."interval" = p."interval"
   AND orc.strategy = p.strategy

  LEFT JOIN LATERAL (
    SELECT m.*
    FROM v_market_memory_orc_context_v17 m
    WHERE m.symbol = p.symbol
      AND m."interval" = p."interval"
    ORDER BY
      COALESCE(m.orc_readiness_score, 0) DESC,
      COALESCE(m.remaining_score, 0) DESC
    LIMIT 1
  ) mme ON true

  LEFT JOIN LATERAL (
    SELECT s.*
    FROM strategy_events s
    WHERE s.symbol = p.symbol
      AND s."interval" = p."interval"
      AND s.strategy = p.strategy
      AND s.created_at BETWEEN p.entry_time - interval '30 minutes'
                           AND p.entry_time + interval '10 minutes'
    ORDER BY abs(extract(epoch FROM (s.created_at - p.entry_time))) ASC
    LIMIT 1
  ) se ON true
),
scored AS (
  SELECT
    *,
    ARRAY_REMOVE(ARRAY[
      CASE WHEN entry_trace_id IS NULL THEN 'ENTRY_TRACE' END,
      CASE WHEN entry_realtime_score IS NULL AND latest_realtime_score IS NULL THEN 'REALTIME' END,
      CASE WHEN orc_symbol IS NULL THEN 'ORC' END,
      CASE WHEN slot_brain_id IS NULL THEN 'SLOT_BRAIN' END,
      CASE WHEN mme_symbol IS NULL THEN 'MME' END,
      CASE WHEN replay_position_id IS NULL THEN 'REPLAY' END,
      CASE WHEN exit_trace_id IS NULL THEN 'EXIT_TRACE' END,
      CASE WHEN exit_learning_id IS NULL THEN 'EXIT_LEARNING' END,
      CASE WHEN strategy_event_id IS NULL THEN 'STRATEGY_RUNTIME' END
    ], NULL) AS missing_fields,

    (
      (CASE WHEN entry_trace_id IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN entry_realtime_score IS NOT NULL OR latest_realtime_score IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN orc_symbol IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN slot_brain_id IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN mme_symbol IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN replay_position_id IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN exit_trace_id IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN exit_learning_id IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN strategy_event_id IS NOT NULL THEN 1 ELSE 0 END)
    ) AS coverage_points
  FROM base
)
SELECT
  *,
  (entry_trace_id IS NOT NULL) AS has_entry_trace,
  (entry_realtime_score IS NOT NULL OR latest_realtime_score IS NOT NULL) AS has_realtime,
  (orc_symbol IS NOT NULL) AS has_orc,
  (slot_brain_id IS NOT NULL) AS has_slot_brain,
  (mme_symbol IS NOT NULL) AS has_mme,
  (replay_position_id IS NOT NULL) AS has_replay,
  (exit_trace_id IS NOT NULL) AS has_exit_trace,
  (exit_learning_id IS NOT NULL) AS has_exit_learning,
  (strategy_event_id IS NOT NULL) AS has_strategy_runtime,
  ROUND((coverage_points::numeric / 9.0) * 100.0, 2) AS coverage_pct,
  CASE
    WHEN (coverage_points::numeric / 9.0) >= 0.95 THEN 'READY_FOR_LEARNING'
    WHEN (coverage_points::numeric / 9.0) >= 0.70 THEN 'PARTIAL'
    ELSE 'INSUFFICIENT_DATA'
  END AS learning_status
FROM scored;

CREATE OR REPLACE VIEW v_learning_feedback_summary_v1 AS
SELECT
  learning_status,
  COUNT(*) AS trades,
  ROUND(AVG(coverage_pct), 2) AS avg_coverage_pct,
  MIN(coverage_pct) AS min_coverage_pct,
  MAX(coverage_pct) AS max_coverage_pct
FROM v_learning_feedback_audit_v1
GROUP BY learning_status
ORDER BY learning_status;

CREATE OR REPLACE VIEW v_learning_feedback_missing_components_v1 AS
SELECT
  missing_component,
  COUNT(*) AS trades
FROM v_learning_feedback_audit_v1 a
CROSS JOIN LATERAL unnest(a.missing_fields) AS missing_component
GROUP BY missing_component
ORDER BY trades DESC, missing_component;

COMMIT;

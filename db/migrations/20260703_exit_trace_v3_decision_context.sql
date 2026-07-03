BEGIN;

CREATE TABLE IF NOT EXISTS exit_trace_v3 (
  id BIGSERIAL PRIMARY KEY,
  position_id BIGINT NOT NULL UNIQUE,

  symbol TEXT NOT NULL,
  "interval" TEXT NOT NULL,
  strategy TEXT NOT NULL,
  side TEXT,

  entry_time TIMESTAMPTZ,
  exit_time TIMESTAMPTZ,

  exit_reason TEXT,
  exit_family TEXT,
  exit_decision_class_v2 TEXT,

  market_regime TEXT,

  net_pnl_usdc NUMERIC,
  mfe_pct NUMERIC,
  mae_pct NUMERIC,
  exit_pct NUMERIC,
  giveback_pct NUMERIC,
  mfe_capture_ratio NUMERIC,
  giveback_ratio NUMERIC,

  sample_confidence TEXT,
  learning_priority TEXT,
  learning_score NUMERIC,
  recoverable_net_usdc NUMERIC,

  exit_thesis_state TEXT,
  exit_context_quality TEXT NOT NULL DEFAULT 'DERIVED_FROM_TRACE_V2',

  decision_context JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_exit_trace_v3_exit_time
ON exit_trace_v3(exit_time DESC);

CREATE INDEX IF NOT EXISTS ix_exit_trace_v3_slot
ON exit_trace_v3(symbol, "interval", strategy, exit_time DESC);

CREATE INDEX IF NOT EXISTS ix_exit_trace_v3_thesis
ON exit_trace_v3(exit_thesis_state, exit_time DESC);

CREATE OR REPLACE FUNCTION refresh_exit_trace_v3(
  p_since TIMESTAMPTZ DEFAULT now() - interval '3 days'
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_count INTEGER := 0;
BEGIN
  INSERT INTO exit_trace_v3 (
    position_id,
    symbol,
    "interval",
    strategy,
    side,
    entry_time,
    exit_time,
    exit_reason,
    exit_family,
    exit_decision_class_v2,
    market_regime,
    net_pnl_usdc,
    mfe_pct,
    mae_pct,
    exit_pct,
    giveback_pct,
    mfe_capture_ratio,
    giveback_ratio,
    sample_confidence,
    learning_priority,
    learning_score,
    recoverable_net_usdc,
    exit_thesis_state,
    exit_context_quality,
    decision_context,
    updated_at
  )
  SELECT
    e.position_id,
    e.symbol,
    e."interval",
    e.strategy,
    e.side,
    e.entry_time,
    e.exit_time,
    e.exit_reason,
    e.exit_family,
    e.exit_decision_class_v2,
    e.market_regime,
    e.net_pnl_usdc,
    e.mfe_pct,
    e.mae_pct,
    e.exit_pct,
    e.giveback_pct,
    e.mfe_capture_ratio,
    e.giveback_ratio,
    l.sample_confidence,
    l.learning_priority,
    l.learning_score,
    l.recoverable_net_usdc,

    CASE
      WHEN e.exit_decision_class_v2 = 'LOSS_AFTER_AVAILABLE_PROFIT_REVIEW'
        THEN 'THESIS_PROFIT_AVAILABLE_BUT_NOT_PROTECTED'
      WHEN e.exit_decision_class_v2 = 'PROFIT_LOCK_TO_LOSS_AFTER_FEES_OR_GIVEBACK'
        THEN 'THESIS_PROFIT_LOCK_TOO_WEAK_AFTER_FEES'
      WHEN e.exit_decision_class_v2 = 'PROFIT_LOCK_HIGH_GIVEBACK_REVIEW'
        THEN 'THESIS_PROFIT_LOCK_HIGH_GIVEBACK'
      WHEN e.exit_decision_class_v2 = 'WIN_WITH_LOW_MFE_CAPTURE_REVIEW'
        THEN 'THESIS_WIN_BUT_LOW_CAPTURE'
      WHEN e.exit_decision_class_v2 = 'TIME_EXIT_LEGACY_REVIEW'
        THEN 'THESIS_TIME_EXIT_LEGACY'
      WHEN e.net_pnl_usdc > 0
        THEN 'THESIS_EXIT_ACCEPTABLE_PROFIT'
      WHEN e.net_pnl_usdc < 0
        THEN 'THESIS_EXIT_LOSS_REVIEW'
      ELSE 'THESIS_EXIT_REVIEW'
    END AS exit_thesis_state,

    'DERIVED_FROM_TRACE_V2_AND_LEARNING',

    jsonb_build_object(
      'shadow_only', true,
      'source', 'refresh_exit_trace_v3',
      'confirmed_sources', jsonb_build_array('exit_trace_v2', 'exit_learning_v1'),
      'missing_future_sources', jsonb_build_array(
        'realtime_exit_snapshot',
        'mme_exit_context',
        'orc_exit_context',
        'slot_brain_exit_snapshot'
      ),
      'exit_reason_raw', e.exit_reason,
      'exit_family', e.exit_family,
      'exit_decision_class_v2', e.exit_decision_class_v2,
      'mfe_pct', e.mfe_pct,
      'mae_pct', e.mae_pct,
      'exit_pct', e.exit_pct,
      'giveback_pct', e.giveback_pct,
      'mfe_capture_ratio', e.mfe_capture_ratio,
      'giveback_ratio', e.giveback_ratio,
      'learning_priority', l.learning_priority,
      'sample_confidence', l.sample_confidence,
      'recoverable_net_usdc', l.recoverable_net_usdc
    ),

    now()
  FROM exit_trace_v2 e
  LEFT JOIN exit_learning_v1 l
    ON l.strategy = e.strategy
   AND l.symbol = e.symbol
   AND l."interval" = e."interval"
   AND COALESCE(l.exit_family, '') = COALESCE(e.exit_family, '')
   AND COALESCE(l.exit_decision_class_v2, '') = COALESCE(e.exit_decision_class_v2, '')
  WHERE e.exit_time >= p_since
  ON CONFLICT (position_id) DO UPDATE SET
    exit_time = EXCLUDED.exit_time,
    exit_reason = EXCLUDED.exit_reason,
    exit_family = EXCLUDED.exit_family,
    exit_decision_class_v2 = EXCLUDED.exit_decision_class_v2,
    market_regime = EXCLUDED.market_regime,
    net_pnl_usdc = EXCLUDED.net_pnl_usdc,
    mfe_pct = EXCLUDED.mfe_pct,
    mae_pct = EXCLUDED.mae_pct,
    exit_pct = EXCLUDED.exit_pct,
    giveback_pct = EXCLUDED.giveback_pct,
    mfe_capture_ratio = EXCLUDED.mfe_capture_ratio,
    giveback_ratio = EXCLUDED.giveback_ratio,
    sample_confidence = EXCLUDED.sample_confidence,
    learning_priority = EXCLUDED.learning_priority,
    learning_score = EXCLUDED.learning_score,
    recoverable_net_usdc = EXCLUDED.recoverable_net_usdc,
    exit_thesis_state = EXCLUDED.exit_thesis_state,
    exit_context_quality = EXCLUDED.exit_context_quality,
    decision_context = EXCLUDED.decision_context,
    updated_at = now();

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;

CREATE OR REPLACE VIEW v_exit_trace_v3_recent AS
SELECT
  position_id,
  exit_time,
  symbol,
  "interval",
  strategy,
  side,
  exit_family,
  exit_decision_class_v2,
  exit_thesis_state,
  sample_confidence,
  learning_priority,
  ROUND(learning_score, 6) AS learning_score,
  ROUND(recoverable_net_usdc, 6) AS recoverable_net_usdc,
  ROUND(net_pnl_usdc, 6) AS net_pnl_usdc,
  ROUND(mfe_pct, 4) AS mfe_pct,
  ROUND(exit_pct, 4) AS exit_pct,
  ROUND(giveback_pct, 4) AS giveback_pct,
  ROUND(mfe_capture_ratio, 4) AS mfe_capture_ratio,
  ROUND(giveback_ratio, 4) AS giveback_ratio,
  market_regime,
  exit_context_quality
FROM exit_trace_v3
ORDER BY exit_time DESC;

CREATE OR REPLACE VIEW v_exit_trace_v3_priority AS
SELECT *
FROM v_exit_trace_v3_recent
WHERE exit_thesis_state IN (
  'THESIS_PROFIT_AVAILABLE_BUT_NOT_PROTECTED',
  'THESIS_PROFIT_LOCK_TOO_WEAK_AFTER_FEES',
  'THESIS_PROFIT_LOCK_HIGH_GIVEBACK',
  'THESIS_WIN_BUT_LOW_CAPTURE',
  'THESIS_TIME_EXIT_LEGACY'
)
ORDER BY
  learning_score DESC NULLS LAST,
  recoverable_net_usdc DESC NULLS LAST,
  exit_time DESC;

CREATE OR REPLACE VIEW v_exit_trace_v3_summary AS
SELECT
  strategy,
  symbol,
  "interval",
  exit_thesis_state,
  sample_confidence,
  COUNT(*) AS trades,
  ROUND(SUM(net_pnl_usdc), 6) AS net_sum,
  ROUND(AVG(mfe_pct), 4) AS avg_mfe_pct,
  ROUND(AVG(exit_pct), 4) AS avg_exit_pct,
  ROUND(AVG(giveback_pct), 4) AS avg_giveback_pct,
  ROUND(AVG(recoverable_net_usdc), 6) AS avg_recoverable_net_usdc,
  ROUND(MAX(learning_score), 6) AS max_learning_score,
  MAX(exit_time) AS last_exit_time
FROM exit_trace_v3
GROUP BY strategy, symbol, "interval", exit_thesis_state, sample_confidence
ORDER BY max_learning_score DESC NULLS LAST, net_sum ASC NULLS LAST;

COMMIT;

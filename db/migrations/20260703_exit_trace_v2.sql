BEGIN;

CREATE OR REPLACE FUNCTION regex_numeric_or_null_v1(p_text TEXT, p_pattern TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_match TEXT;
BEGIN
  IF p_text IS NULL OR p_pattern IS NULL THEN
    RETURN NULL;
  END IF;

  v_match := substring(p_text FROM p_pattern);

  IF v_match IS NULL OR btrim(v_match) = '' THEN
    RETURN NULL;
  END IF;

  RETURN v_match::NUMERIC;

EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END;
$$;

CREATE TABLE IF NOT EXISTS exit_trace_v2 (
  id BIGSERIAL PRIMARY KEY,
  position_id BIGINT NOT NULL UNIQUE,

  symbol TEXT NOT NULL,
  "interval" TEXT NOT NULL,
  strategy TEXT NOT NULL,
  side TEXT,

  entry_time TIMESTAMPTZ,
  exit_time TIMESTAMPTZ,
  entry_price NUMERIC,
  exit_price NUMERIC,
  qty NUMERIC,

  exit_reason TEXT,
  exit_family TEXT,
  exit_decision_class_v1 TEXT,
  exit_decision_class_v2 TEXT,

  market_regime TEXT,
  trace_quality_v1 TEXT,
  trace_quality_v2 TEXT NOT NULL DEFAULT 'EXIT_TRACE_V2_DERIVED',

  gross_pnl_usdc NUMERIC,
  fees_usdc NUMERIC,
  net_pnl_usdc NUMERIC,
  hold_minutes NUMERIC,

  mfe_pct NUMERIC,
  mae_pct NUMERIC,
  exit_pct NUMERIC,
  giveback_pct NUMERIC,

  entry_notional_usdc NUMERIC,
  fee_pct_of_entry NUMERIC,
  net_pct_of_entry NUMERIC,
  mfe_capture_ratio NUMERIC,
  giveback_ratio NUMERIC,

  parsed_peak_pct NUMERIC,
  parsed_current_pct NUMERIC,
  parsed_floor_pct NUMERIC,
  parsed_trail_drop_pct NUMERIC,
  parsed_age_minutes NUMERIC,

  diagnostic_flags JSONB NOT NULL DEFAULT '{}'::jsonb,
  diagnostic_payload JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_exit_trace_v2_exit_time
ON exit_trace_v2(exit_time DESC);

CREATE INDEX IF NOT EXISTS ix_exit_trace_v2_slot
ON exit_trace_v2(symbol, "interval", strategy, exit_time DESC);

CREATE INDEX IF NOT EXISTS ix_exit_trace_v2_decision
ON exit_trace_v2(exit_decision_class_v2, exit_time DESC);

CREATE INDEX IF NOT EXISTS ix_exit_trace_v2_family
ON exit_trace_v2(exit_family, exit_time DESC);

CREATE OR REPLACE FUNCTION classify_exit_decision_v2(
  p_exit_family TEXT,
  p_exit_reason TEXT,
  p_net NUMERIC,
  p_mfe NUMERIC,
  p_exit_pct NUMERIC,
  p_giveback NUMERIC,
  p_fee_pct NUMERIC,
  p_mfe_capture_ratio NUMERIC,
  p_giveback_ratio NUMERIC
)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  IF p_exit_family = 'TIME_EXIT' THEN
    RETURN 'TIME_EXIT_LEGACY_REVIEW';
  END IF;

  IF p_exit_family = 'PROFIT_LOCK' AND COALESCE(p_net, 0) < 0 AND COALESCE(p_mfe, 0) > 0 THEN
    RETURN 'PROFIT_LOCK_TO_LOSS_AFTER_FEES_OR_GIVEBACK';
  END IF;

  IF p_exit_family = 'PROFIT_LOCK'
     AND COALESCE(p_giveback_ratio, 0) >= 0.60
     AND COALESCE(p_mfe, 0) > 0 THEN
    RETURN 'PROFIT_LOCK_HIGH_GIVEBACK_REVIEW';
  END IF;

  IF p_exit_family = 'PROFIT_LOCK'
     AND COALESCE(p_mfe_capture_ratio, 0) >= 0.70
     AND COALESCE(p_net, 0) > 0 THEN
    RETURN 'PROFIT_LOCK_GOOD_CAPTURE_CANDIDATE';
  END IF;

  IF p_exit_family = 'STOP'
     AND COALESCE(p_mfe, 0) >= 0.25
     AND COALESCE(p_net, 0) < 0 THEN
    RETURN 'STOP_AFTER_PROFIT_AVAILABLE_REVIEW';
  END IF;

  IF p_exit_family = 'EARLY_CUT'
     AND COALESCE(p_net, 0) < 0
     AND COALESCE(p_mfe, 0) > 0 THEN
    RETURN 'EARLY_CUT_CAPITAL_PROTECTION_REVIEW';
  END IF;

  IF COALESCE(p_net, 0) > 0
     AND COALESCE(p_mfe, 0) > 0
     AND COALESCE(p_mfe_capture_ratio, 0) < 0.50 THEN
    RETURN 'WIN_WITH_LOW_MFE_CAPTURE_REVIEW';
  END IF;

  IF COALESCE(p_net, 0) < 0
     AND COALESCE(p_mfe, 0) > 0 THEN
    RETURN 'LOSS_AFTER_AVAILABLE_PROFIT_REVIEW';
  END IF;

  IF COALESCE(p_net, 0) < 0 THEN
    RETURN 'LOSS_EXIT_REVIEW';
  END IF;

  IF COALESCE(p_net, 0) > 0 THEN
    RETURN 'PROFIT_EXIT_REVIEW';
  END IF;

  RETURN 'EXIT_REVIEW';
END;
$$;

CREATE OR REPLACE FUNCTION refresh_exit_trace_v2(p_since TIMESTAMPTZ DEFAULT now() - interval '30 days')
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_count INTEGER := 0;
BEGIN
  INSERT INTO exit_trace_v2 (
    position_id,
    symbol,
    "interval",
    strategy,
    side,
    entry_time,
    exit_time,
    entry_price,
    exit_price,
    qty,
    exit_reason,
    exit_family,
    exit_decision_class_v1,
    exit_decision_class_v2,
    market_regime,
    trace_quality_v1,
    trace_quality_v2,
    gross_pnl_usdc,
    fees_usdc,
    net_pnl_usdc,
    hold_minutes,
    mfe_pct,
    mae_pct,
    exit_pct,
    giveback_pct,
    entry_notional_usdc,
    fee_pct_of_entry,
    net_pct_of_entry,
    mfe_capture_ratio,
    giveback_ratio,
    parsed_peak_pct,
    parsed_current_pct,
    parsed_floor_pct,
    parsed_trail_drop_pct,
    parsed_age_minutes,
    diagnostic_flags,
    diagnostic_payload,
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
    e.entry_price,
    e.exit_price,
    e.qty,
    e.exit_reason,
    e.exit_family,
    e.exit_decision_class,
    classify_exit_decision_v2(
      e.exit_family,
      e.exit_reason,
      e.net_pnl_usdc,
      e.mfe_pct,
      e.exit_pct,
      e.giveback_pct,
      CASE
        WHEN e.entry_price IS NOT NULL AND e.qty IS NOT NULL AND abs(e.entry_price * e.qty) > 0
          THEN (abs(COALESCE(e.fees_usdc, 0)) / abs(e.entry_price * e.qty)) * 100
        ELSE NULL
      END,
      CASE
        WHEN e.mfe_pct IS NOT NULL AND e.mfe_pct > 0 AND e.exit_pct IS NOT NULL
          THEN e.exit_pct / e.mfe_pct
        ELSE NULL
      END,
      CASE
        WHEN e.mfe_pct IS NOT NULL AND e.mfe_pct > 0 AND e.giveback_pct IS NOT NULL
          THEN e.giveback_pct / e.mfe_pct
        ELSE NULL
      END
    ),
    e.market_regime,
    e.trace_quality,
    'EXIT_TRACE_V2_DERIVED',
    e.gross_pnl_usdc,
    e.fees_usdc,
    e.net_pnl_usdc,
    e.hold_minutes,
    e.mfe_pct,
    e.mae_pct,
    e.exit_pct,
    e.giveback_pct,
    CASE
      WHEN e.entry_price IS NOT NULL AND e.qty IS NOT NULL
        THEN abs(e.entry_price * e.qty)
      ELSE NULL
    END,
    CASE
      WHEN e.entry_price IS NOT NULL AND e.qty IS NOT NULL AND abs(e.entry_price * e.qty) > 0
        THEN (abs(COALESCE(e.fees_usdc, 0)) / abs(e.entry_price * e.qty)) * 100
      ELSE NULL
    END,
    CASE
      WHEN e.entry_price IS NOT NULL AND e.qty IS NOT NULL AND abs(e.entry_price * e.qty) > 0
        THEN (COALESCE(e.net_pnl_usdc, 0) / abs(e.entry_price * e.qty)) * 100
      ELSE NULL
    END,
    CASE
      WHEN e.mfe_pct IS NOT NULL AND e.mfe_pct > 0 AND e.exit_pct IS NOT NULL
        THEN e.exit_pct / e.mfe_pct
      ELSE NULL
    END,
    CASE
      WHEN e.mfe_pct IS NOT NULL AND e.mfe_pct > 0 AND e.giveback_pct IS NOT NULL
        THEN e.giveback_pct / e.mfe_pct
      ELSE NULL
    END,
    regex_numeric_or_null_v1(e.exit_reason, 'peak=([-0-9.]+)%'),
    regex_numeric_or_null_v1(e.exit_reason, 'current=([-0-9.]+)%'),
    regex_numeric_or_null_v1(e.exit_reason, 'floor=([-0-9.]+)%'),
    regex_numeric_or_null_v1(e.exit_reason, 'trail_drop=([-0-9.]+)%'),
    regex_numeric_or_null_v1(e.exit_reason, 'age=([-0-9.]+)m'),
    jsonb_build_object(
      'is_time_exit_legacy', e.exit_family = 'TIME_EXIT',
      'is_profit_lock', e.exit_family = 'PROFIT_LOCK',
      'is_stop', e.exit_family = 'STOP',
      'is_early_cut', e.exit_family = 'EARLY_CUT',
      'is_reconciled_exit', e.exit_reason ILIKE '%RECONCILED%' OR e.exit_reason ILIKE '%RECOVERED%',
      'has_available_profit', COALESCE(e.mfe_pct, 0) > 0,
      'ended_negative', COALESCE(e.net_pnl_usdc, 0) < 0,
      'ended_positive', COALESCE(e.net_pnl_usdc, 0) > 0,
      'gave_back_profit', COALESCE(e.giveback_pct, 0) > 0
    ),
    jsonb_build_object(
      'source', 'refresh_exit_trace_v2',
      'shadow_only', true,
      'note', 'Derived only from exit_trace_v1 / positions / v_trade_mfe_mae output. No trading impact.',
      'exit_reason_raw', e.exit_reason,
      'v1_class', e.exit_decision_class
    ),
    now()
  FROM exit_trace_v1 e
  WHERE e.exit_time >= p_since
  ON CONFLICT (position_id) DO UPDATE SET
    exit_time = EXCLUDED.exit_time,
    exit_reason = EXCLUDED.exit_reason,
    exit_family = EXCLUDED.exit_family,
    exit_decision_class_v1 = EXCLUDED.exit_decision_class_v1,
    exit_decision_class_v2 = EXCLUDED.exit_decision_class_v2,
    market_regime = EXCLUDED.market_regime,
    trace_quality_v1 = EXCLUDED.trace_quality_v1,
    trace_quality_v2 = EXCLUDED.trace_quality_v2,
    gross_pnl_usdc = EXCLUDED.gross_pnl_usdc,
    fees_usdc = EXCLUDED.fees_usdc,
    net_pnl_usdc = EXCLUDED.net_pnl_usdc,
    hold_minutes = EXCLUDED.hold_minutes,
    mfe_pct = EXCLUDED.mfe_pct,
    mae_pct = EXCLUDED.mae_pct,
    exit_pct = EXCLUDED.exit_pct,
    giveback_pct = EXCLUDED.giveback_pct,
    entry_notional_usdc = EXCLUDED.entry_notional_usdc,
    fee_pct_of_entry = EXCLUDED.fee_pct_of_entry,
    net_pct_of_entry = EXCLUDED.net_pct_of_entry,
    mfe_capture_ratio = EXCLUDED.mfe_capture_ratio,
    giveback_ratio = EXCLUDED.giveback_ratio,
    parsed_peak_pct = EXCLUDED.parsed_peak_pct,
    parsed_current_pct = EXCLUDED.parsed_current_pct,
    parsed_floor_pct = EXCLUDED.parsed_floor_pct,
    parsed_trail_drop_pct = EXCLUDED.parsed_trail_drop_pct,
    parsed_age_minutes = EXCLUDED.parsed_age_minutes,
    diagnostic_flags = EXCLUDED.diagnostic_flags,
    diagnostic_payload = EXCLUDED.diagnostic_payload,
    updated_at = now();

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION trg_refresh_exit_trace_v2_from_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM refresh_exit_trace_v2(COALESCE(NEW.exit_time, now()) - interval '1 minute');
  RETURN NEW;

EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'exit_trace_v2 trigger failed for position_id=%: %', NEW.position_id, SQLERRM;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_refresh_exit_trace_v2_from_v1 ON exit_trace_v1;

CREATE TRIGGER trg_refresh_exit_trace_v2_from_v1
AFTER INSERT OR UPDATE
ON exit_trace_v1
FOR EACH ROW
EXECUTE FUNCTION trg_refresh_exit_trace_v2_from_v1();

CREATE OR REPLACE VIEW v_exit_trace_v2_recent AS
SELECT
  position_id,
  exit_time,
  symbol,
  "interval",
  strategy,
  side,
  exit_reason,
  exit_family,
  exit_decision_class_v1,
  exit_decision_class_v2,
  market_regime,
  ROUND(net_pnl_usdc, 6) AS net_pnl_usdc,
  ROUND(mfe_pct, 4) AS mfe_pct,
  ROUND(exit_pct, 4) AS exit_pct,
  ROUND(giveback_pct, 4) AS giveback_pct,
  ROUND(mfe_capture_ratio, 4) AS mfe_capture_ratio,
  ROUND(giveback_ratio, 4) AS giveback_ratio,
  ROUND(fee_pct_of_entry, 4) AS fee_pct_of_entry,
  ROUND(parsed_peak_pct, 4) AS parsed_peak_pct,
  ROUND(parsed_current_pct, 4) AS parsed_current_pct,
  ROUND(parsed_floor_pct, 4) AS parsed_floor_pct,
  ROUND(parsed_trail_drop_pct, 4) AS parsed_trail_drop_pct,
  ROUND(parsed_age_minutes, 2) AS parsed_age_minutes,
  hold_minutes
FROM exit_trace_v2
ORDER BY exit_time DESC;

CREATE OR REPLACE VIEW v_exit_trace_v2_summary AS
SELECT
  strategy,
  symbol,
  "interval",
  exit_family,
  exit_decision_class_v2,
  COUNT(*) AS trades,
  ROUND(SUM(net_pnl_usdc), 6) AS net_sum,
  ROUND(AVG(net_pnl_usdc), 6) AS avg_net,
  ROUND(AVG(mfe_pct), 4) AS avg_mfe_pct,
  ROUND(AVG(exit_pct), 4) AS avg_exit_pct,
  ROUND(AVG(giveback_pct), 4) AS avg_giveback_pct,
  ROUND(AVG(mfe_capture_ratio), 4) AS avg_mfe_capture_ratio,
  ROUND(AVG(giveback_ratio), 4) AS avg_giveback_ratio,
  ROUND(AVG(fee_pct_of_entry), 4) AS avg_fee_pct_of_entry,
  ROUND(AVG(hold_minutes), 2) AS avg_hold_minutes,
  MAX(exit_time) AS last_exit_time
FROM exit_trace_v2
GROUP BY strategy, symbol, "interval", exit_family, exit_decision_class_v2;

CREATE OR REPLACE VIEW v_exit_trace_v2_profit_lock_review AS
SELECT *
FROM v_exit_trace_v2_recent
WHERE exit_family = 'PROFIT_LOCK'
ORDER BY exit_time DESC;

CREATE OR REPLACE VIEW v_exit_trace_v2_time_exit_review AS
SELECT *
FROM v_exit_trace_v2_recent
WHERE exit_family = 'TIME_EXIT'
   OR exit_decision_class_v2 = 'TIME_EXIT_LEGACY_REVIEW'
ORDER BY exit_time DESC;

CREATE OR REPLACE VIEW v_exit_trace_v2_priority_review AS
SELECT *
FROM v_exit_trace_v2_recent
WHERE exit_decision_class_v2 IN (
  'PROFIT_LOCK_TO_LOSS_AFTER_FEES_OR_GIVEBACK',
  'PROFIT_LOCK_HIGH_GIVEBACK_REVIEW',
  'STOP_AFTER_PROFIT_AVAILABLE_REVIEW',
  'EARLY_CUT_CAPITAL_PROTECTION_REVIEW',
  'WIN_WITH_LOW_MFE_CAPTURE_REVIEW',
  'LOSS_AFTER_AVAILABLE_PROFIT_REVIEW',
  'TIME_EXIT_LEGACY_REVIEW'
)
ORDER BY exit_time DESC;

COMMIT;

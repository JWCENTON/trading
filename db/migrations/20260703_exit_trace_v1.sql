BEGIN;

CREATE TABLE IF NOT EXISTS exit_trace_v1 (
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
  market_regime TEXT,

  gross_pnl_usdc NUMERIC,
  fees_usdc NUMERIC,
  net_pnl_usdc NUMERIC,
  hold_minutes NUMERIC,

  mfe_pct NUMERIC,
  mae_pct NUMERIC,
  exit_pct NUMERIC,
  giveback_pct NUMERIC,

  exit_family TEXT,
  exit_decision_class TEXT,
  trace_quality TEXT NOT NULL DEFAULT 'POSITION_ONLY',

  exit_context JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_position JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_exit_trace_v1_exit_time
ON exit_trace_v1(exit_time DESC);

CREATE INDEX IF NOT EXISTS ix_exit_trace_v1_slot
ON exit_trace_v1(symbol, "interval", strategy, exit_time DESC);

CREATE INDEX IF NOT EXISTS ix_exit_trace_v1_reason
ON exit_trace_v1(exit_reason, exit_time DESC);

CREATE OR REPLACE FUNCTION classify_exit_family_v1(p_exit_reason TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  IF p_exit_reason IS NULL THEN
    RETURN 'UNKNOWN';
  ELSIF p_exit_reason ILIKE '%TIME%' THEN
    RETURN 'TIME_EXIT';
  ELSIF p_exit_reason ILIKE '%PROFIT_LOCK%' THEN
    RETURN 'PROFIT_LOCK';
  ELSIF p_exit_reason ILIKE '%TRAIL%' THEN
    RETURN 'TRAILING';
  ELSIF p_exit_reason ILIKE '%FLOOR%' THEN
    RETURN 'FLOOR';
  ELSIF p_exit_reason ILIKE '%STOP%' THEN
    RETURN 'STOP';
  ELSIF p_exit_reason ILIKE '%SOFT%' THEN
    RETURN 'SOFT_EXIT';
  ELSIF p_exit_reason ILIKE '%TAKE_PROFIT%' OR p_exit_reason ILIKE '%TP%' THEN
    RETURN 'TAKE_PROFIT';
  ELSIF p_exit_reason ILIKE '%EARLY_CUT%' THEN
    RETURN 'EARLY_CUT';
  ELSE
    RETURN 'OTHER';
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION classify_exit_decision_v1(
  p_exit_reason TEXT,
  p_net NUMERIC,
  p_mfe NUMERIC,
  p_exit_pct NUMERIC,
  p_giveback NUMERIC
)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  IF p_exit_reason ILIKE '%TIME%' THEN
    RETURN 'TIME_EXIT_LEGACY_REVIEW';
  END IF;

  IF p_mfe IS NULL OR p_exit_pct IS NULL THEN
    IF p_net IS NULL THEN
      RETURN 'EXIT_RECORDED_NEEDS_CONTEXT';
    ELSIF p_net > 0 THEN
      RETURN 'PROFIT_EXIT_NEEDS_TRACE';
    ELSE
      RETURN 'LOSS_EXIT_NEEDS_TRACE';
    END IF;
  END IF;

  IF p_net > 0 AND COALESCE(p_giveback, 0) <= 0.15 THEN
    RETURN 'GOOD_PROFIT_CAPTURE_CANDIDATE';
  ELSIF p_net > 0 AND p_mfe >= p_exit_pct + 0.30 THEN
    RETURN 'EXIT_TOO_EARLY_CANDIDATE';
  ELSIF p_net <= 0 AND p_mfe > 0.25 THEN
    RETURN 'PROFIT_GIVEN_BACK_TO_LOSS_CANDIDATE';
  ELSIF p_net <= 0 THEN
    RETURN 'CAPITAL_PROTECTION_OR_BAD_ENTRY_REVIEW';
  ELSE
    RETURN 'EXIT_REVIEW';
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION trg_capture_exit_trace_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.status = 'CLOSED' AND NEW.exit_time IS NOT NULL THEN
    INSERT INTO exit_trace_v1 (
      position_id, symbol, "interval", strategy, side,
      entry_time, exit_time, entry_price, exit_price, qty,
      exit_reason, market_regime,
      gross_pnl_usdc, fees_usdc, net_pnl_usdc, hold_minutes,
      exit_family, exit_decision_class, trace_quality,
      exit_context, raw_position, updated_at
    )
    VALUES (
      NEW.id, NEW.symbol, NEW."interval", NEW.strategy, NEW.side,
      NEW.entry_time, NEW.exit_time, NEW.entry_price, NEW.exit_price, NEW.qty,
      NEW.exit_reason, NEW.market_regime,
      NEW.gross_pnl_usdc, NEW.fees_usdc, NEW.net_pnl_usdc, NEW.hold_minutes,
      classify_exit_family_v1(NEW.exit_reason),
      classify_exit_decision_v1(NEW.exit_reason, NEW.net_pnl_usdc, NULL, NULL, NULL),
      'POSITION_ONLY',
      jsonb_build_object(
        'source', 'positions_close_trigger',
        'note', 'Exit Trace V1 shadow-only. Does not affect trading.'
      ),
      to_jsonb(NEW),
      now()
    )
    ON CONFLICT (position_id) DO UPDATE SET
      exit_time = EXCLUDED.exit_time,
      exit_price = EXCLUDED.exit_price,
      exit_reason = EXCLUDED.exit_reason,
      market_regime = EXCLUDED.market_regime,
      gross_pnl_usdc = EXCLUDED.gross_pnl_usdc,
      fees_usdc = EXCLUDED.fees_usdc,
      net_pnl_usdc = EXCLUDED.net_pnl_usdc,
      hold_minutes = EXCLUDED.hold_minutes,
      exit_family = EXCLUDED.exit_family,
      exit_decision_class = EXCLUDED.exit_decision_class,
      raw_position = EXCLUDED.raw_position,
      updated_at = now();
  END IF;

  RETURN NEW;

EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'exit_trace_v1 trigger failed for position_id=%: %', NEW.id, SQLERRM;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_capture_exit_trace_v1 ON positions;

CREATE TRIGGER trg_capture_exit_trace_v1
AFTER INSERT OR UPDATE OF status, exit_time, exit_price, exit_reason
ON positions
FOR EACH ROW
EXECUTE FUNCTION trg_capture_exit_trace_v1();

CREATE OR REPLACE FUNCTION refresh_exit_trace_v1(p_since TIMESTAMPTZ DEFAULT now() - interval '30 days')
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_count INTEGER := 0;
BEGIN
  IF to_regclass('public.v_trade_mfe_mae') IS NOT NULL THEN
    INSERT INTO exit_trace_v1 (
      position_id, symbol, "interval", strategy, side,
      entry_time, exit_time, entry_price, exit_price, qty,
      exit_reason, market_regime,
      gross_pnl_usdc, fees_usdc, net_pnl_usdc, hold_minutes,
      mfe_pct, mae_pct, exit_pct, giveback_pct,
      exit_family, exit_decision_class, trace_quality,
      exit_context, raw_position, updated_at
    )
    SELECT
      p.id, p.symbol, p."interval", p.strategy, p.side,
      p.entry_time, p.exit_time, p.entry_price, p.exit_price, p.qty,
      p.exit_reason, p.market_regime,
      p.gross_pnl_usdc, p.fees_usdc, p.net_pnl_usdc, p.hold_minutes,
      m.mfe_pct, m.mae_pct, m.exit_pct, m.giveback_pct,
      classify_exit_family_v1(p.exit_reason),
      classify_exit_decision_v1(p.exit_reason, p.net_pnl_usdc, m.mfe_pct, m.exit_pct, m.giveback_pct),
      'POSITION_PLUS_MFE_MAE',
      jsonb_build_object(
        'source', 'refresh_exit_trace_v1',
        'mfe_mae_source', 'v_trade_mfe_mae',
        'shadow_only', true
      ),
      to_jsonb(p),
      now()
    FROM positions p
    LEFT JOIN v_trade_mfe_mae m ON m.id = p.id
    WHERE p.status = 'CLOSED'
      AND p.exit_time >= p_since
      AND p.exit_time IS NOT NULL
    ON CONFLICT (position_id) DO UPDATE SET
      mfe_pct = EXCLUDED.mfe_pct,
      mae_pct = EXCLUDED.mae_pct,
      exit_pct = EXCLUDED.exit_pct,
      giveback_pct = EXCLUDED.giveback_pct,
      exit_family = EXCLUDED.exit_family,
      exit_decision_class = EXCLUDED.exit_decision_class,
      trace_quality = EXCLUDED.trace_quality,
      exit_context = EXCLUDED.exit_context,
      raw_position = EXCLUDED.raw_position,
      updated_at = now();

    GET DIAGNOSTICS v_count = ROW_COUNT;
  ELSE
    INSERT INTO exit_trace_v1 (
      position_id, symbol, "interval", strategy, side,
      entry_time, exit_time, entry_price, exit_price, qty,
      exit_reason, market_regime,
      gross_pnl_usdc, fees_usdc, net_pnl_usdc, hold_minutes,
      exit_family, exit_decision_class, trace_quality,
      exit_context, raw_position, updated_at
    )
    SELECT
      p.id, p.symbol, p."interval", p.strategy, p.side,
      p.entry_time, p.exit_time, p.entry_price, p.exit_price, p.qty,
      p.exit_reason, p.market_regime,
      p.gross_pnl_usdc, p.fees_usdc, p.net_pnl_usdc, p.hold_minutes,
      classify_exit_family_v1(p.exit_reason),
      classify_exit_decision_v1(p.exit_reason, p.net_pnl_usdc, NULL, NULL, NULL),
      'POSITION_ONLY',
      jsonb_build_object(
        'source', 'refresh_exit_trace_v1',
        'mfe_mae_source', 'missing',
        'shadow_only', true
      ),
      to_jsonb(p),
      now()
    FROM positions p
    WHERE p.status = 'CLOSED'
      AND p.exit_time >= p_since
      AND p.exit_time IS NOT NULL
    ON CONFLICT (position_id) DO UPDATE SET
      exit_family = EXCLUDED.exit_family,
      exit_decision_class = EXCLUDED.exit_decision_class,
      trace_quality = EXCLUDED.trace_quality,
      exit_context = EXCLUDED.exit_context,
      raw_position = EXCLUDED.raw_position,
      updated_at = now();

    GET DIAGNOSTICS v_count = ROW_COUNT;
  END IF;

  RETURN v_count;
END;
$$;

CREATE OR REPLACE VIEW v_exit_trace_v1_recent AS
SELECT
  position_id,
  exit_time,
  symbol,
  "interval",
  strategy,
  side,
  exit_reason,
  exit_family,
  exit_decision_class,
  trace_quality,
  market_regime,
  ROUND(net_pnl_usdc, 6) AS net_pnl_usdc,
  ROUND(mfe_pct, 4) AS mfe_pct,
  ROUND(mae_pct, 4) AS mae_pct,
  ROUND(exit_pct, 4) AS exit_pct,
  ROUND(giveback_pct, 4) AS giveback_pct,
  hold_minutes
FROM exit_trace_v1
ORDER BY exit_time DESC;

CREATE OR REPLACE VIEW v_exit_trace_v1_summary AS
SELECT
  strategy,
  symbol,
  "interval",
  exit_family,
  exit_decision_class,
  COUNT(*) AS trades,
  ROUND(SUM(net_pnl_usdc), 6) AS net_sum,
  ROUND(AVG(net_pnl_usdc), 6) AS avg_net,
  ROUND(AVG(mfe_pct), 4) AS avg_mfe_pct,
  ROUND(AVG(exit_pct), 4) AS avg_exit_pct,
  ROUND(AVG(giveback_pct), 4) AS avg_giveback_pct,
  ROUND(AVG(hold_minutes), 2) AS avg_hold_minutes,
  MAX(exit_time) AS last_exit_time
FROM exit_trace_v1
GROUP BY strategy, symbol, "interval", exit_family, exit_decision_class;

CREATE OR REPLACE VIEW v_exit_trace_v1_review_queue AS
SELECT *
FROM v_exit_trace_v1_recent
WHERE exit_decision_class IN (
  'EXIT_TOO_EARLY_CANDIDATE',
  'PROFIT_GIVEN_BACK_TO_LOSS_CANDIDATE',
  'TIME_EXIT_LEGACY_REVIEW',
  'CAPITAL_PROTECTION_OR_BAD_ENTRY_REVIEW'
)
ORDER BY exit_time DESC;

COMMIT;

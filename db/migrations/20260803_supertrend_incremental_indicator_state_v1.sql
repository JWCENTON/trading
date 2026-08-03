-- WALTRADE SUPERTREND INCREMENTAL INDICATOR STATE V1
-- Persistent recursive warm state; candle evidence and high-water are committed atomically.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $dependencies$
DECLARE
    conflicting_checksum TEXT;
BEGIN
    IF to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
        RAISE EXCEPTION
            'SUPERTREND_INDICATOR_STATE_REQUIRED_RELATION_MISSING:schema_migration_ledger_v1';
    END IF;
    IF to_regclass('public.supertrend_candle_checkpoint_v1') IS NULL THEN
        RAISE EXCEPTION
            'SUPERTREND_INDICATOR_STATE_REQUIRED_RELATION_MISSING:supertrend_candle_checkpoint_v1';
    END IF;
    SELECT checksum_sha256 INTO conflicting_checksum
    FROM public.schema_migration_ledger_v1
    WHERE migration_id=
          '20260803_supertrend_incremental_indicator_state_v1.sql'
      AND checksum_sha256<>
          '987f666cd9344fe8aff074cc578533a75ce4faa1ce37aec72bb55e0af7d0b1e1'
    ORDER BY applied_at DESC LIMIT 1;
    IF conflicting_checksum IS NOT NULL THEN
        RAISE EXCEPTION
            'SUPERTREND_INDICATOR_STATE_LEDGER_CHECKSUM_CONFLICT:%',
            conflicting_checksum;
    END IF;
END;
$dependencies$;

CREATE TABLE IF NOT EXISTS public.supertrend_indicator_state_v1 (
    environment TEXT NOT NULL CHECK (environment IN ('paper','live')),
    deployment_id TEXT NOT NULL CHECK (btrim(deployment_id) <> ''),
    symbol TEXT NOT NULL
        CHECK (symbol=upper(symbol) AND btrim(symbol) <> ''),
    "interval" TEXT NOT NULL
        CHECK ("interval"=lower("interval") AND btrim("interval") <> ''),
    strategy TEXT NOT NULL CHECK (strategy='SUPERTREND'),
    last_calculated_candle_open_time TIMESTAMPTZ NOT NULL,
    last_close DOUBLE PRECISION NOT NULL
        CHECK (last_close NOT IN ('Infinity'::float8,'-Infinity'::float8,'NaN'::float8)),
    ema_value DOUBLE PRECISION NOT NULL
        CHECK (ema_value NOT IN ('Infinity'::float8,'-Infinity'::float8,'NaN'::float8)),
    atr_value DOUBLE PRECISION NOT NULL
        CHECK (atr_value NOT IN ('Infinity'::float8,'-Infinity'::float8,'NaN'::float8)),
    final_upper_band DOUBLE PRECISION NOT NULL
        CHECK (final_upper_band NOT IN ('Infinity'::float8,'-Infinity'::float8,'NaN'::float8)),
    final_lower_band DOUBLE PRECISION NOT NULL
        CHECK (final_lower_band NOT IN ('Infinity'::float8,'-Infinity'::float8,'NaN'::float8)),
    supertrend_direction INTEGER NOT NULL
        CHECK (supertrend_direction IN (-1,1)),
    parameter_fingerprint TEXT NOT NULL
        CHECK (parameter_fingerprint ~ '^[0-9a-f]{64}$'),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(environment,deployment_id,symbol,"interval",strategy)
);

CREATE INDEX IF NOT EXISTS ix_supertrend_indicator_state_high_water_v1
    ON public.supertrend_indicator_state_v1(
      environment,deployment_id,last_calculated_candle_open_time DESC
    );

COMMENT ON TABLE public.supertrend_indicator_state_v1 IS
  'Canonical restart-safe EMA/ATR/SuperTrend recursive state and indicator high-water.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260803_supertrend_incremental_indicator_state_v1.sql',
    '987f666cd9344fe8aff074cc578533a75ce4faa1ce37aec72bb55e0af7d0b1e1',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'SUPERTREND_INDICATOR_STATE_V1',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    '0ed0c54b46f4ae1998f30fc2a99f3460947fbb57',
    'SUPERTREND_INCREMENTAL_INDICATOR_STATE_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id=
          '20260803_supertrend_incremental_indicator_state_v1.sql'
);

COMMIT;

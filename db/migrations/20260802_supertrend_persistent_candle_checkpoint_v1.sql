-- WALTRADE SUPERTREND PERSISTENT CANDLE CHECKPOINT V1
-- Canonical per-slot high-water and append-only freshness transition evidence.
-- Runtime code validates and uses this schema; it never creates it.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $dependencies$
DECLARE
    conflicting_checksum TEXT;
BEGIN
    IF to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
        RAISE EXCEPTION
            'SUPERTREND_CHECKPOINT_REQUIRED_RELATION_MISSING:schema_migration_ledger_v1';
    END IF;
    SELECT checksum_sha256 INTO conflicting_checksum
    FROM public.schema_migration_ledger_v1
    WHERE migration_id=
          '20260802_supertrend_persistent_candle_checkpoint_v1.sql'
      AND checksum_sha256<>
          '0f202da9cba736fb55016a2749ccc81ed9439321d55dcbcd03c4160164961075'
    ORDER BY applied_at DESC LIMIT 1;
    IF conflicting_checksum IS NOT NULL THEN
        RAISE EXCEPTION
            'SUPERTREND_CHECKPOINT_LEDGER_CHECKSUM_CONFLICT:%',
            conflicting_checksum;
    END IF;
END;
$dependencies$;

CREATE TABLE IF NOT EXISTS public.supertrend_candle_checkpoint_v1 (
    environment TEXT NOT NULL
        CHECK (environment IN ('paper','live')),
    deployment_id TEXT NOT NULL CHECK (btrim(deployment_id) <> ''),
    symbol TEXT NOT NULL
        CHECK (symbol=upper(symbol) AND btrim(symbol) <> ''),
    "interval" TEXT NOT NULL
        CHECK ("interval"=lower("interval") AND btrim("interval") <> ''),
    strategy TEXT NOT NULL CHECK (strategy='SUPERTREND'),
    last_processed_candle_open_time TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    state TEXT NOT NULL CHECK (state IN ('READY','CATCHING_UP','STALLED')),
    latest_closed_candle_open_time TIMESTAMPTZ,
    backlog_size INTEGER NOT NULL CHECK (backlog_size >= 0),
    reason TEXT NOT NULL CHECK (btrim(reason) <> ''),
    resume_source TEXT NOT NULL CHECK (btrim(resume_source) <> ''),
    PRIMARY KEY(environment,deployment_id,symbol,"interval",strategy)
);

CREATE TABLE IF NOT EXISTS public.supertrend_candle_checkpoint_event_v1 (
    event_id BIGSERIAL PRIMARY KEY,
    environment TEXT NOT NULL
        CHECK (environment IN ('paper','live')),
    deployment_id TEXT NOT NULL CHECK (btrim(deployment_id) <> ''),
    symbol TEXT NOT NULL
        CHECK (symbol=upper(symbol) AND btrim(symbol) <> ''),
    "interval" TEXT NOT NULL
        CHECK ("interval"=lower("interval") AND btrim("interval") <> ''),
    strategy TEXT NOT NULL CHECK (strategy='SUPERTREND'),
    event_type TEXT NOT NULL
        CHECK (event_type IN ('OBSERVED','ADVANCED','STALLED')),
    checkpoint_before TIMESTAMPTZ,
    checkpoint_after TIMESTAMPTZ,
    latest_closed_candle_open_time TIMESTAMPTZ,
    state TEXT NOT NULL CHECK (state IN ('READY','CATCHING_UP','STALLED')),
    backlog_size INTEGER NOT NULL CHECK (backlog_size >= 0),
    reason TEXT NOT NULL CHECK (btrim(reason) <> ''),
    resume_source TEXT NOT NULL CHECK (btrim(resume_source) <> ''),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (
      (event_type='ADVANCED' AND checkpoint_after IS NOT NULL)
      OR event_type IN ('OBSERVED','STALLED')
    )
);

CREATE INDEX IF NOT EXISTS ix_supertrend_checkpoint_event_slot_v1
    ON public.supertrend_candle_checkpoint_event_v1(
      environment,deployment_id,symbol,"interval",recorded_at DESC
    );

CREATE OR REPLACE FUNCTION public.prevent_supertrend_checkpoint_event_mutation_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$function$;

DO $append_only_trigger$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname='trg_supertrend_checkpoint_event_append_only_v1'
          AND tgrelid='public.supertrend_candle_checkpoint_event_v1'::regclass
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_supertrend_checkpoint_event_append_only_v1
        BEFORE UPDATE OR DELETE
        ON public.supertrend_candle_checkpoint_event_v1
        FOR EACH ROW
        EXECUTE FUNCTION public.prevent_supertrend_checkpoint_event_mutation_v1();
    END IF;
END;
$append_only_trigger$;

COMMENT ON TABLE public.supertrend_candle_checkpoint_v1 IS
  'Canonical last fully processed closed candle per SUPERTREND runtime slot.';
COMMENT ON TABLE public.supertrend_candle_checkpoint_event_v1 IS
  'Append-only SUPERTREND checkpoint before/after and freshness evidence.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260802_supertrend_persistent_candle_checkpoint_v1.sql',
    '0f202da9cba736fb55016a2749ccc81ed9439321d55dcbcd03c4160164961075',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'SUPERTREND_CHECKPOINT_V1',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    '09cb97e967bfdb3b0b6ef0481dfeb4b2bc0467f3',
    'SUPERTREND_PERSISTENT_CANDLE_CHECKPOINT_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id=
          '20260802_supertrend_persistent_candle_checkpoint_v1.sql'
);

COMMIT;

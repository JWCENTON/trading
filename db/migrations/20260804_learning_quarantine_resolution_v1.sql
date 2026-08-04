-- WALTRADE LEARNING QUARANTINE RESOLUTION V1
-- Append-only compensation. Exclusions themselves remain immutable.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $dependencies$
BEGIN
    IF to_regclass('public.learning_outcome_exclusion_v1') IS NULL
       OR to_regprocedure(
           'public.prevent_legacy_recovery_history_mutation_v1()'
       ) IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_QUARANTINE_RESOLUTION_PREREQUISITE_MISSING';
    END IF;
END;
$dependencies$;

CREATE TABLE IF NOT EXISTS public.learning_outcome_exclusion_resolution_v1 (
    resolution_id BIGSERIAL PRIMARY KEY,
    exclusion_id BIGINT NOT NULL,
    resolution_action TEXT NOT NULL,
    reason TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_reference TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_by TEXT NOT NULL,
    CONSTRAINT fk_learning_outcome_exclusion_resolution_v1_exclusion
        FOREIGN KEY(exclusion_id)
        REFERENCES public.learning_outcome_exclusion_v1(exclusion_id)
        ON DELETE RESTRICT,
    CONSTRAINT ux_learning_outcome_exclusion_resolution_v1_revoke
        UNIQUE(exclusion_id, resolution_action),
    CONSTRAINT ck_learning_outcome_exclusion_resolution_v1_contract CHECK (
        resolution_action = 'REVOKE'
        AND btrim(reason) <> ''
        AND source_type = 'MANUAL_GOVERNANCE_DECISION'
        AND btrim(source_reference) <> ''
        AND btrim(created_by) <> ''
    )
);

CREATE INDEX IF NOT EXISTS ix_learning_exclusion_resolution_exclusion_v1
    ON public.learning_outcome_exclusion_resolution_v1(exclusion_id);

DO $append_only_trigger$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname='trg_learning_exclusion_resolution_v1_append_only'
          AND tgrelid=
              'public.learning_outcome_exclusion_resolution_v1'::regclass
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_learning_exclusion_resolution_v1_append_only
        BEFORE UPDATE OR DELETE
        ON public.learning_outcome_exclusion_resolution_v1
        FOR EACH ROW
        EXECUTE FUNCTION public.prevent_legacy_recovery_history_mutation_v1();
    END IF;
END;
$append_only_trigger$;

CREATE OR REPLACE VIEW public.v_learning_active_outcome_exclusions_v1 AS
SELECT exclusion.*
FROM public.learning_outcome_exclusion_v1 exclusion
WHERE NOT EXISTS (
    SELECT 1
    FROM public.learning_outcome_exclusion_resolution_v1 resolution
    WHERE resolution.exclusion_id = exclusion.exclusion_id
      AND resolution.resolution_action = 'REVOKE'
);

CREATE OR REPLACE FUNCTION public.learning_outcome_is_excluded_v1(
    p_position_id BIGINT
)
RETURNS BOOLEAN
LANGUAGE SQL
STABLE
STRICT
AS $function$
    SELECT EXISTS (
        SELECT 1
        FROM public.v_learning_active_outcome_exclusions_v1 exclusion
        WHERE exclusion.position_id = p_position_id
    )
$function$;

COMMENT ON TABLE public.learning_outcome_exclusion_resolution_v1 IS
    'Append-only authorized compensation history; one terminal REVOKE per exclusion.';
COMMENT ON VIEW public.v_learning_active_outcome_exclusions_v1 IS
    'Deterministic active exclusion state derived only from append-only history.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260804_learning_quarantine_resolution_v1.sql',
    '1d58f0f424f6cb25b8a504ba308f04351fbae0151863272d55a4ed53d0df173b',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'LEARNING_FT_QUARANTINE_V1',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    'c2cef02cbff0c34cef97886f86458ee30020e229',
    'LEARNING_QUARANTINE_RESOLUTION_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id='20260804_learning_quarantine_resolution_v1.sql'
);

COMMIT;

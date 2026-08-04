-- WALTRADE CANONICAL LEARNING UNIVERSE FT QUARANTINE V1
-- New snapshots cannot copy FT-incomplete or actively quarantined outcomes.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $preserve_unfiltered_universe$
DECLARE
    v_live_signature CONSTANT TEXT :=
        'public.learning_canonical_evidence_universe_live_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)';
    v_unfiltered_signature CONSTANT TEXT :=
        'public.learning_canonical_evidence_universe_pre_ft_quarantine_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)';
    v_definition TEXT;
BEGIN
    IF to_regprocedure(v_live_signature) IS NULL
       OR to_regprocedure(
           'public.learning_outcome_is_eligible_v1(bigint)'
       ) IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_FT_QUARANTINE_PREREQUISITE_MISSING';
    END IF;

    IF to_regprocedure(v_unfiltered_signature) IS NULL THEN
        v_definition := pg_get_functiondef(to_regprocedure(v_live_signature));
        IF position('learning_outcome_is_eligible_v1' IN v_definition) > 0 THEN
            RAISE EXCEPTION
                'LEARNING_CANONICAL_FT_QUARANTINE_UNFILTERED_SOURCE_LOST';
        END IF;
        v_definition := replace(
            v_definition,
            'learning_canonical_evidence_universe_live_v1',
            'learning_canonical_evidence_universe_pre_ft_quarantine_v1'
        );
        EXECUTE v_definition;
    END IF;
END;
$preserve_unfiltered_universe$;

CREATE OR REPLACE FUNCTION public.learning_canonical_evidence_universe_live_v1(
    p_environment TEXT,
    p_sample_from TIMESTAMPTZ,
    p_sample_to TIMESTAMPTZ,
    p_evidence_cutoff_at TIMESTAMPTZ
)
RETURNS TABLE (
    environment TEXT, symbol TEXT, "interval" TEXT, strategy TEXT,
    decision_key TEXT, decision_id UUID, position_id BIGINT,
    entry_time TIMESTAMPTZ, exit_time TIMESTAMPTZ,
    outcome_timestamp TIMESTAMPTZ, realized_pnl_usdc NUMERIC,
    gross_pnl_usdc NUMERIC, fees_usdc NUMERIC, mfe_pct NUMERIC,
    mae_pct NUMERIC, regime_identity TEXT, regime_context JSONB,
    source_refreshed_at TIMESTAMPTZ, has_full_context BOOLEAN,
    has_avoid_review BOOLEAN, has_entry_quality_review BOOLEAN,
    has_positive_confirmation BOOLEAN, eligibility_reason TEXT,
    registry_available_at TIMESTAMPTZ, outcome_available_at TIMESTAMPTZ
)
LANGUAGE SQL
STABLE
AS $function$
    SELECT source.*
    FROM public.learning_canonical_evidence_universe_pre_ft_quarantine_v1(
        p_environment, p_sample_from, p_sample_to, p_evidence_cutoff_at
    ) source
    WHERE public.learning_outcome_is_eligible_v1(source.position_id)
    ORDER BY source.decision_key
$function$;

DO $postcondition$
DECLARE
    v_definition TEXT;
BEGIN
    v_definition := pg_get_functiondef(to_regprocedure(
        'public.learning_canonical_evidence_universe_live_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
    ));
    IF position('learning_outcome_is_eligible_v1' IN v_definition) = 0
       OR position(
           'learning_canonical_evidence_universe_pre_ft_quarantine_v1'
           IN v_definition
       ) = 0 THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_FT_QUARANTINE_POSTCONDITION_FAILED';
    END IF;
END;
$postcondition$;

COMMENT ON FUNCTION public.learning_canonical_evidence_universe_live_v1(
    TEXT,TIMESTAMPTZ,TIMESTAMPTZ,TIMESTAMPTZ
) IS
    'Canonical new-generation source, filtered by same-position FT COMPLETE and active quarantine state.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260804_learning_canonical_universe_ft_quarantine_v1.sql',
    'c10c83757577277e84056c06ba1da57c0ff047c233384ef616d55d891855124c',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'LEARNING_FT_QUARANTINE_V1',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    'c2cef02cbff0c34cef97886f86458ee30020e229',
    'LEARNING_CANONICAL_UNIVERSE_FT_QUARANTINE_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id=
          '20260804_learning_canonical_universe_ft_quarantine_v1.sql'
);

COMMIT;

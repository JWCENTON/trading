-- WALTRADE FINANCIAL TRUTH REQUIRED LEARNING ELIGIBILITY V1
-- A closed outcome is eligible only with same-position canonical FT COMPLETE.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $dependencies$
BEGIN
    IF to_regclass('public.positions') IS NULL
       OR to_regclass('public.canonical_financial_truth_v1') IS NULL
       OR to_regclass('public.v_learning_active_outcome_exclusions_v1') IS NULL
       OR to_regprocedure(
           'public.learning_outcome_is_excluded_v1(bigint)'
       ) IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_FT_ELIGIBILITY_PREREQUISITE_MISSING';
    END IF;
END;
$dependencies$;

CREATE OR REPLACE FUNCTION public.learning_outcome_is_eligible_v1(
    p_position_id BIGINT
)
RETURNS BOOLEAN
LANGUAGE SQL
STABLE
STRICT
AS $function$
    SELECT EXISTS (
        SELECT 1
        FROM public.positions position
        JOIN public.canonical_financial_truth_v1 financial_truth
          ON financial_truth.position_id = position.id
        WHERE position.id = p_position_id
          AND position.status = 'CLOSED'
          AND position.exit_time IS NOT NULL
          AND financial_truth.financial_truth_status = 'COMPLETE'
          AND NOT public.learning_outcome_is_excluded_v1(position.id)
    )
$function$;

CREATE OR REPLACE VIEW public.v_learning_eligible_closed_positions_v1 AS
SELECT position.*
FROM public.positions position
WHERE public.learning_outcome_is_eligible_v1(position.id);

CREATE OR REPLACE FUNCTION public.learning_artifact_is_eligible_v1(
    p_position_id BIGINT
)
RETURNS BOOLEAN
LANGUAGE SQL
STABLE
STRICT
AS $function$
    SELECT public.learning_outcome_is_eligible_v1(p_position_id)
$function$;

CREATE OR REPLACE VIEW public.v_learning_eligible_exit_trace_v1 AS
SELECT artifact.* FROM public.exit_trace_v1 artifact
WHERE public.learning_artifact_is_eligible_v1(artifact.position_id);

CREATE OR REPLACE VIEW public.v_learning_eligible_exit_trace_v2 AS
SELECT artifact.* FROM public.exit_trace_v2 artifact
WHERE public.learning_artifact_is_eligible_v1(artifact.position_id);

CREATE OR REPLACE VIEW public.v_learning_eligible_exit_trace_v3 AS
SELECT artifact.* FROM public.exit_trace_v3 artifact
WHERE public.learning_artifact_is_eligible_v1(artifact.position_id);

CREATE OR REPLACE VIEW public.v_learning_eligible_shadow_recommendations_v1 AS
SELECT artifact.*
FROM public.learning_feedback_shadow_recommendations artifact
WHERE public.learning_artifact_is_eligible_v1(artifact.position_id);

CREATE OR REPLACE VIEW public.v_learning_eligible_feature_warehouse_v1 AS
SELECT artifact.* FROM public.learning_feature_warehouse_v1 artifact
WHERE public.learning_artifact_is_eligible_v1(artifact.position_id);

CREATE OR REPLACE VIEW public.v_learning_eligible_decision_replay_v1 AS
SELECT artifact.* FROM public.decision_replay_v1 artifact
WHERE public.learning_artifact_is_eligible_v1(artifact.position_id);

CREATE OR REPLACE VIEW public.v_learning_eligible_decision_registry_v1 AS
SELECT artifact.* FROM public.decision_registry_v1 artifact
WHERE public.learning_artifact_is_eligible_v1(artifact.position_id);

CREATE OR REPLACE VIEW public.v_learning_eligible_decision_outcomes_v1 AS
SELECT artifact.* FROM public.decision_outcomes_v1 artifact
WHERE public.learning_artifact_is_eligible_v1(artifact.position_id);

COMMENT ON FUNCTION public.learning_outcome_is_eligible_v1(BIGINT) IS
    'Canonical Learning boundary: CLOSED, exit timestamp, same-position FT COMPLETE, no active exclusion.';
COMMENT ON VIEW public.v_learning_eligible_closed_positions_v1 IS
    'Canonical FT-complete and active-exclusion-aware Learning outcomes.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260804_learning_ft_eligibility_v1.sql',
    'd5c5b1716de197f0ae6e80b431a3289132448112036311153c481d9b9f20a32c',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'LEARNING_FT_QUARANTINE_V1',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    'c2cef02cbff0c34cef97886f86458ee30020e229',
    'LEARNING_FT_ELIGIBILITY_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id='20260804_learning_ft_eligibility_v1.sql'
);

COMMIT;

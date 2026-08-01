-- WALTRADE LEGACY REPAIR EXISTING ARTIFACT POLICY V1
-- Additive reader boundary only. No artifact update, delete, rebuild or backfill.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $required_objects$
DECLARE
    object_name TEXT;
BEGIN
    FOREACH object_name IN ARRAY ARRAY[
        'learning_outcome_exclusion_v1',
        'exit_trace_v1',
        'exit_trace_v2',
        'exit_trace_v3',
        'learning_feedback_shadow_recommendations',
        'learning_feature_warehouse_v1',
        'decision_replay_v1',
        'decision_registry_v1',
        'decision_outcomes_v1',
        'schema_migration_ledger_v1'
    ] LOOP
        IF to_regclass('public.' || object_name) IS NULL THEN
            RAISE EXCEPTION
                'LEGACY_REPAIR_ARTIFACT_POLICY_REQUIRED_OBJECT_MISSING:%',
                object_name;
        END IF;
    END LOOP;
    IF to_regprocedure(
        'public.learning_outcome_is_excluded_v1(bigint)'
    ) IS NULL THEN
        RAISE EXCEPTION
            'LEGACY_REPAIR_ARTIFACT_POLICY_PREDICATE_MISSING';
    END IF;
END;
$required_objects$;

CREATE OR REPLACE FUNCTION public.learning_artifact_is_eligible_v1(
    p_position_id BIGINT
)
RETURNS BOOLEAN
LANGUAGE SQL
STABLE
STRICT
AS $function$
    SELECT NOT public.learning_outcome_is_excluded_v1(p_position_id)
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

DO $contract_validation$
DECLARE
    missing_objects TEXT;
BEGIN
    WITH required(kind, object_name) AS (
        VALUES
          ('function', 'learning_artifact_is_eligible_v1'),
          ('view', 'v_learning_eligible_exit_trace_v1'),
          ('view', 'v_learning_eligible_exit_trace_v2'),
          ('view', 'v_learning_eligible_exit_trace_v3'),
          ('view', 'v_learning_eligible_shadow_recommendations_v1'),
          ('view', 'v_learning_eligible_feature_warehouse_v1'),
          ('view', 'v_learning_eligible_decision_replay_v1'),
          ('view', 'v_learning_eligible_decision_registry_v1'),
          ('view', 'v_learning_eligible_decision_outcomes_v1')
    ), present AS (
        SELECT 'function'::TEXT AS kind, p.proname::TEXT AS object_name
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname='public'
        UNION ALL
        SELECT 'view', c.relname::TEXT
        FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relkind='v'
    )
    SELECT string_agg(required.kind || ':' || required.object_name, ',')
    INTO missing_objects
    FROM required
    LEFT JOIN present USING(kind, object_name)
    WHERE present.object_name IS NULL;

    IF missing_objects IS NOT NULL THEN
        RAISE EXCEPTION
            'LEGACY_REPAIR_ARTIFACT_POLICY_CONTRACT_MISMATCH:%',
            missing_objects;
    END IF;
END;
$contract_validation$;

COMMENT ON FUNCTION public.learning_artifact_is_eligible_v1(BIGINT) IS
    'Canonical future-Learning predicate; false for quarantined legacy repairs.';
COMMENT ON VIEW public.v_learning_eligible_shadow_recommendations_v1 IS
    'Exclusion-aware shadow source; physical historical artifacts remain unchanged.';
COMMENT ON VIEW public.v_learning_eligible_feature_warehouse_v1 IS
    'Exclusion-aware warehouse source; physical historical artifacts remain unchanged.';
COMMENT ON VIEW public.v_learning_eligible_decision_replay_v1 IS
    'Exclusion-aware replay source; physical historical artifacts remain unchanged.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260801_legacy_repair_existing_artifact_policy_v1.sql',
    '5ee1ef4cc66cf9fac368ce31aa23b2d730b5869bb5d43f3792b8c7689d41e30d',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'LEGACY_REPAIR_ARTIFACT_POLICY_V1',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    'c1bbcdff514d6543e02c07e5b6baccf6e96d65ef',
    'LEGACY_REPAIR_EXISTING_ARTIFACT_POLICY_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id =
          '20260801_legacy_repair_existing_artifact_policy_v1.sql'
);

COMMIT;

-- WALTRADE LEGACY REPAIR LEARNING QUARANTINE V1
-- Additive schema only. No position, order, fill, adoption, or Learning backfill.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $required_objects$
DECLARE
    object_name TEXT;
BEGIN
    FOREACH object_name IN ARRAY ARRAY[
        'positions',
        'exit_trace_v1',
        'exit_trace_v2',
        'exit_trace_v3',
        'learning_feedback_shadow_recommendations',
        'learning_feature_warehouse_v1',
        'decision_replay_v1',
        'decision_registry_v1',
        'decision_outcomes_v1',
        'legacy_repair_audit_v1',
        'legacy_repair_provenance_v1',
        'schema_migration_ledger_v1'
    ] LOOP
        IF to_regclass('public.' || object_name) IS NULL THEN
            RAISE EXCEPTION
                'LEGACY_REPAIR_QUARANTINE_REQUIRED_OBJECT_MISSING:%',
                object_name;
        END IF;
    END LOOP;
    IF to_regprocedure(
        'public.prevent_legacy_recovery_history_mutation_v1()'
    ) IS NULL THEN
        RAISE EXCEPTION
            'LEGACY_REPAIR_QUARANTINE_REQUIRED_FUNCTION_MISSING';
    END IF;
END;
$required_objects$;

CREATE TABLE IF NOT EXISTS public.learning_outcome_exclusion_v1 (
    exclusion_id BIGSERIAL PRIMARY KEY,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    position_id BIGINT NOT NULL,
    exclusion_reason TEXT NOT NULL,
    source_type TEXT NOT NULL,
    semantic_fingerprint_v2 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_by TEXT NOT NULL,
    git_sha TEXT NOT NULL,
    CONSTRAINT ux_learning_outcome_exclusion_v1_identity
        UNIQUE(environment, deployment_id, position_id),
    CONSTRAINT fk_learning_outcome_exclusion_v1_position
        FOREIGN KEY(position_id) REFERENCES public.positions(id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_learning_outcome_exclusion_v1_contract CHECK (
        environment IN ('PAPER', 'LIVE')
        AND btrim(deployment_id) <> ''
        AND exclusion_reason = 'LEGACY_REPAIR'
        AND source_type = 'LEGACY_POSITION_REPAIR'
        AND semantic_fingerprint_v2 ~ '^[0-9a-f]{64}$'
        AND btrim(created_by) <> ''
        AND git_sha ~ '^[0-9a-f]{40}$'
    )
);

CREATE INDEX IF NOT EXISTS ix_learning_outcome_exclusion_v1_position
    ON public.learning_outcome_exclusion_v1(position_id);

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
        FROM public.learning_outcome_exclusion_v1 exclusion
        WHERE exclusion.position_id = p_position_id
    )
$function$;

CREATE OR REPLACE VIEW public.v_learning_eligible_closed_positions_v1 AS
SELECT position.*
FROM public.positions position
WHERE position.status = 'CLOSED'
  AND position.exit_time IS NOT NULL
  AND NOT public.learning_outcome_is_excluded_v1(position.id);

CREATE OR REPLACE FUNCTION public.guard_learning_quarantine_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
BEGIN
    IF NEW.position_id IS NOT NULL
       AND public.learning_outcome_is_excluded_v1(NEW.position_id) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$function$;

DO $guard_triggers$
DECLARE
    table_name TEXT;
    trigger_name TEXT;
BEGIN
    FOR table_name, trigger_name IN
        SELECT * FROM (VALUES
          ('exit_trace_v1', 'trg_lq_exit_trace_v1'),
          ('exit_trace_v2', 'trg_lq_exit_trace_v2'),
          ('exit_trace_v3', 'trg_lq_exit_trace_v3'),
          ('learning_feedback_shadow_recommendations', 'trg_lq_shadow_reco'),
          ('learning_feature_warehouse_v1', 'trg_lq_feature_warehouse'),
          ('decision_replay_v1', 'trg_lq_decision_replay'),
          ('decision_registry_v1', 'trg_lq_decision_registry'),
          ('decision_outcomes_v1', 'trg_lq_decision_outcomes')
        ) AS guards(table_name, trigger_name)
    LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM pg_trigger trigger_row
            JOIN pg_class relation ON relation.oid = trigger_row.tgrelid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'public'
              AND relation.relname = table_name
              AND trigger_row.tgname = trigger_name
              AND NOT trigger_row.tgisinternal
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON public.%I '
                'FOR EACH ROW EXECUTE FUNCTION '
                'public.guard_learning_quarantine_v1()',
                trigger_name, table_name
            );
        END IF;
    END LOOP;
END;
$guard_triggers$;

DO $append_only_trigger$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_learning_outcome_exclusion_v1_append_only'
          AND tgrelid = 'public.learning_outcome_exclusion_v1'::regclass
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_learning_outcome_exclusion_v1_append_only
        BEFORE UPDATE OR DELETE
        ON public.learning_outcome_exclusion_v1
        FOR EACH ROW
        EXECUTE FUNCTION public.prevent_legacy_recovery_history_mutation_v1();
    END IF;
END;
$append_only_trigger$;

DO $contract_validation$
DECLARE
    issues TEXT;
    missing_objects TEXT;
BEGIN
    WITH expected(column_name, data_type, is_nullable) AS (
        VALUES
          ('exclusion_id', 'bigint', 'NO'),
          ('environment', 'text', 'NO'),
          ('deployment_id', 'text', 'NO'),
          ('position_id', 'bigint', 'NO'),
          ('exclusion_reason', 'text', 'NO'),
          ('source_type', 'text', 'NO'),
          ('semantic_fingerprint_v2', 'text', 'NO'),
          ('created_at', 'timestamp with time zone', 'NO'),
          ('created_by', 'text', 'NO'),
          ('git_sha', 'text', 'NO')
    ), actual AS (
        SELECT column_name::TEXT, data_type::TEXT, is_nullable::TEXT
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'learning_outcome_exclusion_v1'
    ), comparison AS (
        SELECT COALESCE(expected.column_name, actual.column_name) AS column_name,
               CASE
                 WHEN expected.column_name IS NULL THEN 'unexpected'
                 WHEN actual.column_name IS NULL THEN 'missing'
                 WHEN expected.data_type <> actual.data_type THEN 'type'
                 WHEN expected.is_nullable <> actual.is_nullable THEN 'nullable'
                 ELSE NULL
               END AS issue
        FROM expected FULL JOIN actual USING(column_name)
    )
    SELECT string_agg(column_name || ':' || issue, ',' ORDER BY column_name)
    INTO issues
    FROM comparison WHERE issue IS NOT NULL;

    IF issues IS NOT NULL THEN
        RAISE EXCEPTION
            'LEGACY_REPAIR_QUARANTINE_COLUMN_CONTRACT_MISMATCH:%', issues;
    END IF;

    WITH required(kind, object_name) AS (
        VALUES
          ('constraint', 'learning_outcome_exclusion_v1_pkey'),
          ('constraint', 'ux_learning_outcome_exclusion_v1_identity'),
          ('constraint', 'fk_learning_outcome_exclusion_v1_position'),
          ('constraint', 'ck_learning_outcome_exclusion_v1_contract'),
          ('index', 'ix_learning_outcome_exclusion_v1_position'),
          ('trigger', 'trg_learning_outcome_exclusion_v1_append_only'),
          ('trigger', 'trg_lq_exit_trace_v1'),
          ('trigger', 'trg_lq_exit_trace_v2'),
          ('trigger', 'trg_lq_exit_trace_v3'),
          ('trigger', 'trg_lq_shadow_reco'),
          ('trigger', 'trg_lq_feature_warehouse'),
          ('trigger', 'trg_lq_decision_replay'),
          ('trigger', 'trg_lq_decision_registry'),
          ('trigger', 'trg_lq_decision_outcomes')
    ), present AS (
        SELECT 'constraint'::TEXT AS kind, conname::TEXT AS object_name
        FROM pg_constraint
        WHERE connamespace='public'::regnamespace
        UNION ALL
        SELECT 'index', indexname::TEXT
        FROM pg_indexes WHERE schemaname='public'
        UNION ALL
        SELECT 'trigger', tgname::TEXT
        FROM pg_trigger
        WHERE NOT tgisinternal
    )
    SELECT string_agg(required.kind || ':' || required.object_name, ',')
    INTO missing_objects
    FROM required
    LEFT JOIN present USING(kind, object_name)
    WHERE present.object_name IS NULL;

    IF missing_objects IS NOT NULL THEN
        RAISE EXCEPTION
            'LEGACY_REPAIR_QUARANTINE_OBJECT_CONTRACT_MISMATCH:%',
            missing_objects;
    END IF;

    IF to_regprocedure(
        'public.learning_outcome_is_excluded_v1(bigint)'
       ) IS NULL
       OR to_regprocedure(
        'public.guard_learning_quarantine_v1()'
       ) IS NULL
       OR to_regclass(
        'public.v_learning_eligible_closed_positions_v1'
       ) IS NULL THEN
        RAISE EXCEPTION
            'LEGACY_REPAIR_QUARANTINE_PREDICATE_CONTRACT_MISMATCH';
    END IF;
END;
$contract_validation$;

COMMENT ON TABLE public.learning_outcome_exclusion_v1 IS
    'Append-only quarantine preventing reconstructed legacy repairs from entering Learning.';
COMMENT ON VIEW public.v_learning_eligible_closed_positions_v1 IS
    'Closed positions eligible for existing Learning ingress after exclusion filtering.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260801_legacy_repair_learning_quarantine_v1.sql',
    'fab46cd6eb55ac7353732834e7144fc26ce2937a442aa244e8a205f3f724d4d0',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'LEGACY_REPAIR_QUARANTINE_V1',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    '2fc6efae2bf2a342ac4ea73968d47432d1a964b5',
    'LEGACY_REPAIR_LEARNING_QUARANTINE_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id =
          '20260801_legacy_repair_learning_quarantine_v1.sql'
);

COMMIT;

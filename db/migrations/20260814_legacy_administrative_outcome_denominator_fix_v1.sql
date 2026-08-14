-- WALTRADE LEGACY ADMINISTRATIVE OUTCOME DENOMINATOR FIX V1
-- Forward-only producer eligibility. Historical artifacts remain immutable.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $prerequisites$
BEGIN
    IF to_regclass('public.positions') IS NULL
       OR to_regclass('public.simulated_orders') IS NULL
       OR to_regclass('public.legacy_repair_provenance_v1') IS NULL
       OR to_regclass('public.decision_registry_v1') IS NULL
       OR to_regclass('public.decision_outcomes_v1') IS NULL
       OR to_regclass('public.schema_migration_ledger_v1') IS NULL
       OR to_regprocedure(
           'public.refresh_decision_identity_outcome_v1(integer,text,text,uuid)'
       ) IS NULL THEN
        RAISE EXCEPTION
            'LEGACY_ADMINISTRATIVE_OUTCOME_DENOMINATOR_V1_PREREQUISITE_MISSING';
    END IF;
END;
$prerequisites$;

CREATE OR REPLACE FUNCTION
public.decision_outcome_is_producer_eligible_v1(p_position_id BIGINT)
RETURNS BOOLEAN
LANGUAGE SQL
STABLE
STRICT
AS $function$
    SELECT NOT EXISTS (
        SELECT 1
        FROM public.positions position
        JOIN public.simulated_orders administrative_order
          ON administrative_order.position_id = position.id
         AND administrative_order.is_exit
         AND administrative_order.order_class =
             'LEGACY_ADMINISTRATIVE_CLOSE'
        JOIN public.legacy_repair_provenance_v1 provenance
          ON provenance.evidence_source =
             'LEGACY_OPEN_POSITION_RETIREMENT'
         AND provenance.immutable_payload #>> '{position,id}' =
             position.id::TEXT
         AND provenance.immutable_payload ->> 'retirement_classification' =
             'LEGACY_ADMINISTRATIVE_CLOSE'
         AND provenance.immutable_payload ->> 'outcome_origin' =
             'LEGACY_ADMINISTRATIVE_CLOSE'
        WHERE position.id = p_position_id
          AND position.exit_reason = 'LEGACY_ADMINISTRATIVE_CLOSE'
    )
$function$;

COMMENT ON FUNCTION
public.decision_outcome_is_producer_eligible_v1(BIGINT) IS
'Canonical Decision Outcome producer predicate. It excludes only proven LEGACY_ADMINISTRATIVE_CLOSE retirements and never infers exclusion from Learning policy.';

DO $patch_producer$
DECLARE
    v_signature CONSTANT TEXT :=
        'public.refresh_decision_identity_outcome_v1(integer,text,text,uuid)';
    v_definition TEXT := pg_get_functiondef(to_regprocedure(v_signature));
    v_old TEXT :=
        'p.entry_time >= clock_timestamp() - make_interval(hours => p_lookback_hours)';
    v_new TEXT :=
        '(p.entry_time >= clock_timestamp() - make_interval(hours => p_lookback_hours)'
        || E'\n'
        || '           AND public.decision_outcome_is_producer_eligible_v1(p.id))';
BEGIN
    IF position('decision_outcome_is_producer_eligible_v1' IN v_definition) = 0 THEN
        IF position(v_old IN v_definition) = 0 THEN
            RAISE EXCEPTION
                'LEGACY_ADMINISTRATIVE_OUTCOME_DENOMINATOR_V1_PRODUCER_ANCHOR_MISSING';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);
        EXECUTE v_definition;
    END IF;
END;
$patch_producer$;

CREATE OR REPLACE VIEW public.v_decision_outcome_coverage_v1 AS
SELECT
    decision.deployment_id,
    decision.environment,
    decision.decision_type,
    count(*) AS decisions,
    count(*) FILTER (WHERE outcome.outcome_id IS NOT NULL) AS outcomes,
    count(*) FILTER (WHERE outcome.outcome_id IS NULL) AS missing_outcomes,
    count(*) FILTER (
        WHERE outcome.outcome_status = 'COMPLETE'
    ) AS complete_outcomes,
    count(*) FILTER (
        WHERE outcome.outcome_status = 'PARTIAL'
    ) AS partial_outcomes
FROM public.decision_registry_v1 decision
LEFT JOIN public.decision_outcomes_v1 outcome
  ON outcome.decision_id = decision.decision_id
WHERE decision.position_id IS NULL
   OR public.decision_outcome_is_producer_eligible_v1(decision.position_id)
GROUP BY decision.deployment_id, decision.environment, decision.decision_type;

COMMENT ON VIEW public.v_decision_outcome_coverage_v1 IS
'Decision Outcome producer/audit denominator using the canonical administrative-retirement eligibility predicate.';

DO $postconditions$
DECLARE
    v_definition TEXT := pg_get_functiondef(
        'public.refresh_decision_identity_outcome_v1(integer,text,text,uuid)'
        ::regprocedure
    );
BEGIN
    IF position('decision_outcome_is_producer_eligible_v1' IN v_definition) = 0
       OR position('learning_outcome_exclusion' IN v_definition) > 0
       OR position(
           'decision_outcome_is_producer_eligible_v1' IN
           pg_get_viewdef('public.v_decision_outcome_coverage_v1'::regclass,true)
       ) = 0 THEN
        RAISE EXCEPTION
            'LEGACY_ADMINISTRATIVE_OUTCOME_DENOMINATOR_V1_POSTCONDITION_FAILED';
    END IF;
END;
$postconditions$;

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260814_legacy_administrative_outcome_denominator_fix_v1.sql',
    COALESCE(
        NULLIF(current_setting('waltrade.migration_checksum',true),''),
        repeat('0',64)
    ),
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'LEGACY_ADMINISTRATIVE_OUTCOME_DENOMINATOR_V1',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    COALESCE(
        NULLIF(current_setting('waltrade.git_sha',true),''),repeat('0',40)
    ),
    'LEGACY_ADMINISTRATIVE_OUTCOME_DENOMINATOR_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id=
      '20260814_legacy_administrative_outcome_denominator_fix_v1.sql'
);

COMMIT;

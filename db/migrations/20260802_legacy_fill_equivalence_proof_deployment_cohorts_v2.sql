-- WALTRADE LEGACY FILL EQUIVALENCE PROOF DEPLOYMENT COHORTS V2
-- Extends the immutable V1 proof row contract to exactly two explicitly
-- enumerated LIVE deployments. This migration performs no backfill.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $dependencies$
DECLARE
    conflicting_checksum TEXT;
    current_definition TEXT;
BEGIN
    IF to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
        RAISE EXCEPTION
            'LEGACY_FILL_EQUIVALENCE_V2_REQUIRED_RELATION_MISSING:%',
            'schema_migration_ledger_v1';
    END IF;
    IF to_regclass('public.legacy_fill_equivalence_proof_v1') IS NULL THEN
        RAISE EXCEPTION
            'LEGACY_FILL_EQUIVALENCE_V2_REQUIRED_RELATION_MISSING:%',
            'legacy_fill_equivalence_proof_v1';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.schema_migration_ledger_v1
        WHERE migration_id='20260802_legacy_fill_equivalence_proof_v1.sql'
          AND success=TRUE
    ) THEN
        RAISE EXCEPTION 'LEGACY_FILL_EQUIVALENCE_V1_MIGRATION_REQUIRED';
    END IF;

    SELECT checksum_sha256 INTO conflicting_checksum
    FROM public.schema_migration_ledger_v1
    WHERE migration_id=
          '20260802_legacy_fill_equivalence_proof_deployment_cohorts_v2.sql'
      AND checksum_sha256<>
          'f1853f9e7b528df24bac05bf2fb58a0ca9333351f7bf4f10f1e73def1ab96312'
    ORDER BY applied_at DESC LIMIT 1;
    IF conflicting_checksum IS NOT NULL THEN
        RAISE EXCEPTION
            'LEGACY_FILL_EQUIVALENCE_V2_LEDGER_CHECKSUM_CONFLICT:%',
            conflicting_checksum;
    END IF;

    SELECT pg_get_constraintdef(oid) INTO current_definition
    FROM pg_constraint
    WHERE conrelid='public.legacy_fill_equivalence_proof_v1'::regclass
      AND conname='ck_legacy_fill_equivalence_contract_v1';
    IF current_definition IS NULL THEN
        RAISE EXCEPTION
            'LEGACY_FILL_EQUIVALENCE_V1_CONSTRAINT_MISSING';
    END IF;
    IF current_definition NOT LIKE
          '%proof_version = ''LEGACY_FILL_EQUIVALENCE_PROOF_V1''::text%'
       OR current_definition NOT LIKE
          '%proof_type = ''LEGACY_CANONICAL_OKX_EQUIVALENCE''::text%'
       OR current_definition NOT LIKE
          '%equivalence_state = ''PROVEN''::text%'
       OR current_definition NOT LIKE
          '%repair_impact = ''NONE''::text%'
    THEN
        RAISE EXCEPTION
            'LEGACY_FILL_EQUIVALENCE_V1_CONSTRAINT_DRIFT:%',
            current_definition;
    END IF;

    IF current_definition LIKE
          '%deployment_id = ''local-live''::text%'
       AND current_definition NOT LIKE '%vps-live%'
    THEN
        ALTER TABLE public.legacy_fill_equivalence_proof_v1
            DROP CONSTRAINT ck_legacy_fill_equivalence_contract_v1;
        ALTER TABLE public.legacy_fill_equivalence_proof_v1
            ADD CONSTRAINT ck_legacy_fill_equivalence_contract_v1 CHECK (
                proof_version='LEGACY_FILL_EQUIVALENCE_PROOF_V1'
                AND environment='LIVE'
                AND deployment_id IN ('local-live', 'vps-live')
                AND source='okx'
                AND btrim(account_identity_key)<>''
                AND symbol=upper(symbol) AND btrim(symbol)<>''
                AND btrim(trade_id)<>''
                AND correction_revision>0
                AND exchange_trade_id=trade_id
                AND proof_type='LEGACY_CANONICAL_OKX_EQUIVALENCE'
                AND equivalence_state='PROVEN'
                AND fill_mutation_required=FALSE
                AND repair_impact='NONE'
                AND entry_or_exit IN ('ENTRY','EXIT')
                AND jsonb_typeof(evidence_payload_json)='object'
                AND btrim(created_by)<>''
                AND git_revision~'^[0-9a-f]{40}$'
                AND latest_observed_fingerprint~'^[0-9a-f]{64}$'
                AND canonical_fill_fingerprint~'^[0-9a-f]{64}$'
                AND okx_truth_fingerprint~'^[0-9a-f]{64}$'
                AND idempotency_key~'^[0-9a-f]{64}$'
            );
    ELSIF current_definition NOT LIKE
          '%deployment_id = ANY (ARRAY[''local-live''::text, ''vps-live''::text])%'
    THEN
        RAISE EXCEPTION
            'LEGACY_FILL_EQUIVALENCE_V2_DEPLOYMENT_CONSTRAINT_DRIFT:%',
            current_definition;
    END IF;

    COMMENT ON CONSTRAINT ck_legacy_fill_equivalence_contract_v1
        ON public.legacy_fill_equivalence_proof_v1 IS
        'LEGACY_FILL_EQUIVALENCE_PROOF_DEPLOYMENT_COHORTS_V2:local-live,vps-live';
END;
$dependencies$;

DO $postcondition$
DECLARE
    current_definition TEXT;
    current_comment TEXT;
BEGIN
    SELECT pg_get_constraintdef(oid),obj_description(oid,'pg_constraint')
    INTO current_definition,current_comment
    FROM pg_constraint
    WHERE conrelid='public.legacy_fill_equivalence_proof_v1'::regclass
      AND conname='ck_legacy_fill_equivalence_contract_v1';
    IF current_definition NOT LIKE
          '%deployment_id = ANY (ARRAY[''local-live''::text, ''vps-live''::text])%'
       OR current_comment IS DISTINCT FROM
          'LEGACY_FILL_EQUIVALENCE_PROOF_DEPLOYMENT_COHORTS_V2:local-live,vps-live'
    THEN
        RAISE EXCEPTION
            'LEGACY_FILL_EQUIVALENCE_V2_POSTCONDITION_FAILED:%:%',
            current_definition,current_comment;
    END IF;
END;
$postcondition$;

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260802_legacy_fill_equivalence_proof_deployment_cohorts_v2.sql',
    'f1853f9e7b528df24bac05bf2fb58a0ca9333351f7bf4f10f1e73def1ab96312',
    'LIVE','local-live,vps-live',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    '74749513cbb0791f71ef39179652ec7996d9a2e1',
    'LEGACY_FILL_EQUIVALENCE_PROOF_DEPLOYMENT_COHORTS_V2'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id=
          '20260802_legacy_fill_equivalence_proof_deployment_cohorts_v2.sql'
);

COMMIT;

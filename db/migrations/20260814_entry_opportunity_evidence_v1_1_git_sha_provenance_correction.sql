-- Explicit operator inputs:
--   SET waltrade.target_environment = 'PAPER';
--   SET waltrade.target_deployment_id = 'LOCAL'; -- or VPS
--   SET waltrade.target_runtime_deployment_id = 'local-paper'; -- or vps-paper
--   SET waltrade.original_git_sha = '<exact stored SHA>';
--   SET waltrade.corrected_git_sha = '<canonical full SHA>';
--   SET waltrade.git_sha = '<correction source revision>';

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $target_contract$
DECLARE
    v_environment text := current_setting('waltrade.target_environment',true);
    v_deployment_id text := current_setting('waltrade.target_deployment_id',true);
    v_runtime_deployment_id text := current_setting(
        'waltrade.target_runtime_deployment_id',true
    );
    v_original_git_sha text := current_setting('waltrade.original_git_sha',true);
    v_corrected_git_sha text := current_setting('waltrade.corrected_git_sha',true);
    v_source_revision text := current_setting('waltrade.git_sha',true);
BEGIN
    IF v_environment IS DISTINCT FROM 'PAPER'
       OR v_deployment_id NOT IN ('LOCAL','VPS')
       OR v_runtime_deployment_id IS DISTINCT FROM (CASE v_deployment_id
           WHEN 'LOCAL' THEN 'local-paper'
           WHEN 'VPS' THEN 'vps-paper'
       END) THEN
        RAISE EXCEPTION
            'ENTRY_OPPORTUNITY_GIT_SHA_CORRECTION_TARGET_NOT_ALLOWED';
    END IF;
    IF COALESCE(v_original_git_sha,'')=''
       OR v_corrected_git_sha !~ '^[0-9a-f]{40}$'
       OR v_source_revision !~ '^[0-9a-f]{40}$'
       OR v_original_git_sha IS NOT DISTINCT FROM v_corrected_git_sha THEN
        RAISE EXCEPTION
            'ENTRY_OPPORTUNITY_GIT_SHA_CORRECTION_INPUT_INVALID';
    END IF;
    IF to_regclass('public.schema_migration_ledger_v1') IS NULL
       OR to_regclass('public.entry_opportunity_evidence_v1') IS NULL THEN
        RAISE EXCEPTION
            'ENTRY_OPPORTUNITY_GIT_SHA_CORRECTION_PREREQUISITE_MISSING';
    END IF;
END
$target_contract$;

CREATE TABLE IF NOT EXISTS public.migration_provenance_correction_v1 (
    correction_id bigserial PRIMARY KEY,
    corrected_ledger_id bigint NOT NULL UNIQUE,
    migration_id text NOT NULL,
    original_environment text NOT NULL,
    original_deployment_id text NOT NULL,
    corrected_environment text NOT NULL,
    corrected_deployment_id text NOT NULL,
    correction_contract text NOT NULL,
    correction_reason text NOT NULL,
    correction_git_sha text NOT NULL,
    corrected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    original_git_sha text,
    corrected_git_sha text,
    runtime_deployment_id text
);

ALTER TABLE public.migration_provenance_correction_v1
    ADD COLUMN IF NOT EXISTS original_git_sha text,
    ADD COLUMN IF NOT EXISTS corrected_git_sha text,
    ADD COLUMN IF NOT EXISTS runtime_deployment_id text;

DO $contract_constraint$
DECLARE
    item record;
BEGIN
    FOR item IN
        SELECT conname
          FROM pg_constraint
         WHERE conrelid='public.migration_provenance_correction_v1'::regclass
           AND contype='c'
           AND pg_get_constraintdef(oid) ILIKE '%correction_contract%'
    LOOP
        EXECUTE format(
            'ALTER TABLE public.migration_provenance_correction_v1 DROP CONSTRAINT %I',
            item.conname
        );
    END LOOP;
END
$contract_constraint$;

ALTER TABLE public.migration_provenance_correction_v1
    ADD CONSTRAINT migration_provenance_correction_contract_v1_ck CHECK (
        correction_contract IN (
            'PAPER_ECONOMIC_TRUTH_DEPLOYMENT_PORTABILITY_V1',
            'ENTRY_OPPORTUNITY_EVIDENCE_V1_1_GIT_SHA_PROVENANCE_CORRECTION'
        )
    ),
    ADD CONSTRAINT migration_provenance_git_sha_correction_v1_ck CHECK (
        correction_contract <>
            'ENTRY_OPPORTUNITY_EVIDENCE_V1_1_GIT_SHA_PROVENANCE_CORRECTION'
        OR (
            correction_reason =
                'NON_CANONICAL_GIT_SHA_PROVENANCE_CORRECTION'
            AND original_git_sha IS NOT NULL
            AND original_git_sha <> ''
            AND corrected_git_sha ~ '^[0-9a-f]{40}$'
            AND original_git_sha <> corrected_git_sha
            AND runtime_deployment_id = (CASE corrected_deployment_id
                WHEN 'LOCAL' THEN 'local-paper'
                WHEN 'VPS' THEN 'vps-paper'
            END)
        )
    );

CREATE OR REPLACE FUNCTION
public.reject_migration_provenance_correction_mutation_v1()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
    RAISE EXCEPTION 'MIGRATION_PROVENANCE_CORRECTION_V1_IMMUTABLE';
END
$function$;

DROP TRIGGER IF EXISTS trg_migration_provenance_correction_v1_immutable
    ON public.migration_provenance_correction_v1;
CREATE TRIGGER trg_migration_provenance_correction_v1_immutable
BEFORE UPDATE OR DELETE ON public.migration_provenance_correction_v1
FOR EACH ROW EXECUTE FUNCTION
    public.reject_migration_provenance_correction_mutation_v1();

INSERT INTO public.migration_provenance_correction_v1(
    corrected_ledger_id,migration_id,
    original_environment,original_deployment_id,
    corrected_environment,corrected_deployment_id,
    correction_contract,correction_reason,correction_git_sha,
    original_git_sha,corrected_git_sha,runtime_deployment_id
)
SELECT
    ledger.ledger_id,ledger.migration_id,
    ledger.environment,ledger.deployment_id,
    ledger.environment,ledger.deployment_id,
    'ENTRY_OPPORTUNITY_EVIDENCE_V1_1_GIT_SHA_PROVENANCE_CORRECTION',
    'NON_CANONICAL_GIT_SHA_PROVENANCE_CORRECTION',
    current_setting('waltrade.git_sha'),
    ledger.git_sha,current_setting('waltrade.corrected_git_sha'),
    current_setting('waltrade.target_runtime_deployment_id')
FROM public.schema_migration_ledger_v1 ledger
WHERE ledger.migration_id=
        '20260814_entry_opportunity_evidence_v1_1_portability.sql'
  AND ledger.checksum_sha256=
        'd95e976b434cde3facb7e35cc3e6bd05aa64d1d9248e3f9411e235ee58509c50'
  AND ledger.environment=current_setting('waltrade.target_environment')
  AND ledger.deployment_id=current_setting('waltrade.target_deployment_id')
  AND ledger.git_sha=current_setting('waltrade.original_git_sha')
ON CONFLICT (corrected_ledger_id) DO NOTHING;

DO $postcondition$
DECLARE
    v_candidate_count integer;
    v_affected_count integer;
    v_correction_count integer;
    v_unexpected_count integer;
    v_effective_git_sha text;
BEGIN
    SELECT count(*) INTO v_candidate_count
      FROM public.schema_migration_ledger_v1
     WHERE migration_id=
            '20260814_entry_opportunity_evidence_v1_1_portability.sql'
       AND environment=current_setting('waltrade.target_environment')
       AND deployment_id=current_setting('waltrade.target_deployment_id');

    SELECT count(*) INTO v_affected_count
      FROM public.schema_migration_ledger_v1
     WHERE migration_id=
            '20260814_entry_opportunity_evidence_v1_1_portability.sql'
       AND checksum_sha256=
            'd95e976b434cde3facb7e35cc3e6bd05aa64d1d9248e3f9411e235ee58509c50'
       AND environment=current_setting('waltrade.target_environment')
       AND deployment_id=current_setting('waltrade.target_deployment_id')
       AND git_sha=current_setting('waltrade.original_git_sha');

    SELECT count(*) INTO v_unexpected_count
      FROM public.schema_migration_ledger_v1
     WHERE migration_id=
            '20260814_entry_opportunity_evidence_v1_1_portability.sql'
       AND environment=current_setting('waltrade.target_environment')
       AND deployment_id=current_setting('waltrade.target_deployment_id')
       AND git_sha NOT IN (
            current_setting('waltrade.original_git_sha'),
            current_setting('waltrade.corrected_git_sha')
       );

    IF v_candidate_count > 1 OR v_affected_count > 1 OR v_unexpected_count > 0 THEN
        RAISE EXCEPTION
            'ENTRY_OPPORTUNITY_GIT_SHA_CORRECTION_AMBIGUOUS_TARGET';
    END IF;

    SELECT count(*) INTO v_correction_count
      FROM public.migration_provenance_correction_v1 correction
      JOIN public.schema_migration_ledger_v1 ledger
        ON ledger.ledger_id=correction.corrected_ledger_id
     WHERE ledger.migration_id=
            '20260814_entry_opportunity_evidence_v1_1_portability.sql'
       AND ledger.environment=current_setting('waltrade.target_environment')
       AND ledger.deployment_id=current_setting('waltrade.target_deployment_id')
       AND correction.correction_contract=
            'ENTRY_OPPORTUNITY_EVIDENCE_V1_1_GIT_SHA_PROVENANCE_CORRECTION';

    IF v_affected_count=0 AND v_correction_count<>0 THEN
        RAISE EXCEPTION
            'ENTRY_OPPORTUNITY_GIT_SHA_CORRECTION_NOT_APPLICABLE_HAS_RECORD';
    ELSIF v_affected_count=1 AND v_correction_count<>1 THEN
        RAISE EXCEPTION
            'ENTRY_OPPORTUNITY_GIT_SHA_CORRECTION_POSTCONDITION_FAILED';
    END IF;

    IF v_affected_count=1 THEN
        SELECT COALESCE(correction.corrected_git_sha,ledger.git_sha)
          INTO v_effective_git_sha
          FROM public.schema_migration_ledger_v1 ledger
          LEFT JOIN public.migration_provenance_correction_v1 correction
            ON correction.corrected_ledger_id=ledger.ledger_id
           AND correction.correction_contract=
                'ENTRY_OPPORTUNITY_EVIDENCE_V1_1_GIT_SHA_PROVENANCE_CORRECTION'
         WHERE ledger.migration_id=
                '20260814_entry_opportunity_evidence_v1_1_portability.sql'
           AND ledger.environment=current_setting('waltrade.target_environment')
           AND ledger.deployment_id=current_setting('waltrade.target_deployment_id');
        IF v_effective_git_sha IS DISTINCT FROM
           current_setting('waltrade.corrected_git_sha') THEN
            RAISE EXCEPTION
                'ENTRY_OPPORTUNITY_EFFECTIVE_GIT_SHA_POSTCONDITION_FAILED';
        END IF;
    END IF;
END
$postcondition$;

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260814_entry_opportunity_evidence_v1_1_git_sha_provenance_correction.sql',
    COALESCE(
        NULLIF(current_setting('waltrade.migration_checksum',true),''),
        repeat('0',64)
    ),
    'PAPER',current_setting('waltrade.target_deployment_id'),
    current_database(),'operator-migration','APPLIED',TRUE,0,
    current_setting('waltrade.git_sha'),
    'ENTRY_OPPORTUNITY_EVIDENCE_V1_1_GIT_SHA_PROVENANCE_CORRECTION'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
     WHERE migration_id=
        '20260814_entry_opportunity_evidence_v1_1_git_sha_provenance_correction.sql'
);

COMMIT;

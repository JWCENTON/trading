-- Required explicit installation identity:
--   SET waltrade.target_environment = 'PAPER';
--   SET waltrade.target_deployment_id = 'LOCAL'; -- or VPS
--   SET waltrade.target_runtime_deployment_id = 'local-paper'; -- or vps-paper
-- The fresh-install wrapper first installs the unchanged V1 schema source and
-- records its ledger row with this identity. Existing LOCAL V1 installations
-- enter here directly and remain untouched.

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $target_contract$
DECLARE
    v_environment text := current_setting(
        'waltrade.target_environment', true
    );
    v_deployment_id text := current_setting(
        'waltrade.target_deployment_id', true
    );
    v_runtime_deployment_id text := current_setting(
        'waltrade.target_runtime_deployment_id', true
    );
    v_expected_runtime_deployment_id text;
    v_original_ledger_count integer;
    v_original_checksum text;
    v_original_environment text;
    v_original_deployment_id text;
BEGIN
    IF v_environment IS DISTINCT FROM 'PAPER'
       OR v_deployment_id NOT IN ('LOCAL','VPS') THEN
        RAISE EXCEPTION
            'ENTRY_OPPORTUNITY_PORTABILITY_TARGET_NOT_ALLOWED: environment=% deployment_id=%',
            COALESCE(v_environment, '<missing>'),
            COALESCE(v_deployment_id, '<missing>');
    END IF;

    v_expected_runtime_deployment_id := CASE v_deployment_id
        WHEN 'LOCAL' THEN 'local-paper'
        WHEN 'VPS' THEN 'vps-paper'
    END;
    IF v_runtime_deployment_id IS DISTINCT FROM
       v_expected_runtime_deployment_id THEN
        RAISE EXCEPTION
            'ENTRY_OPPORTUNITY_RUNTIME_DEPLOYMENT_MISMATCH: ledger=% runtime=%',
            v_deployment_id,
            COALESCE(v_runtime_deployment_id, '<missing>');
    END IF;

    IF to_regclass('public.entry_opportunity_evidence_v1') IS NULL
       OR to_regclass('public.entry_opportunity_evidence_audit_v1') IS NULL
       OR to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
        RAISE EXCEPTION 'ENTRY_OPPORTUNITY_EVIDENCE_V1_SCHEMA_MISSING';
    END IF;

    SELECT count(*),min(checksum_sha256),min(environment),min(deployment_id)
      INTO v_original_ledger_count,v_original_checksum,
           v_original_environment,v_original_deployment_id
      FROM public.schema_migration_ledger_v1
     WHERE migration_id='20260814_entry_opportunity_evidence_v1.sql';

    IF v_original_ledger_count IS DISTINCT FROM 1
       OR v_original_checksum IS DISTINCT FROM
          'ed6f0bd1f0ac22a0e540b960319a117e3850a858907d85b300e613677c28576d'
       OR v_original_environment IS DISTINCT FROM 'PAPER'
       OR v_original_deployment_id IS DISTINCT FROM v_deployment_id THEN
        RAISE EXCEPTION
            'ENTRY_OPPORTUNITY_ORIGINAL_V1_PROVENANCE_INVALID: count=% checksum=% environment=% deployment_id=%',
            v_original_ledger_count,
            COALESCE(v_original_checksum, '<missing>'),
            COALESCE(v_original_environment, '<missing>'),
            COALESCE(v_original_deployment_id, '<missing>');
    END IF;
END
$target_contract$;

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260814_entry_opportunity_evidence_v1_1_portability.sql',
    COALESCE(
        NULLIF(current_setting('waltrade.migration_checksum',true),''),
        repeat('0',64)
    ),
    'PAPER',current_setting('waltrade.target_deployment_id'),
    current_database(),'operator-migration','APPLIED',TRUE,0,
    COALESCE(
        NULLIF(current_setting('waltrade.git_sha',true),''),repeat('0',40)
    ),
    'ENTRY_OPPORTUNITY_EVIDENCE_V1_1_PORTABILITY'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
     WHERE migration_id=
        '20260814_entry_opportunity_evidence_v1_1_portability.sql'
);

COMMIT;

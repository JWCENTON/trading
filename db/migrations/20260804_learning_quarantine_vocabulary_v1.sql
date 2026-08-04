-- WALTRADE LEARNING QUARANTINE VOCABULARY V1
-- Additive provenance and containment vocabulary; no historical row mutation.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $dependencies$
BEGIN
    IF to_regclass('public.learning_outcome_exclusion_v1') IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_QUARANTINE_VOCABULARY_REQUIRED_RELATION_MISSING';
    END IF;
    IF to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_QUARANTINE_VOCABULARY_LEDGER_MISSING';
    END IF;
END;
$dependencies$;

ALTER TABLE public.learning_outcome_exclusion_v1
    ADD COLUMN IF NOT EXISTS source_reference TEXT,
    ADD COLUMN IF NOT EXISTS detail_json JSONB NOT NULL DEFAULT '{}'::JSONB;

ALTER TABLE public.learning_outcome_exclusion_v1
    DROP CONSTRAINT IF EXISTS ck_learning_outcome_exclusion_v1_contract;

ALTER TABLE public.learning_outcome_exclusion_v1
    ADD CONSTRAINT ck_learning_outcome_exclusion_v1_contract CHECK (
        environment IN ('PAPER', 'LIVE')
        AND btrim(deployment_id) <> ''
        AND exclusion_reason IN (
            'LEGACY_REPAIR',
            'FINANCIAL_TRUTH_INCOMPLETE',
            'INVENTORY_ACCOUNT_MISMATCH',
            'EVIDENCE_PROVENANCE_INCOMPLETE'
        )
        AND source_type IN (
            'LEGACY_POSITION_REPAIR',
            'FINANCIAL_TRUTH_CONTAINMENT',
            'INVENTORY_OWNERSHIP_AUDIT',
            'MANUAL_GOVERNANCE_DECISION'
        )
        AND semantic_fingerprint_v2 ~ '^[0-9a-f]{64}$'
        AND (source_reference IS NULL OR btrim(source_reference) <> '')
        AND jsonb_typeof(detail_json) = 'object'
        AND btrim(created_by) <> ''
        AND git_sha ~ '^[0-9a-f]{40}$'
    );

COMMENT ON COLUMN public.learning_outcome_exclusion_v1.source_reference IS
    'Optional immutable external or governance provenance reference.';
COMMENT ON COLUMN public.learning_outcome_exclusion_v1.detail_json IS
    'Immutable structured provenance for the exclusion decision.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260804_learning_quarantine_vocabulary_v1.sql',
    '22a677377437db37ac77a4ad955e6ad5b530953d22c6821c9d4f86d4f2b6e767',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'LEARNING_FT_QUARANTINE_V1',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    'c2cef02cbff0c34cef97886f86458ee30020e229',
    'LEARNING_QUARANTINE_VOCABULARY_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id='20260804_learning_quarantine_vocabulary_v1.sql'
);

COMMIT;

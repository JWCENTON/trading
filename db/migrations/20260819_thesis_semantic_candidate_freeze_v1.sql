BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $prerequisites$
BEGIN
    IF to_regclass('public.schema_migration_ledger_v1') IS NULL
       OR to_regclass('public.thesis_evidence_bundle_v1') IS NULL
       OR to_regclass('public.thesis_structural_observation_v1') IS NULL
       OR to_regclass('public.thesis_mme_sequence_observation_v1') IS NULL THEN
        RAISE EXCEPTION 'THESIS_SEMANTIC_CANDIDATE_FREEZE_V1_PREREQUISITE_MISSING';
    END IF;
END
$prerequisites$;

CREATE TABLE IF NOT EXISTS public.thesis_semantic_candidate_freeze_v1 (
    freeze_id TEXT PRIMARY KEY,
    contract_version TEXT NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    git_revision TEXT NOT NULL,
    forming_rule_id TEXT NOT NULL,
    forming_rule_version TEXT NOT NULL,
    forming_rule_fingerprint TEXT NOT NULL,
    active_candidate_rule_id TEXT NOT NULL,
    active_candidate_rule_version TEXT NOT NULL,
    active_candidate_rule_fingerprint TEXT NOT NULL,
    freeze_fingerprint TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT thesis_semantic_freeze_id_ck CHECK (
        freeze_id ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_semantic_freeze_contract_ck CHECK (
        contract_version = 'THESIS_SEMANTIC_CANDIDATE_FREEZE_V1'
    ),
    CONSTRAINT thesis_semantic_freeze_forming_rule_ck CHECK (
        forming_rule_id = 'THESIS_FORMING_ALIGNMENT_V1'
        AND forming_rule_version = 'V1'
    ),
    CONSTRAINT thesis_semantic_freeze_active_rule_ck CHECK (
        active_candidate_rule_id = 'THESIS_ACTIVE_ADJACENT_COHERENCE_V1'
        AND active_candidate_rule_version = 'V1'
    ),
    CONSTRAINT thesis_semantic_freeze_fingerprints_ck CHECK (
        forming_rule_fingerprint ~ '^[0-9a-f]{64}$'
        AND active_candidate_rule_fingerprint ~ '^[0-9a-f]{64}$'
        AND freeze_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_semantic_freeze_git_revision_ck CHECK (
        git_revision ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'
    ),
    CONSTRAINT thesis_semantic_freeze_identity_uk UNIQUE (
        contract_version,environment,deployment_id
    )
);

CREATE TABLE IF NOT EXISTS public.thesis_semantic_candidate_observation_v1 (
    evaluation_id TEXT PRIMARY KEY,
    freeze_id TEXT NOT NULL
        REFERENCES public.thesis_semantic_candidate_freeze_v1(freeze_id)
        ON DELETE RESTRICT,
    contract_version TEXT NOT NULL,
    candidate_rule_id TEXT NOT NULL,
    candidate_rule_version TEXT NOT NULL,
    rule_fingerprint TEXT NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT,
    from_state TEXT NOT NULL,
    candidate_to_state TEXT NOT NULL,
    evaluation_result TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    evidence_bundle_id TEXT NOT NULL
        REFERENCES public.thesis_evidence_bundle_v1(bundle_id)
        ON DELETE RESTRICT,
    previous_bundle_id TEXT
        REFERENCES public.thesis_evidence_bundle_v1(bundle_id)
        ON DELETE RESTRICT,
    evidence_cutoff TIMESTAMPTZ NOT NULL,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    git_revision TEXT NOT NULL,
    candidate_fingerprint TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT thesis_semantic_evaluation_id_ck CHECK (
        evaluation_id ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_semantic_observation_contract_ck CHECK (
        contract_version = 'THESIS_SEMANTIC_CANDIDATE_OBSERVATION_V1'
    ),
    CONSTRAINT thesis_semantic_rule_identity_ck CHECK (
        (candidate_rule_id = 'THESIS_FORMING_ALIGNMENT_V1'
         AND candidate_rule_version = 'V1'
         AND from_state = 'NO_THESIS'
         AND candidate_to_state = 'FORMING')
        OR
        (candidate_rule_id = 'THESIS_ACTIVE_ADJACENT_COHERENCE_V1'
         AND candidate_rule_version = 'V1'
         AND from_state = 'FORMING'
         AND candidate_to_state = 'ACTIVE_CANDIDATE')
    ),
    CONSTRAINT thesis_semantic_direction_ck CHECK (
        direction IS NULL OR direction IN ('UP','DOWN')
    ),
    CONSTRAINT thesis_semantic_evaluation_result_ck CHECK (
        evaluation_result IN ('MATCH','NO_MATCH','EVIDENCE_INCOMPLETE')
    ),
    CONSTRAINT thesis_semantic_observation_fingerprints_ck CHECK (
        rule_fingerprint ~ '^[0-9a-f]{64}$'
        AND candidate_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_semantic_observation_git_revision_ck CHECK (
        git_revision ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'
    ),
    CONSTRAINT thesis_semantic_evaluation_identity_uk UNIQUE (
        candidate_rule_id,candidate_rule_version,evidence_bundle_id
    )
);

CREATE INDEX IF NOT EXISTS ix_thesis_semantic_candidate_symbol_v1
    ON public.thesis_semantic_candidate_observation_v1(
        environment,deployment_id,symbol,evidence_cutoff DESC,candidate_rule_id
    );

DO $append_only_triggers$
DECLARE
    relation_name text;
    trigger_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'thesis_semantic_candidate_freeze_v1',
        'thesis_semantic_candidate_observation_v1'
    ]
    LOOP
        trigger_name := 'trg_' || relation_name || '_append_only';
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON public.%I',
                       trigger_name,relation_name);
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON public.%I '
            'FOR EACH ROW EXECUTE FUNCTION public.guard_thesis_evidence_append_only_v1()',
            trigger_name,relation_name
        );
        EXECUTE format('REVOKE UPDATE, DELETE ON public.%I FROM PUBLIC',relation_name);
    END LOOP;
END
$append_only_triggers$;

CREATE OR REPLACE VIEW public.v_thesis_semantic_candidate_current_v1 AS
WITH symbols AS (
    SELECT DISTINCT environment,deployment_id,symbol
    FROM public.thesis_semantic_candidate_observation_v1
),
forming AS (
    SELECT DISTINCT ON (environment,deployment_id,symbol)
        environment,deployment_id,symbol,direction,evidence_cutoff,
        evidence_bundle_id,evaluation_id
    FROM public.thesis_semantic_candidate_observation_v1
    WHERE candidate_rule_id='THESIS_FORMING_ALIGNMENT_V1'
      AND evaluation_result='MATCH'
    ORDER BY environment,deployment_id,symbol,evidence_cutoff,created_at
),
latest_complete_active AS (
    SELECT DISTINCT ON (o.environment,o.deployment_id,o.symbol)
        o.environment,o.deployment_id,o.symbol,o.evaluation_result,
        o.evidence_cutoff,o.evidence_bundle_id,o.evaluation_id
    FROM public.thesis_semantic_candidate_observation_v1 o
    JOIN public.thesis_evidence_bundle_v1 b
      ON b.bundle_id=o.evidence_bundle_id
    WHERE o.candidate_rule_id='THESIS_ACTIVE_ADJACENT_COHERENCE_V1'
      AND b.evidence_status='COMPLETE'
    ORDER BY o.environment,o.deployment_id,o.symbol,
             o.evidence_cutoff DESC,o.created_at DESC
)
SELECT
    s.environment,s.deployment_id,s.symbol,
    CASE
        WHEN f.evaluation_id IS NULL THEN 'NO_THESIS'
        WHEN a.evaluation_result='MATCH' THEN 'ACTIVE_CANDIDATE'
        ELSE 'FORMING'
    END AS candidate_state,
    f.direction AS forming_direction,
    COALESCE(a.evidence_cutoff,f.evidence_cutoff) AS state_evidence_cutoff,
    COALESCE(a.evidence_bundle_id,f.evidence_bundle_id) AS evidence_bundle_id,
    a.evaluation_id AS active_candidate_evaluation_id,
    f.evaluation_id AS forming_evaluation_id
FROM symbols s
LEFT JOIN forming f USING(environment,deployment_id,symbol)
LEFT JOIN latest_complete_active a USING(environment,deployment_id,symbol);

COMMENT ON TABLE public.thesis_semantic_candidate_freeze_v1 IS
    'Prospective immutable rule freeze; environment and deployment are provenance only.';
COMMENT ON TABLE public.thesis_semantic_candidate_observation_v1 IS
    'Append-only shadow research observations with zero trading or campaign authority.';
COMMENT ON VIEW public.v_thesis_semantic_candidate_current_v1 IS
    'Audit-only current candidate projection; ACTIVE_CANDIDATE is never trading ACTIVE.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260819_thesis_semantic_candidate_freeze_v1.sql',
    COALESCE(NULLIF(current_setting('waltrade.migration_checksum',true),''),repeat('0',64)),
    COALESCE(NULLIF(current_setting('waltrade.target_environment',true),''),'UNKNOWN'),
    COALESCE(NULLIF(current_setting('waltrade.target_deployment_id',true),''),'UNKNOWN'),
    current_database(),'operator-migration','APPLIED',TRUE,0,
    COALESCE(NULLIF(current_setting('waltrade.git_sha',true),''),repeat('0',40)),
    'THESIS_SEMANTIC_CANDIDATE_FREEZE_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id='20260819_thesis_semantic_candidate_freeze_v1.sql'
);

COMMIT;

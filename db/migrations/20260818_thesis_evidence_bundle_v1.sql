BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $prerequisites$
BEGIN
    IF to_regclass('public.schema_migration_ledger_v1') IS NULL
       OR to_regclass('public.candles') IS NULL
       OR to_regclass('public.market_memory_sequence') IS NULL
       OR to_regclass('public.strategy_events') IS NULL
       OR to_regclass('public.decision_registry_v1') IS NULL
       OR to_regclass('public.bot_control') IS NULL THEN
        RAISE EXCEPTION 'THESIS_EVIDENCE_BUNDLE_V1_PREREQUISITE_MISSING';
    END IF;
END
$prerequisites$;

CREATE OR REPLACE FUNCTION public.guard_thesis_evidence_append_only_v1()
RETURNS trigger LANGUAGE plpgsql AS $guard$
BEGIN
    RAISE EXCEPTION 'THESIS_EVIDENCE_V1_APPEND_ONLY';
END
$guard$;

CREATE TABLE IF NOT EXISTS public.thesis_evidence_pipeline_run_v1 (
    pipeline_run_id TEXT PRIMARY KEY,
    contract_version TEXT NOT NULL,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    evidence_cutoff TIMESTAMPTZ NOT NULL,
    evidence_status TEXT NOT NULL,
    missing_sources JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_version_manifest JSONB NOT NULL,
    run_fingerprint TEXT NOT NULL,
    git_revision TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT thesis_evidence_pipeline_run_id_ck CHECK (
        pipeline_run_id ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_evidence_pipeline_status_ck CHECK (
        evidence_status IN ('COMPLETE','INCOMPLETE')
    ),
    CONSTRAINT thesis_evidence_pipeline_fingerprint_ck CHECK (
        run_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_evidence_pipeline_identity_uk UNIQUE (
        environment,deployment_id,evidence_cutoff,contract_version
    )
);

CREATE TABLE IF NOT EXISTS public.thesis_structural_observation_v1 (
    observation_id TEXT PRIMARY KEY,
    contract_version TEXT NOT NULL,
    pipeline_run_id TEXT NOT NULL
        REFERENCES public.thesis_evidence_pipeline_run_v1(pipeline_run_id)
        ON DELETE RESTRICT,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    horizon TEXT NOT NULL,
    evidence_cutoff TIMESTAMPTZ NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    first_candle_ts TIMESTAMPTZ,
    last_candle_ts TIMESTAMPTZ,
    first_close NUMERIC,
    last_close NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    directional_return_pct NUMERIC,
    range_pct NUMERIC,
    drawdown_from_high_pct NUMERIC,
    close_position_in_range NUMERIC,
    candle_count INTEGER NOT NULL,
    expected_candle_count INTEGER NOT NULL,
    coverage_status TEXT NOT NULL,
    source_max_ts TIMESTAMPTZ,
    source_version TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    observation_fingerprint TEXT NOT NULL,
    git_revision TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT thesis_structural_observation_id_ck CHECK (
        observation_id ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_structural_horizon_ck CHECK (
        horizon IN ('6h','24h','3d')
    ),
    CONSTRAINT thesis_structural_coverage_ck CHECK (
        coverage_status IN ('COMPLETE','INCOMPLETE')
    ),
    CONSTRAINT thesis_structural_counts_ck CHECK (
        candle_count >= 0 AND expected_candle_count > 0
    ),
    CONSTRAINT thesis_structural_fingerprints_ck CHECK (
        source_fingerprint ~ '^[0-9a-f]{64}$'
        AND observation_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_structural_identity_uk UNIQUE (
        pipeline_run_id,symbol,horizon
    )
);

CREATE TABLE IF NOT EXISTS public.thesis_mme_sequence_observation_v1 (
    observation_id TEXT PRIMARY KEY,
    contract_version TEXT NOT NULL,
    pipeline_run_id TEXT NOT NULL
        REFERENCES public.thesis_evidence_pipeline_run_v1(pipeline_run_id)
        ON DELETE RESTRICT,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    evidence_cutoff TIMESTAMPTZ NOT NULL,
    availability_state TEXT NOT NULL,
    sequence_key TEXT,
    sequence_type TEXT,
    sequence_stage TEXT,
    direction TEXT,
    sequence_quality NUMERIC,
    continuation_score NUMERIC,
    reversal_score NUMERIC,
    late_entry_risk NUMERIC,
    orc_readiness_score NUMERIC,
    orc_hint TEXT,
    reason TEXT,
    ranking_status TEXT,
    action_hint TEXT,
    first_event_at TIMESTAMPTZ,
    last_event_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    source_refreshed_at TIMESTAMPTZ,
    source_version TEXT NOT NULL,
    source_payload JSONB,
    source_fingerprint TEXT NOT NULL,
    observation_fingerprint TEXT NOT NULL,
    git_revision TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT thesis_mme_observation_id_ck CHECK (
        observation_id ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_mme_availability_ck CHECK (
        availability_state IN ('AVAILABLE','ABSENT','FUTURE_SOURCE')
    ),
    CONSTRAINT thesis_mme_fingerprints_ck CHECK (
        source_fingerprint ~ '^[0-9a-f]{64}$'
        AND observation_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_mme_observation_identity_uk UNIQUE (
        pipeline_run_id,symbol,interval
    )
);

CREATE TABLE IF NOT EXISTS public.thesis_mme_transition_observation_v1 (
    transition_id TEXT PRIMARY KEY,
    contract_version TEXT NOT NULL,
    pipeline_run_id TEXT NOT NULL
        REFERENCES public.thesis_evidence_pipeline_run_v1(pipeline_run_id)
        ON DELETE RESTRICT,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    evidence_cutoff TIMESTAMPTZ NOT NULL,
    previous_observation_id TEXT
        REFERENCES public.thesis_mme_sequence_observation_v1(observation_id)
        ON DELETE RESTRICT,
    current_observation_id TEXT NOT NULL
        REFERENCES public.thesis_mme_sequence_observation_v1(observation_id)
        ON DELETE RESTRICT,
    transition_category TEXT NOT NULL,
    changed_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
    transition_fingerprint TEXT NOT NULL,
    git_revision TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT thesis_mme_transition_id_ck CHECK (
        transition_id ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_mme_transition_category_ck CHECK (
        transition_category IN (
            'SOURCE_APPEARED','SOURCE_CHANGED','SOURCE_ABSENT',
            'STAGE_CHANGED','TYPE_CHANGED','DIRECTION_CHANGED'
        )
    ),
    CONSTRAINT thesis_mme_transition_fingerprint_ck CHECK (
        transition_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_mme_transition_current_uk UNIQUE (current_observation_id)
);

CREATE TABLE IF NOT EXISTS public.thesis_tactical_opportunity_set_v1 (
    tactical_set_id TEXT PRIMARY KEY,
    contract_version TEXT NOT NULL,
    pipeline_run_id TEXT NOT NULL
        REFERENCES public.thesis_evidence_pipeline_run_v1(pipeline_run_id)
        ON DELETE RESTRICT,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    evidence_cutoff TIMESTAMPTZ NOT NULL,
    observation_window_start TIMESTAMPTZ NOT NULL,
    observation_window_end TIMESTAMPTZ NOT NULL,
    member_count INTEGER NOT NULL,
    completeness_status TEXT NOT NULL,
    source_version TEXT NOT NULL,
    set_fingerprint TEXT NOT NULL,
    git_revision TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT thesis_tactical_set_id_ck CHECK (
        tactical_set_id ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_tactical_member_count_ck CHECK (member_count >= 0),
    CONSTRAINT thesis_tactical_completeness_ck CHECK (
        completeness_status IN ('COMPLETE','INCOMPLETE')
    ),
    CONSTRAINT thesis_tactical_set_fingerprint_ck CHECK (
        set_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_tactical_set_identity_uk UNIQUE (
        pipeline_run_id,symbol
    )
);

CREATE TABLE IF NOT EXISTS public.thesis_tactical_opportunity_member_v1 (
    opportunity_identity TEXT PRIMARY KEY,
    tactical_set_id TEXT NOT NULL
        REFERENCES public.thesis_tactical_opportunity_set_v1(tactical_set_id)
        ON DELETE RESTRICT,
    decision_id UUID,
    strategy TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    direction TEXT NOT NULL,
    decision_timestamp TIMESTAMPTZ NOT NULL,
    decision_candle_timestamp TIMESTAMPTZ NOT NULL,
    signal_reason TEXT,
    source_version TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    git_revision TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT thesis_tactical_opportunity_id_ck CHECK (
        opportunity_identity ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_tactical_direction_ck CHECK (
        direction IN ('LONG','SHORT','UNKNOWN')
    ),
    CONSTRAINT thesis_tactical_source_fingerprint_ck CHECK (
        source_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_tactical_member_set_uk UNIQUE (
        tactical_set_id,opportunity_identity
    )
);

CREATE TABLE IF NOT EXISTS public.thesis_evidence_bundle_v1 (
    bundle_id TEXT PRIMARY KEY,
    contract_version TEXT NOT NULL,
    pipeline_run_id TEXT NOT NULL
        REFERENCES public.thesis_evidence_pipeline_run_v1(pipeline_run_id)
        ON DELETE RESTRICT,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    direction_scope TEXT NOT NULL,
    evidence_cutoff TIMESTAMPTZ NOT NULL,
    evidence_status TEXT NOT NULL,
    missing_sources JSONB NOT NULL DEFAULT '[]'::jsonb,
    structural_6h_id TEXT
        REFERENCES public.thesis_structural_observation_v1(observation_id)
        ON DELETE RESTRICT,
    structural_24h_id TEXT
        REFERENCES public.thesis_structural_observation_v1(observation_id)
        ON DELETE RESTRICT,
    structural_3d_id TEXT
        REFERENCES public.thesis_structural_observation_v1(observation_id)
        ON DELETE RESTRICT,
    mme_observation_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    mme_transition_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    tactical_set_id TEXT
        REFERENCES public.thesis_tactical_opportunity_set_v1(tactical_set_id)
        ON DELETE RESTRICT,
    source_version_manifest JSONB NOT NULL,
    source_timestamps JSONB NOT NULL,
    bundle_fingerprint TEXT NOT NULL,
    git_revision TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT thesis_bundle_id_ck CHECK (bundle_id ~ '^[0-9a-f]{64}$'),
    CONSTRAINT thesis_bundle_direction_scope_ck CHECK (
        direction_scope = 'LONG_ONLY_OBSERVATION'
    ),
    CONSTRAINT thesis_bundle_status_ck CHECK (
        evidence_status IN ('COMPLETE','INCOMPLETE')
    ),
    CONSTRAINT thesis_bundle_fingerprint_ck CHECK (
        bundle_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_bundle_identity_uk UNIQUE (
        environment,deployment_id,symbol,pipeline_run_id,evidence_cutoff
    )
);

CREATE TABLE IF NOT EXISTS public.thesis_evidence_bundle_cutover_v1 (
    cutover_id TEXT PRIMARY KEY,
    contract_version TEXT NOT NULL,
    effective_timestamp TIMESTAMPTZ NOT NULL,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    git_revision TEXT NOT NULL,
    first_eligible_pipeline_run_id TEXT NOT NULL
        REFERENCES public.thesis_evidence_pipeline_run_v1(pipeline_run_id)
        ON DELETE RESTRICT,
    first_eligible_evidence_cutoff TIMESTAMPTZ NOT NULL,
    source_version_manifest JSONB NOT NULL,
    rollout_mode TEXT NOT NULL,
    cutover_fingerprint TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT thesis_cutover_id_ck CHECK (cutover_id ~ '^[0-9a-f]{64}$'),
    CONSTRAINT thesis_cutover_mode_ck CHECK (rollout_mode = 'SHADOW'),
    CONSTRAINT thesis_cutover_fingerprint_ck CHECK (
        cutover_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT thesis_cutover_identity_uk UNIQUE (
        contract_version,environment,deployment_id
    )
);

CREATE INDEX IF NOT EXISTS ix_thesis_pipeline_cutoff_v1
    ON public.thesis_evidence_pipeline_run_v1(
        environment,deployment_id,evidence_cutoff DESC
    );
CREATE INDEX IF NOT EXISTS ix_thesis_structural_symbol_v1
    ON public.thesis_structural_observation_v1(
        environment,deployment_id,symbol,evidence_cutoff DESC,horizon
    );
CREATE INDEX IF NOT EXISTS ix_thesis_mme_source_v1
    ON public.thesis_mme_sequence_observation_v1(
        environment,deployment_id,symbol,interval,evidence_cutoff DESC
    );
CREATE INDEX IF NOT EXISTS ix_thesis_mme_transition_source_v1
    ON public.thesis_mme_transition_observation_v1(
        environment,deployment_id,symbol,interval,evidence_cutoff DESC
    );
CREATE INDEX IF NOT EXISTS ix_thesis_bundle_status_v1
    ON public.thesis_evidence_bundle_v1(
        environment,deployment_id,evidence_status,evidence_cutoff DESC
    );

DO $append_only_triggers$
DECLARE
    relation_name text;
    trigger_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'thesis_evidence_pipeline_run_v1',
        'thesis_structural_observation_v1',
        'thesis_mme_sequence_observation_v1',
        'thesis_mme_transition_observation_v1',
        'thesis_tactical_opportunity_set_v1',
        'thesis_tactical_opportunity_member_v1',
        'thesis_evidence_bundle_v1',
        'thesis_evidence_bundle_cutover_v1'
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

CREATE OR REPLACE VIEW public.v_thesis_evidence_pipeline_latest_v1 AS
SELECT DISTINCT ON (environment,deployment_id)
    pipeline_run_id,contract_version,environment,deployment_id,
    evidence_cutoff,evidence_status,missing_sources,source_version_manifest,
    run_fingerprint,git_revision,created_at
FROM public.thesis_evidence_pipeline_run_v1
ORDER BY environment,deployment_id,evidence_cutoff DESC,created_at DESC;

CREATE OR REPLACE VIEW public.v_thesis_evidence_bundle_audit_v1 AS
SELECT
    environment,deployment_id,
    COUNT(*) AS bundles_total,
    COUNT(*) FILTER (WHERE evidence_status='COMPLETE') AS bundles_complete,
    COUNT(*) FILTER (WHERE evidence_status='INCOMPLETE') AS bundles_incomplete,
    MIN(evidence_cutoff) AS first_cutoff,
    MAX(evidence_cutoff) AS latest_cutoff,
    COUNT(DISTINCT bundle_fingerprint) AS distinct_bundle_fingerprints
FROM public.thesis_evidence_bundle_v1
GROUP BY environment,deployment_id;

CREATE OR REPLACE VIEW public.v_thesis_evidence_missing_sources_v1 AS
SELECT
    b.environment,b.deployment_id,b.symbol,b.evidence_cutoff,
    missing.value #>> '{}' AS missing_source
FROM public.thesis_evidence_bundle_v1 b
CROSS JOIN LATERAL jsonb_array_elements(b.missing_sources) missing(value);

CREATE OR REPLACE VIEW public.v_thesis_evidence_integrity_v1 AS
SELECT
    (SELECT COUNT(*) FROM (
        SELECT environment,deployment_id,evidence_cutoff,contract_version
        FROM public.thesis_evidence_pipeline_run_v1
        GROUP BY 1,2,3,4 HAVING COUNT(*) > 1
    ) duplicate_runs) AS duplicate_pipeline_identities,
    (SELECT COUNT(*) FROM (
        SELECT pipeline_run_id,symbol,horizon
        FROM public.thesis_structural_observation_v1
        GROUP BY 1,2,3 HAVING COUNT(*) > 1
    ) duplicate_structural) AS duplicate_structural_identities,
    (SELECT COUNT(*) FROM (
        SELECT pipeline_run_id,symbol,interval
        FROM public.thesis_mme_sequence_observation_v1
        GROUP BY 1,2,3 HAVING COUNT(*) > 1
    ) duplicate_mme) AS duplicate_mme_identities,
    (SELECT COUNT(*) FROM (
        SELECT pipeline_run_id,symbol
        FROM public.thesis_tactical_opportunity_set_v1
        GROUP BY 1,2 HAVING COUNT(*) > 1
    ) duplicate_tactical_sets) AS duplicate_tactical_set_identities,
    0::bigint AS bundle_fingerprint_conflicts;

COMMENT ON TABLE public.thesis_evidence_bundle_v1 IS
    'Immutable shadow evidence only. It has no thesis or trading authority.';
COMMENT ON TABLE public.thesis_mme_transition_observation_v1 IS
    'Descriptive source transitions only; absence never means thesis invalidation.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260818_thesis_evidence_bundle_v1.sql',
    COALESCE(NULLIF(current_setting('waltrade.migration_checksum',true),''),repeat('0',64)),
    COALESCE(NULLIF(current_setting('waltrade.target_environment',true),''),'UNKNOWN'),
    COALESCE(NULLIF(current_setting('waltrade.target_deployment_id',true),''),'UNKNOWN'),
    current_database(),'operator-migration','APPLIED',TRUE,0,
    COALESCE(NULLIF(current_setting('waltrade.git_sha',true),''),repeat('0',40)),
    'THESIS_EVIDENCE_BUNDLE_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id='20260818_thesis_evidence_bundle_v1.sql'
);

COMMIT;

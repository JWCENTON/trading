\set ON_ERROR_STOP on

-- PostgreSQL 16 empty-database harness for the Learning feature migration
-- chain. These two relations are platform prerequisites, not objects owned by
-- the Feedback feature.
CREATE TABLE automation_kv (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMPTZ
);
CREATE VIEW v_decision_intelligence_v1 AS
SELECT
    NULL::TEXT AS environment,
    NULL::TEXT AS symbol,
    NULL::TEXT AS interval,
    NULL::TEXT AS strategy,
    NULL::TEXT AS decision_key,
    NULL::NUMERIC AS net_pnl_usdc,
    NULL::TEXT AS recommendation_type,
    NULL::TEXT AS recommendation_action,
    NULL::INTEGER AS missing_context_count,
    NULL::TIMESTAMPTZ AS refreshed_at,
    NULL::TEXT AS decision_lifecycle_status,
    NULL::BOOLEAN AS has_pnl
WHERE false;

-- Historical fresh-install chain. This is deliberately separate from the
-- existing-schema production upgrade harness.
\i /repo/db/migrations/20260710_learning_feedback_engine_v1.sql
\i /repo/db/migrations/20260710_learning_feedback_engine_v1_1.sql
\i /repo/db/migrations/20260710_learning_feedback_engine_v1_2_automation.sql
\i /repo/db/migrations/20260710_learning_feedback_engine_v1_3_validation.sql
\i /repo/db/migrations/20260710_learning_feedback_engine_v1_4_shadow_confidence.sql

-- Current Decision SSOT/warehouse platform prerequisites. Empty fresh
-- installations have no historical orphan and therefore do not run the
-- production-only 98b4 repair.
CREATE OR REPLACE VIEW v_decision_intelligence_v1 AS
SELECT
    NULL::TEXT AS environment,
    NULL::TEXT AS symbol,
    NULL::TEXT AS interval,
    NULL::TEXT AS strategy,
    NULL::TEXT AS decision_key,
    NULL::NUMERIC AS net_pnl_usdc,
    NULL::TEXT AS recommendation_type,
    NULL::TEXT AS recommendation_action,
    NULL::INTEGER AS missing_context_count,
    NULL::TIMESTAMPTZ AS refreshed_at,
    NULL::TEXT AS decision_lifecycle_status,
    NULL::BOOLEAN AS has_pnl,
    NULL::TIMESTAMPTZ AS created_at
WHERE false;
CREATE TABLE learning_feature_warehouse_v1 (
    id BIGINT PRIMARY KEY,
    environment TEXT,
    decision_key TEXT,
    position_id BIGINT,
    entry_time TIMESTAMPTZ,
    exit_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    net_pnl_usdc NUMERIC
);
CREATE TABLE decision_registry_v1 (
    decision_id UUID PRIMARY KEY,
    environment TEXT,
    legacy_decision_key TEXT,
    deployment_id TEXT,
    position_id BIGINT,
    market_regime TEXT,
    ingested_at TIMESTAMPTZ
);
CREATE TABLE decision_outcomes_v1 (
    outcome_id UUID PRIMARY KEY,
    decision_id UUID,
    outcome_type TEXT,
    position_id BIGINT,
    gross_pnl_usdc NUMERIC,
    fees_usdc NUMERIC,
    net_pnl_usdc NUMERIC,
    mfe_pct NUMERIC,
    mae_pct NUMERIC,
    outcome_status TEXT,
    calculated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ
);

SET waltrade.deployment_instance_id = 'local';
SET waltrade.environment = 'live';

\i /repo/db/migrations/20260723_learning_feedback_canonical_source_upgrade_v1.sql
\i /repo/db/migrations/20260723_learning_canonical_decision_source_universe_v1.sql
\i /repo/db/migrations/20260721_learning_evidence_manifest_v1.sql

-- Run 2.
\i /repo/db/migrations/20260723_learning_feedback_canonical_source_upgrade_v1.sql
\i /repo/db/migrations/20260723_learning_canonical_decision_source_universe_v1.sql
\i /repo/db/migrations/20260721_learning_evidence_manifest_v1.sql

DO $assertions$
BEGIN
    IF current_setting('server_version_num')::INTEGER < 160000 THEN
        RAISE EXCEPTION 'POSTGRESQL_16_REQUIRED';
    END IF;
    IF to_regprocedure(
        'refresh_learning_feedback_engine_v1(integer,integer,integer)'
    ) IS NULL
       OR to_regprocedure(
        'learning_canonical_evidence_universe_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
       ) IS NULL
       OR to_regclass('public.learning_evidence_manifests_v1') IS NULL THEN
        RAISE EXCEPTION 'FRESH_INSTALL_OBJECT_MISSING';
    END IF;
    IF position(
        'learning_canonical_evidence_universe_v1'
        IN pg_get_functiondef(
            'refresh_learning_feedback_engine_v1(integer,integer,integer)'
            ::regprocedure)
    ) = 0 THEN
        RAISE EXCEPTION 'FRESH_INSTALL_FEEDBACK_NOT_UPGRADED';
    END IF;
END;
$assertions$;

SELECT 'LEARNING_CANONICAL_FRESH_INSTALL_POSTGRES16_PASS' AS result;

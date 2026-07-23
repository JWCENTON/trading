BEGIN;

DO $prerequisites$
BEGIN
    IF to_regprocedure(
        'refresh_learning_feedback_engine_v1(integer,integer,integer)'
    ) IS NULL THEN
        RAISE EXCEPTION
            'CANONICAL_SOURCE_PREREQUISITE_MISSING: feedback upgrade';
    END IF;
    IF position(
        'learning_canonical_evidence_universe_v1'
        IN pg_get_functiondef(
            'refresh_learning_feedback_engine_v1(integer,integer,integer)'
            ::regprocedure)
    ) = 0 THEN
        RAISE EXCEPTION
            'CANONICAL_SOURCE_UPGRADE_ORDER: apply learning_feedback_canonical_source_upgrade_v1 first';
    END IF;
    IF to_regclass('public.learning_feature_warehouse_v1') IS NULL
       OR to_regclass('public.decision_registry_v1') IS NULL
       OR to_regclass('public.decision_outcomes_v1') IS NULL
       OR to_regclass('public.learning_feedback_refresh_runs_v1') IS NULL THEN
        RAISE EXCEPTION
            'CANONICAL_SOURCE_PREREQUISITE_MISSING: canonical source relations';
    END IF;
END;
$prerequisites$;

CREATE OR REPLACE FUNCTION learning_canonical_evidence_universe_v1(
    p_environment TEXT,
    p_sample_from TIMESTAMPTZ,
    p_sample_to TIMESTAMPTZ,
    p_evidence_cutoff_at TIMESTAMPTZ
)
RETURNS TABLE (
    environment TEXT,
    symbol TEXT,
    "interval" TEXT,
    strategy TEXT,
    decision_key TEXT,
    decision_id UUID,
    position_id BIGINT,
    entry_time TIMESTAMPTZ,
    exit_time TIMESTAMPTZ,
    outcome_timestamp TIMESTAMPTZ,
    realized_pnl_usdc NUMERIC,
    gross_pnl_usdc NUMERIC,
    fees_usdc NUMERIC,
    mfe_pct NUMERIC,
    mae_pct NUMERIC,
    regime_identity TEXT,
    regime_context JSONB,
    source_refreshed_at TIMESTAMPTZ,
    has_full_context BOOLEAN,
    has_avoid_review BOOLEAN,
    has_entry_quality_review BOOLEAN,
    has_positive_confirmation BOOLEAN,
    eligibility_reason TEXT,
    registry_available_at TIMESTAMPTZ,
    outcome_available_at TIMESTAMPTZ
)
LANGUAGE SQL
STABLE
AS $$
WITH source_rows AS (
    SELECT d.*
    FROM v_decision_intelligence_v1 d
    WHERE d.environment = p_environment
      AND d.decision_lifecycle_status = 'CLOSED'
      AND d.has_pnl
      AND d.net_pnl_usdc IS NOT NULL
      AND d.refreshed_at BETWEEN p_sample_from AND p_sample_to
), candidates AS (
    SELECT
        d.environment,
        d.symbol,
        d.interval,
        d.strategy,
        d.decision_key,
        max(d.net_pnl_usdc) AS warehouse_pnl_usdc,
        count(DISTINCT d.net_pnl_usdc) AS warehouse_pnl_variants,
        min(d.created_at) AS source_created_at,
        max(d.refreshed_at) AS source_refreshed_at,
        bool_or(COALESCE(d.missing_context_count, 0) = 0)
            AS has_full_context,
        bool_or(
            d.recommendation_type ILIKE '%AVOID%'
            OR d.recommendation_action ILIKE '%AVOID%'
            OR d.recommendation_action ILIKE '%BLOCK%'
        ) AS has_avoid_review,
        bool_or(
            d.recommendation_type ILIKE '%ENTRY%'
            OR d.recommendation_action ILIKE '%ENTRY%'
        ) AS has_entry_quality_review,
        bool_or(
            d.recommendation_type ILIKE '%POSITIVE%'
            OR d.recommendation_action ILIKE '%CONFIRM%'
            OR d.recommendation_action ILIKE '%PROMOTE%'
        ) AS has_positive_confirmation
    FROM source_rows d
    GROUP BY d.environment, d.symbol, d.interval, d.strategy, d.decision_key
), warehouse AS (
    SELECT c.decision_key, w.position_id, w.entry_time, w.exit_time
    FROM candidates c
    LEFT JOIN LATERAL (
        SELECT x.position_id, x.entry_time, x.exit_time
        FROM learning_feature_warehouse_v1 x
        WHERE x.environment = c.environment
          AND x.decision_key = c.decision_key
          AND x.created_at <= p_evidence_cutoff_at
        ORDER BY
            (x.exit_time IS NOT NULL AND x.net_pnl_usdc IS NOT NULL) DESC,
            x.id
        LIMIT 1
    ) w ON true
), registry AS (
    SELECT
        c.decision_key,
        count(*) AS registry_rows,
        count(DISTINCT r.decision_id) AS registry_ids,
        count(DISTINCT r.deployment_id) AS registry_deployments,
        min(r.decision_id::TEXT)::UUID AS decision_id,
        min(r.position_id) AS position_id,
        min(r.market_regime) AS market_regime,
        max(r.ingested_at) AS registry_available_at
    FROM candidates c
    LEFT JOIN decision_registry_v1 r
      ON r.environment = c.environment
     AND r.legacy_decision_key = c.decision_key
     AND r.ingested_at <= p_evidence_cutoff_at
    GROUP BY c.decision_key
), outcomes AS (
    SELECT
        r.decision_key,
        count(o.*) AS outcome_rows,
        count(DISTINCT o.outcome_id) AS outcome_ids,
        count(DISTINCT o.net_pnl_usdc)
            FILTER (WHERE o.net_pnl_usdc IS NOT NULL) AS outcome_pnl_variants,
        min(o.position_id) AS position_id,
        min(o.gross_pnl_usdc) AS gross_pnl_usdc,
        min(o.fees_usdc) AS fees_usdc,
        min(o.net_pnl_usdc) AS net_pnl_usdc,
        min(o.mfe_pct) AS mfe_pct,
        min(o.mae_pct) AS mae_pct,
        min(o.outcome_status) AS outcome_status,
        max(o.calculated_at) AS calculated_at,
        greatest(max(o.created_at), max(o.calculated_at)) AS outcome_available_at
    FROM registry r
    LEFT JOIN decision_outcomes_v1 o
      ON o.decision_id = r.decision_id
     AND o.outcome_type = 'ACTUAL_TRADE'
     AND o.created_at <= p_evidence_cutoff_at
     AND o.calculated_at <= p_evidence_cutoff_at
    GROUP BY r.decision_key
), classified AS (
    SELECT
        c.*, w.position_id AS warehouse_position_id, w.entry_time, w.exit_time,
        r.registry_rows, r.registry_ids, r.registry_deployments, r.decision_id,
        r.position_id AS registry_position_id, r.market_regime,
        r.registry_available_at,
        o.outcome_rows, o.outcome_ids, o.outcome_pnl_variants,
        o.position_id AS outcome_position_id, o.gross_pnl_usdc, o.fees_usdc,
        o.net_pnl_usdc, o.mfe_pct, o.mae_pct, o.outcome_status,
        o.calculated_at, o.outcome_available_at,
        CASE
            WHEN c.source_created_at > p_evidence_cutoff_at
              OR w.position_id IS NULL
                THEN 'EXCLUDED_POST_CUTOFF'
            WHEN r.registry_rows = 0 AND EXISTS (
                SELECT 1 FROM decision_registry_v1 late_r
                 WHERE late_r.environment = c.environment
                   AND late_r.legacy_decision_key = c.decision_key
                   AND late_r.ingested_at > p_evidence_cutoff_at
            ) THEN 'EXCLUDED_POST_CUTOFF'
            WHEN r.registry_rows = 0 THEN 'EXCLUDED_MISSING_REGISTRY'
            WHEN r.registry_ids <> 1 THEN 'EXCLUDED_CONFLICTING_IDENTITY'
            WHEN r.registry_deployments <> 1 THEN 'EXCLUDED_CROSS_DEPLOYMENT'
            WHEN o.outcome_rows = 0 AND EXISTS (
                SELECT 1 FROM decision_outcomes_v1 late_o
                 WHERE late_o.decision_id = r.decision_id
                   AND late_o.outcome_type = 'ACTUAL_TRADE'
                   AND (
                       late_o.created_at > p_evidence_cutoff_at
                       OR late_o.calculated_at > p_evidence_cutoff_at
                   )
            ) THEN 'EXCLUDED_POST_CUTOFF'
            WHEN o.outcome_rows = 0 THEN 'EXCLUDED_MISSING_OUTCOME'
            WHEN o.outcome_ids <> 1 OR o.outcome_pnl_variants <> 1
                THEN 'EXCLUDED_CONFLICTING_OUTCOME'
            WHEN o.outcome_status <> 'COMPLETE'
                THEN 'EXCLUDED_INCOMPLETE_LIFECYCLE'
            WHEN w.exit_time IS NULL OR o.calculated_at < w.exit_time
                THEN 'EXCLUDED_CHRONOLOGY'
            WHEN w.position_id IS DISTINCT FROM r.position_id
              OR w.position_id IS DISTINCT FROM o.position_id
                THEN 'EXCLUDED_CONFLICTING_IDENTITY'
            WHEN c.warehouse_pnl_variants <> 1
              OR c.warehouse_pnl_usdc IS DISTINCT FROM o.net_pnl_usdc
                THEN 'EXCLUDED_CONFLICTING_PNL'
            ELSE 'ELIGIBLE'
        END AS eligibility_reason
    FROM candidates c
    JOIN warehouse w USING (decision_key)
    JOIN registry r USING (decision_key)
    JOIN outcomes o USING (decision_key)
)
SELECT
    x.environment, x.symbol, x.interval, x.strategy, x.decision_key,
    x.decision_id, x.warehouse_position_id, x.entry_time, x.exit_time,
    x.calculated_at, x.net_pnl_usdc, x.gross_pnl_usdc, x.fees_usdc,
    x.mfe_pct, x.mae_pct, x.market_regime,
    CASE WHEN x.market_regime IS NULL THEN NULL
         ELSE jsonb_build_object('market_regime', x.market_regime) END,
    x.source_refreshed_at, x.has_full_context, x.has_avoid_review,
    x.has_entry_quality_review, x.has_positive_confirmation,
    x.eligibility_reason, x.registry_available_at, x.outcome_available_at
FROM classified x
ORDER BY x.decision_key;
$$;

CREATE TABLE IF NOT EXISTS learning_canonical_evidence_selection_v1 (
    feedback_run_id BIGINT NOT NULL
        REFERENCES learning_feedback_refresh_runs_v1(id),
    environment TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    strategy TEXT NOT NULL,
    window_days INTEGER NOT NULL,
    evidence_cutoff_at TIMESTAMPTZ NOT NULL,
    source_candidate_count INTEGER NOT NULL CHECK (source_candidate_count >= 0),
    canonical_eligible_count INTEGER NOT NULL CHECK (canonical_eligible_count >= 0),
    excluded_missing_registry INTEGER NOT NULL CHECK (excluded_missing_registry >= 0),
    excluded_missing_outcome INTEGER NOT NULL CHECK (excluded_missing_outcome >= 0),
    excluded_conflicting_identity INTEGER NOT NULL CHECK (excluded_conflicting_identity >= 0),
    excluded_conflicting_outcome INTEGER NOT NULL CHECK (excluded_conflicting_outcome >= 0),
    excluded_post_cutoff INTEGER NOT NULL CHECK (excluded_post_cutoff >= 0),
    excluded_chronology INTEGER NOT NULL CHECK (excluded_chronology >= 0),
    excluded_other_reason INTEGER NOT NULL CHECK (excluded_other_reason >= 0),
    source_universe_hash TEXT NOT NULL CHECK (length(source_universe_hash) = 64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (feedback_run_id, symbol, interval, strategy, window_days),
    CHECK (
        source_candidate_count =
        canonical_eligible_count + excluded_missing_registry
        + excluded_missing_outcome + excluded_conflicting_identity
        + excluded_conflicting_outcome + excluded_post_cutoff
        + excluded_chronology + excluded_other_reason
    )
);

CREATE OR REPLACE FUNCTION prevent_learning_canonical_evidence_mutation_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'learning canonical evidence telemetry is immutable';
END;
$$;

DROP TRIGGER IF EXISTS learning_canonical_evidence_immutable_v1
    ON learning_canonical_evidence_selection_v1;
CREATE TRIGGER learning_canonical_evidence_immutable_v1
BEFORE UPDATE OR DELETE ON learning_canonical_evidence_selection_v1
FOR EACH ROW EXECUTE FUNCTION prevent_learning_canonical_evidence_mutation_v1();

COMMIT;

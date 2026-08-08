-- WALTRADE LEARNING CANONICAL REGISTRY RESOLUTION V1
-- Prefer the forward position identity and use legacy-key identity only as a
-- transition fallback.  The two identity tiers are mutually exclusive.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $prerequisites$
BEGIN
    IF to_regprocedure(
        'public.learning_canonical_evidence_universe_pre_ft_quarantine_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
    ) IS NULL
       OR to_regclass('public.learning_feature_warehouse_v1') IS NULL
       OR to_regclass('public.decision_registry_v1') IS NULL
       OR to_regclass('public.decision_outcomes_v1') IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_REGISTRY_RESOLUTION_PREREQUISITE_MISSING';
    END IF;
END;
$prerequisites$;

CREATE OR REPLACE FUNCTION public.learning_canonical_evidence_universe_pre_ft_quarantine_v1(
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
AS $function$
WITH source_rows AS (
    SELECT d.*
    FROM public.v_decision_intelligence_v1 d
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
        FROM public.learning_feature_warehouse_v1 x
        WHERE x.environment = c.environment
          AND x.decision_key = c.decision_key
          AND x.created_at <= p_evidence_cutoff_at
        ORDER BY
            (x.exit_time IS NOT NULL AND x.net_pnl_usdc IS NOT NULL) DESC,
            x.id
        LIMIT 1
    ) w ON true
), forward_registry AS (
    SELECT
        c.decision_key, r.decision_id, r.deployment_id, r.position_id,
        r.market_regime, r.ingested_at
    FROM candidates c
    JOIN warehouse w USING (decision_key)
    JOIN public.decision_registry_v1 r
      ON r.environment = c.environment
     AND r.position_id = w.position_id
     AND r.decision_type = 'ENTRY_DECISION'
     AND r.engine_version = 'FORWARD_DECISION_REGISTRY_CONTINUITY_V1'
     AND r.ingested_at <= p_evidence_cutoff_at
), legacy_registry AS (
    SELECT
        c.decision_key, r.decision_id, r.deployment_id, r.position_id,
        r.market_regime, r.ingested_at
    FROM candidates c
    JOIN public.decision_registry_v1 r
      ON r.environment = c.environment
     AND r.legacy_decision_key = c.decision_key
     AND r.ingested_at <= p_evidence_cutoff_at
    WHERE NOT EXISTS (
        SELECT 1
        FROM forward_registry f
        WHERE f.decision_key = c.decision_key
    )
), selected_registry AS (
    SELECT * FROM forward_registry
    UNION ALL
    SELECT * FROM legacy_registry
), registry AS (
    SELECT
        c.decision_key,
        count(r.decision_id) AS registry_rows,
        count(DISTINCT r.decision_id) AS registry_ids,
        count(DISTINCT r.deployment_id) AS registry_deployments,
        min(r.decision_id::TEXT)::UUID AS decision_id,
        min(r.position_id) AS position_id,
        min(r.market_regime) AS market_regime,
        max(r.ingested_at) AS registry_available_at
    FROM candidates c
    LEFT JOIN selected_registry r USING (decision_key)
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
    LEFT JOIN public.decision_outcomes_v1 o
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
                SELECT 1
                FROM public.decision_registry_v1 late_r
                WHERE late_r.environment = c.environment
                  AND late_r.ingested_at > p_evidence_cutoff_at
                  AND (
                      (
                          late_r.position_id = w.position_id
                          AND late_r.decision_type = 'ENTRY_DECISION'
                          AND late_r.engine_version =
                              'FORWARD_DECISION_REGISTRY_CONTINUITY_V1'
                      )
                      OR late_r.legacy_decision_key = c.decision_key
                  )
            ) THEN 'EXCLUDED_POST_CUTOFF'
            WHEN r.registry_rows = 0 THEN 'EXCLUDED_MISSING_REGISTRY'
            WHEN r.registry_ids <> 1 THEN 'EXCLUDED_CONFLICTING_IDENTITY'
            WHEN r.registry_deployments <> 1 THEN 'EXCLUDED_CROSS_DEPLOYMENT'
            WHEN o.outcome_rows = 0 AND EXISTS (
                SELECT 1 FROM public.decision_outcomes_v1 late_o
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
ORDER BY x.decision_key
$function$;

DO $postcondition$
DECLARE
    v_definition TEXT := pg_get_functiondef(to_regprocedure(
        'public.learning_canonical_evidence_universe_pre_ft_quarantine_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
    ));
BEGIN
    IF position('forward_registry AS' IN v_definition) = 0
       OR position('legacy_registry AS' IN v_definition) = 0
       OR position('WHERE NOT EXISTS' IN v_definition) = 0
       OR position('FORWARD_DECISION_REGISTRY_CONTINUITY_V1' IN v_definition) = 0 THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_REGISTRY_RESOLUTION_POSTCONDITION_FAILED';
    END IF;
END;
$postcondition$;

COMMENT ON FUNCTION public.learning_canonical_evidence_universe_pre_ft_quarantine_v1(
    TEXT,TIMESTAMPTZ,TIMESTAMPTZ,TIMESTAMPTZ
) IS
    'Canonical evidence source: pre-cutoff forward ENTRY_DECISION by warehouse position, otherwise legacy decision-key fallback; FT quarantine is applied by the live wrapper.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260808_learning_canonical_registry_resolution_v1.sql',
    'f5780bb398464efc8b65a7b19bff8caef230b154e305399c7f6a39bbcd36452a',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'LEARNING_CANONICAL_REGISTRY_RESOLUTION_V1',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    'ebc62360e76b402f357b266020693b98b0eacac9',
    'LEARNING_CANONICAL_REGISTRY_RESOLUTION_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id =
          '20260808_learning_canonical_registry_resolution_v1.sql'
);

COMMIT;

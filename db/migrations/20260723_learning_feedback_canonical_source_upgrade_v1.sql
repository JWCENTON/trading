BEGIN;

DO $prerequisites$
BEGIN
    IF to_regprocedure(
        'refresh_learning_feedback_engine_v1(integer,integer,integer)'
    ) IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_UPGRADE_PREREQUISITE_MISSING: refresh_learning_feedback_engine_v1';
    END IF;
    IF to_regclass('public.v_decision_intelligence_v1') IS NULL
       OR to_regclass('public.learning_feature_warehouse_v1') IS NULL
       OR to_regclass('public.decision_registry_v1') IS NULL
       OR to_regclass('public.decision_outcomes_v1') IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_UPGRADE_PREREQUISITE_MISSING: canonical source relations';
    END IF;
END;
$prerequisites$;

-- Preserve the exact existing production function contract once. Rollback can
-- restore this function without replaying historical schema or view DDL.
DO $preserve_and_patch$
DECLARE
    v_definition TEXT;
    v_backup_definition TEXT;
    v_start INTEGER;
    v_aggregated INTEGER;
    v_canonical_cte TEXT := $canonical_cte$
    WITH canonical_universe AS (
        SELECT
            u.environment, u.symbol, u.interval, u.strategy, u.decision_key,
            u.realized_pnl_usdc AS net_pnl_usdc,
            u.source_refreshed_at AS refreshed_at,
            u.has_full_context, u.has_avoid_review,
            u.has_entry_quality_review, u.has_positive_confirmation
        FROM learning_canonical_evidence_universe_v1(
            current_database(),
            v_now - make_interval(days => p_window_days),
            v_now,
            v_now
        ) u
        WHERE u.eligibility_reason = 'ELIGIBLE'
    ),
$canonical_cte$;
BEGIN
    v_definition := pg_get_functiondef(
        'refresh_learning_feedback_engine_v1(integer,integer,integer)'
        ::regprocedure);

    IF to_regprocedure(
        'learning_feedback_engine_v1_pre_canonical_source_v1(integer,integer,integer)'
    ) IS NULL THEN
        v_backup_definition := replace(
            v_definition,
            'refresh_learning_feedback_engine_v1',
            'learning_feedback_engine_v1_pre_canonical_source_v1'
        );
        EXECUTE v_backup_definition;
    END IF;

    IF position('learning_canonical_evidence_universe_v1'
                IN v_definition) = 0 THEN
        v_start := position('    WITH source_rows AS (' IN v_definition);
        v_aggregated := position('    aggregated AS (' IN v_definition);
        IF v_start = 0 OR v_aggregated <= v_start THEN
            RAISE EXCEPTION
                'LEARNING_CANONICAL_UPGRADE_PATCH_ANCHOR_MISSING';
        END IF;
        v_definition :=
            left(v_definition, v_start - 1)
            || v_canonical_cte
            || substring(v_definition FROM v_aggregated);
        v_definition := replace(
            v_definition, 'FROM decision_level', 'FROM canonical_universe');
        EXECUTE v_definition;
    END IF;

    IF position(
        'learning_canonical_evidence_universe_v1'
        IN pg_get_functiondef(
            'refresh_learning_feedback_engine_v1(integer,integer,integer)'
            ::regprocedure)
    ) = 0 THEN
        RAISE EXCEPTION 'LEARNING_CANONICAL_UPGRADE_PATCH_VERIFICATION_FAILED';
    END IF;
END;
$preserve_and_patch$;

COMMIT;

BEGIN;

CREATE OR REPLACE FUNCTION public.cleanup_learning_feedback_stale_current_state_v1(
    p_environment TEXT,
    p_window_days INTEGER DEFAULT 30
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := now();
    v_snapshot_token UUID :=
        NULLIF(
            current_setting(
                'waltrade.learning_source_snapshot_token',
                true
            ),
            ''
        )::UUID;

    v_deleted_stats INTEGER := 0;
    v_deleted_proposals INTEGER := 0;
    v_reset_validation INTEGER := 0;
BEGIN
    IF p_window_days <= 0 THEN
        RAISE EXCEPTION 'p_window_days must be greater than zero';
    END IF;

    /*
     * Remove unresolved current proposals belonging to slots that no longer
     * exist in the canonical eligible evidence universe.
     *
     * APPLIED and REJECTED rows are immutable audit history and are retained.
     *
     * source_snapshot_token scoping preserves frozen-source isolation.
     */
    WITH current_slots AS (
        SELECT DISTINCT
            u.strategy,
            u.symbol,
            u.interval
        FROM public.learning_canonical_evidence_universe_v1(
            p_environment,
            v_now - make_interval(days => p_window_days),
            v_now,
            v_now
        ) u
        WHERE u.eligibility_reason = 'ELIGIBLE'
    )
    DELETE FROM public.learning_calibration_proposals_v1 p
    WHERE p.environment = p_environment
      AND p.window_days = p_window_days
      AND p.source_snapshot_token IS NOT DISTINCT FROM v_snapshot_token
      AND p.validation_status IN (
          'PENDING',
          'VALIDATING',
          'EXPIRED'
      )
      AND NOT EXISTS (
          SELECT 1
          FROM current_slots c
          WHERE c.strategy = p.strategy
            AND c.symbol = p.symbol
            AND c.interval = p.interval
      );

    GET DIAGNOSTICS v_deleted_proposals = ROW_COUNT;

    /*
     * Validation state is current-state, not frozen-snapshot history.
     * Reset it only during the normal live/current refresh context.
     */
    IF v_snapshot_token IS NULL THEN
        WITH current_slots AS (
            SELECT DISTINCT
                u.strategy,
                u.symbol,
                u.interval
            FROM public.learning_canonical_evidence_universe_v1(
                p_environment,
                v_now - make_interval(days => p_window_days),
                v_now,
                v_now
            ) u
            WHERE u.eligibility_reason = 'ELIGIBLE'
        )
        UPDATE public.learning_proposal_validation_state_v1 st
           SET validation_status = 'RESET',
               reset_at = v_now,
               updated_at = v_now,
               validation_reason =
                   'RESET: slot absent from current canonical learning evidence universe'
        WHERE st.environment = p_environment
          AND st.window_days = p_window_days
          AND st.validation_status IN ('STABLE', 'VALIDATING')
          AND NOT EXISTS (
              SELECT 1
              FROM current_slots c
              WHERE c.strategy = st.strategy
                AND c.symbol = st.symbol
                AND c.interval = st.interval
          );

        GET DIAGNOSTICS v_reset_validation = ROW_COUNT;
    END IF;

    /*
     * Remove stale current statistics from exactly the same source-snapshot
     * context as the refresh that invoked this function.
     */
    WITH current_slots AS (
        SELECT DISTINCT
            u.strategy,
            u.symbol,
            u.interval
        FROM public.learning_canonical_evidence_universe_v1(
            p_environment,
            v_now - make_interval(days => p_window_days),
            v_now,
            v_now
        ) u
        WHERE u.eligibility_reason = 'ELIGIBLE'
    )
    DELETE FROM public.learning_slot_statistics_v1 s
    WHERE s.environment = p_environment
      AND s.window_days = p_window_days
      AND s.source_snapshot_token IS NOT DISTINCT FROM v_snapshot_token
      AND NOT EXISTS (
          SELECT 1
          FROM current_slots c
          WHERE c.strategy = s.strategy
            AND c.symbol = s.symbol
            AND c.interval = s.interval
      );

    GET DIAGNOSTICS v_deleted_stats = ROW_COUNT;

    RETURN jsonb_build_object(
        'status', 'ok',
        'environment', p_environment,
        'window_days', p_window_days,
        'source_snapshot_token', v_snapshot_token,
        'deleted_stale_statistics', v_deleted_stats,
        'deleted_current_proposals', v_deleted_proposals,
        'reset_validation_states', v_reset_validation
    );
END;
$$;


/*
 * Permanently hook cleanup into the canonical base refresh.
 *
 * Required order:
 *
 * canonical aggregation/upsert
 * -> stale-current cleanup
 * -> proposal generation
 *
 * refresh_learning_feedback_engine_v1_1() already calls this base function
 * before reclassification and rebuilding V1.1 proposals, so all higher
 * Learning Feedback scheduler flows inherit the invariant automatically.
 */
DO $patch$
DECLARE
    v_definition TEXT;
    v_marker TEXT :=
        'PERFORM public.cleanup_learning_feedback_stale_current_state_v1(';
    v_anchor TEXT :=
        'GET DIAGNOSTICS v_stats_upserted = ROW_COUNT;';
BEGIN
    v_definition := pg_get_functiondef(
        'public.refresh_learning_feedback_engine_v1(integer,integer,integer)'
        ::regprocedure
    );

    IF position(v_marker IN v_definition) = 0 THEN
        IF position(v_anchor IN v_definition) = 0 THEN
            RAISE EXCEPTION
                'LEARNING_STALE_CLEANUP_PATCH_ANCHOR_MISSING';
        END IF;

        v_definition := replace(
            v_definition,
            v_anchor,
            v_anchor || E'\n\n'
            || '    PERFORM public.cleanup_learning_feedback_stale_current_state_v1('
            || E'\n'
            || '        current_database(),'
            || E'\n'
            || '        p_window_days'
            || E'\n'
            || '    );'
        );

        EXECUTE v_definition;
    END IF;

    IF position(
        v_marker
        IN pg_get_functiondef(
            'public.refresh_learning_feedback_engine_v1(integer,integer,integer)'
            ::regprocedure
        )
    ) = 0 THEN
        RAISE EXCEPTION
            'LEARNING_STALE_CLEANUP_PATCH_VERIFICATION_FAILED';
    END IF;
END;
$patch$;

COMMENT ON FUNCTION
    public.cleanup_learning_feedback_stale_current_state_v1(TEXT, INTEGER)
IS
'Removes stale current Learning Feedback state absent from the canonical eligible evidence universe; snapshot-scoped statistics/proposals and live-current validation reset only.';

COMMIT;

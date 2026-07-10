BEGIN;

-- ============================================================================
-- WALTRADE — LEARNING FEEDBACK ENGINE V1.1
--
-- Fixes:
--   1. Actions require min_action_sample.
--   2. Profit factor summary is aggregated correctly.
--   3. Only one active shadow proposal exists per slot/window.
--
-- Safety:
--   SHADOW ONLY
--   No writes to bot_control, ORC, allocation or runtime parameters.
-- ============================================================================

DO $$
BEGIN
    IF to_regclass('public.learning_slot_statistics_v1') IS NULL THEN
        RAISE EXCEPTION
            'learning_slot_statistics_v1 does not exist; apply V1 first';
    END IF;

    IF to_regclass('public.learning_calibration_proposals_v1') IS NULL THEN
        RAISE EXCEPTION
            'learning_calibration_proposals_v1 does not exist; apply V1 first';
    END IF;

    IF to_regprocedure(
        'public.refresh_learning_feedback_engine_v1(integer,integer,integer)'
    ) IS NULL THEN
        RAISE EXCEPTION
            'refresh_learning_feedback_engine_v1(integer,integer,integer) does not exist';
    END IF;
END
$$;

-- ============================================================================
-- 1. Correct summary.
--
-- Do not average slot-level PF values because slots with no losses use the
-- sentinel 999. Calculate portfolio-level PF from aggregated gross profit/loss.
-- ============================================================================

DROP VIEW IF EXISTS v_learning_feedback_engine_summary_v1;

CREATE VIEW v_learning_feedback_engine_summary_v1 AS
SELECT
    environment,
    window_days,
    learning_status,

    COUNT(*) AS slots,
    SUM(decisions) AS decisions,

    ROUND(
        SUM(net_pnl_usdc)::NUMERIC,
        8
    ) AS net_pnl_usdc,

    CASE
        WHEN ABS(
            COALESCE(SUM(gross_loss_usdc), 0)
        ) > 0
        THEN ROUND(
            (
                COALESCE(SUM(gross_profit_usdc), 0)
                /
                ABS(SUM(gross_loss_usdc))
            )::NUMERIC,
            6
        )
        ELSE NULL
    END AS aggregate_profit_factor,

    COUNT(*) FILTER (
        WHERE COALESCE(gross_loss_usdc, 0) = 0
          AND COALESCE(gross_profit_usdc, 0) > 0
    ) AS zero_loss_slots,

    ROUND(
        (
            100.0 * SUM(wins)
            / NULLIF(SUM(decisions), 0)
        )::NUMERIC,
        4
    ) AS weighted_win_rate_pct,

    ROUND(
        (
            100.0 * SUM(full_context_decisions)
            / NULLIF(SUM(decisions), 0)
        )::NUMERIC,
        4
    ) AS weighted_context_coverage_pct,

    MAX(calculated_at) AS calculated_at

FROM learning_slot_statistics_v1
GROUP BY
    environment,
    window_days,
    learning_status;

-- ============================================================================
-- 2. Safe wrapper refresh.
--
-- V1 still performs the source aggregation. V1.1 then enforces sample policy
-- and rebuilds one current proposal per slot.
-- ============================================================================

CREATE OR REPLACE FUNCTION refresh_learning_feedback_engine_v1_1(
    p_window_days INTEGER DEFAULT 30,
    p_min_observe_sample INTEGER DEFAULT 10,
    p_min_action_sample INTEGER DEFAULT 30
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_base_result JSONB;
    v_reclassified INTEGER := 0;
    v_deleted_proposals INTEGER := 0;
    v_inserted_proposals INTEGER := 0;
    v_now TIMESTAMPTZ := now();
    v_result JSONB;
BEGIN
    IF p_window_days <= 0 THEN
        RAISE EXCEPTION 'p_window_days must be greater than zero';
    END IF;

    IF p_min_observe_sample <= 0 THEN
        RAISE EXCEPTION 'p_min_observe_sample must be greater than zero';
    END IF;

    IF p_min_action_sample < p_min_observe_sample THEN
        RAISE EXCEPTION
            'p_min_action_sample must be >= p_min_observe_sample';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtext('refresh_learning_feedback_engine_v1_1')
    );

    -- Run existing source aggregation.
    v_base_result := refresh_learning_feedback_engine_v1(
        p_window_days,
        p_min_observe_sample,
        p_min_action_sample
    );

    -- ------------------------------------------------------------------------
    -- Correct classification policy.
    --
    -- 0 .. observe-1  = INSUFFICIENT_SAMPLE
    -- observe .. action-1 = OBSERVE
    -- action+ = evidence may trigger a recommendation
    -- ------------------------------------------------------------------------

    UPDATE learning_slot_statistics_v1 s
    SET
        learning_status = CASE
            WHEN s.decisions < p_min_observe_sample
                THEN 'INSUFFICIENT_SAMPLE'

            WHEN s.decisions < p_min_action_sample
                THEN 'OBSERVE'

            WHEN s.net_pnl_usdc < 0
             AND s.profit_factor < 0.80
                THEN 'NEGATIVE_EDGE'

            WHEN s.net_pnl_usdc < 0
              OR s.profit_factor < 1.00
                THEN 'WEAK_EDGE'

            WHEN s.net_pnl_usdc > 0
             AND s.profit_factor >= 1.50
             AND s.context_coverage_pct >= 90
                THEN 'STRONG_EDGE'

            WHEN s.net_pnl_usdc > 0
             AND s.profit_factor >= 1.20
             AND s.context_coverage_pct >= 80
                THEN 'POSITIVE_EDGE'

            ELSE 'OBSERVE'
        END,

        learning_reason = CASE
            WHEN s.decisions < p_min_observe_sample
                THEN format(
                    'Insufficient sample: decisions=%s, minimum observe=%s',
                    s.decisions,
                    p_min_observe_sample
                )

            WHEN s.decisions < p_min_action_sample
                THEN format(
                    'Observe only: decisions=%s, action requires=%s',
                    s.decisions,
                    p_min_action_sample
                )

            WHEN s.net_pnl_usdc < 0
             AND s.profit_factor < 0.80
                THEN format(
                    'Confirmed negative edge: decisions=%s net=%s PF=%s',
                    s.decisions,
                    round(s.net_pnl_usdc, 6),
                    round(s.profit_factor, 4)
                )

            WHEN s.net_pnl_usdc < 0
              OR s.profit_factor < 1.00
                THEN format(
                    'Weak edge: decisions=%s net=%s PF=%s',
                    s.decisions,
                    round(s.net_pnl_usdc, 6),
                    round(s.profit_factor, 4)
                )

            WHEN s.net_pnl_usdc > 0
             AND s.profit_factor >= 1.50
             AND s.context_coverage_pct >= 90
                THEN format(
                    'Strong edge candidate: decisions=%s net=%s PF=%s coverage=%s%%',
                    s.decisions,
                    round(s.net_pnl_usdc, 6),
                    round(s.profit_factor, 4),
                    round(s.context_coverage_pct, 2)
                )

            WHEN s.net_pnl_usdc > 0
             AND s.profit_factor >= 1.20
             AND s.context_coverage_pct >= 80
                THEN format(
                    'Positive edge: decisions=%s net=%s PF=%s coverage=%s%%',
                    s.decisions,
                    round(s.net_pnl_usdc, 6),
                    round(s.profit_factor, 4),
                    round(s.context_coverage_pct, 2)
                )

            ELSE format(
                'Observe: decisions=%s net=%s PF=%s',
                s.decisions,
                round(s.net_pnl_usdc, 6),
                round(s.profit_factor, 4)
            )
        END,

        evidence = COALESCE(s.evidence, '{}'::jsonb)
            || jsonb_build_object(
                'policy_version',
                'LEARNING_FEEDBACK_SAMPLE_POLICY_V1_1',
                'min_observe_sample',
                p_min_observe_sample,
                'min_action_sample',
                p_min_action_sample,
                'reclassified_at',
                v_now
            ),

        calculated_at = v_now

    WHERE s.window_days = p_window_days;

    GET DIAGNOSTICS v_reclassified = ROW_COUNT;

    -- ------------------------------------------------------------------------
    -- Remove current unresolved V1 proposals for this window.
    --
    -- APPLIED and REJECTED records are retained as immutable audit history.
    -- ------------------------------------------------------------------------

    DELETE FROM learning_calibration_proposals_v1
    WHERE window_days = p_window_days
      AND validation_status IN (
          'PENDING',
          'VALIDATING',
          'EXPIRED'
      );

    GET DIAGNOSTICS v_deleted_proposals = ROW_COUNT;

    -- ------------------------------------------------------------------------
    -- Rebuild exactly one current proposal per slot/window.
    --
    -- proposal_key no longer includes proposal action, which prevents
    -- contradictory active proposals for the same slot.
    -- ------------------------------------------------------------------------

    WITH candidates AS (
        SELECT
            s.*,

            CASE s.learning_status
                WHEN 'NEGATIVE_EDGE' THEN 'SLOT_POLICY'
                WHEN 'WEAK_EDGE' THEN 'CONFIDENCE'
                WHEN 'STRONG_EDGE' THEN 'PROMOTION'
                WHEN 'POSITIVE_EDGE' THEN 'CONFIDENCE'
                ELSE 'CONFIDENCE'
            END AS proposal_type,

            CASE s.learning_status
                WHEN 'NEGATIVE_EDGE' THEN 'BLOCK_CANDIDATE'
                WHEN 'WEAK_EDGE' THEN 'REDUCE_CONFIDENCE'
                WHEN 'STRONG_EDGE' THEN 'PROMOTE_CANDIDATE'
                WHEN 'POSITIVE_EDGE' THEN 'INCREASE_CONFIDENCE'
                ELSE 'OBSERVE'
            END AS proposal_action,

            CASE s.learning_status
                WHEN 'NEGATIVE_EDGE' THEN -0.050000
                WHEN 'WEAK_EDGE' THEN -0.020000
                WHEN 'STRONG_EDGE' THEN 0.050000
                WHEN 'POSITIVE_EDGE' THEN 0.020000
                ELSE 0.000000
            END::NUMERIC(20,8) AS suggested_delta,

            CASE s.learning_status
                WHEN 'NEGATIVE_EDGE' THEN 'P0'
                WHEN 'WEAK_EDGE' THEN 'P1'
                WHEN 'STRONG_EDGE' THEN 'P1'
                WHEN 'POSITIVE_EDGE' THEN 'P2'
                ELSE 'P3'
            END AS priority,

            LEAST(
                0.99,
                GREATEST(
                    0.10,

                    (
                        LEAST(
                            s.decisions,
                            p_min_action_sample * 3
                        )::NUMERIC
                        /
                        NULLIF(p_min_action_sample * 3, 0)
                    ) * 0.55

                    +

                    LEAST(
                        COALESCE(s.context_coverage_pct, 0) / 100.0,
                        1.0
                    ) * 0.25

                    +

                    CASE
                        WHEN s.decisions < p_min_action_sample
                            THEN 0.00

                        WHEN s.learning_status IN (
                            'NEGATIVE_EDGE',
                            'STRONG_EDGE'
                        )
                            THEN 0.20

                        WHEN s.learning_status IN (
                            'WEAK_EDGE',
                            'POSITIVE_EDGE'
                        )
                            THEN 0.10

                        ELSE 0.00
                    END
                )
            )::NUMERIC(10,6) AS proposal_confidence

        FROM learning_slot_statistics_v1 s
        WHERE s.window_days = p_window_days
    )
    INSERT INTO learning_calibration_proposals_v1 (
        proposal_key,
        environment,
        symbol,
        interval,
        strategy,
        window_days,
        proposal_type,
        proposal_action,
        current_value,
        suggested_value,
        suggested_delta,
        confidence,
        priority,
        evidence_decisions,
        evidence_net_pnl_usdc,
        evidence_profit_factor,
        evidence_win_rate_pct,
        evidence_context_coverage_pct,
        reason,
        evidence,
        validation_stage,
        validation_status,
        first_seen_at,
        last_seen_at,
        refreshed_at
    )
    SELECT
        md5(
            concat_ws(
                '|',
                'LEARNING_FEEDBACK_V1_1',
                environment,
                symbol,
                interval,
                strategy,
                p_window_days
            )
        ),
        environment,
        symbol,
        interval,
        strategy,
        p_window_days,
        proposal_type,
        proposal_action,
        NULL,
        NULL,
        suggested_delta,
        proposal_confidence,
        priority,
        decisions,
        net_pnl_usdc,
        profit_factor,
        win_rate_pct,
        context_coverage_pct,
        learning_reason,
        jsonb_build_object(
            'learning_status', learning_status,
            'decisions', decisions,
            'wins', wins,
            'losses', losses,
            'net_pnl_usdc', net_pnl_usdc,
            'expectancy_usdc', expectancy_usdc,
            'profit_factor', profit_factor,
            'win_rate_pct', win_rate_pct,
            'context_coverage_pct', context_coverage_pct,
            'source_table', 'learning_slot_statistics_v1',
            'engine_version', 'LEARNING_FEEDBACK_ENGINE_V1_1',
            'sample_policy', jsonb_build_object(
                'min_observe_sample',
                p_min_observe_sample,
                'min_action_sample',
                p_min_action_sample
            ),
            'apply_allowed', false
        ),
        'SHADOW',
        'PENDING',
        v_now,
        v_now,
        v_now
    FROM candidates
    ON CONFLICT (proposal_key)
    DO UPDATE SET
        proposal_type = EXCLUDED.proposal_type,
        proposal_action = EXCLUDED.proposal_action,
        suggested_delta = EXCLUDED.suggested_delta,
        confidence = EXCLUDED.confidence,
        priority = EXCLUDED.priority,
        evidence_decisions = EXCLUDED.evidence_decisions,
        evidence_net_pnl_usdc =
            EXCLUDED.evidence_net_pnl_usdc,
        evidence_profit_factor =
            EXCLUDED.evidence_profit_factor,
        evidence_win_rate_pct =
            EXCLUDED.evidence_win_rate_pct,
        evidence_context_coverage_pct =
            EXCLUDED.evidence_context_coverage_pct,
        reason = EXCLUDED.reason,
        evidence = EXCLUDED.evidence,
        validation_stage = 'SHADOW',
        validation_status = 'PENDING',
        last_seen_at = EXCLUDED.last_seen_at,
        refreshed_at = EXCLUDED.refreshed_at;

    GET DIAGNOSTICS v_inserted_proposals = ROW_COUNT;

    v_result := jsonb_build_object(
        'status', 'ok',
        'engine_version', 'LEARNING_FEEDBACK_ENGINE_V1_1',
        'mode', 'SHADOW_ADVISOR',
        'apply_enabled', false,
        'window_days', p_window_days,
        'min_observe_sample', p_min_observe_sample,
        'min_action_sample', p_min_action_sample,
        'base_refresh', v_base_result,
        'slots_reclassified', v_reclassified,
        'old_pending_proposals_deleted', v_deleted_proposals,
        'current_proposals_upserted', v_inserted_proposals,
        'refreshed_at', v_now
    );

    INSERT INTO automation_kv (
        key,
        value,
        updated_at
    )
    VALUES (
        'learning_feedback_engine_v1_1_last_stats',
        v_result::TEXT,
        v_now
    )
    ON CONFLICT (key)
    DO UPDATE SET
        value = EXCLUDED.value,
        updated_at = EXCLUDED.updated_at;

    INSERT INTO automation_kv (
        key,
        value,
        updated_at
    )
    VALUES (
        'learning_feedback_engine_v1_1_last_status',
        'ok',
        v_now
    )
    ON CONFLICT (key)
    DO UPDATE SET
        value = EXCLUDED.value,
        updated_at = EXCLUDED.updated_at;

    INSERT INTO automation_kv (
        key,
        value,
        updated_at
    )
    VALUES (
        'learning_feedback_engine_active_version',
        'LEARNING_FEEDBACK_ENGINE_V1_1',
        v_now
    )
    ON CONFLICT (key)
    DO UPDATE SET
        value = EXCLUDED.value,
        updated_at = EXCLUDED.updated_at;

    INSERT INTO automation_kv (
        key,
        value,
        updated_at
    )
    VALUES (
        'learning_feedback_engine_apply_enabled',
        '0',
        v_now
    )
    ON CONFLICT (key)
    DO UPDATE SET
        value = EXCLUDED.value,
        updated_at = EXCLUDED.updated_at;

    RETURN v_result;

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO automation_kv (
            key,
            value,
            updated_at
        )
        VALUES (
            'learning_feedback_engine_v1_1_last_status',
            'error',
            now()
        )
        ON CONFLICT (key)
        DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = EXCLUDED.updated_at;

        INSERT INTO automation_kv (
            key,
            value,
            updated_at
        )
        VALUES (
            'learning_feedback_engine_v1_1_last_error',
            SQLERRM,
            now()
        )
        ON CONFLICT (key)
        DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = EXCLUDED.updated_at;

        RAISE;
END;
$$;

COMMENT ON FUNCTION refresh_learning_feedback_engine_v1_1(
    INTEGER,
    INTEGER,
    INTEGER
) IS
'V1.1 shadow-only refresh with minimum sample enforcement, consistent proposals and corrected summary PF.';

COMMIT;

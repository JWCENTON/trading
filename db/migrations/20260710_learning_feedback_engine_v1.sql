BEGIN;

-- ============================================================================
-- WALTRADE — LEARNING FEEDBACK ENGINE V1
--
-- Mode:
--   SHADOW / ADVISORY ONLY
--
-- Safety:
--   - does not update bot_control
--   - does not update ORC weights
--   - does not update runtime parameters
--   - does not enable LIVE slots
-- ============================================================================

DO $$
BEGIN
    IF to_regclass('public.v_decision_intelligence_v1') IS NULL THEN
        RAISE EXCEPTION
            'Required relation public.v_decision_intelligence_v1 does not exist';
    END IF;

    IF to_regclass('public.automation_kv') IS NULL THEN
        RAISE EXCEPTION
            'Required relation public.automation_kv does not exist';
    END IF;
END
$$;

-- ============================================================================
-- 1. Aggregated learning statistics per slot
-- ============================================================================

CREATE TABLE IF NOT EXISTS learning_slot_statistics_v1 (
    id BIGSERIAL PRIMARY KEY,

    environment TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    strategy TEXT NOT NULL,

    window_days INTEGER NOT NULL,

    sample_from TIMESTAMPTZ,
    sample_to TIMESTAMPTZ,

    decisions INTEGER NOT NULL DEFAULT 0,
    wins INTEGER NOT NULL DEFAULT 0,
    losses INTEGER NOT NULL DEFAULT 0,
    breakeven INTEGER NOT NULL DEFAULT 0,

    full_context_decisions INTEGER NOT NULL DEFAULT 0,
    incomplete_context_decisions INTEGER NOT NULL DEFAULT 0,
    context_coverage_pct NUMERIC(12,4),

    gross_profit_usdc NUMERIC(28,12),
    gross_loss_usdc NUMERIC(28,12),

    net_pnl_usdc NUMERIC(28,12),
    avg_net_pnl_usdc NUMERIC(28,12),
    median_net_pnl_usdc NUMERIC(28,12),
    stddev_net_pnl_usdc NUMERIC(28,12),

    win_rate_pct NUMERIC(12,4),
    profit_factor NUMERIC(28,12),
    expectancy_usdc NUMERIC(28,12),

    avoid_review_rows INTEGER NOT NULL DEFAULT 0,
    entry_quality_review_rows INTEGER NOT NULL DEFAULT 0,
    positive_confirmation_rows INTEGER NOT NULL DEFAULT 0,

    learning_status TEXT NOT NULL,
    learning_reason TEXT NOT NULL,

    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,

    calculated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_learning_slot_statistics_v1
        UNIQUE (
            environment,
            symbol,
            interval,
            strategy,
            window_days
        ),

    CONSTRAINT ck_learning_slot_statistics_window
        CHECK (window_days > 0),

    CONSTRAINT ck_learning_slot_statistics_status
        CHECK (
            learning_status IN (
                'INSUFFICIENT_SAMPLE',
                'NEGATIVE_EDGE',
                'WEAK_EDGE',
                'OBSERVE',
                'POSITIVE_EDGE',
                'STRONG_EDGE'
            )
        )
);

CREATE INDEX IF NOT EXISTS ix_learning_slot_statistics_v1_status
    ON learning_slot_statistics_v1 (
        environment,
        learning_status,
        calculated_at DESC
    );

CREATE INDEX IF NOT EXISTS ix_learning_slot_statistics_v1_slot
    ON learning_slot_statistics_v1 (
        environment,
        symbol,
        interval,
        strategy,
        window_days
    );

-- ============================================================================
-- 2. Shadow calibration proposals
-- ============================================================================

CREATE TABLE IF NOT EXISTS learning_calibration_proposals_v1 (
    id BIGSERIAL PRIMARY KEY,

    proposal_key TEXT NOT NULL UNIQUE,

    environment TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    strategy TEXT NOT NULL,

    window_days INTEGER NOT NULL,

    proposal_type TEXT NOT NULL,
    proposal_action TEXT NOT NULL,

    current_value NUMERIC(20,8),
    suggested_value NUMERIC(20,8),
    suggested_delta NUMERIC(20,8),

    confidence NUMERIC(10,6) NOT NULL,
    priority TEXT NOT NULL,

    evidence_decisions INTEGER NOT NULL DEFAULT 0,
    evidence_net_pnl_usdc NUMERIC(28,12),
    evidence_profit_factor NUMERIC(28,12),
    evidence_win_rate_pct NUMERIC(12,4),
    evidence_context_coverage_pct NUMERIC(12,4),

    reason TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,

    validation_stage TEXT NOT NULL DEFAULT 'SHADOW',
    validation_status TEXT NOT NULL DEFAULT 'PENDING',

    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    approved_at TIMESTAMPTZ,
    applied_at TIMESTAMPTZ,

    CONSTRAINT ck_learning_proposal_type
        CHECK (
            proposal_type IN (
                'SLOT_POLICY',
                'CONFIDENCE',
                'PROMOTION'
            )
        ),

    CONSTRAINT ck_learning_proposal_action
        CHECK (
            proposal_action IN (
                'BLOCK_CANDIDATE',
                'REDUCE_CONFIDENCE',
                'OBSERVE',
                'INCREASE_CONFIDENCE',
                'PROMOTE_CANDIDATE'
            )
        ),

    CONSTRAINT ck_learning_proposal_priority
        CHECK (priority IN ('P0', 'P1', 'P2', 'P3')),

    CONSTRAINT ck_learning_proposal_stage
        CHECK (
            validation_stage IN (
                'SHADOW',
                'CANDIDATE',
                'PAPER',
                'LIMITED_LIVE',
                'LIVE'
            )
        ),

    CONSTRAINT ck_learning_proposal_status
        CHECK (
            validation_status IN (
                'PENDING',
                'VALIDATING',
                'PASSED',
                'REJECTED',
                'EXPIRED',
                'APPLIED'
            )
        ),

    CONSTRAINT ck_learning_proposal_confidence
        CHECK (confidence >= 0 AND confidence <= 1)
);

CREATE INDEX IF NOT EXISTS ix_learning_calibration_proposals_status
    ON learning_calibration_proposals_v1 (
        environment,
        validation_status,
        priority,
        refreshed_at DESC
    );

CREATE INDEX IF NOT EXISTS ix_learning_calibration_proposals_slot
    ON learning_calibration_proposals_v1 (
        environment,
        symbol,
        interval,
        strategy
    );

-- ============================================================================
-- 3. Refresh function
-- ============================================================================

CREATE OR REPLACE FUNCTION refresh_learning_feedback_engine_v1(
    p_window_days INTEGER DEFAULT 30,
    p_min_observe_sample INTEGER DEFAULT 10,
    p_min_action_sample INTEGER DEFAULT 30
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_stats_upserted INTEGER := 0;
    v_proposals_upserted INTEGER := 0;
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

    -- Serialize refreshes.
    PERFORM pg_advisory_xact_lock(
        hashtext('refresh_learning_feedback_engine_v1')
    );

    -- ------------------------------------------------------------------------
    -- Aggregate closed decisions.
    -- ------------------------------------------------------------------------

    WITH source_rows AS (
        SELECT
            d.environment,
            d.symbol,
            d.interval,
            d.strategy,
            d.decision_key,
            d.net_pnl_usdc,
            d.recommendation_type,
            d.recommendation_action,
            d.missing_context_count,
            d.refreshed_at
        FROM v_decision_intelligence_v1 d
        WHERE d.decision_lifecycle_status = 'CLOSED'
          AND d.has_pnl = true
          AND d.net_pnl_usdc IS NOT NULL
          AND d.refreshed_at >= v_now - make_interval(days => p_window_days)
    ),
    decision_level AS (
        -- v_decision_intelligence_v1 can contain several recommendation rows
        -- for one decision. Collapse them before calculating trading metrics.
        SELECT
            environment,
            symbol,
            interval,
            strategy,
            decision_key,

            MAX(net_pnl_usdc) AS net_pnl_usdc,
            MAX(refreshed_at) AS refreshed_at,

            BOOL_OR(COALESCE(missing_context_count, 0) = 0)
                AS has_full_context,

            BOOL_OR(
                recommendation_type ILIKE '%AVOID%'
                OR recommendation_action ILIKE '%AVOID%'
                OR recommendation_action ILIKE '%BLOCK%'
            ) AS has_avoid_review,

            BOOL_OR(
                recommendation_type ILIKE '%ENTRY%'
                OR recommendation_action ILIKE '%ENTRY%'
            ) AS has_entry_quality_review,

            BOOL_OR(
                recommendation_type ILIKE '%POSITIVE%'
                OR recommendation_action ILIKE '%CONFIRM%'
                OR recommendation_action ILIKE '%PROMOTE%'
            ) AS has_positive_confirmation

        FROM source_rows
        GROUP BY
            environment,
            symbol,
            interval,
            strategy,
            decision_key
    ),
    aggregated AS (
        SELECT
            environment,
            symbol,
            interval,
            strategy,

            MIN(refreshed_at) AS sample_from,
            MAX(refreshed_at) AS sample_to,

            COUNT(*)::INTEGER AS decisions,

            COUNT(*) FILTER (
                WHERE net_pnl_usdc > 0
            )::INTEGER AS wins,

            COUNT(*) FILTER (
                WHERE net_pnl_usdc < 0
            )::INTEGER AS losses,

            COUNT(*) FILTER (
                WHERE net_pnl_usdc = 0
            )::INTEGER AS breakeven,

            COUNT(*) FILTER (
                WHERE has_full_context
            )::INTEGER AS full_context_decisions,

            COUNT(*) FILTER (
                WHERE NOT has_full_context
            )::INTEGER AS incomplete_context_decisions,

            SUM(net_pnl_usdc) FILTER (
                WHERE net_pnl_usdc > 0
            ) AS gross_profit_usdc,

            SUM(net_pnl_usdc) FILTER (
                WHERE net_pnl_usdc < 0
            ) AS gross_loss_usdc,

            SUM(net_pnl_usdc) AS net_pnl_usdc,
            AVG(net_pnl_usdc) AS avg_net_pnl_usdc,
            percentile_cont(0.5) WITHIN GROUP (
                ORDER BY net_pnl_usdc
            ) AS median_net_pnl_usdc,
            STDDEV_SAMP(net_pnl_usdc) AS stddev_net_pnl_usdc,

            COUNT(*) FILTER (
                WHERE has_avoid_review
            )::INTEGER AS avoid_review_rows,

            COUNT(*) FILTER (
                WHERE has_entry_quality_review
            )::INTEGER AS entry_quality_review_rows,

            COUNT(*) FILTER (
                WHERE has_positive_confirmation
            )::INTEGER AS positive_confirmation_rows

        FROM decision_level
        GROUP BY
            environment,
            symbol,
            interval,
            strategy
    ),
    classified AS (
        SELECT
            a.*,

            ROUND(
                100.0 * a.full_context_decisions
                / NULLIF(a.decisions, 0),
                4
            ) AS context_coverage_pct,

            ROUND(
                100.0 * a.wins
                / NULLIF(a.decisions, 0),
                4
            ) AS win_rate_pct,

            CASE
                WHEN COALESCE(ABS(a.gross_loss_usdc), 0) = 0
                     AND COALESCE(a.gross_profit_usdc, 0) > 0
                    THEN 999::NUMERIC

                WHEN COALESCE(ABS(a.gross_loss_usdc), 0) = 0
                    THEN 0::NUMERIC

                ELSE
                    COALESCE(a.gross_profit_usdc, 0)
                    / ABS(a.gross_loss_usdc)
            END AS profit_factor,

            a.avg_net_pnl_usdc AS expectancy_usdc

        FROM aggregated a
    ),
    final_rows AS (
        SELECT
            c.*,

            CASE
                WHEN c.decisions < p_min_observe_sample
                    THEN 'INSUFFICIENT_SAMPLE'

                WHEN c.decisions >= p_min_action_sample
                 AND c.net_pnl_usdc < 0
                 AND c.profit_factor < 0.80
                    THEN 'NEGATIVE_EDGE'

                WHEN c.net_pnl_usdc < 0
                  OR c.profit_factor < 1.00
                    THEN 'WEAK_EDGE'

                WHEN c.decisions >= p_min_action_sample
                 AND c.net_pnl_usdc > 0
                 AND c.profit_factor >= 1.50
                 AND c.context_coverage_pct >= 90
                    THEN 'STRONG_EDGE'

                WHEN c.decisions >= p_min_action_sample
                 AND c.net_pnl_usdc > 0
                 AND c.profit_factor >= 1.20
                 AND c.context_coverage_pct >= 80
                    THEN 'POSITIVE_EDGE'

                ELSE 'OBSERVE'
            END AS learning_status,

            CASE
                WHEN c.decisions < p_min_observe_sample
                    THEN format(
                        'Sample %s < minimum observe sample %s',
                        c.decisions,
                        p_min_observe_sample
                    )

                WHEN c.decisions >= p_min_action_sample
                 AND c.net_pnl_usdc < 0
                 AND c.profit_factor < 0.80
                    THEN format(
                        'Confirmed negative edge: decisions=%s net=%s PF=%s',
                        c.decisions,
                        round(c.net_pnl_usdc, 6),
                        round(c.profit_factor, 4)
                    )

                WHEN c.net_pnl_usdc < 0
                  OR c.profit_factor < 1.00
                    THEN format(
                        'Weak edge: decisions=%s net=%s PF=%s',
                        c.decisions,
                        round(c.net_pnl_usdc, 6),
                        round(c.profit_factor, 4)
                    )

                WHEN c.decisions >= p_min_action_sample
                 AND c.net_pnl_usdc > 0
                 AND c.profit_factor >= 1.50
                 AND c.context_coverage_pct >= 90
                    THEN format(
                        'Strong edge candidate: decisions=%s net=%s PF=%s coverage=%s%%',
                        c.decisions,
                        round(c.net_pnl_usdc, 6),
                        round(c.profit_factor, 4),
                        round(c.context_coverage_pct, 2)
                    )

                WHEN c.decisions >= p_min_action_sample
                 AND c.net_pnl_usdc > 0
                 AND c.profit_factor >= 1.20
                 AND c.context_coverage_pct >= 80
                    THEN format(
                        'Positive edge: decisions=%s net=%s PF=%s coverage=%s%%',
                        c.decisions,
                        round(c.net_pnl_usdc, 6),
                        round(c.profit_factor, 4),
                        round(c.context_coverage_pct, 2)
                    )

                ELSE format(
                    'Observe: decisions=%s net=%s PF=%s',
                    c.decisions,
                    round(c.net_pnl_usdc, 6),
                    round(c.profit_factor, 4)
                )
            END AS learning_reason

        FROM classified c
    )
    INSERT INTO learning_slot_statistics_v1 (
        environment,
        symbol,
        interval,
        strategy,
        window_days,
        sample_from,
        sample_to,
        decisions,
        wins,
        losses,
        breakeven,
        full_context_decisions,
        incomplete_context_decisions,
        context_coverage_pct,
        gross_profit_usdc,
        gross_loss_usdc,
        net_pnl_usdc,
        avg_net_pnl_usdc,
        median_net_pnl_usdc,
        stddev_net_pnl_usdc,
        win_rate_pct,
        profit_factor,
        expectancy_usdc,
        avoid_review_rows,
        entry_quality_review_rows,
        positive_confirmation_rows,
        learning_status,
        learning_reason,
        evidence,
        calculated_at
    )
    SELECT
        environment,
        symbol,
        interval,
        strategy,
        p_window_days,
        sample_from,
        sample_to,
        decisions,
        wins,
        losses,
        breakeven,
        full_context_decisions,
        incomplete_context_decisions,
        context_coverage_pct,
        gross_profit_usdc,
        gross_loss_usdc,
        net_pnl_usdc,
        avg_net_pnl_usdc,
        median_net_pnl_usdc,
        stddev_net_pnl_usdc,
        win_rate_pct,
        profit_factor,
        expectancy_usdc,
        avoid_review_rows,
        entry_quality_review_rows,
        positive_confirmation_rows,
        learning_status,
        learning_reason,
        jsonb_build_object(
            'engine_version', 'LEARNING_FEEDBACK_ENGINE_V1',
            'mode', 'SHADOW_ADVISOR',
            'window_days', p_window_days,
            'min_observe_sample', p_min_observe_sample,
            'min_action_sample', p_min_action_sample,
            'sample_from', sample_from,
            'sample_to', sample_to
        ),
        v_now
    FROM final_rows
    ON CONFLICT (
        environment,
        symbol,
        interval,
        strategy,
        window_days
    )
    DO UPDATE SET
        sample_from = EXCLUDED.sample_from,
        sample_to = EXCLUDED.sample_to,
        decisions = EXCLUDED.decisions,
        wins = EXCLUDED.wins,
        losses = EXCLUDED.losses,
        breakeven = EXCLUDED.breakeven,
        full_context_decisions = EXCLUDED.full_context_decisions,
        incomplete_context_decisions =
            EXCLUDED.incomplete_context_decisions,
        context_coverage_pct = EXCLUDED.context_coverage_pct,
        gross_profit_usdc = EXCLUDED.gross_profit_usdc,
        gross_loss_usdc = EXCLUDED.gross_loss_usdc,
        net_pnl_usdc = EXCLUDED.net_pnl_usdc,
        avg_net_pnl_usdc = EXCLUDED.avg_net_pnl_usdc,
        median_net_pnl_usdc = EXCLUDED.median_net_pnl_usdc,
        stddev_net_pnl_usdc = EXCLUDED.stddev_net_pnl_usdc,
        win_rate_pct = EXCLUDED.win_rate_pct,
        profit_factor = EXCLUDED.profit_factor,
        expectancy_usdc = EXCLUDED.expectancy_usdc,
        avoid_review_rows = EXCLUDED.avoid_review_rows,
        entry_quality_review_rows =
            EXCLUDED.entry_quality_review_rows,
        positive_confirmation_rows =
            EXCLUDED.positive_confirmation_rows,
        learning_status = EXCLUDED.learning_status,
        learning_reason = EXCLUDED.learning_reason,
        evidence = EXCLUDED.evidence,
        calculated_at = EXCLUDED.calculated_at;

    GET DIAGNOSTICS v_stats_upserted = ROW_COUNT;

    -- ------------------------------------------------------------------------
    -- Generate shadow-only proposals.
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
                        LEAST(s.decisions, p_min_action_sample * 3)::NUMERIC
                        / NULLIF(p_min_action_sample * 3, 0)
                    ) * 0.55
                    +
                    LEAST(
                        COALESCE(s.context_coverage_pct, 0) / 100.0,
                        1.0
                    ) * 0.25
                    +
                    CASE
                        WHEN s.learning_status IN (
                            'NEGATIVE_EDGE',
                            'STRONG_EDGE'
                        ) THEN 0.20
                        WHEN s.learning_status IN (
                            'WEAK_EDGE',
                            'POSITIVE_EDGE'
                        ) THEN 0.10
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
                environment,
                symbol,
                interval,
                strategy,
                p_window_days,
                proposal_type,
                proposal_action
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
            'avoid_review_rows', avoid_review_rows,
            'entry_quality_review_rows', entry_quality_review_rows,
            'positive_confirmation_rows',
                positive_confirmation_rows,
            'source_table', 'learning_slot_statistics_v1',
            'engine_version', 'LEARNING_FEEDBACK_ENGINE_V1',
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
        last_seen_at = EXCLUDED.last_seen_at,
        refreshed_at = EXCLUDED.refreshed_at,
        validation_status = CASE
            WHEN learning_calibration_proposals_v1.validation_status
                 IN ('APPLIED', 'REJECTED')
                THEN learning_calibration_proposals_v1.validation_status
            ELSE 'PENDING'
        END;

    GET DIAGNOSTICS v_proposals_upserted = ROW_COUNT;

    v_result := jsonb_build_object(
        'status', 'ok',
        'engine_version', 'LEARNING_FEEDBACK_ENGINE_V1',
        'mode', 'SHADOW_ADVISOR',
        'apply_enabled', false,
        'window_days', p_window_days,
        'min_observe_sample', p_min_observe_sample,
        'min_action_sample', p_min_action_sample,
        'slot_statistics_upserted', v_stats_upserted,
        'proposals_upserted', v_proposals_upserted,
        'refreshed_at', v_now
    );

    INSERT INTO automation_kv (
        key,
        value,
        updated_at
    )
    VALUES (
        'learning_feedback_engine_v1_last_stats',
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
        'learning_feedback_engine_v1_last_status',
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
        'learning_feedback_engine_v1_apply_enabled',
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
            'learning_feedback_engine_v1_last_status',
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
            'learning_feedback_engine_v1_last_error',
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

-- ============================================================================
-- 4. Explain views
-- ============================================================================

CREATE OR REPLACE VIEW v_learning_slot_statistics_v1 AS
SELECT
    environment,
    symbol,
    interval,
    strategy,
    window_days,
    sample_from,
    sample_to,
    decisions,
    wins,
    losses,
    breakeven,
    full_context_decisions,
    incomplete_context_decisions,
    context_coverage_pct,
    net_pnl_usdc,
    avg_net_pnl_usdc,
    median_net_pnl_usdc,
    stddev_net_pnl_usdc,
    win_rate_pct,
    profit_factor,
    expectancy_usdc,
    avoid_review_rows,
    entry_quality_review_rows,
    positive_confirmation_rows,
    learning_status,
    learning_reason,
    calculated_at
FROM learning_slot_statistics_v1;

CREATE OR REPLACE VIEW v_learning_calibration_proposals_v1 AS
SELECT
    id,
    proposal_key,
    environment,
    symbol,
    interval,
    strategy,
    window_days,
    proposal_type,
    proposal_action,
    suggested_delta,
    confidence,
    priority,
    evidence_decisions,
    evidence_net_pnl_usdc,
    evidence_profit_factor,
    evidence_win_rate_pct,
    evidence_context_coverage_pct,
    reason,
    validation_stage,
    validation_status,
    first_seen_at,
    last_seen_at,
    refreshed_at,
    evidence
FROM learning_calibration_proposals_v1;

CREATE OR REPLACE VIEW v_learning_feedback_engine_summary_v1 AS
SELECT
    environment,
    window_days,
    learning_status,
    COUNT(*) AS slots,
    SUM(decisions) AS decisions,
    ROUND(SUM(net_pnl_usdc)::NUMERIC, 8) AS net_pnl_usdc,
    ROUND(AVG(profit_factor)::NUMERIC, 6) AS avg_profit_factor,
    ROUND(AVG(win_rate_pct)::NUMERIC, 4) AS avg_win_rate_pct,
    ROUND(AVG(context_coverage_pct)::NUMERIC, 4)
        AS avg_context_coverage_pct,
    MAX(calculated_at) AS calculated_at
FROM learning_slot_statistics_v1
GROUP BY
    environment,
    window_days,
    learning_status;

-- Explicitly document that V1 cannot apply anything.
COMMENT ON TABLE learning_calibration_proposals_v1 IS
'Shadow-only learning recommendations. V1 must not modify ORC, bot_control, runtime parameters or capital allocation.';

COMMENT ON FUNCTION refresh_learning_feedback_engine_v1(
    INTEGER,
    INTEGER,
    INTEGER
) IS
'Builds slot statistics and shadow calibration proposals. No production apply path exists in V1.';

COMMIT;

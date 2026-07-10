BEGIN;

-- ============================================================================
-- WALTRADE — LEARNING FEEDBACK ENGINE V1.3
--
-- Purpose:
--   Persistent validation of recommendation stability across successful
--   Learning Feedback Engine refreshes.
--
-- V1.3 validates:
--   - action persistence,
--   - sample sufficiency,
--   - confidence persistence,
--   - evidence monotonicity,
--   - minimum validation duration.
--
-- V1.3 does NOT:
--   - update bot_control,
--   - update ORC weights,
--   - update runtime confidence,
--   - promote slots,
--   - allocate capital,
--   - place orders.
--
-- Existing V1.2 scheduler remains unchanged.
-- ============================================================================

DO $$
BEGIN
    IF to_regclass(
        'public.learning_calibration_proposals_v1'
    ) IS NULL THEN
        RAISE EXCEPTION
            'Required table learning_calibration_proposals_v1 does not exist';
    END IF;

    IF to_regclass(
        'public.learning_feedback_refresh_runs_v1'
    ) IS NULL THEN
        RAISE EXCEPTION
            'Required table learning_feedback_refresh_runs_v1 does not exist';
    END IF;

    IF to_regclass('public.automation_kv') IS NULL THEN
        RAISE EXCEPTION
            'Required table automation_kv does not exist';
    END IF;
END
$$;

-- ============================================================================
-- 1. Immutable observation history
-- ============================================================================

CREATE TABLE IF NOT EXISTS learning_proposal_observations_v1 (
    id BIGSERIAL PRIMARY KEY,

    refresh_run_id BIGINT NOT NULL,
    environment TEXT NOT NULL,

    proposal_key TEXT NOT NULL,

    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    strategy TEXT NOT NULL,
    window_days INTEGER NOT NULL,

    proposal_type TEXT NOT NULL,
    proposal_action TEXT NOT NULL,

    suggested_delta NUMERIC(20,8),
    confidence NUMERIC(10,6) NOT NULL,

    priority TEXT NOT NULL,

    evidence_decisions INTEGER NOT NULL,
    evidence_net_pnl_usdc NUMERIC(28,12),
    evidence_profit_factor NUMERIC(28,12),
    evidence_win_rate_pct NUMERIC(12,4),
    evidence_context_coverage_pct NUMERIC(12,4),

    source_validation_stage TEXT NOT NULL,
    source_validation_status TEXT NOT NULL,

    reason TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,

    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_learning_proposal_observation_run_slot
        UNIQUE (
            refresh_run_id,
            environment,
            symbol,
            interval,
            strategy,
            window_days
        ),

    CONSTRAINT ck_learning_proposal_observation_confidence
        CHECK (confidence >= 0 AND confidence <= 1),

    CONSTRAINT ck_learning_proposal_observation_window
        CHECK (window_days > 0),

    CONSTRAINT ck_learning_proposal_observation_decisions
        CHECK (evidence_decisions >= 0),

    CONSTRAINT fk_learning_proposal_observation_refresh
        FOREIGN KEY (refresh_run_id)
        REFERENCES learning_feedback_refresh_runs_v1(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS
    ix_learning_proposal_observations_slot_time
ON learning_proposal_observations_v1 (
    environment,
    symbol,
    interval,
    strategy,
    window_days,
    observed_at DESC
);

CREATE INDEX IF NOT EXISTS
    ix_learning_proposal_observations_action_time
ON learning_proposal_observations_v1 (
    proposal_action,
    observed_at DESC
);

CREATE INDEX IF NOT EXISTS
    ix_learning_proposal_observations_refresh
ON learning_proposal_observations_v1 (
    refresh_run_id
);

-- ============================================================================
-- 2. Current persistent validation state
-- ============================================================================

CREATE TABLE IF NOT EXISTS learning_proposal_validation_state_v1 (
    id BIGSERIAL PRIMARY KEY,

    environment TEXT NOT NULL,

    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    strategy TEXT NOT NULL,
    window_days INTEGER NOT NULL,

    current_proposal_key TEXT NOT NULL,
    current_proposal_type TEXT NOT NULL,
    current_proposal_action TEXT NOT NULL,

    validation_status TEXT NOT NULL,

    consecutive_observations INTEGER NOT NULL DEFAULT 0,
    total_observations INTEGER NOT NULL DEFAULT 0,
    action_change_count INTEGER NOT NULL DEFAULT 0,

    first_observed_at TIMESTAMPTZ NOT NULL,
    action_first_observed_at TIMESTAMPTZ NOT NULL,
    last_observed_at TIMESTAMPTZ NOT NULL,

    validation_started_at TIMESTAMPTZ,
    stable_at TIMESTAMPTZ,
    reset_at TIMESTAMPTZ,

    latest_confidence NUMERIC(10,6) NOT NULL,
    minimum_confidence NUMERIC(10,6) NOT NULL,
    maximum_confidence NUMERIC(10,6) NOT NULL,
    average_confidence NUMERIC(10,6) NOT NULL,

    latest_evidence_decisions INTEGER NOT NULL,
    previous_evidence_decisions INTEGER,
    maximum_evidence_decisions INTEGER NOT NULL,

    latest_net_pnl_usdc NUMERIC(28,12),
    latest_profit_factor NUMERIC(28,12),
    latest_win_rate_pct NUMERIC(12,4),
    latest_context_coverage_pct NUMERIC(12,4),

    required_observations INTEGER NOT NULL,
    required_span_hours INTEGER NOT NULL,
    minimum_action_sample INTEGER NOT NULL,
    minimum_confidence_required NUMERIC(10,6) NOT NULL,

    action_is_consistent BOOLEAN NOT NULL DEFAULT false,
    evidence_is_non_decreasing BOOLEAN NOT NULL DEFAULT true,
    sample_is_sufficient BOOLEAN NOT NULL DEFAULT false,
    confidence_is_sufficient BOOLEAN NOT NULL DEFAULT false,
    span_is_sufficient BOOLEAN NOT NULL DEFAULT false,

    validation_reason TEXT NOT NULL,
    validation_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,

    last_refresh_run_id BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_learning_proposal_validation_slot
        UNIQUE (
            environment,
            symbol,
            interval,
            strategy,
            window_days
        ),

    CONSTRAINT ck_learning_validation_status
        CHECK (
            validation_status IN (
                'OBSERVE_ONLY',
                'VALIDATING',
                'STABLE',
                'RESET'
            )
        ),

    CONSTRAINT ck_learning_validation_observations
        CHECK (
            consecutive_observations >= 0
            AND total_observations >= 0
            AND action_change_count >= 0
            AND required_observations >= 1
        ),

    CONSTRAINT ck_learning_validation_hours
        CHECK (required_span_hours >= 0),

    CONSTRAINT ck_learning_validation_sample
        CHECK (
            minimum_action_sample >= 1
            AND latest_evidence_decisions >= 0
            AND maximum_evidence_decisions >= 0
        ),

    CONSTRAINT ck_learning_validation_confidence
        CHECK (
            latest_confidence >= 0
            AND latest_confidence <= 1
            AND minimum_confidence >= 0
            AND minimum_confidence <= 1
            AND maximum_confidence >= 0
            AND maximum_confidence <= 1
            AND average_confidence >= 0
            AND average_confidence <= 1
            AND minimum_confidence_required >= 0
            AND minimum_confidence_required <= 1
        ),

    CONSTRAINT fk_learning_validation_refresh
        FOREIGN KEY (last_refresh_run_id)
        REFERENCES learning_feedback_refresh_runs_v1(id)
);

CREATE INDEX IF NOT EXISTS
    ix_learning_proposal_validation_status
ON learning_proposal_validation_state_v1 (
    environment,
    validation_status,
    updated_at DESC
);

CREATE INDEX IF NOT EXISTS
    ix_learning_proposal_validation_action
ON learning_proposal_validation_state_v1 (
    environment,
    current_proposal_action,
    validation_status
);

-- ============================================================================
-- 3. Validation run audit
-- ============================================================================

CREATE TABLE IF NOT EXISTS learning_proposal_validation_runs_v1 (
    id BIGSERIAL PRIMARY KEY,

    refresh_run_id BIGINT NOT NULL UNIQUE,
    environment TEXT NOT NULL,

    engine_version TEXT NOT NULL,

    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,

    status TEXT NOT NULL,

    observations_inserted INTEGER NOT NULL DEFAULT 0,
    states_upserted INTEGER NOT NULL DEFAULT 0,

    observe_only_states INTEGER NOT NULL DEFAULT 0,
    validating_states INTEGER NOT NULL DEFAULT 0,
    stable_states INTEGER NOT NULL DEFAULT 0,
    reset_states INTEGER NOT NULL DEFAULT 0,

    result JSONB,
    error_text TEXT,

    CONSTRAINT ck_learning_validation_run_status
        CHECK (status IN ('RUNNING', 'OK', 'ERROR')),

    CONSTRAINT fk_learning_validation_run_refresh
        FOREIGN KEY (refresh_run_id)
        REFERENCES learning_feedback_refresh_runs_v1(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS
    ix_learning_proposal_validation_runs_latest
ON learning_proposal_validation_runs_v1 (
    environment,
    started_at DESC
);

-- ============================================================================
-- 4. Validation configuration
--
-- Explicit V1.3 policy:
--   - refresh cadence is 12 hours,
--   - 3 consecutive observations cover approximately 24 hours,
--   - actionable recommendation still requires 30 decisions,
--   - stability confidence floor is 0.60.
--
-- These values are configuration, not hard-coded trading parameters.
-- They can be changed through automation_kv without code changes.
-- ============================================================================

INSERT INTO automation_kv (
    key,
    value,
    updated_at
)
VALUES
    (
        'learning_feedback_validation_enabled',
        '1',
        now()
    ),
    (
        'learning_feedback_validation_required_observations',
        '3',
        now()
    ),
    (
        'learning_feedback_validation_required_span_hours',
        '24',
        now()
    ),
    (
        'learning_feedback_validation_minimum_action_sample',
        '30',
        now()
    ),
    (
        'learning_feedback_validation_minimum_confidence',
        '0.60',
        now()
    ),
    (
        'learning_feedback_validation_apply_enabled',
        '0',
        now()
    )
ON CONFLICT (key)
DO NOTHING;

-- ============================================================================
-- 5. Validation function
-- ============================================================================

CREATE OR REPLACE FUNCTION refresh_learning_proposal_validation_v1_3(
    p_refresh_run_id BIGINT
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := clock_timestamp();

    v_environment TEXT;
    v_refresh_status TEXT;

    v_enabled BOOLEAN := true;

    v_required_observations INTEGER := 3;
    v_required_span_hours INTEGER := 24;
    v_minimum_action_sample INTEGER := 30;
    v_minimum_confidence NUMERIC(10,6) := 0.60;

    v_validation_run_id BIGINT;

    v_observations_inserted INTEGER := 0;
    v_states_upserted INTEGER := 0;

    v_observe_only_states INTEGER := 0;
    v_validating_states INTEGER := 0;
    v_stable_states INTEGER := 0;
    v_reset_states INTEGER := 0;

    v_result JSONB;
BEGIN
    IF p_refresh_run_id IS NULL THEN
        RAISE EXCEPTION 'p_refresh_run_id cannot be null';
    END IF;

    SELECT
        environment,
        status
    INTO
        v_environment,
        v_refresh_status
    FROM learning_feedback_refresh_runs_v1
    WHERE id = p_refresh_run_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Learning feedback refresh run % does not exist',
            p_refresh_run_id;
    END IF;

    IF v_refresh_status <> 'OK' THEN
        RAISE EXCEPTION
            'Learning feedback refresh run % has status %, expected OK',
            p_refresh_run_id,
            v_refresh_status;
    END IF;

    SELECT
        CASE
            WHEN lower(trim(value)) IN ('1', 'true', 'yes', 'on')
                THEN true
            ELSE false
        END
    INTO v_enabled
    FROM automation_kv
    WHERE key = 'learning_feedback_validation_enabled';

    v_enabled := COALESCE(v_enabled, true);

    IF NOT v_enabled THEN
        RETURN jsonb_build_object(
            'status', 'disabled',
            'engine_version',
                'LEARNING_FEEDBACK_VALIDATION_V1_3',
            'refresh_run_id', p_refresh_run_id,
            'environment', v_environment,
            'checked_at', v_now
        );
    END IF;

    SELECT COALESCE(
        (
            SELECT value::INTEGER
            FROM automation_kv
            WHERE key =
                'learning_feedback_validation_required_observations'
        ),
        3
    )
    INTO v_required_observations;

    SELECT COALESCE(
        (
            SELECT value::INTEGER
            FROM automation_kv
            WHERE key =
                'learning_feedback_validation_required_span_hours'
        ),
        24
    )
    INTO v_required_span_hours;

    SELECT COALESCE(
        (
            SELECT value::INTEGER
            FROM automation_kv
            WHERE key =
                'learning_feedback_validation_minimum_action_sample'
        ),
        30
    )
    INTO v_minimum_action_sample;

    SELECT COALESCE(
        (
            SELECT value::NUMERIC
            FROM automation_kv
            WHERE key =
                'learning_feedback_validation_minimum_confidence'
        ),
        0.60
    )
    INTO v_minimum_confidence;

    IF v_required_observations < 1 THEN
        RAISE EXCEPTION
            'required observations must be >= 1';
    END IF;

    IF v_required_span_hours < 0 THEN
        RAISE EXCEPTION
            'required span hours must be >= 0';
    END IF;

    IF v_minimum_action_sample < 1 THEN
        RAISE EXCEPTION
            'minimum action sample must be >= 1';
    END IF;

    IF v_minimum_confidence < 0
       OR v_minimum_confidence > 1
    THEN
        RAISE EXCEPTION
            'minimum confidence must be between 0 and 1';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtext(
            'refresh_learning_proposal_validation_v1_3'
            || ':'
            || v_environment
        )
    );

    INSERT INTO learning_proposal_validation_runs_v1 (
        refresh_run_id,
        environment,
        engine_version,
        started_at,
        status
    )
    VALUES (
        p_refresh_run_id,
        v_environment,
        'LEARNING_FEEDBACK_VALIDATION_V1_3',
        v_now,
        'RUNNING'
    )
    ON CONFLICT (refresh_run_id)
    DO NOTHING
    RETURNING id
    INTO v_validation_run_id;

    -- Idempotency: if this refresh run was already validated, return the
    -- stored result instead of processing it again.
    IF v_validation_run_id IS NULL THEN
        SELECT result
        INTO v_result
        FROM learning_proposal_validation_runs_v1
        WHERE refresh_run_id = p_refresh_run_id;

        RETURN COALESCE(
            v_result,
            jsonb_build_object(
                'status', 'already_processed',
                'engine_version',
                    'LEARNING_FEEDBACK_VALIDATION_V1_3',
                'refresh_run_id', p_refresh_run_id,
                'environment', v_environment
            )
        );
    END IF;

    -- ------------------------------------------------------------------------
    -- Snapshot current proposals produced by the successful V1.2 refresh.
    -- ------------------------------------------------------------------------

    INSERT INTO learning_proposal_observations_v1 (
        refresh_run_id,
        environment,
        proposal_key,
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
        source_validation_stage,
        source_validation_status,
        reason,
        evidence,
        observed_at
    )
    SELECT
        p_refresh_run_id,
        p.environment,
        p.proposal_key,
        p.symbol,
        p.interval,
        p.strategy,
        p.window_days,
        p.proposal_type,
        p.proposal_action,
        p.suggested_delta,
        p.confidence,
        p.priority,
        p.evidence_decisions,
        p.evidence_net_pnl_usdc,
        p.evidence_profit_factor,
        p.evidence_win_rate_pct,
        p.evidence_context_coverage_pct,
        p.validation_stage,
        p.validation_status,
        p.reason,
        p.evidence,
        v_now
    FROM learning_calibration_proposals_v1 p
    WHERE p.environment = v_environment
      AND p.validation_stage = 'SHADOW'
      AND p.validation_status = 'PENDING'
    ON CONFLICT (
        refresh_run_id,
        environment,
        symbol,
        interval,
        strategy,
        window_days
    )
    DO NOTHING;

    GET DIAGNOSTICS v_observations_inserted = ROW_COUNT;

    -- ------------------------------------------------------------------------
    -- Build latest observation plus previous persistent state.
    -- ------------------------------------------------------------------------

    WITH latest AS (
        SELECT
            o.*
        FROM learning_proposal_observations_v1 o
        WHERE o.refresh_run_id = p_refresh_run_id
          AND o.environment = v_environment
    ),
    calculated AS (
        SELECT
            l.*,

            s.id AS previous_state_id,
            s.current_proposal_action AS previous_action,
            s.consecutive_observations AS previous_consecutive,
            s.total_observations AS previous_total,
            s.action_change_count AS previous_change_count,
            s.first_observed_at AS previous_first_observed_at,
            s.action_first_observed_at
                AS previous_action_first_observed_at,
            s.minimum_confidence AS previous_minimum_confidence,
            s.maximum_confidence AS previous_maximum_confidence,
            s.average_confidence AS previous_average_confidence,
            s.latest_evidence_decisions
                AS previous_evidence_decisions,
            s.maximum_evidence_decisions
                AS previous_maximum_evidence_decisions,

            CASE
                WHEN s.id IS NULL THEN 1
                WHEN s.current_proposal_action = l.proposal_action
                    THEN s.consecutive_observations + 1
                ELSE 1
            END AS new_consecutive_observations,

            CASE
                WHEN s.id IS NULL THEN 1
                ELSE s.total_observations + 1
            END AS new_total_observations,

            CASE
                WHEN s.id IS NULL THEN 0
                WHEN s.current_proposal_action = l.proposal_action
                    THEN s.action_change_count
                ELSE s.action_change_count + 1
            END AS new_action_change_count,

            CASE
                WHEN s.id IS NULL
                  OR s.current_proposal_action <> l.proposal_action
                    THEN l.observed_at
                ELSE s.action_first_observed_at
            END AS new_action_first_observed_at,

            CASE
                WHEN s.id IS NULL THEN l.confidence
                WHEN s.current_proposal_action <> l.proposal_action
                    THEN l.confidence
                ELSE LEAST(
                    s.minimum_confidence,
                    l.confidence
                )
            END AS new_minimum_confidence,

            CASE
                WHEN s.id IS NULL THEN l.confidence
                WHEN s.current_proposal_action <> l.proposal_action
                    THEN l.confidence
                ELSE GREATEST(
                    s.maximum_confidence,
                    l.confidence
                )
            END AS new_maximum_confidence,

            CASE
                WHEN s.id IS NULL THEN l.confidence
                WHEN s.current_proposal_action <> l.proposal_action
                    THEN l.confidence
                ELSE (
                    (
                        s.average_confidence
                        * s.consecutive_observations
                    )
                    + l.confidence
                )
                /
                NULLIF(
                    s.consecutive_observations + 1,
                    0
                )
            END AS new_average_confidence,

            CASE
                WHEN s.id IS NULL THEN true
                WHEN s.current_proposal_action
                     <> l.proposal_action
                    THEN true
                ELSE
                    l.evidence_decisions
                    >= s.latest_evidence_decisions
            END AS new_evidence_non_decreasing

        FROM latest l
        LEFT JOIN learning_proposal_validation_state_v1 s
          ON s.environment = l.environment
         AND s.symbol = l.symbol
         AND s.interval = l.interval
         AND s.strategy = l.strategy
         AND s.window_days = l.window_days
    ),
    classified AS (
        SELECT
            c.*,

            (
                c.new_consecutive_observations
                >= v_required_observations
            ) AS observations_sufficient,

            (
                c.observed_at
                >= (
                    c.new_action_first_observed_at
                    + make_interval(
                        hours => v_required_span_hours
                    )
                )
            ) AS span_sufficient,

            (
                c.evidence_decisions
                >= v_minimum_action_sample
            ) AS sample_sufficient,

            (
                c.new_minimum_confidence
                >= v_minimum_confidence
            ) AS confidence_sufficient,

            CASE
                WHEN c.proposal_action = 'OBSERVE'
                    THEN 'OBSERVE_ONLY'

                WHEN c.previous_state_id IS NOT NULL
                 AND c.previous_action <> c.proposal_action
                    THEN 'RESET'

                WHEN c.new_consecutive_observations
                     >= v_required_observations
                 AND c.observed_at
                     >= (
                        c.new_action_first_observed_at
                        + make_interval(
                            hours => v_required_span_hours
                        )
                     )
                 AND c.evidence_decisions
                     >= v_minimum_action_sample
                 AND c.new_minimum_confidence
                     >= v_minimum_confidence
                 AND c.new_evidence_non_decreasing
                    THEN 'STABLE'

                ELSE 'VALIDATING'
            END AS new_validation_status

        FROM calculated c
    )
    INSERT INTO learning_proposal_validation_state_v1 (
        environment,
        symbol,
        interval,
        strategy,
        window_days,
        current_proposal_key,
        current_proposal_type,
        current_proposal_action,
        validation_status,
        consecutive_observations,
        total_observations,
        action_change_count,
        first_observed_at,
        action_first_observed_at,
        last_observed_at,
        validation_started_at,
        stable_at,
        reset_at,
        latest_confidence,
        minimum_confidence,
        maximum_confidence,
        average_confidence,
        latest_evidence_decisions,
        previous_evidence_decisions,
        maximum_evidence_decisions,
        latest_net_pnl_usdc,
        latest_profit_factor,
        latest_win_rate_pct,
        latest_context_coverage_pct,
        required_observations,
        required_span_hours,
        minimum_action_sample,
        minimum_confidence_required,
        action_is_consistent,
        evidence_is_non_decreasing,
        sample_is_sufficient,
        confidence_is_sufficient,
        span_is_sufficient,
        validation_reason,
        validation_evidence,
        last_refresh_run_id,
        created_at,
        updated_at
    )
    SELECT
        environment,
        symbol,
        interval,
        strategy,
        window_days,
        proposal_key,
        proposal_type,
        proposal_action,
        new_validation_status,
        new_consecutive_observations,
        new_total_observations,
        new_action_change_count,
        COALESCE(previous_first_observed_at, observed_at),
        new_action_first_observed_at,
        observed_at,

        CASE
            WHEN proposal_action = 'OBSERVE'
                THEN NULL
            WHEN previous_state_id IS NULL
              OR previous_action <> proposal_action
                THEN observed_at
            ELSE (
                SELECT validation_started_at
                FROM learning_proposal_validation_state_v1 existing
                WHERE existing.environment =
                    classified.environment
                  AND existing.symbol =
                    classified.symbol
                  AND existing.interval =
                    classified.interval
                  AND existing.strategy =
                    classified.strategy
                  AND existing.window_days =
                    classified.window_days
            )
        END,

        CASE
            WHEN new_validation_status = 'STABLE'
                THEN observed_at
            ELSE NULL
        END,

        CASE
            WHEN new_validation_status = 'RESET'
                THEN observed_at
            ELSE NULL
        END,

        confidence,
        new_minimum_confidence,
        new_maximum_confidence,
        new_average_confidence,

        evidence_decisions,
        previous_evidence_decisions,
        GREATEST(
            COALESCE(
                previous_maximum_evidence_decisions,
                0
            ),
            evidence_decisions
        ),

        evidence_net_pnl_usdc,
        evidence_profit_factor,
        evidence_win_rate_pct,
        evidence_context_coverage_pct,

        v_required_observations,
        v_required_span_hours,
        v_minimum_action_sample,
        v_minimum_confidence,

        (
            new_consecutive_observations
            >= v_required_observations
        ),
        new_evidence_non_decreasing,
        sample_sufficient,
        confidence_sufficient,
        span_sufficient,

        CASE
            WHEN proposal_action = 'OBSERVE'
                THEN format(
                    'Observe-only recommendation; decisions=%s confidence=%s',
                    evidence_decisions,
                    round(confidence, 4)
                )

            WHEN new_validation_status = 'RESET'
                THEN format(
                    'Recommendation changed from %s to %s; validation sequence reset',
                    previous_action,
                    proposal_action
                )

            WHEN new_validation_status = 'STABLE'
                THEN format(
                    'Stable recommendation: action=%s observations=%s span_hours=%s decisions=%s min_confidence=%s',
                    proposal_action,
                    new_consecutive_observations,
                    round(
                        EXTRACT(
                            EPOCH FROM (
                                observed_at
                                - new_action_first_observed_at
                            )
                        ) / 3600.0,
                        2
                    ),
                    evidence_decisions,
                    round(new_minimum_confidence, 4)
                )

            ELSE format(
                'Validating recommendation: action=%s observations=%s/%s span_ok=%s sample_ok=%s confidence_ok=%s evidence_non_decreasing=%s',
                proposal_action,
                new_consecutive_observations,
                v_required_observations,
                span_sufficient,
                sample_sufficient,
                confidence_sufficient,
                new_evidence_non_decreasing
            )
        END,

        jsonb_build_object(
            'engine_version',
                'LEARNING_FEEDBACK_VALIDATION_V1_3',
            'refresh_run_id',
                p_refresh_run_id,
            'proposal_action',
                proposal_action,
            'previous_action',
                previous_action,
            'consecutive_observations',
                new_consecutive_observations,
            'total_observations',
                new_total_observations,
            'required_observations',
                v_required_observations,
            'required_span_hours',
                v_required_span_hours,
            'minimum_action_sample',
                v_minimum_action_sample,
            'minimum_confidence',
                v_minimum_confidence,
            'observations_sufficient',
                observations_sufficient,
            'span_sufficient',
                span_sufficient,
            'sample_sufficient',
                sample_sufficient,
            'confidence_sufficient',
                confidence_sufficient,
            'evidence_non_decreasing',
                new_evidence_non_decreasing,
            'apply_allowed',
                false
        ),

        p_refresh_run_id,
        v_now,
        v_now

    FROM classified

    ON CONFLICT (
        environment,
        symbol,
        interval,
        strategy,
        window_days
    )
    DO UPDATE SET
        current_proposal_key =
            EXCLUDED.current_proposal_key,
        current_proposal_type =
            EXCLUDED.current_proposal_type,
        current_proposal_action =
            EXCLUDED.current_proposal_action,
        validation_status =
            EXCLUDED.validation_status,
        consecutive_observations =
            EXCLUDED.consecutive_observations,
        total_observations =
            EXCLUDED.total_observations,
        action_change_count =
            EXCLUDED.action_change_count,
        action_first_observed_at =
            EXCLUDED.action_first_observed_at,
        last_observed_at =
            EXCLUDED.last_observed_at,
        validation_started_at =
            EXCLUDED.validation_started_at,
        stable_at =
            EXCLUDED.stable_at,
        reset_at =
            EXCLUDED.reset_at,
        latest_confidence =
            EXCLUDED.latest_confidence,
        minimum_confidence =
            EXCLUDED.minimum_confidence,
        maximum_confidence =
            EXCLUDED.maximum_confidence,
        average_confidence =
            EXCLUDED.average_confidence,
        previous_evidence_decisions =
            learning_proposal_validation_state_v1
                .latest_evidence_decisions,
        latest_evidence_decisions =
            EXCLUDED.latest_evidence_decisions,
        maximum_evidence_decisions =
            EXCLUDED.maximum_evidence_decisions,
        latest_net_pnl_usdc =
            EXCLUDED.latest_net_pnl_usdc,
        latest_profit_factor =
            EXCLUDED.latest_profit_factor,
        latest_win_rate_pct =
            EXCLUDED.latest_win_rate_pct,
        latest_context_coverage_pct =
            EXCLUDED.latest_context_coverage_pct,
        required_observations =
            EXCLUDED.required_observations,
        required_span_hours =
            EXCLUDED.required_span_hours,
        minimum_action_sample =
            EXCLUDED.minimum_action_sample,
        minimum_confidence_required =
            EXCLUDED.minimum_confidence_required,
        action_is_consistent =
            EXCLUDED.action_is_consistent,
        evidence_is_non_decreasing =
            EXCLUDED.evidence_is_non_decreasing,
        sample_is_sufficient =
            EXCLUDED.sample_is_sufficient,
        confidence_is_sufficient =
            EXCLUDED.confidence_is_sufficient,
        span_is_sufficient =
            EXCLUDED.span_is_sufficient,
        validation_reason =
            EXCLUDED.validation_reason,
        validation_evidence =
            EXCLUDED.validation_evidence,
        last_refresh_run_id =
            EXCLUDED.last_refresh_run_id,
        updated_at =
            EXCLUDED.updated_at;

    GET DIAGNOSTICS v_states_upserted = ROW_COUNT;

    SELECT
        COUNT(*) FILTER (
            WHERE validation_status = 'OBSERVE_ONLY'
        ),
        COUNT(*) FILTER (
            WHERE validation_status = 'VALIDATING'
        ),
        COUNT(*) FILTER (
            WHERE validation_status = 'STABLE'
        ),
        COUNT(*) FILTER (
            WHERE validation_status = 'RESET'
        )
    INTO
        v_observe_only_states,
        v_validating_states,
        v_stable_states,
        v_reset_states
    FROM learning_proposal_validation_state_v1
    WHERE environment = v_environment;

    v_result := jsonb_build_object(
        'status', 'ok',
        'engine_version',
            'LEARNING_FEEDBACK_VALIDATION_V1_3',
        'refresh_run_id', p_refresh_run_id,
        'environment', v_environment,
        'observations_inserted',
            v_observations_inserted,
        'states_upserted',
            v_states_upserted,
        'observe_only_states',
            v_observe_only_states,
        'validating_states',
            v_validating_states,
        'stable_states',
            v_stable_states,
        'reset_states',
            v_reset_states,
        'required_observations',
            v_required_observations,
        'required_span_hours',
            v_required_span_hours,
        'minimum_action_sample',
            v_minimum_action_sample,
        'minimum_confidence',
            v_minimum_confidence,
        'apply_enabled',
            false,
        'finished_at',
            clock_timestamp()
    );

    UPDATE learning_proposal_validation_runs_v1
    SET
        status = 'OK',
        finished_at = clock_timestamp(),
        observations_inserted =
            v_observations_inserted,
        states_upserted =
            v_states_upserted,
        observe_only_states =
            v_observe_only_states,
        validating_states =
            v_validating_states,
        stable_states =
            v_stable_states,
        reset_states =
            v_reset_states,
        result =
            v_result
    WHERE id = v_validation_run_id;

    INSERT INTO automation_kv (
        key,
        value,
        updated_at
    )
    VALUES (
        'learning_feedback_validation_v1_3_last_status',
        'ok',
        clock_timestamp()
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
        'learning_feedback_validation_v1_3_last_stats',
        v_result::TEXT,
        clock_timestamp()
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
        'learning_feedback_validation_apply_enabled',
        '0',
        clock_timestamp()
    )
    ON CONFLICT (key)
    DO UPDATE SET
        value = EXCLUDED.value,
        updated_at = EXCLUDED.updated_at;

    RETURN v_result;

EXCEPTION
    WHEN OTHERS THEN
        IF v_validation_run_id IS NOT NULL THEN
            UPDATE learning_proposal_validation_runs_v1
            SET
                status = 'ERROR',
                finished_at = clock_timestamp(),
                error_text = SQLERRM,
                result = jsonb_build_object(
                    'status', 'error',
                    'sqlstate', SQLSTATE,
                    'error', SQLERRM
                )
            WHERE id = v_validation_run_id;
        END IF;

        INSERT INTO automation_kv (
            key,
            value,
            updated_at
        )
        VALUES (
            'learning_feedback_validation_v1_3_last_status',
            'error',
            clock_timestamp()
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
            'learning_feedback_validation_v1_3_last_error',
            SQLERRM,
            clock_timestamp()
        )
        ON CONFLICT (key)
        DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = EXCLUDED.updated_at;

        RAISE;
END;
$$;

-- ============================================================================
-- 6. Trigger: validate only after successful V1.2 refresh
-- ============================================================================

CREATE OR REPLACE FUNCTION
    trigger_learning_proposal_validation_v1_3()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = 'OK'
       AND (
            TG_OP = 'INSERT'
            OR OLD.status IS DISTINCT FROM NEW.status
       )
    THEN
        PERFORM refresh_learning_proposal_validation_v1_3(
            NEW.id
        );
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS
    trg_learning_proposal_validation_v1_3
ON learning_feedback_refresh_runs_v1;

CREATE TRIGGER trg_learning_proposal_validation_v1_3
AFTER INSERT OR UPDATE OF status
ON learning_feedback_refresh_runs_v1
FOR EACH ROW
EXECUTE FUNCTION
    trigger_learning_proposal_validation_v1_3();

-- ============================================================================
-- 7. Monitoring views
-- ============================================================================

CREATE OR REPLACE VIEW
    v_learning_proposal_validation_v1 AS
SELECT
    environment,
    symbol,
    interval,
    strategy,
    window_days,

    current_proposal_type,
    current_proposal_action,
    validation_status,

    consecutive_observations,
    total_observations,
    action_change_count,

    first_observed_at,
    action_first_observed_at,
    last_observed_at,

    validation_started_at,
    stable_at,
    reset_at,

    latest_confidence,
    minimum_confidence,
    maximum_confidence,
    average_confidence,

    latest_evidence_decisions,
    previous_evidence_decisions,
    maximum_evidence_decisions,

    latest_net_pnl_usdc,
    latest_profit_factor,
    latest_win_rate_pct,
    latest_context_coverage_pct,

    required_observations,
    required_span_hours,
    minimum_action_sample,
    minimum_confidence_required,

    action_is_consistent,
    evidence_is_non_decreasing,
    sample_is_sufficient,
    confidence_is_sufficient,
    span_is_sufficient,

    validation_reason,
    validation_evidence,

    last_refresh_run_id,
    updated_at

FROM learning_proposal_validation_state_v1;

CREATE OR REPLACE VIEW
    v_learning_proposal_validation_summary_v1 AS
SELECT
    environment,
    validation_status,
    current_proposal_action,

    COUNT(*) AS slots,

    SUM(latest_evidence_decisions)
        AS evidence_decisions,

    ROUND(
        AVG(latest_confidence)::NUMERIC,
        4
    ) AS avg_confidence,

    ROUND(
        AVG(latest_profit_factor)::NUMERIC,
        6
    ) AS avg_profit_factor,

    ROUND(
        SUM(latest_net_pnl_usdc)::NUMERIC,
        8
    ) AS net_pnl_usdc,

    MIN(action_first_observed_at)
        AS earliest_action_observed_at,

    MAX(last_observed_at)
        AS latest_observed_at

FROM learning_proposal_validation_state_v1
GROUP BY
    environment,
    validation_status,
    current_proposal_action;

CREATE OR REPLACE VIEW
    v_learning_proposal_stable_candidates_v1 AS
SELECT
    environment,
    symbol,
    interval,
    strategy,
    window_days,

    current_proposal_type,
    current_proposal_action,

    consecutive_observations,
    total_observations,

    latest_confidence,
    minimum_confidence,
    average_confidence,

    latest_evidence_decisions,
    latest_net_pnl_usdc,
    latest_profit_factor,
    latest_win_rate_pct,
    latest_context_coverage_pct,

    action_first_observed_at,
    stable_at,

    validation_reason,
    validation_evidence

FROM learning_proposal_validation_state_v1
WHERE validation_status = 'STABLE';

COMMENT ON TABLE learning_proposal_observations_v1 IS
'Immutable snapshots of Learning Feedback proposals captured after successful V1.2 refreshes.';

COMMENT ON TABLE learning_proposal_validation_state_v1 IS
'Persistent shadow-only stability state for Learning Feedback proposals. STABLE does not authorize apply.';

COMMENT ON FUNCTION refresh_learning_proposal_validation_v1_3(
    BIGINT
) IS
'Validates proposal persistence across successful V1.2 refreshes. Shadow-only; never applies recommendations.';

COMMIT;

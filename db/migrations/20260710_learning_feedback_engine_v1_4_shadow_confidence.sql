BEGIN;

-- WALTRADE Learning Engine V1.4 -- Shadow Confidence Calibration
-- SHADOW ONLY. This migration creates proposals and audit data only.

DO $$
BEGIN
    IF to_regclass('public.learning_feedback_refresh_runs_v1') IS NULL THEN
        RAISE EXCEPTION 'Required table learning_feedback_refresh_runs_v1 does not exist';
    END IF;
    IF to_regclass('public.learning_proposal_validation_state_v1') IS NULL THEN
        RAISE EXCEPTION 'Required table learning_proposal_validation_state_v1 does not exist';
    END IF;
    IF to_regclass('public.v_learning_proposal_stable_candidates_v1') IS NULL THEN
        RAISE EXCEPTION 'Required view v_learning_proposal_stable_candidates_v1 does not exist';
    END IF;
    IF to_regclass('public.automation_kv') IS NULL THEN
        RAISE EXCEPTION 'Required table automation_kv does not exist';
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS learning_shadow_confidence_proposals_v1 (
    id BIGSERIAL PRIMARY KEY,
    proposal_key TEXT NOT NULL UNIQUE,
    environment TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    strategy TEXT NOT NULL,
    window_days INTEGER NOT NULL,
    source_validation_state_id BIGINT,
    source_refresh_run_id BIGINT,
    source_proposal_key TEXT,
    source_proposal_action TEXT NOT NULL,
    proposed_delta NUMERIC(20,8) NOT NULL,
    calibration_confidence NUMERIC(10,6) NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    superseded_at TIMESTAMPTZ,
    superseded_by_key TEXT,

    CONSTRAINT ck_learning_shadow_confidence_window
        CHECK (window_days > 0),
    CONSTRAINT ck_learning_shadow_confidence_value
        CHECK (proposed_delta >= -0.05 AND proposed_delta <= 0.05),
    CONSTRAINT ck_learning_shadow_confidence_calibration
        CHECK (calibration_confidence >= 0 AND calibration_confidence <= 1),
    CONSTRAINT ck_learning_shadow_confidence_status
        CHECK (status IN ('ACTIVE', 'SUPERSEDED', 'EXPIRED')),
    CONSTRAINT ck_learning_shadow_confidence_action
        CHECK (source_proposal_action IN ('INCREASE_CONFIDENCE', 'REDUCE_CONFIDENCE')),
    CONSTRAINT fk_learning_shadow_confidence_state
        FOREIGN KEY (source_validation_state_id)
        REFERENCES learning_proposal_validation_state_v1(id),
    CONSTRAINT fk_learning_shadow_confidence_refresh
        FOREIGN KEY (source_refresh_run_id)
        REFERENCES learning_feedback_refresh_runs_v1(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS
    ux_learning_shadow_confidence_one_active_slot
ON learning_shadow_confidence_proposals_v1 (
    environment, symbol, interval, strategy, window_days
)
WHERE status = 'ACTIVE';

CREATE INDEX IF NOT EXISTS ix_learning_shadow_confidence_history
ON learning_shadow_confidence_proposals_v1 (
    environment, symbol, interval, strategy, window_days, refreshed_at DESC
);

CREATE TABLE IF NOT EXISTS learning_shadow_confidence_runs_v1 (
    id BIGSERIAL PRIMARY KEY,
    source_refresh_run_id BIGINT,
    environment TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    run_source TEXT NOT NULL,
    status TEXT NOT NULL,
    stable_inputs INTEGER NOT NULL DEFAULT 0,
    inserted INTEGER NOT NULL DEFAULT 0,
    refreshed INTEGER NOT NULL DEFAULT 0,
    superseded INTEGER NOT NULL DEFAULT 0,
    unchanged INTEGER NOT NULL DEFAULT 0,
    skipped INTEGER NOT NULL DEFAULT 0,
    apply_enabled BOOLEAN NOT NULL DEFAULT false,
    result JSONB,
    error_text TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,

    CONSTRAINT ck_learning_shadow_confidence_run_status
        CHECK (status IN ('RUNNING', 'OK', 'ERROR', 'SKIPPED')),
    CONSTRAINT ck_learning_shadow_confidence_apply_disabled
        CHECK (apply_enabled IS FALSE),
    CONSTRAINT ck_learning_shadow_confidence_run_counts
        CHECK (
            stable_inputs >= 0 AND inserted >= 0 AND refreshed >= 0
            AND superseded >= 0 AND unchanged >= 0 AND skipped >= 0
        ),
    CONSTRAINT fk_learning_shadow_confidence_run_refresh
        FOREIGN KEY (source_refresh_run_id)
        REFERENCES learning_feedback_refresh_runs_v1(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS
    ux_learning_shadow_confidence_run_source
ON learning_shadow_confidence_runs_v1 (source_refresh_run_id, environment)
WHERE source_refresh_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_learning_shadow_confidence_runs_latest
ON learning_shadow_confidence_runs_v1 (environment, started_at DESC);

INSERT INTO automation_kv (key, value, updated_at)
VALUES
    ('learning_engine_v14_enabled', '1', now()),
    ('learning_engine_v14_apply_enabled', '0', now()),
    ('learning_engine_v14_version', 'LEARNING_ENGINE_V1_4', now()),
    ('learning_engine_v14_last_status', 'not_run', now())
ON CONFLICT (key) DO NOTHING;

CREATE OR REPLACE FUNCTION refresh_learning_shadow_confidence_proposals_v1_4(
    p_refresh_run_id BIGINT,
    p_run_source TEXT DEFAULT 'AUTOMATION'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_environment TEXT;
    v_source_status TEXT;
    v_enabled BOOLEAN := true;
    v_run_id BIGINT;
    v_existing_result JSONB;
    v_result JSONB;
    v_now TIMESTAMPTZ := clock_timestamp();
    v_stable_inputs INTEGER := 0;
    v_inserted INTEGER := 0;
    v_refreshed INTEGER := 0;
    v_superseded INTEGER := 0;
    v_unchanged INTEGER := 0;
    v_skipped INTEGER := 0;
    v_sample_delta NUMERIC(20,8);
    v_confidence_cap NUMERIC(20,8);
    v_unsigned_delta NUMERIC(20,8);
    v_final_delta NUMERIC(20,8);
    v_proposal_key TEXT;
    v_active learning_shadow_confidence_proposals_v1%ROWTYPE;
    v_active_found BOOLEAN;
    v_target_exists BOOLEAN;
    r RECORD;
BEGIN
    IF p_refresh_run_id IS NULL THEN
        v_result := jsonb_build_object(
            'status', 'error',
            'engine_version', 'LEARNING_ENGINE_V1_4',
            'mode', 'SHADOW_ONLY',
            'error', 'p_refresh_run_id must not be null',
            'apply_enabled', false
        );
        INSERT INTO automation_kv (key, value, updated_at)
        VALUES ('learning_engine_v14_last_status', 'error', clock_timestamp())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;
        RETURN v_result;
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtext('refresh_learning_shadow_confidence_proposals_v1_4')
    );

    SELECT environment, status
    INTO v_environment, v_source_status
    FROM learning_feedback_refresh_runs_v1
    WHERE id = p_refresh_run_id;

    IF NOT FOUND OR v_source_status <> 'OK' THEN
        v_result := jsonb_build_object(
            'status', 'error',
            'engine_version', 'LEARNING_ENGINE_V1_4',
            'mode', 'SHADOW_ONLY',
            'source_refresh_run_id', p_refresh_run_id,
            'error', CASE
                WHEN v_environment IS NULL THEN 'source refresh run does not exist'
                ELSE 'source refresh run status is not OK'
            END,
            'apply_enabled', false
        );
        INSERT INTO automation_kv (key, value, updated_at)
        VALUES ('learning_engine_v14_last_status', 'error', clock_timestamp())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;
        RETURN v_result;
    END IF;

    SELECT result
    INTO v_existing_result
    FROM learning_shadow_confidence_runs_v1
    WHERE source_refresh_run_id = p_refresh_run_id
      AND environment = v_environment;

    IF FOUND THEN
        RETURN COALESCE(
            v_existing_result,
            jsonb_build_object(
                'status', 'skipped',
                'reason', 'SOURCE_RUN_ALREADY_PROCESSED',
                'engine_version', 'LEARNING_ENGINE_V1_4',
                'mode', 'SHADOW_ONLY',
                'source_refresh_run_id', p_refresh_run_id,
                'apply_enabled', false
            )
        );
    END IF;

    SELECT CASE
        WHEN value IS NULL THEN true
        WHEN lower(trim(value)) IN ('1', 'true', 'yes', 'on') THEN true
        ELSE false
    END
    INTO v_enabled
    FROM automation_kv
    WHERE key = 'learning_engine_v14_enabled';
    v_enabled := COALESCE(v_enabled, true);

    INSERT INTO learning_shadow_confidence_runs_v1 (
        source_refresh_run_id, environment, engine_version,
        run_source, status, apply_enabled
    ) VALUES (
        p_refresh_run_id, v_environment, 'LEARNING_ENGINE_V1_4',
        COALESCE(NULLIF(trim(p_run_source), ''), 'AUTOMATION'),
        CASE WHEN v_enabled THEN 'RUNNING' ELSE 'SKIPPED' END,
        false
    )
    RETURNING id INTO v_run_id;

    IF NOT v_enabled THEN
        v_result := jsonb_build_object(
            'status', 'skipped',
            'reason', 'ENGINE_DISABLED',
            'engine_version', 'LEARNING_ENGINE_V1_4',
            'mode', 'SHADOW_ONLY',
            'stable_inputs', 0,
            'inserted', 0,
            'refreshed', 0,
            'superseded', 0,
            'unchanged', 0,
            'skipped', 0,
            'apply_enabled', false
        );
        UPDATE learning_shadow_confidence_runs_v1
        SET result = v_result, finished_at = clock_timestamp()
        WHERE id = v_run_id;
        INSERT INTO automation_kv (key, value, updated_at)
        VALUES ('learning_engine_v14_last_status', 'skipped', clock_timestamp())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;
        RETURN v_result;
    END IF;

    -- This nested block is a PostgreSQL subtransaction. On error, proposal
    -- mutations roll back while the RUNNING row above remains available to be
    -- durably marked ERROR by the exception handler.
    BEGIN
        FOR r IN
            SELECT
                st.id AS validation_state_id,
                st.last_refresh_run_id,
                st.current_proposal_key,
                st.current_proposal_action,
                st.validation_status,
                st.sample_is_sufficient,
                st.confidence_is_sufficient,
                st.span_is_sufficient,
                st.action_is_consistent,
                st.evidence_is_non_decreasing,
                s.environment,
                s.symbol,
                s.interval,
                s.strategy,
                s.window_days,
                s.consecutive_observations,
                s.total_observations,
                s.latest_confidence,
                s.minimum_confidence,
                s.average_confidence,
                s.latest_evidence_decisions,
                s.latest_net_pnl_usdc,
                s.latest_profit_factor,
                s.latest_win_rate_pct,
                s.latest_context_coverage_pct,
                st.first_observed_at,
                s.action_first_observed_at,
                s.stable_at
            FROM v_learning_proposal_stable_candidates_v1 s
            JOIN learning_proposal_validation_state_v1 st
              ON st.environment = s.environment
             AND st.symbol = s.symbol
             AND st.interval = s.interval
             AND st.strategy = s.strategy
             AND st.window_days = s.window_days
            WHERE s.environment = v_environment
              AND st.last_refresh_run_id = p_refresh_run_id
              AND st.validation_status = 'STABLE'
              AND st.sample_is_sufficient IS TRUE
              AND st.confidence_is_sufficient IS TRUE
              AND st.span_is_sufficient IS TRUE
              AND st.action_is_consistent IS TRUE
              AND st.evidence_is_non_decreasing IS TRUE
            ORDER BY s.environment, s.symbol, s.interval, s.strategy, s.window_days
        LOOP
            v_stable_inputs := v_stable_inputs + 1;

            IF r.current_proposal_action NOT IN (
                'INCREASE_CONFIDENCE', 'REDUCE_CONFIDENCE'
            ) THEN
                v_skipped := v_skipped + 1;
                CONTINUE;
            END IF;

            v_sample_delta := CASE
                WHEN r.latest_evidence_decisions >= 100 THEN 0.03
                WHEN r.latest_evidence_decisions >= 50 THEN 0.02
                WHEN r.latest_evidence_decisions >= 30 THEN 0.01
                ELSE 0.00
            END;
            v_confidence_cap := CASE
                WHEN r.latest_confidence >= 0.85 THEN 0.03
                WHEN r.latest_confidence >= 0.70 THEN 0.02
                ELSE 0.01
            END;
            v_unsigned_delta := LEAST(v_sample_delta, v_confidence_cap, 0.05);

            IF v_unsigned_delta <= 0 THEN
                v_skipped := v_skipped + 1;
                CONTINUE;
            END IF;

            v_final_delta := CASE r.current_proposal_action
                WHEN 'INCREASE_CONFIDENCE' THEN v_unsigned_delta
                ELSE -v_unsigned_delta
            END;

            v_proposal_key := 'LE14:' || md5(concat_ws('|',
                'LEARNING_ENGINE_V1_4', r.environment, r.symbol,
                r.interval, r.strategy, r.window_days::TEXT,
                r.current_proposal_action, v_final_delta::TEXT,
                r.current_proposal_key
            ));

            SELECT * INTO v_active
            FROM learning_shadow_confidence_proposals_v1
            WHERE environment = r.environment
              AND symbol = r.symbol
              AND interval = r.interval
              AND strategy = r.strategy
              AND window_days = r.window_days
              AND status = 'ACTIVE'
            FOR UPDATE;

            v_active_found := FOUND;

            IF v_active_found AND v_active.proposal_key = v_proposal_key THEN
                UPDATE learning_shadow_confidence_proposals_v1
                SET
                    source_validation_state_id = r.validation_state_id,
                    source_refresh_run_id = p_refresh_run_id,
                    calibration_confidence = r.latest_confidence,
                    reason = format(
                        'Stable %s proposal; bounded shadow delta=%s',
                        r.current_proposal_action, v_final_delta
                    ),
                    evidence = jsonb_build_object(
                        'engine_version', 'LEARNING_ENGINE_V1_4',
                        'mode', 'SHADOW_ONLY',
                        'source_engine', 'LEARNING_FEEDBACK_ENGINE_V1_3',
                        'source_validation_status', 'STABLE',
                        'source_validation_state_id', r.validation_state_id,
                        'source_refresh_run_id', p_refresh_run_id,
                        'source_proposal_key', r.current_proposal_key,
                        'source_proposal_action', r.current_proposal_action,
                        'environment', r.environment,
                        'symbol', r.symbol,
                        'interval', r.interval,
                        'strategy', r.strategy,
                        'window_days', r.window_days,
                        'latest_confidence', r.latest_confidence,
                        'minimum_confidence', r.minimum_confidence,
                        'average_confidence', r.average_confidence,
                        'evidence_decisions', r.latest_evidence_decisions,
                        'net_pnl_usdc', r.latest_net_pnl_usdc,
                        'profit_factor', r.latest_profit_factor,
                        'win_rate_pct', r.latest_win_rate_pct,
                        'context_coverage_pct', r.latest_context_coverage_pct,
                        'consecutive_observations', r.consecutive_observations,
                        'total_observations', r.total_observations,
                        'first_observed_at', r.first_observed_at,
                        'stable_at', r.stable_at,
                        'delta_policy', jsonb_build_object(
                            'sample_delta', v_sample_delta,
                            'confidence_cap', v_confidence_cap,
                            'final_delta', v_final_delta
                        ),
                        'safety', jsonb_build_object(
                            'apply_enabled', false,
                            'runtime_mutation_allowed', false
                        )
                    ),
                    last_seen_at = v_now,
                    refreshed_at = v_now
                WHERE id = v_active.id;
                v_unchanged := v_unchanged + 1;
                CONTINUE;
            END IF;

            SELECT EXISTS (
                SELECT 1 FROM learning_shadow_confidence_proposals_v1
                WHERE proposal_key = v_proposal_key
            ) INTO v_target_exists;

            IF v_active_found THEN
                UPDATE learning_shadow_confidence_proposals_v1
                SET status = 'SUPERSEDED',
                    superseded_at = v_now,
                    superseded_by_key = v_proposal_key,
                    refreshed_at = v_now
                WHERE id = v_active.id;
                v_superseded := v_superseded + 1;
            END IF;

            INSERT INTO learning_shadow_confidence_proposals_v1 (
                proposal_key, environment, symbol, interval, strategy,
                window_days, source_validation_state_id, source_refresh_run_id,
                source_proposal_key, source_proposal_action, proposed_delta,
                calibration_confidence, status, reason, evidence,
                first_seen_at, last_seen_at, refreshed_at,
                superseded_at, superseded_by_key
            ) VALUES (
                v_proposal_key, r.environment, r.symbol, r.interval, r.strategy,
                r.window_days, r.validation_state_id, p_refresh_run_id,
                r.current_proposal_key, r.current_proposal_action, v_final_delta,
                r.latest_confidence, 'ACTIVE',
                format('Stable %s proposal; bounded shadow delta=%s',
                       r.current_proposal_action, v_final_delta),
                jsonb_build_object(
                    'engine_version', 'LEARNING_ENGINE_V1_4',
                    'mode', 'SHADOW_ONLY',
                    'source_engine', 'LEARNING_FEEDBACK_ENGINE_V1_3',
                    'source_validation_status', 'STABLE',
                    'source_validation_state_id', r.validation_state_id,
                    'source_refresh_run_id', p_refresh_run_id,
                    'source_proposal_key', r.current_proposal_key,
                    'source_proposal_action', r.current_proposal_action,
                    'environment', r.environment,
                    'symbol', r.symbol,
                    'interval', r.interval,
                    'strategy', r.strategy,
                    'window_days', r.window_days,
                    'latest_confidence', r.latest_confidence,
                    'minimum_confidence', r.minimum_confidence,
                    'average_confidence', r.average_confidence,
                    'evidence_decisions', r.latest_evidence_decisions,
                    'net_pnl_usdc', r.latest_net_pnl_usdc,
                    'profit_factor', r.latest_profit_factor,
                    'win_rate_pct', r.latest_win_rate_pct,
                    'context_coverage_pct', r.latest_context_coverage_pct,
                    'consecutive_observations', r.consecutive_observations,
                    'total_observations', r.total_observations,
                    'first_observed_at', r.first_observed_at,
                    'stable_at', r.stable_at,
                    'delta_policy', jsonb_build_object(
                        'sample_delta', v_sample_delta,
                        'confidence_cap', v_confidence_cap,
                        'final_delta', v_final_delta
                    ),
                    'safety', jsonb_build_object(
                        'apply_enabled', false,
                        'runtime_mutation_allowed', false
                    )
                ),
                v_now, v_now, v_now, NULL, NULL
            )
            ON CONFLICT (proposal_key) DO UPDATE
            SET
                source_validation_state_id = EXCLUDED.source_validation_state_id,
                source_refresh_run_id = EXCLUDED.source_refresh_run_id,
                calibration_confidence = EXCLUDED.calibration_confidence,
                status = 'ACTIVE',
                reason = EXCLUDED.reason,
                evidence = EXCLUDED.evidence,
                last_seen_at = EXCLUDED.last_seen_at,
                refreshed_at = EXCLUDED.refreshed_at,
                superseded_at = NULL,
                superseded_by_key = NULL;

            IF v_target_exists THEN
                v_refreshed := v_refreshed + 1;
            ELSE
                v_inserted := v_inserted + 1;
            END IF;
        END LOOP;

        v_result := jsonb_build_object(
            'status', 'ok',
            'engine_version', 'LEARNING_ENGINE_V1_4',
            'mode', 'SHADOW_ONLY',
            'source_refresh_run_id', p_refresh_run_id,
            'stable_inputs', v_stable_inputs,
            'inserted', v_inserted,
            'refreshed', v_refreshed,
            'superseded', v_superseded,
            'unchanged', v_unchanged,
            'skipped', v_skipped,
            'apply_enabled', false
        );

        UPDATE learning_shadow_confidence_runs_v1
        SET status = 'OK', stable_inputs = v_stable_inputs,
            inserted = v_inserted, refreshed = v_refreshed,
            superseded = v_superseded, unchanged = v_unchanged,
            skipped = v_skipped, apply_enabled = false,
            result = v_result, finished_at = clock_timestamp()
        WHERE id = v_run_id;

        INSERT INTO automation_kv (key, value, updated_at)
        VALUES
            ('learning_engine_v14_last_status', 'ok', clock_timestamp()),
            ('learning_engine_v14_last_result', v_result::TEXT, clock_timestamp()),
            ('learning_engine_v14_apply_enabled', '0', clock_timestamp())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;

        RETURN v_result;
    EXCEPTION
        WHEN OTHERS THEN
            v_result := jsonb_build_object(
                'status', 'error',
                'engine_version', 'LEARNING_ENGINE_V1_4',
                'mode', 'SHADOW_ONLY',
                'source_refresh_run_id', p_refresh_run_id,
                'sqlstate', SQLSTATE,
                'error', SQLERRM,
                'apply_enabled', false
            );
            UPDATE learning_shadow_confidence_runs_v1
            SET status = 'ERROR', error_text = SQLERRM,
                result = v_result, apply_enabled = false,
                finished_at = clock_timestamp()
            WHERE id = v_run_id;
            INSERT INTO automation_kv (key, value, updated_at)
            VALUES
                ('learning_engine_v14_last_status', 'error', clock_timestamp()),
                ('learning_engine_v14_last_error', SQLERRM, clock_timestamp()),
                ('learning_engine_v14_apply_enabled', '0', clock_timestamp())
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;
            RETURN v_result;
    END;
END;
$$;

CREATE OR REPLACE VIEW v_learning_shadow_confidence_active_v1 AS
SELECT *
FROM learning_shadow_confidence_proposals_v1
WHERE status = 'ACTIVE';

CREATE OR REPLACE VIEW v_learning_shadow_confidence_history_v1 AS
SELECT *
FROM learning_shadow_confidence_proposals_v1;

CREATE OR REPLACE VIEW v_learning_shadow_confidence_summary_v1 AS
SELECT
    environment,
    source_proposal_action,
    status,
    COUNT(*) AS slots,
    SUM(COALESCE((evidence ->> 'evidence_decisions')::INTEGER, 0))
        AS evidence_decisions,
    ROUND(AVG(calibration_confidence)::NUMERIC, 6)
        AS avg_calibration_confidence,
    ROUND(AVG(proposed_delta)::NUMERIC, 8) AS avg_proposed_delta,
    MIN(proposed_delta) AS min_proposed_delta,
    MAX(proposed_delta) AS max_proposed_delta,
    MIN(first_seen_at) AS earliest_first_seen_at,
    MAX(refreshed_at) AS latest_refreshed_at
FROM learning_shadow_confidence_proposals_v1
GROUP BY environment, source_proposal_action, status;

CREATE OR REPLACE VIEW v_learning_shadow_confidence_safety_audit_v1 AS
SELECT
    'INVALID_DELTA'::TEXT AS violation_type,
    proposal_key,
    environment,
    symbol,
    interval,
    strategy,
    window_days,
    'proposed_delta outside [-0.05, 0.05]'::TEXT AS detail
FROM learning_shadow_confidence_proposals_v1
WHERE proposed_delta < -0.05 OR proposed_delta > 0.05
UNION ALL
SELECT 'INVALID_CONFIDENCE', proposal_key, environment, symbol, interval,
       strategy, window_days, 'calibration_confidence outside [0, 1]'
FROM learning_shadow_confidence_proposals_v1
WHERE calibration_confidence < 0 OR calibration_confidence > 1
UNION ALL
SELECT 'MULTIPLE_ACTIVE', MIN(proposal_key), environment, symbol, interval,
       strategy, window_days, format('active proposals=%s', COUNT(*))
FROM learning_shadow_confidence_proposals_v1
WHERE status = 'ACTIVE'
GROUP BY environment, symbol, interval, strategy, window_days
HAVING COUNT(*) > 1
UNION ALL
SELECT 'UNSUPPORTED_ACTION', proposal_key, environment, symbol, interval,
       strategy, window_days, source_proposal_action
FROM learning_shadow_confidence_proposals_v1
WHERE source_proposal_action NOT IN ('INCREASE_CONFIDENCE', 'REDUCE_CONFIDENCE')
UNION ALL
SELECT 'APPLY_ENABLED', ('run:' || id)::TEXT, environment, NULL::TEXT,
       NULL::TEXT, NULL::TEXT, NULL::INTEGER, 'apply_enabled must be false'
FROM learning_shadow_confidence_runs_v1
WHERE apply_enabled IS NOT FALSE
UNION ALL
SELECT 'MISSING_SLOT_KEY', proposal_key, environment, symbol, interval,
       strategy, window_days, 'full slot key contains null or empty value'
FROM learning_shadow_confidence_proposals_v1
WHERE trim(environment) = '' OR trim(symbol) = '' OR trim(interval) = ''
   OR trim(strategy) = '' OR window_days IS NULL;

COMMENT ON FUNCTION refresh_learning_shadow_confidence_proposals_v1_4(BIGINT, TEXT) IS
'Creates bounded, shadow-only confidence delta proposals from stable V1.3 candidates. Never applies runtime changes and never propagates processing errors.';

COMMENT ON TABLE learning_shadow_confidence_proposals_v1 IS
'Learning Engine V1.4 shadow-only confidence delta proposal history. No current or proposed runtime values.';

COMMENT ON TABLE learning_shadow_confidence_runs_v1 IS
'Learning Engine V1.4 isolated refresh audit history. apply_enabled is constrained to false.';

COMMIT;

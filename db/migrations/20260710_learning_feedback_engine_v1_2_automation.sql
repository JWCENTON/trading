BEGIN;

-- ============================================================================
-- WALTRADE — LEARNING FEEDBACK ENGINE V1.2
--
-- Purpose:
--   Automatic, database-controlled scheduling of V1.1 refresh.
--
-- Default cadence:
--   12 hours
--
-- Safety:
--   - SHADOW only
--   - no bot_control writes
--   - no ORC parameter writes
--   - no confidence apply
--   - no promotion apply
--   - no capital allocation changes
-- ============================================================================

DO $$
BEGIN
    IF to_regprocedure(
        'public.refresh_learning_feedback_engine_v1_1(integer,integer,integer)'
    ) IS NULL THEN
        RAISE EXCEPTION
            'Required function refresh_learning_feedback_engine_v1_1(integer,integer,integer) does not exist';
    END IF;

    IF to_regclass('public.automation_kv') IS NULL THEN
        RAISE EXCEPTION
            'Required table automation_kv does not exist';
    END IF;
END
$$;

-- ============================================================================
-- Run history
-- ============================================================================

CREATE TABLE IF NOT EXISTS learning_feedback_refresh_runs_v1 (
    id BIGSERIAL PRIMARY KEY,

    environment TEXT NOT NULL,
    engine_version TEXT NOT NULL,

    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,

    trigger_source TEXT NOT NULL DEFAULT 'AUTOMATION_RUNNER',

    status TEXT NOT NULL,

    window_days INTEGER NOT NULL,
    min_observe_sample INTEGER NOT NULL,
    min_action_sample INTEGER NOT NULL,
    interval_hours INTEGER NOT NULL,

    result JSONB,
    error_text TEXT,

    CONSTRAINT ck_learning_feedback_refresh_status
        CHECK (
            status IN (
                'RUNNING',
                'OK',
                'SKIPPED_NOT_DUE',
                'SKIPPED_DISABLED',
                'ERROR'
            )
        ),

    CONSTRAINT ck_learning_feedback_refresh_interval
        CHECK (interval_hours >= 1),

    CONSTRAINT ck_learning_feedback_refresh_window
        CHECK (window_days >= 1),

    CONSTRAINT ck_learning_feedback_refresh_samples
        CHECK (
            min_observe_sample >= 1
            AND min_action_sample >= min_observe_sample
        )
);

CREATE INDEX IF NOT EXISTS ix_learning_feedback_refresh_runs_v1_latest
    ON learning_feedback_refresh_runs_v1 (
        environment,
        requested_at DESC
    );

CREATE INDEX IF NOT EXISTS ix_learning_feedback_refresh_runs_v1_status
    ON learning_feedback_refresh_runs_v1 (
        status,
        requested_at DESC
    );

-- ============================================================================
-- Due-aware refresh
-- ============================================================================

CREATE OR REPLACE FUNCTION refresh_learning_feedback_engine_v1_2_if_due(
    p_interval_hours INTEGER DEFAULT 12,
    p_window_days INTEGER DEFAULT 30,
    p_min_observe_sample INTEGER DEFAULT 10,
    p_min_action_sample INTEGER DEFAULT 30,
    p_force BOOLEAN DEFAULT false,
    p_trigger_source TEXT DEFAULT 'AUTOMATION_RUNNER'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := now();
    v_environment TEXT;
    v_enabled BOOLEAN := true;

    v_last_success_at TIMESTAMPTZ;
    v_due_at TIMESTAMPTZ;

    v_run_id BIGINT;
    v_refresh_result JSONB;
    v_result JSONB;
BEGIN
    IF p_interval_hours < 1 THEN
        RAISE EXCEPTION 'p_interval_hours must be >= 1';
    END IF;

    IF p_window_days < 1 THEN
        RAISE EXCEPTION 'p_window_days must be >= 1';
    END IF;

    IF p_min_observe_sample < 1 THEN
        RAISE EXCEPTION 'p_min_observe_sample must be >= 1';
    END IF;

    IF p_min_action_sample < p_min_observe_sample THEN
        RAISE EXCEPTION
            'p_min_action_sample must be >= p_min_observe_sample';
    END IF;

    -- Prevent concurrent execution in the same database.
    IF NOT pg_try_advisory_xact_lock(
        hashtext('refresh_learning_feedback_engine_v1_2_if_due')
    ) THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'reason', 'LOCK_NOT_ACQUIRED',
            'engine_version', 'LEARNING_FEEDBACK_ENGINE_V1_2',
            'refreshed_at', v_now
        );
    END IF;

    SELECT current_database()
    INTO v_environment;

    SELECT CASE
        WHEN value IS NULL THEN true
        WHEN lower(trim(value)) IN ('1', 'true', 'yes', 'on') THEN true
        ELSE false
    END
    INTO v_enabled
    FROM automation_kv
    WHERE key = 'learning_feedback_engine_automation_enabled';

    v_enabled := COALESCE(v_enabled, true);

    IF NOT v_enabled AND NOT p_force THEN
        INSERT INTO learning_feedback_refresh_runs_v1 (
            environment,
            engine_version,
            requested_at,
            finished_at,
            trigger_source,
            status,
            window_days,
            min_observe_sample,
            min_action_sample,
            interval_hours,
            result
        )
        VALUES (
            v_environment,
            'LEARNING_FEEDBACK_ENGINE_V1_2',
            v_now,
            v_now,
            p_trigger_source,
            'SKIPPED_DISABLED',
            p_window_days,
            p_min_observe_sample,
            p_min_action_sample,
            p_interval_hours,
            jsonb_build_object(
                'status', 'skipped',
                'reason', 'AUTOMATION_DISABLED'
            )
        );

        RETURN jsonb_build_object(
            'status', 'skipped',
            'reason', 'AUTOMATION_DISABLED',
            'engine_version', 'LEARNING_FEEDBACK_ENGINE_V1_2',
            'environment', v_environment,
            'checked_at', v_now
        );
    END IF;

    SELECT MAX(finished_at)
    INTO v_last_success_at
    FROM learning_feedback_refresh_runs_v1
    WHERE environment = v_environment
      AND status = 'OK';

    -- Compatibility with manually executed V1.1 refresh before V1.2 existed.
    IF v_last_success_at IS NULL THEN
        SELECT updated_at
        INTO v_last_success_at
        FROM automation_kv
        WHERE key = 'learning_feedback_engine_v1_1_last_status'
          AND value = 'ok'
        ORDER BY updated_at DESC
        LIMIT 1;
    END IF;

    IF v_last_success_at IS NOT NULL THEN
        v_due_at :=
            v_last_success_at
            + make_interval(hours => p_interval_hours);
    ELSE
        v_due_at := v_now;
    END IF;

    IF NOT p_force
       AND v_last_success_at IS NOT NULL
       AND v_now < v_due_at
    THEN
        INSERT INTO learning_feedback_refresh_runs_v1 (
            environment,
            engine_version,
            requested_at,
            finished_at,
            trigger_source,
            status,
            window_days,
            min_observe_sample,
            min_action_sample,
            interval_hours,
            result
        )
        VALUES (
            v_environment,
            'LEARNING_FEEDBACK_ENGINE_V1_2',
            v_now,
            v_now,
            p_trigger_source,
            'SKIPPED_NOT_DUE',
            p_window_days,
            p_min_observe_sample,
            p_min_action_sample,
            p_interval_hours,
            jsonb_build_object(
                'status', 'skipped',
                'reason', 'NOT_DUE',
                'last_success_at', v_last_success_at,
                'next_due_at', v_due_at
            )
        );

        v_result := jsonb_build_object(
            'status', 'skipped',
            'reason', 'NOT_DUE',
            'engine_version', 'LEARNING_FEEDBACK_ENGINE_V1_2',
            'environment', v_environment,
            'interval_hours', p_interval_hours,
            'last_success_at', v_last_success_at,
            'next_due_at', v_due_at,
            'checked_at', v_now
        );

        INSERT INTO automation_kv (
            key,
            value,
            updated_at
        )
        VALUES (
            'learning_feedback_engine_v1_2_last_check',
            v_result::TEXT,
            v_now
        )
        ON CONFLICT (key)
        DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = EXCLUDED.updated_at;

        RETURN v_result;
    END IF;

    INSERT INTO learning_feedback_refresh_runs_v1 (
        environment,
        engine_version,
        requested_at,
        started_at,
        trigger_source,
        status,
        window_days,
        min_observe_sample,
        min_action_sample,
        interval_hours
    )
    VALUES (
        v_environment,
        'LEARNING_FEEDBACK_ENGINE_V1_2',
        v_now,
        clock_timestamp(),
        p_trigger_source,
        'RUNNING',
        p_window_days,
        p_min_observe_sample,
        p_min_action_sample,
        p_interval_hours
    )
    RETURNING id INTO v_run_id;

    BEGIN
        v_refresh_result :=
            refresh_learning_feedback_engine_v1_1(
                p_window_days,
                p_min_observe_sample,
                p_min_action_sample
            );

        UPDATE learning_feedback_refresh_runs_v1
        SET
            status = 'OK',
            finished_at = clock_timestamp(),
            result = v_refresh_result
        WHERE id = v_run_id;

        v_result := jsonb_build_object(
            'status', 'ok',
            'engine_version', 'LEARNING_FEEDBACK_ENGINE_V1_2',
            'wrapped_engine_version',
                'LEARNING_FEEDBACK_ENGINE_V1_1',
            'environment', v_environment,
            'run_id', v_run_id,
            'trigger_source', p_trigger_source,
            'interval_hours', p_interval_hours,
            'window_days', p_window_days,
            'min_observe_sample', p_min_observe_sample,
            'min_action_sample', p_min_action_sample,
            'forced', p_force,
            'refresh_result', v_refresh_result,
            'refreshed_at', clock_timestamp()
        );

        INSERT INTO automation_kv (
            key,
            value,
            updated_at
        )
        VALUES (
            'learning_feedback_engine_v1_2_last_stats',
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
            'learning_feedback_engine_v1_2_last_status',
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
            'learning_feedback_engine_v1_2_last_success_at',
            clock_timestamp()::TEXT,
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
            'learning_feedback_engine_active_version',
            'LEARNING_FEEDBACK_ENGINE_V1_2',
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
            'learning_feedback_engine_apply_enabled',
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
            UPDATE learning_feedback_refresh_runs_v1
            SET
                status = 'ERROR',
                finished_at = clock_timestamp(),
                error_text = SQLERRM,
                result = jsonb_build_object(
                    'status', 'error',
                    'sqlstate', SQLSTATE,
                    'error', SQLERRM
                )
            WHERE id = v_run_id;

            INSERT INTO automation_kv (
                key,
                value,
                updated_at
            )
            VALUES (
                'learning_feedback_engine_v1_2_last_status',
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
                'learning_feedback_engine_v1_2_last_error',
                SQLERRM,
                clock_timestamp()
            )
            ON CONFLICT (key)
            DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = EXCLUDED.updated_at;

            RAISE;
    END;
END;
$$;

-- ============================================================================
-- Monitoring view
-- ============================================================================

CREATE OR REPLACE VIEW v_learning_feedback_automation_status_v1 AS
WITH last_ok AS (
    SELECT
        MAX(finished_at) AS last_success_at
    FROM learning_feedback_refresh_runs_v1
    WHERE status = 'OK'
),
last_run AS (
    SELECT DISTINCT ON (environment)
        environment,
        id AS last_run_id,
        requested_at,
        started_at,
        finished_at,
        trigger_source,
        status,
        interval_hours,
        result,
        error_text
    FROM learning_feedback_refresh_runs_v1
    ORDER BY environment, requested_at DESC
),
config AS (
    SELECT
        COALESCE(
            MAX(value) FILTER (
                WHERE key =
                    'learning_feedback_engine_automation_enabled'
            ),
            '1'
        ) AS automation_enabled,

        COALESCE(
            MAX(value) FILTER (
                WHERE key =
                    'learning_feedback_engine_interval_hours'
            ),
            '12'
        ) AS configured_interval_hours
    FROM automation_kv
)
SELECT
    current_database() AS environment,

    c.automation_enabled,
    c.configured_interval_hours::INTEGER
        AS configured_interval_hours,

    l.last_run_id,
    l.status AS last_run_status,
    l.trigger_source,
    l.requested_at AS last_check_at,
    l.started_at,
    l.finished_at,

    o.last_success_at,

    CASE
        WHEN o.last_success_at IS NULL
            THEN now()

        ELSE
            o.last_success_at
            + make_interval(
                hours =>
                    c.configured_interval_hours::INTEGER
            )
    END AS next_due_at,

    CASE
        WHEN o.last_success_at IS NULL
            THEN true

        ELSE
            now() >= (
                o.last_success_at
                + make_interval(
                    hours =>
                        c.configured_interval_hours::INTEGER
                )
            )
    END AS is_due,

    CASE
        WHEN o.last_success_at IS NULL
            THEN true

        ELSE
            now() > (
                o.last_success_at
                + make_interval(
                    hours =>
                        c.configured_interval_hours::INTEGER
                        + 2
                )
            )
    END AS is_stale,

    l.error_text,
    l.result

FROM config c
LEFT JOIN last_run l
    ON true
LEFT JOIN last_ok o
    ON true;

-- ============================================================================
-- Configuration defaults
-- ============================================================================

INSERT INTO automation_kv (
    key,
    value,
    updated_at
)
VALUES
    (
        'learning_feedback_engine_automation_enabled',
        '1',
        now()
    ),
    (
        'learning_feedback_engine_interval_hours',
        '12',
        now()
    ),
    (
        'learning_feedback_engine_window_days',
        '30',
        now()
    ),
    (
        'learning_feedback_engine_min_observe_sample',
        '10',
        now()
    ),
    (
        'learning_feedback_engine_min_action_sample',
        '30',
        now()
    ),
    (
        'learning_feedback_engine_apply_enabled',
        '0',
        now()
    )
ON CONFLICT (key)
DO NOTHING;

COMMENT ON FUNCTION refresh_learning_feedback_engine_v1_2_if_due(
    INTEGER,
    INTEGER,
    INTEGER,
    INTEGER,
    BOOLEAN,
    TEXT
) IS
'Due-aware shadow-only Learning Feedback refresh. Default cadence 12h. Never applies proposals to trading.';

COMMENT ON TABLE learning_feedback_refresh_runs_v1 IS
'Audit history for automatic Learning Feedback Engine refreshes.';

COMMIT;

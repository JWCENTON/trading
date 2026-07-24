\set ON_ERROR_STOP on

CREATE TABLE learning_canonical_source_snapshots_v2 (
    snapshot_token UUID PRIMARY KEY,
    snapshot_status TEXT NOT NULL,
    feedback_run_id BIGINT NOT NULL UNIQUE
);

CREATE TABLE learning_slot_statistics_v1 (
    id BIGSERIAL PRIMARY KEY,
    environment TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    strategy TEXT NOT NULL,
    window_days INTEGER NOT NULL,
    decisions INTEGER NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL,
    source_snapshot_token UUID REFERENCES learning_canonical_source_snapshots_v2,
    UNIQUE (environment, symbol, interval, strategy, window_days)
);

CREATE TABLE learning_calibration_proposals_v1 (
    proposal_key TEXT PRIMARY KEY,
    environment TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    strategy TEXT NOT NULL,
    window_days INTEGER NOT NULL,
    decisions INTEGER NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL,
    validation_status TEXT NOT NULL,
    source_snapshot_token UUID REFERENCES learning_canonical_source_snapshots_v2
);

CREATE OR REPLACE FUNCTION propagate_learning_source_snapshot_token_v2()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_token_text TEXT := current_setting(
        'waltrade.learning_source_snapshot_token', true
    );
    v_token UUID;
BEGIN
    IF v_token_text IS NULL OR v_token_text = '' THEN
        RETURN NEW;
    END IF;
    v_token := v_token_text::UUID;
    PERFORM 1
      FROM learning_canonical_source_snapshots_v2
     WHERE snapshot_token = v_token AND snapshot_status = 'COMPLETE';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'LEARNING_FROZEN_SOURCE_CONTEXT_MISSING';
    END IF;
    IF NEW.source_snapshot_token IS NOT NULL
       AND NEW.source_snapshot_token <> v_token THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_PAYLOAD_CONFLICT table=%', TG_TABLE_NAME;
    END IF;
    NEW.source_snapshot_token := v_token;
    RETURN NEW;
END;
$$;

CREATE TRIGGER propagate_learning_source_snapshot_v2
BEFORE INSERT OR UPDATE ON learning_slot_statistics_v1
FOR EACH ROW EXECUTE FUNCTION propagate_learning_source_snapshot_token_v2();
CREATE TRIGGER propagate_learning_source_snapshot_v2
BEFORE INSERT OR UPDATE ON learning_calibration_proposals_v1
FOR EACH ROW EXECUTE FUNCTION propagate_learning_source_snapshot_token_v2();

CREATE OR REPLACE FUNCTION refresh_learning_feedback_engine_v1(
    p_window_days INTEGER DEFAULT 30,
    p_min_observe_sample INTEGER DEFAULT 10,
    p_min_action_sample INTEGER DEFAULT 30
)
RETURNS JSONB LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO learning_slot_statistics_v1 (
        environment, symbol, interval, strategy, window_days,
        decisions, calculated_at
    ) VALUES (
        'trading_live', 'BTCUSDC', '1m', 'RSI', p_window_days,
        p_min_observe_sample, clock_timestamp()
    )
    ON CONFLICT (
        environment, symbol, interval, strategy, window_days
    )
    DO UPDATE SET
        decisions = EXCLUDED.decisions,
        calculated_at = EXCLUDED.calculated_at;

    INSERT INTO learning_calibration_proposals_v1 (
        proposal_key, environment, symbol, interval, strategy, window_days,
        decisions, last_seen_at, refreshed_at, validation_status
    ) VALUES (
        'v1-current', 'trading_live', 'BTCUSDC', '1m', 'RSI', p_window_days,
        p_min_observe_sample, clock_timestamp(), clock_timestamp(), 'PENDING'
    )
    ON CONFLICT (proposal_key)
    DO UPDATE SET
        decisions = EXCLUDED.decisions,
        last_seen_at = EXCLUDED.last_seen_at,
        refreshed_at = EXCLUDED.refreshed_at,
        validation_status = CASE
            WHEN learning_calibration_proposals_v1.validation_status
                 IN ('APPLIED', 'REJECTED')
                THEN learning_calibration_proposals_v1.validation_status
            ELSE 'PENDING'
        END;
    RETURN jsonb_build_object('status', 'ok');
END;
$$;

CREATE OR REPLACE FUNCTION refresh_learning_feedback_engine_v1_1(
    p_window_days INTEGER DEFAULT 30,
    p_min_observe_sample INTEGER DEFAULT 10,
    p_min_action_sample INTEGER DEFAULT 30
)
RETURNS JSONB LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO learning_calibration_proposals_v1 (
        proposal_key, environment, symbol, interval, strategy, window_days,
        decisions, last_seen_at, refreshed_at, validation_status
    ) VALUES (
        'v1-1-current', 'trading_live', 'BTCUSDC', '1m', 'RSI', p_window_days,
        p_min_observe_sample, clock_timestamp(), clock_timestamp(), 'PENDING'
    )
    ON CONFLICT (proposal_key)
    DO UPDATE SET
        decisions = EXCLUDED.decisions,
        last_seen_at = EXCLUDED.last_seen_at,
        refreshed_at = EXCLUDED.refreshed_at;
    RETURN jsonb_build_object('status', 'ok');
END;
$$;

INSERT INTO learning_canonical_source_snapshots_v2
    (snapshot_token, snapshot_status, feedback_run_id)
VALUES
    ('11111111-1111-4111-8111-111111111111', 'COMPLETE', 1),
    ('22222222-2222-4222-8222-222222222222', 'COMPLETE', 2);

-- First Frozen V2 capture stamps the current-state rows.
SELECT set_config(
    'waltrade.learning_source_snapshot_token',
    '11111111-1111-4111-8111-111111111111',
    false
);
SELECT refresh_learning_feedback_engine_v1(30, 10, 30);
SELECT refresh_learning_feedback_engine_v1_1(30, 10, 30);

-- Reproduce the VPS failure exactly: the next feedback run targets the same
-- slot key, so OLD.source_snapshot_token is carried into NEW by the UPSERT.
DO $reproduce$
BEGIN
    PERFORM set_config(
        'waltrade.learning_source_snapshot_token',
        '22222222-2222-4222-8222-222222222222',
        false
    );
    BEGIN
        PERFORM refresh_learning_feedback_engine_v1(30, 10, 30);
        RAISE EXCEPTION 'EXPECTED_VPS_PAYLOAD_CONFLICT_NOT_RAISED';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'EXPECTED_VPS_PAYLOAD_CONFLICT_NOT_RAISED'
           OR SQLERRM <>
              'LEARNING_FROZEN_SOURCE_PAYLOAD_CONFLICT table=learning_slot_statistics_v1'
        THEN
            RAISE;
        END IF;
    END;
    IF (
        SELECT source_snapshot_token
          FROM learning_slot_statistics_v1
         WHERE symbol = 'BTCUSDC'
    ) <> '11111111-1111-4111-8111-111111111111'::UUID THEN
        RAISE EXCEPTION 'FAILED_CONFLICT_LEFT_PARTIAL_STATE';
    END IF;
END;
$reproduce$;

\ir ../../db/migrations/20260724_learning_frozen_source_snapshot_v2_1_payload_propagation.sql
\ir ../../db/migrations/20260724_learning_frozen_source_snapshot_v2_1_payload_propagation.sql

-- A new run now rolls current-state provenance forward explicitly. An
-- identical retry with the same token is idempotent.
SELECT set_config(
    'waltrade.learning_source_snapshot_token',
    '22222222-2222-4222-8222-222222222222',
    false
);
SELECT refresh_learning_feedback_engine_v1(30, 10, 30);
SELECT refresh_learning_feedback_engine_v1_1(30, 10, 30);
SELECT refresh_learning_feedback_engine_v1(30, 10, 30);
SELECT refresh_learning_feedback_engine_v1_1(30, 10, 30);

DO $postconditions$
DECLARE
    v_count INTEGER;
BEGIN
    IF EXISTS (
        SELECT 1 FROM learning_slot_statistics_v1
         WHERE source_snapshot_token <>
               '22222222-2222-4222-8222-222222222222'::UUID
    ) OR EXISTS (
        SELECT 1 FROM learning_calibration_proposals_v1
         WHERE source_snapshot_token <>
               '22222222-2222-4222-8222-222222222222'::UUID
    ) THEN
        RAISE EXCEPTION 'CURRENT_STATE_TOKEN_NOT_ROLLED_FORWARD';
    END IF;

    SELECT count(*) INTO v_count FROM learning_slot_statistics_v1;
    IF v_count <> 1 THEN
        RAISE EXCEPTION 'SAME_DECISION_IDENTITY_DUPLICATED %', v_count;
    END IF;

    -- A distinct slot identity remains a distinct row.
    INSERT INTO learning_slot_statistics_v1 (
        environment, symbol, interval, strategy, window_days,
        decisions, calculated_at
    ) VALUES (
        'trading_live', 'ETHUSDC', '1m', 'RSI', 30, 10, clock_timestamp()
    );
    SELECT count(*) INTO v_count FROM learning_slot_statistics_v1;
    IF v_count <> 2 THEN
        RAISE EXCEPTION 'DISTINCT_DECISION_IDENTITY_NOT_INSERTED';
    END IF;

    -- The original COMPLETE snapshots were never updated or deleted.
    IF (SELECT count(*) FROM learning_canonical_source_snapshots_v2) <> 2
       OR EXISTS (
           SELECT 1 FROM learning_canonical_source_snapshots_v2
            WHERE snapshot_status <> 'COMPLETE'
       ) THEN
        RAISE EXCEPTION 'COMPLETE_SNAPSHOT_CHANGED';
    END IF;

    -- The unchanged trigger must still reject an explicitly wrong token.
    BEGIN
        INSERT INTO learning_slot_statistics_v1 (
            environment, symbol, interval, strategy, window_days,
            decisions, calculated_at, source_snapshot_token
        ) VALUES (
            'trading_live', 'SOLUSDC', '1m', 'RSI', 30, 10,
            clock_timestamp(),
            '11111111-1111-4111-8111-111111111111'
        );
        RAISE EXCEPTION 'TRUE_PAYLOAD_CONFLICT_NOT_RAISED';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'TRUE_PAYLOAD_CONFLICT_NOT_RAISED'
           OR SQLERRM <>
              'LEARNING_FROZEN_SOURCE_PAYLOAD_CONFLICT table=learning_slot_statistics_v1'
        THEN
            RAISE;
        END IF;
    END;
END;
$postconditions$;

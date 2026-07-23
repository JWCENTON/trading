BEGIN;

DO $prerequisites$
BEGIN
    IF to_regprocedure(
        'learning_canonical_evidence_universe_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
    ) IS NULL
       OR to_regprocedure(
        'refresh_learning_feedback_engine_v1_2_if_due(integer,integer,integer,integer,boolean,text)'
    ) IS NULL
       OR to_regclass('public.learning_evidence_manifests_v1') IS NULL
       OR to_regclass('public.learning_evidence_membership_v1') IS NULL
       OR to_regclass('public.learning_evidence_aggregates_v1') IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_V2_PREREQUISITE_MISSING';
    END IF;
END;
$prerequisites$;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS learning_canonical_source_snapshots_v2 (
    snapshot_token UUID PRIMARY KEY,
    feedback_run_id BIGINT NOT NULL UNIQUE
        REFERENCES learning_feedback_refresh_runs_v1(id),
    deployment_instance_id TEXT NOT NULL,
    environment TEXT NOT NULL CHECK (environment IN ('live', 'paper')),
    deployment_id TEXT NOT NULL,
    source_environment TEXT NOT NULL,
    evidence_window_start TIMESTAMPTZ NOT NULL,
    evidence_window_end TIMESTAMPTZ NOT NULL,
    evidence_cutoff_at TIMESTAMPTZ NOT NULL,
    source_snapshot_at TIMESTAMPTZ NOT NULL,
    snapshot_status TEXT NOT NULL
        CHECK (snapshot_status IN ('BUILDING', 'COMPLETE')),
    source_row_count INTEGER NOT NULL DEFAULT 0
        CHECK (source_row_count >= 0),
    eligible_row_count INTEGER NOT NULL DEFAULT 0
        CHECK (eligible_row_count >= 0),
    snapshot_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    CHECK (deployment_id = deployment_instance_id || '-' || environment),
    CHECK (evidence_window_start <= evidence_window_end),
    CHECK (evidence_window_end = evidence_cutoff_at),
    CHECK (
        (snapshot_status = 'BUILDING'
         AND snapshot_hash IS NULL AND completed_at IS NULL)
        OR
        (snapshot_status = 'COMPLETE'
         AND length(snapshot_hash) = 64 AND completed_at IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS learning_canonical_source_snapshot_rows_v2 (
    snapshot_token UUID NOT NULL
        REFERENCES learning_canonical_source_snapshots_v2(snapshot_token),
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    environment TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    strategy TEXT NOT NULL,
    decision_key TEXT NOT NULL,
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
    has_full_context BOOLEAN NOT NULL,
    has_avoid_review BOOLEAN NOT NULL,
    has_entry_quality_review BOOLEAN NOT NULL,
    has_positive_confirmation BOOLEAN NOT NULL,
    eligibility_reason TEXT NOT NULL,
    registry_available_at TIMESTAMPTZ,
    outcome_available_at TIMESTAMPTZ,
    row_hash TEXT NOT NULL CHECK (length(row_hash) = 64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (snapshot_token, ordinal),
    UNIQUE (snapshot_token, decision_key)
);

CREATE INDEX IF NOT EXISTS
    ix_learning_canonical_snapshot_rows_v2_slot
ON learning_canonical_source_snapshot_rows_v2 (
    snapshot_token, symbol, interval, strategy, eligibility_reason, ordinal
);

ALTER TABLE learning_slot_statistics_v1
    ADD COLUMN IF NOT EXISTS source_snapshot_token UUID;
ALTER TABLE learning_calibration_proposals_v1
    ADD COLUMN IF NOT EXISTS source_snapshot_token UUID;
ALTER TABLE learning_proposal_observations_v1
    ADD COLUMN IF NOT EXISTS source_snapshot_token UUID;
ALTER TABLE learning_canonical_evidence_selection_v1
    ADD COLUMN IF NOT EXISTS source_snapshot_token UUID;
ALTER TABLE learning_evidence_manifests_v1
    ADD COLUMN IF NOT EXISTS source_snapshot_token UUID;

DO $foreign_keys$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'learning_slot_statistics_snapshot_v2_fk'
    ) THEN
        ALTER TABLE learning_slot_statistics_v1
            ADD CONSTRAINT learning_slot_statistics_snapshot_v2_fk
            FOREIGN KEY (source_snapshot_token)
            REFERENCES learning_canonical_source_snapshots_v2(snapshot_token);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'learning_calibration_proposals_snapshot_v2_fk'
    ) THEN
        ALTER TABLE learning_calibration_proposals_v1
            ADD CONSTRAINT learning_calibration_proposals_snapshot_v2_fk
            FOREIGN KEY (source_snapshot_token)
            REFERENCES learning_canonical_source_snapshots_v2(snapshot_token);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'learning_proposal_observations_snapshot_v2_fk'
    ) THEN
        ALTER TABLE learning_proposal_observations_v1
            ADD CONSTRAINT learning_proposal_observations_snapshot_v2_fk
            FOREIGN KEY (source_snapshot_token)
            REFERENCES learning_canonical_source_snapshots_v2(snapshot_token);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'learning_canonical_selection_snapshot_v2_fk'
    ) THEN
        ALTER TABLE learning_canonical_evidence_selection_v1
            ADD CONSTRAINT learning_canonical_selection_snapshot_v2_fk
            FOREIGN KEY (source_snapshot_token)
            REFERENCES learning_canonical_source_snapshots_v2(snapshot_token);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'learning_evidence_manifest_snapshot_v2_fk'
    ) THEN
        ALTER TABLE learning_evidence_manifests_v1
            ADD CONSTRAINT learning_evidence_manifest_snapshot_v2_fk
            FOREIGN KEY (source_snapshot_token)
            REFERENCES learning_canonical_source_snapshots_v2(snapshot_token);
    END IF;
END;
$foreign_keys$;

CREATE OR REPLACE FUNCTION prevent_learning_frozen_source_mutation_v2()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'learning_canonical_source_snapshots_v2' THEN
        IF TG_OP = 'UPDATE'
           AND OLD.snapshot_status = 'BUILDING'
           AND NEW.snapshot_status = 'COMPLETE'
           AND NEW.snapshot_token = OLD.snapshot_token
           AND NEW.feedback_run_id = OLD.feedback_run_id
           AND NEW.deployment_instance_id = OLD.deployment_instance_id
           AND NEW.environment = OLD.environment
           AND NEW.deployment_id = OLD.deployment_id
           AND NEW.source_environment = OLD.source_environment
           AND NEW.evidence_window_start = OLD.evidence_window_start
           AND NEW.evidence_window_end = OLD.evidence_window_end
           AND NEW.evidence_cutoff_at = OLD.evidence_cutoff_at
           AND NEW.source_snapshot_at = OLD.source_snapshot_at
           AND NEW.created_at = OLD.created_at
        THEN
            RETURN NEW;
        END IF;
    END IF;
    RAISE EXCEPTION
        'LEARNING_FROZEN_SOURCE_IMMUTABLE table=% operation=%',
        TG_TABLE_NAME, TG_OP;
END;
$$;

DROP TRIGGER IF EXISTS learning_frozen_snapshot_immutable_v2
    ON learning_canonical_source_snapshots_v2;
CREATE TRIGGER learning_frozen_snapshot_immutable_v2
BEFORE UPDATE OR DELETE ON learning_canonical_source_snapshots_v2
FOR EACH ROW EXECUTE FUNCTION prevent_learning_frozen_source_mutation_v2();

DROP TRIGGER IF EXISTS learning_frozen_snapshot_rows_immutable_v2
    ON learning_canonical_source_snapshot_rows_v2;
CREATE TRIGGER learning_frozen_snapshot_rows_immutable_v2
BEFORE UPDATE OR DELETE ON learning_canonical_source_snapshot_rows_v2
FOR EACH ROW EXECUTE FUNCTION prevent_learning_frozen_source_mutation_v2();

-- Preserve the exact deployed live query once. The public function becomes a
-- context-aware reader: live outside a Learning run, frozen inside one.
DO $preserve_live_universe$
DECLARE
    v_signature CONSTANT TEXT :=
        'learning_canonical_evidence_universe_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)';
    v_definition TEXT;
BEGIN
    IF to_regprocedure(
        'learning_canonical_evidence_universe_live_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
    ) IS NULL THEN
        v_definition := pg_get_functiondef(to_regprocedure(v_signature));
        IF position('learning_canonical_source_snapshot_rows_v2'
                    IN v_definition) > 0 THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_V2_UNEXPECTED_UNIVERSE_DEFINITION';
        END IF;
        v_definition := replace(
            v_definition,
            'learning_canonical_evidence_universe_v1',
            'learning_canonical_evidence_universe_live_v1'
        );
        EXECUTE v_definition;
    END IF;
END;
$preserve_live_universe$;

CREATE OR REPLACE FUNCTION learning_canonical_evidence_universe_v1(
    p_environment TEXT,
    p_sample_from TIMESTAMPTZ,
    p_sample_to TIMESTAMPTZ,
    p_evidence_cutoff_at TIMESTAMPTZ
)
RETURNS TABLE (
    environment TEXT, symbol TEXT, "interval" TEXT, strategy TEXT,
    decision_key TEXT, decision_id UUID, position_id BIGINT,
    entry_time TIMESTAMPTZ, exit_time TIMESTAMPTZ,
    outcome_timestamp TIMESTAMPTZ, realized_pnl_usdc NUMERIC,
    gross_pnl_usdc NUMERIC, fees_usdc NUMERIC, mfe_pct NUMERIC,
    mae_pct NUMERIC, regime_identity TEXT, regime_context JSONB,
    source_refreshed_at TIMESTAMPTZ, has_full_context BOOLEAN,
    has_avoid_review BOOLEAN, has_entry_quality_review BOOLEAN,
    has_positive_confirmation BOOLEAN, eligibility_reason TEXT,
    registry_available_at TIMESTAMPTZ, outcome_available_at TIMESTAMPTZ
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_token_text TEXT := current_setting(
        'waltrade.learning_source_snapshot_token', true
    );
    v_token UUID;
    v_header learning_canonical_source_snapshots_v2%ROWTYPE;
BEGIN
    IF v_token_text IS NULL OR v_token_text = '' THEN
        RETURN QUERY
        SELECT * FROM learning_canonical_evidence_universe_live_v1(
            p_environment, p_sample_from, p_sample_to, p_evidence_cutoff_at
        );
        RETURN;
    END IF;

    v_token := v_token_text::UUID;
    SELECT * INTO STRICT v_header
      FROM learning_canonical_source_snapshots_v2
     WHERE snapshot_token = v_token
       AND snapshot_status = 'COMPLETE';
    IF p_environment <> v_header.source_environment THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_CONTEXT_MISMATCH snapshot=% expected=% actual=%',
            v_token, v_header.source_environment, p_environment;
    END IF;

    RETURN QUERY
    SELECT r.environment, r.symbol, r.interval, r.strategy, r.decision_key,
           r.decision_id, r.position_id, r.entry_time, r.exit_time,
           r.outcome_timestamp, r.realized_pnl_usdc, r.gross_pnl_usdc,
           r.fees_usdc, r.mfe_pct, r.mae_pct, r.regime_identity,
           r.regime_context, r.source_refreshed_at, r.has_full_context,
           r.has_avoid_review, r.has_entry_quality_review,
           r.has_positive_confirmation, r.eligibility_reason,
           r.registry_available_at, r.outcome_available_at
      FROM learning_canonical_source_snapshot_rows_v2 r
     WHERE r.snapshot_token = v_token
     ORDER BY r.ordinal;
END;
$$;

CREATE OR REPLACE FUNCTION capture_learning_canonical_source_snapshot_v2(
    p_feedback_run_id BIGINT
)
RETURNS UUID LANGUAGE plpgsql AS $$
DECLARE
    v_run learning_feedback_refresh_runs_v1%ROWTYPE;
    v_identity RECORD;
    v_token UUID;
    v_existing learning_canonical_source_snapshots_v2%ROWTYPE;
    v_source_environment TEXT;
    v_cutoff TIMESTAMPTZ;
    v_window_start TIMESTAMPTZ;
    v_source_snapshot_at TIMESTAMPTZ := clock_timestamp();
    v_source_count INTEGER;
    v_eligible_count INTEGER;
    v_snapshot_hash TEXT;
BEGIN
    SELECT * INTO STRICT v_run
      FROM learning_feedback_refresh_runs_v1
     WHERE id = p_feedback_run_id AND status IN ('RUNNING', 'OK');
    SELECT * INTO STRICT v_identity
      FROM learning_evidence_runtime_identity_v1();
    v_source_environment := CASE v_identity.environment
        WHEN 'live' THEN 'trading_live'
        WHEN 'paper' THEN 'trading_paper'
    END;
    IF v_run.environment <> v_source_environment THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_DATABASE_IDENTITY_MISMATCH run=% runtime=%',
            v_run.environment, v_source_environment;
    END IF;
    v_cutoff := COALESCE(v_run.started_at, v_run.requested_at);
    v_window_start := v_cutoff - make_interval(days => v_run.window_days);

    SELECT * INTO v_existing
      FROM learning_canonical_source_snapshots_v2
     WHERE feedback_run_id = p_feedback_run_id;
    IF FOUND THEN
        IF v_existing.snapshot_status <> 'COMPLETE'
           OR v_existing.deployment_instance_id <> v_identity.deployment_instance_id
           OR v_existing.environment <> v_identity.environment
           OR v_existing.deployment_id <> v_identity.deployment_id
           OR v_existing.source_environment <> v_source_environment
           OR v_existing.evidence_window_start <> v_window_start
           OR v_existing.evidence_window_end <> v_cutoff
           OR v_existing.evidence_cutoff_at <> v_cutoff
           OR v_existing.source_row_count <> (
                SELECT count(*) FROM learning_canonical_source_snapshot_rows_v2
                 WHERE snapshot_token = v_existing.snapshot_token
           )
           OR v_existing.snapshot_hash <> (
                SELECT encode(digest(COALESCE(string_agg(
                    row_hash, E'\n' ORDER BY ordinal), ''), 'sha256'), 'hex')
                  FROM learning_canonical_source_snapshot_rows_v2
                 WHERE snapshot_token = v_existing.snapshot_token
           )
        THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_IDEMPOTENCY_CONFLICT run=%',
                p_feedback_run_id;
        END IF;
        PERFORM set_config(
            'waltrade.learning_source_snapshot_token',
            v_existing.snapshot_token::TEXT, true
        );
        RETURN v_existing.snapshot_token;
    END IF;
    IF v_run.status <> 'RUNNING' THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_NEW_SNAPSHOT_REQUIRES_RUNNING run=% status=%',
            p_feedback_run_id, v_run.status;
    END IF;

    v_token := gen_random_uuid();
    INSERT INTO learning_canonical_source_snapshots_v2 (
        snapshot_token, feedback_run_id, deployment_instance_id, environment,
        deployment_id, source_environment, evidence_window_start,
        evidence_window_end, evidence_cutoff_at, source_snapshot_at,
        snapshot_status
    ) VALUES (
        v_token, p_feedback_run_id, v_identity.deployment_instance_id,
        v_identity.environment, v_identity.deployment_id,
        v_source_environment, v_window_start, v_cutoff, v_cutoff,
        v_source_snapshot_at, 'BUILDING'
    );

    INSERT INTO learning_canonical_source_snapshot_rows_v2 (
        snapshot_token, ordinal, environment, symbol, interval, strategy,
        decision_key, decision_id, position_id, entry_time, exit_time,
        outcome_timestamp, realized_pnl_usdc, gross_pnl_usdc, fees_usdc,
        mfe_pct, mae_pct, regime_identity, regime_context,
        source_refreshed_at, has_full_context, has_avoid_review,
        has_entry_quality_review, has_positive_confirmation,
        eligibility_reason, registry_available_at, outcome_available_at,
        row_hash
    )
    SELECT v_token, row_number() OVER (ORDER BY u.decision_key),
           u.environment, u.symbol, u.interval, u.strategy, u.decision_key,
           u.decision_id, u.position_id, u.entry_time, u.exit_time,
           u.outcome_timestamp, u.realized_pnl_usdc, u.gross_pnl_usdc,
           u.fees_usdc, u.mfe_pct, u.mae_pct, u.regime_identity,
           u.regime_context, u.source_refreshed_at, u.has_full_context,
           u.has_avoid_review, u.has_entry_quality_review,
           u.has_positive_confirmation, u.eligibility_reason,
           u.registry_available_at, u.outcome_available_at,
           encode(digest(jsonb_build_array(
               u.environment, u.symbol, u.interval, u.strategy,
               u.decision_key, u.decision_id, u.position_id, u.entry_time,
               u.exit_time, u.outcome_timestamp, u.realized_pnl_usdc,
               u.gross_pnl_usdc, u.fees_usdc, u.mfe_pct, u.mae_pct,
               u.regime_identity, u.regime_context, u.source_refreshed_at,
               u.has_full_context, u.has_avoid_review,
               u.has_entry_quality_review, u.has_positive_confirmation,
               u.eligibility_reason, u.registry_available_at,
               u.outcome_available_at
           )::TEXT, 'sha256'), 'hex')
      FROM learning_canonical_evidence_universe_live_v1(
           v_source_environment, v_window_start, v_cutoff, v_cutoff
      ) u
     ORDER BY u.decision_key;
    GET DIAGNOSTICS v_source_count = ROW_COUNT;

    SELECT count(*) FILTER (WHERE eligibility_reason = 'ELIGIBLE'),
           encode(digest(COALESCE(string_agg(
               row_hash, E'\n' ORDER BY ordinal), ''), 'sha256'), 'hex')
      INTO v_eligible_count, v_snapshot_hash
      FROM learning_canonical_source_snapshot_rows_v2
     WHERE snapshot_token = v_token;

    UPDATE learning_canonical_source_snapshots_v2
       SET snapshot_status = 'COMPLETE',
           source_row_count = v_source_count,
           eligible_row_count = v_eligible_count,
           snapshot_hash = v_snapshot_hash,
           completed_at = clock_timestamp()
     WHERE snapshot_token = v_token AND snapshot_status = 'BUILDING';
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_FINALIZE_CONFLICT snapshot=%', v_token;
    END IF;
    PERFORM set_config(
        'waltrade.learning_source_snapshot_token', v_token::TEXT, true
    );
    RETURN v_token;
END;
$$;

CREATE OR REPLACE FUNCTION propagate_learning_source_snapshot_token_v2()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_token_text TEXT := current_setting(
        'waltrade.learning_source_snapshot_token', true
    );
    v_token UUID;
    v_header learning_canonical_source_snapshots_v2%ROWTYPE;
BEGIN
    IF v_token_text IS NULL OR v_token_text = '' THEN
        RETURN NEW;
    END IF;
    v_token := v_token_text::UUID;
    SELECT * INTO STRICT v_header
      FROM learning_canonical_source_snapshots_v2
     WHERE snapshot_token = v_token AND snapshot_status = 'COMPLETE';
    IF NEW.source_snapshot_token IS NOT NULL
       AND NEW.source_snapshot_token <> v_token THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_PAYLOAD_CONFLICT table=%', TG_TABLE_NAME;
    END IF;
    NEW.source_snapshot_token := v_token;
    IF TG_TABLE_NAME = 'learning_evidence_manifests_v1' THEN
        IF NEW.feedback_run_id <> v_header.feedback_run_id THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_RUN_CONFLICT manifest_run=% snapshot_run=%',
                NEW.feedback_run_id, v_header.feedback_run_id;
        END IF;
        NEW.evidence_window_start := v_header.evidence_window_start;
        NEW.evidence_window_end := v_header.evidence_window_end;
        NEW.evidence_cutoff_at := v_header.evidence_cutoff_at;
        NEW.source_snapshot_at := v_header.source_snapshot_at;
    END IF;
    RETURN NEW;
END;
$$;

DO $token_triggers$
DECLARE
    v_table TEXT;
BEGIN
    FOREACH v_table IN ARRAY ARRAY[
        'learning_slot_statistics_v1',
        'learning_calibration_proposals_v1',
        'learning_proposal_observations_v1',
        'learning_canonical_evidence_selection_v1',
        'learning_evidence_manifests_v1'
    ] LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS propagate_learning_source_snapshot_v2 ON %I',
            v_table
        );
        EXECUTE format(
            'CREATE TRIGGER propagate_learning_source_snapshot_v2 '
            'BEFORE INSERT OR UPDATE ON %I FOR EACH ROW '
            'EXECUTE FUNCTION propagate_learning_source_snapshot_token_v2()',
            v_table
        );
    END LOOP;
END;
$token_triggers$;

-- Insert capture after the RUNNING header exists and before Feedback reads its
-- source. Preserve a byte-exact backup for a safe no-data rollback.
DO $patch_due_wrapper$
DECLARE
    v_signature CONSTANT TEXT :=
        'refresh_learning_feedback_engine_v1_2_if_due(integer,integer,integer,integer,boolean,text)';
    v_definition TEXT;
    v_backup TEXT;
    v_anchor TEXT := 'RETURNING id INTO v_run_id;';
BEGIN
    v_definition := pg_get_functiondef(to_regprocedure(v_signature));
    IF to_regprocedure(
        'refresh_learning_feedback_v1_2_pre_snapshot_v2(integer,integer,integer,integer,boolean,text)'
    ) IS NULL THEN
        v_backup := replace(
            v_definition,
            'refresh_learning_feedback_engine_v1_2_if_due',
            'refresh_learning_feedback_v1_2_pre_snapshot_v2'
        );
        EXECUTE v_backup;
    END IF;
    IF position('capture_learning_canonical_source_snapshot_v2'
                IN v_definition) = 0 THEN
        IF position(v_anchor IN v_definition) = 0 THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_V2_WRAPPER_PATCH_ANCHOR_MISSING';
        END IF;
        v_definition := replace(
            v_definition, v_anchor,
            v_anchor || E'\n\n    PERFORM capture_learning_canonical_source_snapshot_v2(v_run_id);'
        );
        EXECUTE v_definition;
    END IF;
END;
$patch_due_wrapper$;

CREATE OR REPLACE FUNCTION validate_learning_frozen_source_parity_v2()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_snapshot_count INTEGER;
    v_stats_count INTEGER;
    v_proposal_count INTEGER;
    v_observation_count INTEGER;
    v_selection_count INTEGER;
    v_aggregate_count INTEGER;
    v_membership_count INTEGER;
BEGIN
    IF NEW.manifest_status <> 'COMPLETE'
       OR NEW.source_snapshot_token IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT count(*) INTO v_snapshot_count
      FROM learning_canonical_source_snapshot_rows_v2
     WHERE snapshot_token = NEW.source_snapshot_token
       AND symbol = NEW.symbol AND interval = NEW.interval
       AND strategy = NEW.strategy AND eligibility_reason = 'ELIGIBLE';
    SELECT decisions INTO STRICT v_stats_count
      FROM learning_slot_statistics_v1
     WHERE environment = CASE NEW.environment
             WHEN 'live' THEN 'trading_live'
             WHEN 'paper' THEN 'trading_paper' END
       AND symbol = NEW.symbol AND interval = NEW.interval
       AND strategy = NEW.strategy AND window_days = NEW.window_days
       AND source_snapshot_token = NEW.source_snapshot_token;
    SELECT evidence_decisions INTO STRICT v_proposal_count
      FROM learning_calibration_proposals_v1
     WHERE environment = CASE NEW.environment
             WHEN 'live' THEN 'trading_live'
             WHEN 'paper' THEN 'trading_paper' END
       AND symbol = NEW.symbol AND interval = NEW.interval
       AND strategy = NEW.strategy AND window_days = NEW.window_days
       AND source_snapshot_token = NEW.source_snapshot_token;
    SELECT evidence_decisions INTO STRICT v_observation_count
      FROM learning_proposal_observations_v1
     WHERE refresh_run_id = NEW.feedback_run_id
       AND symbol = NEW.symbol AND interval = NEW.interval
       AND strategy = NEW.strategy AND window_days = NEW.window_days
       AND source_snapshot_token = NEW.source_snapshot_token;
    SELECT canonical_eligible_count INTO STRICT v_selection_count
      FROM learning_canonical_evidence_selection_v1
     WHERE feedback_run_id = NEW.feedback_run_id
       AND symbol = NEW.symbol AND interval = NEW.interval
       AND strategy = NEW.strategy AND window_days = NEW.window_days
       AND source_snapshot_token = NEW.source_snapshot_token;
    SELECT decisions INTO STRICT v_aggregate_count
      FROM learning_evidence_aggregates_v1
     WHERE evidence_manifest_id = NEW.evidence_manifest_id;
    SELECT count(*) INTO v_membership_count
      FROM learning_evidence_membership_v1
     WHERE evidence_manifest_id = NEW.evidence_manifest_id;

    IF v_snapshot_count <> NEW.evidence_decision_count
       OR v_stats_count <> v_snapshot_count
       OR v_proposal_count <> v_snapshot_count
       OR v_observation_count <> v_snapshot_count
       OR v_selection_count <> v_snapshot_count
       OR v_aggregate_count <> v_snapshot_count
       OR v_membership_count <> v_snapshot_count THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_PARITY_MISMATCH manifest=% snapshot=% stats=% proposal=% observation=% selection=% aggregate=% membership=% header=%',
            NEW.evidence_manifest_id, v_snapshot_count, v_stats_count,
            v_proposal_count, v_observation_count, v_selection_count,
            v_aggregate_count, v_membership_count,
            NEW.evidence_decision_count;
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS learning_frozen_source_parity_v2
    ON learning_evidence_manifests_v1;
CREATE CONSTRAINT TRIGGER learning_frozen_source_parity_v2
AFTER INSERT OR UPDATE ON learning_evidence_manifests_v1
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_learning_frozen_source_parity_v2();

COMMIT;

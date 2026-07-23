\set ON_ERROR_STOP on

DO $schema_contract$
DECLARE
    v_table TEXT;
BEGIN
    FOREACH v_table IN ARRAY ARRAY[
        'learning_canonical_source_snapshots_v2',
        'learning_canonical_source_snapshot_rows_v2'
    ] LOOP
        IF to_regclass('public.' || v_table) IS NULL THEN
            RAISE EXCEPTION 'FROZEN_SOURCE_V2_TABLE_MISSING %', v_table;
        END IF;
    END LOOP;
    IF to_regprocedure(
        'capture_learning_canonical_source_snapshot_v2(bigint)'
    ) IS NULL THEN
        RAISE EXCEPTION 'FROZEN_SOURCE_V2_CAPTURE_MISSING';
    END IF;
    IF position(
        'capture_learning_canonical_source_snapshot_v2'
        IN pg_get_functiondef(
            'refresh_learning_feedback_engine_v1_2_if_due(integer,integer,integer,integer,boolean,text)'
            ::regprocedure
        )
    ) = 0 THEN
        RAISE EXCEPTION 'FROZEN_SOURCE_V2_WRAPPER_NOT_PATCHED';
    END IF;
END;
$schema_contract$;

DO $legacy_contract$
DECLARE
    v_manifest learning_evidence_manifests_v1%ROWTYPE;
BEGIN
    SELECT * INTO v_manifest
      FROM learning_evidence_manifests_v1
     WHERE evidence_manifest_id = 'ada8a02a-49d8-4344-b451-886cf25022c3';
    IF FOUND THEN
        IF v_manifest.manifest_status <> 'LEGACY_AGGREGATE_ONLY'
           OR v_manifest.exact_membership_available <> false
           OR v_manifest.evidence_decision_count <> 39
           OR v_manifest.source_snapshot_token IS NOT NULL THEN
            RAISE EXCEPTION 'FROZEN_SOURCE_V2_LEGACY_HISTORY_CHANGED';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM learning_proposal_observations_v1
             WHERE refresh_run_id = v_manifest.feedback_run_id
               AND evidence_decisions = 39
        ) THEN
            RAISE EXCEPTION 'FROZEN_SOURCE_V2_LEGACY_OBSERVATION_CHANGED';
        END IF;
    END IF;
    IF EXISTS (
        SELECT 1 FROM learning_evidence_manifests_v1
         WHERE manifest_status = 'LEGACY_AGGREGATE_ONLY'
           AND (exact_membership_available OR source_snapshot_token IS NOT NULL)
    ) THEN
        RAISE EXCEPTION 'FROZEN_SOURCE_V2_LEGACY_BACKFILL_DETECTED';
    END IF;
END;
$legacy_contract$;

DO $complete_snapshot_integrity$
DECLARE
    v_bad INTEGER;
BEGIN
    SELECT count(*) INTO v_bad
      FROM learning_canonical_source_snapshots_v2 h
     WHERE h.snapshot_status = 'COMPLETE'
       AND (
           h.source_row_count <> (
               SELECT count(*)
                 FROM learning_canonical_source_snapshot_rows_v2 r
                WHERE r.snapshot_token = h.snapshot_token
           )
           OR h.eligible_row_count <> (
               SELECT count(*)
                 FROM learning_canonical_source_snapshot_rows_v2 r
                WHERE r.snapshot_token = h.snapshot_token
                  AND r.eligibility_reason = 'ELIGIBLE'
           )
           OR h.snapshot_hash <> (
               SELECT encode(digest(COALESCE(string_agg(
                   r.row_hash, E'\n' ORDER BY r.ordinal), ''), 'sha256'), 'hex')
                 FROM learning_canonical_source_snapshot_rows_v2 r
                WHERE r.snapshot_token = h.snapshot_token
           )
       );
    IF v_bad <> 0 THEN
        RAISE EXCEPTION 'FROZEN_SOURCE_V2_SNAPSHOT_HASH_OR_COUNT_MISMATCH %',
            v_bad;
    END IF;
    IF EXISTS (
        SELECT 1 FROM learning_canonical_source_snapshots_v2
         WHERE snapshot_status = 'BUILDING'
    ) THEN
        RAISE EXCEPTION 'FROZEN_SOURCE_V2_BUILDING_LEFTOVER';
    END IF;
END;
$complete_snapshot_integrity$;

DO $all_slot_parity$
DECLARE
    v_bad INTEGER;
BEGIN
    SELECT count(*) INTO v_bad
      FROM learning_evidence_manifests_v1 m
      JOIN learning_canonical_source_snapshots_v2 h
        ON h.snapshot_token = m.source_snapshot_token
      JOIN learning_slot_statistics_v1 s
        ON s.source_snapshot_token = h.snapshot_token
       AND (s.symbol, s.interval, s.strategy, s.window_days)
         = (m.symbol, m.interval, m.strategy, m.window_days)
      JOIN learning_calibration_proposals_v1 p
        ON p.source_snapshot_token = h.snapshot_token
       AND (p.symbol, p.interval, p.strategy, p.window_days)
         = (m.symbol, m.interval, m.strategy, m.window_days)
      JOIN learning_proposal_observations_v1 o
        ON o.refresh_run_id = m.feedback_run_id
       AND (o.symbol, o.interval, o.strategy, o.window_days)
         = (m.symbol, m.interval, m.strategy, m.window_days)
      JOIN learning_canonical_evidence_selection_v1 c
        ON c.feedback_run_id = m.feedback_run_id
       AND (c.symbol, c.interval, c.strategy, c.window_days)
         = (m.symbol, m.interval, m.strategy, m.window_days)
      JOIN learning_evidence_aggregates_v1 a
        ON a.evidence_manifest_id = m.evidence_manifest_id
      CROSS JOIN LATERAL (
          SELECT count(*)::INTEGER AS frozen_count
            FROM learning_canonical_source_snapshot_rows_v2 r
           WHERE r.snapshot_token = h.snapshot_token
             AND r.symbol = m.symbol AND r.interval = m.interval
             AND r.strategy = m.strategy
             AND r.eligibility_reason = 'ELIGIBLE'
      ) f
     WHERE m.manifest_status = 'COMPLETE'
       AND (
           f.frozen_count <> s.decisions
           OR f.frozen_count <> p.evidence_decisions
           OR f.frozen_count <> o.evidence_decisions
           OR f.frozen_count <> c.canonical_eligible_count
           OR f.frozen_count <> a.decisions
           OR f.frozen_count <> m.evidence_decision_count
           OR f.frozen_count <> (
               SELECT count(*) FROM learning_evidence_membership_v1 mm
                WHERE mm.evidence_manifest_id = m.evidence_manifest_id
           )
       );
    IF v_bad <> 0 THEN
        RAISE EXCEPTION 'FROZEN_SOURCE_V2_ALL_SLOT_PARITY_MISMATCH %', v_bad;
    END IF;
END;
$all_slot_parity$;

-- Retry an already COMPLETE token. It must return the same identity without
-- reading the mutable live universe or inserting rows.
DO $retry_contract$
DECLARE
    v_header learning_canonical_source_snapshots_v2%ROWTYPE;
    v_retry UUID;
    v_before INTEGER;
    v_after INTEGER;
BEGIN
    SELECT * INTO v_header
      FROM learning_canonical_source_snapshots_v2
     WHERE snapshot_status = 'COMPLETE'
     ORDER BY feedback_run_id
     LIMIT 1;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'FROZEN_SOURCE_V2_COMPLETE_FIXTURE_MISSING';
    END IF;
    PERFORM set_config(
        'waltrade.deployment_instance_id',
        v_header.deployment_instance_id, true
    );
    PERFORM set_config('waltrade.environment', v_header.environment, true);
    SELECT count(*) INTO v_before
      FROM learning_canonical_source_snapshot_rows_v2
     WHERE snapshot_token = v_header.snapshot_token;
    v_retry := capture_learning_canonical_source_snapshot_v2(
        v_header.feedback_run_id
    );
    SELECT count(*) INTO v_after
      FROM learning_canonical_source_snapshot_rows_v2
     WHERE snapshot_token = v_header.snapshot_token;
    IF v_retry <> v_header.snapshot_token OR v_after <> v_before THEN
        RAISE EXCEPTION 'FROZEN_SOURCE_V2_RETRY_NOT_IDEMPOTENT';
    END IF;
END;
$retry_contract$;

-- Payload mutation must fail closed. The nested exception rolls back only the
-- attempted mutation and proves the COMPLETE evidence remains unchanged.
DO $immutability_contract$
DECLARE
    v_token UUID;
BEGIN
    SELECT snapshot_token INTO STRICT v_token
      FROM learning_canonical_source_snapshots_v2
     WHERE snapshot_status = 'COMPLETE'
     ORDER BY feedback_run_id
     LIMIT 1;
    BEGIN
        UPDATE learning_canonical_source_snapshot_rows_v2
           SET decision_key = decision_key || '-conflict'
         WHERE snapshot_token = v_token AND ordinal = 1;
        RAISE EXCEPTION 'FROZEN_SOURCE_V2_MUTATION_UNEXPECTEDLY_ALLOWED';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLERRM = 'FROZEN_SOURCE_V2_MUTATION_UNEXPECTEDLY_ALLOWED' THEN
                RAISE;
            END IF;
            IF position('LEARNING_FROZEN_SOURCE_IMMUTABLE' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;
END;
$immutability_contract$;

-- A failure after the run header and snapshot header were prepared must roll
-- back the entire construction subtransaction and leave no BUILDING residue.
DO $transaction_rollback_contract$
DECLARE
    v_before_runs INTEGER;
    v_before_snapshots INTEGER;
    v_run_id BIGINT;
    v_now TIMESTAMPTZ := clock_timestamp();
BEGIN
    SELECT count(*) INTO v_before_runs
      FROM learning_feedback_refresh_runs_v1
     WHERE trigger_source = 'FROZEN_SOURCE_V2_FAILURE_INJECTION';
    SELECT count(*) INTO v_before_snapshots
      FROM learning_canonical_source_snapshots_v2
     WHERE snapshot_status = 'BUILDING';
    BEGIN
        INSERT INTO learning_feedback_refresh_runs_v1 (
            environment, engine_version, requested_at, started_at,
            trigger_source, status, window_days, min_observe_sample,
            min_action_sample, interval_hours
        ) VALUES (
            current_database(), 'LEARNING_FEEDBACK_ENGINE_V1_2',
            clock_timestamp(), clock_timestamp(),
            'FROZEN_SOURCE_V2_FAILURE_INJECTION', 'RUNNING', 30, 10, 30, 12
        ) RETURNING id INTO v_run_id;
        INSERT INTO learning_canonical_source_snapshots_v2 (
            snapshot_token, feedback_run_id, deployment_instance_id,
            environment, deployment_id, source_environment,
            evidence_window_start, evidence_window_end, evidence_cutoff_at,
            source_snapshot_at, snapshot_status
        ) VALUES (
            gen_random_uuid(), v_run_id, 'local', 'live', 'local-live',
            current_database(), v_now - interval '30 days',
            v_now, v_now, v_now, 'BUILDING'
        );
        RAISE EXCEPTION 'FROZEN_SOURCE_V2_INJECTED_AFTER_HEADER_FAILURE';
    EXCEPTION
        WHEN OTHERS THEN
            IF position(
                'FROZEN_SOURCE_V2_INJECTED_AFTER_HEADER_FAILURE' IN SQLERRM
            ) = 0 THEN
                RAISE;
            END IF;
    END;
    IF (SELECT count(*) FROM learning_feedback_refresh_runs_v1
         WHERE trigger_source = 'FROZEN_SOURCE_V2_FAILURE_INJECTION')
       <> v_before_runs
       OR (SELECT count(*) FROM learning_canonical_source_snapshots_v2
            WHERE snapshot_status = 'BUILDING') <> v_before_snapshots THEN
        RAISE EXCEPTION 'FROZEN_SOURCE_V2_TRANSACTION_ROLLBACK_FAILED';
    END IF;
END;
$transaction_rollback_contract$;

SELECT 'LEARNING_CANONICAL_FROZEN_SOURCE_SNAPSHOT_V2_HARNESS_PASS';

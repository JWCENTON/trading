BEGIN;

DO $prerequisites$
BEGIN
    IF to_regprocedure(
        'refresh_learning_feedback_engine_v1(integer,integer,integer)'
    ) IS NULL
       OR to_regprocedure(
        'refresh_learning_feedback_engine_v1_1(integer,integer,integer)'
    ) IS NULL
       OR to_regprocedure(
        'propagate_learning_source_snapshot_token_v2()'
    ) IS NULL
       OR to_regclass('public.learning_canonical_source_snapshots_v2') IS NULL
       OR NOT EXISTS (
           SELECT 1
             FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'learning_slot_statistics_v1'
              AND column_name = 'source_snapshot_token'
       ) THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_V2_1_PREREQUISITE_MISSING';
    END IF;
END;
$prerequisites$;

-- learning_slot_statistics_v1 and learning_calibration_proposals_v1 are
-- current-state projections keyed independently of a feedback run. PostgreSQL
-- carries the stored token into NEW on their ON CONFLICT UPDATE paths unless
-- the assignment is explicit. Propagate the transaction-local token in those
-- assignments; the unchanged V2 trigger still rejects every explicitly
-- conflicting token and still verifies that the token names a COMPLETE
-- snapshot.
DO $patch_current_state_upserts$
DECLARE
    v_v1_signature CONSTANT TEXT :=
        'refresh_learning_feedback_engine_v1(integer,integer,integer)';
    v_v1_1_signature CONSTANT TEXT :=
        'refresh_learning_feedback_engine_v1_1(integer,integer,integer)';
    v_definition TEXT;
    v_old TEXT;
    v_new TEXT;
BEGIN
    v_definition := pg_get_functiondef(
        to_regprocedure(v_v1_signature)
    );

    IF position(
        'source_snapshot_token = COALESCE(' IN v_definition
    ) = 0 THEN
        v_old := '        calculated_at = EXCLUDED.calculated_at;';
        v_new := '        calculated_at = EXCLUDED.calculated_at,' || E'\n'
            || '        source_snapshot_token = COALESCE(' || E'\n'
            || '            NULLIF(current_setting(' || E'\n'
            || '                ''waltrade.learning_source_snapshot_token'', true'
            || E'\n'
            || '            ), '''')::UUID,' || E'\n'
            || '            learning_slot_statistics_v1.source_snapshot_token'
            || E'\n'
            || '        );';
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_V2_1_STATS_PATCH_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);

        v_old := '        last_seen_at = EXCLUDED.last_seen_at,' || E'\n'
            || '        refreshed_at = EXCLUDED.refreshed_at,' || E'\n'
            || '        validation_status = CASE';
        v_new := '        last_seen_at = EXCLUDED.last_seen_at,' || E'\n'
            || '        refreshed_at = EXCLUDED.refreshed_at,' || E'\n'
            || '        source_snapshot_token = COALESCE(' || E'\n'
            || '            NULLIF(current_setting(' || E'\n'
            || '                ''waltrade.learning_source_snapshot_token'', true'
            || E'\n'
            || '            ), '''')::UUID,' || E'\n'
            || '            learning_calibration_proposals_v1.source_snapshot_token'
            || E'\n'
            || '        ),' || E'\n'
            || '        validation_status = CASE';
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_V2_1_V1_PROPOSAL_PATCH_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);
        EXECUTE v_definition;
    END IF;

    v_definition := pg_get_functiondef(
        to_regprocedure(v_v1_1_signature)
    );
    IF position(
        'learning_calibration_proposals_v1.source_snapshot_token'
        IN v_definition
    ) = 0 THEN
        v_old := '        last_seen_at = EXCLUDED.last_seen_at,' || E'\n'
            || '        refreshed_at = EXCLUDED.refreshed_at;';
        v_new := '        last_seen_at = EXCLUDED.last_seen_at,' || E'\n'
            || '        refreshed_at = EXCLUDED.refreshed_at,' || E'\n'
            || '        source_snapshot_token = COALESCE(' || E'\n'
            || '            NULLIF(current_setting(' || E'\n'
            || '                ''waltrade.learning_source_snapshot_token'', true'
            || E'\n'
            || '            ), '''')::UUID,' || E'\n'
            || '            learning_calibration_proposals_v1.source_snapshot_token'
            || E'\n'
            || '        );';
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_V2_1_V1_1_PROPOSAL_PATCH_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);
        EXECUTE v_definition;
    END IF;
END;
$patch_current_state_upserts$;

DO $postcondition$
DECLARE
    v_v1 TEXT := pg_get_functiondef(
        'refresh_learning_feedback_engine_v1(integer,integer,integer)'
        ::regprocedure
    );
    v_v1_1 TEXT := pg_get_functiondef(
        'refresh_learning_feedback_engine_v1_1(integer,integer,integer)'
        ::regprocedure
    );
    v_guard TEXT := pg_get_functiondef(
        'propagate_learning_source_snapshot_token_v2()'::regprocedure
    );
BEGIN
    IF position(
        'learning_slot_statistics_v1.source_snapshot_token' IN v_v1
    ) = 0
       OR position(
           'learning_calibration_proposals_v1.source_snapshot_token' IN v_v1
       ) = 0
       OR position(
           'learning_calibration_proposals_v1.source_snapshot_token' IN v_v1_1
       ) = 0 THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_V2_1_PROPAGATION_PATCH_MISSING';
    END IF;
    IF position('LEARNING_FROZEN_SOURCE_PAYLOAD_CONFLICT' IN v_guard) = 0
       OR position('snapshot_status = ''COMPLETE''' IN v_guard) = 0 THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_V2_1_FAIL_CLOSED_GUARD_CHANGED';
    END IF;
END;
$postcondition$;

COMMIT;

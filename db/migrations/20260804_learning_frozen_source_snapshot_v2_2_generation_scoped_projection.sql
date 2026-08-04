-- WALTRADE LEARNING FROZEN SOURCE V2.2
-- Scope mutable Learning projections to the transaction's frozen generation.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $prerequisites$
BEGIN
    IF to_regprocedure(
           'public.refresh_learning_feedback_engine_v1(integer,integer,integer)'
       ) IS NULL
       OR to_regprocedure(
           'public.refresh_learning_feedback_engine_v1_1(integer,integer,integer)'
       ) IS NULL
       OR to_regprocedure(
           'public.refresh_learning_proposal_validation_v1_3(bigint)'
       ) IS NULL
       OR to_regprocedure(
           'public.capture_learning_evidence_manifests_v1(bigint)'
       ) IS NULL
       OR to_regprocedure(
           'public.propagate_learning_source_snapshot_token_v2()'
       ) IS NULL
       OR to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_V2_2_PREREQUISITE_MISSING';
    END IF;
END;
$prerequisites$;

DO $patch_feedback_v1$
DECLARE
    v_signature CONSTANT TEXT :=
        'public.refresh_learning_feedback_engine_v1(integer,integer,integer)';
    v_definition TEXT := pg_get_functiondef(to_regprocedure(v_signature));
    v_old CONSTANT TEXT :=
        '        FROM learning_slot_statistics_v1 s' || E'\n'
        || '        WHERE s.window_days = p_window_days';
    v_new CONSTANT TEXT :=
        '        FROM learning_slot_statistics_v1 s' || E'\n'
        || '        WHERE s.window_days = p_window_days' || E'\n'
        || '          AND s.source_snapshot_token = NULLIF(' || E'\n'
        || '              current_setting(' || E'\n'
        || '                  ''waltrade.learning_source_snapshot_token'', true' || E'\n'
        || '              ), ''''' || E'\n'
        || '          )::UUID';
BEGIN
    IF position(v_new IN v_definition) = 0 THEN
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_V2_2_V1_CANDIDATE_ANCHOR_CONFLICT';
        END IF;
        EXECUTE replace(v_definition, v_old, v_new);
    END IF;
END;
$patch_feedback_v1$;

DO $patch_feedback_v1_1$
DECLARE
    v_signature CONSTANT TEXT :=
        'public.refresh_learning_feedback_engine_v1_1(integer,integer,integer)';
    v_definition TEXT := pg_get_functiondef(to_regprocedure(v_signature));
    v_old TEXT;
    v_new TEXT;
BEGIN
    v_old := '    WHERE s.window_days = p_window_days;' || E'\n\n'
        || '    GET DIAGNOSTICS v_reclassified = ROW_COUNT;';
    v_new := '    WHERE s.window_days = p_window_days' || E'\n'
        || '      AND s.source_snapshot_token = NULLIF(' || E'\n'
        || '          current_setting(' || E'\n'
        || '              ''waltrade.learning_source_snapshot_token'', true' || E'\n'
        || '          ), ''''' || E'\n'
        || '      )::UUID;' || E'\n\n'
        || '    GET DIAGNOSTICS v_reclassified = ROW_COUNT;';
    IF position(v_new IN v_definition) = 0 THEN
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_V2_2_RECLASSIFY_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);
    END IF;

    v_old := '    DELETE FROM learning_calibration_proposals_v1' || E'\n'
        || '    WHERE window_days = p_window_days' || E'\n'
        || '      AND validation_status IN (';
    v_new := '    DELETE FROM learning_calibration_proposals_v1' || E'\n'
        || '    WHERE window_days = p_window_days' || E'\n'
        || '      AND source_snapshot_token = NULLIF(' || E'\n'
        || '          current_setting(' || E'\n'
        || '              ''waltrade.learning_source_snapshot_token'', true' || E'\n'
        || '          ), ''''' || E'\n'
        || '      )::UUID' || E'\n'
        || '      AND validation_status IN (';
    IF position(v_new IN v_definition) = 0 THEN
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_V2_2_DELETE_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);
    END IF;

    v_old := '        FROM learning_slot_statistics_v1 s' || E'\n'
        || '        WHERE s.window_days = p_window_days' || E'\n'
        || '    )' || E'\n'
        || '    INSERT INTO learning_calibration_proposals_v1 (';
    v_new := '        FROM learning_slot_statistics_v1 s' || E'\n'
        || '        WHERE s.window_days = p_window_days' || E'\n'
        || '          AND s.source_snapshot_token = NULLIF(' || E'\n'
        || '              current_setting(' || E'\n'
        || '                  ''waltrade.learning_source_snapshot_token'', true' || E'\n'
        || '              ), ''''' || E'\n'
        || '          )::UUID' || E'\n'
        || '    )' || E'\n'
        || '    INSERT INTO learning_calibration_proposals_v1 (';
    IF position(v_new IN v_definition) = 0 THEN
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_V2_2_V1_1_CANDIDATE_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);
    END IF;

    EXECUTE v_definition;
END;
$patch_feedback_v1_1$;

DO $patch_validation_observations$
DECLARE
    v_signature CONSTANT TEXT :=
        'public.refresh_learning_proposal_validation_v1_3(bigint)';
    v_definition TEXT := pg_get_functiondef(to_regprocedure(v_signature));
    v_old CONSTANT TEXT :=
        '    WHERE p.environment = v_environment' || E'\n'
        || '      AND p.validation_stage = ''SHADOW''' || E'\n'
        || '      AND p.validation_status = ''PENDING''';
    v_new CONSTANT TEXT :=
        '    WHERE p.environment = v_environment' || E'\n'
        || '      AND p.source_snapshot_token = NULLIF(' || E'\n'
        || '          current_setting(' || E'\n'
        || '              ''waltrade.learning_source_snapshot_token'', true' || E'\n'
        || '          ), ''''' || E'\n'
        || '      )::UUID' || E'\n'
        || '      AND p.validation_stage = ''SHADOW''' || E'\n'
        || '      AND p.validation_status = ''PENDING''';
BEGIN
    IF position(v_new IN v_definition) = 0 THEN
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_V2_2_OBSERVATION_ANCHOR_CONFLICT';
        END IF;
        EXECUTE replace(v_definition, v_old, v_new);
    END IF;
END;
$patch_validation_observations$;

DO $patch_manifest_statistics_join$
DECLARE
    v_signature CONSTANT TEXT :=
        'public.capture_learning_evidence_manifests_v1(bigint)';
    v_definition TEXT := pg_get_functiondef(to_regprocedure(v_signature));
    v_old CONSTANT TEXT :=
        '          JOIN learning_slot_statistics_v1 s USING (environment, symbol, interval, strategy, window_days)' || E'\n'
        || '         WHERE o.refresh_run_id = p_feedback_run_id';
    v_new CONSTANT TEXT :=
        '          JOIN learning_slot_statistics_v1 s' || E'\n'
        || '            ON s.environment = o.environment' || E'\n'
        || '           AND s.symbol = o.symbol' || E'\n'
        || '           AND s.interval = o.interval' || E'\n'
        || '           AND s.strategy = o.strategy' || E'\n'
        || '           AND s.window_days = o.window_days' || E'\n'
        || '           AND s.source_snapshot_token = o.source_snapshot_token' || E'\n'
        || '         WHERE o.refresh_run_id = p_feedback_run_id';
BEGIN
    IF position(v_new IN v_definition) = 0 THEN
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_V2_2_MANIFEST_JOIN_ANCHOR_CONFLICT';
        END IF;
        EXECUTE replace(v_definition, v_old, v_new);
    END IF;
END;
$patch_manifest_statistics_join$;

DO $postconditions$
DECLARE
    v_v1 TEXT := pg_get_functiondef(
        'public.refresh_learning_feedback_engine_v1(integer,integer,integer)'
        ::regprocedure
    );
    v_v1_1 TEXT := pg_get_functiondef(
        'public.refresh_learning_feedback_engine_v1_1(integer,integer,integer)'
        ::regprocedure
    );
    v_validation TEXT := pg_get_functiondef(
        'public.refresh_learning_proposal_validation_v1_3(bigint)'
        ::regprocedure
    );
    v_manifest TEXT := pg_get_functiondef(
        'public.capture_learning_evidence_manifests_v1(bigint)'::regprocedure
    );
    v_guard TEXT := pg_get_functiondef(
        'public.propagate_learning_source_snapshot_token_v2()'::regprocedure
    );
BEGIN
    IF position(
           'AND s.source_snapshot_token = NULLIF(' IN v_v1
       ) = 0
       OR position(
           'AND s.source_snapshot_token = NULLIF(' IN v_v1_1
       ) = 0
       OR position(
           'AND source_snapshot_token = NULLIF(' IN v_v1_1
       ) = 0
       OR position(
           'AND p.source_snapshot_token = NULLIF(' IN v_validation
       ) = 0
       OR position(
           'AND s.source_snapshot_token = o.source_snapshot_token'
           IN v_manifest
       ) = 0 THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_V2_2_POSTCONDITION_FAILED';
    END IF;
    IF position('LEARNING_FROZEN_SOURCE_PAYLOAD_CONFLICT' IN v_guard) = 0
       OR position('snapshot_status = ''COMPLETE''' IN v_guard) = 0 THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_V2_2_FAIL_CLOSED_GUARD_CHANGED';
    END IF;
END;
$postconditions$;

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260804_learning_frozen_source_snapshot_v2_2_generation_scoped_projection.sql',
    '2d734f8cbbf320ce7a21672339cff30a743faf50463a6652630b977bf9d877b3',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'LEARNING_FROZEN_SOURCE_V2_2',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    'e1d7eb7dfe7b5c7a22391b05c927317a4bd253cc',
    'LEARNING_FROZEN_SOURCE_V2_2'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id =
          '20260804_learning_frozen_source_snapshot_v2_2_generation_scoped_projection.sql'
);

COMMIT;

BEGIN;

DO $forward_compatible_rollback$
DECLARE
    v_complete INTEGER;
    v_definition TEXT;
BEGIN
    SELECT count(*) INTO v_complete
      FROM learning_canonical_source_snapshots_v2
     WHERE snapshot_status = 'COMPLETE';
    IF v_complete > 0 THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_V2_ROLLBACK_REFUSED complete_snapshots=%; preserve evidence and deploy a forward compatibility patch',
            v_complete;
    END IF;
    IF to_regprocedure(
        'refresh_learning_feedback_v1_2_pre_snapshot_v2(integer,integer,integer,integer,boolean,text)'
    ) IS NULL
       OR to_regprocedure(
        'learning_canonical_evidence_universe_live_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
    ) IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_V2_ROLLBACK_BACKUP_MISSING';
    END IF;

    v_definition := pg_get_functiondef(
        'refresh_learning_feedback_v1_2_pre_snapshot_v2(integer,integer,integer,integer,boolean,text)'
        ::regprocedure
    );
    v_definition := replace(
        v_definition,
        'refresh_learning_feedback_v1_2_pre_snapshot_v2',
        'refresh_learning_feedback_engine_v1_2_if_due'
    );
    EXECUTE v_definition;

    v_definition := pg_get_functiondef(
        'learning_canonical_evidence_universe_live_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
        ::regprocedure
    );
    v_definition := replace(
        v_definition,
        'learning_canonical_evidence_universe_live_v1',
        'learning_canonical_evidence_universe_v1'
    );
    EXECUTE v_definition;
END;
$forward_compatible_rollback$;

DROP TRIGGER IF EXISTS learning_frozen_source_parity_v2
    ON learning_evidence_manifests_v1;
DROP FUNCTION IF EXISTS validate_learning_frozen_source_parity_v2();

DO $drop_token_triggers$
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
    END LOOP;
END;
$drop_token_triggers$;

DROP FUNCTION IF EXISTS propagate_learning_source_snapshot_token_v2();
DROP FUNCTION IF EXISTS capture_learning_canonical_source_snapshot_v2(BIGINT);
DROP FUNCTION IF EXISTS
    refresh_learning_feedback_v1_2_pre_snapshot_v2(
        INTEGER, INTEGER, INTEGER, INTEGER, BOOLEAN, TEXT
    );
DROP FUNCTION IF EXISTS learning_canonical_evidence_universe_live_v1(
    TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ
);
DROP TRIGGER IF EXISTS learning_frozen_snapshot_rows_immutable_v2
    ON learning_canonical_source_snapshot_rows_v2;
DROP TRIGGER IF EXISTS learning_frozen_snapshot_immutable_v2
    ON learning_canonical_source_snapshots_v2;
DROP FUNCTION IF EXISTS prevent_learning_frozen_source_mutation_v2();

ALTER TABLE learning_slot_statistics_v1
    DROP COLUMN IF EXISTS source_snapshot_token;
ALTER TABLE learning_calibration_proposals_v1
    DROP COLUMN IF EXISTS source_snapshot_token;
ALTER TABLE learning_proposal_observations_v1
    DROP COLUMN IF EXISTS source_snapshot_token;
ALTER TABLE learning_canonical_evidence_selection_v1
    DROP COLUMN IF EXISTS source_snapshot_token;
ALTER TABLE learning_evidence_manifests_v1
    DROP COLUMN IF EXISTS source_snapshot_token;

DROP TABLE IF EXISTS learning_canonical_source_snapshot_rows_v2;
DROP TABLE IF EXISTS learning_canonical_source_snapshots_v2;

COMMIT;

BEGIN;

DO $shared_rollback$
DECLARE
    v_database TEXT := current_database();
    v_instance TEXT := current_setting(
        'waltrade.deployment_instance_id', true
    );
    v_environment TEXT := current_setting(
        'waltrade.environment', true
    );
    v_deployment_id TEXT;
    v_feedback_definition TEXT;
    v_has_backup BOOLEAN;
    v_has_shared_objects BOOLEAN;
BEGIN
    IF v_environment NOT IN ('live', 'paper')
       OR v_instance IS NULL
       OR length(v_instance) NOT BETWEEN 1 AND 63
       OR v_instance !~ '^[a-z0-9]+(?:-[a-z0-9]+)*$'
       OR v_instance LIKE '%-live'
       OR v_instance LIKE '%-paper' THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_SHARED_ROLLBACK_INVALID_IDENTITY database=% instance=% environment=%',
            v_database, COALESCE(v_instance, '<missing>'),
            COALESCE(v_environment, '<missing>');
    END IF;

    v_deployment_id := v_instance || '-' || v_environment;
    IF (v_environment = 'live' AND v_database <> 'trading_live')
       OR (v_environment = 'paper' AND v_database <> 'trading_paper') THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_SHARED_ROLLBACK_DATABASE_IDENTITY_MISMATCH database=% deployment_id=%',
            v_database, v_deployment_id;
    END IF;

    v_has_backup := to_regprocedure(
        'learning_feedback_engine_v1_pre_canonical_source_v1(integer,integer,integer)'
    ) IS NOT NULL;
    v_has_shared_objects :=
        to_regclass('public.learning_evidence_manifests_v1') IS NOT NULL
        OR to_regclass('public.learning_evidence_membership_v1') IS NOT NULL
        OR to_regclass('public.learning_evidence_aggregates_v1') IS NOT NULL
        OR to_regclass(
            'public.learning_canonical_evidence_selection_v1'
        ) IS NOT NULL
        OR to_regprocedure(
            'learning_canonical_evidence_universe_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
        ) IS NOT NULL;

    IF NOT v_has_backup THEN
        IF to_regprocedure(
            'refresh_learning_feedback_engine_v1(integer,integer,integer)'
        ) IS NULL THEN
            RAISE EXCEPTION
                'LEARNING_CANONICAL_SHARED_ROLLBACK_FEEDBACK_FUNCTION_MISSING';
        END IF;
        v_feedback_definition := pg_get_functiondef(
            'refresh_learning_feedback_engine_v1(integer,integer,integer)'
            ::regprocedure
        );
        IF v_has_shared_objects
           OR position(
                'learning_canonical_evidence_universe_v1'
                IN v_feedback_definition
           ) > 0 THEN
            RAISE EXCEPTION
                'LEARNING_CANONICAL_SHARED_ROLLBACK_PARTIAL_STATE deployment_id=%',
                v_deployment_id;
        END IF;
        RAISE NOTICE
            'LEARNING_CANONICAL_SHARED_ROLLBACK_ALREADY_APPLIED deployment_id=%',
            v_deployment_id;
        RETURN;
    END IF;

    IF to_regclass(
        'public.learning_shadow_confidence_proposals_v1'
    ) IS NOT NULL THEN
        EXECUTE
            'DROP TRIGGER IF EXISTS trg_learning_shadow_manifest_required_v1 '
            'ON learning_shadow_confidence_proposals_v1';
    END IF;
    IF to_regclass('public.learning_feedback_refresh_runs_v1') IS NOT NULL THEN
        EXECUTE
            'DROP TRIGGER IF EXISTS trg_zz_learning_evidence_manifest_v1 '
            'ON learning_feedback_refresh_runs_v1';
    END IF;

    IF to_regclass('public.learning_evidence_manifests_v1') IS NOT NULL THEN
        EXECUTE
            'DROP TRIGGER IF EXISTS learning_evidence_complete_deferred_v1 '
            'ON learning_evidence_manifests_v1';
        EXECUTE
            'DROP TRIGGER IF EXISTS learning_evidence_manifest_construction_v1 '
            'ON learning_evidence_manifests_v1';
        EXECUTE
            'DROP TRIGGER IF EXISTS learning_evidence_manifest_immutable_v1 '
            'ON learning_evidence_manifests_v1';
    END IF;
    IF to_regclass('public.learning_evidence_membership_v1') IS NOT NULL THEN
        EXECUTE
            'DROP TRIGGER IF EXISTS learning_evidence_membership_immutable_v1 '
            'ON learning_evidence_membership_v1';
        EXECUTE
            'DROP TRIGGER IF EXISTS learning_evidence_membership_same_tx_v1 '
            'ON learning_evidence_membership_v1';
    END IF;
    IF to_regclass('public.learning_evidence_aggregates_v1') IS NOT NULL THEN
        EXECUTE
            'DROP TRIGGER IF EXISTS learning_evidence_aggregate_immutable_v1 '
            'ON learning_evidence_aggregates_v1';
        EXECUTE
            'DROP TRIGGER IF EXISTS learning_evidence_aggregate_same_tx_v1 '
            'ON learning_evidence_aggregates_v1';
    END IF;

    DROP TABLE IF EXISTS learning_evidence_aggregates_v1;
    DROP TABLE IF EXISTS learning_evidence_membership_v1;
    DROP TABLE IF EXISTS learning_evidence_manifests_v1;

    DROP FUNCTION IF EXISTS require_complete_learning_evidence_manifest_v1();
    DROP FUNCTION IF EXISTS trigger_capture_learning_evidence_manifests_v1();
    DROP FUNCTION IF EXISTS capture_learning_evidence_manifests_v1(BIGINT);
    DROP FUNCTION IF EXISTS finalize_learning_evidence_manifest_v1(UUID);
    DROP FUNCTION IF EXISTS validate_complete_learning_evidence_manifest_v1();
    DROP FUNCTION IF EXISTS require_manifest_construction_transaction_v1();
    DROP FUNCTION IF EXISTS prevent_learning_evidence_manifest_mutation_v1();
    DROP FUNCTION IF EXISTS require_manifest_header_construction_v1();
    DROP FUNCTION IF EXISTS learning_evidence_runtime_identity_v1();

    IF to_regclass(
        'public.learning_canonical_evidence_selection_v1'
    ) IS NOT NULL THEN
        EXECUTE
            'DROP TRIGGER IF EXISTS learning_canonical_evidence_immutable_v1 '
            'ON learning_canonical_evidence_selection_v1';
    END IF;
    DROP TABLE IF EXISTS learning_canonical_evidence_selection_v1;
    DROP FUNCTION IF EXISTS prevent_learning_canonical_evidence_mutation_v1();

    v_feedback_definition := pg_get_functiondef(
        'learning_feedback_engine_v1_pre_canonical_source_v1(integer,integer,integer)'
        ::regprocedure
    );
    v_feedback_definition := replace(
        v_feedback_definition,
        'learning_feedback_engine_v1_pre_canonical_source_v1',
        'refresh_learning_feedback_engine_v1'
    );
    EXECUTE v_feedback_definition;

    DROP FUNCTION
        learning_feedback_engine_v1_pre_canonical_source_v1(
            INTEGER, INTEGER, INTEGER
        );
    DROP FUNCTION IF EXISTS learning_canonical_evidence_universe_v1(
        TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ
    );

    RAISE NOTICE
        'LEARNING_CANONICAL_SHARED_ROLLBACK_APPLIED deployment_id=% database=%',
        v_deployment_id, v_database;
END;
$shared_rollback$;

COMMIT;

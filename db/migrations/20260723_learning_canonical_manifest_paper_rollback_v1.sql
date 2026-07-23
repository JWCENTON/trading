BEGIN;

DO $prerequisites$
BEGIN
    IF current_database() IS DISTINCT FROM 'trading_paper'
       OR current_setting(
            'waltrade.deployment_instance_id', true
          ) IS DISTINCT FROM 'local'
       OR current_setting(
            'waltrade.environment', true
          ) IS DISTINCT FROM 'paper' THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_PAPER_ROLLBACK_RUNTIME_IDENTITY_MISMATCH';
    END IF;
    IF to_regprocedure(
        'learning_feedback_engine_v1_pre_canonical_source_v1(integer,integer,integer)'
    ) IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_PAPER_ROLLBACK_PREREQUISITE_MISSING: preserved feedback function';
    END IF;
END;
$prerequisites$;

DROP TRIGGER IF EXISTS trg_learning_shadow_manifest_required_v1
    ON learning_shadow_confidence_proposals_v1;
DROP TRIGGER IF EXISTS trg_zz_learning_evidence_manifest_v1
    ON learning_feedback_refresh_runs_v1;

DROP TRIGGER IF EXISTS learning_evidence_complete_deferred_v1
    ON learning_evidence_manifests_v1;
DROP TRIGGER IF EXISTS learning_evidence_manifest_construction_v1
    ON learning_evidence_manifests_v1;
DROP TRIGGER IF EXISTS learning_evidence_manifest_immutable_v1
    ON learning_evidence_manifests_v1;
DROP TRIGGER IF EXISTS learning_evidence_membership_immutable_v1
    ON learning_evidence_membership_v1;
DROP TRIGGER IF EXISTS learning_evidence_membership_same_tx_v1
    ON learning_evidence_membership_v1;
DROP TRIGGER IF EXISTS learning_evidence_aggregate_immutable_v1
    ON learning_evidence_aggregates_v1;
DROP TRIGGER IF EXISTS learning_evidence_aggregate_same_tx_v1
    ON learning_evidence_aggregates_v1;

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

DROP TRIGGER IF EXISTS learning_canonical_evidence_immutable_v1
    ON learning_canonical_evidence_selection_v1;
DROP TABLE IF EXISTS learning_canonical_evidence_selection_v1;
DROP FUNCTION IF EXISTS prevent_learning_canonical_evidence_mutation_v1();

DO $restore_feedback$
DECLARE
    v_definition TEXT;
BEGIN
    v_definition := pg_get_functiondef(
        'learning_feedback_engine_v1_pre_canonical_source_v1(integer,integer,integer)'
        ::regprocedure);
    v_definition := replace(
        v_definition,
        'learning_feedback_engine_v1_pre_canonical_source_v1',
        'refresh_learning_feedback_engine_v1'
    );
    EXECUTE v_definition;
END;
$restore_feedback$;

DROP FUNCTION
    learning_feedback_engine_v1_pre_canonical_source_v1(
        INTEGER, INTEGER, INTEGER
    );
DROP FUNCTION learning_canonical_evidence_universe_v1(
    TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ
);

COMMIT;

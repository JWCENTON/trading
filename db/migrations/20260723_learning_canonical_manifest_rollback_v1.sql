BEGIN;

DO $prerequisites$
BEGIN
    IF current_database() IS DISTINCT FROM 'trading_live'
       OR current_setting(
            'waltrade.deployment_instance_id', true
          ) IS DISTINCT FROM 'local'
       OR current_setting(
            'waltrade.environment', true
          ) IS DISTINCT FROM 'live' THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_ROLLBACK_RUNTIME_IDENTITY_MISMATCH';
    END IF;
    IF to_regprocedure(
        'learning_feedback_engine_v1_pre_canonical_source_v1(integer,integer,integer)'
    ) IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_ROLLBACK_PREREQUISITE_MISSING: preserved feedback function';
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

DO $exact_repair_rollback$
DECLARE
    v_repair_id CONSTANT UUID :=
        '72e73dc9-8b2d-572f-bef9-1fc18a877adf';
    v_decision_key CONSTANT TEXT :=
        '98b4eb54128ca4800d8cc91499026e7f';
    v_decision_id CONSTANT UUID :=
        '2cf22538-41ff-5be3-ab51-40cbb9f468e1';
    v_outcome_id CONSTANT UUID :=
        '46821b51-7075-593b-8166-3d39f923e391';
    v_idempotency_status TEXT;
BEGIN
    IF to_regclass('public.learning_decision_identity_repairs_v1') IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_ROLLBACK_EXACT_REPAIR_AUDIT_MISSING';
    END IF;

    SELECT idempotency_status
      INTO STRICT v_idempotency_status
      FROM learning_decision_identity_repairs_v1
     WHERE repair_id = v_repair_id
       AND decision_key = v_decision_key
       AND decision_id = v_decision_id
       AND outcome_id = v_outcome_id
       AND position_id = 3078
       AND deployment_instance_id = 'local'
       AND runtime_environment = 'live'
       AND runtime_deployment_id = 'local-live';

    IF v_idempotency_status NOT IN ('INSERTED', 'EXISTING_IDENTICAL') THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_ROLLBACK_INVALID_REPAIR_STATUS';
    END IF;

    IF v_idempotency_status = 'INSERTED' THEN
        DELETE FROM decision_outcomes_v1
         WHERE outcome_id = v_outcome_id
           AND decision_id = v_decision_id
           AND position_id = 3078
           AND deployment_id = 'LOCAL'
           AND environment = 'trading_live';
        IF NOT FOUND THEN
            RAISE EXCEPTION
                'LEARNING_CANONICAL_ROLLBACK_EXACT_OUTCOME_MISSING';
        END IF;

        DELETE FROM decision_registry_v1
         WHERE decision_id = v_decision_id
           AND legacy_decision_key = v_decision_key
           AND position_id = 3078
           AND source_natural_key =
                'LOCAL|trading_live|positions|3078|TRADE_EXECUTED'
           AND deployment_id = 'LOCAL'
           AND environment = 'trading_live';
        IF NOT FOUND THEN
            RAISE EXCEPTION
                'LEARNING_CANONICAL_ROLLBACK_EXACT_REGISTRY_MISSING';
        END IF;
    END IF;

    DROP TRIGGER IF EXISTS learning_decision_identity_repairs_immutable_v1
        ON learning_decision_identity_repairs_v1;
    DELETE FROM learning_decision_identity_repairs_v1
     WHERE repair_id = v_repair_id;
END;
$exact_repair_rollback$;

DROP TABLE IF EXISTS learning_decision_identity_repairs_v1;
DROP FUNCTION IF EXISTS
    prevent_learning_decision_identity_repair_mutation_v1();

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

BEGIN;

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
    IF current_database() <> 'trading_live'
       OR current_setting(
            'waltrade.deployment_instance_id', true
          ) IS DISTINCT FROM 'local'
       OR current_setting(
            'waltrade.environment', true
          ) IS DISTINCT FROM 'live' THEN
        RAISE EXCEPTION
            'LEARNING_98B4_ROLLBACK_RUNTIME_IDENTITY_MISMATCH';
    END IF;

    IF to_regclass(
        'public.learning_decision_identity_repairs_v1'
    ) IS NULL THEN
        IF EXISTS (
            SELECT 1 FROM decision_registry_v1
             WHERE decision_id = v_decision_id
                OR legacy_decision_key = v_decision_key
        ) OR EXISTS (
            SELECT 1 FROM decision_outcomes_v1
             WHERE outcome_id = v_outcome_id
                OR decision_id = v_decision_id
        ) THEN
            RAISE EXCEPTION
                'LEARNING_98B4_ROLLBACK_AUDIT_MISSING_WITH_REPAIR_ROWS';
        END IF;
        RAISE NOTICE 'LEARNING_98B4_ROLLBACK_ALREADY_APPLIED';
        RETURN;
    END IF;

    SELECT idempotency_status
      INTO v_idempotency_status
      FROM learning_decision_identity_repairs_v1
     WHERE repair_id = v_repair_id
       AND decision_key = v_decision_key
       AND decision_id = v_decision_id
       AND outcome_id = v_outcome_id
       AND position_id = 3078
       AND deployment_instance_id = 'local'
       AND runtime_environment = 'live'
       AND runtime_deployment_id = 'local-live';

    IF NOT FOUND THEN
        RAISE NOTICE 'LEARNING_98B4_ROLLBACK_ALREADY_APPLIED';
        RETURN;
    END IF;
    IF v_idempotency_status NOT IN ('INSERTED', 'EXISTING_IDENTICAL') THEN
        RAISE EXCEPTION 'LEARNING_98B4_ROLLBACK_INVALID_REPAIR_STATUS';
    END IF;

    IF v_idempotency_status = 'INSERTED' THEN
        DELETE FROM decision_outcomes_v1
         WHERE outcome_id = v_outcome_id
           AND decision_id = v_decision_id
           AND position_id = 3078
           AND deployment_id = 'LOCAL'
           AND environment = 'trading_live';
        IF NOT FOUND THEN
            RAISE EXCEPTION 'LEARNING_98B4_ROLLBACK_EXACT_OUTCOME_MISSING';
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
            RAISE EXCEPTION 'LEARNING_98B4_ROLLBACK_EXACT_REGISTRY_MISSING';
        END IF;
    END IF;

    DROP TRIGGER IF EXISTS learning_decision_identity_repairs_immutable_v1
        ON learning_decision_identity_repairs_v1;
    DELETE FROM learning_decision_identity_repairs_v1
     WHERE repair_id = v_repair_id;
    CREATE TRIGGER learning_decision_identity_repairs_immutable_v1
    BEFORE UPDATE OR DELETE ON learning_decision_identity_repairs_v1
    FOR EACH ROW
    EXECUTE FUNCTION prevent_learning_decision_identity_repair_mutation_v1();
END;
$exact_repair_rollback$;

COMMIT;

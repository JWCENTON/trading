BEGIN;

-- Causal Learning V1.3
--
-- Position-linked replay and warehouse artifacts may be created after the
-- forward decision registry has already received canonical causal attribution.
-- Reconcile only already-attributed forward registry decisions into those
-- downstream artifacts after the normal shadow-learning refresh completes.

CREATE OR REPLACE FUNCTION public.reconcile_forward_causal_artifacts_v1_3(
    p_since TIMESTAMPTZ
)
RETURNS JSONB
LANGUAGE plpgsql
AS $function$
DECLARE
    v_registry RECORD;
    v_warehouse_id BIGINT;
    v_replay_updates INTEGER := 0;
    v_warehouse_updates INTEGER := 0;
    v_rows INTEGER;
BEGIN
    IF p_since IS NULL THEN
        RAISE EXCEPTION 'CAUSAL_RECONCILIATION_SINCE_REQUIRED';
    END IF;

    FOR v_registry IN
        SELECT
            d.decision_id,
            d.legacy_decision_key,
            d.environment,
            d.position_id,
            d.recommendation_id,
            d.recommendation_version,
            d.activation_id,
            d.experiment_id,
            d.experiment_arm,
            d.baseline_policy_version,
            d.candidate_policy_version,
            d.causal_linkage_status,
            d.decision_payload->>'runtime_deployment_id'
                AS runtime_deployment_id
        FROM public.decision_registry_v1 d
        WHERE d.decision_source =
              'FINAL_DECISION_EXECUTION_EPILOG'
          AND d.engine_version =
              'FORWARD_DECISION_REGISTRY_CONTINUITY_V1'
          AND d.position_id IS NOT NULL
          AND d.recommendation_id IS NOT NULL
          AND d.activation_id IS NOT NULL
          AND d.causal_linkage_status LIKE 'ATTRIBUTED_%'
          AND d.decision_timestamp >= p_since
        ORDER BY d.decision_timestamp, d.decision_id
    LOOP
        UPDATE public.decision_replay_v1
           SET recommendation_id =
                   v_registry.recommendation_id,
               recommendation_version =
                   v_registry.recommendation_version,
               activation_id =
                   v_registry.activation_id,
               experiment_id =
                   v_registry.experiment_id,
               experiment_arm =
                   v_registry.experiment_arm,
               baseline_policy_version =
                   v_registry.baseline_policy_version,
               candidate_policy_version =
                   v_registry.candidate_policy_version,
               causal_linkage_status =
                   v_registry.causal_linkage_status,
               observation_decision_key =
                   v_registry.legacy_decision_key
         WHERE environment = v_registry.environment
           AND position_id = v_registry.position_id
           AND causal_linkage_status =
               'LEGACY_NOT_ATTRIBUTABLE'
           AND (
               deployment_id = 'legacy-unknown'
               OR deployment_id =
                  v_registry.runtime_deployment_id
           );

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_replay_updates := v_replay_updates + v_rows;

        SELECT id
          INTO v_warehouse_id
          FROM public.learning_feature_warehouse_v1
         WHERE environment = v_registry.environment
           AND position_id = v_registry.position_id
           AND causal_linkage_status =
               'LEGACY_NOT_ATTRIBUTABLE'
           AND (
               deployment_id = 'legacy-unknown'
               OR deployment_id =
                  v_registry.runtime_deployment_id
           )
         ORDER BY
             (
                 exit_time IS NOT NULL
                 AND net_pnl_usdc IS NOT NULL
             ) DESC,
             id
         LIMIT 1;

        IF v_warehouse_id IS NOT NULL THEN
            UPDATE public.learning_feature_warehouse_v1
               SET recommendation_id =
                       v_registry.recommendation_id,
                   recommendation_version =
                       v_registry.recommendation_version,
                   activation_id =
                       v_registry.activation_id,
                   experiment_id =
                       v_registry.experiment_id,
                   experiment_arm =
                       v_registry.experiment_arm,
                   baseline_policy_version =
                       v_registry.baseline_policy_version,
                   candidate_policy_version =
                       v_registry.candidate_policy_version,
                   causal_linkage_status =
                       v_registry.causal_linkage_status,
                   observation_decision_key =
                       v_registry.legacy_decision_key
             WHERE id = v_warehouse_id;

            GET DIAGNOSTICS v_rows = ROW_COUNT;
            v_warehouse_updates :=
                v_warehouse_updates + v_rows;
        END IF;
    END LOOP;

    RETURN jsonb_build_object(
        'status', 'ok',
        'since', p_since,
        'replay_updates', v_replay_updates,
        'warehouse_updates', v_warehouse_updates
    );
END;
$function$;

COMMIT;

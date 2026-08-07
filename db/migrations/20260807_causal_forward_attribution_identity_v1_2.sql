BEGIN;

-- Causal Learning V1.2
--
-- decision_registry_v1 intentionally retains legacy deployment provenance
-- (LOCAL/VPS), while forward runtime identity is canonical
-- (local-paper/local-live/vps-paper/vps-live) in decision_payload.
--
-- Historical causal attribution is not rewritten.

CREATE OR REPLACE FUNCTION public.attribute_decision_causally_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    v_match RECORD;
    v_slot_key TEXT;
    v_activation_deployment_id TEXT;
BEGIN
    IF NEW.causal_attributed_at IS NOT NULL THEN
        RETURN NEW;
    END IF;

    v_slot_key := upper(concat_ws(
        '|',
        NEW.environment,
        NEW.strategy,
        NEW.symbol,
        NEW.interval,
        COALESCE(NEW.market_regime, '*')
    ));

    IF NEW.decision_source = 'FINAL_DECISION_EXECUTION_EPILOG'
       AND NEW.engine_version = 'FORWARD_DECISION_REGISTRY_CONTINUITY_V1'
    THEN
        v_activation_deployment_id :=
            NULLIF(
                btrim(NEW.decision_payload->>'runtime_deployment_id'),
                ''
            );
    ELSE
        v_activation_deployment_id := NEW.deployment_id;
    END IF;

    SELECT
        a.activation_id,
        a.experiment_id,
        a.experiment_arm,
        a.baseline_policy_version,
        a.candidate_policy_version,
        a.promotion_event_id,
        a.promotion_candidate_id,
        a.promotion_payload_hash,
        a.promotion_policy_version,
        r.recommendation_id,
        r.recommendation_version,
        a.apply_mode
    INTO v_match
    FROM public.learning_recommendation_activations_v1 a
    JOIN public.learning_recommendation_snapshots_v1 r
      ON r.deployment_id = a.deployment_id
     AND r.recommendation_id = a.recommendation_id
    WHERE a.deployment_id = v_activation_deployment_id
      AND a.environment = NEW.environment
      AND a.slot_key = v_slot_key
      AND a.effective_from <= NEW.decision_timestamp
      AND a.expires_at > NEW.decision_timestamp
      AND (
          a.deactivated_at IS NULL
          OR a.deactivated_at > NEW.decision_timestamp
      )
      AND r.status IN ('FROZEN', 'ACTIVE')
      AND r.evidence_cutoff_at < NEW.decision_timestamp
      AND r.reset_at IS NULL
    ORDER BY a.effective_from DESC, a.created_at DESC
    LIMIT 1;

    IF FOUND THEN
        NEW.recommendation_id := v_match.recommendation_id;
        NEW.recommendation_version := v_match.recommendation_version;
        NEW.activation_id := v_match.activation_id;
        NEW.experiment_id := v_match.experiment_id;
        NEW.experiment_arm := v_match.experiment_arm;
        NEW.baseline_policy_version := v_match.baseline_policy_version;
        NEW.candidate_policy_version := v_match.candidate_policy_version;
        NEW.promotion_event_id := v_match.promotion_event_id;
        NEW.promotion_candidate_id := v_match.promotion_candidate_id;
        NEW.consumed_promotion_hash := NULL;
        NEW.consumed_promotion_version := NULL;
        NEW.causal_linkage_status :=
            CASE
                WHEN v_match.apply_mode = 'SHADOW_OBSERVATION'
                    THEN 'ATTRIBUTED_SHADOW_OBSERVATION'
                ELSE 'ATTRIBUTED_EXPERIMENT'
            END;
    ELSE
        NEW.causal_linkage_status := 'NO_ACTIVE_RECOMMENDATION';
        NEW.experiment_arm := 'BASELINE';
    END IF;

    NEW.causal_attributed_at := clock_timestamp();
    RETURN NEW;
END;
$function$;


CREATE OR REPLACE FUNCTION public.propagate_decision_causal_linkage_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    v_key TEXT :=
        COALESCE(NEW.legacy_decision_key, NEW.decision_id::text);
    v_runtime_deployment_id TEXT;
    v_warehouse_id BIGINT;
BEGIN
    IF NEW.position_id IS NULL THEN
        RETURN NEW;
    END IF;

    v_runtime_deployment_id :=
        NULLIF(
            btrim(NEW.decision_payload->>'runtime_deployment_id'),
            ''
        );

    UPDATE public.decision_replay_v1
       SET recommendation_id = NEW.recommendation_id,
           recommendation_version = NEW.recommendation_version,
           activation_id = NEW.activation_id,
           experiment_id = NEW.experiment_id,
           experiment_arm = NEW.experiment_arm,
           baseline_policy_version = NEW.baseline_policy_version,
           candidate_policy_version = NEW.candidate_policy_version,
           causal_linkage_status = NEW.causal_linkage_status,
           observation_decision_key = v_key
     WHERE environment = NEW.environment
       AND position_id = NEW.position_id
       AND causal_linkage_status = 'LEGACY_NOT_ATTRIBUTABLE'
       AND (
           deployment_id = 'legacy-unknown'
           OR deployment_id = v_runtime_deployment_id
       );

    SELECT id
      INTO v_warehouse_id
      FROM public.learning_feature_warehouse_v1
     WHERE environment = NEW.environment
       AND position_id = NEW.position_id
       AND causal_linkage_status = 'LEGACY_NOT_ATTRIBUTABLE'
       AND (
           deployment_id = 'legacy-unknown'
           OR deployment_id = v_runtime_deployment_id
       )
     ORDER BY
         (exit_time IS NOT NULL AND net_pnl_usdc IS NOT NULL) DESC,
         id
     LIMIT 1;

    IF v_warehouse_id IS NOT NULL THEN
        UPDATE public.learning_feature_warehouse_v1
           SET recommendation_id = NEW.recommendation_id,
               recommendation_version = NEW.recommendation_version,
               activation_id = NEW.activation_id,
               experiment_id = NEW.experiment_id,
               experiment_arm = NEW.experiment_arm,
               baseline_policy_version = NEW.baseline_policy_version,
               candidate_policy_version = NEW.candidate_policy_version,
               causal_linkage_status = NEW.causal_linkage_status,
               observation_decision_key = v_key
         WHERE id = v_warehouse_id;
    END IF;

    RETURN NEW;
END;
$function$;


-- The forward registry row is inserted before the simulated entry fill exists.
-- position_id is therefore NULL during the original AFTER INSERT propagation.
-- Retry propagation when the deterministic position linkage is established.

DROP TRIGGER IF EXISTS
    decision_registry_causal_position_propagation_v1_2
ON public.decision_registry_v1;

CREATE TRIGGER decision_registry_causal_position_propagation_v1_2
AFTER UPDATE OF position_id
ON public.decision_registry_v1
FOR EACH ROW
WHEN (
    OLD.position_id IS NULL
    AND NEW.position_id IS NOT NULL
)
EXECUTE FUNCTION public.propagate_decision_causal_linkage_v1();

COMMIT;

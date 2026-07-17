BEGIN;

ALTER TABLE learning_recommendation_snapshots_v1 ADD COLUMN IF NOT EXISTS deployment_id TEXT;
ALTER TABLE learning_recommendation_activations_v1 ADD COLUMN IF NOT EXISTS deployment_id TEXT;
ALTER TABLE learning_would_trade_decisions_v1 ADD COLUMN IF NOT EXISTS deployment_id TEXT;
ALTER TABLE learning_counterfactual_outcomes_v1 ADD COLUMN IF NOT EXISTS deployment_id TEXT;
ALTER TABLE decision_replay_v1 ADD COLUMN IF NOT EXISTS deployment_id TEXT;
ALTER TABLE learning_feature_warehouse_v1 ADD COLUMN IF NOT EXISTS deployment_id TEXT;

UPDATE learning_recommendation_snapshots_v1 SET deployment_id='legacy-unknown' WHERE deployment_id IS NULL;
UPDATE learning_recommendation_activations_v1 SET deployment_id='legacy-unknown' WHERE deployment_id IS NULL;
UPDATE learning_would_trade_decisions_v1 SET deployment_id='legacy-unknown' WHERE deployment_id IS NULL;
UPDATE learning_counterfactual_outcomes_v1 SET deployment_id='legacy-unknown' WHERE deployment_id IS NULL;
UPDATE decision_replay_v1 SET deployment_id='legacy-unknown' WHERE deployment_id IS NULL;
UPDATE learning_feature_warehouse_v1 SET deployment_id='legacy-unknown' WHERE deployment_id IS NULL;

ALTER TABLE learning_recommendation_snapshots_v1 ALTER COLUMN deployment_id SET NOT NULL;
ALTER TABLE learning_recommendation_activations_v1 ALTER COLUMN deployment_id SET NOT NULL;
ALTER TABLE learning_would_trade_decisions_v1 ALTER COLUMN deployment_id SET NOT NULL;
ALTER TABLE learning_counterfactual_outcomes_v1 ALTER COLUMN deployment_id SET NOT NULL;
ALTER TABLE decision_replay_v1 ALTER COLUMN deployment_id SET NOT NULL;
ALTER TABLE learning_feature_warehouse_v1 ALTER COLUMN deployment_id SET NOT NULL;
-- Legacy refresh functions remain operational until the separately reviewed
-- adapter wiring. Their rows stay visibly unclassified rather than guessed.
ALTER TABLE decision_replay_v1 ALTER COLUMN deployment_id SET DEFAULT 'legacy-unknown';
ALTER TABLE learning_feature_warehouse_v1 ALTER COLUMN deployment_id SET DEFAULT 'legacy-unknown';

DROP INDEX IF EXISTS ux_learning_recommendation_identity_v1;
CREATE UNIQUE INDEX IF NOT EXISTS ux_learning_recommendation_identity_v1_1
ON learning_recommendation_snapshots_v1 (
 deployment_id, environment, strategy, symbol, interval,
 COALESCE(market_regime,''), recommendation_version
);
DROP INDEX IF EXISTS ix_learning_activation_lookup_v1;
CREATE INDEX IF NOT EXISTS ix_learning_activation_lookup_v1_1
ON learning_recommendation_activations_v1
(deployment_id, environment, slot_key, effective_from, expires_at);
CREATE UNIQUE INDEX IF NOT EXISTS ux_learning_activation_deployment_v1_1
ON learning_recommendation_activations_v1(deployment_id, activation_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_would_trade_deployment_decision_v1_1
ON learning_would_trade_decisions_v1(deployment_id, decision_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_counterfactual_deployment_decision_v1_1
ON learning_counterfactual_outcomes_v1(deployment_id, decision_key);

ALTER TABLE learning_would_trade_decisions_v1
 ADD COLUMN IF NOT EXISTS actual_action TEXT,
 ADD COLUMN IF NOT EXISTS actual_execution_eligible BOOLEAN NOT NULL DEFAULT false,
 ADD COLUMN IF NOT EXISTS recommended_shadow_action TEXT,
 ADD COLUMN IF NOT EXISTS would_trade_without_recommendation BOOLEAN,
 ADD COLUMN IF NOT EXISTS would_trade_with_recommendation BOOLEAN,
 ADD COLUMN IF NOT EXISTS recommendation_effect_applied BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE learning_would_trade_decisions_v1 DROP CONSTRAINT IF EXISTS ck_would_trade_effect_not_applied_v1_1;
ALTER TABLE learning_would_trade_decisions_v1 ADD CONSTRAINT ck_would_trade_effect_not_applied_v1_1
 CHECK (recommendation_effect_applied IS FALSE);

ALTER TABLE learning_counterfactual_outcomes_v1
 ADD COLUMN IF NOT EXISTS outcome_status TEXT NOT NULL DEFAULT 'PENDING_OUTCOME',
 ADD COLUMN IF NOT EXISTS evaluation_type TEXT NOT NULL DEFAULT 'DIRECTIONAL_ONLY';
ALTER TABLE learning_counterfactual_outcomes_v1 DROP CONSTRAINT IF EXISTS ck_counterfactual_outcome_status_v1_1;
ALTER TABLE learning_counterfactual_outcomes_v1 ADD CONSTRAINT ck_counterfactual_outcome_status_v1_1 CHECK
 (outcome_status IN ('PENDING_OUTCOME','BENEFICIAL_DIRECTIONAL','HARMFUL_DIRECTIONAL','NEUTRAL_DIRECTIONAL','NOT_EVALUABLE'));
ALTER TABLE learning_counterfactual_outcomes_v1 DROP CONSTRAINT IF EXISTS ck_counterfactual_evaluation_type_v1_1;
ALTER TABLE learning_counterfactual_outcomes_v1 ADD CONSTRAINT ck_counterfactual_evaluation_type_v1_1
 CHECK (evaluation_type='DIRECTIONAL_ONLY');

ALTER TABLE decision_registry_v1 DROP CONSTRAINT IF EXISTS ck_decision_registry_causal_status_v1;
ALTER TABLE decision_registry_v1 DROP CONSTRAINT IF EXISTS ck_decision_registry_causal_status_v1_1;
ALTER TABLE decision_registry_v1 DISABLE TRIGGER decision_registry_causal_immutable_v1;
UPDATE decision_registry_v1 SET causal_linkage_status='ATTRIBUTED_SHADOW_OBSERVATION'
 WHERE causal_linkage_status='ATTRIBUTED';
ALTER TABLE decision_registry_v1 ENABLE TRIGGER decision_registry_causal_immutable_v1;
ALTER TABLE decision_registry_v1 ADD CONSTRAINT ck_decision_registry_causal_status_v1_1 CHECK
 (causal_linkage_status IN ('LEGACY_NOT_ATTRIBUTABLE','NO_ACTIVE_RECOMMENDATION','NOT_ELIGIBLE',
  'ATTRIBUTED_SHADOW_OBSERVATION','ATTRIBUTED_EXPERIMENT','EXPIRED','RESET'));

ALTER TABLE decision_replay_v1
 ADD COLUMN IF NOT EXISTS observation_decision_key TEXT,
 ADD COLUMN IF NOT EXISTS activation_mode TEXT,
 ADD COLUMN IF NOT EXISTS promotion_consumption_event_id UUID;
ALTER TABLE learning_feature_warehouse_v1
 ADD COLUMN IF NOT EXISTS observation_decision_key TEXT,
 ADD COLUMN IF NOT EXISTS activation_mode TEXT,
 ADD COLUMN IF NOT EXISTS promotion_consumption_event_id UUID;
CREATE UNIQUE INDEX IF NOT EXISTS ux_decision_replay_causal_observation_v1_1
 ON decision_replay_v1(deployment_id, observation_decision_key)
 WHERE observation_decision_key IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_warehouse_causal_observation_v1_1
 ON learning_feature_warehouse_v1(deployment_id, observation_decision_key)
 WHERE observation_decision_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS causal_decision_observation_v1 (
 event_id UUID PRIMARY KEY,
 decision_key TEXT NOT NULL,
 decision_created_at TIMESTAMPTZ NOT NULL,
 environment TEXT NOT NULL,
 deployment_id TEXT NOT NULL,
 strategy TEXT NOT NULL, symbol TEXT NOT NULL, interval TEXT NOT NULL, slot_key TEXT NOT NULL,
 regime TEXT, regime_confidence NUMERIC, action TEXT NOT NULL, direction TEXT, confidence NUMERIC,
 quantity_intent NUMERIC, entry_intent NUMERIC, stop_loss_intent NUMERIC,
 take_profit_intent NUMERIC, exit_intent TEXT, execution_eligible BOOLEAN NOT NULL,
 decision_reason TEXT NOT NULL, decision_payload_hash TEXT NOT NULL,
 semantic_digest TEXT NOT NULL, event_digest TEXT NOT NULL,
 source_service TEXT NOT NULL, source_instance TEXT NOT NULL,
 decision_kind TEXT NOT NULL CHECK (decision_kind IN ('TRADE','NO_TRADE','BLOCKED_BY_EXISTING_LOGIC','EXIT','HOLD')),
 schema_version TEXT NOT NULL DEFAULT 'CAUSAL_DECISION_OBSERVATION_V1',
 created_at TIMESTAMPTZ NOT NULL, inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE (deployment_id, decision_key)
);

CREATE TABLE IF NOT EXISTS causal_promotion_consumption_v1 (
 promotion_consumption_event_id UUID PRIMARY KEY,
 deployment_id TEXT NOT NULL,
 decision_key TEXT NOT NULL,
 promotion_event_id BIGINT NOT NULL,
 promotion_hash TEXT NOT NULL,
 promotion_version TEXT NOT NULL,
 consumer TEXT NOT NULL,
 consumed_at TIMESTAMPTZ NOT NULL,
 activation_id UUID NOT NULL,
 recommendation_id TEXT NOT NULL,
 UNIQUE(deployment_id, decision_key, promotion_event_id, consumer)
);

CREATE OR REPLACE FUNCTION prevent_causal_v1_1_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'causal V1.1 evidence is append-only'; END; $$;
DROP TRIGGER IF EXISTS causal_decision_observation_immutable_v1 ON causal_decision_observation_v1;
CREATE TRIGGER causal_decision_observation_immutable_v1 BEFORE UPDATE OR DELETE ON causal_decision_observation_v1
 FOR EACH ROW EXECUTE FUNCTION prevent_causal_v1_1_mutation();
DROP TRIGGER IF EXISTS causal_promotion_consumption_immutable_v1 ON causal_promotion_consumption_v1;
CREATE TRIGGER causal_promotion_consumption_immutable_v1 BEFORE UPDATE OR DELETE ON causal_promotion_consumption_v1
 FOR EACH ROW EXECUTE FUNCTION prevent_causal_v1_1_mutation();

CREATE OR REPLACE FUNCTION attribute_decision_causally_v1() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v_match RECORD; v_slot_key TEXT;
BEGIN
 IF NEW.causal_attributed_at IS NOT NULL THEN RETURN NEW; END IF;
 v_slot_key:=upper(concat_ws('|',NEW.environment,NEW.strategy,NEW.symbol,NEW.interval,COALESCE(NEW.market_regime,'*')));
 SELECT a.activation_id,a.experiment_id,a.experiment_arm,a.baseline_policy_version,a.candidate_policy_version,
  a.promotion_event_id,a.promotion_candidate_id,a.promotion_payload_hash,a.promotion_policy_version,
  r.recommendation_id,r.recommendation_version,a.apply_mode INTO v_match
 FROM learning_recommendation_activations_v1 a JOIN learning_recommendation_snapshots_v1 r
  ON r.deployment_id=a.deployment_id AND r.recommendation_id=a.recommendation_id
 WHERE a.deployment_id=NEW.deployment_id AND a.environment=NEW.environment AND a.slot_key=v_slot_key
  AND a.effective_from<=NEW.decision_timestamp AND a.expires_at>NEW.decision_timestamp
  AND (a.deactivated_at IS NULL OR a.deactivated_at>NEW.decision_timestamp)
  AND r.status IN ('FROZEN','ACTIVE') AND r.evidence_cutoff_at<NEW.decision_timestamp AND r.reset_at IS NULL
 ORDER BY a.effective_from DESC,a.created_at DESC LIMIT 1;
 IF FOUND THEN
  NEW.recommendation_id:=v_match.recommendation_id; NEW.recommendation_version:=v_match.recommendation_version;
  NEW.activation_id:=v_match.activation_id; NEW.experiment_id:=v_match.experiment_id; NEW.experiment_arm:=v_match.experiment_arm;
  NEW.baseline_policy_version:=v_match.baseline_policy_version; NEW.candidate_policy_version:=v_match.candidate_policy_version;
  NEW.promotion_event_id:=v_match.promotion_event_id; NEW.promotion_candidate_id:=v_match.promotion_candidate_id;
  NEW.consumed_promotion_hash:=NULL; NEW.consumed_promotion_version:=NULL;
  NEW.causal_linkage_status:=CASE WHEN v_match.apply_mode='SHADOW_OBSERVATION'
   THEN 'ATTRIBUTED_SHADOW_OBSERVATION' ELSE 'ATTRIBUTED_EXPERIMENT' END;
 ELSE NEW.causal_linkage_status:='NO_ACTIVE_RECOMMENDATION'; NEW.experiment_arm:='BASELINE'; END IF;
 NEW.causal_attributed_at:=clock_timestamp(); RETURN NEW;
END; $$;

CREATE OR REPLACE FUNCTION propagate_decision_causal_linkage_v1() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v_key TEXT:=COALESCE(NEW.legacy_decision_key,NEW.decision_id::text);
BEGIN
 IF NEW.position_id IS NULL THEN RETURN NEW; END IF;
 UPDATE decision_replay_v1 SET recommendation_id=NEW.recommendation_id,recommendation_version=NEW.recommendation_version,
  activation_id=NEW.activation_id,experiment_id=NEW.experiment_id,experiment_arm=NEW.experiment_arm,
  baseline_policy_version=NEW.baseline_policy_version,candidate_policy_version=NEW.candidate_policy_version,
  causal_linkage_status=NEW.causal_linkage_status,observation_decision_key=v_key
 WHERE deployment_id=NEW.deployment_id AND environment=NEW.environment AND position_id=NEW.position_id
  AND causal_linkage_status='LEGACY_NOT_ATTRIBUTABLE';
 UPDATE learning_feature_warehouse_v1 SET recommendation_id=NEW.recommendation_id,recommendation_version=NEW.recommendation_version,
  activation_id=NEW.activation_id,experiment_id=NEW.experiment_id,experiment_arm=NEW.experiment_arm,
  baseline_policy_version=NEW.baseline_policy_version,candidate_policy_version=NEW.candidate_policy_version,
  causal_linkage_status=NEW.causal_linkage_status,observation_decision_key=v_key
 WHERE deployment_id=NEW.deployment_id AND environment=NEW.environment AND position_id=NEW.position_id
  AND causal_linkage_status='LEGACY_NOT_ATTRIBUTABLE';
 RETURN NEW;
END; $$;

CREATE OR REPLACE VIEW v_learning_causal_attribution_v1_1 AS
SELECT d.decision_id,d.deployment_id,d.environment,d.symbol,d.interval,d.strategy,d.market_regime,
 d.decision_timestamp,d.recommendation_id,d.recommendation_version,d.activation_id,d.experiment_id,
 d.causal_linkage_status,d.causal_attributed_at
FROM decision_registry_v1 d;
CREATE OR REPLACE VIEW v_learning_causal_coverage_v1_1 AS
SELECT environment,deployment_id,count(*) decisions,
 count(*) FILTER(WHERE causal_linkage_status LIKE 'ATTRIBUTED_%') attributed,
 count(*) FILTER(WHERE causal_linkage_status='NOT_ELIGIBLE') not_eligible
FROM decision_registry_v1 GROUP BY environment,deployment_id;
CREATE OR REPLACE VIEW v_learning_counterfactual_outcomes_v1_1 AS
SELECT w.deployment_id,w.decision_key,w.recommendation_id,w.recommendation_version,w.activation_id,
 w.environment,w.slot_key,w.strategy,w.symbol,w.interval,w.market_regime,
 w.actual_action,w.actual_execution_eligible,w.recommended_shadow_action,
 w.would_trade_without_recommendation,w.would_trade_with_recommendation,w.recommendation_effect_applied,
 o.outcome_status,o.evaluation_type,o.gross_pnl,o.estimated_fees,o.net_pnl,o.outcome_at
FROM learning_would_trade_decisions_v1 w LEFT JOIN learning_counterfactual_outcomes_v1 o
 ON o.deployment_id=w.deployment_id AND o.decision_key=w.decision_key;
CREATE OR REPLACE VIEW v_learning_experiment_readiness_v1_1 AS
SELECT r.environment,r.deployment_id,r.recommendation_id,r.recommendation_version,r.strategy,r.symbol,r.interval,
 r.market_regime,a.activation_id,a.apply_mode,a.experiment_id,
 (a.apply_mode='SHADOW_OBSERVATION' AND r.environment='trading_paper') shadow_observation_ready
FROM learning_recommendation_snapshots_v1 r LEFT JOIN learning_recommendation_activations_v1 a
 ON a.deployment_id=r.deployment_id AND a.recommendation_id=r.recommendation_id;

INSERT INTO automation_kv(key,value,updated_at) VALUES
 ('causal_shadow_observation_enabled','0',now()),
 ('causal_learning_auto_apply_enabled','0',now())
ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=EXCLUDED.updated_at;

COMMIT;

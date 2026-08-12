\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';

DO $do$ BEGIN
 IF current_database()<>'trading_paper' THEN RAISE EXCEPTION 'LOCAL_PAPER_ONLY'; END IF;
 IF (SELECT count(*) FROM strategy_events WHERE event_type='LIVE_ORDER_SENT')<>0 THEN
   RAISE EXCEPTION 'PAPER_LIVE_ORDER_SENT_NOT_ZERO';
 END IF;
END $do$;

CREATE TEMP TABLE rg_fixture(snapshot_id uuid,activation_id uuid,future_gate bigint,
 historical_gate bigint,control_attribution uuid,second_attribution uuid,ft_before bigint);
INSERT INTO rg_fixture(ft_before) SELECT count(*) FROM canonical_financial_truth_v1;

DO $do$ BEGIN
 BEGIN
  PERFORM create_regime_gate_policy_snapshot_v1('local-live','ETHUSDC','1m','BBRANGE','TREND_UP',
   'fixture-wrong-deployment',clock_timestamp(),clock_timestamp()-interval '1 second',
   'fixture',clock_timestamp()-interval '2 seconds','fixture','must reject');
  RAISE EXCEPTION 'WRONG_DEPLOYMENT_WAS_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN
  IF SQLERRM='WRONG_DEPLOYMENT_WAS_ACCEPTED' THEN RAISE; END IF;
 END;
 BEGIN
  PERFORM create_regime_gate_policy_snapshot_v1('local-paper','ETHUSDC','1m','BBRANGE','TREND_UP',
   'fixture-missing-approval',clock_timestamp(),clock_timestamp()-interval '1 second',
   NULL,clock_timestamp()-interval '2 seconds','fixture','must reject');
  RAISE EXCEPTION 'MISSING_APPROVAL_WAS_ACCEPTED';
 EXCEPTION WHEN not_null_violation THEN NULL;
 END;
END $do$;

WITH made AS (
 SELECT create_regime_gate_policy_snapshot_v1('local-paper','ETHUSDC','1m','BBRANGE','TREND_UP',
  'REGIME_POLICY_FIXTURE_V1',clock_timestamp()-interval '2 seconds',clock_timestamp()-interval '5 seconds',
  'controlled-fixture',clock_timestamp()-interval '6 seconds','LOCAL-PAPER-PROOF','transaction rollback proof') id
) UPDATE rg_fixture SET snapshot_id=made.id FROM made;

INSERT INTO regime_gate_experiment_activations_v1(experiment_id,policy_snapshot_id,deployment_id,environment,
 symbol,interval,strategy,regime,effective_from,expires_at)
SELECT 'RG-CONTROL-SHADOW-FIXTURE',snapshot_id,'local-paper','trading_paper','ETHUSDC','1m','BBRANGE','TREND_UP',
 clock_timestamp()-interval '1 second',clock_timestamp()+interval '10 minutes' FROM rg_fixture
RETURNING activation_id \gset
UPDATE rg_fixture SET activation_id=:'activation_id';

DO $do$ BEGIN
 BEGIN
  INSERT INTO regime_gate_experiment_activations_v1(experiment_id,policy_snapshot_id,deployment_id,environment,
   symbol,interval,strategy,regime,effective_from,expires_at)
  SELECT 'RG-OVERLAP-FIXTURE',snapshot_id,'local-paper','trading_paper','ETHUSDC','1m','BBRANGE','TREND_UP',
   clock_timestamp(),clock_timestamp()+interval '5 minutes' FROM rg_fixture;
  RAISE EXCEPTION 'OVERLAP_WAS_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN
  IF SQLERRM='OVERLAP_WAS_ACCEPTED' THEN RAISE; END IF;
 END;
END $do$;

INSERT INTO regime_gate_events(created_at,symbol,interval,strategy,decision,allow,regime,mode,would_block,why,meta)
VALUES(clock_timestamp()-interval '10 seconds','ETHUSDC','1m','BBRANGE','ENTRY_CHECK',true,'TREND_UP','DRY_RUN',true,
 'POLICY_WOULD_BLOCK','{"fixture":"historical"}') RETURNING id \gset
UPDATE rg_fixture SET historical_gate=:'id';
INSERT INTO causal_decision_observation_v1(event_id,decision_key,decision_created_at,environment,deployment_id,
 strategy,symbol,interval,slot_key,regime,action,execution_eligible,decision_reason,decision_payload_hash,
 semantic_digest,event_digest,source_service,source_instance,decision_kind,schema_version,created_at,regime_gate_event_id)
SELECT '00000000-0000-4000-8000-000000000101','rg-fixture-historical',clock_timestamp()-interval '10 seconds',
 'trading_paper','local-paper','BBRANGE','ETHUSDC','1m','TRADING_PAPER|BBRANGE|ETHUSDC|1M|TREND_UP',
 'TREND_UP','SIMULATE',true,'FIXTURE',repeat('1',64),repeat('2',64),repeat('3',64),'fixture','fixture',
 'TRADE','CAUSAL_DECISION_OBSERVATION_V1',clock_timestamp()-interval '10 seconds',historical_gate FROM rg_fixture;
DO $do$ BEGIN
 IF persist_regime_gate_experiment_attribution_v1('local-paper','rg-fixture-historical',
   (SELECT historical_gate FROM rg_fixture)) IS NOT NULL THEN RAISE EXCEPTION 'HISTORICAL_ATTRIBUTION'; END IF;
END $do$;

INSERT INTO regime_gate_events(symbol,interval,strategy,decision,allow,regime,mode,would_block,why,meta)
VALUES('ETHUSDC','1m','BBRANGE','ENTRY_CHECK',true,'TREND_UP','DRY_RUN',true,'POLICY_WOULD_BLOCK',
 '{"fixture":"future"}') RETURNING id \gset
UPDATE rg_fixture SET future_gate=:'id';
INSERT INTO causal_decision_observation_v1(event_id,decision_key,decision_created_at,environment,deployment_id,
 strategy,symbol,interval,slot_key,regime,action,execution_eligible,decision_reason,decision_payload_hash,
 semantic_digest,event_digest,source_service,source_instance,decision_kind,schema_version,created_at,regime_gate_event_id)
SELECT '00000000-0000-4000-8000-000000000102','rg-fixture-future',clock_timestamp(),
 'trading_paper','local-paper','BBRANGE','ETHUSDC','1m','TRADING_PAPER|BBRANGE|ETHUSDC|1M|TREND_UP',
 'TREND_UP','SIMULATE',true,'FIXTURE',repeat('4',64),repeat('5',64),repeat('6',64),'fixture','fixture',
 'TRADE','CAUSAL_DECISION_OBSERVATION_V1',clock_timestamp(),future_gate FROM rg_fixture;
INSERT INTO decision_replay_v1(environment,decision_key,symbol,interval,strategy,replay_status,deployment_id,
 observation_decision_key,causal_linkage_status) VALUES('trading_paper','rg-fixture-future','ETHUSDC','1m','BBRANGE',
 'OBSERVATION_ONLY','local-paper','rg-fixture-future','NO_ACTIVE_RECOMMENDATION');
INSERT INTO learning_feature_warehouse_v1(environment,decision_key,symbol,interval,strategy,evidence_status,
 deployment_id,observation_decision_key,causal_linkage_status) VALUES('trading_paper','rg-fixture-future',
 'ETHUSDC','1m','BBRANGE','OBSERVATION_ONLY','local-paper','rg-fixture-future','NO_ACTIVE_RECOMMENDATION');
UPDATE rg_fixture SET control_attribution=persist_regime_gate_experiment_attribution_v1(
 'local-paper','rg-fixture-future',future_gate);
UPDATE rg_fixture SET second_attribution=persist_regime_gate_experiment_attribution_v1(
 'local-paper','rg-fixture-future',future_gate);

DO $do$ BEGIN
 IF EXISTS(SELECT 1 FROM rg_fixture WHERE control_attribution IS NULL OR control_attribution<>second_attribution)
   THEN RAISE EXCEPTION 'ATTRIBUTION_NOT_IDEMPOTENT'; END IF;
 IF (SELECT count(*) FROM regime_gate_decision_attribution_v1 a JOIN rg_fixture f
   ON a.attribution_id=f.control_attribution WHERE a.economic_owner_count=1 AND a.shadow_economic_owner_count=0)<>1
   THEN RAISE EXCEPTION 'ECONOMIC_OWNER_INVARIANT'; END IF;
 IF (SELECT count(*) FROM regime_gate_shadow_treatment_v1 s JOIN rg_fixture f
   ON s.attribution_id=f.control_attribution WHERE s.counterfactual_status='BLOCKED_BY_TREATMENT'
   AND s.evaluation_quality='EXACT_DECISION_EFFECT' AND s.economic_owner_count=0)<>1
   THEN RAISE EXCEPTION 'SHADOW_INVARIANT'; END IF;
 IF (SELECT count(*) FROM regime_gate_experiment_replay_v1 r JOIN rg_fixture f
   ON r.attribution_id=f.control_attribution WHERE r.control_allow AND r.control_would_block
   AND NOT r.treatment_allow AND r.treatment_status='BLOCKED_BY_TREATMENT')<>1
   THEN RAISE EXCEPTION 'REPLAY_INVARIANT'; END IF;
 IF (SELECT count(*) FROM canonical_financial_truth_v1)<>(SELECT ft_before FROM rg_fixture)
   THEN RAISE EXCEPTION 'SYNTHETIC_FT_CREATED'; END IF;
 IF (SELECT effectiveness_verdict FROM v_regime_gate_experiment_effectiveness_v1 v JOIN rg_fixture f
   ON v.activation_id=f.activation_id)<>'TRUSTED' THEN RAISE EXCEPTION 'EQUITY_V2_NOT_TRUSTED'; END IF;
END $do$;

SELECT deactivate_regime_gate_experiment_v1(activation_id,'controlled fixture rollback',clock_timestamp())
FROM rg_fixture;
DO $do$ BEGIN
 IF EXISTS(SELECT 1 FROM regime_gate_experiment_activations_v1 a JOIN rg_fixture f USING(activation_id)
   WHERE a.deactivated_at IS NULL) THEN RAISE EXCEPTION 'DEACTIVATION_FAILED'; END IF;
 IF (SELECT count(*) FROM strategy_events WHERE event_type='LIVE_ORDER_SENT')<>0
   THEN RAISE EXCEPTION 'DUPLICATE_OR_LIVE_EXPOSURE'; END IF;
END $do$;

SELECT 'CONTROL_ECONOMIC_OWNER',sum(a.economic_owner_count),
 'SHADOW_ECONOMIC_OWNER',sum(a.shadow_economic_owner_count),
 'DUPLICATE_EXPOSURE',0,
 'REPLAY',min(r.treatment_status),
 'EQUITY',min(v.effectiveness_verdict),
 'ROLLBACK',bool_and(x.deactivated_at IS NOT NULL)
FROM rg_fixture f JOIN regime_gate_decision_attribution_v1 a ON a.attribution_id=f.control_attribution
JOIN regime_gate_experiment_replay_v1 r ON r.attribution_id=a.attribution_id
JOIN regime_gate_experiment_activations_v1 x ON x.activation_id=f.activation_id
JOIN v_regime_gate_experiment_effectiveness_v1 v ON v.activation_id=x.activation_id;
ROLLBACK;

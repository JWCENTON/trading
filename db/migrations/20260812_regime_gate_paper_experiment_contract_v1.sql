-- WALTRADE REGIME GATE PAPER EXPERIMENT CONTRACT V1
-- Additive PAPER-only CONTROL vs immutable SHADOW-TREATMENT evidence.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION regime_gate_paper_context_v1()
RETURNS boolean LANGUAGE sql STABLE AS $fn$
  SELECT current_database()='trading_paper' OR EXISTS (
    SELECT 1 FROM automation_kv
    WHERE key='waltrade_disposable_test_db' AND lower(value)='true'
  )
$fn$;

CREATE OR REPLACE FUNCTION regime_gate_policy_fingerprint_v1(
  p_deployment_id text,p_environment text,p_symbol text,p_interval text,
  p_strategy text,p_regime text,p_allow_entry boolean,p_policy_version text,
  p_effective_from timestamptz,p_evidence_cutoff_at timestamptz
) RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $fn$
 SELECT encode(digest(concat_ws('|',p_deployment_id,p_environment,upper(p_symbol),
   p_interval,upper(p_strategy),upper(p_regime),p_allow_entry::text,p_policy_version,
   to_char(p_effective_from AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US'),
   to_char(p_evidence_cutoff_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US')),'sha256'),'hex')
$fn$;

CREATE TABLE IF NOT EXISTS regime_gate_policy_snapshots_v1 (
  policy_snapshot_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_id text NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper')),
  environment text NOT NULL CHECK (environment='trading_paper'),
  symbol text NOT NULL CHECK (btrim(symbol)<>''), interval text NOT NULL CHECK (btrim(interval)<>''),
  strategy text NOT NULL CHECK (btrim(strategy)<>''), regime text NOT NULL CHECK (btrim(regime)<>''),
  allow_entry boolean NOT NULL,
  policy_version text NOT NULL CHECK (btrim(policy_version)<>''),
  policy_fingerprint text NOT NULL UNIQUE CHECK (policy_fingerprint ~ '^[0-9a-f]{64}$'),
  effective_from timestamptz NOT NULL, evidence_cutoff_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  approved_by text NOT NULL CHECK (btrim(approved_by)<>''),
  approved_at timestamptz NOT NULL,
  approval_reference text NOT NULL CHECK (btrim(approval_reference)<>''),
  approval_reason text NOT NULL CHECK (btrim(approval_reason)<>''),
  source_payload jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(source_payload)='object'),
  schema_version text NOT NULL DEFAULT 'REGIME_GATE_PAPER_EXPERIMENT_CONTRACT_V1',
  CHECK (evidence_cutoff_at<=effective_from), CHECK (approved_at<=effective_from),
  CHECK (policy_fingerprint=regime_gate_policy_fingerprint_v1(deployment_id,environment,
    symbol,interval,strategy,regime,allow_entry,policy_version,effective_from,evidence_cutoff_at))
);

CREATE OR REPLACE FUNCTION create_regime_gate_policy_snapshot_v1(
 p_deployment_id text,p_symbol text,p_interval text,p_strategy text,p_regime text,
 p_policy_version text,p_effective_from timestamptz,p_evidence_cutoff_at timestamptz,
 p_approved_by text,p_approved_at timestamptz,p_approval_reference text,p_approval_reason text
) RETURNS uuid LANGUAGE plpgsql AS $fn$
DECLARE v_allow boolean; v_note text; v_id uuid; v_fp text;
BEGIN
 IF NOT regime_gate_paper_context_v1() OR p_deployment_id NOT IN ('local-paper','vps-paper')
   THEN RAISE EXCEPTION 'REGIME_GATE_EXPERIMENT_PAPER_ONLY'; END IF;
 SELECT allow_entry,note INTO v_allow,v_note FROM regime_policy
  WHERE upper(strategy)=upper(p_strategy) AND upper(regime)=upper(p_regime);
 IF NOT FOUND THEN RAISE EXCEPTION 'REGIME_GATE_POLICY_NOT_FOUND'; END IF;
 v_fp:=regime_gate_policy_fingerprint_v1(p_deployment_id,'trading_paper',p_symbol,p_interval,
   p_strategy,p_regime,v_allow,p_policy_version,p_effective_from,p_evidence_cutoff_at);
 INSERT INTO regime_gate_policy_snapshots_v1(deployment_id,environment,symbol,interval,strategy,regime,
  allow_entry,policy_version,policy_fingerprint,effective_from,evidence_cutoff_at,approved_by,approved_at,
  approval_reference,approval_reason,source_payload)
 VALUES(p_deployment_id,'trading_paper',p_symbol,p_interval,p_strategy,p_regime,v_allow,p_policy_version,
  v_fp,p_effective_from,p_evidence_cutoff_at,p_approved_by,p_approved_at,p_approval_reference,
  p_approval_reason,jsonb_build_object('source','regime_policy','policy_note',v_note))
 ON CONFLICT(policy_fingerprint) DO NOTHING RETURNING policy_snapshot_id INTO v_id;
 IF v_id IS NULL THEN SELECT policy_snapshot_id INTO v_id FROM regime_gate_policy_snapshots_v1
  WHERE policy_fingerprint=v_fp; END IF;
 RETURN v_id;
END $fn$;

CREATE TABLE IF NOT EXISTS regime_gate_experiment_activations_v1 (
  activation_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  experiment_id text NOT NULL CHECK (btrim(experiment_id)<>''),
  policy_snapshot_id uuid NOT NULL REFERENCES regime_gate_policy_snapshots_v1(policy_snapshot_id),
  deployment_id text NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper')),
  environment text NOT NULL CHECK (environment='trading_paper'),
  symbol text NOT NULL, interval text NOT NULL, strategy text NOT NULL, regime text NOT NULL,
  control_mode text NOT NULL DEFAULT 'DRY_RUN' CHECK (control_mode='DRY_RUN'),
  treatment_mode text NOT NULL DEFAULT 'ENFORCE' CHECK (treatment_mode='ENFORCE'),
  effective_from timestamptz NOT NULL, expires_at timestamptz NOT NULL,
  deactivated_at timestamptz, deactivation_reason text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (effective_from<expires_at),
  CHECK (deactivated_at IS NULL OR (deactivated_at>=effective_from AND deactivated_at<=expires_at)),
  CHECK ((deactivated_at IS NULL)=(deactivation_reason IS NULL))
);

CREATE OR REPLACE FUNCTION validate_regime_gate_activation_v1()
RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE s regime_gate_policy_snapshots_v1%ROWTYPE;
BEGIN
  IF NOT regime_gate_paper_context_v1() THEN RAISE EXCEPTION 'REGIME_GATE_EXPERIMENT_PAPER_ONLY'; END IF;
  SELECT * INTO s FROM regime_gate_policy_snapshots_v1 WHERE policy_snapshot_id=NEW.policy_snapshot_id;
  IF NOT FOUND OR s.approved_by IS NULL OR s.approved_at IS NULL OR s.approval_reference IS NULL
     OR s.approval_reason IS NULL THEN RAISE EXCEPTION 'REGIME_GATE_EXPERIMENT_APPROVAL_REQUIRED'; END IF;
  IF (NEW.deployment_id,NEW.environment,upper(NEW.symbol),NEW.interval,upper(NEW.strategy),upper(NEW.regime))
     IS DISTINCT FROM (s.deployment_id,s.environment,upper(s.symbol),s.interval,upper(s.strategy),upper(s.regime))
     THEN RAISE EXCEPTION 'REGIME_GATE_EXPERIMENT_SCOPE_MISMATCH'; END IF;
  IF NEW.effective_from<s.effective_from OR NEW.effective_from<=s.evidence_cutoff_at
     THEN RAISE EXCEPTION 'REGIME_GATE_EXPERIMENT_STALE_BOUNDARY'; END IF;
  IF TG_OP='INSERT' AND EXISTS (
    SELECT 1 FROM regime_gate_experiment_activations_v1 a
    WHERE a.deployment_id=NEW.deployment_id AND a.environment=NEW.environment
      AND upper(a.symbol)=upper(NEW.symbol) AND a.interval=NEW.interval
      AND upper(a.strategy)=upper(NEW.strategy) AND upper(a.regime)=upper(NEW.regime)
      AND tstzrange(a.effective_from,least(a.expires_at,coalesce(a.deactivated_at,a.expires_at)),'[)')
          && tstzrange(NEW.effective_from,NEW.expires_at,'[)')
  ) THEN RAISE EXCEPTION 'REGIME_GATE_EXPERIMENT_OVERLAP'; END IF;
  RETURN NEW;
END $fn$;
DROP TRIGGER IF EXISTS trg_regime_gate_activation_validate_v1 ON regime_gate_experiment_activations_v1;
CREATE TRIGGER trg_regime_gate_activation_validate_v1 BEFORE INSERT ON regime_gate_experiment_activations_v1
FOR EACH ROW EXECUTE FUNCTION validate_regime_gate_activation_v1();

CREATE OR REPLACE FUNCTION protect_regime_gate_activation_v1()
RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'REGIME_GATE_ACTIVATION_IDENTITY_IMMUTABLE'; END IF;
 IF (NEW.activation_id,NEW.experiment_id,NEW.policy_snapshot_id,NEW.deployment_id,NEW.environment,
   NEW.symbol,NEW.interval,NEW.strategy,NEW.regime,NEW.control_mode,NEW.treatment_mode,
   NEW.effective_from,NEW.expires_at,NEW.created_at)
   IS DISTINCT FROM
   (OLD.activation_id,OLD.experiment_id,OLD.policy_snapshot_id,OLD.deployment_id,OLD.environment,
   OLD.symbol,OLD.interval,OLD.strategy,OLD.regime,OLD.control_mode,OLD.treatment_mode,
   OLD.effective_from,OLD.expires_at,OLD.created_at)
   OR OLD.deactivated_at IS NOT NULL OR NEW.deactivated_at IS NULL OR NEW.deactivation_reason IS NULL
 THEN RAISE EXCEPTION 'REGIME_GATE_ACTIVATION_IDENTITY_IMMUTABLE'; END IF;
 RETURN NEW;
END $fn$;
CREATE OR REPLACE FUNCTION reject_regime_gate_evidence_mutation_v1()
RETURNS trigger LANGUAGE plpgsql AS $fn$ BEGIN RAISE EXCEPTION 'REGIME_GATE_EVIDENCE_APPEND_ONLY'; END $fn$;
DROP TRIGGER IF EXISTS trg_regime_gate_activation_protect_v1 ON regime_gate_experiment_activations_v1;
CREATE TRIGGER trg_regime_gate_activation_protect_v1 BEFORE UPDATE OR DELETE ON regime_gate_experiment_activations_v1
FOR EACH ROW EXECUTE FUNCTION protect_regime_gate_activation_v1();
DROP TRIGGER IF EXISTS trg_regime_gate_snapshot_immutable_v1 ON regime_gate_policy_snapshots_v1;
CREATE TRIGGER trg_regime_gate_snapshot_immutable_v1 BEFORE UPDATE OR DELETE ON regime_gate_policy_snapshots_v1
FOR EACH ROW EXECUTE FUNCTION reject_regime_gate_evidence_mutation_v1();

ALTER TABLE causal_decision_observation_v1 ADD COLUMN IF NOT EXISTS regime_gate_event_id bigint;
ALTER TABLE decision_replay_v1 ADD COLUMN IF NOT EXISTS regime_gate_experiment_id text,
 ADD COLUMN IF NOT EXISTS regime_gate_activation_id uuid,
 ADD COLUMN IF NOT EXISTS regime_gate_policy_snapshot_id uuid,
 ADD COLUMN IF NOT EXISTS regime_gate_event_id bigint;
ALTER TABLE learning_feature_warehouse_v1 ADD COLUMN IF NOT EXISTS regime_gate_experiment_id text,
 ADD COLUMN IF NOT EXISTS regime_gate_activation_id uuid,
 ADD COLUMN IF NOT EXISTS regime_gate_policy_snapshot_id uuid,
 ADD COLUMN IF NOT EXISTS regime_gate_event_id bigint;

CREATE TABLE IF NOT EXISTS regime_gate_decision_attribution_v1 (
  attribution_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_id text NOT NULL, environment text NOT NULL CHECK (environment='trading_paper'),
  decision_key text NOT NULL, observation_event_id uuid NOT NULL REFERENCES causal_decision_observation_v1(event_id),
  gate_event_id bigint NOT NULL REFERENCES regime_gate_events(id),
  experiment_id text NOT NULL, activation_id uuid NOT NULL REFERENCES regime_gate_experiment_activations_v1,
  policy_snapshot_id uuid NOT NULL REFERENCES regime_gate_policy_snapshots_v1,
  experiment_arm text NOT NULL DEFAULT 'CONTROL' CHECK (experiment_arm='CONTROL'),
  economic_owner_count integer NOT NULL DEFAULT 1 CHECK (economic_owner_count=1),
  shadow_economic_owner_count integer NOT NULL DEFAULT 0 CHECK (shadow_economic_owner_count=0),
  actual_action text NOT NULL, actual_execution_eligible boolean NOT NULL,
  attributed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(deployment_id,decision_key), UNIQUE(deployment_id,gate_event_id)
);
CREATE TABLE IF NOT EXISTS regime_gate_shadow_treatment_v1 (
  shadow_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  attribution_id uuid NOT NULL UNIQUE REFERENCES regime_gate_decision_attribution_v1,
  experiment_arm text NOT NULL DEFAULT 'SHADOW_TREATMENT' CHECK (experiment_arm='SHADOW_TREATMENT'),
  counterfactual_status text NOT NULL CHECK (counterfactual_status='BLOCKED_BY_TREATMENT'),
  evaluation_quality text NOT NULL CHECK (evaluation_quality='EXACT_DECISION_EFFECT'),
  economic_evaluation text NOT NULL DEFAULT 'NOT_EVALUABLE'
    CHECK (economic_evaluation IN ('DIRECTIONAL_ONLY','NOT_EVALUABLE')),
  economic_owner_count integer NOT NULL DEFAULT 0 CHECK (economic_owner_count=0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS regime_gate_experiment_replay_v1 (
  replay_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), attribution_id uuid NOT NULL UNIQUE REFERENCES regime_gate_decision_attribution_v1,
  policy_snapshot_id uuid NOT NULL REFERENCES regime_gate_policy_snapshots_v1, policy_fingerprint text NOT NULL,
  control_mode text NOT NULL CHECK (control_mode='DRY_RUN'), treatment_mode text NOT NULL CHECK (treatment_mode='ENFORCE'),
  control_allow boolean NOT NULL, control_would_block boolean NOT NULL,
  treatment_allow boolean NOT NULL, treatment_status text NOT NULL,
  replay_quality text NOT NULL CHECK (replay_quality='EXACT_DECISION_EFFECT'),
  replay_fingerprint text NOT NULL CHECK (replay_fingerprint ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
DO $do$ DECLARE t text; BEGIN FOREACH t IN ARRAY ARRAY['regime_gate_decision_attribution_v1','regime_gate_shadow_treatment_v1','regime_gate_experiment_replay_v1'] LOOP
 EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_immutable ON %I',t,t);
 EXECUTE format('CREATE TRIGGER trg_%s_immutable BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reject_regime_gate_evidence_mutation_v1()',t,t);
END LOOP; END $do$;

CREATE OR REPLACE FUNCTION replay_regime_gate_experiment_v1(p_attribution_id uuid)
RETURNS uuid LANGUAGE plpgsql AS $fn$
DECLARE a regime_gate_decision_attribution_v1%ROWTYPE; s regime_gate_policy_snapshots_v1%ROWTYPE; r_id uuid; fp text;
BEGIN
 SELECT * INTO a FROM regime_gate_decision_attribution_v1 WHERE attribution_id=p_attribution_id;
 IF NOT FOUND THEN RAISE EXCEPTION 'REGIME_GATE_ATTRIBUTION_NOT_FOUND'; END IF;
 SELECT * INTO s FROM regime_gate_policy_snapshots_v1 WHERE policy_snapshot_id=a.policy_snapshot_id;
 fp:=encode(digest(concat_ws('|',a.gate_event_id,s.policy_fingerprint,'DRY_RUN','true','true','ENFORCE',s.allow_entry::text),'sha256'),'hex');
 INSERT INTO regime_gate_experiment_replay_v1(attribution_id,policy_snapshot_id,policy_fingerprint,
  control_mode,treatment_mode,control_allow,control_would_block,treatment_allow,treatment_status,replay_quality,replay_fingerprint)
 VALUES(a.attribution_id,s.policy_snapshot_id,s.policy_fingerprint,'DRY_RUN','ENFORCE',true,true,s.allow_entry,
  CASE WHEN s.allow_entry THEN 'ALLOWED_BY_TREATMENT' ELSE 'BLOCKED_BY_TREATMENT' END,'EXACT_DECISION_EFFECT',fp)
 ON CONFLICT(attribution_id) DO NOTHING RETURNING replay_id INTO r_id;
 IF r_id IS NULL THEN SELECT replay_id INTO r_id FROM regime_gate_experiment_replay_v1 WHERE attribution_id=a.attribution_id AND replay_fingerprint=fp; END IF;
 IF r_id IS NULL THEN RAISE EXCEPTION 'REGIME_GATE_REPLAY_IDEMPOTENCY_CONFLICT'; END IF;
 RETURN r_id;
END $fn$;

CREATE OR REPLACE FUNCTION persist_regime_gate_experiment_attribution_v1(
 p_deployment_id text,p_decision_key text,p_gate_event_id bigint
) RETURNS uuid LANGUAGE plpgsql AS $fn$
DECLARE o causal_decision_observation_v1%ROWTYPE; g regime_gate_events%ROWTYPE; a regime_gate_experiment_activations_v1%ROWTYPE; x_id uuid;
BEGIN
 IF NOT regime_gate_paper_context_v1() OR p_deployment_id NOT IN ('local-paper','vps-paper') THEN RAISE EXCEPTION 'REGIME_GATE_EXPERIMENT_PAPER_ONLY'; END IF;
 SELECT * INTO o FROM causal_decision_observation_v1 WHERE deployment_id=p_deployment_id AND decision_key=p_decision_key;
 SELECT * INTO g FROM regime_gate_events WHERE id=p_gate_event_id;
 IF NOT FOUND OR o.event_id IS NULL OR o.regime_gate_event_id IS DISTINCT FROM p_gate_event_id
   OR g.decision<>'ENTRY_CHECK' OR upper(coalesce(g.mode,''))<>'DRY_RUN' OR g.allow IS NOT TRUE OR g.would_block IS NOT TRUE
   THEN RETURN NULL; END IF;
 SELECT act.* INTO a FROM regime_gate_experiment_activations_v1 act
 JOIN regime_gate_policy_snapshots_v1 s ON s.policy_snapshot_id=act.policy_snapshot_id
 WHERE act.deployment_id=o.deployment_id AND act.environment=o.environment
  AND upper(act.symbol)=upper(o.symbol) AND act.interval=o.interval AND upper(act.strategy)=upper(o.strategy)
  AND upper(act.regime)=upper(g.regime) AND s.allow_entry=false
  AND o.decision_created_at>=act.effective_from AND o.decision_created_at<act.expires_at
  AND g.created_at>=act.effective_from AND g.created_at<act.expires_at
  AND (act.deactivated_at IS NULL OR o.decision_created_at<act.deactivated_at)
  AND o.decision_created_at>s.evidence_cutoff_at
 ORDER BY act.effective_from DESC LIMIT 1;
 IF NOT FOUND THEN RETURN NULL; END IF;
 INSERT INTO regime_gate_decision_attribution_v1(deployment_id,environment,decision_key,observation_event_id,
  gate_event_id,experiment_id,activation_id,policy_snapshot_id,actual_action,actual_execution_eligible)
 VALUES(o.deployment_id,o.environment,o.decision_key,o.event_id,g.id,a.experiment_id,a.activation_id,a.policy_snapshot_id,o.action,o.execution_eligible)
 ON CONFLICT(deployment_id,decision_key) DO NOTHING RETURNING attribution_id INTO x_id;
 IF x_id IS NULL THEN SELECT attribution_id INTO x_id FROM regime_gate_decision_attribution_v1
   WHERE deployment_id=o.deployment_id AND decision_key=o.decision_key AND gate_event_id=g.id AND activation_id=a.activation_id; END IF;
 IF x_id IS NULL THEN RAISE EXCEPTION 'REGIME_GATE_ATTRIBUTION_IDEMPOTENCY_CONFLICT'; END IF;
 INSERT INTO regime_gate_shadow_treatment_v1(attribution_id,counterfactual_status,evaluation_quality)
 VALUES(x_id,'BLOCKED_BY_TREATMENT','EXACT_DECISION_EFFECT') ON CONFLICT(attribution_id) DO NOTHING;
 UPDATE decision_replay_v1 SET regime_gate_experiment_id=a.experiment_id,regime_gate_activation_id=a.activation_id,
  regime_gate_policy_snapshot_id=a.policy_snapshot_id,regime_gate_event_id=g.id
  WHERE deployment_id=o.deployment_id AND observation_decision_key=o.decision_key
   AND regime_gate_activation_id IS NULL;
 UPDATE learning_feature_warehouse_v1 SET regime_gate_experiment_id=a.experiment_id,regime_gate_activation_id=a.activation_id,
  regime_gate_policy_snapshot_id=a.policy_snapshot_id,regime_gate_event_id=g.id
  WHERE deployment_id=o.deployment_id AND observation_decision_key=o.decision_key
   AND regime_gate_activation_id IS NULL;
 PERFORM replay_regime_gate_experiment_v1(x_id);
 RETURN x_id;
END $fn$;

CREATE OR REPLACE FUNCTION deactivate_regime_gate_experiment_v1(p_activation_id uuid,p_reason text,p_at timestamptz DEFAULT clock_timestamp())
RETURNS boolean LANGUAGE plpgsql AS $fn$ BEGIN
 IF NOT regime_gate_paper_context_v1() OR nullif(btrim(p_reason),'') IS NULL THEN RAISE EXCEPTION 'REGIME_GATE_DEACTIVATION_REJECTED'; END IF;
 UPDATE regime_gate_experiment_activations_v1 SET deactivated_at=p_at,deactivation_reason=p_reason
 WHERE activation_id=p_activation_id AND deactivated_at IS NULL;
 RETURN FOUND;
END $fn$;

CREATE OR REPLACE VIEW v_regime_gate_experiment_effectiveness_v1 AS
WITH linked AS (
 SELECT a.*,d.position_id,p.status position_status,ft.financial_truth_status,ft.authoritative_net_pnl,
  outc.mfe_pct,outc.mae_pct,s.economic_evaluation
 FROM regime_gate_decision_attribution_v1 a
 JOIN regime_gate_shadow_treatment_v1 s ON s.attribution_id=a.attribution_id
 LEFT JOIN decision_registry_v1 d ON d.environment=a.environment AND d.legacy_decision_key=a.decision_key
  AND ((a.deployment_id='local-paper' AND d.deployment_id='LOCAL') OR (a.deployment_id='vps-paper' AND d.deployment_id='VPS'))
 LEFT JOIN positions p ON p.id=d.position_id
 LEFT JOIN canonical_financial_truth_v1 ft ON ft.position_id=d.position_id
 LEFT JOIN decision_outcomes_v1 outc ON outc.decision_id=d.decision_id AND outc.outcome_type='ACTUAL_TRADE' AND outc.outcome_status='COMPLETE'
), base AS (
 SELECT a.*,b.cutover_boundary,
  (SELECT e.evidence_status FROM equity_daily_snapshot_v1 e WHERE e.deployment_id=a.deployment_id ORDER BY e.snapshot_date DESC,e.source_timestamp DESC LIMIT 1) equity_status
 FROM regime_gate_experiment_activations_v1 a LEFT JOIN paper_equity_baseline_v2 b ON b.deployment_id=a.deployment_id
)
SELECT b.deployment_id,b.experiment_id,b.activation_id,b.policy_snapshot_id,
 count(l.attribution_id) eligible_decisions,count(l.attribution_id) would_block_count,
 count(l.attribution_id) FILTER(WHERE l.actual_execution_eligible) actual_control_entries,
 count(l.attribution_id) FILTER(WHERE l.position_status='CLOSED' AND l.financial_truth_status='COMPLETE') actual_control_closed_outcomes,
 sum(l.authoritative_net_pnl) FILTER(WHERE l.financial_truth_status='COMPLETE') canonical_net_pnl_after_fees,
 sum(l.authoritative_net_pnl) FILTER(WHERE l.authoritative_net_pnl>0)/nullif(abs(sum(l.authoritative_net_pnl) FILTER(WHERE l.authoritative_net_pnl<0)),0) profit_factor,
 avg(l.authoritative_net_pnl) FILTER(WHERE l.financial_truth_status='COMPLETE') expectancy,
 avg((l.authoritative_net_pnl>0)::int) FILTER(WHERE l.financial_truth_status='COMPLETE') win_rate,
 avg(l.mfe_pct) mfe,avg(l.mae_pct) mae,
 count(*) FILTER(WHERE l.authoritative_net_pnl<0) avoided_loss,
 count(*) FILTER(WHERE l.authoritative_net_pnl>0) missed_profit,
 count(*) FILTER(WHERE l.financial_truth_status IS DISTINCT FROM 'COMPLETE') not_evaluable,
 CASE WHEN b.cutover_boundary IS NOT NULL AND b.equity_status='COMPLETE'
  AND count(*) FILTER(WHERE l.position_status='CLOSED' AND l.financial_truth_status IS DISTINCT FROM 'COMPLETE')=0
  THEN 'TRUSTED' ELSE 'BLOCKED' END effectiveness_verdict
FROM base b LEFT JOIN linked l ON l.activation_id=b.activation_id
GROUP BY b.deployment_id,b.experiment_id,b.activation_id,b.policy_snapshot_id,b.cutover_boundary,b.equity_status;

COMMENT ON VIEW v_regime_gate_experiment_effectiveness_v1 IS
'Directional-only shadow classification; never synthetic counterfactual financial truth.';
COMMIT;

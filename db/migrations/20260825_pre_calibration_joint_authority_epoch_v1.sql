-- WALTRADE PRE-CALIBRATION JOINT AUTHORITY EPOCH V1
-- Additive lifecycle metadata only. No economic or observation backfill.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE IF NOT EXISTS public.joint_authority_activation_attempt_v1 (
  attempt_id UUID PRIMARY KEY,
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper')),
  authority_identity TEXT NOT NULL CHECK (btrim(authority_identity)<>''),
  previous_failed_attempt_id UUID REFERENCES
    public.joint_authority_activation_attempt_v1(attempt_id),
  attempt_status TEXT NOT NULL CHECK (attempt_status IN ('PREPARED','FAILED','ACTIVATED')),
  requested_activation_boundary TIMESTAMPTZ NOT NULL,
  prepared_at TIMESTAMPTZ NOT NULL,
  failure_reason TEXT CHECK (
    (attempt_status='FAILED' AND failure_reason IS NOT NULL AND btrim(failure_reason)<>'')
    OR (attempt_status<>'FAILED' AND failure_reason IS NULL)
  ),
  activated_at TIMESTAMPTZ CHECK (
    (attempt_status='ACTIVATED' AND activated_at IS NOT NULL)
    OR (attempt_status<>'ACTIVATED' AND activated_at IS NULL)
  ),
  source_fingerprints JSONB NOT NULL CHECK (
    jsonb_typeof(source_fingerprints)='object' AND source_fingerprints<>'{}'::jsonb
  ),
  producer_revision TEXT NOT NULL CHECK (producer_revision ~ '^[0-9a-f]{40}$'),
  attempt_fingerprint TEXT NOT NULL UNIQUE CHECK (attempt_fingerprint ~ '^[0-9a-f]{64}$'),
  contract_version TEXT NOT NULL CHECK (contract_version='JOINT_AUTHORITY_EPOCH_V1'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (date_trunc('minute',requested_activation_boundary)=requested_activation_boundary),
  CHECK ((extract(minute FROM requested_activation_boundary)::int % 15)=0),
  CHECK (extract(second FROM requested_activation_boundary)=0)
);

CREATE TABLE IF NOT EXISTS public.joint_authority_epoch_v1 (
  authority_epoch_id BIGSERIAL PRIMARY KEY,
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper')),
  baseline_id BIGINT NOT NULL REFERENCES public.paper_equity_baseline_v2(baseline_id),
  baseline_fingerprint TEXT NOT NULL CHECK (baseline_fingerprint ~ '^[0-9a-f]{64}$'),
  replay_cutover_id BIGINT NOT NULL REFERENCES public.paper_portfolio_replay_cutover_v1(cutover_id),
  replay_cutover_fingerprint TEXT NOT NULL CHECK (replay_cutover_fingerprint ~ '^[0-9a-f]{64}$'),
  drawdown_activation_id BIGINT NOT NULL UNIQUE REFERENCES
    public.paper_drawdown_history_activation_v1(activation_id),
  drawdown_generation INTEGER NOT NULL CHECK (drawdown_generation>0),
  drawdown_generation_fingerprint TEXT NOT NULL CHECK (
    drawdown_generation_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  drawdown_activation_boundary TIMESTAMPTZ NOT NULL,
  first_required_cadence TIMESTAMPTZ NOT NULL,
  activation_attempt_id UUID NOT NULL UNIQUE REFERENCES
    public.joint_authority_activation_attempt_v1(attempt_id),
  git_revision TEXT NOT NULL CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  contract_versions JSONB NOT NULL CHECK (
    jsonb_typeof(contract_versions)='object' AND contract_versions<>'{}'::jsonb
  ),
  contract_fingerprints JSONB NOT NULL CHECK (
    jsonb_typeof(contract_fingerprints)='object' AND contract_fingerprints<>'{}'::jsonb
  ),
  deployment_identity TEXT NOT NULL CHECK (btrim(deployment_identity)<>''),
  epoch_fingerprint TEXT NOT NULL UNIQUE CHECK (epoch_fingerprint ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (deployment_id,replay_cutover_id,drawdown_generation),
  CHECK (first_required_cadence=drawdown_activation_boundary+INTERVAL '15 minutes'),
  CHECK (date_trunc('minute',drawdown_activation_boundary)=drawdown_activation_boundary),
  CHECK ((extract(minute FROM drawdown_activation_boundary)::int % 15)=0),
  CHECK (extract(second FROM drawdown_activation_boundary)=0)
);

CREATE TABLE IF NOT EXISTS public.joint_authority_epoch_selection_v1 (
  selection_id BIGSERIAL PRIMARY KEY,
  authority_epoch_id BIGINT NOT NULL UNIQUE REFERENCES
    public.joint_authority_epoch_v1(authority_epoch_id),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper')),
  previous_selection_id BIGINT UNIQUE REFERENCES
    public.joint_authority_epoch_selection_v1(selection_id),
  previous_authority_epoch_id BIGINT REFERENCES
    public.joint_authority_epoch_v1(authority_epoch_id),
  selected_at TIMESTAMPTZ NOT NULL,
  selection_reason TEXT NOT NULL CHECK (selection_reason IN (
    'INITIAL_JOINT_AUTHORITY_EPOCH',
    'UPSTREAM_REPLAY_CUTOVER_EPOCH_CHANGE',
    'AUTHORITY_ACTIVATION_FAILURE_RECOVERY',
    'PRECISION_DEFECT_RECOVERY'
  )),
  selection_fingerprint TEXT NOT NULL UNIQUE CHECK (selection_fingerprint ~ '^[0-9a-f]{64}$'),
  git_revision TEXT NOT NULL CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (
    (previous_selection_id IS NULL AND previous_authority_epoch_id IS NULL)
    OR (previous_selection_id IS NOT NULL AND previous_authority_epoch_id IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS public.risk_budget_authority_epoch_binding_v1 (
  binding_id BIGSERIAL PRIMARY KEY,
  event_id UUID NOT NULL UNIQUE REFERENCES public.risk_budget_event_v1(event_id),
  authority_epoch_id BIGINT NOT NULL REFERENCES public.joint_authority_epoch_v1(authority_epoch_id),
  evaluation_as_of TIMESTAMPTZ NOT NULL,
  calibration_replay_eligible BOOLEAN NOT NULL,
  baseline_fingerprint TEXT NOT NULL CHECK (baseline_fingerprint ~ '^[0-9a-f]{64}$'),
  replay_cutover_fingerprint TEXT NOT NULL CHECK (replay_cutover_fingerprint ~ '^[0-9a-f]{64}$'),
  drawdown_generation_fingerprint TEXT NOT NULL CHECK (drawdown_generation_fingerprint ~ '^[0-9a-f]{64}$'),
  risk_budget_source_fingerprint TEXT NOT NULL CHECK (risk_budget_source_fingerprint ~ '^[0-9a-f]{64}$'),
  binding_fingerprint TEXT NOT NULL UNIQUE CHECK (binding_fingerprint ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (authority_epoch_id,evaluation_as_of),
  CHECK (calibration_replay_eligible)
);

CREATE OR REPLACE FUNCTION public.reject_joint_authority_epoch_mutation_v1()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  RAISE EXCEPTION 'JOINT_AUTHORITY_EPOCH_V1_APPEND_ONLY';
END
$function$;

CREATE OR REPLACE FUNCTION public.validate_joint_authority_epoch_v1()
RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE replay_record public.paper_portfolio_replay_cutover_v1%ROWTYPE;
DECLARE activation_record public.paper_drawdown_history_activation_v1%ROWTYPE;
DECLARE attempt_record public.joint_authority_activation_attempt_v1%ROWTYPE;
DECLARE baseline_record public.paper_equity_baseline_v2%ROWTYPE;
BEGIN
  SELECT * INTO baseline_record FROM public.paper_equity_baseline_v2
  WHERE baseline_id=NEW.baseline_id;
  SELECT * INTO replay_record FROM public.paper_portfolio_replay_cutover_v1
  WHERE cutover_id=NEW.replay_cutover_id;
  SELECT * INTO activation_record FROM public.paper_drawdown_history_activation_v1
  WHERE activation_id=NEW.drawdown_activation_id;
  SELECT * INTO attempt_record FROM public.joint_authority_activation_attempt_v1
  WHERE attempt_id=NEW.activation_attempt_id;
  IF baseline_record.baseline_id IS NULL
     OR replay_record.cutover_id IS NULL
     OR activation_record.activation_id IS NULL
     OR attempt_record.attempt_id IS NULL
     OR replay_record.deployment_id<>NEW.deployment_id
     OR baseline_record.deployment_id<>NEW.deployment_id
     OR baseline_record.activation_fingerprint<>NEW.baseline_fingerprint
     OR baseline_record.baseline_timestamp>replay_record.cutover_at
     OR replay_record.cutover_fingerprint<>NEW.replay_cutover_fingerprint
     OR NEW.drawdown_activation_boundary<replay_record.cutover_at
     OR activation_record.deployment_id<>NEW.deployment_id
     OR activation_record.generation<>NEW.drawdown_generation
     OR activation_record.activated_at<>NEW.drawdown_activation_boundary
     OR activation_record.activation_evidence_fingerprint<>
        NEW.drawdown_generation_fingerprint
     OR attempt_record.deployment_id<>NEW.deployment_id
     OR attempt_record.attempt_status<>'ACTIVATED'
     OR attempt_record.requested_activation_boundary<>
        NEW.drawdown_activation_boundary THEN
    RAISE EXCEPTION 'JOINT_AUTHORITY_EPOCH_V1_SOURCE_INVALID';
  END IF;
  RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION public.validate_joint_authority_attempt_v1()
RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE previous_record public.joint_authority_activation_attempt_v1%ROWTYPE;
BEGIN
  IF NEW.previous_failed_attempt_id IS NOT NULL THEN
    SELECT * INTO previous_record FROM public.joint_authority_activation_attempt_v1
    WHERE attempt_id=NEW.previous_failed_attempt_id;
    IF NOT FOUND OR previous_record.attempt_status<>'FAILED'
       OR previous_record.deployment_id<>NEW.deployment_id
       OR NEW.prepared_at<=previous_record.prepared_at THEN
      RAISE EXCEPTION 'JOINT_AUTHORITY_EPOCH_V1_RETRY_INVALID';
    END IF;
  END IF;
  RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION public.validate_joint_authority_selection_v1()
RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE epoch_record public.joint_authority_epoch_v1%ROWTYPE;
DECLARE previous_record public.joint_authority_epoch_selection_v1%ROWTYPE;
BEGIN
  SELECT * INTO epoch_record FROM public.joint_authority_epoch_v1
  WHERE authority_epoch_id=NEW.authority_epoch_id;
  IF NOT FOUND OR epoch_record.deployment_id<>NEW.deployment_id
     OR NEW.selected_at<epoch_record.drawdown_activation_boundary THEN
    RAISE EXCEPTION 'JOINT_AUTHORITY_EPOCH_V1_SELECTION_INVALID';
  END IF;
  IF NEW.previous_selection_id IS NOT NULL THEN
    SELECT * INTO previous_record FROM public.joint_authority_epoch_selection_v1
    WHERE selection_id=NEW.previous_selection_id;
    IF NOT FOUND OR previous_record.authority_epoch_id<>
       NEW.previous_authority_epoch_id OR previous_record.deployment_id<>
       NEW.deployment_id THEN
      RAISE EXCEPTION 'JOINT_AUTHORITY_EPOCH_V1_SELECTION_CHAIN_INVALID';
    END IF;
  END IF;
  RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION public.validate_risk_budget_epoch_binding_v1()
RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE epoch_record public.joint_authority_epoch_v1%ROWTYPE;
DECLARE event_record public.risk_budget_event_v1%ROWTYPE;
BEGIN
  SELECT * INTO epoch_record FROM public.joint_authority_epoch_v1
  WHERE authority_epoch_id=NEW.authority_epoch_id;
  SELECT * INTO event_record FROM public.risk_budget_event_v1
  WHERE event_id=NEW.event_id;
  IF epoch_record.authority_epoch_id IS NULL OR event_record.event_id IS NULL
     OR event_record.event_type<>'STATE_EVALUATION'
     OR event_record.environment<>'PAPER'
     OR event_record.deployment_id<>epoch_record.deployment_id
     OR event_record.event_at<>NEW.evaluation_as_of
     OR NEW.evaluation_as_of<epoch_record.first_required_cadence
     OR NEW.baseline_fingerprint<>epoch_record.baseline_fingerprint
     OR NEW.replay_cutover_fingerprint<>epoch_record.replay_cutover_fingerprint
     OR NEW.drawdown_generation_fingerprint<>
        epoch_record.drawdown_generation_fingerprint THEN
    RAISE EXCEPTION 'JOINT_AUTHORITY_EPOCH_V1_RISK_BUDGET_BINDING_INVALID';
  END IF;
  RETURN NEW;
END
$function$;

DO $block$
DECLARE table_name TEXT;
DECLARE trigger_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'joint_authority_activation_attempt_v1','joint_authority_epoch_v1',
    'joint_authority_epoch_selection_v1','risk_budget_authority_epoch_binding_v1'
  ] LOOP
    trigger_name := 'trg_' || table_name || '_append_only';
    IF NOT EXISTS (
      SELECT 1 FROM pg_trigger
      WHERE tgname=trigger_name AND tgrelid=('public.' || table_name)::regclass
    ) THEN
      EXECUTE format(
        'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON public.%I '
        'FOR EACH ROW EXECUTE FUNCTION public.reject_joint_authority_epoch_mutation_v1()',
        trigger_name,table_name
      );
    END IF;
  END LOOP;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE
    tgname='trg_joint_authority_activation_attempt_v1_validate') THEN
    CREATE TRIGGER trg_joint_authority_activation_attempt_v1_validate
    BEFORE INSERT ON public.joint_authority_activation_attempt_v1 FOR EACH ROW
    EXECUTE FUNCTION public.validate_joint_authority_attempt_v1();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE
    tgname='trg_joint_authority_epoch_v1_validate') THEN
    CREATE TRIGGER trg_joint_authority_epoch_v1_validate
    BEFORE INSERT ON public.joint_authority_epoch_v1 FOR EACH ROW
    EXECUTE FUNCTION public.validate_joint_authority_epoch_v1();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE
    tgname='trg_joint_authority_epoch_selection_v1_validate') THEN
    CREATE TRIGGER trg_joint_authority_epoch_selection_v1_validate
    BEFORE INSERT ON public.joint_authority_epoch_selection_v1 FOR EACH ROW
    EXECUTE FUNCTION public.validate_joint_authority_selection_v1();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE
    tgname='trg_risk_budget_authority_epoch_binding_v1_validate') THEN
    CREATE TRIGGER trg_risk_budget_authority_epoch_binding_v1_validate
    BEFORE INSERT ON public.risk_budget_authority_epoch_binding_v1 FOR EACH ROW
    EXECUTE FUNCTION public.validate_risk_budget_epoch_binding_v1();
  END IF;
END
$block$;

COMMENT ON TABLE public.joint_authority_epoch_v1 IS
  'Immutable B/R/D/C1 authority epoch; Q0 is bound by the first eligible Risk Budget binding.';
COMMENT ON TABLE public.joint_authority_activation_attempt_v1 IS
  'Immutable terminal activation attempts; retries append and reference a prior FAILED attempt.';
COMMENT ON TABLE public.risk_budget_authority_epoch_binding_v1 IS
  'Immutable same-epoch Q0 and later calibration-eligible Risk Budget evidence.';

COMMIT;

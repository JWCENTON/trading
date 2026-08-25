-- WALTRADE PAPER DRAWDOWN HISTORY AUTHORITY V1
-- Additive, immutable, forward-only authority. No historical backfill.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE IF NOT EXISTS public.paper_drawdown_history_activation_v1 (
  activation_id BIGSERIAL PRIMARY KEY,
  baseline_id BIGINT NOT NULL REFERENCES public.paper_equity_baseline_v2(baseline_id),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper')),
  activated_at TIMESTAMPTZ NOT NULL,
  activation_bucket_at TIMESTAMPTZ NOT NULL,
  baseline_activation_fingerprint TEXT NOT NULL CHECK (
    baseline_activation_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  activation_identity TEXT NOT NULL UNIQUE CHECK (activation_identity ~ '^[0-9a-f]{64}$'),
  activation_evidence_fingerprint TEXT NOT NULL CHECK (
    activation_evidence_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  activation_evidence JSONB NOT NULL CHECK (
    jsonb_typeof(activation_evidence)='object' AND activation_evidence<>'{}'::jsonb
  ),
  producer_identity TEXT NOT NULL CHECK (btrim(producer_identity)<>''),
  git_revision TEXT NOT NULL CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  contract_version TEXT NOT NULL CHECK (
    contract_version='PAPER_DRAWDOWN_HISTORY_AUTHORITY_V1'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (baseline_id, contract_version),
  CHECK (
    activated_at>=activation_bucket_at
    AND activated_at<activation_bucket_at+INTERVAL '15 minutes'
  )
);

CREATE TABLE IF NOT EXISTS public.paper_managed_equity_observation_v1 (
  observation_id BIGSERIAL PRIMARY KEY,
  activation_id BIGINT NOT NULL REFERENCES public.paper_drawdown_history_activation_v1(activation_id),
  baseline_id BIGINT NOT NULL REFERENCES public.paper_equity_baseline_v2(baseline_id),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper')),
  observed_at TIMESTAMPTZ NOT NULL,
  observation_bucket_at TIMESTAMPTZ NOT NULL,
  observation_trigger TEXT NOT NULL CHECK (
    observation_trigger IN ('BASELINE_ACTIVATION','CADENCE_15M','FINANCIAL_TRUTH_COMPLETE')
  ),
  trigger_reference TEXT NOT NULL CHECK (btrim(trigger_reference)<>''),
  observation_identity TEXT NOT NULL UNIQUE CHECK (observation_identity ~ '^[0-9a-f]{64}$'),
  managed_equity NUMERIC(38,18) NOT NULL,
  managed_equity_status TEXT NOT NULL CHECK (managed_equity_status='CANONICAL'),
  realized_pnl NUMERIC(38,18) NOT NULL,
  realized_pnl_status TEXT NOT NULL CHECK (realized_pnl_status='CANONICAL'),
  unrealized_pnl NUMERIC(38,18) NOT NULL,
  unrealized_pnl_status TEXT NOT NULL CHECK (unrealized_pnl_status='CANONICAL'),
  baseline_activation_fingerprint TEXT NOT NULL CHECK (
    baseline_activation_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  portfolio_state_fingerprint TEXT NOT NULL CHECK (
    portfolio_state_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  source_fingerprints JSONB NOT NULL CHECK (
    jsonb_typeof(source_fingerprints)='object' AND source_fingerprints<>'{}'::jsonb
  ),
  portfolio_state_evidence JSONB NOT NULL CHECK (
    jsonb_typeof(portfolio_state_evidence)='object' AND portfolio_state_evidence<>'{}'::jsonb
  ),
  evidence_fingerprint TEXT NOT NULL CHECK (evidence_fingerprint ~ '^[0-9a-f]{64}$'),
  history_evidence_status TEXT NOT NULL CHECK (history_evidence_status='CANONICAL'),
  producer_identity TEXT NOT NULL CHECK (btrim(producer_identity)<>''),
  git_revision TEXT NOT NULL CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  contract_version TEXT NOT NULL CHECK (
    contract_version='PAPER_DRAWDOWN_HISTORY_AUTHORITY_V1'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (observed_at>=observation_bucket_at)
);

CREATE INDEX IF NOT EXISTS ix_paper_drawdown_history_order_v1
  ON public.paper_managed_equity_observation_v1(
    activation_id,observed_at,observation_id
  );

CREATE OR REPLACE FUNCTION public.reject_paper_drawdown_history_mutation_v1()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  RAISE EXCEPTION 'PAPER_DRAWDOWN_HISTORY_AUTHORITY_V1_APPEND_ONLY';
END
$function$;

CREATE OR REPLACE FUNCTION public.validate_paper_drawdown_observation_v1()
RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE
  activation_record public.paper_drawdown_history_activation_v1%ROWTYPE;
  baseline_managed NUMERIC;
  baseline_unrealized NUMERIC;
BEGIN
  SELECT * INTO activation_record
  FROM public.paper_drawdown_history_activation_v1
  WHERE activation_id=NEW.activation_id;
  IF NOT FOUND
     OR activation_record.baseline_id<>NEW.baseline_id
     OR activation_record.deployment_id<>NEW.deployment_id
     OR activation_record.baseline_activation_fingerprint<>
        NEW.baseline_activation_fingerprint
     OR NEW.observed_at<activation_record.activated_at THEN
    RAISE EXCEPTION 'PAPER_DRAWDOWN_HISTORY_AUTHORITY_V1_FORWARD_BOUNDARY_INVALID';
  END IF;
  SELECT baseline_managed_equity,baseline_unrealized_pnl
    INTO baseline_managed,baseline_unrealized
  FROM public.paper_equity_baseline_v2
  WHERE baseline_id=NEW.baseline_id
    AND baseline_version='PAPER_EQUITY_BASELINE_V2'
    AND evidence_status='COMPLETE'
    AND activation_fingerprint=NEW.baseline_activation_fingerprint;
  IF NOT FOUND
     OR NEW.managed_equity<>
        baseline_managed+NEW.realized_pnl+NEW.unrealized_pnl-baseline_unrealized
     OR (NEW.portfolio_state_evidence->>'total_capital')::NUMERIC IS DISTINCT FROM
        NEW.managed_equity
     OR NEW.portfolio_state_evidence->>'total_capital_status' IS DISTINCT FROM 'CANONICAL'
     OR NEW.portfolio_state_evidence->>'realized_pnl_status' IS DISTINCT FROM 'CANONICAL'
     OR NEW.portfolio_state_evidence->>'unrealized_pnl_status' IS DISTINCT FROM 'CANONICAL' THEN
    RAISE EXCEPTION 'PAPER_DRAWDOWN_HISTORY_AUTHORITY_V1_CAPITAL_BASIS_INVALID';
  END IF;
  RETURN NEW;
END
$function$;

DO $block$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname='trg_paper_drawdown_activation_v1_append_only'
      AND tgrelid='public.paper_drawdown_history_activation_v1'::regclass
  ) THEN
    CREATE TRIGGER trg_paper_drawdown_activation_v1_append_only
    BEFORE UPDATE OR DELETE ON public.paper_drawdown_history_activation_v1
    FOR EACH ROW EXECUTE FUNCTION public.reject_paper_drawdown_history_mutation_v1();
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname='trg_paper_managed_equity_observation_v1_append_only'
      AND tgrelid='public.paper_managed_equity_observation_v1'::regclass
  ) THEN
    CREATE TRIGGER trg_paper_managed_equity_observation_v1_append_only
    BEFORE UPDATE OR DELETE ON public.paper_managed_equity_observation_v1
    FOR EACH ROW EXECUTE FUNCTION public.reject_paper_drawdown_history_mutation_v1();
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname='trg_paper_managed_equity_observation_v1_validate'
      AND tgrelid='public.paper_managed_equity_observation_v1'::regclass
  ) THEN
    CREATE TRIGGER trg_paper_managed_equity_observation_v1_validate
    BEFORE INSERT ON public.paper_managed_equity_observation_v1
    FOR EACH ROW EXECUTE FUNCTION public.validate_paper_drawdown_observation_v1();
  END IF;
END
$block$;

COMMENT ON TABLE public.paper_drawdown_history_activation_v1 IS
  'Immutable accepted forward cutover; never historical backfill.';
COMMENT ON TABLE public.paper_managed_equity_observation_v1 IS
  'Canonical PAPER managed-equity observations; daily snapshots are not an input.';

COMMIT;

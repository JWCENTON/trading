-- WALTRADE PAPER DRAWDOWN HISTORY AUTHORITY V1
-- Additive immutable activation generations and explicit supersession chain.
-- Existing activation/observation rows are adopted as generation 1 by metadata
-- defaults only; no observation or economic history is copied or rewritten.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE public.paper_drawdown_history_activation_v1
  ADD COLUMN IF NOT EXISTS generation INTEGER NOT NULL DEFAULT 1
  CHECK (generation > 0);

ALTER TABLE public.paper_managed_equity_observation_v1
  ADD COLUMN IF NOT EXISTS activation_generation INTEGER NOT NULL DEFAULT 1
  CHECK (activation_generation > 0);

DO $block$
DECLARE
  old_constraint TEXT;
BEGIN
  SELECT constraint_name INTO old_constraint
  FROM information_schema.table_constraints
  WHERE table_schema='public'
    AND table_name='paper_drawdown_history_activation_v1'
    AND constraint_type='UNIQUE'
    AND constraint_name<>'paper_drawdown_history_activation_v1_activation_identity_key'
    AND (
      SELECT array_agg(column_name::TEXT ORDER BY ordinal_position)
      FROM information_schema.key_column_usage key_column
      WHERE key_column.constraint_schema=table_constraints.constraint_schema
        AND key_column.constraint_name=table_constraints.constraint_name
    )=ARRAY['baseline_id','contract_version'];
  IF old_constraint IS NOT NULL THEN
    EXECUTE format(
      'ALTER TABLE public.paper_drawdown_history_activation_v1 DROP CONSTRAINT %I',
      old_constraint
    );
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname='uq_paper_drawdown_activation_generation_v1'
      AND conrelid='public.paper_drawdown_history_activation_v1'::regclass
  ) THEN
    ALTER TABLE public.paper_drawdown_history_activation_v1
      ADD CONSTRAINT uq_paper_drawdown_activation_generation_v1
      UNIQUE (baseline_id,contract_version,generation);
  END IF;
END
$block$;

CREATE TABLE IF NOT EXISTS public.paper_drawdown_history_generation_selection_v1 (
  selection_id BIGSERIAL PRIMARY KEY,
  activation_id BIGINT NOT NULL UNIQUE REFERENCES
    public.paper_drawdown_history_activation_v1(activation_id),
  baseline_id BIGINT NOT NULL REFERENCES public.paper_equity_baseline_v2(baseline_id),
  contract_version TEXT NOT NULL CHECK (
    contract_version='PAPER_DRAWDOWN_HISTORY_AUTHORITY_V1'
  ),
  generation INTEGER NOT NULL CHECK (generation > 0),
  previous_selection_id BIGINT UNIQUE REFERENCES
    public.paper_drawdown_history_generation_selection_v1(selection_id),
  previous_activation_id BIGINT REFERENCES
    public.paper_drawdown_history_activation_v1(activation_id),
  selected_at TIMESTAMPTZ NOT NULL,
  selection_reason TEXT NOT NULL CHECK (btrim(selection_reason)<>''),
  approval_evidence JSONB NOT NULL CHECK (
    jsonb_typeof(approval_evidence)='object' AND approval_evidence<>'{}'::jsonb
  ),
  selection_identity TEXT NOT NULL UNIQUE CHECK (
    selection_identity ~ '^[0-9a-f]{64}$'
  ),
  selection_evidence_fingerprint TEXT NOT NULL CHECK (
    selection_evidence_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  selection_evidence JSONB NOT NULL CHECK (
    jsonb_typeof(selection_evidence)='object' AND selection_evidence<>'{}'::jsonb
  ),
  producer_identity TEXT NOT NULL CHECK (btrim(producer_identity)<>''),
  git_revision TEXT NOT NULL CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_paper_drawdown_selection_generation_v1
    UNIQUE (baseline_id,contract_version,generation),
  CHECK (
    (generation=1 AND previous_selection_id IS NULL AND previous_activation_id IS NULL)
    OR
    (generation>1 AND previous_selection_id IS NOT NULL AND previous_activation_id IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS ix_paper_drawdown_observation_generation_v1
  ON public.paper_managed_equity_observation_v1(
    activation_id,activation_generation,observed_at,observation_id
  );

CREATE OR REPLACE FUNCTION public.validate_paper_drawdown_generation_selection_v1()
RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE
  activation_record public.paper_drawdown_history_activation_v1%ROWTYPE;
  previous_record public.paper_drawdown_history_generation_selection_v1%ROWTYPE;
BEGIN
  SELECT * INTO activation_record
  FROM public.paper_drawdown_history_activation_v1
  WHERE activation_id=NEW.activation_id;
  IF NOT FOUND
     OR activation_record.baseline_id<>NEW.baseline_id
     OR activation_record.contract_version<>NEW.contract_version
     OR activation_record.generation<>NEW.generation
     OR NEW.selected_at<activation_record.activated_at THEN
    RAISE EXCEPTION 'PAPER_DRAWDOWN_GENERATION_SELECTION_ACTIVATION_INVALID';
  END IF;
  IF NEW.generation=1 THEN
    IF NEW.previous_selection_id IS NOT NULL OR NEW.previous_activation_id IS NOT NULL THEN
      RAISE EXCEPTION 'PAPER_DRAWDOWN_GENERATION_SELECTION_CHAIN_INVALID';
    END IF;
  ELSE
    SELECT * INTO previous_record
    FROM public.paper_drawdown_history_generation_selection_v1
    WHERE selection_id=NEW.previous_selection_id;
    IF NOT FOUND
       OR previous_record.baseline_id<>NEW.baseline_id
       OR previous_record.contract_version<>NEW.contract_version
       OR previous_record.generation<>NEW.generation-1
       OR previous_record.activation_id<>NEW.previous_activation_id
       OR NEW.selected_at<previous_record.selected_at THEN
      RAISE EXCEPTION 'PAPER_DRAWDOWN_GENERATION_SELECTION_CHAIN_INVALID';
    END IF;
  END IF;
  RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION public.validate_paper_drawdown_observation_v1()
RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE
  activation_record public.paper_drawdown_history_activation_v1%ROWTYPE;
  baseline_managed NUMERIC;
  baseline_unrealized NUMERIC;
  canonical_calculated_managed_equity NUMERIC(38,18);
  canonical_source_managed_equity NUMERIC(38,18);
BEGIN
  SELECT * INTO activation_record
  FROM public.paper_drawdown_history_activation_v1
  WHERE activation_id=NEW.activation_id;
  IF NOT FOUND
     OR activation_record.baseline_id<>NEW.baseline_id
     OR activation_record.deployment_id<>NEW.deployment_id
     OR activation_record.generation<>NEW.activation_generation
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
  IF FOUND THEN
    canonical_calculated_managed_equity := round(
      baseline_managed
      +(NEW.portfolio_state_evidence->>'realized_pnl')::NUMERIC
      +(NEW.portfolio_state_evidence->>'unrealized_pnl')::NUMERIC
      -baseline_unrealized,
      18
    );
    canonical_source_managed_equity := round(
      (NEW.portfolio_state_evidence->>'total_capital')::NUMERIC,
      18
    );
  END IF;
  IF NOT FOUND
     OR canonical_calculated_managed_equity IS DISTINCT FROM NEW.managed_equity
     OR canonical_source_managed_equity IS DISTINCT FROM NEW.managed_equity
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
    WHERE tgname='trg_paper_drawdown_generation_selection_v1_validate'
      AND tgrelid='public.paper_drawdown_history_generation_selection_v1'::regclass
  ) THEN
    CREATE TRIGGER trg_paper_drawdown_generation_selection_v1_validate
    BEFORE INSERT ON public.paper_drawdown_history_generation_selection_v1
    FOR EACH ROW EXECUTE FUNCTION
      public.validate_paper_drawdown_generation_selection_v1();
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname='trg_paper_drawdown_generation_selection_v1_append_only'
      AND tgrelid='public.paper_drawdown_history_generation_selection_v1'::regclass
  ) THEN
    CREATE TRIGGER trg_paper_drawdown_generation_selection_v1_append_only
    BEFORE UPDATE OR DELETE ON public.paper_drawdown_history_generation_selection_v1
    FOR EACH ROW EXECUTE FUNCTION public.reject_paper_drawdown_history_mutation_v1();
  END IF;
END
$block$;

COMMENT ON COLUMN public.paper_drawdown_history_activation_v1.generation IS
  'Immutable activation generation; existing activation is generation 1.';
COMMENT ON COLUMN public.paper_managed_equity_observation_v1.activation_generation IS
  'Immutable owning activation generation; metadata adoption only, never backfill.';
COMMENT ON TABLE public.paper_drawdown_history_generation_selection_v1 IS
  'Append-only explicit generation-selection chain; sole chain head is active.';

COMMIT;

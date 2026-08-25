-- WALTRADE PAPER DRAWDOWN HISTORY AUTHORITY V1
-- Additive correction: make the immutable INSERT guard use the table's
-- canonical NUMERIC(38,18) boundary. No data mutation or historical backfill.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

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

COMMENT ON FUNCTION public.validate_paper_drawdown_observation_v1() IS
  'Exact PAPER capital-basis guard after canonical NUMERIC(38,18) rounding; raw source evidence remains immutable.';

COMMIT;

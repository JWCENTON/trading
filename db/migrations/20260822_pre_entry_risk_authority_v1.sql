BEGIN;

CREATE TABLE IF NOT EXISTS public.pre_entry_risk_event_v1 (
  event_id uuid PRIMARY KEY,
  pre_entry_risk_id uuid NOT NULL,
  event_sequence bigint NOT NULL CHECK (event_sequence > 0),
  source_event_identity text NOT NULL CHECK (btrim(source_event_identity) <> ''),
  environment text NOT NULL CHECK (environment IN ('PAPER','LIVE')),
  deployment_id text NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper','local-live','vps-live')),
  account_identity_fingerprint text NOT NULL CHECK (account_identity_fingerprint ~ '^[0-9a-f]{64}$'),
  decision_id text NOT NULL CHECK (btrim(decision_id) <> ''),
  commitment_id text NOT NULL CHECK (btrim(commitment_id) <> ''),
  reservation_id uuid NOT NULL,
  intent_id text,
  order_identity text,
  symbol text NOT NULL CHECK (symbol=upper(symbol) AND btrim(symbol)<>''),
  strategy text NOT NULL CHECK (strategy=upper(strategy) AND btrim(strategy)<>''),
  interval text NOT NULL CHECK (interval=lower(interval) AND btrim(interval)<>''),
  side text NOT NULL CHECK (side='LONG'),
  boundary_id uuid NOT NULL,
  boundary_policy_id text NOT NULL CHECK (btrim(boundary_policy_id)<>''),
  boundary_policy_version text NOT NULL CHECK (btrim(boundary_policy_version)<>''),
  boundary_policy_fingerprint text NOT NULL CHECK (boundary_policy_fingerprint ~ '^[0-9a-f]{64}$'),
  boundary_distance_pct numeric(20,12) NOT NULL CHECK (boundary_distance_pct>0 AND boundary_distance_pct<100),
  proposed_boundary_price numeric(38,18) NOT NULL CHECK (proposed_boundary_price>0),
  reference_price numeric(38,18) NOT NULL CHECK (reference_price>proposed_boundary_price),
  reference_price_timestamp timestamptz NOT NULL,
  reference_price_source text NOT NULL CHECK (reference_price_source='candles.close/FRESH_20_MINUTES'),
  reference_price_row_identity text NOT NULL CHECK (btrim(reference_price_row_identity)<>''),
  reference_price_fingerprint text NOT NULL CHECK (reference_price_fingerprint ~ '^[0-9a-f]{64}$'),
  proposed_quantity numeric(38,18) NOT NULL CHECK (proposed_quantity>0),
  quantity_source text NOT NULL CHECK (btrim(quantity_source)<>''),
  quantity_evidence_fingerprint text NOT NULL CHECK (quantity_evidence_fingerprint ~ '^[0-9a-f]{64}$'),
  exit_cost_snapshot_or_model_id text NOT NULL CHECK (btrim(exit_cost_snapshot_or_model_id)<>''),
  exit_cost_evidence_fingerprint text NOT NULL CHECK (exit_cost_evidence_fingerprint ~ '^[0-9a-f]{64}$'),
  canonical_exit_fee_rate numeric(20,12) NOT NULL CHECK (canonical_exit_fee_rate>=0 AND canonical_exit_fee_rate<=0.10),
  pre_entry_core_price_risk numeric(38,18) NOT NULL CHECK (pre_entry_core_price_risk>=0),
  pre_entry_exit_fee_estimate numeric(38,18) NOT NULL CHECK (pre_entry_exit_fee_estimate>=0),
  total_pre_entry_risk numeric(38,18) NOT NULL CHECK (total_pre_entry_risk=pre_entry_core_price_risk+pre_entry_exit_fee_estimate),
  original_quantity numeric(38,18) NOT NULL CHECK (original_quantity>0),
  transferred_quantity numeric(38,18) NOT NULL CHECK (transferred_quantity>=0),
  remaining_committed_quantity numeric(38,18) NOT NULL CHECK (remaining_committed_quantity>=0),
  released_quantity numeric(38,18) NOT NULL CHECK (released_quantity>=0),
  evidence_status text NOT NULL CHECK (evidence_status IN ('CANONICAL','MISSING_BOUNDARY_POLICY','MISSING_REFERENCE_PRICE','STALE_REFERENCE_PRICE','MISSING_PROPOSED_QUANTITY','MISSING_EXIT_COST_AUTHORITY','STALE_EXIT_COST_AUTHORITY','ACCOUNT_IDENTITY_MISMATCH','DEPLOYMENT_MISMATCH','INSTRUMENT_MISMATCH','INVALID_QUANTITY','INVALID_BOUNDARY','EVIDENCE_INCOMPLETE')),
  lifecycle_state text NOT NULL CHECK (lifecycle_state IN ('ACTIVE_COMMITTED','PARTIALLY_TRANSFERRED','REPLACED_BY_OPEN_RISK','RELEASED')),
  open_risk_position_id bigint,
  open_risk_boundary_id uuid,
  open_risk_evidence_fingerprint text CHECK (open_risk_evidence_fingerprint IS NULL OR open_risk_evidence_fingerprint ~ '^[0-9a-f]{64}$'),
  runtime_revision text NOT NULL CHECK (runtime_revision ~ '^[0-9a-f]{40}$'),
  effective_at timestamptz NOT NULL,
  source_authority text NOT NULL CHECK (btrim(source_authority)<>''),
  provenance jsonb NOT NULL CHECK (jsonb_typeof(provenance)='object'),
  evidence_fingerprint text NOT NULL CHECK (evidence_fingerprint ~ '^[0-9a-f]{64}$'),
  contract_version text NOT NULL CHECK (contract_version='PRE_ENTRY_RISK_AUTHORITY_V1'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(pre_entry_risk_id,event_sequence),
  UNIQUE(pre_entry_risk_id,source_event_identity),
  CHECK (original_quantity=transferred_quantity+remaining_committed_quantity+released_quantity),
  CHECK ((remaining_committed_quantity=0 AND total_pre_entry_risk=0) OR remaining_committed_quantity>0),
  CHECK ((open_risk_position_id IS NULL AND open_risk_boundary_id IS NULL AND open_risk_evidence_fingerprint IS NULL)
      OR (open_risk_position_id IS NOT NULL AND open_risk_boundary_id IS NOT NULL AND open_risk_evidence_fingerprint IS NOT NULL)),
  CHECK ((environment='PAPER' AND deployment_id IN ('local-paper','vps-paper')) OR
         (environment='LIVE' AND deployment_id IN ('local-live','vps-live')))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_pre_entry_risk_reservation_v1
ON public.pre_entry_risk_event_v1(reservation_id) WHERE event_sequence=1;

CREATE INDEX IF NOT EXISTS ix_pre_entry_risk_current_scope_v1
ON public.pre_entry_risk_event_v1(environment,deployment_id,account_identity_fingerprint,lifecycle_state);

CREATE OR REPLACE FUNCTION public.pre_entry_risk_event_v1_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE prev public.pre_entry_risk_event_v1%ROWTYPE;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended(NEW.pre_entry_risk_id::text,0));
  SELECT * INTO prev FROM public.pre_entry_risk_event_v1
   WHERE pre_entry_risk_id=NEW.pre_entry_risk_id
   ORDER BY event_sequence DESC LIMIT 1 FOR UPDATE;
  IF NOT FOUND THEN
    IF NEW.event_sequence<>1 OR NEW.lifecycle_state<>'ACTIVE_COMMITTED'
       OR NEW.evidence_status<>'CANONICAL' OR NEW.transferred_quantity<>0
       OR NEW.released_quantity<>0 THEN
      RAISE EXCEPTION 'PRE_ENTRY_RISK_CANONICAL_FREEZE_REQUIRED';
    END IF;
  ELSE
    -- Let INSERT ... ON CONFLICT(event_id) resolve a byte-identical replay.
    -- The advisory lock makes this safe under concurrent freeze attempts.
    IF NEW.event_id=prev.event_id
       AND NEW.evidence_fingerprint=prev.evidence_fingerprint THEN
      RETURN NEW;
    END IF;
    IF NEW.event_sequence<>prev.event_sequence+1 THEN
      RAISE EXCEPTION 'PRE_ENTRY_RISK_EVENT_SEQUENCE_INVALID';
    END IF;
    IF (NEW.environment,NEW.deployment_id,NEW.account_identity_fingerprint,
        NEW.decision_id,NEW.commitment_id,NEW.reservation_id,NEW.intent_id,
        NEW.order_identity,NEW.symbol,NEW.strategy,NEW.interval,NEW.side,
        NEW.boundary_id,NEW.boundary_policy_id,NEW.boundary_policy_version,
        NEW.boundary_policy_fingerprint,NEW.boundary_distance_pct,
        NEW.proposed_boundary_price,NEW.reference_price,
        NEW.reference_price_timestamp,NEW.reference_price_source,
        NEW.reference_price_row_identity,NEW.reference_price_fingerprint,
        NEW.proposed_quantity,NEW.quantity_source,
        NEW.quantity_evidence_fingerprint,NEW.exit_cost_snapshot_or_model_id,
        NEW.exit_cost_evidence_fingerprint,NEW.canonical_exit_fee_rate,
        NEW.original_quantity,NEW.runtime_revision,NEW.contract_version,
        NEW.evidence_status)
       IS DISTINCT FROM
       (prev.environment,prev.deployment_id,prev.account_identity_fingerprint,
        prev.decision_id,prev.commitment_id,prev.reservation_id,prev.intent_id,
        prev.order_identity,prev.symbol,prev.strategy,prev.interval,prev.side,
        prev.boundary_id,prev.boundary_policy_id,prev.boundary_policy_version,
        prev.boundary_policy_fingerprint,prev.boundary_distance_pct,
        prev.proposed_boundary_price,prev.reference_price,
        prev.reference_price_timestamp,prev.reference_price_source,
        prev.reference_price_row_identity,prev.reference_price_fingerprint,
        prev.proposed_quantity,prev.quantity_source,
        prev.quantity_evidence_fingerprint,prev.exit_cost_snapshot_or_model_id,
        prev.exit_cost_evidence_fingerprint,prev.canonical_exit_fee_rate,
        prev.original_quantity,prev.runtime_revision,prev.contract_version,
        prev.evidence_status) THEN
      RAISE EXCEPTION 'PRE_ENTRY_RISK_FROZEN_EVIDENCE_IMMUTABLE';
    END IF;
    IF NEW.transferred_quantity<prev.transferred_quantity
       OR NEW.released_quantity<prev.released_quantity
       OR NEW.remaining_committed_quantity>prev.remaining_committed_quantity THEN
      RAISE EXCEPTION 'PRE_ENTRY_RISK_ACCOUNTING_REGRESSION';
    END IF;
    IF prev.lifecycle_state IN ('REPLACED_BY_OPEN_RISK','RELEASED') THEN
      RAISE EXCEPTION 'PRE_ENTRY_RISK_TERMINAL_REACTIVATION_FORBIDDEN';
    END IF;
    IF NOT ((prev.lifecycle_state='ACTIVE_COMMITTED' AND NEW.lifecycle_state IN ('PARTIALLY_TRANSFERRED','REPLACED_BY_OPEN_RISK','RELEASED'))
         OR (prev.lifecycle_state='PARTIALLY_TRANSFERRED' AND NEW.lifecycle_state IN ('PARTIALLY_TRANSFERRED','REPLACED_BY_OPEN_RISK','RELEASED'))) THEN
      RAISE EXCEPTION 'PRE_ENTRY_RISK_STATE_TRANSITION_INVALID';
    END IF;
  END IF;
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS pre_entry_risk_event_v1_guard_trg ON public.pre_entry_risk_event_v1;
CREATE TRIGGER pre_entry_risk_event_v1_guard_trg BEFORE INSERT
ON public.pre_entry_risk_event_v1 FOR EACH ROW EXECUTE FUNCTION public.pre_entry_risk_event_v1_guard();

CREATE OR REPLACE FUNCTION public.pre_entry_risk_event_v1_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'PRE_ENTRY_RISK_APPEND_ONLY'; END $$;

DROP TRIGGER IF EXISTS pre_entry_risk_event_v1_append_only_trg ON public.pre_entry_risk_event_v1;
CREATE TRIGGER pre_entry_risk_event_v1_append_only_trg BEFORE UPDATE OR DELETE
ON public.pre_entry_risk_event_v1 FOR EACH ROW EXECUTE FUNCTION public.pre_entry_risk_event_v1_append_only();

CREATE OR REPLACE VIEW public.v_pre_entry_risk_current_v1 AS
SELECT DISTINCT ON (pre_entry_risk_id) * FROM public.pre_entry_risk_event_v1
ORDER BY pre_entry_risk_id,event_sequence DESC;

COMMIT;

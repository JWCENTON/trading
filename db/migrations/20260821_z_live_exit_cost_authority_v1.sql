BEGIN;

CREATE TABLE IF NOT EXISTS public.live_exit_cost_snapshot_v1 (
  exit_cost_snapshot_id uuid PRIMARY KEY,
  environment text NOT NULL CHECK (environment='LIVE'),
  deployment_id text NOT NULL CHECK (deployment_id IN ('local-live','vps-live')),
  account_identity_fingerprint text NOT NULL CHECK (account_identity_fingerprint ~ '^[0-9a-f]{64}$'),
  instrument_type text NOT NULL CHECK (instrument_type='SPOT'),
  symbol text NOT NULL CHECK (symbol=upper(symbol) AND btrim(symbol)<>''),
  fee_role text NOT NULL CHECK (fee_role IN ('TAKER','MAKER')),
  canonical_fee_rate numeric(38,18) NOT NULL CHECK (canonical_fee_rate>=0 AND canonical_fee_rate<=0.10),
  raw_fee_rate numeric(38,18) NOT NULL,
  raw_sign_semantics text NOT NULL CHECK (raw_sign_semantics='OKX_SIGNED_RATE_NEGATIVE_MEANS_COST'),
  rule_type text,
  account_level text,
  observed_at timestamptz NOT NULL,
  effective_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL CHECK (expires_at>effective_at),
  source text NOT NULL CHECK (source='OKX_API_V5_ACCOUNT_TRADE_FEE'),
  contract_version text NOT NULL CHECK (contract_version='LIVE_EXIT_COST_AUTHORITY_V1'),
  source_evidence_fingerprint text NOT NULL CHECK (source_evidence_fingerprint ~ '^[0-9a-f]{64}$'),
  snapshot_fingerprint text NOT NULL UNIQUE CHECK (snapshot_fingerprint ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS ix_live_exit_cost_snapshot_scope_v1
ON public.live_exit_cost_snapshot_v1(
  deployment_id,account_identity_fingerprint,instrument_type,symbol,fee_role,
  effective_at DESC
);

CREATE TABLE IF NOT EXISTS public.live_position_exit_cost_link_v1 (
  link_id uuid PRIMARY KEY,
  position_id bigint NOT NULL,
  boundary_id uuid NOT NULL,
  exit_cost_snapshot_id uuid NOT NULL REFERENCES public.live_exit_cost_snapshot_v1(exit_cost_snapshot_id),
  effective_at timestamptz NOT NULL,
  link_fingerprint text NOT NULL UNIQUE CHECK (link_fingerprint ~ '^[0-9a-f]{64}$'),
  contract_version text NOT NULL CHECK (contract_version='LIVE_EXIT_COST_AUTHORITY_V1'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(position_id,boundary_id)
);

CREATE OR REPLACE FUNCTION public.live_exit_cost_link_v1_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE boundary_row public.position_risk_boundary_event_v1%ROWTYPE;
DECLARE snapshot_row public.live_exit_cost_snapshot_v1%ROWTYPE;
BEGIN
  SELECT * INTO boundary_row FROM public.v_position_risk_boundary_current_v1
   WHERE boundary_id=NEW.boundary_id AND position_id=NEW.position_id
     AND state IN ('BOUNDARY_ACTIVATED','BOUNDARY_REVISED_ENTRY_BASIS');
  IF NOT FOUND OR boundary_row.environment<>'LIVE' THEN
    RAISE EXCEPTION 'LIVE_EXIT_COST_BOUNDARY_LINKAGE_INVALID';
  END IF;
  SELECT * INTO STRICT snapshot_row FROM public.live_exit_cost_snapshot_v1
   WHERE exit_cost_snapshot_id=NEW.exit_cost_snapshot_id;
  IF (snapshot_row.deployment_id,snapshot_row.account_identity_fingerprint,
      snapshot_row.symbol,snapshot_row.instrument_type,snapshot_row.fee_role)
     IS DISTINCT FROM
     (boundary_row.deployment_id,boundary_row.account_identity_fingerprint,
      boundary_row.symbol,'SPOT','TAKER') THEN
    RAISE EXCEPTION 'LIVE_EXIT_COST_SCOPE_MISMATCH';
  END IF;
  IF snapshot_row.effective_at>NEW.effective_at OR snapshot_row.expires_at<=NEW.effective_at THEN
    RAISE EXCEPTION 'LIVE_EXIT_COST_SNAPSHOT_NOT_FRESH_AT_FREEZE';
  END IF;
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS live_exit_cost_link_v1_guard_trg ON public.live_position_exit_cost_link_v1;
CREATE TRIGGER live_exit_cost_link_v1_guard_trg BEFORE INSERT
ON public.live_position_exit_cost_link_v1 FOR EACH ROW EXECUTE FUNCTION public.live_exit_cost_link_v1_guard();

CREATE OR REPLACE FUNCTION public.live_exit_cost_v1_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'LIVE_EXIT_COST_AUTHORITY_APPEND_ONLY'; END $$;

DROP TRIGGER IF EXISTS live_exit_cost_snapshot_v1_append_only_trg ON public.live_exit_cost_snapshot_v1;
CREATE TRIGGER live_exit_cost_snapshot_v1_append_only_trg BEFORE UPDATE OR DELETE
ON public.live_exit_cost_snapshot_v1 FOR EACH ROW EXECUTE FUNCTION public.live_exit_cost_v1_append_only();

DROP TRIGGER IF EXISTS live_exit_cost_link_v1_append_only_trg ON public.live_position_exit_cost_link_v1;
CREATE TRIGGER live_exit_cost_link_v1_append_only_trg BEFORE UPDATE OR DELETE
ON public.live_position_exit_cost_link_v1 FOR EACH ROW EXECUTE FUNCTION public.live_exit_cost_v1_append_only();

COMMIT;

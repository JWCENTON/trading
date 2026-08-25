-- WALTRADE PAPER PORTFOLIO REPLAY CUTOVER V1
-- Additive, immutable, forward-only inventory replay authority. No backfill.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE IF NOT EXISTS public.paper_portfolio_replay_cutover_v1 (
  cutover_id BIGSERIAL PRIMARY KEY,
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper')),
  cutover_at TIMESTAMPTZ NOT NULL,
  git_revision TEXT NOT NULL CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  contract_version TEXT NOT NULL CHECK (
    contract_version='PAPER_PORTFOLIO_REPLAY_CUTOVER_V1'
  ),
  portfolio_state_fingerprint TEXT NOT NULL CHECK (
    portfolio_state_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  cutover_fingerprint TEXT NOT NULL UNIQUE CHECK (
    cutover_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  inventory_position_count INTEGER NOT NULL CHECK (inventory_position_count>=0),
  source_evidence JSONB NOT NULL CHECK (
    jsonb_typeof(source_evidence)='object' AND source_evidence<>'{}'::jsonb
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (deployment_id,contract_version)
);

CREATE TABLE IF NOT EXISTS public.paper_portfolio_replay_cutover_position_v1 (
  cutover_id BIGINT NOT NULL REFERENCES public.paper_portfolio_replay_cutover_v1(cutover_id),
  position_id BIGINT NOT NULL REFERENCES public.positions(id),
  symbol TEXT NOT NULL CHECK (btrim(symbol)<>''),
  strategy TEXT NOT NULL CHECK (btrim(strategy)<>''),
  interval TEXT NOT NULL CHECK (btrim(interval)<>''),
  side TEXT NOT NULL CHECK (upper(side) IN ('BUY','LONG')),
  remaining_inventory_qty NUMERIC(38,18) NOT NULL CHECK (remaining_inventory_qty>0),
  entry_basis_price NUMERIC(38,18) NOT NULL CHECK (entry_basis_price>0),
  inventory_evidence_status TEXT NOT NULL CHECK (inventory_evidence_status='COMPLETE'),
  entry_lineage JSONB NOT NULL CHECK (
    jsonb_typeof(entry_lineage)='object' AND entry_lineage<>'{}'::jsonb
  ),
  boundary_id UUID NOT NULL,
  boundary_policy_fingerprint TEXT NOT NULL CHECK (
    boundary_policy_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  boundary_effective_at TIMESTAMPTZ NOT NULL,
  risk_owner TEXT NOT NULL CHECK (risk_owner='POSITION_OPEN_RISK'),
  open_risk_evidence_fingerprint TEXT NOT NULL CHECK (
    open_risk_evidence_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  position_evidence_fingerprint TEXT NOT NULL UNIQUE CHECK (
    position_evidence_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (cutover_id,position_id)
);

CREATE OR REPLACE FUNCTION public.reject_paper_portfolio_replay_cutover_mutation_v1()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  RAISE EXCEPTION 'PAPER_PORTFOLIO_REPLAY_CUTOVER_V1_APPEND_ONLY';
END
$function$;

DO $block$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname='trg_paper_portfolio_replay_cutover_v1_append_only'
      AND tgrelid='public.paper_portfolio_replay_cutover_v1'::regclass
  ) THEN
    CREATE TRIGGER trg_paper_portfolio_replay_cutover_v1_append_only
    BEFORE UPDATE OR DELETE ON public.paper_portfolio_replay_cutover_v1
    FOR EACH ROW EXECUTE FUNCTION public.reject_paper_portfolio_replay_cutover_mutation_v1();
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname='trg_paper_portfolio_replay_cutover_position_v1_append_only'
      AND tgrelid='public.paper_portfolio_replay_cutover_position_v1'::regclass
  ) THEN
    CREATE TRIGGER trg_paper_portfolio_replay_cutover_position_v1_append_only
    BEFORE UPDATE OR DELETE ON public.paper_portfolio_replay_cutover_position_v1
    FOR EACH ROW EXECUTE FUNCTION public.reject_paper_portfolio_replay_cutover_mutation_v1();
  END IF;
END
$block$;

COMMENT ON TABLE public.paper_portfolio_replay_cutover_v1 IS
  'Immutable forward-only PAPER inventory replay boundary; not an economic baseline.';
COMMENT ON TABLE public.paper_portfolio_replay_cutover_position_v1 IS
  'Exact canonical inventory, boundary and open-risk ownership frozen at replay cutover.';

COMMIT;

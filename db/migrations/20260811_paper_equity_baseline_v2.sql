-- WALTRADE PAPER EQUITY BASELINE V2
-- Explicit PAPER-only cutover. No historical economics or FT are modified.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE IF NOT EXISTS public.paper_equity_baseline_v2 (
  baseline_id BIGSERIAL PRIMARY KEY,
  deployment_id TEXT NOT NULL CHECK (
    deployment_id IN ('local-paper','vps-paper')
  ),
  baseline_version TEXT NOT NULL CHECK (
    baseline_version='PAPER_EQUITY_BASELINE_V2'
  ),
  baseline_timestamp TIMESTAMPTZ NOT NULL,
  cutover_boundary TIMESTAMPTZ NOT NULL,
  baseline_account_total NUMERIC(38,18) NOT NULL CHECK (
    baseline_account_total >= 0
  ),
  baseline_managed_equity NUMERIC(38,18) NOT NULL CHECK (
    baseline_managed_equity >= 0
  ),
  baseline_external_manual NUMERIC(38,18) NOT NULL CHECK (
    baseline_external_manual >= 0
  ),
  baseline_available NUMERIC(38,18) NOT NULL,
  baseline_inventory_value NUMERIC(38,18) NOT NULL CHECK (
    baseline_inventory_value >= 0
  ),
  baseline_realized_net_pnl NUMERIC(38,18) NOT NULL,
  baseline_unrealized_pnl NUMERIC(38,18) NOT NULL,
  baseline_fees NUMERIC(38,18) NOT NULL CHECK (baseline_fees >= 0),
  baseline_open_positions INTEGER NOT NULL CHECK (
    baseline_open_positions >= 0
  ),
  frozen_pre_baseline_unresolved_count INTEGER NOT NULL CHECK (
    frozen_pre_baseline_unresolved_count >= 0
  ),
  evidence_status TEXT NOT NULL CHECK (evidence_status='COMPLETE'),
  source_authority TEXT NOT NULL CHECK (
    source_authority='CANONICAL_PAPER_ACCOUNT_READ_MODEL_V1'
  ),
  approved_by TEXT NOT NULL CHECK (btrim(approved_by) <> ''),
  approval_provenance JSONB NOT NULL CHECK (
    jsonb_typeof(approval_provenance)='object'
    AND approval_provenance <> '{}'::jsonb
  ),
  activation_fingerprint TEXT NOT NULL CHECK (
    activation_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT uq_paper_equity_baseline_v2_deployment_version
    UNIQUE (deployment_id,baseline_version),
  CONSTRAINT uq_paper_equity_baseline_v2_fingerprint
    UNIQUE (activation_fingerprint),
  CONSTRAINT ck_paper_equity_baseline_v2_boundary
    CHECK (cutover_boundary=baseline_timestamp),
  CONSTRAINT ck_paper_equity_baseline_v2_account_equation
    CHECK (
      baseline_managed_equity =
        baseline_account_total-baseline_external_manual
    )
);

CREATE TABLE IF NOT EXISTS public.paper_equity_frozen_outcome_v2 (
  frozen_id BIGSERIAL PRIMARY KEY,
  baseline_id BIGINT NOT NULL REFERENCES paper_equity_baseline_v2(baseline_id),
  deployment_id TEXT NOT NULL CHECK (
    deployment_id IN ('local-paper','vps-paper')
  ),
  position_id BIGINT NOT NULL,
  classification TEXT NOT NULL CHECK (
    classification='PRE_BASELINE_FROZEN'
  ),
  original_outcome_status TEXT NOT NULL CHECK (
    original_outcome_status='UNRESOLVED'
  ),
  original_evidence_status TEXT NOT NULL CHECK (
    original_evidence_status='INCOMPLETE'
  ),
  original_blocking_reasons JSONB NOT NULL,
  immutable_economic_snapshot JSONB NOT NULL,
  original_financial_truth_rows INTEGER NOT NULL CHECK (
    original_financial_truth_rows >= 0
  ),
  frozen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT uq_paper_equity_frozen_outcome_v2
    UNIQUE (baseline_id,position_id)
);

CREATE INDEX IF NOT EXISTS ix_paper_equity_frozen_outcome_v2_deployment
  ON paper_equity_frozen_outcome_v2(deployment_id,position_id);

CREATE OR REPLACE FUNCTION reject_paper_equity_baseline_v2_mutation()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  RAISE EXCEPTION 'PAPER_EQUITY_BASELINE_V2_APPEND_ONLY';
END;
$function$;

DROP TRIGGER IF EXISTS trg_paper_equity_baseline_v2_append_only
  ON paper_equity_baseline_v2;
CREATE TRIGGER trg_paper_equity_baseline_v2_append_only
BEFORE UPDATE OR DELETE ON paper_equity_baseline_v2
FOR EACH ROW EXECUTE FUNCTION reject_paper_equity_baseline_v2_mutation();

DROP TRIGGER IF EXISTS trg_paper_equity_frozen_outcome_v2_append_only
  ON paper_equity_frozen_outcome_v2;
CREATE TRIGGER trg_paper_equity_frozen_outcome_v2_append_only
BEFORE UPDATE OR DELETE ON paper_equity_frozen_outcome_v2
FOR EACH ROW EXECUTE FUNCTION reject_paper_equity_baseline_v2_mutation();

COMMENT ON TABLE public.paper_equity_baseline_v2 IS
  'Product-Owner-approved PAPER-only Equity cutover from authoritative current account state; append-only.';
COMMENT ON TABLE public.paper_equity_frozen_outcome_v2 IS
  'Immutable audit cohort of unresolved pre-baseline outcomes; no position, PnL, fill, order, or FT mutation.';

COMMIT;

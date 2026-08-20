-- WALTRADE LIVE MANAGED CAPITAL AUTHORITY V1
-- Additive schema only.  No baseline, owner flow or observation is backfilled.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE IF NOT EXISTS public.live_managed_capital_baseline_v1 (
  baseline_id BIGSERIAL PRIMARY KEY,
  environment TEXT NOT NULL CHECK (environment='LIVE'),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-live','vps-live')),
  contract_version TEXT NOT NULL CHECK (contract_version='LIVE_MANAGED_CAPITAL_AUTHORITY_V1'),
  account_identity_fingerprint TEXT NOT NULL CHECK (account_identity_fingerprint ~ '^[0-9a-f]{64}$'),
  account_scope TEXT NOT NULL CHECK (account_scope='DEDICATED_WALTRADE_MANAGED_ACCOUNT'),
  accepted_at TIMESTAMPTZ NOT NULL,
  managed_asset_scope JSONB NOT NULL CHECK (jsonb_typeof(managed_asset_scope)='array'),
  raw_balance_snapshot JSONB NOT NULL CHECK (jsonb_typeof(raw_balance_snapshot)='object'),
  valuation_snapshot JSONB NOT NULL CHECK (jsonb_typeof(valuation_snapshot)='object'),
  baseline_managed_equity NUMERIC(38,18) NOT NULL CHECK (baseline_managed_equity >= 0),
  raw_okx_usdc_avail_bal NUMERIC(38,18) NOT NULL CHECK (raw_okx_usdc_avail_bal >= 0),
  available_capital NUMERIC(38,18),
  available_capital_status TEXT NOT NULL CHECK (available_capital_status IN ('CANONICAL','INCOMPLETE')),
  reserved_capital NUMERIC(38,18),
  reserved_capital_status TEXT NOT NULL CHECK (reserved_capital_status IN ('CANONICAL','NOT_YET_CANONICAL')),
  ownership_reconciliation_status TEXT NOT NULL CHECK (ownership_reconciliation_status='CANONICAL'),
  runtime_revision TEXT NOT NULL CHECK (runtime_revision ~ '^[0-9a-f]{40}$'),
  approved_by TEXT NOT NULL CHECK (btrim(approved_by)<>''),
  approval_reference JSONB NOT NULL CHECK (jsonb_typeof(approval_reference)='object' AND approval_reference<>'{}'::jsonb),
  activation_fingerprint TEXT NOT NULL CHECK (activation_fingerprint ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(deployment_id,contract_version),
  UNIQUE(activation_fingerprint),
  CHECK ((available_capital_status='CANONICAL')=(available_capital IS NOT NULL)),
  CHECK ((reserved_capital_status='CANONICAL')=(reserved_capital IS NOT NULL)),
  CHECK ((environment='LIVE' AND deployment_id IN ('local-live','vps-live')))
);

CREATE TABLE IF NOT EXISTS public.owner_capital_flow_v1 (
  flow_id BIGSERIAL PRIMARY KEY,
  environment TEXT NOT NULL CHECK (environment='LIVE'),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-live','vps-live')),
  account_identity_fingerprint TEXT NOT NULL CHECK (account_identity_fingerprint ~ '^[0-9a-f]{64}$'),
  source_event_identity TEXT NOT NULL CHECK (btrim(source_event_identity)<>''),
  asset TEXT NOT NULL CHECK (btrim(asset)<>''),
  quantity NUMERIC(38,18) NOT NULL CHECK (quantity>0),
  value_usdc NUMERIC(38,18) NOT NULL CHECK (value_usdc>0),
  event_at TIMESTAMPTZ NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN ('DEPOSIT','WITHDRAWAL','TRANSFER_IN','TRANSFER_OUT')),
  source TEXT NOT NULL CHECK (btrim(source)<>''),
  evidence_status TEXT NOT NULL CHECK (evidence_status='COMPLETE'),
  raw_provenance_reference JSONB NOT NULL CHECK (jsonb_typeof(raw_provenance_reference)='object' AND raw_provenance_reference<>'{}'::jsonb),
  valuation_provenance JSONB NOT NULL CHECK (jsonb_typeof(valuation_provenance)='object' AND valuation_provenance<>'{}'::jsonb),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(environment,deployment_id,account_identity_fingerprint,source,source_event_identity),
  CHECK ((environment='LIVE' AND deployment_id IN ('local-live','vps-live')))
);

CREATE TABLE IF NOT EXISTS public.live_managed_equity_observation_v1 (
  observation_id BIGSERIAL PRIMARY KEY,
  baseline_id BIGINT NOT NULL REFERENCES live_managed_capital_baseline_v1(baseline_id),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-live','vps-live')),
  observed_at TIMESTAMPTZ NOT NULL,
  raw_managed_equity NUMERIC(38,18) NOT NULL CHECK (raw_managed_equity>=0),
  cumulative_flow_in_usdc NUMERIC(38,18) NOT NULL CHECK (cumulative_flow_in_usdc>=0),
  cumulative_flow_out_usdc NUMERIC(38,18) NOT NULL CHECK (cumulative_flow_out_usdc>=0),
  flow_adjusted_equity NUMERIC(38,18) NOT NULL,
  evidence_fingerprint TEXT NOT NULL CHECK (evidence_fingerprint ~ '^[0-9a-f]{64}$'),
  evidence_status TEXT NOT NULL CHECK (evidence_status='COMPLETE'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(baseline_id,observed_at),
  UNIQUE(evidence_fingerprint),
  CHECK (flow_adjusted_equity=raw_managed_equity-cumulative_flow_in_usdc+cumulative_flow_out_usdc)
);

CREATE INDEX IF NOT EXISTS ix_owner_capital_flow_v1_cutover
  ON owner_capital_flow_v1(deployment_id,event_at,flow_id);
CREATE INDEX IF NOT EXISTS ix_live_managed_equity_observation_v1_peak
  ON live_managed_equity_observation_v1(baseline_id,flow_adjusted_equity DESC);

CREATE OR REPLACE FUNCTION reject_live_managed_capital_v1_mutation()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  RAISE EXCEPTION 'LIVE_MANAGED_CAPITAL_AUTHORITY_V1_APPEND_ONLY';
END;
$function$;

DROP TRIGGER IF EXISTS trg_live_managed_capital_baseline_v1_append_only ON live_managed_capital_baseline_v1;
CREATE TRIGGER trg_live_managed_capital_baseline_v1_append_only BEFORE UPDATE OR DELETE
ON live_managed_capital_baseline_v1 FOR EACH ROW EXECUTE FUNCTION reject_live_managed_capital_v1_mutation();
DROP TRIGGER IF EXISTS trg_owner_capital_flow_v1_append_only ON owner_capital_flow_v1;
CREATE TRIGGER trg_owner_capital_flow_v1_append_only BEFORE UPDATE OR DELETE
ON owner_capital_flow_v1 FOR EACH ROW EXECUTE FUNCTION reject_live_managed_capital_v1_mutation();
DROP TRIGGER IF EXISTS trg_live_managed_equity_observation_v1_append_only ON live_managed_equity_observation_v1;
CREATE TRIGGER trg_live_managed_equity_observation_v1_append_only BEFORE UPDATE OR DELETE
ON live_managed_equity_observation_v1 FOR EACH ROW EXECUTE FUNCTION reject_live_managed_capital_v1_mutation();

COMMENT ON TABLE live_managed_capital_baseline_v1 IS 'Explicit Product-Owner-approved immutable LIVE managed-capital cutover; never auto-created.';
COMMENT ON TABLE owner_capital_flow_v1 IS 'Canonical append-only owner deposit/withdrawal/transfer evidence; never trading PnL.';
COMMENT ON TABLE live_managed_equity_observation_v1 IS 'Append-only flow-adjusted LIVE managed-equity observations for canonical drawdown.';
COMMIT;

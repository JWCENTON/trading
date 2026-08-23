-- WALTRADE OWNER CAPITAL FLOW CANONICAL SYNC AUTHORITY V1
-- Additive, append-only authority. No owner flows or watermarks are backfilled.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE IF NOT EXISTS public.owner_capital_flow_sync_run_v1 (
  run_id UUID PRIMARY KEY,
  environment TEXT NOT NULL CHECK (environment='LIVE'),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-live','vps-live')),
  account_identity_fingerprint TEXT NOT NULL
    CHECK (account_identity_fingerprint ~ '^[0-9a-f]{64}$'),
  source TEXT NOT NULL CHECK (source='TRADING_ACCOUNT_BILLS'),
  contract_version TEXT NOT NULL
    CHECK (contract_version='OWNER_CAPITAL_FLOW_CANONICAL_SYNC_AUTHORITY_V1'),
  range_from TIMESTAMPTZ NOT NULL,
  source_cutoff TIMESTAMPTZ NOT NULL,
  overlap_from TIMESTAMPTZ NOT NULL,
  sync_through TIMESTAMPTZ,
  source_endpoint TEXT NOT NULL CHECK (source_endpoint IN (
    '/api/v5/account/bills','/api/v5/account/bills-archive'
  )),
  terminal_cursor TEXT,
  last_source_event_id TEXT,
  page_count INTEGER NOT NULL CHECK (page_count>=0),
  source_event_count INTEGER NOT NULL CHECK (source_event_count>=0),
  canonical_event_count INTEGER NOT NULL CHECK (canonical_event_count>=0),
  late_event_count INTEGER NOT NULL CHECK (late_event_count>=0),
  started_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL,
  producer_identity TEXT NOT NULL CHECK (btrim(producer_identity)<>''),
  git_revision TEXT NOT NULL CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  source_fingerprint TEXT NOT NULL CHECK (source_fingerprint ~ '^[0-9a-f]{64}$'),
  status TEXT NOT NULL CHECK (status IN (
    'CANONICAL','NO_SYNC','PARTIAL_SYNC','STALE_SYNC',
    'SOURCE_UNAVAILABLE','PAGINATION_INCOMPLETE',
    'ACCOUNT_IDENTITY_MISMATCH','UNSUPPORTED_ASSET',
    'LATE_EVENT_RECONCILIATION_REQUIRED'
  )),
  error_code TEXT,
  evidence JSONB NOT NULL
    CHECK (jsonb_typeof(evidence)='object' AND evidence<>'{}'::jsonb),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (range_from<=source_cutoff),
  CHECK (overlap_from>=range_from AND overlap_from<=source_cutoff),
  CHECK ((status='CANONICAL')=(sync_through IS NOT NULL)),
  CHECK (sync_through IS NULL OR sync_through=source_cutoff),
  CHECK (completed_at>=started_at)
);

CREATE INDEX IF NOT EXISTS ix_owner_capital_flow_sync_run_latest_v1
  ON owner_capital_flow_sync_run_v1(
    environment,deployment_id,account_identity_fingerprint,source,
    completed_at DESC,created_at DESC
  );

CREATE TABLE IF NOT EXISTS public.owner_capital_flow_reconciliation_v1 (
  reconciliation_evidence_id BIGSERIAL PRIMARY KEY,
  reconciliation_key TEXT NOT NULL CHECK (reconciliation_key ~ '^[0-9a-f]{64}$'),
  environment TEXT NOT NULL CHECK (environment='LIVE'),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-live','vps-live')),
  account_identity_fingerprint TEXT NOT NULL
    CHECK (account_identity_fingerprint ~ '^[0-9a-f]{64}$'),
  source TEXT NOT NULL CHECK (source='TRADING_ACCOUNT_BILLS'),
  source_event_identity TEXT NOT NULL CHECK (btrim(source_event_identity)<>''),
  event_at TIMESTAMPTZ NOT NULL,
  prior_sync_through TIMESTAMPTZ NOT NULL,
  affected_from TIMESTAMPTZ NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('REQUIRED','RESOLVED')),
  source_run_id UUID NOT NULL REFERENCES owner_capital_flow_sync_run_v1(run_id),
  evidence JSONB NOT NULL
    CHECK (jsonb_typeof(evidence)='object' AND evidence<>'{}'::jsonb),
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(reconciliation_key,state),
  CHECK (affected_from=event_at),
  CHECK (event_at<prior_sync_through)
);

CREATE INDEX IF NOT EXISTS ix_owner_capital_flow_reconciliation_scope_v1
  ON owner_capital_flow_reconciliation_v1(
    environment,deployment_id,account_identity_fingerprint,affected_from
  );

CREATE OR REPLACE FUNCTION reject_owner_capital_flow_sync_v1_mutation()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  RAISE EXCEPTION 'OWNER_CAPITAL_FLOW_CANONICAL_SYNC_AUTHORITY_V1_APPEND_ONLY';
END;
$function$;

DROP TRIGGER IF EXISTS trg_owner_capital_flow_sync_run_v1_append_only
  ON owner_capital_flow_sync_run_v1;
CREATE TRIGGER trg_owner_capital_flow_sync_run_v1_append_only
BEFORE UPDATE OR DELETE ON owner_capital_flow_sync_run_v1
FOR EACH ROW EXECUTE FUNCTION reject_owner_capital_flow_sync_v1_mutation();

DROP TRIGGER IF EXISTS trg_owner_capital_flow_reconciliation_v1_append_only
  ON owner_capital_flow_reconciliation_v1;
CREATE TRIGGER trg_owner_capital_flow_reconciliation_v1_append_only
BEFORE UPDATE OR DELETE ON owner_capital_flow_reconciliation_v1
FOR EACH ROW EXECUTE FUNCTION reject_owner_capital_flow_sync_v1_mutation();

CREATE OR REPLACE VIEW public.v_owner_capital_flow_sync_authority_v1 AS
SELECT DISTINCT ON (
  environment,deployment_id,account_identity_fingerprint,source
)
  run_id,environment,deployment_id,account_identity_fingerprint,source,
  contract_version,range_from,source_cutoff,overlap_from,sync_through,
  source_endpoint,terminal_cursor,last_source_event_id,page_count,
  source_event_count,canonical_event_count,late_event_count,started_at,
  completed_at,producer_identity,git_revision,source_fingerprint,status,
  error_code,evidence,created_at
FROM owner_capital_flow_sync_run_v1
ORDER BY environment,deployment_id,account_identity_fingerprint,source,
         completed_at DESC,created_at DESC,run_id DESC;

CREATE OR REPLACE VIEW public.v_owner_capital_flow_reconciliation_current_v1 AS
SELECT DISTINCT ON (reconciliation_key)
  reconciliation_evidence_id,reconciliation_key,environment,deployment_id,
  account_identity_fingerprint,source,source_event_identity,event_at,
  prior_sync_through,affected_from,state,source_run_id,evidence,recorded_at
FROM owner_capital_flow_reconciliation_v1
ORDER BY reconciliation_key,recorded_at DESC,reconciliation_evidence_id DESC;

CREATE OR REPLACE VIEW public.v_live_managed_equity_observation_flow_authority_v1 AS
SELECT
  observation.*,
  CASE WHEN EXISTS (
    SELECT 1
    FROM v_owner_capital_flow_reconciliation_current_v1 reconciliation
    WHERE reconciliation.environment=baseline.environment
      AND reconciliation.deployment_id=baseline.deployment_id
      AND reconciliation.account_identity_fingerprint=
          baseline.account_identity_fingerprint
      AND reconciliation.state='REQUIRED'
      AND reconciliation.affected_from<=observation.observed_at
  ) THEN 'LATE_EVENT_RECONCILIATION_REQUIRED'
  WHEN authority.run_id IS NULL THEN 'NO_SYNC'
  WHEN authority.status<>'CANONICAL' THEN authority.status
  WHEN authority.sync_through<observation.observed_at THEN 'STALE_SYNC'
  ELSE observation.evidence_status END AS flow_history_status
FROM live_managed_equity_observation_v1 observation
JOIN live_managed_capital_baseline_v1 baseline
  ON baseline.baseline_id=observation.baseline_id
LEFT JOIN v_owner_capital_flow_sync_authority_v1 authority
  ON authority.environment=baseline.environment
 AND authority.deployment_id=baseline.deployment_id
 AND authority.account_identity_fingerprint=baseline.account_identity_fingerprint
 AND authority.source='TRADING_ACCOUNT_BILLS';

COMMENT ON TABLE owner_capital_flow_sync_run_v1 IS
  'Append-only exhaustive Trading Account bills sync and canonical watermark evidence.';
COMMENT ON TABLE owner_capital_flow_reconciliation_v1 IS
  'Append-only late owner-flow invalidation and resolution authority.';
COMMENT ON VIEW v_owner_capital_flow_sync_authority_v1 IS
  'Latest required-source owner-flow sync status; never infers zero-flow completeness.';
COMMENT ON VIEW v_live_managed_equity_observation_flow_authority_v1 IS
  'Read-only invalidation projection for observations affected by unresolved late flows.';

COMMIT;

-- WALTRADE RISK BUDGET AUTHORITY V1
-- Additive append-only READ_ONLY/SHADOW evidence. No historical backfill.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE IF NOT EXISTS public.risk_budget_event_v1 (
  event_id UUID PRIMARY KEY,
  event_type TEXT NOT NULL CHECK (event_type IN (
    'STATE_EVALUATION','PRE_ENTRY_GATE_DECISION'
  )),
  event_identity TEXT NOT NULL CHECK (btrim(event_identity)<>''),
  environment TEXT NOT NULL CHECK (environment IN ('PAPER','LIVE')),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN (
    'local-paper','vps-paper','local-live','vps-live'
  )),
  account_identity_fingerprint TEXT NOT NULL
    CHECK (account_identity_fingerprint ~ '^[0-9a-f]{64}$'),
  event_at TIMESTAMPTZ NOT NULL,
  policy_version TEXT NOT NULL CHECK (btrim(policy_version)<>''),
  policy_fingerprint TEXT NOT NULL CHECK (policy_fingerprint ~ '^[0-9a-f]{64}$'),
  authority_status TEXT NOT NULL CHECK (authority_status IN (
    'CANONICAL','MISSING_POLICY','INCOMPLETE_PORTFOLIO_STATE',
    'INCOMPLETE_DRAWDOWN_HISTORY','INCOMPLETE_OPEN_RISK',
    'INCOMPLETE_PRE_ENTRY_RISK','ACCOUNT_IDENTITY_MISMATCH',
    'SOURCE_FINGERPRINT_MISMATCH','STALE_AUTHORITY',
    'RISK_CAPACITY_EXHAUSTED'
  )),
  policy_state TEXT CHECK (policy_state IS NULL OR policy_state IN (
    'NORMAL','REDUCED','NO_NEW_RISK','PAUSED'
  )),
  total_capital NUMERIC(38,18),
  open_risk NUMERIC(38,18),
  pre_entry_committed_risk NUMERIC(38,18),
  used_risk NUMERIC(38,18),
  current_drawdown_abs NUMERIC(38,18),
  current_drawdown_pct NUMERIC(38,18),
  max_drawdown_abs NUMERIC(38,18),
  max_drawdown_pct NUMERIC(38,18),
  recovery_status TEXT,
  drawdown_history_status TEXT NOT NULL,
  total_risk_capacity NUMERIC(38,18),
  available_risk_capacity NUMERIC(38,18),
  candidate_pre_entry_risk NUMERIC(38,18),
  candidate_evidence_fingerprint TEXT CHECK (
    candidate_evidence_fingerprint IS NULL OR
    candidate_evidence_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  advisory_result TEXT CHECK (advisory_result IS NULL OR advisory_result IN (
    'ALLOW','REDUCE','BLOCK_NEW_RISK'
  )),
  reason_codes JSONB NOT NULL CHECK (jsonb_typeof(reason_codes)='array'),
  source_fingerprints JSONB NOT NULL CHECK (
    jsonb_typeof(source_fingerprints)='object' AND
    source_fingerprints<>'{}'::jsonb
  ),
  evidence JSONB NOT NULL CHECK (
    jsonb_typeof(evidence)='object' AND evidence<>'{}'::jsonb
  ),
  producer_identity TEXT NOT NULL CHECK (btrim(producer_identity)<>''),
  git_revision TEXT NOT NULL CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  contract_version TEXT NOT NULL CHECK (
    contract_version='RISK_BUDGET_AUTHORITY_V1'
  ),
  event_fingerprint TEXT NOT NULL CHECK (event_fingerprint ~ '^[0-9a-f]{64}$'),
  shadow_only BOOLEAN NOT NULL DEFAULT TRUE CHECK (shadow_only),
  paper_controlled_influence_ready BOOLEAN NOT NULL DEFAULT FALSE
    CHECK (NOT paper_controlled_influence_ready),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(environment,deployment_id,account_identity_fingerprint,
         event_type,event_identity),
  CHECK ((environment='PAPER' AND deployment_id IN ('local-paper','vps-paper')) OR
         (environment='LIVE' AND deployment_id IN ('local-live','vps-live'))),
  CHECK (total_capital IS NULL OR total_capital>=0),
  CHECK (open_risk IS NULL OR open_risk>=0),
  CHECK (pre_entry_committed_risk IS NULL OR pre_entry_committed_risk>=0),
  CHECK (used_risk IS NULL OR used_risk>=0),
  CHECK (total_risk_capacity IS NULL OR total_risk_capacity>=0),
  CHECK (available_risk_capacity IS NULL OR available_risk_capacity>=0),
  CHECK (candidate_pre_entry_risk IS NULL OR candidate_pre_entry_risk>=0),
  CHECK ((open_risk IS NULL OR pre_entry_committed_risk IS NULL OR used_risk IS NULL)
         OR used_risk=open_risk+pre_entry_committed_risk),
  CHECK ((total_risk_capacity IS NULL OR used_risk IS NULL OR
          available_risk_capacity IS NULL)
         OR available_risk_capacity=greatest(0,total_risk_capacity-used_risk)),
  CHECK (
    (event_type='STATE_EVALUATION' AND candidate_pre_entry_risk IS NULL
      AND candidate_evidence_fingerprint IS NULL AND advisory_result IS NULL)
    OR
    (event_type='PRE_ENTRY_GATE_DECISION' AND candidate_pre_entry_risk IS NOT NULL
      AND candidate_evidence_fingerprint IS NOT NULL
      AND advisory_result IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS ix_risk_budget_event_scope_v1
ON public.risk_budget_event_v1(
  environment,deployment_id,account_identity_fingerprint,event_at DESC,created_at DESC
);

CREATE OR REPLACE FUNCTION public.reject_risk_budget_event_v1_mutation()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  RAISE EXCEPTION 'RISK_BUDGET_AUTHORITY_V1_APPEND_ONLY';
END;
$function$;

DROP TRIGGER IF EXISTS trg_risk_budget_event_v1_append_only
ON public.risk_budget_event_v1;
CREATE TRIGGER trg_risk_budget_event_v1_append_only
BEFORE UPDATE OR DELETE ON public.risk_budget_event_v1
FOR EACH ROW EXECUTE FUNCTION public.reject_risk_budget_event_v1_mutation();

CREATE OR REPLACE VIEW public.v_risk_budget_current_v1 AS
SELECT DISTINCT ON (environment,deployment_id,account_identity_fingerprint)
  *
FROM public.risk_budget_event_v1
WHERE event_type='STATE_EVALUATION'
ORDER BY environment,deployment_id,account_identity_fingerprint,
         event_at DESC,created_at DESC,event_id DESC;

COMMENT ON TABLE public.risk_budget_event_v1 IS
  'Append-only READ_ONLY/SHADOW Risk Budget state and advisory gate evidence; never execution influence.';
COMMENT ON VIEW public.v_risk_budget_current_v1 IS
  'Latest shadow state evaluation per exact environment/deployment/account scope.';

COMMIT;

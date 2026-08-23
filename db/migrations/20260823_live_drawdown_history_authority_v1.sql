-- WALTRADE LIVE DRAWDOWN HISTORY AUTHORITY V1
-- Additive extension of the existing observation authority. No backfill.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE public.live_managed_equity_observation_v1
  ADD COLUMN IF NOT EXISTS environment TEXT,
  ADD COLUMN IF NOT EXISTS account_identity_fingerprint TEXT,
  ADD COLUMN IF NOT EXISTS observation_bucket_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS observation_trigger TEXT,
  ADD COLUMN IF NOT EXISTS trigger_reference TEXT,
  ADD COLUMN IF NOT EXISTS observation_identity TEXT,
  ADD COLUMN IF NOT EXISTS managed_equity_status TEXT,
  ADD COLUMN IF NOT EXISTS realized_pnl NUMERIC(38,18),
  ADD COLUMN IF NOT EXISTS realized_pnl_status TEXT,
  ADD COLUMN IF NOT EXISTS unrealized_pnl NUMERIC(38,18),
  ADD COLUMN IF NOT EXISTS unrealized_pnl_status TEXT,
  ADD COLUMN IF NOT EXISTS deployed_capital NUMERIC(38,18),
  ADD COLUMN IF NOT EXISTS deployed_capital_status TEXT,
  ADD COLUMN IF NOT EXISTS reserved_capital NUMERIC(38,18),
  ADD COLUMN IF NOT EXISTS reserved_capital_status TEXT,
  ADD COLUMN IF NOT EXISTS available_capital NUMERIC(38,18),
  ADD COLUMN IF NOT EXISTS available_capital_status TEXT,
  ADD COLUMN IF NOT EXISTS flow_history_status TEXT,
  ADD COLUMN IF NOT EXISTS flow_sync_through TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS baseline_activation_fingerprint TEXT,
  ADD COLUMN IF NOT EXISTS portfolio_state_fingerprint TEXT,
  ADD COLUMN IF NOT EXISTS managed_capital_source_fingerprint TEXT,
  ADD COLUMN IF NOT EXISTS owner_flow_source_fingerprint TEXT,
  ADD COLUMN IF NOT EXISTS source_fingerprints JSONB,
  ADD COLUMN IF NOT EXISTS portfolio_state_evidence JSONB,
  ADD COLUMN IF NOT EXISTS managed_capital_evidence JSONB,
  ADD COLUMN IF NOT EXISTS history_evidence_status TEXT,
  ADD COLUMN IF NOT EXISTS contract_version TEXT,
  ADD COLUMN IF NOT EXISTS producer_identity TEXT,
  ADD COLUMN IF NOT EXISTS git_revision TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS ux_live_drawdown_observation_identity_v1
  ON public.live_managed_equity_observation_v1(observation_identity)
  WHERE observation_identity IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_live_drawdown_history_order_v1
  ON public.live_managed_equity_observation_v1(
    baseline_id,observation_bucket_at,observed_at,observation_id
  ) WHERE contract_version='LIVE_DRAWDOWN_HISTORY_AUTHORITY_V1';

DO $block$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname='ck_live_drawdown_history_v1_canonical_evidence'
      AND conrelid='public.live_managed_equity_observation_v1'::regclass
  ) THEN
    ALTER TABLE public.live_managed_equity_observation_v1
      ADD CONSTRAINT ck_live_drawdown_history_v1_canonical_evidence CHECK (
        contract_version IS DISTINCT FROM 'LIVE_DRAWDOWN_HISTORY_AUTHORITY_V1'
        OR (
          environment='LIVE'
          AND deployment_id IN ('local-live','vps-live')
          AND account_identity_fingerprint ~ '^[0-9a-f]{64}$'
          AND observation_identity ~ '^[0-9a-f]{64}$'
          AND observation_trigger IN (
            'CADENCE_15M','OWNER_CAPITAL_FLOW','FINANCIAL_TRUTH_COMPLETE'
          )
          AND managed_equity_status='CANONICAL'
          AND realized_pnl_status='CANONICAL'
          AND unrealized_pnl_status='CANONICAL'
          AND deployed_capital_status='CANONICAL'
          AND reserved_capital_status='CANONICAL'
          AND available_capital_status='CANONICAL'
          AND flow_history_status='CANONICAL'
          AND history_evidence_status='CANONICAL'
          AND flow_sync_through>=observed_at
          AND baseline_activation_fingerprint ~ '^[0-9a-f]{64}$'
          AND portfolio_state_fingerprint ~ '^[0-9a-f]{64}$'
          AND managed_capital_source_fingerprint ~ '^[0-9a-f]{64}$'
          AND owner_flow_source_fingerprint ~ '^[0-9a-f]{64}$'
          AND jsonb_typeof(source_fingerprints)='object'
          AND source_fingerprints<>'{}'::jsonb
          AND jsonb_typeof(portfolio_state_evidence)='object'
          AND portfolio_state_evidence<>'{}'::jsonb
          AND jsonb_typeof(managed_capital_evidence)='object'
          AND managed_capital_evidence<>'{}'::jsonb
          AND btrim(producer_identity)<>''
          AND git_revision ~ '^[0-9a-f]{40}$'
          AND flow_adjusted_equity=
            raw_managed_equity-cumulative_flow_in_usdc+cumulative_flow_out_usdc
        )
      ) NOT VALID;
  END IF;
END
$block$;

CREATE OR REPLACE VIEW public.v_live_drawdown_history_observation_v1 AS
SELECT observation.*,
  COALESCE(
    (reemission.correction->>'cumulative_flow_in_usdc')::NUMERIC,
    observation.cumulative_flow_in_usdc
  ) AS effective_cumulative_flow_in_usdc,
  COALESCE(
    (reemission.correction->>'cumulative_flow_out_usdc')::NUMERIC,
    observation.cumulative_flow_out_usdc
  ) AS effective_cumulative_flow_out_usdc,
  COALESCE(
    (reemission.correction->>'flow_adjusted_equity')::NUMERIC,
    observation.flow_adjusted_equity
  ) AS effective_flow_adjusted_equity,
  COALESCE(
    reemission.correction->>'owner_flow_source_fingerprint',
    observation.owner_flow_source_fingerprint
  ) AS effective_owner_flow_source_fingerprint,
  COALESCE(
    reemission.correction->>'evidence_fingerprint',
    observation.evidence_fingerprint
  ) AS effective_evidence_fingerprint,
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM owner_capital_flow_reconciliation_v1 required
      WHERE required.environment='LIVE'
        AND required.deployment_id=observation.deployment_id
        AND required.account_identity_fingerprint=
            observation.account_identity_fingerprint
        AND required.state='REQUIRED'
        AND required.affected_from<=observation.observed_at
        AND NOT EXISTS (
          SELECT 1
          FROM owner_capital_flow_reconciliation_v1 resolved
          WHERE resolved.reconciliation_key=required.reconciliation_key
            AND resolved.state='RESOLVED'
            AND resolved.evidence->'reemitted_observations'
                  ? observation.observation_identity
        )
    ) THEN 'INCOMPLETE_CAPITAL_FLOW'
    ELSE observation.history_evidence_status
  END AS effective_history_status
FROM public.live_managed_equity_observation_v1 observation
LEFT JOIN LATERAL (
  SELECT resolved.evidence->'reemitted_observations'
           ->observation.observation_identity AS correction
  FROM owner_capital_flow_reconciliation_v1 required
  JOIN owner_capital_flow_reconciliation_v1 resolved
    ON resolved.reconciliation_key=required.reconciliation_key
   AND resolved.state='RESOLVED'
  WHERE required.state='REQUIRED'
    AND required.environment='LIVE'
    AND required.deployment_id=observation.deployment_id
    AND required.account_identity_fingerprint=
        observation.account_identity_fingerprint
    AND required.affected_from<=observation.observed_at
    AND resolved.evidence->'reemitted_observations'
          ? observation.observation_identity
  ORDER BY resolved.recorded_at DESC,resolved.reconciliation_evidence_id DESC
  LIMIT 1
) reemission ON TRUE
WHERE observation.contract_version='LIVE_DRAWDOWN_HISTORY_AUTHORITY_V1';

COMMENT ON VIEW public.v_live_drawdown_history_observation_v1 IS
  'Forward-only drawdown observations with append-only late-flow invalidation and exact resolution re-emission evidence.';

COMMIT;

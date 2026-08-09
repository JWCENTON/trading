-- WALTRADE EQUITY CURVE BASELINE V1
-- Additive, forward-only daily measurements. No historical backfill.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE IF NOT EXISTS public.equity_daily_snapshot_v1 (
  id BIGSERIAL PRIMARY KEY,
  snapshot_date DATE NOT NULL,
  deployment_id TEXT NOT NULL CHECK (deployment_id IN (
    'local-paper','local-live','vps-paper','vps-live'
  )),
  trading_mode TEXT NOT NULL CHECK (trading_mode IN ('PAPER','LIVE')),
  account_total_value_usdc NUMERIC(38,18) NOT NULL CHECK (account_total_value_usdc >= 0),
  external_manual_value_usdc NUMERIC(38,18),
  waltrade_managed_equity_usdc NUMERIC(38,18),
  available_usdc NUMERIC(38,18) NOT NULL,
  bot_inventory_value_usdc NUMERIC(38,18) NOT NULL,
  realized_net_pnl_usdc NUMERIC(38,18),
  unrealized_pnl_usdc NUMERIC(38,18),
  fees_usdc NUMERIC(38,18),
  open_positions INTEGER NOT NULL CHECK (open_positions >= 0),
  evidence_status TEXT NOT NULL CHECK (evidence_status IN ('COMPLETE','INCOMPLETE')),
  source_timestamp TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT uq_equity_daily_snapshot_v1_deployment_day
    UNIQUE (deployment_id, snapshot_date),
  CONSTRAINT ck_equity_daily_snapshot_v1_mode_deployment CHECK (
    (trading_mode='PAPER' AND deployment_id IN ('local-paper','vps-paper')) OR
    (trading_mode='LIVE' AND deployment_id IN ('local-live','vps-live'))
  ),
  CONSTRAINT ck_equity_daily_snapshot_v1_fail_closed CHECK (
    (evidence_status='COMPLETE' AND external_manual_value_usdc IS NOT NULL
      AND waltrade_managed_equity_usdc IS NOT NULL) OR
    (evidence_status='INCOMPLETE' AND waltrade_managed_equity_usdc IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS ix_equity_daily_snapshot_v1_history
  ON public.equity_daily_snapshot_v1(deployment_id, snapshot_date DESC);

COMMENT ON TABLE public.equity_daily_snapshot_v1 IS
  'Forward-only canonical daily equity measurements; measurement-only V1.';

COMMIT;

BEGIN;

CREATE TABLE IF NOT EXISTS runtime_contract_adoption_v1 (
  adoption_id BIGSERIAL PRIMARY KEY,
  contract_name TEXT NOT NULL,
  environment TEXT NOT NULL CHECK (environment IN ('live', 'paper')),
  deployment_id TEXT NOT NULL,
  adopted_at TIMESTAMPTZ NOT NULL,
  git_revision TEXT NOT NULL,
  migration_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (contract_name, environment, deployment_id)
);

COMMENT ON TABLE runtime_contract_adoption_v1 IS
  'Explicit deployment boundary. Migrations never auto-adopt legacy rows.';

CREATE TABLE IF NOT EXISTS exchange_fill_ingestion_state_v2 (
  ingestion_id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  account_identity_key TEXT NOT NULL,
  symbol TEXT NOT NULL,
  trade_id TEXT NOT NULL,
  order_id TEXT NOT NULL,
  side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  source_fingerprint TEXT NOT NULL,
  applied_fingerprint TEXT,
  applied_at TIMESTAMPTZ,
  application_status TEXT NOT NULL CHECK (
    application_status IN (
      'NEW',
      'DUPLICATE',
      'CORRECTION_PENDING',
      'CORRECTION_APPLIED',
      'AMBIGUOUS',
      'REJECTED'
    )
  ),
  correction_revision INTEGER NOT NULL DEFAULT 0,
  authoritative_payload JSONB NOT NULL,
  last_decision TEXT NOT NULL,
  UNIQUE (source, account_identity_key, symbol, trade_id)
);

CREATE INDEX IF NOT EXISTS ix_exchange_fill_ingestion_change_v2
  ON exchange_fill_ingestion_state_v2 (
    application_status, source, symbol, last_seen_at
  );

COMMENT ON TABLE exchange_fill_ingestion_state_v2 IS
  'Stable fill identity and authoritative change ledger for replay-safe C2.2.1 mutation.';

COMMIT;

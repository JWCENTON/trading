BEGIN;

ALTER TABLE exchange_fill_ingestion_state_v2
  DROP CONSTRAINT IF EXISTS exchange_fill_ingestion_state_v2_application_status_check;

ALTER TABLE exchange_fill_ingestion_state_v2
  ADD CONSTRAINT exchange_fill_ingestion_state_v2_application_status_check
  CHECK (application_status IN (
    'NEW','DUPLICATE','CORRECTION_PENDING','CORRECTION_APPLIED','AMBIGUOUS',
    'REJECTED','OBSERVED_NOT_APPLIED','APPLIED','TRUE_DUPLICATE_APPLIED',
    'IDEMPOTENCY_CONFLICT','EXTERNAL_OR_MANUAL_UNLINKED',
    'BLOCKED_MISSING_CONTEXT'
  )),
  ADD COLUMN IF NOT EXISTS local_fill_id BIGINT
    REFERENCES binance_order_fills(id) ON DELETE RESTRICT,
  ADD COLUMN IF NOT EXISTS linked_position_id BIGINT
    REFERENCES positions(id) ON DELETE RESTRICT,
  ADD COLUMN IF NOT EXISTS ownership_classification TEXT,
  ADD COLUMN IF NOT EXISTS classification_payload JSONB NOT NULL
    DEFAULT '{}'::JSONB;

CREATE TABLE IF NOT EXISTS legacy_repair_provenance_v1 (
  provenance_id BIGSERIAL PRIMARY KEY,
  incident_type TEXT NOT NULL,
  incident_identity TEXT NOT NULL,
  evidence_kind TEXT NOT NULL,
  provenance_source TEXT NOT NULL CHECK (provenance_source IN (
    'STORED_IMMUTABLE_SNAPSHOT','EXCHANGE_PAYLOAD',
    'CANONICAL_SYMBOL_RESOLVER','DEPLOYMENT_ACCOUNT_SNAPSHOT','UNKNOWN'
  )),
  immutable_payload JSONB NOT NULL,
  semantic_fingerprint TEXT NOT NULL,
  captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (incident_type,incident_identity,evidence_kind,semantic_fingerprint)
);

CREATE TABLE IF NOT EXISTS legacy_repair_audit_v1 (
  audit_id BIGSERIAL PRIMARY KEY,
  incident_type TEXT NOT NULL,
  incident_identity TEXT NOT NULL,
  semantic_fingerprint TEXT NOT NULL,
  action_status TEXT NOT NULL,
  raw_inventory_delta NUMERIC,
  normalized_inventory_qty NUMERIC,
  precision_status TEXT,
  precision_source TEXT,
  normalization_reason TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::JSONB,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (incident_type,incident_identity)
);

COMMENT ON TABLE legacy_repair_provenance_v1 IS
  'Immutable enrichment evidence; historical exchange fill payloads are never rewritten.';
COMMENT ON TABLE legacy_repair_audit_v1 IS
  'Explicit-ID, semantic-CAS audit for bounded legacy recovery.';

COMMIT;

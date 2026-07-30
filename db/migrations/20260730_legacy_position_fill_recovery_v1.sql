-- WALTRADE LEGACY RECOVERY ROLLOUT CONTRACT V2
-- Contract checksum is the SHA-256 of the canonical programmatic manifest.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $$
DECLARE
  expected_checksum CONSTANT text :=
    'dba35a4c037064823f90dd9aee9da4862e5ff37f525545b668b6d9ee8857ee65';
  found_checksum text;
BEGIN
  IF to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
    RAISE EXCEPTION 'LEGACY_RECOVERY_LEDGER_MISSING';
  END IF;
  SELECT checksum_sha256 INTO found_checksum
  FROM schema_migration_ledger_v1
  WHERE migration_id='20260730_legacy_position_fill_recovery_v1.sql'
  ORDER BY applied_at DESC LIMIT 1;
  IF found_checksum IS NOT NULL AND found_checksum <> expected_checksum THEN
    RAISE EXCEPTION 'LEGACY_RECOVERY_MIGRATION_CHECKSUM_CONFLICT';
  END IF;
END $$;

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
  ADD COLUMN IF NOT EXISTS local_fill_id BIGINT,
  ADD COLUMN IF NOT EXISTS linked_position_id BIGINT,
  ADD COLUMN IF NOT EXISTS ownership_classification TEXT,
  ADD COLUMN IF NOT EXISTS classification_payload JSONB NOT NULL DEFAULT '{}'::JSONB;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_exchange_fill_ingestion_local_fill') THEN
    ALTER TABLE exchange_fill_ingestion_state_v2
      ADD CONSTRAINT fk_exchange_fill_ingestion_local_fill
      FOREIGN KEY(local_fill_id) REFERENCES binance_order_fills(id) ON DELETE RESTRICT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_exchange_fill_ingestion_position') THEN
    ALTER TABLE exchange_fill_ingestion_state_v2
      ADD CONSTRAINT fk_exchange_fill_ingestion_position
      FOREIGN KEY(linked_position_id) REFERENCES positions(id) ON DELETE RESTRICT;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS legacy_repair_provenance_v1 (
  provenance_id BIGSERIAL PRIMARY KEY,
  evidence_source TEXT NOT NULL,
  source_identity TEXT NOT NULL,
  source_fingerprint TEXT NOT NULL CHECK (source_fingerprint ~ '^[0-9a-f]{64}$'),
  instrument_identity TEXT,
  account_provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  deployment_provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  fee_evidence JSONB NOT NULL DEFAULT '{}'::JSONB,
  valuation_evidence JSONB NOT NULL DEFAULT '{}'::JSONB,
  immutable_payload JSONB NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT ux_legacy_repair_provenance_source_identity
    UNIQUE(evidence_source,source_identity)
);

CREATE TABLE IF NOT EXISTS legacy_repair_audit_v1 (
  audit_id BIGSERIAL PRIMARY KEY,
  incident_type TEXT NOT NULL,
  incident_identity TEXT NOT NULL,
  operation_type TEXT NOT NULL,
  planner_version TEXT NOT NULL,
  writer_version TEXT,
  semantic_fingerprint_before TEXT,
  semantic_fingerprint_expected TEXT,
  semantic_fingerprint_after TEXT,
  plan_status TEXT NOT NULL,
  execution_status TEXT NOT NULL,
  invocation_identity TEXT NOT NULL,
  requested_at TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  actor_source TEXT NOT NULL,
  blocking_reasons JSONB NOT NULL DEFAULT '[]'::JSONB,
  eligible_actions JSONB NOT NULL DEFAULT '[]'::JSONB,
  executed_actions JSONB NOT NULL DEFAULT '[]'::JSONB,
  expected_changes JSONB NOT NULL DEFAULT '[]'::JSONB,
  actual_changes JSONB NOT NULL DEFAULT '[]'::JSONB,
  post_state_invariants JSONB NOT NULL DEFAULT '[]'::JSONB,
  error_code TEXT,
  error_detail TEXT,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT ux_legacy_repair_audit_invocation UNIQUE(invocation_identity)
);

CREATE INDEX IF NOT EXISTS ix_legacy_repair_audit_incident_history
  ON legacy_repair_audit_v1(incident_type,incident_identity,recorded_at DESC,audit_id DESC);
CREATE INDEX IF NOT EXISTS ix_legacy_repair_audit_semantic_expected
  ON legacy_repair_audit_v1(semantic_fingerprint_expected);
CREATE INDEX IF NOT EXISTS ix_legacy_repair_provenance_fingerprint
  ON legacy_repair_provenance_v1(source_fingerprint);
CREATE INDEX IF NOT EXISTS ix_legacy_repair_provenance_instrument_observed
  ON legacy_repair_provenance_v1(instrument_identity,observed_at DESC);
CREATE INDEX IF NOT EXISTS ix_exchange_fill_ingestion_recovery_lookup
  ON exchange_fill_ingestion_state_v2(source,symbol,order_id,trade_id);
CREATE INDEX IF NOT EXISTS ix_exchange_fill_ingestion_application
  ON exchange_fill_ingestion_state_v2(application_status,applied_fingerprint);

CREATE OR REPLACE FUNCTION prevent_legacy_recovery_history_mutation_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='trg_legacy_repair_audit_append_only') THEN
    CREATE TRIGGER trg_legacy_repair_audit_append_only
    BEFORE UPDATE OR DELETE ON legacy_repair_audit_v1
    FOR EACH ROW EXECUTE FUNCTION prevent_legacy_recovery_history_mutation_v1();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='trg_legacy_repair_provenance_immutable') THEN
    CREATE TRIGGER trg_legacy_repair_provenance_immutable
    BEFORE UPDATE OR DELETE ON legacy_repair_provenance_v1
    FOR EACH ROW EXECUTE FUNCTION prevent_legacy_recovery_history_mutation_v1();
  END IF;
END $$;

INSERT INTO schema_migration_ledger_v1(
  migration_id,checksum_sha256,environment,deployment_id,database_name,
  applied_by,status,success,execution_duration_ms,git_sha,
  schema_baseline_version
)
SELECT
  '20260730_legacy_position_fill_recovery_v1.sql',
  'dba35a4c037064823f90dd9aee9da4862e5ff37f525545b668b6d9ee8857ee65',
  CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
  'LEGACY_RECOVERY_V2',current_database(),'operator-migration','APPLIED',true,0,
  'e3feea37d3115615849f30a8a14d4e1cc70be23d',
  'LEGACY_RECOVERY_SCHEMA_V2'
WHERE NOT EXISTS (
  SELECT 1 FROM schema_migration_ledger_v1
  WHERE migration_id='20260730_legacy_position_fill_recovery_v1.sql'
);

COMMIT;

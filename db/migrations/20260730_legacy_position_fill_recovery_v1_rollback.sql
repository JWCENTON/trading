BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $$
BEGIN
  IF to_regclass('public.legacy_repair_audit_v1') IS NULL
     AND to_regclass('public.legacy_repair_provenance_v1') IS NULL THEN
    RETURN;
  END IF;
  IF to_regclass('public.legacy_repair_audit_v1') IS NULL
     OR to_regclass('public.legacy_repair_provenance_v1') IS NULL THEN
    RAISE EXCEPTION 'LEGACY_RECOVERY_ROLLBACK_PARTIAL_SCHEMA';
  END IF;
  IF EXISTS (SELECT 1 FROM legacy_repair_audit_v1)
     OR EXISTS (SELECT 1 FROM legacy_repair_provenance_v1) THEN
    RAISE EXCEPTION 'LEGACY_RECOVERY_ROLLBACK_BLOCKED_HISTORY_EXISTS';
  END IF;
END $$;

DROP TABLE IF EXISTS legacy_repair_audit_v1;
DROP TABLE IF EXISTS legacy_repair_provenance_v1;
DROP FUNCTION IF EXISTS prevent_legacy_recovery_history_mutation_v1();
DROP INDEX IF EXISTS ix_exchange_fill_ingestion_recovery_lookup;
DROP INDEX IF EXISTS ix_exchange_fill_ingestion_application;

ALTER TABLE exchange_fill_ingestion_state_v2
  DROP CONSTRAINT IF EXISTS exchange_fill_ingestion_state_v2_application_status_check,
  DROP COLUMN IF EXISTS local_fill_id,
  DROP COLUMN IF EXISTS linked_position_id,
  DROP COLUMN IF EXISTS ownership_classification,
  DROP COLUMN IF EXISTS classification_payload;
ALTER TABLE exchange_fill_ingestion_state_v2
  ADD CONSTRAINT exchange_fill_ingestion_state_v2_application_status_check
  CHECK (application_status IN (
    'NEW','DUPLICATE','CORRECTION_PENDING','CORRECTION_APPLIED',
    'AMBIGUOUS','REJECTED'
  ));

-- The canonical migration ledger is append-only. Rollback is represented by
-- schema absence; its APPLIED row is intentionally retained as history.
COMMIT;

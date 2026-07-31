-- WALTRADE LEI1C formal rollback. Immutable evidence or application decisions
-- make schema removal unsafe and therefore fail closed.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Acquire both ledger locks before taking the emptiness snapshot. Lock the
-- application table first so an in-flight application writer (which then
-- locks its evidence row) cannot deadlock with rollback.
DO $$
BEGIN
  IF to_regclass('public.live_entry_fill_evidence_v1') IS NOT NULL
     AND to_regclass('public.live_entry_fill_applications_v1') IS NOT NULL THEN
    EXECUTE
      'LOCK TABLE live_entry_fill_applications_v1, '
      'live_entry_fill_evidence_v1 IN ACCESS EXCLUSIVE MODE';
  END IF;
END $$;

DO $$
DECLARE
  evidence_present BOOLEAN :=
    to_regclass('public.live_entry_fill_evidence_v1') IS NOT NULL;
  applications_present BOOLEAN :=
    to_regclass('public.live_entry_fill_applications_v1') IS NOT NULL;
BEGIN
  IF NOT evidence_present AND NOT applications_present THEN
    RETURN;
  END IF;
  IF evidence_present <> applications_present THEN
    RAISE EXCEPTION 'LEI1C_ROLLBACK_PARTIAL_SCHEMA';
  END IF;
  IF EXISTS (SELECT 1 FROM live_entry_fill_evidence_v1)
     OR EXISTS (SELECT 1 FROM live_entry_fill_applications_v1) THEN
    RAISE EXCEPTION 'LEI1C_ROLLBACK_BLOCKED_IMMUTABLE_EVIDENCE_EXISTS';
  END IF;
END $$;

DROP TABLE IF EXISTS live_entry_fill_applications_v1;
DROP TABLE IF EXISTS live_entry_fill_evidence_v1;
DROP FUNCTION IF EXISTS validate_live_entry_fill_application_v1();
DROP FUNCTION IF EXISTS validate_live_entry_fill_evidence_v1();
DROP FUNCTION IF EXISTS prevent_live_entry_fill_history_mutation_v1();
DROP FUNCTION IF EXISTS live_entry_fill_position_link_matches_v1(
  TEXT,TEXT,TEXT,TEXT,BIGINT
);

-- schema_migration_ledger_v1 is append-only. The successful application row
-- remains as provenance; a later re-application must present the same contract
-- checksum and recreates only empty LEI1C ledgers.
COMMIT;

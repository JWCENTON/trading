-- WALTRADE LEI1B formal rollback.  Evidence makes rollback unsafe.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $$
DECLARE
  submissions_present BOOLEAN :=
    to_regclass('public.live_entry_submissions_v1') IS NOT NULL;
  acks_present BOOLEAN :=
    to_regclass('public.live_entry_order_acks_v1') IS NOT NULL;
BEGIN
  IF NOT submissions_present AND NOT acks_present THEN
    RETURN;
  END IF;
  IF submissions_present <> acks_present THEN
    RAISE EXCEPTION 'LEI1B_ROLLBACK_PARTIAL_SCHEMA';
  END IF;
  IF EXISTS (SELECT 1 FROM live_entry_submissions_v1)
     OR EXISTS (SELECT 1 FROM live_entry_order_acks_v1) THEN
    RAISE EXCEPTION 'LEI1B_ROLLBACK_BLOCKED_IMMUTABLE_EVIDENCE_EXISTS';
  END IF;
END $$;

DROP TABLE IF EXISTS live_entry_order_acks_v1;
DROP TABLE IF EXISTS live_entry_submissions_v1;
DROP FUNCTION IF EXISTS validate_live_entry_ack_attribution_v1();
DROP FUNCTION IF EXISTS validate_live_entry_submission_attribution_v1();
DROP FUNCTION IF EXISTS prevent_live_entry_submission_history_mutation_v1();

COMMIT;

\set ON_ERROR_STOP on
\i /repo/db/migrations/20260720_orc_immutable_apply_ledger_v1.sql

DO $$
DECLARE
  required_count integer;
BEGIN
  SELECT count(*) INTO required_count
    FROM information_schema.columns
   WHERE table_name = 'orc_apply_runs_v1'
     AND column_name IN ('source_candidate_count','candidate_universe_count',
                         'slot_decision_count','source_excluded_count')
     AND is_nullable = 'NO';
  IF required_count <> 4 THEN
    RAISE EXCEPTION 'fresh schema counter contract missing';
  END IF;
END $$;

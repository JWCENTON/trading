\set ON_ERROR_STOP on

CREATE TABLE orc_apply_runs_v1 (
  run_id uuid PRIMARY KEY,
  candidate_universe_count integer NOT NULL,
  transaction_outcome text NOT NULL,
  schema_version text NOT NULL
);
INSERT INTO orc_apply_runs_v1 VALUES
  ('c760ea87-cfe4-4460-9cb0-128ae98fe711', 32, 'ROLLED_BACK',
   'ORC_APPLY_LEDGER_V1');

\i /repo/db/migrations/20260720_orc_immutable_apply_ledger_v1_1_counter_semantics.sql

DO $$
DECLARE
  legacy record;
BEGIN
  SELECT * INTO legacy FROM orc_apply_runs_v1
   WHERE run_id = 'c760ea87-cfe4-4460-9cb0-128ae98fe711';
  IF legacy.candidate_universe_count <> 32
     OR legacy.transaction_outcome <> 'ROLLED_BACK'
     OR legacy.schema_version <> 'ORC_APPLY_LEDGER_V1'
     OR legacy.source_candidate_count <> 0
     OR legacy.slot_decision_count <> 0
     OR legacy.source_excluded_count <> 0 THEN
    RAISE EXCEPTION 'immutable legacy evidence changed';
  END IF;
END $$;

INSERT INTO orc_apply_runs_v1 VALUES
  ('11111111-1111-4111-8111-111111111111', 28, 'COMMITTED',
   'ORC_APPLY_LEDGER_V1_1', 32, 28, 4);
INSERT INTO orc_apply_runs_v1 VALUES
  ('22222222-2222-4222-8222-222222222222', 28, 'COMMITTED',
   'ORC_APPLY_LEDGER_V1_1', 28, 28, 0);

DO $$
BEGIN
  BEGIN
    INSERT INTO orc_apply_runs_v1 VALUES
      ('33333333-3333-4333-8333-333333333333', 28, 'COMMITTED',
       'ORC_APPLY_LEDGER_V1_1', 28, 27, 0);
    RAISE EXCEPTION 'mismatched committed row was accepted';
  EXCEPTION WHEN check_violation THEN
    NULL;
  END;
END $$;

BEGIN;

-- Additive upgrade for databases that already contain immutable V1 evidence.
-- Existing rows are never updated or deleted. PostgreSQL supplies zero only for
-- legacy rows while adding each NOT NULL column; defaults are then removed so
-- every V1.1 writer must provide the counters explicitly.
ALTER TABLE orc_apply_runs_v1
  ADD COLUMN IF NOT EXISTS source_candidate_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS slot_decision_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS source_excluded_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE orc_apply_runs_v1
  ALTER COLUMN source_candidate_count DROP DEFAULT,
  ALTER COLUMN slot_decision_count DROP DEFAULT,
  ALTER COLUMN source_excluded_count DROP DEFAULT,
  ALTER COLUMN schema_version SET DEFAULT 'ORC_APPLY_LEDGER_V1_1';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'orc_apply_runs_v1'::regclass
       AND conname = 'orc_apply_runs_source_candidate_nonnegative_v1_1'
  ) THEN
    ALTER TABLE orc_apply_runs_v1
      ADD CONSTRAINT orc_apply_runs_source_candidate_nonnegative_v1_1
      CHECK (source_candidate_count >= 0);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'orc_apply_runs_v1'::regclass
       AND conname = 'orc_apply_runs_slot_decision_nonnegative_v1_1'
  ) THEN
    ALTER TABLE orc_apply_runs_v1
      ADD CONSTRAINT orc_apply_runs_slot_decision_nonnegative_v1_1
      CHECK (slot_decision_count >= 0);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'orc_apply_runs_v1'::regclass
       AND conname = 'orc_apply_runs_source_excluded_nonnegative_v1_1'
  ) THEN
    ALTER TABLE orc_apply_runs_v1
      ADD CONSTRAINT orc_apply_runs_source_excluded_nonnegative_v1_1
      CHECK (source_excluded_count >= 0);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'orc_apply_runs_v1'::regclass
       AND conname = 'orc_apply_runs_counter_semantics_v1_1'
  ) THEN
    ALTER TABLE orc_apply_runs_v1
      ADD CONSTRAINT orc_apply_runs_counter_semantics_v1_1
      CHECK (
        schema_version <> 'ORC_APPLY_LEDGER_V1_1'
        OR (
          source_excluded_count = source_candidate_count - candidate_universe_count
          AND (
            transaction_outcome <> 'COMMITTED'
            OR candidate_universe_count = slot_decision_count
          )
        )
      );
  END IF;
END $$;

COMMIT;

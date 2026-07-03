BEGIN;

ALTER TABLE exit_learning_v1
  ADD COLUMN IF NOT EXISTS sample_confidence TEXT,
  ADD COLUMN IF NOT EXISTS recoverable_net_usdc NUMERIC,
  ADD COLUMN IF NOT EXISTS recoverable_reason TEXT,
  ADD COLUMN IF NOT EXISTS learning_score NUMERIC;

DROP VIEW IF EXISTS v_exit_learning_top_recoverable_v1;
DROP VIEW IF EXISTS v_exit_learning_priority_v1;

\i db/migrations/20260703_exit_learning_engine_v11.sql

COMMIT;

BEGIN;

DROP VIEW IF EXISTS v_mme_learning_missing_recent_v1;
DROP VIEW IF EXISTS v_mme_learning_slot_coverage_v1;
DROP VIEW IF EXISTS v_mme_learning_available_context_v1;

\i db/migrations/20260704_learning_feedback_audit_v12_mme_context.sql

COMMIT;

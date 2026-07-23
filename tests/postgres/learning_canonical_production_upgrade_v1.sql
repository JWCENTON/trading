\set ON_ERROR_STOP on

-- PostgreSQL 16 existing-schema production-upgrade harness.
-- Run only against a writable clone restored from the immutable LOCAL LIVE
-- baseline. The caller mounts this repository read-only at /repo.
SET waltrade.deployment_instance_id = 'local';
SET waltrade.environment = 'live';

CREATE TEMP TABLE upgrade_contract_prestate AS
SELECT
    (SELECT count(*) FROM learning_slot_statistics_v1) AS statistics_rows,
    (SELECT count(*) FROM learning_proposal_observations_v1) AS observation_rows,
    (SELECT count(*) FROM learning_feedback_refresh_runs_v1) AS feedback_runs,
    (SELECT count(*) FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'v_learning_slot_statistics_v1') AS statistics_view_columns,
    (SELECT count(*) FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'v_learning_calibration_proposals_v1') AS proposals_view_columns,
    (SELECT count(*) FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'v_learning_feedback_engine_summary_v1') AS summary_view_columns;

\i /repo/db/migrations/20260723_learning_feedback_canonical_source_upgrade_v1.sql
\i /repo/db/migrations/20260723_learning_decision_98b4_repair_v1.sql
\i /repo/db/migrations/20260723_learning_canonical_decision_source_universe_v1.sql
\i /repo/db/migrations/20260721_learning_evidence_manifest_v1.sql

DO $run_one$
DECLARE
    p upgrade_contract_prestate%ROWTYPE;
BEGIN
    SELECT * INTO STRICT p FROM upgrade_contract_prestate;
    IF p.statistics_rows <> (SELECT count(*) FROM learning_slot_statistics_v1)
       OR p.observation_rows <>
            (SELECT count(*) FROM learning_proposal_observations_v1)
       OR p.feedback_runs <>
            (SELECT count(*) FROM learning_feedback_refresh_runs_v1) THEN
        RAISE EXCEPTION 'PRODUCTION_UPGRADE_HISTORY_REWRITE';
    END IF;
    IF p.statistics_view_columns <> (
           SELECT count(*) FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'v_learning_slot_statistics_v1')
       OR p.proposals_view_columns <> (
           SELECT count(*) FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'v_learning_calibration_proposals_v1')
       OR p.summary_view_columns <> (
           SELECT count(*) FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'v_learning_feedback_engine_summary_v1') THEN
        RAISE EXCEPTION 'PRODUCTION_UPGRADE_VIEW_CONTRACT_REGRESSION';
    END IF;
    IF (SELECT count(*) FROM learning_decision_identity_repairs_v1
         WHERE decision_key = '98b4eb54128ca4800d8cc91499026e7f'
           AND decision_id =
                '2cf22538-41ff-5be3-ab51-40cbb9f468e1'::UUID
           AND outcome_id =
                '46821b51-7075-593b-8166-3d39f923e391'::UUID) <> 1 THEN
        RAISE EXCEPTION 'PRODUCTION_UPGRADE_EXACT_REPAIR_MISSING';
    END IF;
    IF (SELECT count(*) FROM decision_registry_v1
         WHERE decision_id =
            '2cf22538-41ff-5be3-ab51-40cbb9f468e1'::UUID) <> 1
       OR (SELECT count(*) FROM decision_outcomes_v1
         WHERE outcome_id =
            '46821b51-7075-593b-8166-3d39f923e391'::UUID) <> 1 THEN
        RAISE EXCEPTION 'PRODUCTION_UPGRADE_EXACT_REPAIR_PARITY';
    END IF;
END;
$run_one$;

-- Run 2: every migration is idempotent; repair is a no-op that revalidates
-- all exact source and inserted semantics.
\i /repo/db/migrations/20260723_learning_feedback_canonical_source_upgrade_v1.sql
\i /repo/db/migrations/20260723_learning_decision_98b4_repair_v1.sql
\i /repo/db/migrations/20260723_learning_canonical_decision_source_universe_v1.sql
\i /repo/db/migrations/20260721_learning_evidence_manifest_v1.sql

DO $run_two$
BEGIN
    IF (SELECT count(*) FROM learning_decision_identity_repairs_v1) <> 1
       OR (SELECT count(*) FROM decision_registry_v1
            WHERE decision_id =
                '2cf22538-41ff-5be3-ab51-40cbb9f468e1'::UUID) <> 1
       OR (SELECT count(*) FROM decision_outcomes_v1
            WHERE outcome_id =
                '46821b51-7075-593b-8166-3d39f923e391'::UUID) <> 1 THEN
        RAISE EXCEPTION 'PRODUCTION_UPGRADE_RUN_TWO_NOT_IDEMPOTENT';
    END IF;
    IF EXISTS (
        SELECT decision_id FROM decision_registry_v1
         GROUP BY decision_id HAVING count(*) > 1
    ) OR EXISTS (
        SELECT outcome_id FROM decision_outcomes_v1
         GROUP BY outcome_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'PRODUCTION_UPGRADE_DUPLICATE_IDENTITY';
    END IF;
END;
$run_two$;

SELECT 'LEARNING_CANONICAL_PRODUCTION_UPGRADE_POSTGRES16_PASS' AS result;

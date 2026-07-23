\set ON_ERROR_STOP on

\if :{?deployment_instance_id}
\else
  \echo 'deployment_instance_id psql variable is required'
  \quit
\endif
\if :{?runtime_environment}
\else
  \echo 'runtime_environment psql variable is required'
  \quit
\endif

SELECT set_config(
    'waltrade.deployment_instance_id',
    :'deployment_instance_id',
    false
);
SELECT set_config(
    'waltrade.environment',
    :'runtime_environment',
    false
);

CREATE TABLE IF NOT EXISTS learning_feedback_refresh_runs_v1 (
    id BIGINT PRIMARY KEY,
    status TEXT
);
CREATE TABLE IF NOT EXISTS learning_proposal_validation_runs_v1 (
    id BIGINT PRIMARY KEY,
    status TEXT
);
INSERT INTO learning_feedback_refresh_runs_v1 VALUES (101, 'OK')
ON CONFLICT DO NOTHING;
INSERT INTO learning_proposal_validation_runs_v1 VALUES (201, 'OK')
ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION refresh_learning_feedback_engine_v1(
    INTEGER, INTEGER, INTEGER
)
RETURNS JSONB LANGUAGE SQL AS $$
    SELECT '{"contract":"canonical"}'::JSONB
$$;
CREATE OR REPLACE FUNCTION
learning_feedback_engine_v1_pre_canonical_source_v1(
    INTEGER, INTEGER, INTEGER
)
RETURNS JSONB LANGUAGE SQL AS $$
    SELECT '{"contract":"legacy"}'::JSONB
$$;
CREATE OR REPLACE FUNCTION learning_canonical_evidence_universe_v1(
    TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ
)
RETURNS SETOF INTEGER LANGUAGE SQL AS $$ SELECT 1 WHERE false $$;

CREATE TABLE learning_evidence_manifests_v1 (id UUID PRIMARY KEY);
CREATE TABLE learning_evidence_membership_v1 (id UUID PRIMARY KEY);
CREATE TABLE learning_evidence_aggregates_v1 (id UUID PRIMARY KEY);
CREATE TABLE learning_canonical_evidence_selection_v1 (
    feedback_run_id BIGINT PRIMARY KEY
);
CREATE OR REPLACE FUNCTION prevent_learning_canonical_evidence_mutation_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;

\ir ../../db/migrations/20260724_learning_canonical_shared_rollback_v2.sql

DO $run_one$
BEGIN
    IF to_regclass('public.learning_evidence_manifests_v1') IS NOT NULL
       OR to_regclass('public.learning_evidence_membership_v1') IS NOT NULL
       OR to_regclass('public.learning_evidence_aggregates_v1') IS NOT NULL
       OR to_regclass(
            'public.learning_canonical_evidence_selection_v1'
          ) IS NOT NULL
       OR to_regprocedure(
            'learning_canonical_evidence_universe_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
          ) IS NOT NULL THEN
        RAISE EXCEPTION 'SHARED_ROLLBACK_OBJECT_RESIDUE';
    END IF;
    IF to_regprocedure(
        'learning_feedback_engine_v1_pre_canonical_source_v1(integer,integer,integer)'
    ) IS NOT NULL THEN
        RAISE EXCEPTION 'SHARED_ROLLBACK_BACKUP_RESIDUE';
    END IF;
    IF position(
        '"contract":"legacy"'
        IN pg_get_functiondef(
            'refresh_learning_feedback_engine_v1(integer,integer,integer)'
            ::regprocedure
        )
    ) = 0 THEN
        RAISE EXCEPTION 'SHARED_ROLLBACK_FEEDBACK_NOT_RESTORED';
    END IF;
    IF (SELECT count(*) FROM learning_feedback_refresh_runs_v1) <> 1
       OR (SELECT count(*) FROM learning_proposal_validation_runs_v1) <> 1 THEN
        RAISE EXCEPTION 'SHARED_ROLLBACK_HISTORY_REWRITE';
    END IF;
    IF to_regclass(
        'public.learning_decision_identity_repairs_v1'
    ) IS NOT NULL THEN
        RAISE EXCEPTION 'SHARED_ROLLBACK_REPAIR_DEPENDENCY_CREATED';
    END IF;
END;
$run_one$;

-- Run 2 must be an identity-validated no-op.
\ir ../../db/migrations/20260724_learning_canonical_shared_rollback_v2.sql

DO $run_two$
BEGIN
    IF (SELECT count(*) FROM learning_feedback_refresh_runs_v1) <> 1
       OR (SELECT count(*) FROM learning_proposal_validation_runs_v1) <> 1 THEN
        RAISE EXCEPTION 'SHARED_ROLLBACK_RUN_TWO_HISTORY_REWRITE';
    END IF;
END;
$run_two$;

SELECT
    :'deployment_instance_id' || '-' || :'runtime_environment'
        AS tested_deployment_id,
    'LEARNING_CANONICAL_SHARED_ROLLBACK_V2_PASS' AS result;

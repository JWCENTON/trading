\set ON_ERROR_STOP on

-- Required psql variables:
--   target_position_ids comma-separated explicit position ids (maximum 1000)
--   target_environment   must be trading_paper
--   target_deployment_id must be LOCAL or VPS
--
-- This operator is intentionally not invoked by the migration. It is a bounded,
-- explicitly targeted PAPER-only repair for already-projected rows with
-- canonical FT COMPLETE.

\if :{?target_position_ids}
\else
    \echo 'Missing required variable: target_position_ids'
    \quit
\endif

\if :{?target_environment}
\else
    \echo 'Missing required variable: target_environment'
    \quit
\endif

\if :{?target_deployment_id}
\else
    \echo 'Missing required variable: target_deployment_id'
    \quit
\endif

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TEMP TABLE decision_outcome_reprojection_contract_v1 (
    environment text NOT NULL,
    deployment_id text NOT NULL
) ON COMMIT DROP;

INSERT INTO decision_outcome_reprojection_contract_v1(
    environment,deployment_id
) VALUES (:'target_environment', :'target_deployment_id');

DO $$
DECLARE
    v_environment text;
    v_deployment_id text;
BEGIN
    SELECT environment,deployment_id
      INTO v_environment,v_deployment_id
      FROM decision_outcome_reprojection_contract_v1;

    IF v_environment IS DISTINCT FROM 'trading_paper'
       OR v_deployment_id NOT IN ('LOCAL','VPS') THEN
        RAISE EXCEPTION
            'REPROJECTION_TARGET_NOT_ALLOWED: environment=% deployment_id=%',
            COALESCE(v_environment, '<null>'),
            COALESCE(v_deployment_id, '<null>');
    END IF;
END
$$;

CREATE TEMP TABLE decision_outcome_reprojection_targets_v1 (
    position_id bigint PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO decision_outcome_reprojection_targets_v1 (position_id)
SELECT DISTINCT trim(value)::bigint
FROM unnest(string_to_array(:'target_position_ids', ',')) AS ids(value)
WHERE trim(value) <> '';

DO $$
DECLARE
    v_target_count integer;
BEGIN
    SELECT count(*) INTO v_target_count
    FROM decision_outcome_reprojection_targets_v1;

    IF v_target_count < 1 OR v_target_count > 1000 THEN
        RAISE EXCEPTION
            'Bounded reprojection requires between 1 and 1000 explicit position ids; got %',
            v_target_count;
    END IF;
END
$$;

CREATE TEMP TABLE decision_outcome_reprojection_candidates_v1
ON COMMIT DROP AS
SELECT
    outcome.outcome_id,
    outcome.position_id,
    financial_truth.authoritative_gross_pnl,
    COALESCE(
        financial_truth.authoritative_fees_usdc,
        financial_truth.authoritative_entry_fees_usdc
            + financial_truth.authoritative_exit_fees_usdc
    ) AS authoritative_fees_usdc,
    financial_truth.authoritative_net_pnl
FROM decision_outcomes_v1 outcome
JOIN decision_outcome_reprojection_contract_v1 contract
  ON contract.environment = outcome.environment
 AND contract.deployment_id = outcome.deployment_id
JOIN decision_outcome_reprojection_targets_v1 target
  ON target.position_id = outcome.position_id
JOIN canonical_financial_truth_v1 financial_truth
  ON financial_truth.position_id = outcome.position_id
 AND financial_truth.financial_truth_status = 'COMPLETE'
WHERE outcome.outcome_type = 'ACTUAL_TRADE'
  AND financial_truth.authoritative_gross_pnl IS NOT NULL
  AND COALESCE(
          financial_truth.authoritative_fees_usdc,
          financial_truth.authoritative_entry_fees_usdc
              + financial_truth.authoritative_exit_fees_usdc
      ) IS NOT NULL
  AND financial_truth.authoritative_net_pnl IS NOT NULL;

DO $$
DECLARE
    v_target_count integer;
    v_candidate_count integer;
BEGIN
    SELECT count(*) INTO v_target_count
    FROM decision_outcome_reprojection_targets_v1;

    SELECT count(*) INTO v_candidate_count
    FROM decision_outcome_reprojection_candidates_v1;

    IF v_candidate_count <> v_target_count THEN
        RAISE EXCEPTION
            'Refusing partial reprojection: % explicit targets but % eligible FT COMPLETE outcomes',
            v_target_count,
            v_candidate_count;
    END IF;
END
$$;

UPDATE decision_outcomes_v1 outcome
SET
    gross_pnl_usdc = candidate.authoritative_gross_pnl,
    fees_usdc = candidate.authoritative_fees_usdc,
    net_pnl_usdc = candidate.authoritative_net_pnl,
    outcome_status = 'COMPLETE',
    evidence = outcome.evidence || jsonb_build_object(
        'economics_source', 'CANONICAL_FINANCIAL_TRUTH_V1',
        'financial_truth_status', 'COMPLETE',
        'projection_contract', 'DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1',
        'bounded_reprojection', true,
        'reprojection_environment', contract.environment,
        'reprojection_deployment_id', contract.deployment_id
    ),
    refreshed_at = CASE
        WHEN outcome.gross_pnl_usdc IS DISTINCT FROM
                 candidate.authoritative_gross_pnl
          OR outcome.fees_usdc IS DISTINCT FROM
                 candidate.authoritative_fees_usdc
          OR outcome.net_pnl_usdc IS DISTINCT FROM
                 candidate.authoritative_net_pnl
          OR outcome.outcome_status <> 'COMPLETE'
          OR outcome.evidence->>'economics_source' IS DISTINCT FROM
                 'CANONICAL_FINANCIAL_TRUTH_V1'
          OR outcome.evidence->>'reprojection_environment' IS DISTINCT FROM
                 contract.environment
          OR outcome.evidence->>'reprojection_deployment_id' IS DISTINCT FROM
                 contract.deployment_id
        THEN clock_timestamp()
        ELSE outcome.refreshed_at
    END
FROM decision_outcome_reprojection_candidates_v1 candidate
JOIN decision_outcome_reprojection_contract_v1 contract ON true
WHERE outcome.outcome_id = candidate.outcome_id;

DO $$
DECLARE
    v_mismatch_count integer;
BEGIN
    SELECT count(*) INTO v_mismatch_count
    FROM decision_outcomes_v1 outcome
    JOIN decision_outcome_reprojection_candidates_v1 candidate
      ON candidate.outcome_id = outcome.outcome_id
    WHERE outcome.gross_pnl_usdc IS DISTINCT FROM candidate.authoritative_gross_pnl
       OR outcome.fees_usdc IS DISTINCT FROM candidate.authoritative_fees_usdc
       OR outcome.net_pnl_usdc IS DISTINCT FROM candidate.authoritative_net_pnl
       OR outcome.outcome_status <> 'COMPLETE';

    IF v_mismatch_count <> 0 THEN
        RAISE EXCEPTION
            'Bounded canonical reprojection verification failed for % rows',
            v_mismatch_count;
    END IF;
END
$$;

SELECT
    count(*) AS reprojected_rows,
    min(position_id) AS minimum_position_id,
    max(position_id) AS maximum_position_id
FROM decision_outcome_reprojection_candidates_v1;

COMMIT;

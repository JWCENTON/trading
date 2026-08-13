\set ON_ERROR_STOP on

-- Required psql variables:
--   target_environment     must be trading_paper
--   target_deployment_id   must be LOCAL or VPS
--   effective_at           explicit UTC cutover timestamp
--   simulation_fee_rate    deployment configuration, currently 0.0035
--   git_sha                deployed 40-character commit SHA

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
\if :{?effective_at}
\else
    \echo 'Missing required variable: effective_at'
    \quit
\endif
\if :{?simulation_fee_rate}
\else
    \echo 'Missing required variable: simulation_fee_rate'
    \quit
\endif
\if :{?git_sha}
\else
    \echo 'Missing required variable: git_sha'
    \quit
\endif

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TEMP TABLE paper_fee_cutover_contract_input_v1(
    environment text NOT NULL,
    deployment_id text NOT NULL,
    effective_at timestamptz NOT NULL,
    simulation_fee_rate numeric NOT NULL,
    git_sha text NOT NULL
) ON COMMIT DROP;

INSERT INTO paper_fee_cutover_contract_input_v1 VALUES (
    :'target_environment', :'target_deployment_id', :'effective_at'::timestamptz,
    :'simulation_fee_rate'::numeric, :'git_sha'
);

DO $target_contract$
DECLARE
    input paper_fee_cutover_contract_input_v1%ROWTYPE;
BEGIN
    SELECT * INTO input FROM paper_fee_cutover_contract_input_v1;
    IF input.environment IS DISTINCT FROM 'trading_paper'
       OR input.deployment_id NOT IN ('LOCAL','VPS') THEN
        RAISE EXCEPTION
            'PAPER_FEE_CUTOVER_TARGET_NOT_ALLOWED: environment=% deployment_id=%',
            input.environment, input.deployment_id;
    END IF;
    IF input.simulation_fee_rate < 0
       OR input.simulation_fee_rate > 0.10 THEN
        RAISE EXCEPTION 'INVALID_PAPER_SIMULATION_FEE_RATE_INPUT';
    END IF;
    IF input.git_sha !~ '^[0-9a-f]{40}$' THEN
        RAISE EXCEPTION 'INVALID_GIT_SHA_INPUT';
    END IF;
END
$target_contract$;

INSERT INTO public.paper_simulation_fee_cutover_v2(
    cutover_name,effective_at,simulation_fee_rate,
    fee_model_version,fee_config_source,git_sha
) SELECT
    'COST_CORRECTED_PAPER_ECONOMIC_CUTOVER',
    input.effective_at,
    input.simulation_fee_rate,
    'PAPER_SIMULATOR_FINANCIAL_MODEL_V2',
    'ENV:PAPER_SIMULATION_FEE_RATE',
    input.git_sha
FROM paper_fee_cutover_contract_input_v1 input
ON CONFLICT (cutover_name) DO NOTHING;

INSERT INTO public.paper_economic_contract_provenance_v1(
    contract_name,environment,deployment_id,simulation_fee_rate,
    fee_model_version,fee_config_source,economic_cutover_at,
    source_migration_id
) SELECT
    'PAPER_SIMULATION_FEE_CONTRACT_V2',
    input.environment, input.deployment_id,
    input.simulation_fee_rate,
    'PAPER_SIMULATOR_FINANCIAL_MODEL_V2',
    'ENV:PAPER_SIMULATION_FEE_RATE',
    input.effective_at,
    '20260813_paper_simulation_fee_contract_v2.sql'
FROM paper_fee_cutover_contract_input_v1 input
ON CONFLICT (contract_name) DO NOTHING;

DO $postcondition$
DECLARE
    v_bad integer;
    input paper_fee_cutover_contract_input_v1%ROWTYPE;
BEGIN
    SELECT * INTO input FROM paper_fee_cutover_contract_input_v1;
    SELECT count(*) INTO v_bad
    FROM public.paper_simulation_fee_cutover_v2 cutover
    JOIN public.paper_economic_contract_provenance_v1 provenance
      ON provenance.contract_name = 'PAPER_SIMULATION_FEE_CONTRACT_V2'
    WHERE cutover.cutover_name = 'COST_CORRECTED_PAPER_ECONOMIC_CUTOVER'
      AND (
          cutover.effective_at IS DISTINCT FROM input.effective_at
          OR cutover.simulation_fee_rate IS DISTINCT FROM
             input.simulation_fee_rate
          OR cutover.git_sha IS DISTINCT FROM input.git_sha
          OR provenance.environment IS DISTINCT FROM input.environment
          OR provenance.deployment_id IS DISTINCT FROM input.deployment_id
          OR provenance.simulation_fee_rate IS DISTINCT FROM
             input.simulation_fee_rate
          OR provenance.economic_cutover_at IS DISTINCT FROM
             input.effective_at
      );

    IF v_bad <> 0 OR NOT EXISTS (
        SELECT 1
        FROM public.paper_simulation_fee_cutover_v2 cutover
        JOIN public.paper_economic_contract_provenance_v1 provenance
          ON provenance.contract_name = 'PAPER_SIMULATION_FEE_CONTRACT_V2'
        WHERE cutover.cutover_name =
              'COST_CORRECTED_PAPER_ECONOMIC_CUTOVER'
    ) THEN
        RAISE EXCEPTION 'PAPER_FEE_CUTOVER_PROVENANCE_POSTCONDITION_FAILED';
    END IF;
END
$postcondition$;

COMMIT;

-- Required session inputs (set explicitly by the operator before \i):
--   SET waltrade.target_environment = 'trading_paper';
--   SET waltrade.target_deployment_id = 'LOCAL'; -- or VPS
--   SET waltrade.paper_simulation_fee_rate = '0.0035';

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $target_contract$
DECLARE
    v_environment text := current_setting(
        'waltrade.target_environment', true
    );
    v_deployment_id text := current_setting(
        'waltrade.target_deployment_id', true
    );
    v_fee_rate numeric;
BEGIN
    IF v_environment IS DISTINCT FROM 'trading_paper'
       OR v_deployment_id NOT IN ('LOCAL','VPS') THEN
        RAISE EXCEPTION
            'PAPER_ECONOMIC_TRUTH_TARGET_NOT_ALLOWED: environment=% deployment_id=%',
            COALESCE(v_environment, '<missing>'),
            COALESCE(v_deployment_id, '<missing>');
    END IF;

    BEGIN
        v_fee_rate := current_setting(
            'waltrade.paper_simulation_fee_rate', true
        )::numeric;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'INVALID_PAPER_SIMULATION_FEE_RATE_INPUT';
    END;

    IF v_fee_rate IS NULL OR v_fee_rate < 0 OR v_fee_rate > 0.10 THEN
        RAISE EXCEPTION 'INVALID_PAPER_SIMULATION_FEE_RATE_INPUT';
    END IF;
END
$target_contract$;

CREATE TABLE IF NOT EXISTS public.migration_provenance_correction_v1 (
    correction_id bigserial PRIMARY KEY,
    corrected_ledger_id bigint NOT NULL UNIQUE,
    migration_id text NOT NULL,
    original_environment text NOT NULL,
    original_deployment_id text NOT NULL,
    corrected_environment text NOT NULL,
    corrected_deployment_id text NOT NULL,
    correction_contract text NOT NULL CHECK (
        correction_contract =
            'PAPER_ECONOMIC_TRUTH_DEPLOYMENT_PORTABILITY_V1'
    ),
    correction_reason text NOT NULL,
    correction_git_sha text NOT NULL,
    corrected_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE public.migration_provenance_correction_v1 IS
'Append-only audit evidence for separately migrated corrections to migration ledger provenance.';

CREATE OR REPLACE FUNCTION
public.reject_migration_provenance_correction_mutation_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'MIGRATION_PROVENANCE_CORRECTION_V1_IMMUTABLE';
END
$function$;

DROP TRIGGER IF EXISTS trg_migration_provenance_correction_v1_immutable
    ON public.migration_provenance_correction_v1;
CREATE TRIGGER trg_migration_provenance_correction_v1_immutable
BEFORE UPDATE OR DELETE ON public.migration_provenance_correction_v1
FOR EACH ROW EXECUTE FUNCTION
    public.reject_migration_provenance_correction_mutation_v1();

CREATE TABLE IF NOT EXISTS public.paper_economic_contract_provenance_v1 (
    contract_name text PRIMARY KEY CHECK (contract_name IN (
        'DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1',
        'PAPER_SIMULATION_FEE_CONTRACT_V2'
    )),
    environment text NOT NULL CHECK (environment = 'trading_paper'),
    deployment_id text NOT NULL CHECK (deployment_id IN ('LOCAL','VPS')),
    simulation_fee_rate numeric CHECK (
        simulation_fee_rate IS NULL
        OR simulation_fee_rate BETWEEN 0 AND 0.10
    ),
    fee_model_version text,
    fee_config_source text,
    economic_cutover_at timestamptz,
    source_migration_id text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        contract_name <> 'PAPER_SIMULATION_FEE_CONTRACT_V2'
        OR (
            simulation_fee_rate IS NOT NULL
            AND fee_model_version = 'PAPER_SIMULATOR_FINANCIAL_MODEL_V2'
            AND fee_config_source = 'ENV:PAPER_SIMULATION_FEE_RATE'
            AND economic_cutover_at IS NOT NULL
        )
    )
);

COMMENT ON TABLE public.paper_economic_contract_provenance_v1 IS
'Immutable per-database deployment identity for PAPER economic truth contracts.';

CREATE OR REPLACE FUNCTION
public.reject_paper_economic_contract_provenance_mutation_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'PAPER_ECONOMIC_CONTRACT_PROVENANCE_V1_IMMUTABLE';
END
$function$;

DROP TRIGGER IF EXISTS trg_paper_economic_contract_provenance_v1_immutable
    ON public.paper_economic_contract_provenance_v1;
CREATE TRIGGER trg_paper_economic_contract_provenance_v1_immutable
BEFORE UPDATE OR DELETE ON public.paper_economic_contract_provenance_v1
FOR EACH ROW EXECUTE FUNCTION
    public.reject_paper_economic_contract_provenance_mutation_v1();

CREATE TEMP TABLE migration_provenance_targets_v1
ON COMMIT DROP AS
SELECT ledger.ledger_id,ledger.migration_id,ledger.environment,
       ledger.deployment_id
FROM public.schema_migration_ledger_v1 ledger
WHERE ledger.migration_id IN (
    '20260813_decision_outcome_canonical_financial_truth_source_v1.sql',
    '20260813_paper_simulation_fee_contract_v2.sql'
)
  AND (
      ledger.environment IS DISTINCT FROM 'PAPER'
      OR ledger.deployment_id IS DISTINCT FROM
         current_setting('waltrade.target_deployment_id')
  );

INSERT INTO public.migration_provenance_correction_v1(
    corrected_ledger_id,migration_id,
    original_environment,original_deployment_id,
    corrected_environment,corrected_deployment_id,
    correction_contract,correction_reason,correction_git_sha
)
SELECT
    target.ledger_id,target.migration_id,
    target.environment,target.deployment_id,
    'PAPER',current_setting('waltrade.target_deployment_id'),
    'PAPER_ECONOMIC_TRUTH_DEPLOYMENT_PORTABILITY_V1',
    'Correct hardcoded LOCAL provenance using explicit PAPER deployment target',
    COALESCE(
        NULLIF(current_setting('waltrade.git_sha',true),''),repeat('0',40)
    )
FROM migration_provenance_targets_v1 target
ON CONFLICT (corrected_ledger_id) DO NOTHING;

UPDATE public.schema_migration_ledger_v1 ledger
SET environment = 'PAPER',
    deployment_id = current_setting('waltrade.target_deployment_id')
FROM migration_provenance_targets_v1 target
WHERE target.ledger_id = ledger.ledger_id;

INSERT INTO public.paper_economic_contract_provenance_v1(
    contract_name,environment,deployment_id,simulation_fee_rate,
    fee_model_version,fee_config_source,economic_cutover_at,
    source_migration_id
)
SELECT
    'DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1',
    current_setting('waltrade.target_environment'),
    current_setting('waltrade.target_deployment_id'),
    NULL,NULL,NULL,NULL,
    '20260813_decision_outcome_canonical_financial_truth_source_v1.sql'
WHERE EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id =
        '20260813_decision_outcome_canonical_financial_truth_source_v1.sql'
)
ON CONFLICT (contract_name) DO NOTHING;

INSERT INTO public.paper_economic_contract_provenance_v1(
    contract_name,environment,deployment_id,simulation_fee_rate,
    fee_model_version,fee_config_source,economic_cutover_at,
    source_migration_id
)
SELECT
    'PAPER_SIMULATION_FEE_CONTRACT_V2',
    current_setting('waltrade.target_environment'),
    current_setting('waltrade.target_deployment_id'),
    current_setting('waltrade.paper_simulation_fee_rate')::numeric,
    'PAPER_SIMULATOR_FINANCIAL_MODEL_V2',
    'ENV:PAPER_SIMULATION_FEE_RATE',
    cutover.effective_at,
    '20260813_paper_simulation_fee_contract_v2.sql'
FROM public.paper_simulation_fee_cutover_v2 cutover
WHERE cutover.cutover_name = 'COST_CORRECTED_PAPER_ECONOMIC_CUTOVER'
  AND EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id = '20260813_paper_simulation_fee_contract_v2.sql'
)
ON CONFLICT (contract_name) DO NOTHING;

DO $postcondition$
DECLARE
    v_target text := current_setting('waltrade.target_deployment_id');
    v_bad_ledger integer;
    v_bad_contract integer;
BEGIN
    SELECT count(*) INTO v_bad_ledger
    FROM public.schema_migration_ledger_v1
    WHERE migration_id IN (
        '20260813_decision_outcome_canonical_financial_truth_source_v1.sql',
        '20260813_paper_simulation_fee_contract_v2.sql'
    )
      AND (environment <> 'PAPER' OR deployment_id <> v_target);

    SELECT count(*) INTO v_bad_contract
    FROM public.paper_economic_contract_provenance_v1
    WHERE environment <> 'trading_paper'
       OR deployment_id <> v_target
       OR (
           contract_name = 'PAPER_SIMULATION_FEE_CONTRACT_V2'
           AND (
               simulation_fee_rate IS DISTINCT FROM
                   current_setting('waltrade.paper_simulation_fee_rate')::numeric
               OR fee_model_version IS DISTINCT FROM
                   'PAPER_SIMULATOR_FINANCIAL_MODEL_V2'
               OR fee_config_source IS DISTINCT FROM
                   'ENV:PAPER_SIMULATION_FEE_RATE'
               OR economic_cutover_at IS NULL
           )
       );

    IF v_bad_ledger <> 0 OR v_bad_contract <> 0 THEN
        RAISE EXCEPTION
            'PAPER_ECONOMIC_TRUTH_PROVENANCE_POSTCONDITION_FAILED ledger=% contract=%',
            v_bad_ledger,v_bad_contract;
    END IF;
END
$postcondition$;

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260813_paper_economic_truth_deployment_portability_v1.sql',
    COALESCE(
        NULLIF(current_setting('waltrade.migration_checksum',true),''),
        repeat('0',64)
    ),
    'PAPER',current_setting('waltrade.target_deployment_id'),
    current_database(),'operator-migration','APPLIED',TRUE,0,
    COALESCE(
        NULLIF(current_setting('waltrade.git_sha',true),''),repeat('0',40)
    ),
    'PAPER_ECONOMIC_TRUTH_DEPLOYMENT_PORTABILITY_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id =
        '20260813_paper_economic_truth_deployment_portability_v1.sql'
);

COMMIT;

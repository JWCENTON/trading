BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $paper_only$
BEGIN
    IF current_database() <> 'trading_paper' THEN
        RAISE EXCEPTION 'PAPER_SIMULATION_FEE_CONTRACT_V2_PAPER_ONLY';
    END IF;
END
$paper_only$;

ALTER TABLE public.simulated_execution_fills_v1
    ADD COLUMN IF NOT EXISTS simulation_fee_rate NUMERIC,
    ADD COLUMN IF NOT EXISTS fee_model_version TEXT,
    ADD COLUMN IF NOT EXISTS fee_config_source TEXT;

ALTER TABLE public.simulated_execution_fills_v1
    DROP CONSTRAINT IF EXISTS ck_simulated_execution_fee_contract_v2;

ALTER TABLE public.simulated_execution_fills_v1
    ADD CONSTRAINT ck_simulated_execution_fee_contract_v2 CHECK (
        simulation_model_version <> 'PAPER_SIMULATOR_FINANCIAL_MODEL_V2'
        OR (
            simulation_fee_rate IS NOT NULL
            AND simulation_fee_rate >= 0
            AND simulation_fee_rate <= 0.10
            AND fee_model_version = simulation_model_version
            AND NULLIF(btrim(fee_config_source), '') IS NOT NULL
        )
    );

CREATE TABLE IF NOT EXISTS public.paper_simulation_fee_cutover_v2 (
    cutover_name TEXT PRIMARY KEY CHECK (
        cutover_name = 'COST_CORRECTED_PAPER_ECONOMIC_CUTOVER'
    ),
    effective_at TIMESTAMPTZ NOT NULL,
    simulation_fee_rate NUMERIC NOT NULL CHECK (
        simulation_fee_rate >= 0 AND simulation_fee_rate <= 0.10
    ),
    fee_model_version TEXT NOT NULL CHECK (
        fee_model_version = 'PAPER_SIMULATOR_FINANCIAL_MODEL_V2'
    ),
    fee_config_source TEXT NOT NULL CHECK (
        fee_config_source = 'ENV:PAPER_SIMULATION_FEE_RATE'
    ),
    git_sha TEXT NOT NULL CHECK (git_sha ~ '^[0-9a-f]{40}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION public.reject_paper_simulation_fee_cutover_mutation_v2()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'PAPER_SIMULATION_FEE_CUTOVER_V2_IMMUTABLE';
END
$function$;

DROP TRIGGER IF EXISTS trg_paper_simulation_fee_cutover_v2_immutable
    ON public.paper_simulation_fee_cutover_v2;
CREATE TRIGGER trg_paper_simulation_fee_cutover_v2_immutable
BEFORE UPDATE OR DELETE ON public.paper_simulation_fee_cutover_v2
FOR EACH ROW EXECUTE FUNCTION
    public.reject_paper_simulation_fee_cutover_mutation_v2();

CREATE OR REPLACE VIEW public.v_paper_fee_model_outcome_classification_v2 AS
WITH fill_contract AS (
    SELECT
        fill.position_id,
        min(fill.execution_at) AS first_fill_at,
        bool_and(
            fill.simulation_model_version =
                'PAPER_SIMULATOR_FINANCIAL_MODEL_V2'
            AND fill.simulation_fee_rate IS NOT NULL
            AND fill.fee_model_version = fill.simulation_model_version
            AND NULLIF(btrim(fill.fee_config_source), '') IS NOT NULL
        ) AS all_fills_fee_model_v2,
        min(fill.simulation_fee_rate) AS minimum_fee_rate,
        max(fill.simulation_fee_rate) AS maximum_fee_rate,
        min(fill.fee_config_source) AS fee_config_source
    FROM public.simulated_execution_fills_v1 fill
    GROUP BY fill.position_id
), cutover AS (
    SELECT *
    FROM public.paper_simulation_fee_cutover_v2
    WHERE cutover_name = 'COST_CORRECTED_PAPER_ECONOMIC_CUTOVER'
)
SELECT
    truth.position_id,
    CASE
        WHEN contract.all_fills_fee_model_v2
         AND contract.first_fill_at >= cutover.effective_at
         AND contract.minimum_fee_rate = contract.maximum_fee_rate
            THEN 'FEE_MODEL_V2'
        ELSE 'PRE_FEE_MODEL_V2'
    END AS paper_fee_economic_cohort,
    contract.minimum_fee_rate AS simulation_fee_rate,
    contract.fee_config_source,
    cutover.effective_at AS cost_corrected_cutover_at
FROM public.canonical_financial_truth_v1 truth
JOIN fill_contract contract ON contract.position_id = truth.position_id
LEFT JOIN cutover ON true
WHERE truth.financial_truth_status = 'COMPLETE';

COMMENT ON TABLE public.paper_simulation_fee_cutover_v2 IS
'Immutable LOCAL PAPER activation boundary for cost-corrected economic evidence.';
COMMENT ON VIEW public.v_paper_fee_model_outcome_classification_v2 IS
'Forward-only fee cohort classification; historical fills and FT remain immutable.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260813_paper_simulation_fee_contract_v2.sql',
    COALESCE(NULLIF(current_setting('waltrade.migration_checksum',true),''),
             repeat('0',64)),
    'PAPER','LOCAL',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    COALESCE(NULLIF(current_setting('waltrade.git_sha',true),''),repeat('0',40)),
    'PAPER_SIMULATION_FEE_CONTRACT_V2'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id='20260813_paper_simulation_fee_contract_v2.sql'
);

COMMIT;

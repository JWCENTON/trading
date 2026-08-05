-- WALTRADE LEARNING EVIDENCE AGGREGATE NUMERIC PARITY V1
-- Forward-only: normalize future aggregate payloads; immutable history is untouched.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $prerequisites$
DECLARE
    v_bad_scale TEXT;
    v_scale_column_count INTEGER;
BEGIN
    IF to_regprocedure(
           'public.capture_learning_evidence_manifests_v1(bigint)'
       ) IS NULL
       OR to_regclass('public.learning_slot_statistics_v1') IS NULL
       OR to_regclass('public.learning_evidence_aggregates_v1') IS NULL
       OR to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_PREREQUISITE_MISSING';
    END IF;

    SELECT string_agg(column_name || '=' ||
                      COALESCE(numeric_scale::TEXT, '<unbounded>'), ',')
      INTO v_bad_scale
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = 'learning_slot_statistics_v1'
       AND column_name IN (
           'gross_profit_usdc', 'gross_loss_usdc', 'net_pnl_usdc',
           'profit_factor', 'expectancy_usdc'
       )
       AND (data_type <> 'numeric' OR numeric_scale IS DISTINCT FROM 12);
    IF v_bad_scale IS NOT NULL THEN
        RAISE EXCEPTION
            'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_SOURCE_SCALE_MISMATCH:%',
            v_bad_scale;
    END IF;
    SELECT count(*)
      INTO v_scale_column_count
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = 'learning_slot_statistics_v1'
       AND column_name IN (
           'gross_profit_usdc', 'gross_loss_usdc', 'net_pnl_usdc',
           'profit_factor', 'expectancy_usdc'
       );
    IF v_scale_column_count <> 5 THEN
        RAISE EXCEPTION
            'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_SOURCE_COLUMNS_MISSING';
    END IF;
END;
$prerequisites$;

CREATE OR REPLACE FUNCTION public.learning_financial_normalize_v1(
    value NUMERIC
)
RETURNS NUMERIC
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS $function$
    SELECT round(value, 12)
$function$;

COMMENT ON FUNCTION public.learning_financial_normalize_v1(NUMERIC) IS
    'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1: PostgreSQL NUMERIC round(value,12); future Learning aggregate payload/hash/parity only';

DO $patch_capture$
DECLARE
    v_signature CONSTANT TEXT :=
        'public.capture_learning_evidence_manifests_v1(bigint)';
    v_definition TEXT := pg_get_functiondef(to_regprocedure(v_signature));
    v_old TEXT;
    v_new TEXT;
BEGIN
    IF position(
           '''financial_normalization_contract'',''LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1'''
           IN v_definition
       ) = 0 THEN
        v_old :=
            '                   sum(realized_pnl_usdc) FILTER (WHERE realized_pnl_usdc>0) gross_profit,';
        v_new :=
            '                   learning_financial_normalize_v1(sum(realized_pnl_usdc) FILTER (WHERE realized_pnl_usdc>0)) gross_profit,';
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_GROSS_PROFIT_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);

        v_old :=
            '                   sum(realized_pnl_usdc) FILTER (WHERE realized_pnl_usdc<0) gross_loss,';
        v_new :=
            '                   learning_financial_normalize_v1(sum(realized_pnl_usdc) FILTER (WHERE realized_pnl_usdc<0)) gross_loss,';
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_GROSS_LOSS_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);

        v_old :=
            '                   sum(realized_pnl_usdc) net_pnl, avg(realized_pnl_usdc) expectancy,';
        v_new :=
            '                   learning_financial_normalize_v1(sum(realized_pnl_usdc)) net_pnl,' || E'\n'
            || '                   learning_financial_normalize_v1(avg(realized_pnl_usdc)) expectancy,';
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_NET_EXPECTANCY_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);

        v_old :=
            '                   sum(fees_usdc) fees, max(drawdown) max_drawdown, avg(mfe_pct) mfe_avg,';
        v_new :=
            '                   learning_financial_normalize_v1(sum(fees_usdc)) fees,' || E'\n'
            || '                   learning_financial_normalize_v1(max(drawdown)) max_drawdown, avg(mfe_pct) mfe_avg,';
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_FEES_DRAWDOWN_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);

        v_old :=
            '               jsonb_build_object(''decisions'',a.decisions,''wins'',a.wins,''losses'',a.losses,''breakeven'',a.breakeven,';
        v_new :=
            '               jsonb_build_object(''financial_normalization_contract'',''LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1'',' || E'\n'
            || '                 ''decisions'',a.decisions,''wins'',a.wins,''losses'',a.losses,''breakeven'',a.breakeven,';
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_PAYLOAD_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);

        v_old :=
            '           OR (v_aggregate->>''gross_profit_usdc'')::NUMERIC' || E'\n'
            || '                IS DISTINCT FROM v_observation.source_gross_profit_usdc';
        v_new :=
            '           OR learning_financial_normalize_v1((v_aggregate->>''gross_profit_usdc'')::NUMERIC)' || E'\n'
            || '                IS DISTINCT FROM learning_financial_normalize_v1(v_observation.source_gross_profit_usdc)';
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_GROSS_PROFIT_GUARD_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);

        v_old :=
            '           OR (v_aggregate->>''gross_loss_usdc'')::NUMERIC' || E'\n'
            || '                IS DISTINCT FROM v_observation.source_gross_loss_usdc';
        v_new :=
            '           OR learning_financial_normalize_v1((v_aggregate->>''gross_loss_usdc'')::NUMERIC)' || E'\n'
            || '                IS DISTINCT FROM learning_financial_normalize_v1(v_observation.source_gross_loss_usdc)';
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_GROSS_LOSS_GUARD_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);

        v_old :=
            '           OR (v_aggregate->>''net_pnl_usdc'')::NUMERIC' || E'\n'
            || '                IS DISTINCT FROM v_observation.source_net_pnl_usdc';
        v_new :=
            '           OR learning_financial_normalize_v1((v_aggregate->>''net_pnl_usdc'')::NUMERIC)' || E'\n'
            || '                IS DISTINCT FROM learning_financial_normalize_v1(v_observation.source_net_pnl_usdc)';
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_NET_GUARD_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);

        v_old :=
            '           OR round((v_aggregate->>''profit_factor'')::NUMERIC,12)' || E'\n'
            || '                IS DISTINCT FROM round(v_observation.source_profit_factor,12)';
        v_new :=
            '           OR learning_financial_normalize_v1((v_aggregate->>''profit_factor'')::NUMERIC)' || E'\n'
            || '                IS DISTINCT FROM learning_financial_normalize_v1(v_observation.source_profit_factor)';
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_PROFIT_FACTOR_GUARD_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);

        v_old :=
            '           OR round((v_aggregate->>''expectancy_usdc'')::NUMERIC,12)' || E'\n'
            || '                IS DISTINCT FROM round(v_observation.source_expectancy_usdc,12)';
        v_new :=
            '           OR learning_financial_normalize_v1((v_aggregate->>''expectancy_usdc'')::NUMERIC)' || E'\n'
            || '                IS DISTINCT FROM learning_financial_normalize_v1(v_observation.source_expectancy_usdc)';
        IF length(v_definition) - length(replace(v_definition, v_old, ''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_EXPECTANCY_GUARD_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition, v_old, v_new);

        EXECUTE v_definition;
    END IF;
END;
$patch_capture$;

DO $postconditions$
DECLARE
    v_capture TEXT := pg_get_functiondef(
        'public.capture_learning_evidence_manifests_v1(bigint)'::regprocedure
    );
BEGIN
    IF to_regprocedure(
           'public.learning_financial_normalize_v1(numeric)'
       ) IS NULL
       OR position(
           '''financial_normalization_contract'',''LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1'''
           IN v_capture
       ) = 0
       OR position(
           'learning_financial_normalize_v1(sum(realized_pnl_usdc)) net_pnl'
           IN v_capture
       ) = 0
       OR position(
           'IS DISTINCT FROM learning_financial_normalize_v1(v_observation.source_net_pnl_usdc)'
           IN v_capture
       ) = 0
       OR position(
           'v_aggregate_hash := encode(digest(v_aggregate::text,''sha256''),''hex'')'
           IN v_capture
       ) = 0 THEN
        RAISE EXCEPTION
            'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1_POSTCONDITION_FAILED';
    END IF;
END;
$postconditions$;

INSERT INTO public.schema_migration_ledger_v1(
    migration_id, checksum_sha256, environment, deployment_id, database_name,
    applied_by, status, success, execution_duration_ms, git_sha,
    schema_baseline_version
)
SELECT
    '20260805_learning_evidence_aggregate_numeric_parity_v1.sql',
    '0e09144631b9dfe0c9300eb8c435edb4bc7c9d2e209115091664924353e0ea82',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1', current_database(),
    'operator-migration', 'APPLIED', TRUE, 0,
    '60bbf1000793fa9f6fa972a363099940908d7ed8',
    'LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1'
WHERE NOT EXISTS (
    SELECT 1
      FROM public.schema_migration_ledger_v1
     WHERE migration_id =
           '20260805_learning_evidence_aggregate_numeric_parity_v1.sql'
);

COMMIT;

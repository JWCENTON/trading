BEGIN;

DO $$
DECLARE
    v_function_definition text;
    v_updated_definition text;
    v_old_fragment text;
    v_new_fragment text;
BEGIN
    IF to_regprocedure(
        'refresh_decision_identity_outcome_v1(integer,text,text,uuid)'
    ) IS NULL THEN
        RAISE EXCEPTION
            'DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1 requires refresh_decision_identity_outcome_v1(integer,text,text,uuid)';
    END IF;

    IF to_regclass('canonical_financial_truth_v1') IS NULL THEN
        RAISE EXCEPTION
            'DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1 requires canonical_financial_truth_v1';
    END IF;

    SELECT pg_get_functiondef(
               'refresh_decision_identity_outcome_v1(integer,text,text,uuid)'::regprocedure
           )
      INTO v_function_definition;

    IF position('DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1' IN v_function_definition) > 0 THEN
        RETURN;
    END IF;

    v_updated_definition := v_function_definition;

    v_old_fragment := $fragment$
            p.gross_pnl_usdc,
            p.fees_usdc,
            p.net_pnl_usdc,
            e.mfe_pct,$fragment$;
    v_new_fragment := $fragment$
            CASE
                WHEN financial_truth.financial_truth_status = 'COMPLETE'
                    THEN financial_truth.authoritative_gross_pnl
                ELSE NULL
            END AS gross_pnl_usdc,
            CASE
                WHEN financial_truth.financial_truth_status = 'COMPLETE'
                    THEN COALESCE(
                        financial_truth.authoritative_fees_usdc,
                        financial_truth.authoritative_entry_fees_usdc
                            + financial_truth.authoritative_exit_fees_usdc
                    )
                ELSE NULL
            END AS fees_usdc,
            CASE
                WHEN financial_truth.financial_truth_status = 'COMPLETE'
                    THEN financial_truth.authoritative_net_pnl
                ELSE NULL
            END AS net_pnl_usdc,
            financial_truth.financial_truth_status,
            e.mfe_pct,$fragment$;

    IF position(v_old_fragment IN v_updated_definition) = 0 THEN
        RAISE EXCEPTION
            'DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1 economics anchor not found';
    END IF;
    v_updated_definition := replace(
        v_updated_definition,
        v_old_fragment,
        v_new_fragment
    );

    v_old_fragment := $fragment$
        FROM decision_registry_v1 d
        JOIN positions p ON p.id = d.position_id
        LEFT JOIN exit_trace_v1 e ON e.position_id = p.id$fragment$;
    v_new_fragment := $fragment$
        FROM decision_registry_v1 d
        JOIN positions p ON p.id = d.position_id
        LEFT JOIN canonical_financial_truth_v1 financial_truth
          ON financial_truth.position_id = p.id
        LEFT JOIN exit_trace_v1 e ON e.position_id = p.id$fragment$;

    IF position(v_old_fragment IN v_updated_definition) = 0 THEN
        RAISE EXCEPTION
            'DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1 source join anchor not found';
    END IF;
    v_updated_definition := replace(
        v_updated_definition,
        v_old_fragment,
        v_new_fragment
    );

    v_old_fragment := $fragment$
            CASE WHEN s.net_pnl_usdc IS NULL THEN 'PARTIAL' ELSE 'COMPLETE' END,
            CASE WHEN s.net_pnl_usdc IS NULL
                THEN 'Closed position has incomplete net PnL'
                ELSE s.exit_reason END,$fragment$;
    v_new_fragment := $fragment$
            CASE
                WHEN s.financial_truth_status = 'COMPLETE'
                 AND s.gross_pnl_usdc IS NOT NULL
                 AND s.fees_usdc IS NOT NULL
                 AND s.net_pnl_usdc IS NOT NULL
                    THEN 'COMPLETE'
                ELSE 'PARTIAL'
            END,
            CASE
                WHEN s.financial_truth_status IS DISTINCT FROM 'COMPLETE'
                    THEN 'Canonical Financial Truth is not COMPLETE'
                WHEN s.gross_pnl_usdc IS NULL
                  OR s.fees_usdc IS NULL
                  OR s.net_pnl_usdc IS NULL
                    THEN 'Canonical Financial Truth COMPLETE economics are incomplete'
                ELSE s.exit_reason
            END,$fragment$;

    IF position(v_old_fragment IN v_updated_definition) = 0 THEN
        RAISE EXCEPTION
            'DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1 outcome status anchor not found';
    END IF;
    v_updated_definition := replace(
        v_updated_definition,
        v_old_fragment,
        v_new_fragment
    );

    v_old_fragment := $fragment$
            jsonb_build_object(
                'exit_time', s.exit_time,
                'exit_reason', s.exit_reason,
                'path_source', CASE WHEN s.mfe_pct IS NULL AND s.mae_pct IS NULL
                    THEN 'missing' ELSE 'exit_trace_v1' END
            ),$fragment$;
    v_new_fragment := $fragment$
            jsonb_build_object(
                'exit_time', s.exit_time,
                'exit_reason', s.exit_reason,
                'path_source', CASE WHEN s.mfe_pct IS NULL AND s.mae_pct IS NULL
                    THEN 'missing' ELSE 'exit_trace_v1' END,
                'economics_source', CASE
                    WHEN s.financial_truth_status = 'COMPLETE'
                        THEN 'CANONICAL_FINANCIAL_TRUTH_V1'
                    ELSE 'UNRESOLVED'
                END,
                'financial_truth_status', COALESCE(
                    s.financial_truth_status,
                    'ABSENT'
                ),
                'projection_contract', 'DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1'
            ),$fragment$;

    IF position(v_old_fragment IN v_updated_definition) = 0 THEN
        RAISE EXCEPTION
            'DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1 evidence anchor not found';
    END IF;
    v_updated_definition := replace(
        v_updated_definition,
        v_old_fragment,
        v_new_fragment
    );

    EXECUTE v_updated_definition;
END
$$;

DO $$
DECLARE
    v_function_definition text;
    v_source_outcomes text;
BEGIN
    SELECT pg_get_functiondef(
               'refresh_decision_identity_outcome_v1(integer,text,text,uuid)'::regprocedure
           )
      INTO v_function_definition;

    IF position('DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1' IN v_function_definition) = 0
       OR position('canonical_financial_truth_v1 financial_truth' IN v_function_definition) = 0
       OR position('financial_truth.authoritative_gross_pnl' IN v_function_definition) = 0
       OR position('financial_truth.authoritative_fees_usdc' IN v_function_definition) = 0
       OR position('financial_truth.authoritative_net_pnl' IN v_function_definition) = 0 THEN
        RAISE EXCEPTION
            'DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1 postcondition failed: canonical authority missing';
    END IF;

    v_source_outcomes := split_part(
        split_part(v_function_definition, 'source_outcomes AS (', 2),
        '), upserted AS (',
        1
    );

    IF position('p.gross_pnl_usdc' IN v_source_outcomes) > 0
       OR position('p.fees_usdc' IN v_source_outcomes) > 0
       OR position('p.net_pnl_usdc' IN v_source_outcomes) > 0 THEN
        RAISE EXCEPTION
            'DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1 postcondition failed: positions economics remain authoritative';
    END IF;
END
$$;

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260813_decision_outcome_canonical_financial_truth_source_v1.sql',
    COALESCE(NULLIF(current_setting('waltrade.migration_checksum',true),''),
             repeat('0',64)),
    'PAPER','LOCAL',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    COALESCE(NULLIF(current_setting('waltrade.git_sha',true),''),repeat('0',40)),
    'DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
     WHERE migration_id=
           '20260813_decision_outcome_canonical_financial_truth_source_v1.sql'
);

COMMIT;

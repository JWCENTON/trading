\set ON_ERROR_STOP on

BEGIN;

DO $repair_producer$
DECLARE
    original_definition TEXT;
    corrected_definition TEXT;
    old_fragment TEXT := $old$WHEN jsonb_typeof(snapshot.strategy_features->'price')='number'
                    THEN (snapshot.strategy_features->>'price')::numeric$old$;
    new_fragment TEXT := $new$WHEN pg_input_is_valid(
                    snapshot.strategy_features->>'price','numeric'
                ) THEN (snapshot.strategy_features->>'price')::numeric$new$;
BEGIN
    SELECT pg_get_functiondef(
        'public.refresh_entry_opportunity_bounded_horizon_labels_v1(text,text,integer)'::regprocedure
    ) INTO original_definition;
    corrected_definition := replace(original_definition,old_fragment,new_fragment);
    IF corrected_definition=original_definition THEN
        IF original_definition NOT LIKE '%pg_input_is_valid(%strategy_features->>''price''%''numeric''%' THEN
            RAISE EXCEPTION 'BOUNDED_HORIZON_REFERENCE_PARSER_REPAIR_TARGET_NOT_FOUND';
        END IF;
    ELSE
        EXECUTE corrected_definition;
    END IF;
END
$repair_producer$;

CREATE OR REPLACE FUNCTION public.guard_entry_opportunity_bounded_label_immutable_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    IF OLD.label_status='COMPLETE' THEN
        RAISE EXCEPTION 'ENTRY_OPPORTUNITY_BOUNDED_HORIZON_LABEL_IMMUTABLE';
    END IF;
    IF TG_OP='DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$function$;

DELETE FROM public.entry_opportunity_bounded_horizon_labels_v1 label
USING public.entry_opportunity_evidence_v1 snapshot
WHERE label.snapshot_id=snapshot.snapshot_id
  AND label.target_version='NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_V1'
  AND label.producer_version='NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_PRODUCER_V1'
  AND label.label_status='INVALID_REFERENCE'
  AND label.reference_price IS NULL
  AND snapshot.signal_action IN ('BUY','SELL')
  AND pg_input_is_valid(snapshot.strategy_features->>'price','numeric')
  AND (snapshot.strategy_features->>'price')::numeric>0
  AND pg_input_is_valid(
      snapshot.strategy_features->>'signal_created_at','timestamp with time zone'
  );

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260815_next_full_minute_bounded_horizon_mfe_v1_1_reference_fix.sql',
    COALESCE(
        NULLIF(current_setting('waltrade.migration_checksum',true),''),
        repeat('0',64)
    ),
    'PAPER',
    COALESCE(
        NULLIF(current_setting('waltrade.target_deployment_id',true),''),
        'LOCAL'
    ),
    current_database(),'operator-migration','APPLIED',TRUE,0,
    COALESCE(
        NULLIF(current_setting('waltrade.git_sha',true),''),repeat('0',40)
    ),
    'NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_V1_1_REFERENCE_FIX'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id=
      '20260815_next_full_minute_bounded_horizon_mfe_v1_1_reference_fix.sql'
);

COMMIT;

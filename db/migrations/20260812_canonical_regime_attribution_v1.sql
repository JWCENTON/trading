-- WALTRADE CANONICAL REGIME ATTRIBUTION V1
-- Forward-only PAPER decision lineage; historical NULL values stay frozen.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $paper_only$
BEGIN
    IF current_database()<>'trading_paper' THEN
        RAISE EXCEPTION 'CANONICAL_REGIME_ATTRIBUTION_PAPER_ONLY';
    END IF;
END;
$paper_only$;

CREATE OR REPLACE FUNCTION public.register_forward_entry_decision_v1(
    p_symbol TEXT,
    p_interval TEXT,
    p_strategy TEXT,
    p_side TEXT,
    p_price NUMERIC,
    p_quantity NUMERIC,
    p_reason TEXT,
    p_candle_open_time TIMESTAMPTZ,
    p_market_regime TEXT,
    p_regime_source JSONB
)
RETURNS UUID
LANGUAGE plpgsql
AS $function$
DECLARE
    v_namespace CONSTANT UUID :=
        'd40e98ca-a837-5a56-af74-fdce3f0a7aa4';
    v_environment CONSTANT TEXT := 'trading_paper';
    v_runtime_deployment_id TEXT;
    v_source_revision TEXT;
    v_deployment_id TEXT;
    v_identity JSONB;
    v_payload JSONB;
    v_source_record_id TEXT;
    v_payload_fingerprint TEXT;
    v_decision_id UUID;
    v_existing RECORD;
BEGIN
    SELECT CASE WHEN count(DISTINCT deployment_id)=1
                THEN min(deployment_id) END,
           CASE WHEN count(DISTINCT git_revision)=1
                THEN min(git_revision) END
      INTO v_runtime_deployment_id,v_source_revision
      FROM public.runtime_contract_adoption_v2
     WHERE contract_name='FEE_AWARE_INVENTORY_C2_2'
       AND environment='paper'
       AND status='ACTIVE';
    IF v_runtime_deployment_id IS NULL OR v_source_revision IS NULL
       OR v_runtime_deployment_id !~ '^(local|vps)-paper$' THEN
        RAISE EXCEPTION
            'FORWARD_DECISION_REGISTRY_RUNTIME_IDENTITY_MISSING';
    END IF;
    v_deployment_id := upper(split_part(v_runtime_deployment_id,'-',1));

    IF nullif(btrim(p_symbol),'') IS NULL
       OR nullif(btrim(p_interval),'') IS NULL
       OR upper(p_strategy) NOT IN ('RSI','TREND','SUPERTREND','BBRANGE')
       OR p_strategy<>upper(p_strategy)
       OR upper(p_side) NOT IN ('BUY','SELL')
       OR p_price IS NULL OR p_price<0
       OR p_quantity IS NULL OR p_quantity<=0
       OR p_candle_open_time IS NULL
       OR nullif(btrim(p_market_regime),'') IS NULL
       OR jsonb_typeof(p_regime_source)<>'object'
       OR p_regime_source->>'regime_attribution_version'
            <>'CANONICAL_REGIME_ATTRIBUTION_V1'
       OR p_regime_source->>'regime_source'<>'market_regime'
       OR upper(p_regime_source->>'regime_source_symbol')<>upper(p_symbol)
       OR lower(p_regime_source->>'regime_source_interval')<>lower(p_interval)
       OR nullif(p_regime_source->>'regime_source_ts','') IS NULL
       OR (p_regime_source->>'regime_source_ts')::TIMESTAMPTZ
            > p_candle_open_time THEN
        RAISE EXCEPTION 'CANONICAL_REGIME_ATTRIBUTION_PAYLOAD_INVALID';
    END IF;

    v_identity := jsonb_build_object(
        'contract','FORWARD_DECISION_REGISTRY_CONTINUITY_V1',
        'regime_attribution_version','CANONICAL_REGIME_ATTRIBUTION_V1',
        'runtime_deployment_id',v_runtime_deployment_id,
        'environment',v_environment,
        'symbol',upper(p_symbol),'interval',lower(p_interval),
        'strategy',p_strategy,
        'decision_timestamp',p_candle_open_time,
        'decision_type','ENTRY_DECISION',
        'market_regime',p_market_regime,
        'regime_source_ts',p_regime_source->>'regime_source_ts'
    );
    v_source_record_id := encode(digest(v_identity::TEXT,'sha256'),'hex');
    v_decision_id := public.waltrade_uuid_v5_v1(
        v_namespace,v_source_record_id
    );
    v_payload := jsonb_build_object(
        'contract_version','FORWARD_DECISION_REGISTRY_CONTINUITY_V1',
        'regime_attribution_version','CANONICAL_REGIME_ATTRIBUTION_V1',
        'producer_identity','bot-'||lower(p_strategy),
        'source_revision',v_source_revision,
        'runtime_deployment_id',v_runtime_deployment_id,
        'decision_identity',v_identity,
        'final_action','EXECUTE',
        'execution_side',upper(p_side),
        'price',p_price,'quantity',p_quantity,
        'reason',COALESCE(p_reason,''),
        'market_regime',p_market_regime,
        'regime_source',p_regime_source,
        'order_correlation_identity',v_source_record_id
    );
    v_payload_fingerprint := encode(digest(v_payload::TEXT,'sha256'),'hex');
    v_payload := v_payload || jsonb_build_object(
        'decision_payload_fingerprint',v_payload_fingerprint
    );

    INSERT INTO public.decision_registry_v1(
        decision_id,legacy_decision_key,deployment_id,environment,
        decision_type,decision_source,symbol,interval,strategy,market_regime,
        decision_timestamp,source_table,source_record_id,
        source_natural_key,source_created_at,observed_at,
        engine_name,engine_version,schema_version,decision_action,
        decision_reason,decision_payload
    ) VALUES (
        v_decision_id,v_source_record_id,v_deployment_id,v_environment,
        'ENTRY_DECISION','FINAL_DECISION_EXECUTION_EPILOG',
        upper(p_symbol),lower(p_interval),p_strategy,p_market_regime,
        p_candle_open_time,'simulated_orders_forward_v1',v_source_record_id,
        'CANONICAL_REGIME_ATTRIBUTION_V1|'||v_source_record_id,
        p_candle_open_time,clock_timestamp(),
        'COMMON_SIMULATED_ORDER_WRITER',
        'FORWARD_DECISION_REGISTRY_CONTINUITY_V1',
        'FORWARD_DECISION_REGISTRY_V1','EXECUTE',p_reason,v_payload
    )
    ON CONFLICT(
        deployment_id,environment,source_table,source_record_id,decision_type
    ) DO NOTHING;

    SELECT decision_id,
           decision_payload->>'decision_payload_fingerprint' fingerprint,
           market_regime
      INTO STRICT v_existing
      FROM public.decision_registry_v1
     WHERE deployment_id=v_deployment_id
       AND environment=v_environment
       AND source_table='simulated_orders_forward_v1'
       AND source_record_id=v_source_record_id
       AND decision_type='ENTRY_DECISION'
     FOR SHARE;
    IF v_existing.decision_id<>v_decision_id
       OR v_existing.fingerprint IS DISTINCT FROM v_payload_fingerprint
       OR v_existing.market_regime IS DISTINCT FROM p_market_regime THEN
        RAISE EXCEPTION
            'FORWARD_DECISION_REGISTRY_IDEMPOTENCY_CONFLICT:%',
            v_source_record_id;
    END IF;
    RETURN v_decision_id;
END;
$function$;

CREATE OR REPLACE FUNCTION public.require_forward_entry_decision_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
DECLARE v_registry RECORD;
BEGIN
    IF NEW.order_class='FORWARD' AND NOT NEW.is_exit THEN
        IF NEW.decision_id IS NULL
           OR NEW.decision_contract_version<>'CANONICAL_REGIME_ATTRIBUTION_V1'
        THEN
            RAISE EXCEPTION 'CANONICAL_REGIME_ATTRIBUTION_REQUIRED';
        END IF;
        SELECT symbol,interval,strategy,decision_timestamp,market_regime,
               decision_payload->>'regime_attribution_version' version
          INTO STRICT v_registry
          FROM public.decision_registry_v1
         WHERE decision_id=NEW.decision_id
           AND decision_type='ENTRY_DECISION'
         FOR SHARE;
        IF (v_registry.symbol,v_registry.interval,v_registry.strategy,
            v_registry.decision_timestamp)
           IS DISTINCT FROM
           (upper(NEW.symbol),lower(NEW.interval),upper(NEW.strategy),
            NEW.candle_open_time)
           OR v_registry.market_regime IS NULL
           OR v_registry.version<>'CANONICAL_REGIME_ATTRIBUTION_V1' THEN
            RAISE EXCEPTION 'CANONICAL_REGIME_ATTRIBUTION_CONFLICT';
        END IF;
    END IF;
    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.correlate_forward_entry_fill_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
DECLARE v_order RECORD; v_registry_regime TEXT;
BEGIN
    IF NEW.order_purpose<>'ENTRY' THEN RETURN NEW; END IF;
    SELECT decision_id,decision_contract_version,is_exit,order_class
      INTO STRICT v_order
      FROM public.simulated_orders
     WHERE id=NEW.simulated_order_id
     FOR SHARE;
    IF v_order.order_class='FORWARD' THEN
        IF v_order.is_exit OR v_order.decision_id IS NULL
           OR v_order.decision_contract_version<>
              'CANONICAL_REGIME_ATTRIBUTION_V1' THEN
            RAISE EXCEPTION 'FORWARD_DECISION_ORDER_CORRELATION_INVALID';
        END IF;
        SELECT market_regime INTO STRICT v_registry_regime
          FROM public.decision_registry_v1
         WHERE decision_id=v_order.decision_id FOR SHARE;
        IF v_registry_regime IS NULL OR NOT EXISTS (
            SELECT 1 FROM public.positions
             WHERE id=NEW.position_id
               AND market_regime IS NOT DISTINCT FROM v_registry_regime
        ) THEN
            RAISE EXCEPTION 'FORWARD_POSITION_REGIME_CONFLICT';
        END IF;
        NEW.decision_id := v_order.decision_id;
        NEW.decision_contract_version := v_order.decision_contract_version;
        UPDATE public.decision_registry_v1
           SET position_id=NEW.position_id,
               decision_payload=decision_payload||jsonb_build_object(
                   'simulated_order_id',NEW.simulated_order_id,
                   'position_id',NEW.position_id
               ),
               refreshed_at=clock_timestamp()
         WHERE decision_id=v_order.decision_id
           AND decision_type='ENTRY_DECISION'
           AND (position_id IS NULL
                OR position_id IS NOT DISTINCT FROM NEW.position_id);
        IF NOT FOUND THEN
            RAISE EXCEPTION 'FORWARD_DECISION_POSITION_CORRELATION_CONFLICT';
        END IF;
    END IF;
    RETURN NEW;
END;
$function$;

DO $postconditions$
BEGIN
    IF to_regprocedure(
        'public.register_forward_entry_decision_v1(text,text,text,text,numeric,numeric,text,timestamp with time zone,text,jsonb)'
       ) IS NULL
       OR position('CANONICAL_REGIME_ATTRIBUTION_V1' IN pg_get_functiondef(
           'public.require_forward_entry_decision_v1()'::regprocedure))=0
       OR position('FORWARD_POSITION_REGIME_CONFLICT' IN pg_get_functiondef(
           'public.correlate_forward_entry_fill_v1()'::regprocedure))=0 THEN
        RAISE EXCEPTION
            'CANONICAL_REGIME_ATTRIBUTION_V1_POSTCONDITION_FAILED';
    END IF;
END;
$postconditions$;

COMMENT ON FUNCTION public.register_forward_entry_decision_v1(
    TEXT,TEXT,TEXT,TEXT,NUMERIC,NUMERIC,TEXT,TIMESTAMPTZ,TEXT,JSONB
) IS 'Frozen PAPER decision regime; CANONICAL_REGIME_ATTRIBUTION_V1.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260812_canonical_regime_attribution_v1.sql',
    COALESCE(NULLIF(current_setting('waltrade.migration_checksum',true),''),
             repeat('0',64)),
    'PAPER','CANONICAL_REGIME_ATTRIBUTION_V1',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    COALESCE(NULLIF(current_setting('waltrade.git_sha',true),''),repeat('0',40)),
    'CANONICAL_REGIME_ATTRIBUTION_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
     WHERE migration_id='20260812_canonical_regime_attribution_v1.sql'
);

COMMIT;

-- WALTRADE FORWARD DECISION REGISTRY CONTINUITY V1
-- Forward-only identity guard for canonical simulated entry execution.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $prerequisites$
BEGIN
    IF to_regclass('public.decision_registry_v1') IS NULL
       OR to_regclass('public.decision_outcomes_v1') IS NULL
       OR to_regclass('public.simulated_orders') IS NULL
       OR to_regclass('public.simulated_execution_fills_v1') IS NULL
       OR to_regclass('public.positions') IS NULL
       OR to_regclass('public.runtime_contract_adoption_v2') IS NULL
       OR to_regclass('public.canonical_financial_truth_v1') IS NULL
       OR to_regclass('public.schema_migration_ledger_v1') IS NULL
       OR to_regprocedure('public.waltrade_uuid_v5_v1(uuid,text)') IS NULL
       OR to_regprocedure(
           'public.refresh_decision_identity_outcome_v1(integer,text,text,uuid)'
       ) IS NULL THEN
        RAISE EXCEPTION
            'FORWARD_DECISION_REGISTRY_CONTINUITY_V1_PREREQUISITE_MISSING';
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema='public' AND table_name='simulated_orders'
           AND column_name='order_class'
    ) THEN
        RAISE EXCEPTION
            'FORWARD_DECISION_REGISTRY_CONTINUITY_V1_NAMESPACE_REQUIRED';
    END IF;
END;
$prerequisites$;

ALTER TABLE public.simulated_orders
    ADD COLUMN IF NOT EXISTS decision_id UUID,
    ADD COLUMN IF NOT EXISTS decision_contract_version TEXT;

ALTER TABLE public.simulated_execution_fills_v1
    ADD COLUMN IF NOT EXISTS decision_id UUID,
    ADD COLUMN IF NOT EXISTS decision_contract_version TEXT;

ALTER TABLE public.decision_registry_v1
    DROP CONSTRAINT IF EXISTS ck_decision_registry_type;
ALTER TABLE public.decision_registry_v1
    ADD CONSTRAINT ck_decision_registry_type CHECK (
        decision_type IN (
            'ENTRY_DECISION', 'TRADE_EXECUTED', 'NO_TRADE',
            'SIGNAL_REJECTED', 'ENTRY_BLOCKED', 'ENTRY_SUPPRESSED',
            'PAPER_SIMULATION'
        )
    );

DO $foreign_keys$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname='simulated_orders_decision_registry_v1_fk'
           AND conrelid='public.simulated_orders'::regclass
    ) THEN
        ALTER TABLE public.simulated_orders
            ADD CONSTRAINT simulated_orders_decision_registry_v1_fk
            FOREIGN KEY(decision_id)
            REFERENCES public.decision_registry_v1(decision_id)
            NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname='simulated_fills_decision_registry_v1_fk'
           AND conrelid='public.simulated_execution_fills_v1'::regclass
    ) THEN
        ALTER TABLE public.simulated_execution_fills_v1
            ADD CONSTRAINT simulated_fills_decision_registry_v1_fk
            FOREIGN KEY(decision_id)
            REFERENCES public.decision_registry_v1(decision_id)
            NOT VALID;
    END IF;
END;
$foreign_keys$;

CREATE UNIQUE INDEX IF NOT EXISTS
    ux_simulated_orders_forward_decision_v1
ON public.simulated_orders(decision_id)
WHERE decision_id IS NOT NULL AND NOT is_exit;

CREATE UNIQUE INDEX IF NOT EXISTS
    ux_simulated_fills_forward_entry_decision_v1
ON public.simulated_execution_fills_v1(decision_id)
WHERE decision_id IS NOT NULL AND order_purpose='ENTRY';

CREATE OR REPLACE FUNCTION public.register_forward_entry_decision_v1(
    p_symbol TEXT,
    p_interval TEXT,
    p_strategy TEXT,
    p_side TEXT,
    p_price NUMERIC,
    p_quantity NUMERIC,
    p_reason TEXT,
    p_candle_open_time TIMESTAMPTZ
)
RETURNS UUID
LANGUAGE plpgsql
AS $function$
DECLARE
    v_namespace CONSTANT UUID :=
        'd40e98ca-a837-5a56-af74-fdce3f0a7aa4';
    v_environment TEXT := current_database();
    v_runtime_environment TEXT;
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
    IF v_environment NOT IN ('trading_paper','trading_live') THEN
        RAISE EXCEPTION
            'FORWARD_DECISION_REGISTRY_ENVIRONMENT_INVALID:%',v_environment;
    END IF;
    v_runtime_environment := CASE v_environment
        WHEN 'trading_paper' THEN 'paper' ELSE 'live' END;

    SELECT CASE WHEN count(DISTINCT deployment_id)=1
                THEN min(deployment_id) END,
           CASE WHEN count(DISTINCT git_revision)=1
                THEN min(git_revision) END
      INTO v_runtime_deployment_id,v_source_revision
      FROM public.runtime_contract_adoption_v2
     WHERE contract_name='FEE_AWARE_INVENTORY_C2_2'
       AND environment=v_runtime_environment
       AND status='ACTIVE';
    IF v_runtime_deployment_id IS NULL OR v_source_revision IS NULL
       OR v_runtime_deployment_id !~ '^(local|vps)-(paper|live)$'
       OR split_part(v_runtime_deployment_id,'-',2)<>v_runtime_environment THEN
        RAISE EXCEPTION
            'FORWARD_DECISION_REGISTRY_RUNTIME_IDENTITY_MISSING';
    END IF;
    v_deployment_id := upper(split_part(v_runtime_deployment_id,'-',1));

    IF nullif(btrim(p_symbol),'') IS NULL
       OR nullif(btrim(p_interval),'') IS NULL
       OR nullif(btrim(p_strategy),'') IS NULL
       OR upper(p_side) NOT IN ('BUY','SELL')
       OR p_price IS NULL OR p_price<0
       OR p_quantity IS NULL OR p_quantity<=0
       OR p_candle_open_time IS NULL THEN
        RAISE EXCEPTION 'FORWARD_DECISION_REGISTRY_PAYLOAD_INVALID';
    END IF;

    v_identity := jsonb_build_object(
        'contract','FORWARD_DECISION_REGISTRY_CONTINUITY_V1',
        'runtime_deployment_id',v_runtime_deployment_id,
        'environment',v_environment,
        'symbol',upper(p_symbol),'interval',lower(p_interval),
        'strategy',upper(p_strategy),
        'decision_timestamp',p_candle_open_time,
        'decision_type','ENTRY_DECISION'
    );
    v_source_record_id := encode(
        digest(v_identity::TEXT,'sha256'),'hex'
    );
    v_decision_id := public.waltrade_uuid_v5_v1(
        v_namespace,v_source_record_id
    );
    v_payload := jsonb_build_object(
        'contract_version','FORWARD_DECISION_REGISTRY_CONTINUITY_V1',
        'producer_identity','bot-'||lower(p_strategy),
        'source_revision',v_source_revision,
        'runtime_deployment_id',v_runtime_deployment_id,
        'decision_identity',v_identity,
        'final_action','EXECUTE',
        'execution_side',upper(p_side),
        'price',p_price,'quantity',p_quantity,
        'reason',COALESCE(p_reason,''),
        'order_correlation_identity',v_source_record_id
    );
    v_payload_fingerprint := encode(
        digest(v_payload::TEXT,'sha256'),'hex'
    );
    v_payload := v_payload || jsonb_build_object(
        'decision_payload_fingerprint',v_payload_fingerprint
    );

    INSERT INTO public.decision_registry_v1(
        decision_id,legacy_decision_key,deployment_id,environment,
        decision_type,decision_source,symbol,interval,strategy,
        decision_timestamp,source_table,source_record_id,
        source_natural_key,source_created_at,observed_at,
        engine_name,engine_version,schema_version,decision_action,
        decision_reason,decision_payload
    ) VALUES (
        v_decision_id,v_source_record_id,v_deployment_id,v_environment,
        'ENTRY_DECISION','FINAL_DECISION_EXECUTION_EPILOG',
        upper(p_symbol),lower(p_interval),upper(p_strategy),
        p_candle_open_time,'simulated_orders_forward_v1',
        v_source_record_id,
        'FORWARD_DECISION_REGISTRY_CONTINUITY_V1|'||v_source_record_id,
        p_candle_open_time,clock_timestamp(),
        'COMMON_SIMULATED_ORDER_WRITER',
        'FORWARD_DECISION_REGISTRY_CONTINUITY_V1',
        'FORWARD_DECISION_REGISTRY_V1','EXECUTE',p_reason,v_payload
    )
    ON CONFLICT(
        deployment_id,environment,source_table,source_record_id,decision_type
    ) DO NOTHING;

    SELECT decision_id,decision_payload->>'decision_payload_fingerprint' fingerprint,
           position_id
      INTO STRICT v_existing
      FROM public.decision_registry_v1
     WHERE deployment_id=v_deployment_id
       AND environment=v_environment
       AND source_table='simulated_orders_forward_v1'
       AND source_record_id=v_source_record_id
       AND decision_type='ENTRY_DECISION'
     FOR SHARE;
    IF v_existing.decision_id<>v_decision_id
       OR v_existing.fingerprint IS DISTINCT FROM v_payload_fingerprint THEN
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
BEGIN
    IF NEW.order_class='FORWARD' AND NOT NEW.is_exit THEN
        NEW.decision_id := public.register_forward_entry_decision_v1(
            NEW.symbol,NEW.interval,NEW.strategy,NEW.side,NEW.price,
            NEW.quantity_btc,NEW.reason,NEW.candle_open_time
        );
        NEW.decision_contract_version :=
            'FORWARD_DECISION_REGISTRY_CONTINUITY_V1';
        IF NEW.decision_id IS NULL THEN
            RAISE EXCEPTION 'FORWARD_DECISION_REGISTRY_REQUIRED';
        END IF;
    END IF;
    RETURN NEW;
END;
$function$;

DROP TRIGGER IF EXISTS forward_entry_decision_registry_v1
ON public.simulated_orders;
CREATE TRIGGER forward_entry_decision_registry_v1
BEFORE INSERT ON public.simulated_orders
FOR EACH ROW EXECUTE FUNCTION public.require_forward_entry_decision_v1();

CREATE OR REPLACE FUNCTION public.correlate_forward_entry_fill_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
DECLARE
    v_order RECORD;
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
              'FORWARD_DECISION_REGISTRY_CONTINUITY_V1' THEN
            RAISE EXCEPTION 'FORWARD_DECISION_ORDER_CORRELATION_INVALID';
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
           AND (
               position_id IS NULL
               OR position_id IS NOT DISTINCT FROM NEW.position_id
           );
        IF NOT FOUND THEN
            RAISE EXCEPTION 'FORWARD_DECISION_POSITION_CORRELATION_CONFLICT';
        END IF;
    END IF;
    RETURN NEW;
END;
$function$;

DROP TRIGGER IF EXISTS forward_entry_fill_correlation_v1
ON public.simulated_execution_fills_v1;
CREATE TRIGGER forward_entry_fill_correlation_v1
BEFORE INSERT ON public.simulated_execution_fills_v1
FOR EACH ROW EXECUTE FUNCTION public.correlate_forward_entry_fill_v1();

DO $patch_outcome_adapter$
DECLARE
    v_signature CONSTANT TEXT :=
        'public.refresh_decision_identity_outcome_v1(integer,text,text,uuid)';
    v_definition TEXT := pg_get_functiondef(to_regprocedure(v_signature));
    v_old TEXT;
    v_new TEXT;
BEGIN
    IF position('FORWARD_DECISION_REGISTRY_CONTINUITY_V1' IN v_definition)=0 THEN
        v_old := '        FROM positions p' || E'\n'
            || '        WHERE p.entry_time >= clock_timestamp() - make_interval(hours => p_lookback_hours)' || E'\n'
            || '    ), upserted AS (';
        v_new := '        FROM positions p' || E'\n'
            || '        WHERE p.entry_time >= clock_timestamp() - make_interval(hours => p_lookback_hours)' || E'\n'
            || '          AND NOT EXISTS (' || E'\n'
            || '              SELECT 1 FROM decision_registry_v1 forward_decision' || E'\n'
            || '               WHERE forward_decision.position_id=p.id' || E'\n'
            || '                 AND forward_decision.deployment_id=p_deployment_id' || E'\n'
            || '                 AND forward_decision.environment=p_environment' || E'\n'
            || '                 AND forward_decision.decision_type=''ENTRY_DECISION''' || E'\n'
            || '                 AND forward_decision.engine_version=' || E'\n'
            || '                     ''FORWARD_DECISION_REGISTRY_CONTINUITY_V1''' || E'\n'
            || '          )' || E'\n'
            || '    ), upserted AS (';
        IF length(v_definition)-length(replace(v_definition,v_old,''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'FORWARD_DECISION_REGISTRY_SOURCE_ANCHOR_CONFLICT';
        END IF;
        v_definition := replace(v_definition,v_old,v_new);

        v_old := '          AND d.decision_type = ''TRADE_EXECUTED''' || E'\n'
            || '          AND p.exit_time IS NOT NULL';
        v_new := '          AND d.decision_type IN (''TRADE_EXECUTED'',''ENTRY_DECISION'')' || E'\n'
            || '          AND p.exit_time IS NOT NULL' || E'\n'
            || '          AND (d.decision_type<>''ENTRY_DECISION'' OR EXISTS (' || E'\n'
            || '              SELECT 1 FROM canonical_financial_truth_v1 financial_truth' || E'\n'
            || '               WHERE financial_truth.position_id=p.id' || E'\n'
            || '                 AND financial_truth.financial_truth_status=''COMPLETE''' || E'\n'
            || '          ))';
        IF length(v_definition)-length(replace(v_definition,v_old,''))
           <> length(v_old) THEN
            RAISE EXCEPTION
                'FORWARD_DECISION_OUTCOME_ANCHOR_CONFLICT';
        END IF;
        EXECUTE replace(v_definition,v_old,v_new);
    END IF;
END;
$patch_outcome_adapter$;

DO $postconditions$
DECLARE
    v_outcome TEXT := pg_get_functiondef(
        'public.refresh_decision_identity_outcome_v1(integer,text,text,uuid)'::regprocedure
    );
BEGIN
    IF to_regprocedure(
           'public.register_forward_entry_decision_v1(text,text,text,text,numeric,numeric,text,timestamp with time zone)'
       ) IS NULL
       OR position('FORWARD_DECISION_REGISTRY_CONTINUITY_V1' IN v_outcome)=0
       OR position('financial_truth.financial_truth_status=''COMPLETE''' IN v_outcome)=0
       OR NOT EXISTS (
           SELECT 1 FROM pg_trigger
            WHERE tgrelid='public.simulated_orders'::regclass
              AND tgname='forward_entry_decision_registry_v1'
              AND tgenabled<>'D'
       )
       OR NOT EXISTS (
           SELECT 1 FROM pg_trigger
            WHERE tgrelid='public.simulated_execution_fills_v1'::regclass
              AND tgname='forward_entry_fill_correlation_v1'
              AND tgenabled<>'D'
       ) THEN
        RAISE EXCEPTION
            'FORWARD_DECISION_REGISTRY_CONTINUITY_V1_POSTCONDITION_FAILED';
    END IF;
END;
$postconditions$;

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260805_forward_decision_registry_continuity_v1.sql',
    'e23234bdc477931499efd342d401a052367030f3e04888775692ab0df8b70cf1',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'FORWARD_DECISION_REGISTRY_CONTINUITY_V1',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    '3018ea949a5bb9bff605635ff2f9eefd84bdeb73',
    'FORWARD_DECISION_REGISTRY_CONTINUITY_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
     WHERE migration_id='20260805_forward_decision_registry_continuity_v1.sql'
);

COMMIT;

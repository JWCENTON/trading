-- WALTRADE SIMULATED ORDER NAMESPACE V1
-- Separates canonical administrative retirement orders from forward slots.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $dependencies$
DECLARE
    required_relation TEXT;
    conflict_checksum TEXT;
BEGIN
    FOREACH required_relation IN ARRAY ARRAY[
        'simulated_orders', 'positions', 'simulated_execution_fills_v1',
        'schema_migration_ledger_v1'
    ] LOOP
        IF to_regclass('public.' || required_relation) IS NULL THEN
            RAISE EXCEPTION
                'SIMULATED_ORDER_NAMESPACE_REQUIRED_RELATION_MISSING:%',
                required_relation;
        END IF;
    END LOOP;

    SELECT checksum_sha256 INTO conflict_checksum
    FROM public.schema_migration_ledger_v1
    WHERE migration_id='20260802_simulated_order_namespace_v1.sql'
      AND checksum_sha256 <>
          'c130ae635082d1c57b81c2cb6ca072436650a081b877584c31db0fb006da4746'
    ORDER BY applied_at DESC LIMIT 1;
    IF conflict_checksum IS NOT NULL THEN
        RAISE EXCEPTION
            'SIMULATED_ORDER_NAMESPACE_LEDGER_CHECKSUM_CONFLICT:%',
            conflict_checksum;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='positions'
          AND column_name='id' AND data_type IN ('integer','bigint')
          AND is_nullable='NO'
    ) THEN
        RAISE EXCEPTION 'SIMULATED_ORDER_NAMESPACE_POSITION_ID_INCOMPATIBLE';
    END IF;
END;
$dependencies$;

ALTER TABLE public.simulated_orders
    ADD COLUMN IF NOT EXISTS order_class TEXT NOT NULL DEFAULT 'FORWARD',
    ADD COLUMN IF NOT EXISTS position_id BIGINT NULL,
    ADD COLUMN IF NOT EXISTS environment TEXT NULL,
    ADD COLUMN IF NOT EXISTS deployment_id TEXT NULL;

DO $column_contract$
DECLARE
    issues TEXT;
BEGIN
    WITH expected(column_name,data_type,is_nullable,default_required) AS (
        VALUES
          ('order_class','text','NO',TRUE),
          ('position_id','bigint','YES',FALSE),
          ('environment','text','YES',FALSE),
          ('deployment_id','text','YES',FALSE)
    ), actual AS (
        SELECT column_name::TEXT,data_type::TEXT,is_nullable::TEXT,
               column_default::TEXT
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='simulated_orders'
          AND column_name IN (
            'order_class','position_id','environment','deployment_id'
          )
    ), compared AS (
        SELECT expected.column_name,
               CASE
                 WHEN actual.column_name IS NULL THEN 'missing'
                 WHEN actual.data_type<>expected.data_type THEN 'type'
                 WHEN actual.is_nullable<>expected.is_nullable THEN 'nullable'
                 WHEN expected.default_required AND
                      lower(COALESCE(actual.column_default,'')) NOT IN (
                        '''forward''::text','''forward'''
                      ) THEN 'default'
                 WHEN NOT expected.default_required AND
                      actual.column_default IS NOT NULL THEN 'default'
                 ELSE NULL
               END AS issue
        FROM expected LEFT JOIN actual USING(column_name)
    )
    SELECT string_agg(column_name || ':' || issue, ',' ORDER BY column_name)
    INTO issues FROM compared WHERE issue IS NOT NULL;
    IF issues IS NOT NULL THEN
        RAISE EXCEPTION
            'SIMULATED_ORDER_NAMESPACE_COLUMN_CONTRACT_MISMATCH:%',issues;
    END IF;
END;
$column_contract$;

DO $constraints$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.simulated_orders'::regclass
          AND conname='ck_sim_orders_order_class'
    ) THEN
        ALTER TABLE public.simulated_orders
            ADD CONSTRAINT ck_sim_orders_order_class CHECK (
                order_class IN ('FORWARD','LEGACY_ADMINISTRATIVE_CLOSE')
            ) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.simulated_orders'::regclass
          AND conname='ck_sim_orders_order_identity'
    ) THEN
        ALTER TABLE public.simulated_orders
            ADD CONSTRAINT ck_sim_orders_order_identity CHECK (
                (
                    order_class='FORWARD'
                    AND position_id IS NULL
                    AND environment IS NULL
                    AND deployment_id IS NULL
                ) OR (
                    order_class='LEGACY_ADMINISTRATIVE_CLOSE'
                    AND position_id IS NOT NULL
                    AND environment IS NOT NULL
                    AND btrim(environment)<>''
                    AND deployment_id IS NOT NULL
                    AND btrim(deployment_id)<>''
                    AND is_exit=TRUE
                    AND side='SELL'
                    AND reason='LEGACY_ADMINISTRATIVE_CLOSE'
                )
            ) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.simulated_orders'::regclass
          AND conname='fk_sim_orders_position'
    ) THEN
        ALTER TABLE public.simulated_orders
            ADD CONSTRAINT fk_sim_orders_position
            FOREIGN KEY(position_id) REFERENCES public.positions(id)
            ON DELETE RESTRICT NOT VALID;
    END IF;
END;
$constraints$;

DO $backfill_preconditions$
DECLARE
    invalid_order BIGINT;
BEGIN
    SELECT id INTO invalid_order
    FROM public.simulated_orders
    WHERE order_class NOT IN ('FORWARD','LEGACY_ADMINISTRATIVE_CLOSE')
       OR order_class IS NULL
    ORDER BY id LIMIT 1;
    IF invalid_order IS NOT NULL THEN
        RAISE EXCEPTION
            'SIMULATED_ORDER_NAMESPACE_UNKNOWN_ORDER_CLASS:%',invalid_order;
    END IF;

    SELECT id INTO invalid_order
    FROM public.simulated_orders
    WHERE reason='LEGACY_ADMINISTRATIVE_CLOSE'
      AND (NOT is_exit OR side<>'SELL')
    ORDER BY id LIMIT 1;
    IF invalid_order IS NOT NULL THEN
        RAISE EXCEPTION
            'SIMULATED_ORDER_NAMESPACE_ADMIN_SHAPE_INVALID:%',invalid_order;
    END IF;

    WITH evidence AS (
        SELECT orders.id,
               count(fills.id) AS fill_count,
               count(DISTINCT fills.position_id) AS position_count,
               count(DISTINCT fills.environment) AS environment_count,
               count(DISTINCT fills.deployment_id) AS deployment_count,
               count(positions.id) AS linked_position_count,
               bool_and(fills.order_purpose='EXIT') AS exit_purpose,
               bool_and(fills.side='SELL') AS sell_side,
               bool_and(
                   fills.environment IS NOT NULL
                   AND btrim(fills.environment)<>''
                   AND fills.deployment_id IS NOT NULL
                   AND btrim(fills.deployment_id)<>''
               ) AS complete_identity
        FROM public.simulated_orders orders
        LEFT JOIN public.simulated_execution_fills_v1 fills
          ON fills.simulated_order_id=orders.id
        LEFT JOIN public.positions positions ON positions.id=fills.position_id
        WHERE orders.reason='LEGACY_ADMINISTRATIVE_CLOSE'
          AND orders.is_exit AND orders.side='SELL'
        GROUP BY orders.id
    )
    SELECT id INTO invalid_order FROM evidence
    WHERE fill_count<>1 OR position_count<>1 OR environment_count<>1
       OR deployment_count<>1 OR linked_position_count<>1
       OR exit_purpose IS DISTINCT FROM TRUE
       OR sell_side IS DISTINCT FROM TRUE
       OR complete_identity IS DISTINCT FROM TRUE
    ORDER BY id LIMIT 1;
    IF invalid_order IS NOT NULL THEN
        RAISE EXCEPTION
            'SIMULATED_ORDER_NAMESPACE_ADMIN_BACKFILL_AMBIGUOUS:%',invalid_order;
    END IF;
END;
$backfill_preconditions$;

UPDATE public.simulated_orders orders
SET order_class='LEGACY_ADMINISTRATIVE_CLOSE',
    position_id=fills.position_id,
    environment=fills.environment,
    deployment_id=fills.deployment_id
FROM public.simulated_execution_fills_v1 fills
WHERE orders.reason='LEGACY_ADMINISTRATIVE_CLOSE'
  AND orders.is_exit AND orders.side='SELL'
  AND fills.simulated_order_id=orders.id
  AND (
    orders.order_class<>'LEGACY_ADMINISTRATIVE_CLOSE'
    OR orders.position_id IS DISTINCT FROM fills.position_id
    OR orders.environment IS DISTINCT FROM fills.environment
    OR orders.deployment_id IS DISTINCT FROM fills.deployment_id
  );

ALTER TABLE public.simulated_orders
    VALIDATE CONSTRAINT ck_sim_orders_order_class;
ALTER TABLE public.simulated_orders
    VALIDATE CONSTRAINT ck_sim_orders_order_identity;
ALTER TABLE public.simulated_orders
    VALIDATE CONSTRAINT fk_sim_orders_position;

DO $data_validation$
DECLARE
    conflict_identity TEXT;
BEGIN
    SELECT format('%s/%s/%s/%s',symbol,"interval",strategy,candle_open_time)
    INTO conflict_identity
    FROM public.simulated_orders
    WHERE order_class='FORWARD'
    GROUP BY symbol,"interval",strategy,candle_open_time
    HAVING count(*)>1
    ORDER BY 1 LIMIT 1;
    IF conflict_identity IS NOT NULL THEN
        RAISE EXCEPTION
            'SIMULATED_ORDER_NAMESPACE_FORWARD_DUPLICATE:%',conflict_identity;
    END IF;

    SELECT format('%s/%s/%s',environment,deployment_id,position_id)
    INTO conflict_identity
    FROM public.simulated_orders
    WHERE order_class='LEGACY_ADMINISTRATIVE_CLOSE'
    GROUP BY environment,deployment_id,position_id
    HAVING count(*)>1
    ORDER BY 1 LIMIT 1;
    IF conflict_identity IS NOT NULL THEN
        RAISE EXCEPTION
            'SIMULATED_ORDER_NAMESPACE_ADMIN_DUPLICATE:%',conflict_identity;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.simulated_orders orders
        JOIN public.simulated_execution_fills_v1 fills
          ON fills.simulated_order_id=orders.id
        WHERE orders.order_class='LEGACY_ADMINISTRATIVE_CLOSE'
          AND (
            orders.position_id IS DISTINCT FROM fills.position_id
            OR orders.environment IS DISTINCT FROM fills.environment
            OR orders.deployment_id IS DISTINCT FROM fills.deployment_id
            OR fills.order_purpose<>'EXIT' OR fills.side<>'SELL'
          )
    ) THEN
        RAISE EXCEPTION
            'SIMULATED_ORDER_NAMESPACE_ADMIN_LINKAGE_MISMATCH';
    END IF;
END;
$data_validation$;

ALTER TABLE public.simulated_orders
    DROP CONSTRAINT IF EXISTS sim_orders_uniq_candle_exit;
DROP INDEX IF EXISTS public.sim_orders_uniq_candle_exit;
DROP INDEX IF EXISTS public.ux_sim_orders_one_per_candle;
DROP INDEX IF EXISTS public.ux_sim_orders_one_per_candle_isexit;

CREATE UNIQUE INDEX IF NOT EXISTS ux_sim_orders_forward_one_per_candle
    ON public.simulated_orders(
        symbol,"interval",strategy,candle_open_time
    ) WHERE order_class='FORWARD';

CREATE UNIQUE INDEX IF NOT EXISTS ux_sim_orders_admin_position
    ON public.simulated_orders(
        environment,deployment_id,position_id
    ) WHERE order_class='LEGACY_ADMINISTRATIVE_CLOSE';

DO $final_contract$
DECLARE
    issues TEXT;
BEGIN
    WITH required(name,key_fragment,predicate_fragment) AS (
        VALUES
          (
            'ux_sim_orders_forward_one_per_candle',
            '(symbol,"interval",strategy,candle_open_time)',
            'order_class=''forward''::text'
          ),
          (
            'ux_sim_orders_admin_position',
            '(environment,deployment_id,position_id)',
            'order_class=''legacy_administrative_close''::text'
          )
    ), actual AS (
        SELECT relation.relname::TEXT AS name,
               regexp_replace(lower(pg_get_indexdef(index_row.indexrelid)),
                              '[[:space:]]+','','g') AS definition,
               regexp_replace(lower(pg_get_expr(
                    index_row.indpred,index_row.indrelid
               )),'[[:space:]()]','','g') AS predicate,
               index_row.indisunique,index_row.indisvalid,index_row.indisready
        FROM pg_index index_row
        JOIN pg_class relation ON relation.oid=index_row.indexrelid
        WHERE index_row.indrelid='public.simulated_orders'::regclass
    ), compared AS (
        SELECT required.name,
               CASE
                 WHEN actual.name IS NULL THEN 'missing'
                 WHEN NOT actual.indisunique THEN 'not_unique'
                 WHEN NOT actual.indisvalid OR NOT actual.indisready
                   THEN 'not_ready'
                 WHEN position(
                   regexp_replace(lower(required.key_fragment),'[[:space:]]+','','g')
                   IN actual.definition
                 )=0 THEN 'keys'
                 WHEN actual.predicate<>
                   regexp_replace(lower(required.predicate_fragment),
                                  '[[:space:]()]','','g') THEN 'predicate'
                 ELSE NULL
               END AS issue
        FROM required LEFT JOIN actual USING(name)
    )
    SELECT string_agg(name || ':' || issue,',' ORDER BY name)
    INTO issues FROM compared WHERE issue IS NOT NULL;
    IF issues IS NOT NULL THEN
        RAISE EXCEPTION
            'SIMULATED_ORDER_NAMESPACE_INDEX_CONTRACT_MISMATCH:%',issues;
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_class relation
        JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
        WHERE namespace.nspname='public'
          AND relation.relname IN (
            'sim_orders_uniq_candle_exit',
            'ux_sim_orders_one_per_candle',
            'ux_sim_orders_one_per_candle_isexit'
          )
    ) OR EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.simulated_orders'::regclass
          AND conname='sim_orders_uniq_candle_exit'
    ) THEN
        RAISE EXCEPTION
            'SIMULATED_ORDER_NAMESPACE_LEGACY_UNIQUENESS_REMAINS';
    END IF;
END;
$final_contract$;

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260802_simulated_order_namespace_v1.sql',
    'c130ae635082d1c57b81c2cb6ca072436650a081b877584c31db0fb006da4746',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'SIMULATED_ORDER_NAMESPACE_V1',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    'ee536658abc9c992a029eed7fe69a2b9cfd7c84e',
    'SIMULATED_ORDER_NAMESPACE_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id='20260802_simulated_order_namespace_v1.sql'
);

COMMIT;

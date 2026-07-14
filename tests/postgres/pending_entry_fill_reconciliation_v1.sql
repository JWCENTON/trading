\set ON_ERROR_STOP on

-- This script is destructive only to its own fixture rows, but it executes the
-- migration. Require BOTH a disposable database name and an operator-created
-- marker before the first migration is included.
DO $$
DECLARE
  disposable_marker text;
BEGIN
  IF current_database() !~* '_test$' THEN
    RAISE EXCEPTION
      'refusing pending-entry validation: database % does not end in _test',
      current_database();
  END IF;
  IF to_regclass('public.automation_kv') IS NULL THEN
    RAISE EXCEPTION
      'refusing pending-entry validation: automation_kv is missing';
  END IF;
  SELECT value INTO disposable_marker
  FROM public.automation_kv
  WHERE key = 'waltrade_disposable_test_db';
  IF lower(COALESCE(disposable_marker, '')) <> 'true' THEN
    RAISE EXCEPTION
      'refusing pending-entry validation: waltrade_disposable_test_db=true is required';
  END IF;
END $$;

-- First migration run.
\ir ../../db/migrations/20260714_pending_entry_fill_reconciliation_v1.sql

CREATE OR REPLACE FUNCTION pg_temp.assert_pending_entry_schema_contract()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  marker_value text;
  trigger_count integer;
  index_definition text;
BEGIN
  SELECT value INTO marker_value
  FROM public.automation_kv
  WHERE key = 'pending_entry_reconciliation_schema_version';
  IF marker_value IS DISTINCT FROM '1' THEN
    RAISE EXCEPTION
      'schema marker mismatch: expected 1, got %', marker_value;
  END IF;

  SELECT count(*) INTO trigger_count
  FROM pg_trigger t
  JOIN pg_class c ON c.oid = t.tgrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_proc p ON p.oid = t.tgfoid
  WHERE t.tgname = 'trg_mirror_live_orders'
    AND n.nspname = 'public'
    AND c.relname = 'strategy_events'
    AND p.proname = 'mirror_live_order_sent_to_binance_orders'
    AND NOT t.tgisinternal;
  IF trigger_count <> 1 THEN
    RAISE EXCEPTION 'expected exactly one correctly-linked mirror trigger, got %',
      trigger_count;
  END IF;

  IF to_regclass('public.v_pending_entry_fill_reconciliation_audit') IS NULL THEN
    RAISE EXCEPTION 'pending-entry audit view is missing';
  END IF;
  PERFORM audit_status
  FROM public.v_pending_entry_fill_reconciliation_audit
  LIMIT 0;

  FOREACH index_definition IN ARRAY ARRAY[
    'ux_binance_orders_source_symbol_order_id',
    'ux_binance_orders_legacy_null_source_symbol_order_id',
    'ix_binance_orders_pending_entry_reconcile',
    'ix_binance_order_fills_entry_reconcile_lookup',
    'ux_binance_order_fill_trade',
    'ux_positions_open'
  ] LOOP
    IF (SELECT count(*) FROM pg_indexes
        WHERE schemaname = 'public' AND indexname = index_definition) <> 1 THEN
      RAISE EXCEPTION 'expected exactly one index named %', index_definition;
    END IF;
  END LOOP;

  SELECT lower(indexdef) INTO index_definition
  FROM pg_indexes
  WHERE schemaname = 'public'
    AND indexname = 'ux_binance_orders_source_symbol_order_id';
  IF index_definition NOT LIKE '%unique index%'
     OR index_definition NOT LIKE '%(exchange_source, symbol, order_id)%' THEN
    RAISE EXCEPTION 'source-aware index definition mismatch: %', index_definition;
  END IF;

  SELECT lower(indexdef) INTO index_definition
  FROM pg_indexes
  WHERE schemaname = 'public'
    AND indexname = 'ux_binance_orders_legacy_null_source_symbol_order_id';
  IF index_definition NOT LIKE '%unique index%'
     OR index_definition NOT LIKE '%(symbol, order_id)%'
     OR index_definition NOT LIKE '%exchange_source is null%' THEN
    RAISE EXCEPTION 'legacy NULL-source index definition mismatch: %',
      index_definition;
  END IF;

  SELECT lower(indexdef) INTO index_definition
  FROM pg_indexes
  WHERE schemaname = 'public'
    AND indexname = 'ix_binance_orders_pending_entry_reconcile';
  IF index_definition NOT LIKE '%(reconciled_at, created_at, id)%'
     OR index_definition NOT LIKE '%order_purpose = ''entry''%'
     OR index_definition NOT LIKE '%order_id is not null%' THEN
    RAISE EXCEPTION 'pending lookup index definition mismatch: %',
      index_definition;
  END IF;
END;
$$;

-- Contract must hold after the first run.
SELECT pg_temp.assert_pending_entry_schema_contract();

-- Replay the exact migration and re-check marker, trigger cardinality, indexes
-- and view compilation.
\ir ../../db/migrations/20260714_pending_entry_fill_reconciliation_v1.sql
SELECT pg_temp.assert_pending_entry_schema_contract();

BEGIN;

-- Legacy NULL-source collision must fail, while the same symbol/order identity
-- may coexist for two named exchanges.
INSERT INTO public.binance_orders(
  symbol, side, order_type, order_id, strategy, "interval",
  order_purpose, order_accepted, exchange_source
) VALUES (
  'WTPG_LEGACY_USDC', 'BUY', 'MARKET', 'wtpg-legacy-collision',
  'RSI', '1m', 'ENTRY', true, NULL
);

DO $$
DECLARE
  collision_rejected boolean := false;
  violated_constraint text;
BEGIN
  BEGIN
    INSERT INTO public.binance_orders(
      symbol, side, order_type, order_id, strategy, "interval",
      order_purpose, order_accepted, exchange_source
    ) VALUES (
      'WTPG_LEGACY_USDC', 'BUY', 'MARKET', 'wtpg-legacy-collision',
      'RSI', '1m', 'ENTRY', true, NULL
    );
  EXCEPTION WHEN unique_violation THEN
    GET STACKED DIAGNOSTICS violated_constraint = CONSTRAINT_NAME;
    IF violated_constraint IS DISTINCT FROM
       'ux_binance_orders_legacy_null_source_symbol_order_id' THEN
      RAISE EXCEPTION 'unexpected legacy collision constraint: %',
        violated_constraint;
    END IF;
    collision_rejected := true;
  END;
  IF NOT collision_rejected THEN
    RAISE EXCEPTION 'legacy NULL-source duplicate was not rejected';
  END IF;
END $$;

INSERT INTO public.binance_orders(
  symbol, side, order_type, order_id, strategy, "interval",
  order_purpose, order_accepted, exchange_source
) VALUES
  ('WTPG_LEGACY_USDC','BUY','MARKET','wtpg-legacy-collision','RSI','1m','ENTRY',true,'okx'),
  ('WTPG_LEGACY_USDC','BUY','MARKET','wtpg-legacy-collision','RSI','1m','ENTRY',true,'binance');

DO $$
BEGIN
  IF (SELECT count(*) FROM public.binance_orders
      WHERE symbol = 'WTPG_LEGACY_USDC'
        AND order_id = 'wtpg-legacy-collision') <> 3 THEN
    RAISE EXCEPTION 'NULL/okx/binance source identities did not coexist as expected';
  END IF;
END $$;

-- Identical non-NULL canonical identity must fail on the source-aware index,
-- rather than on the legacy NULL-source guard or another fixture constraint.
DO $$
DECLARE
  collision_rejected boolean := false;
  violated_constraint text;
BEGIN
  BEGIN
    INSERT INTO public.binance_orders(
      symbol, side, order_type, order_id, strategy, "interval",
      order_purpose, order_accepted, exchange_source
    ) VALUES (
      'WTPG_LEGACY_USDC','BUY','MARKET','wtpg-legacy-collision',
      'RSI','1m','ENTRY',true,'okx'
    );
  EXCEPTION WHEN unique_violation THEN
    GET STACKED DIAGNOSTICS violated_constraint = CONSTRAINT_NAME;
    IF violated_constraint IS DISTINCT FROM
       'ux_binance_orders_source_symbol_order_id' THEN
      RAISE EXCEPTION 'unexpected source-aware collision constraint: %',
        violated_constraint;
    END IF;
    collision_rejected := true;
  END;
  IF NOT collision_rejected THEN
    RAISE EXCEPTION 'identical source-aware duplicate was not rejected';
  END IF;
END $$;

-- RSI ACK mirror: accepted identity, rejection, missing order ID, idempotent
-- replay and sparse metadata replay that must not erase better stored values.
INSERT INTO public.strategy_events(
  symbol, "interval", strategy, event_type, decision, reason, info
) VALUES (
  'WTPG_RSI_MIRROR_USDC', '1m', 'RSI', 'LIVE_ORDER_SENT', 'BUY',
  'ORDER_ACCEPTED_PENDING_FILL',
  '{"is_exit":false,"order_accepted":true,"requested_qty":"0.10","exchange_source":" OKX ","client_order_id":"wtpg-rsi-client","resp":{"orderId":"wtpg-rsi-accepted","clientOrderId":"wtpg-rsi-client","status":"NEW","side":"BUY","type":"LIMIT"}}'::jsonb
);

-- Exact replay and then sparse metadata replay.
INSERT INTO public.strategy_events(
  symbol, "interval", strategy, event_type, decision, reason, info
)
SELECT symbol, "interval", strategy, event_type, decision, reason, info
FROM public.strategy_events
WHERE symbol = 'WTPG_RSI_MIRROR_USDC'
ORDER BY created_at DESC
LIMIT 1;

INSERT INTO public.strategy_events(
  symbol, "interval", strategy, event_type, decision, reason, info
) VALUES (
  'WTPG_RSI_MIRROR_USDC', '1m', 'RSI', 'LIVE_ORDER_SENT', 'BUY',
  'ORDER_ACCEPTED_PENDING_FILL',
  '{"is_exit":false,"order_accepted":true,"exchange_source":"okx","resp":{"orderId":"wtpg-rsi-accepted"}}'::jsonb
);

INSERT INTO public.strategy_events(
  symbol, "interval", strategy, event_type, decision, reason, info
) VALUES
  ('WTPG_RSI_REJECT_USDC','1m','RSI','LIVE_ORDER_SENT','BUY','ORDER_REJECTED',
   '{"is_exit":false,"order_accepted":false,"exchange_source":"okx","resp":{"orderId":"wtpg-rsi-rejected","side":"BUY","type":"MARKET"}}'::jsonb),
  ('WTPG_RSI_NO_ID_USDC','1m','RSI','LIVE_ORDER_SENT','BUY','ORDER_REJECTED',
   '{"is_exit":false,"order_accepted":false,"exchange_source":"okx","resp":{"side":"BUY","type":"MARKET"}}'::jsonb);

DO $$
BEGIN
  IF (SELECT count(*) FROM public.binance_orders
      WHERE exchange_source='okx' AND symbol='WTPG_RSI_MIRROR_USDC'
        AND order_id='wtpg-rsi-accepted') <> 1 THEN
    RAISE EXCEPTION 'RSI mirror replay was not idempotent';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM public.binance_orders
    WHERE exchange_source='okx' AND symbol='WTPG_RSI_MIRROR_USDC'
      AND order_id='wtpg-rsi-accepted' AND strategy='RSI'
      AND "interval"='1m' AND side='BUY' AND order_type='LIMIT'
      AND order_purpose='ENTRY' AND order_accepted IS TRUE
      AND client_order_id='wtpg-rsi-client' AND requested_qty=0.10
  ) THEN
    RAISE EXCEPTION 'RSI mirror lost full accepted order identity or sparse replay erased metadata';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM public.binance_orders
    WHERE exchange_source='okx' AND symbol='WTPG_RSI_REJECT_USDC'
      AND order_id='wtpg-rsi-rejected' AND order_accepted IS FALSE
  ) THEN
    RAISE EXCEPTION 'RSI rejection was promoted to an accepted pending order';
  END IF;
  IF EXISTS (
    SELECT 1 FROM public.binance_orders
    WHERE symbol='WTPG_RSI_NO_ID_USDC'
  ) THEN
    RAISE EXCEPTION 'RSI event without order ID created an order row';
  END IF;
END $$;

-- A direct executor write followed by the RSI event must use the same
-- source-aware identity. Richer event metadata may fill NULLs, while conflicting
-- identity metadata must not overwrite the trusted direct row.
INSERT INTO public.binance_orders(
  symbol, side, order_type, client_order_id, order_id, status, raw,
  is_exit, strategy, "interval", order_purpose, requested_qty,
  order_accepted, exchange_source
) VALUES (
  'WTPG_DIRECT_TRIGGER_USDC','BUY','LIMIT',NULL,'wtpg-direct-trigger',
  'NEW','{}',false,'RSI','1m','ENTRY',NULL,true,'okx'
);

INSERT INTO public.strategy_events(
  symbol, "interval", strategy, event_type, decision, reason, info
) VALUES (
  'WTPG_DIRECT_TRIGGER_USDC','5m','RSI','LIVE_ORDER_SENT','SELL',
  'ORDER_ACCEPTED_PENDING_FILL',
  '{"is_exit":false,"order_accepted":true,"requested_qty":"0.20","exchange_source":" OKX ","client_order_id":"wtpg-direct-event-client","resp":{"orderId":"wtpg-direct-trigger","clientOrderId":"wtpg-direct-event-client","status":"NEW","side":"SELL","type":"MARKET"}}'::jsonb
), (
  'WTPG_DIRECT_TRIGGER_USDC','5m','RSI','LIVE_ORDER_SENT','SELL',
  'ORDER_ACCEPTED_PENDING_FILL',
  '{"is_exit":false,"order_accepted":true,"exchange_source":"okx","resp":{"orderId":"wtpg-direct-trigger"}}'::jsonb
);

DO $$
BEGIN
  IF (SELECT count(*) FROM public.binance_orders
      WHERE exchange_source='okx' AND symbol='WTPG_DIRECT_TRIGGER_USDC'
        AND order_id='wtpg-direct-trigger') <> 1 THEN
    RAISE EXCEPTION 'direct executor plus RSI trigger created a duplicate';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM public.binance_orders
    WHERE exchange_source='okx' AND symbol='WTPG_DIRECT_TRIGGER_USDC'
      AND order_id='wtpg-direct-trigger' AND strategy='RSI'
      AND "interval"='1m' AND side='BUY' AND order_type='LIMIT'
      AND order_purpose='ENTRY' AND order_accepted IS TRUE
      AND client_order_id='wtpg-direct-event-client' AND requested_qty=0.20
  ) THEN
    RAISE EXCEPTION 'direct/trigger upsert overwrote identity or failed additive metadata merge';
  END IF;
END $$;

-- Audit matrix fixtures. Each symbol is a separate canonical identity so the
-- partial OPEN-slot unique index cannot collapse cases.
CREATE TEMP TABLE expected_pending_entry_audit(
  exchange_source text NOT NULL,
  symbol text NOT NULL,
  order_id text NOT NULL,
  expected_status text NOT NULL,
  PRIMARY KEY(exchange_source, symbol, order_id)
) ON COMMIT DROP;

INSERT INTO expected_pending_entry_audit VALUES
  ('waltrade_test','WTPG_CANARY_USDC','wtpg-canary','CANARY_IGNORE'),
  ('waltrade_test','WTPG_EXIT_ACK_USDC','wtpg-exit-ack','EXIT_ACK_PENDING'),
  ('waltrade_test','WTPG_EXIT_FILL_USDC','wtpg-exit-fill','EXIT_FILL'),
  ('waltrade_test','WTPG_AMBIG_USDC','wtpg-ambiguous','AMBIGUOUS_ENTRY_FILL'),
  ('waltrade_test','WTPG_PENDING_USDC','wtpg-pending','PENDING_ENTRY_ACK'),
  ('waltrade_test','WTPG_LATE_USDC','wtpg-late','LATE_ENTRY_FILL_AFTER_POSITION_CLOSED'),
  ('waltrade_test','WTPG_ERROR_USDC','wtpg-error','ENTRY_FILL_RECONCILIATION_ERROR'),
  ('waltrade_test','WTPG_MISMATCH_USDC','wtpg-mismatch','OPEN_POSITION_ORDER_MISMATCH'),
  ('waltrade_test','WTPG_CREATED_USDC','wtpg-created','ENTRY_FILL_POSITION_CREATED'),
  ('waltrade_test','WTPG_UPDATED_USDC','wtpg-updated','ENTRY_FILL_POSITION_UPDATED'),
  ('waltrade_test','WTPG_ALREADY_USDC','wtpg-already','ENTRY_FILL_ALREADY_RECONCILED'),
  ('waltrade_test','WTPG_MATCHED_USDC','wtpg-matched','MATCHED_ENTRY_FILL'),
  ('waltrade_test','WTPG_ORPHAN_ORDER_USDC','wtpg-orphan-order','ORPHAN_ENTRY_FILL'),
  ('waltrade_test','WTPG_MANUAL_USDC','wtpg-manual','MANUAL_OR_EXTERNAL_FILL');

INSERT INTO public.binance_orders(
  created_at, symbol, side, order_type, client_order_id, order_id, status,
  raw, is_exit, strategy, "interval", order_purpose, requested_qty,
  order_accepted, exchange_source, reconciliation_status,
  last_reconciliation_action
) VALUES
  (now()-interval '14 min','WTPG_CANARY_USDC','BUY','MARKET','OKXCANARY-fixture','wtpg-canary','FILLED','{}',false,'RSI','1m','ENTRY',0.1,true,'waltrade_test',NULL,NULL),
  (now()-interval '13 min','WTPG_EXIT_ACK_USDC','SELL','MARKET','wtpg-exit-ack-ci','wtpg-exit-ack','NEW','{}',true,'RSI','1m','EXIT',0.1,true,'waltrade_test',NULL,NULL),
  (now()-interval '12 min','WTPG_EXIT_FILL_USDC','SELL','MARKET','wtpg-exit-fill-ci','wtpg-exit-fill','FILLED','{}',true,'RSI','1m','EXIT',0.1,true,'waltrade_test',NULL,NULL),
  (now()-interval '11 min','WTPG_AMBIG_USDC','BUY','MARKET','wtpg-ambig-ci','wtpg-ambiguous','FILLED','{}',false,'RSI','1m',NULL,0.1,true,'waltrade_test',NULL,NULL),
  (now()-interval '10 min','WTPG_PENDING_USDC','BUY','MARKET','wtpg-pending-ci','wtpg-pending','NEW','{}',false,'RSI','1m','ENTRY',0.1,true,'waltrade_test',NULL,NULL),
  (now()-interval '9 min','WTPG_LATE_USDC','BUY','MARKET','wtpg-late-ci','wtpg-late','FILLED','{}',false,'RSI','1m','ENTRY',0.1,true,'waltrade_test',NULL,'ENTRY_FILL_POSITION_UPDATED'),
  (now()-interval '8 min','WTPG_ERROR_USDC','BUY','MARKET','wtpg-error-ci','wtpg-error','FILLED','{}',false,'RSI','1m','ENTRY',0.1,true,'waltrade_test','ENTRY_FILL_RECONCILIATION_ERROR','ENTRY_FILL_RECONCILIATION_ERROR'),
  (now()-interval '7 min','WTPG_MISMATCH_USDC','BUY','MARKET','wtpg-mismatch-ci','wtpg-mismatch','FILLED','{}',false,'RSI','1m','ENTRY',0.1,true,'waltrade_test','OPEN_POSITION_ORDER_MISMATCH','OPEN_POSITION_ORDER_MISMATCH'),
  (now()-interval '6 min','WTPG_CREATED_USDC','BUY','MARKET','wtpg-created-ci','wtpg-created','FILLED','{}',false,'RSI','1m','ENTRY',0.1,true,'waltrade_test','ENTRY_FILL_POSITION_CREATED','ENTRY_FILL_POSITION_CREATED'),
  (now()-interval '5 min','WTPG_UPDATED_USDC','BUY','MARKET','wtpg-updated-ci','wtpg-updated','FILLED','{}',false,'RSI','1m','ENTRY',0.1,true,'waltrade_test','ENTRY_FILL_POSITION_UPDATED','ENTRY_FILL_POSITION_UPDATED'),
  (now()-interval '4 min','WTPG_ALREADY_USDC','BUY','MARKET','wtpg-already-ci','wtpg-already','FILLED','{}',false,'RSI','1m','ENTRY',0.1,true,'waltrade_test','ENTRY_FILL_ALREADY_RECONCILED','ENTRY_FILL_POSITION_CREATED'),
  (now()-interval '3 min','WTPG_MATCHED_USDC','BUY','MARKET','wtpg-matched-ci','wtpg-matched','FILLED','{}',false,'RSI','1m','ENTRY',0.1,true,'waltrade_test',NULL,NULL),
  (now()-interval '2 min','WTPG_ORPHAN_ORDER_USDC','BUY','MARKET','wtpg-orphan-ci','wtpg-orphan-order','FILLED','{}',false,'RSI','1m','ENTRY',0.1,true,'waltrade_test',NULL,NULL);

INSERT INTO public.binance_order_fills(
  source, trade_id, order_id, symbol, side, role, executed_qty, avg_price,
  quote_notional_usdc, commission_amount, commission_asset, commission_usdc,
  event_time, fill_idx, raw
) VALUES
  ('waltrade_test',910001,'wtpg-canary','WTPG_CANARY_USDC','BUY','TAKER',0.1,100,10,0.01,'USDC',0.01,now()-interval '14 min',0,'{"raw":{"clOrdId":"OKXCANARY-fixture"}}'),
  ('waltrade_test',910002,'wtpg-exit-fill','WTPG_EXIT_FILL_USDC','SELL','TAKER',0.1,100,10,0.01,'USDC',0.01,now()-interval '12 min',0,'{"raw":{"clOrdId":"wtpg-exit-fill-ci"}}'),
  ('waltrade_test',910003,'wtpg-ambiguous','WTPG_AMBIG_USDC','BUY','TAKER',0.1,100,10,0.01,'USDC',0.01,now()-interval '11 min',0,'{"raw":{"clOrdId":"wtpg-ambig-ci"}}'),
  ('waltrade_test',910004,'wtpg-late','WTPG_LATE_USDC','BUY','TAKER',0.1,100,10,0.01,'USDC',0.01,now()-interval '9 min',0,'{"raw":{"clOrdId":"wtpg-late-ci"}}'),
  ('waltrade_test',910005,'wtpg-error','WTPG_ERROR_USDC','BUY','TAKER',0.1,100,10,0.01,'USDC',0.01,now()-interval '8 min',0,'{"raw":{"clOrdId":"wtpg-error-ci"}}'),
  ('waltrade_test',910006,'wtpg-mismatch','WTPG_MISMATCH_USDC','BUY','TAKER',0.1,100,10,0.01,'USDC',0.01,now()-interval '7 min',0,'{"raw":{"clOrdId":"wtpg-mismatch-ci"}}'),
  ('waltrade_test',910007,'wtpg-created','WTPG_CREATED_USDC','BUY','TAKER',0.1,100,10,0.01,'USDC',0.01,now()-interval '6 min',0,'{"raw":{"clOrdId":"wtpg-created-ci"}}'),
  ('waltrade_test',910008,'wtpg-updated','WTPG_UPDATED_USDC','BUY','TAKER',0.1,100,10,0.01,'USDC',0.01,now()-interval '5 min',0,'{"raw":{"clOrdId":"wtpg-updated-ci"}}'),
  ('waltrade_test',910009,'wtpg-already','WTPG_ALREADY_USDC','BUY','TAKER',0.1,100,10,0.01,'USDC',0.01,now()-interval '4 min',0,'{"raw":{"clOrdId":"wtpg-already-ci"}}'),
  ('waltrade_test',910010,'wtpg-matched','WTPG_MATCHED_USDC','BUY','TAKER',0.1,100,10,0.01,'USDC',0.01,now()-interval '3 min',0,'{"raw":{"clOrdId":"wtpg-matched-ci"}}'),
  ('waltrade_test',910011,'wtpg-orphan-order','WTPG_ORPHAN_ORDER_USDC','BUY','TAKER',0.1,100,10,0.01,'USDC',0.01,now()-interval '2 min',0,'{"raw":{"clOrdId":"wtpg-orphan-ci"}}'),
  ('waltrade_test',910012,'wtpg-manual','WTPG_MANUAL_USDC','BUY','TAKER',0.1,100,10,0.01,'USDC',0.01,now()-interval '1 min',0,'{"raw":{}}');

INSERT INTO public.positions(
  symbol, strategy, "interval", status, side, qty, entry_price, entry_time,
  entry_order_id, entry_client_order_id, fees_usdc
) VALUES
  ('WTPG_LATE_USDC','RSI','1m','CLOSED','LONG',0.04,100,now()-interval '9 min','wtpg-late','wtpg-late-ci',0.004),
  ('WTPG_CREATED_USDC','RSI','1m','OPEN','LONG',0.1,100,now()-interval '6 min','wtpg-created','wtpg-created-ci',0.01),
  ('WTPG_UPDATED_USDC','RSI','1m','OPEN','LONG',0.1,100,now()-interval '5 min','wtpg-updated','wtpg-updated-ci',0.01),
  ('WTPG_ALREADY_USDC','RSI','1m','OPEN','LONG',0.1,100,now()-interval '4 min','wtpg-already','wtpg-already-ci',0.01),
  ('WTPG_MATCHED_USDC','RSI','1m','OPEN','LONG',0.1,100,now()-interval '3 min','wtpg-matched','wtpg-matched-ci',0.01);

UPDATE public.binance_orders bo
SET reconciled_position_id = p.id,
    reconciled_fill_count = 1,
    reconciled_executed_qty = 0.1,
    unreconciled_qty = 0,
    reconciled_at = now()
FROM public.positions p
WHERE bo.exchange_source='waltrade_test'
  AND bo.symbol=p.symbol
  AND bo.order_id=p.entry_order_id
  AND bo.symbol IN (
    'WTPG_CREATED_USDC','WTPG_UPDATED_USDC','WTPG_ALREADY_USDC'
  );

DO $$
DECLARE
  fixture record;
  actual_count integer;
  actual_status text;
BEGIN
  FOR fixture IN SELECT * FROM expected_pending_entry_audit ORDER BY symbol LOOP
    SELECT count(*), min(audit_status)
    INTO actual_count, actual_status
    FROM public.v_pending_entry_fill_reconciliation_audit
    WHERE exchange_source = fixture.exchange_source
      AND symbol = fixture.symbol
      AND order_id = fixture.order_id;
    IF actual_count <> 1 THEN
      RAISE EXCEPTION
        'audit identity %/%/% returned % rows, expected exactly one',
        fixture.exchange_source, fixture.symbol, fixture.order_id, actual_count;
    END IF;
    IF actual_status IS DISTINCT FROM fixture.expected_status THEN
      RAISE EXCEPTION
        'audit identity %/%/% expected %, got %',
        fixture.exchange_source, fixture.symbol, fixture.order_id,
        fixture.expected_status, actual_status;
    END IF;
  END LOOP;
END $$;

-- Current audit state and historical action are separate dimensions.
DO $$
DECLARE
  current_status text;
  historical_action text;
BEGIN
  SELECT audit_status,last_reconciliation_action
  INTO current_status,historical_action
  FROM public.v_pending_entry_fill_reconciliation_audit
  WHERE exchange_source='waltrade_test' AND symbol='WTPG_ALREADY_USDC'
    AND order_id='wtpg-already';
  IF current_status IS DISTINCT FROM 'ENTRY_FILL_ALREADY_RECONCILED'
     OR historical_action IS DISTINCT FROM 'ENTRY_FILL_POSITION_CREATED' THEN
    RAISE EXCEPTION 'reconciled current/action split mismatch: current=%, action=%',
      current_status,historical_action;
  END IF;

  SELECT audit_status,last_reconciliation_action
  INTO current_status,historical_action
  FROM public.v_pending_entry_fill_reconciliation_audit
  WHERE exchange_source='waltrade_test' AND symbol='WTPG_LATE_USDC'
    AND order_id='wtpg-late';
  IF current_status IS DISTINCT FROM 'LATE_ENTRY_FILL_AFTER_POSITION_CLOSED'
     OR historical_action IS DISTINCT FROM 'ENTRY_FILL_POSITION_UPDATED' THEN
    RAISE EXCEPTION 'late current/action split mismatch: current=%, action=%',
      current_status,historical_action;
  END IF;
END $$;

-- Explicit priority checks: the exact CLOSED match with excess aggregate must
-- remain LATE, and an EXIT ACK without a fill must not become EXIT_FILL.
DO $$
BEGIN
  IF (SELECT audit_status FROM public.v_pending_entry_fill_reconciliation_audit
      WHERE exchange_source='waltrade_test' AND symbol='WTPG_LATE_USDC'
        AND order_id='wtpg-late') <> 'LATE_ENTRY_FILL_AFTER_POSITION_CLOSED' THEN
    RAISE EXCEPTION 'late CLOSED fill was hidden by a general match';
  END IF;
  IF (SELECT audit_status FROM public.v_pending_entry_fill_reconciliation_audit
      WHERE exchange_source='waltrade_test' AND symbol='WTPG_EXIT_ACK_USDC'
        AND order_id='wtpg-exit-ack') <> 'EXIT_ACK_PENDING' THEN
    RAISE EXCEPTION 'EXIT ACK without fill was classified as a fill';
  END IF;
END $$;

-- CLOSED history must not block a new OPEN cycle. Only the second OPEN row for
-- the same slot must fail, and it must fail on the runtime partial index.
INSERT INTO public.positions(
  symbol,strategy,"interval",status,side,qty,entry_price
) VALUES
  ('WTPG_OPEN_GUARD_USDC','RSI','1m','CLOSED','LONG',0.1,100),
  ('WTPG_OPEN_GUARD_USDC','RSI','1m','OPEN','LONG',0.1,101);

DO $$
DECLARE
  collision_rejected boolean := false;
  violated_constraint text;
BEGIN
  BEGIN
    INSERT INTO public.positions(
      symbol, strategy, "interval", status, side, qty, entry_price
    ) VALUES ('WTPG_OPEN_GUARD_USDC','RSI','1m','OPEN','LONG',0.01,102);
  EXCEPTION WHEN unique_violation THEN
    GET STACKED DIAGNOSTICS violated_constraint = CONSTRAINT_NAME;
    IF violated_constraint IS DISTINCT FROM 'ux_positions_open' THEN
      RAISE EXCEPTION 'unexpected OPEN-slot collision constraint: %',
        violated_constraint;
    END IF;
    collision_rejected := true;
  END;
  IF NOT collision_rejected THEN
    RAISE EXCEPTION 'partial OPEN-slot unique index did not reject duplicate';
  END IF;
END $$;

-- The exact production candidate query is a Python constant. Its EXPLAIN gate
-- therefore lives in test_pending_entry_fill_reconciliation_pg.py, which imports
-- that constant directly, recursively inspects FORMAT JSON and requires exactly
-- ix_binance_orders_pending_entry_reconcile. Keeping a second SQL copy here
-- would allow the rollout gate to drift from production.

-- Fixture data, temporary expectations and ANALYZE statistics are isolated.
-- The two committed migration runs intentionally remain in this disposable DB.
ROLLBACK;

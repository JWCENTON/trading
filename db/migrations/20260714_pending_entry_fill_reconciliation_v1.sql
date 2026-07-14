BEGIN;

ALTER TABLE public.binance_orders
  ADD COLUMN IF NOT EXISTS strategy text,
  ADD COLUMN IF NOT EXISTS "interval" text,
  ADD COLUMN IF NOT EXISTS order_purpose text,
  ADD COLUMN IF NOT EXISTS requested_qty numeric,
  ADD COLUMN IF NOT EXISTS order_accepted boolean,
  ADD COLUMN IF NOT EXISTS exchange_source text,
  ADD COLUMN IF NOT EXISTS reconciliation_status text,
  ADD COLUMN IF NOT EXISTS reconciled_position_id bigint,
  ADD COLUMN IF NOT EXISTS reconciled_at timestamptz,
  ADD COLUMN IF NOT EXISTS reconciled_fill_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reconciled_executed_qty numeric NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS unreconciled_qty numeric NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reconciliation_error text,
  ADD COLUMN IF NOT EXISTS last_reconciliation_action text;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ck_binance_orders_order_purpose'
      AND conrelid = 'public.binance_orders'::regclass
  ) THEN
    ALTER TABLE public.binance_orders
      ADD CONSTRAINT ck_binance_orders_order_purpose
      CHECK (order_purpose IS NULL OR order_purpose IN ('ENTRY', 'EXIT'))
      NOT VALID;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.binance_orders
    WHERE exchange_source IS NOT NULL AND order_id IS NOT NULL
    GROUP BY exchange_source, symbol, order_id
    HAVING count(*) > 1
  ) THEN
    RAISE EXCEPTION
      'source-aware binance_orders duplicates require manual pre-audit';
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_binance_orders_source_symbol_order_id
  ON public.binance_orders (exchange_source, symbol, order_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_binance_orders_legacy_null_source_symbol_order_id
  ON public.binance_orders (symbol, order_id)
  WHERE exchange_source IS NULL;

ALTER TABLE public.binance_orders
  DROP CONSTRAINT IF EXISTS ux_binance_orders_symbol_order_id;

CREATE INDEX IF NOT EXISTS ix_binance_orders_pending_entry_reconcile
  ON public.binance_orders (reconciled_at, created_at, id)
  WHERE order_purpose = 'ENTRY' AND order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_binance_orders_reconciled_position
  ON public.binance_orders (reconciled_position_id)
  WHERE reconciled_position_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_binance_order_fills_entry_reconcile_lookup
  ON public.binance_order_fills (source, symbol, order_id);

CREATE OR REPLACE FUNCTION public.mirror_live_order_sent_to_binance_orders()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_pos_id bigint;
  v_coid text;
  v_order_id text;
  v_is_exit boolean;
  v_source text;
BEGIN
  IF NEW.event_type <> 'LIVE_ORDER_SENT' THEN
    RETURN NEW;
  END IF;

  v_order_id := NEW.info->'resp'->>'orderId';
  IF v_order_id IS NULL THEN
    RETURN NEW;
  END IF;

  v_coid := COALESCE(
    NULLIF(NEW.info->>'client_order_id', ''),
    NULLIF(NEW.info->'resp'->>'clientOrderId', '')
  );
  v_is_exit := COALESCE((NEW.info->>'is_exit')::boolean, false);
  v_source := lower(NULLIF(btrim(NEW.info->>'exchange_source'), ''));

  v_pos_id := NULL;
  IF v_coid IS NOT NULL AND v_coid ~ '-P[0-9]+-' THEN
    v_pos_id := (regexp_match(v_coid, '-P([0-9]+)-'))[1]::bigint;
  END IF;

  IF v_source IS NULL THEN
    INSERT INTO public.binance_orders (
      created_at, symbol, side, order_type, client_order_id, order_id,
      status, raw, position_id, is_exit, attempt, strategy, "interval",
      order_purpose, requested_qty, order_accepted, exchange_source
    )
    VALUES (
      NEW.created_at, NEW.symbol,
      COALESCE(NULLIF(NEW.info->'resp'->>'side', ''), NEW.decision),
      COALESCE(NULLIF(NEW.info->'resp'->>'type', ''), 'MARKET'),
      v_coid, v_order_id,
      NULLIF(COALESCE(NEW.info->>'status', NEW.info->'resp'->>'status'), ''),
      NEW.info->'resp', v_pos_id, v_is_exit, NULL, NEW.strategy,
      NEW."interval", CASE WHEN v_is_exit THEN 'EXIT' ELSE 'ENTRY' END,
      NULLIF(NEW.info->>'requested_qty', '')::numeric,
      COALESCE(NULLIF(NEW.info->>'order_accepted', '')::boolean, true),
      NULL
    )
    ON CONFLICT DO NOTHING;
    RETURN NEW;
  END IF;

  INSERT INTO public.binance_orders (
    created_at, symbol, side, order_type, client_order_id, order_id,
    status, raw, position_id, is_exit, attempt, strategy, "interval",
    order_purpose, requested_qty, order_accepted, exchange_source
  )
  VALUES (
    NEW.created_at,
    NEW.symbol,
    COALESCE(NULLIF(NEW.info->'resp'->>'side', ''), NEW.decision),
    COALESCE(NULLIF(NEW.info->'resp'->>'type', ''), 'MARKET'),
    v_coid,
    v_order_id,
    NULLIF(COALESCE(NEW.info->>'status', NEW.info->'resp'->>'status'), ''),
    NEW.info->'resp',
    v_pos_id,
    v_is_exit,
    NULL,
    NEW.strategy,
    NEW."interval",
    CASE WHEN v_is_exit THEN 'EXIT' ELSE 'ENTRY' END,
    NULLIF(NEW.info->>'requested_qty', '')::numeric,
    COALESCE(NULLIF(NEW.info->>'order_accepted', '')::boolean, true),
    v_source
  )
  ON CONFLICT (exchange_source, symbol, order_id) DO UPDATE
  SET
    client_order_id = COALESCE(binance_orders.client_order_id, EXCLUDED.client_order_id),
    strategy = COALESCE(binance_orders.strategy, EXCLUDED.strategy),
    "interval" = COALESCE(binance_orders."interval", EXCLUDED."interval"),
    order_purpose = COALESCE(binance_orders.order_purpose, EXCLUDED.order_purpose),
    requested_qty = COALESCE(binance_orders.requested_qty, EXCLUDED.requested_qty),
    order_accepted = COALESCE(binance_orders.order_accepted, EXCLUDED.order_accepted),
    exchange_source = COALESCE(binance_orders.exchange_source, EXCLUDED.exchange_source),
    is_exit = COALESCE(binance_orders.is_exit, EXCLUDED.is_exit)
  WHERE binance_orders.exchange_source IS NULL
     OR binance_orders.exchange_source = EXCLUDED.exchange_source;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_mirror_live_orders ON public.strategy_events;
CREATE TRIGGER trg_mirror_live_orders
AFTER INSERT ON public.strategy_events
FOR EACH ROW
EXECUTE FUNCTION public.mirror_live_order_sent_to_binance_orders();

CREATE OR REPLACE VIEW public.v_pending_entry_fill_reconciliation_audit AS
WITH fill_totals AS (
  SELECT
    f.source,
    f.symbol,
    f.order_id,
    count(*) AS fill_count,
    sum(f.executed_qty) AS executed_qty,
    CASE WHEN sum(f.executed_qty) > 0
      THEN sum(f.executed_qty * f.avg_price) / sum(f.executed_qty)
      ELSE NULL
    END AS weighted_avg_price,
    sum(f.commission_usdc) AS fees_usdc,
    min(f.event_time) AS first_fill_time,
    max(f.event_time) AS last_fill_time,
    count(DISTINCT f.side) AS fill_side_count,
    min(f.side) AS fill_side,
    max(NULLIF(f.raw->'raw'->>'clOrdId', '')) AS fill_client_order_id,
    bool_or(
      COALESCE(NULLIF(f.raw->'raw'->>'clOrdId', ''), '') ILIKE 'OKXCANARY%'
    ) AS is_canary
  FROM public.binance_order_fills f
  WHERE f.executed_qty > 0
  GROUP BY f.source, f.symbol, f.order_id
), order_audit AS (
  SELECT
    bo.id AS order_row_id,
    bo.exchange_source,
    bo.symbol,
    bo.strategy,
    bo."interval",
    bo.side,
    bo.order_id,
    bo.client_order_id,
    bo.order_purpose,
    bo.requested_qty,
    bo.status AS order_status,
    bo.reconciliation_status,
    bo.reconciliation_error,
    bo.last_reconciliation_action,
    bo.reconciled_position_id,
    bo.reconciled_at,
    CASE
      WHEN ft.executed_qty IS NOT NULL
        THEN GREATEST(
          ft.executed_qty - COALESCE(bo.reconciled_executed_qty, 0),
          0
        )
      ELSE bo.unreconciled_qty
    END AS unreconciled_qty,
    ft.fill_count,
    ft.executed_qty,
    ft.weighted_avg_price,
    ft.fees_usdc,
    ft.first_fill_time,
    ft.last_fill_time,
    COALESCE(rp.status, ep.status) AS position_status,
    CASE
      WHEN COALESCE(ft.is_canary, false) THEN 'CANARY_IGNORE'
      WHEN bo.order_purpose = 'EXIT' AND ft.order_id IS NULL THEN 'EXIT_ACK_PENDING'
      WHEN bo.order_purpose = 'EXIT' THEN 'EXIT_FILL'
      WHEN bo.order_purpose IS DISTINCT FROM 'ENTRY'
        OR bo.strategy IS NULL OR bo."interval" IS NULL
        THEN 'AMBIGUOUS_ENTRY_FILL'
      WHEN bo.order_accepted IS DISTINCT FROM true
        THEN 'AMBIGUOUS_ENTRY_FILL'
      WHEN ft.order_id IS NULL THEN 'PENDING_ENTRY_ACK'
      WHEN ft.fill_side_count <> 1 OR ft.fill_side IS DISTINCT FROM bo.side
        THEN 'AMBIGUOUS_ENTRY_FILL'
      WHEN COALESCE(ep.match_count, 0) > 1 THEN 'AMBIGUOUS_ENTRY_FILL'
      WHEN bo.reconciliation_status IN (
        'LATE_ENTRY_FILL_AFTER_POSITION_CLOSED',
        'ENTRY_FILL_RECONCILIATION_ERROR',
        'OPEN_POSITION_ORDER_MISMATCH',
        'AMBIGUOUS_ENTRY_FILL'
      ) THEN bo.reconciliation_status
      WHEN COALESCE(rp.status, ep.status) = 'CLOSED'
       AND ft.executed_qty > COALESCE(rp.qty, ep.qty)
        THEN 'LATE_ENTRY_FILL_AFTER_POSITION_CLOSED'
      WHEN COALESCE(rp.status, ep.status) = 'CLOSED'
       AND ft.executed_qty = COALESCE(rp.qty, ep.qty)
        THEN 'ENTRY_FILL_ALREADY_RECONCILED'
      WHEN bo.reconciliation_status IN (
        'ENTRY_FILL_POSITION_CREATED', 'ENTRY_FILL_POSITION_UPDATED',
        'ENTRY_FILL_ALREADY_RECONCILED'
      ) THEN bo.reconciliation_status
      WHEN rp.id IS NOT NULL OR ep.id IS NOT NULL THEN 'MATCHED_ENTRY_FILL'
      ELSE 'ORPHAN_ENTRY_FILL'
    END AS audit_status
  FROM public.binance_orders bo
  LEFT JOIN fill_totals ft
    ON ft.source = bo.exchange_source
   AND ft.symbol = bo.symbol AND ft.order_id = bo.order_id
  LEFT JOIN public.positions rp
    ON rp.id = bo.reconciled_position_id
  LEFT JOIN LATERAL (
    SELECT p.id, p.status, p.qty, count(*) OVER () AS match_count
    FROM public.positions p
    WHERE p.symbol = bo.symbol
      AND p.strategy = bo.strategy
      AND p."interval" = bo."interval"
      AND (
        p.entry_order_id = bo.order_id
        OR (
          bo.client_order_id IS NOT NULL
          AND p.entry_client_order_id = bo.client_order_id
        )
      )
    ORDER BY p.id
    LIMIT 1
  ) ep ON true
  WHERE bo.order_accepted IS TRUE OR ft.order_id IS NOT NULL
), orphan_fills AS (
  SELECT
    NULL::bigint AS order_row_id,
    ft.source AS exchange_source,
    ft.symbol,
    NULL::text AS strategy,
    NULL::text AS "interval",
    ft.fill_side AS side,
    ft.order_id,
    ft.fill_client_order_id AS client_order_id,
    NULL::text AS order_purpose,
    NULL::numeric AS requested_qty,
    NULL::text AS order_status,
    NULL::text AS reconciliation_status,
    NULL::text AS reconciliation_error,
    NULL::text AS last_reconciliation_action,
    NULL::bigint AS reconciled_position_id,
    NULL::timestamptz AS reconciled_at,
    ft.executed_qty AS unreconciled_qty,
    ft.fill_count,
    ft.executed_qty,
    ft.weighted_avg_price,
    ft.fees_usdc,
    ft.first_fill_time,
    ft.last_fill_time,
    NULL::text AS position_status,
    CASE
      WHEN ft.is_canary THEN 'CANARY_IGNORE'
      WHEN COALESCE(ft.fill_client_order_id, '') = '' THEN 'MANUAL_OR_EXTERNAL_FILL'
      ELSE 'ORPHAN_ENTRY_FILL'
    END AS audit_status
  FROM fill_totals ft
  WHERE NOT EXISTS (
    SELECT 1 FROM public.binance_orders bo
    WHERE bo.exchange_source = ft.source
      AND bo.symbol = ft.symbol AND bo.order_id = ft.order_id
  )
)
SELECT * FROM order_audit
UNION ALL
SELECT * FROM orphan_fills;

INSERT INTO public.automation_kv(key, value, updated_at)
VALUES
  ('pending_entry_reconciliation_schema_version', '1', now()),
  ('pending_entry_reconciliation_enabled', '1', now()),
  ('pending_entry_reconciliation_interval_seconds', '30', now()),
  ('pending_entry_reconciliation_last_run', '1970-01-01T00:00:00+00:00', now()),
  ('pending_entry_reconciliation_last_status', 'NEVER_RUN', now()),
  ('pending_entry_reconciliation_last_stats', '{}', now()),
  ('pending_entry_reconciliation_last_error', '', now())
ON CONFLICT (key) DO NOTHING;

COMMIT;

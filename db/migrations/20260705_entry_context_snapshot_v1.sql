BEGIN;

CREATE TABLE IF NOT EXISTS entry_context_snapshot_v1 (
  position_id BIGINT PRIMARY KEY,
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  strategy TEXT NOT NULL,
  entry_time TIMESTAMPTZ,
  exit_time TIMESTAMPTZ,

  entry_trace_event_id BIGINT,
  entry_trace_created_at TIMESTAMPTZ,
  entry_trace_snapshot JSONB,

  has_entry_trace BOOLEAN NOT NULL DEFAULT false,
  has_orc_context BOOLEAN NOT NULL DEFAULT false,
  has_mme_context BOOLEAN NOT NULL DEFAULT false,

  orc_context_snapshot JSONB,
  mme_context_snapshot JSONB,

  snapshot_status TEXT NOT NULL,
  missing_context TEXT[] NOT NULL DEFAULT '{}',

  source_note TEXT NOT NULL DEFAULT 'shadow-only entry context snapshot; no trading impact',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_entry_context_snapshot_v1_slot
ON entry_context_snapshot_v1(symbol, interval, strategy);

CREATE INDEX IF NOT EXISTS idx_entry_context_snapshot_v1_entry_time
ON entry_context_snapshot_v1(entry_time DESC);

CREATE OR REPLACE FUNCTION refresh_entry_context_snapshot_v1(
  p_since TIMESTAMPTZ DEFAULT now() - interval '24 hours'
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_count INTEGER := 0;
BEGIN
  INSERT INTO entry_context_snapshot_v1 (
    position_id,
    symbol,
    interval,
    strategy,
    entry_time,
    exit_time,

    entry_trace_event_id,
    entry_trace_created_at,
    entry_trace_snapshot,

    has_entry_trace,
    has_orc_context,
    has_mme_context,

    orc_context_snapshot,
    mme_context_snapshot,

    snapshot_status,
    missing_context,
    updated_at
  )
  SELECT
    p.id,
    p.symbol,
    p."interval",
    p.strategy,
    p.entry_time,
    p.exit_time,

    e.id,
    e.created_at,
    to_jsonb(e),

    e.id IS NOT NULL,
    o.symbol IS NOT NULL,
    m.symbol IS NOT NULL,

    to_jsonb(o),
    to_jsonb(m),

    CASE
      WHEN e.id IS NOT NULL AND o.symbol IS NOT NULL AND m.symbol IS NOT NULL
        THEN 'READY_CONTEXT_SNAPSHOT'
      ELSE 'PARTIAL_CONTEXT_SNAPSHOT'
    END,

    ARRAY_REMOVE(ARRAY[
      CASE WHEN e.id IS NULL THEN 'ENTRY_TRACE' END,
      CASE WHEN o.symbol IS NULL THEN 'ORC' END,
      CASE WHEN m.symbol IS NULL THEN 'MME' END
    ], NULL),

    now()
  FROM positions p

  LEFT JOIN LATERAL (
    SELECT e.*
    FROM entry_trace_events e
    WHERE e.symbol = p.symbol
      AND e."interval" = p."interval"
      AND e.strategy = p.strategy
      AND e.created_at BETWEEN p.entry_time - interval '30 minutes'
                           AND p.entry_time + interval '30 minutes'
    ORDER BY abs(EXTRACT(EPOCH FROM (e.created_at - p.entry_time)))
    LIMIT 1
  ) e ON true

  LEFT JOIN LATERAL (
    SELECT o.*
    FROM v_orc_candidate_context_v1 o
    WHERE o.symbol = p.symbol
      AND o."interval" = p."interval"
      AND o.strategy = p.strategy
    LIMIT 1
  ) o ON true

  LEFT JOIN LATERAL (
    SELECT m.*
    FROM v_market_memory_orc_context_v17 m
    WHERE m.symbol = p.symbol
      AND m."interval" = p."interval"
    LIMIT 1
  ) m ON true

  WHERE p.entry_time >= p_since
    AND p.entry_time IS NOT NULL

  ON CONFLICT (position_id) DO UPDATE SET
    entry_time = EXCLUDED.entry_time,
    exit_time = EXCLUDED.exit_time,
    entry_trace_event_id = EXCLUDED.entry_trace_event_id,
    entry_trace_created_at = EXCLUDED.entry_trace_created_at,
    entry_trace_snapshot = EXCLUDED.entry_trace_snapshot,
    has_entry_trace = EXCLUDED.has_entry_trace,
    has_orc_context = EXCLUDED.has_orc_context,
    has_mme_context = EXCLUDED.has_mme_context,
    orc_context_snapshot = EXCLUDED.orc_context_snapshot,
    mme_context_snapshot = EXCLUDED.mme_context_snapshot,
    snapshot_status = EXCLUDED.snapshot_status,
    missing_context = EXCLUDED.missing_context,
    updated_at = now();

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;

CREATE OR REPLACE VIEW v_entry_context_snapshot_audit_v1 AS
SELECT
  position_id,
  symbol,
  interval,
  strategy,
  entry_time,
  exit_time,
  has_entry_trace,
  has_orc_context,
  has_mme_context,
  snapshot_status,
  missing_context,
  updated_at
FROM entry_context_snapshot_v1;

COMMIT;

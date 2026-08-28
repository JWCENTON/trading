\set ON_ERROR_STOP on

-- This migration intentionally runs outside an explicit transaction because
-- the active PAPER tables are continuously written.  CONCURRENTLY preserves
-- forward observation availability while the shared lookup indexes are built.
CREATE INDEX CONCURRENTLY IF NOT EXISTS
    ix_entry_opportunity_evidence_decision_captured_v1
    ON public.entry_opportunity_evidence_v1(
        decision_key,
        captured_at DESC
    );

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    ix_entry_trace_events_opportunity_projection_v1
    ON public.entry_trace_events(
        symbol,
        interval,
        strategy,
        candle_open_time,
        created_at DESC,
        id DESC
    );

-- FinalDecision Producer Audit Ledger V1 canonical audit queries.
-- Replace both literals once, freeze them, and reuse the same half-open window.
\set audit_start_utc '''2026-01-01T00:00:00Z'''
\set audit_end_utc   '''2026-01-02T00:00:00Z'''

-- Lifecycle equation summary.
WITH windowed AS (
  SELECT *
  FROM final_decision_producer_audit_v1
  WHERE finalized_at >= :audit_start_utc::timestamptz
    AND finalized_at <  :audit_end_utc::timestamptz
),
counts AS (
  SELECT
    count(*) FILTER (WHERE event_type='FINALIZED') AS finalized,
    count(*) FILTER (WHERE event_type='PRODUCER_ATTEMPTED') AS attempted,
    count(*) FILTER (WHERE event_type='SKIPPED_DISABLED') AS skipped_disabled,
    count(*) FILTER (WHERE event_type='SKIPPED_KILL_SWITCH') AS skipped_kill_switch,
    count(*) FILTER (WHERE event_type='ACCEPTED') AS accepted,
    count(*) FILTER (WHERE event_type='IDEMPOTENT_EXISTING') AS idempotent_existing,
    count(*) FILTER (WHERE event_type='IDEMPOTENCY_CONFLICT') AS conflicts,
    count(*) FILTER (WHERE event_type='VALIDATION_REJECTED') AS validation_rejected,
    count(*) FILTER (WHERE event_type='SERIALIZATION_FAILED') AS serialization_failed,
    count(*) FILTER (WHERE event_type='OUTBOX_WRITE_FAILED') AS outbox_write_failed
  FROM windowed
)
SELECT *,
  finalized-skipped_disabled-skipped_kill_switch-attempted
    AS unclassified_finalized,
  attempted-accepted-idempotent_existing-conflicts-validation_rejected
    -serialization_failed-outbox_write_failed
    AS in_flight_attempts
FROM counts;

-- Details for every finalized decision missing a legal branch, and every
-- producer attempt missing a terminal outcome.
WITH windowed AS (
  SELECT *
  FROM final_decision_producer_audit_v1
  WHERE finalized_at >= :audit_start_utc::timestamptz
    AND finalized_at <  :audit_end_utc::timestamptz
),
finalized AS (
  SELECT * FROM windowed WHERE event_type='FINALIZED'
),
branches AS (
  SELECT finalized_event_id,
         count(*) FILTER (WHERE event_type IN (
           'SKIPPED_DISABLED','SKIPPED_KILL_SWITCH','PRODUCER_ATTEMPTED'
         )) AS branch_count
  FROM windowed
  GROUP BY finalized_event_id
),
attempts AS (
  SELECT * FROM windowed WHERE event_type='PRODUCER_ATTEMPTED'
),
terminals AS (
  SELECT attempt_id, count(*) AS terminal_count
  FROM windowed
  WHERE event_type IN (
    'ACCEPTED','IDEMPOTENT_EXISTING','IDEMPOTENCY_CONFLICT',
    'VALIDATION_REJECTED','SERIALIZATION_FAILED','OUTBOX_WRITE_FAILED'
  )
  GROUP BY attempt_id
)
SELECT
  f.deployment_id,f.decision_key,f.finalized_event_id,NULL::uuid AS attempt_id,
  f.source_service,f.strategy,f.symbol,f.interval,
  f.original_decision_type,f.decision_kind,f.finalized_at,
  'FINALIZED_WITHOUT_EXACTLY_ONE_BRANCH' AS gap
FROM finalized f
LEFT JOIN branches b USING(finalized_event_id)
WHERE coalesce(b.branch_count,0) <> 1
UNION ALL
SELECT
  a.deployment_id,a.decision_key,a.finalized_event_id,a.attempt_id,
  a.source_service,a.strategy,a.symbol,a.interval,
  a.original_decision_type,a.decision_kind,a.finalized_at,
  'ATTEMPT_WITHOUT_EXACTLY_ONE_TERMINAL' AS gap
FROM attempts a
LEFT JOIN terminals t USING(attempt_id)
WHERE coalesce(t.terminal_count,0) <> 1
ORDER BY finalized_at DESC
LIMIT 100;

-- Canonical dimensional breakdown. Add/remove grouping sets only after the
-- denominator definition is frozen; outbox rows are never the denominator.
WITH finalized AS (
  SELECT *
  FROM final_decision_producer_audit_v1
  WHERE event_type='FINALIZED'
    AND finalized_at >= :audit_start_utc::timestamptz
    AND finalized_at <  :audit_end_utc::timestamptz
),
outcomes AS (
  SELECT finalized_event_id,event_type
  FROM final_decision_producer_audit_v1
  WHERE event_type <> 'FINALIZED'
    AND finalized_at >= :audit_start_utc::timestamptz
    AND finalized_at <  :audit_end_utc::timestamptz
)
SELECT
  f.deployment_id,
  f.environment,
  f.source_service,
  f.strategy,
  f.symbol,
  f.interval,
  f.original_decision_type,
  f.decision_kind,
  date_trunc('hour',f.finalized_at) AS hour_utc,
  count(DISTINCT f.finalized_event_id) AS finalized,
  count(*) FILTER (WHERE o.event_type='PRODUCER_ATTEMPTED') AS attempted,
  count(*) FILTER (WHERE o.event_type='ACCEPTED') AS accepted,
  count(*) FILTER (WHERE o.event_type='IDEMPOTENT_EXISTING') AS idempotent_existing,
  count(*) FILTER (WHERE o.event_type IN (
    'SKIPPED_DISABLED','SKIPPED_KILL_SWITCH'
  )) AS legal_skips,
  count(*) FILTER (WHERE o.event_type IN (
    'IDEMPOTENCY_CONFLICT','VALIDATION_REJECTED',
    'SERIALIZATION_FAILED','OUTBOX_WRITE_FAILED'
  )) AS failures
FROM finalized f
LEFT JOIN outcomes o USING(finalized_event_id)
GROUP BY
  f.deployment_id,f.environment,f.source_service,f.strategy,f.symbol,f.interval,
  f.original_decision_type,f.decision_kind,date_trunc('hour',f.finalized_at)
ORDER BY hour_utc,deployment_id,source_service,strategy,symbol,interval;

-- ACCEPTED/IDEMPOTENT_EXISTING/CONFLICT linkage must resolve to a real outbox
-- row. Expected count: zero.
SELECT count(*) AS terminal_outbox_linkage_gaps
FROM final_decision_producer_audit_v1 a
LEFT JOIN causal_decision_observation_outbox_v1 o
  ON o.event_id=a.outbox_event_id
 AND o.deployment_id=a.deployment_id
 AND o.decision_key=a.decision_key
WHERE a.event_type IN (
    'ACCEPTED','IDEMPOTENT_EXISTING','IDEMPOTENCY_CONFLICT'
  )
  AND a.finalized_at >= :audit_start_utc::timestamptz
  AND a.finalized_at <  :audit_end_utc::timestamptz
  AND o.event_id IS NULL;

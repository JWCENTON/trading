#!/usr/bin/env bash
set -euo pipefail

KEEP_DAYS="${KEEP_DAYS:-14}"
BATCH_SIZE="${BATCH_SIZE:-200000}"
MAX_BATCHES="${MAX_BATCHES:-50}"

echo "[db-retention] keep_days=$KEEP_DAYS batch_size=$BATCH_SIZE max_batches=$MAX_BATCHES"

run_sql() {
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 "$@"
}

for i in $(seq 1 "$MAX_BATCHES"); do
  echo "[db-retention] batch=$i"

  run_sql -c "
WITH d AS (
  SELECT ctid FROM orchestrator.risk_metrics
  WHERE created_at < now() - interval '${KEEP_DAYS} days'
  LIMIT ${BATCH_SIZE}
)
DELETE FROM orchestrator.risk_metrics WHERE ctid IN (SELECT ctid FROM d);

WITH d AS (
  SELECT ctid FROM orchestrator.decision_log
  WHERE created_at < now() - interval '${KEEP_DAYS} days'
  LIMIT ${BATCH_SIZE}
)
DELETE FROM orchestrator.decision_log WHERE ctid IN (SELECT ctid FROM d);

WITH d AS (
  SELECT ctid FROM public.strategy_events
  WHERE created_at < now() - interval '${KEEP_DAYS} days'
  LIMIT ${BATCH_SIZE}
)
DELETE FROM public.strategy_events WHERE ctid IN (SELECT ctid FROM d);

WITH d AS (
  SELECT ctid FROM public.orchestrator_decisions
  WHERE created_at < now() - interval '${KEEP_DAYS} days'
  LIMIT ${BATCH_SIZE}
)
DELETE FROM public.orchestrator_decisions WHERE ctid IN (SELECT ctid FROM d);
"

  remaining="$(run_sql -At -c "
SELECT
  (SELECT count(*) FROM orchestrator.risk_metrics WHERE created_at < now() - interval '${KEEP_DAYS} days')
+ (SELECT count(*) FROM orchestrator.decision_log WHERE created_at < now() - interval '${KEEP_DAYS} days')
+ (SELECT count(*) FROM public.strategy_events WHERE created_at < now() - interval '${KEEP_DAYS} days')
+ (SELECT count(*) FROM public.orchestrator_decisions WHERE created_at < now() - interval '${KEEP_DAYS} days');
")"

  echo "[db-retention] remaining=$remaining"

  if [ "$remaining" = "0" ]; then
    break
  fi
done

echo "[db-retention] analyze heavy tables"
run_sql -c "
ANALYZE orchestrator.risk_metrics;
ANALYZE orchestrator.decision_log;
ANALYZE public.strategy_events;
ANALYZE public.orchestrator_decisions;
"

echo "[db-retention] done"

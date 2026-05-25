#!/usr/bin/env bash
set -euo pipefail

KEEP_DAYS="${KEEP_DAYS:-14}"
BATCH_SIZE="${BATCH_SIZE:-200000}"
MAX_BATCHES="${MAX_BATCHES:-50}"
API_KEY_VALIDATION_KEEP_DAYS="${API_KEY_VALIDATION_KEEP_DAYS:-180}"
API_KEY_VALIDATION_KEEP_LATEST="${API_KEY_VALIDATION_KEEP_LATEST:-1000}"

echo "[db-retention] keep_days=$KEEP_DAYS batch_size=$BATCH_SIZE max_batches=$MAX_BATCHES api_key_validation_keep_days=$API_KEY_VALIDATION_KEEP_DAYS api_key_validation_keep_latest=$API_KEY_VALIDATION_KEEP_LATEST"

run_sql() {
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 "$@"
}

API_KEY_VALIDATION_TABLE_EXISTS="$(run_sql -At -c "SELECT CASE WHEN to_regclass('public.api_key_validation_events') IS NULL THEN '0' ELSE '1' END;")"
echo "[db-retention] api_key_validation_events_exists=$API_KEY_VALIDATION_TABLE_EXISTS"

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

  if [ "$API_KEY_VALIDATION_TABLE_EXISTS" = "1" ]; then
    run_sql -c "
WITH d AS (
  SELECT ctid FROM public.api_key_validation_events
  WHERE created_at < now() - interval '${API_KEY_VALIDATION_KEEP_DAYS} days'
  LIMIT ${BATCH_SIZE}
)
DELETE FROM public.api_key_validation_events WHERE ctid IN (SELECT ctid FROM d);

WITH ranked AS (
  SELECT ctid, row_number() OVER (ORDER BY created_at DESC, id DESC) AS rn
  FROM public.api_key_validation_events
), d AS (
  SELECT ctid FROM ranked
  WHERE rn > ${API_KEY_VALIDATION_KEEP_LATEST}
  LIMIT ${BATCH_SIZE}
)
DELETE FROM public.api_key_validation_events WHERE ctid IN (SELECT ctid FROM d);
"
  fi

  remaining="$(run_sql -At -c "
SELECT
  (SELECT count(*) FROM orchestrator.risk_metrics WHERE created_at < now() - interval '${KEEP_DAYS} days')
+ (SELECT count(*) FROM orchestrator.decision_log WHERE created_at < now() - interval '${KEEP_DAYS} days')
+ (SELECT count(*) FROM public.strategy_events WHERE created_at < now() - interval '${KEEP_DAYS} days')
+ (SELECT count(*) FROM public.orchestrator_decisions WHERE created_at < now() - interval '${KEEP_DAYS} days');
")"

  if [ "$API_KEY_VALIDATION_TABLE_EXISTS" = "1" ]; then
    api_key_validation_remaining="$(run_sql -At -c "
SELECT
  (SELECT count(*) FROM public.api_key_validation_events WHERE created_at < now() - interval '${API_KEY_VALIDATION_KEEP_DAYS} days')
+ (SELECT GREATEST(count(*) - ${API_KEY_VALIDATION_KEEP_LATEST}, 0) FROM public.api_key_validation_events);
")"
    remaining="$((remaining + api_key_validation_remaining))"
  fi

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

if [ "$API_KEY_VALIDATION_TABLE_EXISTS" = "1" ]; then
  run_sql -c "ANALYZE public.api_key_validation_events;"
fi

echo "[db-retention] done"

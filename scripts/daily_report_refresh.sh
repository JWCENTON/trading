#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== $(date -Is) refresh_daily_bot_report: PAPER =="
docker compose -p trading-paper --env-file .env.paper -f docker-compose.yaml -f docker-compose.paper.override.yaml \
  exec -T db psql -U botuser -d trading_paper -v ON_ERROR_STOP=1 <<'SQL'
DELETE FROM daily_bot_report WHERE day = CURRENT_DATE;
SELECT COUNT(*) AS bots FROM bot_control;
SELECT COUNT(*) AS samples_10m FROM bot_heartbeat_samples WHERE seen_at >= now() - interval '10 minutes';
SELECT refresh_daily_bot_report(CURRENT_DATE);
SELECT COUNT(*) AS rows_today FROM daily_bot_report WHERE day = CURRENT_DATE;
SQL

echo "== $(date -Is) refresh_daily_bot_report: LIVE =="
docker compose -p trading-live --env-file .env.live -f docker-compose.yaml -f docker-compose.live.override.yaml \
  exec -T db psql -U botuser -d trading_live -v ON_ERROR_STOP=1 <<'SQL'
DELETE FROM daily_bot_report WHERE day = CURRENT_DATE;
SELECT COUNT(*) AS bots FROM bot_control;
SELECT COUNT(*) AS samples_10m FROM bot_heartbeat_samples WHERE seen_at >= now() - interval '10 minutes';
SELECT refresh_daily_bot_report(CURRENT_DATE);
SELECT COUNT(*) AS rows_today FROM daily_bot_report WHERE day = CURRENT_DATE;
SQL

echo "OK"

#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== $(date -Is) ORCH_TICK LIVE =="

docker compose -p trading-live --env-file .env.live -f docker-compose.yaml -f docker-compose.live.override.yaml \
  exec -T db psql -U botuser -d trading_live -v ON_ERROR_STOP=1 < scripts/orchestrator_live_tick.sql

echo "OK"

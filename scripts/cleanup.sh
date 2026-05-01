#!/bin/bash
set -e

cd /opt/trading-bot

# LIVE
docker compose -p trading-live \
  --env-file .env.live \
  -f docker-compose.yaml \
  -f docker-compose.live.override.yaml \
  exec -T db psql -U botuser -d trading_live < scripts/cleanup.sql

# PAPER
docker compose -p trading-paper \
  --env-file .env.paper \
  -f docker-compose.yaml \
  -f docker-compose.paper.override.yaml \
  --profile legacy-paper-ui \
  exec -T db psql -U botuser -d trading_paper < scripts/cleanup.sql

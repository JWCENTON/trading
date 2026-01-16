#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

LOCKFILE="/tmp/run_daily_report.lock"
exec 9>"$LOCKFILE"
flock -n 9 || exit 0

run_psql() {
  local project="$1" envfile="$2" override="$3" dbname="$4" sqlfile="$5"
  cat "$sqlfile" | docker compose -p "$project" --env-file "$envfile" \
    -f docker-compose.yaml -f "$override" exec -T db psql -U botuser -d "$dbname"
}

# LIVE
run_psql "trading-live"  ".env.live"  "docker-compose.live.override.yaml"  "trading_live"  "scripts/010_daily_report_upsert.sql"
run_psql "trading-live"  ".env.live"  "docker-compose.live.override.yaml"  "trading_live"  "scripts/011_daily_report_retention.sql"

# PAPER
run_psql "trading-paper" ".env.paper" "docker-compose.paper.override.yaml" "trading_paper" "scripts/010_daily_report_upsert.sql"
run_psql "trading-paper" ".env.paper" "docker-compose.paper.override.yaml" "trading_paper" "scripts/011_daily_report_retention.sql"

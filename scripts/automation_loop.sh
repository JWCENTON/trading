#!/usr/bin/env bash
set -euo pipefail

# Interval in seconds (default 3600 = 1h)
INTERVAL_S="${INTERVAL_S:-3600}"

# Which automations run
RUN_REGIME_GATE="${RUN_REGIME_GATE:-1}"
RUN_DAILY_REPORT="${RUN_DAILY_REPORT:-1}"

while true; do
  echo "== $(date -u '+%F %T') UTC :: automation tick =="

  if [[ "$RUN_REGIME_GATE" == "1" ]]; then
    /app/scripts/run_regime_gate_live_only.sh || true
  fi

  if [[ "$RUN_DAILY_REPORT" == "1" ]]; then
    # This script should be safe idempotent; it is, by ON CONFLICT + retention
    /app/scripts/run_daily_report_live_only.sh || true
  fi

  echo "== sleeping ${INTERVAL_S}s =="
  sleep "$INTERVAL_S"
done

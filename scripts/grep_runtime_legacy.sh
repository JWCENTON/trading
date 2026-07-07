#!/usr/bin/env bash
set -euo pipefail

grep -RIn \
  "policy_version=orc_v1\|ORC_V7_READY\|BINANCE_MYTRADES\|binance_mytrades\|binance_order_fills\|binance_orders\|TIME_EXIT_ENABLED\|MAX_POSITION_MINUTES\|TIME_EXIT" \
  --exclude-dir=.git \
  --exclude-dir=__pycache__ \
  --exclude-dir=.venv \
  --exclude-dir=_old \
  --exclude-dir=db_live_data \
  --exclude-dir=db_data \
  --exclude-dir=backups \
  --exclude='*.log' \
  --exclude='*backup*.sql' \
  --exclude='*full_backup*.sql' \
  --exclude='*schema_*.sql' \
  --exclude='deep_trading_audit_*.txt' \
  --exclude='exit_trace_incremental_audit_*.txt' \
  --exclude='logs.txt' \
  .

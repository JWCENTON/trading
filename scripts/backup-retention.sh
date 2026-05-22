#!/usr/bin/env bash
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-backups/drill}"
KEEP_DAYS="${KEEP_DAYS:-7}"
KEEP_MIN_PER_PREFIX="${KEEP_MIN_PER_PREFIX:-2}"

mkdir -p "$BACKUP_DIR"

echo "[backup-retention] dir=$BACKUP_DIR keep_days=$KEEP_DAYS keep_min_per_prefix=$KEEP_MIN_PER_PREFIX"

for prefix in live paper; do
  count="$(find "$BACKUP_DIR" -maxdepth 1 -type f -name "${prefix}-*.dump" | wc -l)"
  echo "[backup-retention] prefix=$prefix count=$count"

  if [ "$count" -gt "$KEEP_MIN_PER_PREFIX" ]; then
    find "$BACKUP_DIR" -maxdepth 1 -type f -name "${prefix}-*.dump" -mtime +"$KEEP_DAYS" -print -delete
  fi
done

find "$BACKUP_DIR" -maxdepth 1 -type f -name "*.tmp" -print -delete

echo "[backup-retention] done"

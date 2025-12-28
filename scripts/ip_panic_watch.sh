#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

ENV_FILE="$PROJECT_DIR/.env.live"
STATE_FILE="$PROJECT_DIR/.last_public_ip"
LOG_FILE="$PROJECT_DIR/watchdog_ip_panic.log"
LOCK_FILE="$PROJECT_DIR/.ip_panic_watch.lock"

get_public_ip() {
  curl -fsS --max-time 5 https://api.ipify.org
}

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

log() {
  echo "$(timestamp) $*" | tee -a "$LOG_FILE" >/dev/null
}

# ---- LOCK (anti-overlap) ----
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  # jeśli poprzednie uruchomienie nadal działa, wychodzimy cicho
  exit 0
fi

if [[ ! -f "$ENV_FILE" ]]; then
  log "ERROR: missing $ENV_FILE"
  exit 1
fi

NEW_IP="$(get_public_ip || true)"
if [[ -z "${NEW_IP}" ]]; then
  log "WARN: could not fetch public IP"
  exit 0
fi

OLD_IP=""
if [[ -f "$STATE_FILE" ]]; then
  OLD_IP="$(cat "$STATE_FILE" 2>/dev/null || true)"
fi

if [[ "$NEW_IP" == "$OLD_IP" ]]; then
  exit 0
fi

log "PUBLIC_IP_CHANGED old='${OLD_IP}' new='${NEW_IP}'"
echo "$NEW_IP" > "$STATE_FILE"

# ustaw PANIC_DISABLE_TRADING=1 (idempotent)
if grep -qE '^PANIC_DISABLE_TRADING=' "$ENV_FILE"; then
  sed -i 's/^PANIC_DISABLE_TRADING=.*/PANIC_DISABLE_TRADING=1/' "$ENV_FILE"
else
  echo "PANIC_DISABLE_TRADING=1" >> "$ENV_FILE"
fi

log "SET PANIC_DISABLE_TRADING=1 in ${ENV_FILE}"

# restart tylko bota (możesz rozszerzyć na inne live serwisy)
docker compose -f "$PROJECT_DIR/docker-compose.live.yaml" up -d --force-recreate --no-deps bot-btc-live

log "RECREATED service bot-btc-live (LIVE panic engaged)"
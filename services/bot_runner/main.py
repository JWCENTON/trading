#!/usr/bin/env python3
import os
import time
import signal
import logging
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] bot-runner: %(message)s"
)
logger = logging.getLogger(__name__)

POLL_SECONDS = int(os.getenv("BOT_RUNNER_POLL_SECONDS", "5"))
GRACE_SECONDS = int(os.getenv("BOT_RUNNER_GRACE_SECONDS", "20"))
KILL_SECONDS = int(os.getenv("BOT_RUNNER_KILL_SECONDS", "5"))
RESTART_BACKOFF_SECONDS = int(os.getenv("BOT_RUNNER_RESTART_BACKOFF_SECONDS", "15"))

TRADING_MODE = os.getenv("TRADING_MODE", "").strip()
if TRADING_MODE not in ("LIVE", "PAPER"):
    raise RuntimeError("TRADING_MODE must be LIVE or PAPER (set in env_file)")

# Mapowanie strategii -> komenda uruchomieniowa
# Dostosuj ścieżki do swoich entrypointów.
STRATEGY_CMD = {
  "RSI": ["python", "-u", "/app/bot/main.py"],
  "BBRANGE": ["python", "-u", "/app/bot_bbrange/main.py"],  
  "TREND": ["python", "-u", "/app/bot_trend/main.py"], 
  "SUPER_TREND": ["python", "-u", "/app/bot_supertrend/main.py"],
}

@dataclass(frozen=True)
class BotKey:
    symbol: str
    interval: str
    strategy: str

@dataclass
class BotProc:
    key: BotKey
    popen: subprocess.Popen
    started_at: float
    last_exit_at: Optional[float] = None
    last_exit_code: Optional[int] = None


_shutdown = False

def _handle_sigterm(signum, frame):
    global _shutdown
    _shutdown = True
    logger.warning("Received signal %s, shutting down...", signum)

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


def db_connect():
    host = os.getenv("DB_HOST", "db")
    port = int(os.getenv("DB_PORT", "5432"))
    user = os.getenv("DB_USER") or os.getenv("POSTGRES_USER") or "botuser"

    # najważniejsze: obsłuż realne nazwy z Twojego systemu
    password = (
        os.getenv("DB_PASS")
        or os.getenv("DB_PASSWORD")
        or os.getenv("POSTGRES_PASSWORD")
        or ""
    )

    name = os.getenv("DB_NAME") or os.getenv("POSTGRES_DB") or ""
    if not name:
        raise RuntimeError("DB_NAME/POSTGRES_DB is required")
    if password == "":
        raise RuntimeError("DB_PASS/DB_PASSWORD/POSTGRES_PASSWORD is missing")

    return psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=name
    )


def fetch_desired_configs(conn) -> Dict[BotKey, dict]:
    """
    bot_control schema (yours):
      symbol, strategy, interval, enabled, live_orders_enabled,
      regime_enabled, regime_mode, mode, reason, updated_at
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
              symbol,
              interval,
              strategy,
              enabled,
              live_orders_enabled,
              regime_enabled,
              regime_mode
            FROM bot_control
            """
        )
        rows = cur.fetchall()

    desired = {}
    for r in rows:
        key = BotKey(
            symbol=r["symbol"],
            interval=r["interval"],
            strategy=r["strategy"],
        )
        desired[key] = r
    return desired


def build_env(row: dict) -> dict:
    env = dict(os.environ)

    env["SYMBOL"] = row["symbol"]
    env["INTERVAL"] = row["interval"]

    # zgodnie z Twoimi botami:
    env["STRATEGY_NAME"] = row["strategy"]

    # TRADING_MODE jest z env kontenera (LIVE/PAPER), nie z DB:
    env["TRADING_MODE"] = TRADING_MODE

    env["LIVE_ORDERS_ENABLED"] = "1" if row.get("live_orders_enabled") else "0"
    env["REGIME_ENABLED"] = "1" if row.get("regime_enabled") else "0"
    env["REGIME_MODE"] = (row.get("regime_mode") or "DRY_RUN")

    return env


def start_bot(row: dict) -> subprocess.Popen:
    strategy = row["strategy"]
    cmd = STRATEGY_CMD.get(strategy)
    if not cmd:
        raise RuntimeError(f"Unknown strategy={strategy}. Add to STRATEGY_CMD.")

    env = build_env(row)

    logger.info(
        "START %s %s %s (TRADING_MODE=%s LIVE_ORDERS=%s REGIME_ENABLED=%s REGIME_MODE=%s)",
        row["strategy"], row["symbol"], row["interval"],
        TRADING_MODE,
        "1" if row.get("live_orders_enabled") else "0",
        "1" if row.get("regime_enabled") else "0",
        row.get("regime_mode")
    )

    # stdout/stderr dziedziczone -> widoczne w docker logs bot-runner
    return subprocess.Popen(cmd, env=env)


def stop_bot(proc: BotProc):
    if proc.popen.poll() is not None:
        return

    logger.info("STOP  %s %s %s (pid=%s)",
                proc.key.strategy, proc.key.symbol, proc.key.interval, proc.popen.pid)

    try:
        proc.popen.terminate()
    except Exception:
        pass

    t0 = time.time()
    while time.time() - t0 < GRACE_SECONDS:
        if proc.popen.poll() is not None:
            return
        time.sleep(0.25)

    logger.warning("KILL  %s %s %s (pid=%s)",
                   proc.key.strategy, proc.key.symbol, proc.key.interval, proc.popen.pid)

    try:
        proc.popen.kill()
    except Exception:
        pass

    t1 = time.time()
    while time.time() - t1 < KILL_SECONDS:
        if proc.popen.poll() is not None:
            return
        time.sleep(0.25)


def main():
    logger.info("bot-runner starting (TRADING_MODE=%s POLL=%ss)", TRADING_MODE, POLL_SECONDS)

    conn = db_connect()
    conn.autocommit = True

    running: Dict[BotKey, BotProc] = {}
    last_restart_attempt: Dict[BotKey, float] = {}

    try:
        while not _shutdown:
            desired = fetch_desired_configs(conn)

            # 1) Zatrzymaj te, które nie powinny działać
            for key, proc in list(running.items()):
                row = desired.get(key)
                if not row or not row.get("enabled", False):
                    stop_bot(proc)
                    running.pop(key, None)

            # 2) Sprawdź crashe i restartuj (z backoff)
            for key, proc in list(running.items()):
                rc = proc.popen.poll()
                if rc is None:
                    continue
                now = time.time()
                proc.last_exit_at = now
                proc.last_exit_code = rc
                running.pop(key, None)

                logger.error(
                    "EXIT  %s %s %s (rc=%s)",
                    key.strategy, key.symbol, key.interval, rc
                )

                last = last_restart_attempt.get(key, 0.0)
                if now - last < RESTART_BACKOFF_SECONDS:
                    continue
                last_restart_attempt[key] = now

                row = desired.get(key)
                if row and row.get("enabled", False):
                    try:
                        p = start_bot(row)
                        running[key] = BotProc(key=key, popen=p, started_at=time.time())
                    except Exception as e:
                        logger.exception("Failed to restart %s: %s", key, e)

            # 3) Startuj brakujące
            for key, row in desired.items():
                if not row.get("enabled", False):
                    continue
                if key in running:
                    continue

                now = time.time()
                last = last_restart_attempt.get(key, 0.0)
                if now - last < RESTART_BACKOFF_SECONDS:
                    continue
                last_restart_attempt[key] = now

                try:
                    p = start_bot(row)
                    running[key] = BotProc(key=key, popen=p, started_at=time.time())
                except Exception as e:
                    logger.exception("Failed to start %s: %s", key, e)

            time.sleep(POLL_SECONDS)

    finally:
        logger.warning("bot-runner stopping - terminating all bots (%d)", len(running))
        for proc in list(running.values()):
            stop_bot(proc)
        try:
            conn.close()
        except Exception:
            pass
        logger.info("bot-runner stopped")


if __name__ == "__main__":
    main()
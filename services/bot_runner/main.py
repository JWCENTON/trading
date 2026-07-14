#!/usr/bin/env python3
import os
import time
import signal
import logging
import subprocess
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from common.worker_heartbeat import record_worker_heartbeat
from common.schema_readiness import validate_strategy_runtime_schema
from common.runtime import normalize_trading_mode

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
STARTUP_STAGGER_SECONDS = float(
    os.getenv("BOT_RUNNER_STARTUP_STAGGER_SECONDS", "1.5")
)
if not math.isfinite(STARTUP_STAGGER_SECONDS) or STARTUP_STAGGER_SECONDS < 0:
    raise RuntimeError("BOT_RUNNER_STARTUP_STAGGER_SECONDS must be finite and >= 0")

TRADING_MODE = normalize_trading_mode(os.getenv("TRADING_MODE"))

# Mapowanie strategii -> komenda uruchomieniowa
# Dostosuj ścieżki do swoich entrypointów.
STRATEGY_CMD = {
  "RSI": ["python", "-u", "/app/bot/main.py"],
  "BBRANGE": ["python", "-u", "/app/bot_bbrange/main.py"],  
  "TREND": ["python", "-u", "/app/bot_trend/main.py"], 
  "SUPERTREND": ["python", "-u", "/app/bot_supertrend/main.py"],
}
STRATEGY_ORDER = {name: index for index, name in enumerate(STRATEGY_CMD)}

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
            ORDER BY
              CASE strategy
                WHEN 'RSI' THEN 0
                WHEN 'BBRANGE' THEN 1
                WHEN 'TREND' THEN 2
                WHEN 'SUPERTREND' THEN 3
                ELSE 99
              END,
              symbol,
              interval
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


def worker_sort_key(key: BotKey) -> tuple[int, str, str, str]:
    """Stable order that separates strategies using the same market slot."""
    strategy = key.strategy.replace("_", "").upper()
    return (
        STRATEGY_ORDER.get(strategy, len(STRATEGY_ORDER)),
        key.symbol,
        key.interval,
        strategy,
    )


def ordered_start_candidates(
    desired: Dict[BotKey, dict],
    running: Dict[BotKey, BotProc],
) -> list[tuple[BotKey, dict]]:
    candidates = [
        (key, row)
        for key, row in desired.items()
        if row.get("enabled", False) and key not in running
    ]
    return sorted(candidates, key=lambda item: worker_sort_key(item[0]))


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
    strategy = row["strategy"].replace("_","").upper()

    cmd = STRATEGY_CMD.get(strategy)
    if not cmd:
        raise RuntimeError(
            f"Unknown strategy={strategy}). Add to STRATEGY_CMD."
        )

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


def interruptible_wait(
    seconds: float,
    shutdown_requested=lambda: _shutdown,
    *,
    monotonic=time.monotonic,
    sleep=time.sleep,
) -> bool:
    """Wait up to seconds, returning False promptly when shutdown is requested."""
    deadline = monotonic() + max(0.0, float(seconds))
    while not shutdown_requested():
        remaining = deadline - monotonic()
        if remaining <= 0:
            return True
        sleep(min(0.1, remaining))
    return False


def start_worker_batch(
    candidates: list[tuple[BotKey, dict]],
    running: Dict[BotKey, BotProc],
    last_restart_attempt: Dict[BotKey, float],
    *,
    stagger_seconds: float = STARTUP_STAGGER_SECONDS,
    start_fn=start_bot,
    now_fn=time.time,
    wait_fn=interruptible_wait,
    shutdown_requested=lambda: _shutdown,
) -> int:
    """Start an ordered batch with waits only between consecutive attempts."""
    total = len(candidates)
    if total == 0:
        return 0

    logger.info(
        "BOT_RUNNER_STARTUP_BATCH size=%d stagger_seconds=%.3f",
        total,
        stagger_seconds,
    )
    started = 0
    for index, (key, row) in enumerate(candidates, start=1):
        if shutdown_requested():
            break
        if index > 1 and not wait_fn(stagger_seconds, shutdown_requested):
            break
        if shutdown_requested():
            break

        attempted_at = now_fn()
        last_restart_attempt[key] = attempted_at
        logger.info(
            "BOT_RUNNER_START worker=%d/%d strategy=%s symbol=%s interval=%s spawn_timestamp=%.6f",
            index,
            total,
            key.strategy,
            key.symbol,
            key.interval,
            attempted_at,
        )
        try:
            popen = start_fn(row)
            running[key] = BotProc(key=key, popen=popen, started_at=now_fn())
            started += 1
        except Exception as exc:
            logger.exception("Failed to start %s: %s", key, exc)

    logger.info(
        "BOT_RUNNER_STARTUP_BATCH_DONE requested=%d started=%d",
        total,
        started,
    )
    return started


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

    try:
        readiness = validate_strategy_runtime_schema(
            conn,
            trading_mode=TRADING_MODE,
        )
        logger.info(
            "strategy runtime schema readiness: status=%s environment=%s "
            "pending_entry_reconciliation_applicable=%s",
            readiness.status,
            readiness.environment,
            readiness.pending_entry_reconciliation_applicable,
        )
    except Exception as exc:
        logger.exception("strategy runtime schema readiness failed")
        record_worker_heartbeat(
            "bot-runner",
            status="error",
            error=exc,
            meta={"schema_ready": False, "trading_mode": TRADING_MODE},
            conn=conn,
        )
        conn.close()
        raise

    running: Dict[BotKey, BotProc] = {}
    last_restart_attempt: Dict[BotKey, float] = {}

    try:
        while not _shutdown:
            tick_start = time.perf_counter()
            tick_error = None
            desired = fetch_desired_configs(conn)

            # 1) Zatrzymaj te, które nie powinny działać
            for key, proc in list(running.items()):
                row = desired.get(key)
                if not row or not row.get("enabled", False):
                    stop_bot(proc)
                    running.pop(key, None)

            # 2) Usuń zakończone procesy. Krok 3 uruchomi kwalifikujące się
            # restarty: pojedynczy natychmiast, wiele jako bounded batch.
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

            # 3) Startuj brakujące w stabilnym, ograniczonym batchu. Pojedynczy
            # restart/enable nie czeka, bo wait występuje tylko między elementami.
            candidates = []
            now = time.time()
            for key, row in ordered_start_candidates(desired, running):
                last = last_restart_attempt.get(key, 0.0)
                if now - last >= RESTART_BACKOFF_SECONDS:
                    candidates.append((key, row))

            start_worker_batch(
                candidates,
                running,
                last_restart_attempt,
                stagger_seconds=STARTUP_STAGGER_SECONDS,
            )

            elapsed = time.perf_counter() - tick_start
            record_worker_heartbeat(
                "bot-runner",
                status="healthy",
                loop_duration_s=elapsed,
                meta={
                    "running_bots": len(running),
                    "desired_bots": len(desired),
                    "poll_seconds": POLL_SECONDS,
                    "trading_mode": TRADING_MODE,
                },
                conn=conn,
            )

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

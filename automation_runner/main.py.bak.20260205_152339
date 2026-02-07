import os
import time
import logging
import requests
from datetime import datetime, timezone

from common.db import get_db_conn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] automation-runner: %(message)s")


def q1(cur, sql, params=None):
    cur.execute(sql, params or ())
    row = cur.fetchone()
    return row[0] if row else None


def exec_sql(cur, sql, params=None):
    cur.execute(sql, params or ())


def upsert_kv(cur, key, value):
    exec_sql(cur, """
        INSERT INTO automation_kv(key, value, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (key) DO UPDATE
        SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at;
    """, (key, value))


def set_panic(cur, enabled: bool, reason: str):
    exec_sql(cur, """
        UPDATE panic_state
        SET panic_enabled=%s, reason=%s, updated_at=now()
        WHERE id=true;
    """, (enabled, reason))


def disable_live_orders(cur, reason: str):
    # DISABLE-ONLY: tylko wyłączamy (nigdy nie włączamy)
    exec_sql(cur, """
        UPDATE bot_control
        SET live_orders_enabled=false, reason=%s, updated_at=now()
        WHERE live_orders_enabled=true;
    """, (reason,))


def now_utc_hhmm():
    return datetime.now(timezone.utc).strftime("%H%M")


def run_daily_report(conn):
    if os.getenv("DAILY_REPORT_ENABLED", "0") != "1":
        return

    target = os.getenv("DAILY_REPORT_HHMM_UTC", "0020")
    cur_hhmm = now_utc_hhmm()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logging.info("daily_report: check cur=%s target=%s", cur_hhmm, target)

    with conn.cursor() as cur:
        last = q1(cur, "SELECT value FROM automation_kv WHERE key='daily_report_last_run';")

        # Idempotencja: już wykonane dziś
        if last == today:
            return

        # Jeszcze za wcześnie (nie minęliśmy targetu)
        if cur_hhmm < target:
            return

        sql1 = "/app/scripts/010_daily_report_upsert.sql"
        sql2 = "/app/scripts/011_daily_report_retention.sql"

        logging.info(
            "daily_report: running 010/011 for %s (sql1=%s sql2=%s)",
            os.getenv("DB_NAME"), sql1, sql2
        )

        for path in (sql1, sql2):
            with open(path, "r", encoding="utf-8") as f:
                cur.execute(f.read())

        upsert_kv(cur, "daily_report_last_run", today)

    conn.commit()
    logging.info("daily_report: done")


def run_ip_panic_watch(conn):
    if os.getenv("IP_PANIC_WATCH_ENABLED", "0") != "1":
        return

    url = os.getenv("IPIFY_URL", "https://api.ipify.org")
    try:
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        new_ip = r.text.strip()
    except Exception as e:
        logging.warning("ip_panic_watch: could not fetch IP: %s", e)
        return

    with conn.cursor() as cur:
        old_ip = q1(cur, "SELECT value FROM automation_kv WHERE key='last_public_ip';")
        if old_ip == new_ip:
            return

        upsert_kv(cur, "last_public_ip", new_ip)
        reason = f"PUBLIC_IP_CHANGED old='{old_ip}' new='{new_ip}'"
        logging.warning("ip_panic_watch: %s", reason)

        set_panic(cur, True, reason)
        disable_live_orders(cur, "FAILSAFE: panic engaged (IP changed)")

    conn.commit()


def run_regime_watchdog(conn):
    if os.getenv("REGIME_WATCHDOG_ENABLED", "0") != "1":
        return

    max_1m = int(os.getenv("REGIME_LAG_MAX_1M_SECONDS", "420"))
    max_5m = int(os.getenv("REGIME_LAG_MAX_5M_SECONDS", "1200"))

    with conn.cursor() as cur:
        cur.execute("""
            SELECT interval, EXTRACT(EPOCH FROM (now() - MAX(ts)))::int AS lag_s
            FROM market_regime
            GROUP BY interval;
        """)
        rows = cur.fetchall()

        bad = []
        for interval, lag_s in rows:
            if interval == "1m" and lag_s > max_1m:
                bad.append((interval, lag_s, max_1m))
            if interval == "5m" and lag_s > max_5m:
                bad.append((interval, lag_s, max_5m))

        if not bad:
            return

        msg = "FAILSAFE: stale market_regime " + ", ".join([f"{i} lag={ls}s>{mx}s" for i, ls, mx in bad])
        logging.error("regime_watchdog: %s", msg)

        set_panic(cur, True, msg)
        disable_live_orders(cur, msg)

    conn.commit()


def main():
    if os.getenv("AUTOMATION_ENABLED", "0") != "1":
        logging.info("AUTOMATION_ENABLED!=1; exiting")
        return

    mode = os.getenv("AUTOMATION_MODE", "DISABLE_ONLY")
    if mode != "DISABLE_ONLY":
        logging.error("Refusing to start: AUTOMATION_MODE must be DISABLE_ONLY")
        return

    ip_int = int(os.getenv("IP_CHECK_INTERVAL_SECONDS", "60"))
    rg_int = int(os.getenv("REGIME_WATCH_INTERVAL_SECONDS", "60"))

    last_ip = 0.0
    last_rg = 0.0

    tick_s = int(os.environ.get("AUTOMATION_TICK_SECONDS", "60"))
    dbname = os.environ.get("DB_NAME", "")

    logging.info("started (mode=%s, db=%s, tick_s=%s)", mode, dbname, tick_s)

    while True:
        try:
            conn = get_db_conn()
            conn.autocommit = False

            now = time.time()

            run_daily_report(conn)
            logging.info("tick ok")

            if now - last_ip >= ip_int:
                run_ip_panic_watch(conn)
                last_ip = now

            if now - last_rg >= rg_int:
                run_regime_watchdog(conn)
                last_rg = now

            conn.close()
        except Exception as e:
            logging.exception("tick failed: %s", str(e))

        time.sleep(tick_s)


if __name__ == "__main__":
    main()
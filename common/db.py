# common/db.py
import os
import time
import psycopg2

def get_db_conn():
    host = os.environ.get("DB_HOST", "db")
    port = int(os.environ.get("DB_PORT", "5432"))
    dbname = os.environ.get("DB_NAME", os.environ.get("POSTGRES_DB", "trading"))
    user = os.environ.get("DB_USER", os.environ.get("POSTGRES_USER", "botuser"))
    password = os.environ.get("DB_PASS", os.environ.get("POSTGRES_PASSWORD", "botpass"))

    # retry/backoff na start DB (PAPER/LIVE)
    max_wait_s = int(os.environ.get("DB_CONNECT_MAX_WAIT_SECONDS", "60"))
    connect_timeout_s = int(os.environ.get("DB_CONNECT_TIMEOUT_SECONDS", "5"))

    delay_s = 1.0
    t0 = time.time()

    while True:
        try:
            return psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
                connect_timeout=connect_timeout_s,
            )
        except Exception:
            if time.time() - t0 >= max_wait_s:
                raise
            time.sleep(delay_s)
            delay_s = min(delay_s * 1.5, 5.0)


def get_latest_regime(symbol: str, interval: str):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT ts, regime, vol_regime, trend_dir, trend_strength_pct, atr_pct, shock_z,
                   ema_fast, ema_slow, score_trend, score_vol, score_shock
            FROM market_regime
            WHERE symbol=%s AND interval=%s
            ORDER BY ts DESC
            LIMIT 1;
            """,
            (symbol, interval),
        )
        row = cur.fetchone()
        if not row:
            return None
        keys = [
            "ts", "regime", "vol_regime", "trend_dir", "trend_strength_pct", "atr_pct", "shock_z",
            "ema_fast", "ema_slow", "score_trend", "score_vol", "score_shock",
        ]
        return dict(zip(keys, row))
    finally:
        cur.close()
        conn.close()
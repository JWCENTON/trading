# common/db.py
import os
import psycopg2

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "trading")
DB_USER = os.environ.get("DB_USER", "botuser")
DB_PASS = os.environ.get("DB_PASS", "botpass")

def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )

def get_latest_regime(symbol: str, interval: str):
    conn = get_db_conn()
    cur = conn.cursor()
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
    cur.close()
    conn.close()
    if not row:
        return None
    keys = [
        "ts", "regime", "vol_regime", "trend_dir", "trend_strength_pct", "atr_pct", "shock_z",
        "ema_fast", "ema_slow", "score_trend", "score_vol", "score_shock",
    ]
    return dict(zip(keys, row))
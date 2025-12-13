import os
import time
import json
import logging
import pandas as pd
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_batch
from binance.client import Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# =========================
# ENV / CONFIG
# =========================

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "trading")
DB_USER = os.environ.get("DB_USER", "botuser")
DB_PASS = os.environ.get("DB_PASS", "botpass")

STRATEGY_NAME = os.environ.get("STRATEGY_NAME", "RSI")
TRADING_MODE = os.environ.get("TRADING_MODE", "PAPER").upper()  # PAPER | LIVE

SYMBOL = os.environ.get("SYMBOL", "BTCUSDT")
INTERVAL = os.environ.get("INTERVAL", "1m")

RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))
EMA_PERIOD = int(os.environ.get("EMA_PERIOD", "21"))

RSI_OVERSOLD = float(os.environ.get("RSI_OVERSOLD", "25"))
RSI_OVERBOUGHT = float(os.environ.get("RSI_OVERBOUGHT", "75"))

PAPER_START_USDT = float(os.environ.get("PAPER_START_USDT", "1000"))

STOP_LOSS_PCT = float(os.environ.get("STOP_LOSS_PCT", "0.8"))
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "1.2"))

DISABLE_HOURS = os.environ.get("DISABLE_HOURS", "")
DISABLE_HOURS_SET = {int(h) for h in DISABLE_HOURS.split(",") if h.strip()}

TREND_BUFFER = float(os.environ.get("TREND_BUFFER", "0.001"))
MAX_POSITION_MINUTES = int(os.environ.get("MAX_POSITION_MINUTES", "90"))
DAILY_MAX_LOSS_PCT = float(os.environ.get("DAILY_MAX_LOSS_PCT", "0.5"))

ORDER_QTY_BTC = float(os.environ.get("ORDER_QTY_BTC", "0.0001"))
MAX_DIST_FROM_EMA_PCT = float(os.environ.get("MAX_DIST_FROM_EMA_PCT", "0.5"))

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

client = Client(api_key=API_KEY, api_secret=API_SECRET)

# =========================
# DB HELPERS
# =========================

def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def heartbeat(info: dict):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO bot_heartbeat(symbol, strategy, last_seen, info)
        VALUES (%s, %s, now(), %s::jsonb)
        ON CONFLICT (symbol, strategy)
        DO UPDATE SET last_seen=now(), info=EXCLUDED.info;
        """,
        (SYMBOL, STRATEGY_NAME, json.dumps(info)),
    )
    conn.commit()
    cur.close()
    conn.close()


def get_mode() -> str:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT mode FROM bot_control WHERE symbol=%s AND strategy=%s",
        (SYMBOL, STRATEGY_NAME),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else "NORMAL"


def set_mode(mode: str, reason: str = None):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO bot_control(symbol, strategy, mode, reason, updated_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (symbol, strategy)
        DO UPDATE SET mode=EXCLUDED.mode, reason=EXCLUDED.reason, updated_at=now();
        """,
        (SYMBOL, STRATEGY_NAME, mode, reason),
    )
    conn.commit()
    cur.close()
    conn.close()


def get_open_position():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, side, qty, entry_price, entry_time
        FROM positions
        WHERE symbol=%s AND strategy=%s AND interval=%s AND status='OPEN'
        ORDER BY entry_time DESC
        LIMIT 1
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def open_position(side: str, qty: float, entry_price: float) -> bool:
    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT 1 FROM positions
        WHERE symbol=%s AND strategy=%s AND interval=%s AND status='OPEN'
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL),
    )
    if cur.fetchone():
        cur.close()
        conn.close()
        logging.info("RSI: open_position skipped – already OPEN.")
        return False

    cur.execute(
        """
        INSERT INTO positions(symbol, strategy, interval, status, side, qty, entry_price, entry_time)
        VALUES (%s, %s, %s, 'OPEN', %s, %s, %s, now())
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL, side, qty, entry_price),
    )
    conn.commit()
    cur.close()
    conn.close()

    logging.info("RSI: position OPENED %s qty=%.8f entry=%.2f", side, qty, entry_price)
    return True


def close_position(exit_price: float, reason: str) -> bool:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE positions
        SET status='CLOSED', exit_price=%s, exit_time=now(), exit_reason=%s
        WHERE symbol=%s AND strategy=%s AND interval=%s AND status='OPEN'
        RETURNING id;
        """,
        (exit_price, reason, SYMBOL, STRATEGY_NAME, INTERVAL),
    )
    closed = cur.fetchone() is not None
    conn.commit()
    cur.close()
    conn.close()

    if closed:
        logging.info("RSI: position CLOSED exit=%.2f reason=%s", exit_price, reason)
    else:
        logging.info("RSI: close_position skipped – no OPEN position.")
    return closed


# =========================
# DB INIT
# =========================

def init_db():
    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS candles (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        open_time TIMESTAMPTZ NOT NULL,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume NUMERIC,
        close_time TIMESTAMPTZ NOT NULL,
        trades INTEGER,
        ema_21 NUMERIC,
        rsi_14 NUMERIC,
        UNIQUE(symbol, interval, open_time)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS simulated_orders (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        strategy TEXT NOT NULL,
        side TEXT NOT NULL,
        price NUMERIC NOT NULL,
        quantity_btc NUMERIC NOT NULL,
        reason TEXT,
        rsi_14 NUMERIC,
        ema_21 NUMERIC,
        candle_open_time TIMESTAMPTZ NOT NULL
    );
    """)

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_sim_orders_one_per_candle
    ON simulated_orders(symbol, interval, strategy, candle_open_time);
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS bot_control (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        strategy TEXT NOT NULL,
        mode TEXT NOT NULL DEFAULT 'NORMAL',
        reason TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE(symbol, strategy)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS bot_heartbeat (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        strategy TEXT NOT NULL,
        last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
        info JSONB,
        UNIQUE(symbol, strategy)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS positions (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        strategy TEXT NOT NULL,
        interval TEXT NOT NULL,
        status TEXT NOT NULL,
        side TEXT NOT NULL,
        qty NUMERIC NOT NULL,
        entry_price NUMERIC NOT NULL,
        entry_time TIMESTAMPTZ NOT NULL DEFAULT now(),
        exit_price NUMERIC,
        exit_time TIMESTAMPTZ,
        exit_reason TEXT
    );
    """)

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_positions_open
    ON positions(symbol, strategy, interval)
    WHERE status='OPEN';
    """)

    cur.execute(
        """
        INSERT INTO bot_control(symbol, strategy, mode)
        VALUES (%s, %s, 'NORMAL')
        ON CONFLICT DO NOTHING;
        """,
        (SYMBOL, STRATEGY_NAME),
    )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("RSI DB initialized.")


# =========================
# ORDERS (IDEMPOTENT)
# =========================

def insert_simulated_order(
    symbol,
    interval,
    side,
    price,
    qty_btc,
    reason,
    rsi_14,
    ema_21,
    candle_open_time,
):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO simulated_orders (
            symbol, interval, strategy, side, price, quantity_btc,
            reason, rsi_14, ema_21, candle_open_time
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, interval, strategy, candle_open_time)
        DO NOTHING
        RETURNING id;
        """,
        (
            symbol, interval, STRATEGY_NAME, side, price, qty_btc,
            reason, rsi_14, ema_21, candle_open_time,
        ),
    )
    inserted = cur.fetchone() is not None
    conn.commit()
    cur.close()
    conn.close()

    if inserted:
        logging.info("RSI simulated %s @ %.2f qty=%.8f (%s)", side, price, qty_btc, reason)
    else:
        logging.info("RSI simulated order skipped by DB guard.")
    return inserted


# =========================
# PNL
# =========================

def compute_daily_pnl_pct(symbol: str, interval: str, current_price: float) -> float:
    if DAILY_MAX_LOSS_PCT <= 0:
        return 0.0

    today = datetime.utcnow().date()

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT created_at, side, price, quantity_btc
        FROM simulated_orders
        WHERE symbol=%s AND interval=%s AND strategy=%s
        ORDER BY created_at ASC
        """,
        (symbol, interval, STRATEGY_NAME),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    cash = PAPER_START_USDT
    btc = 0.0
    equity_start_today = None

    for created_at, side, price, qty in rows:
        created_utc = created_at.astimezone(timezone.utc)
        price = float(price)
        qty = float(qty)

        if equity_start_today is None and created_utc.date() >= today:
            equity_start_today = cash + btc * price

        if side == "BUY":
            cash -= qty * price
            btc += qty
        else:
            cash += qty * price
            btc -= qty

    if equity_start_today is None:
        return 0.0

    equity_now = cash + btc * current_price
    return (equity_now - equity_start_today) / equity_start_today * 100.0


# =========================
# MAIN LOOP
# =========================

def main_loop():
    init_db()
    while True:
        start = time.perf_counter()
        try:
            # fetch candles
            klines = client.get_klines(symbol=SYMBOL, interval=INTERVAL, limit=50)

            rows = []
            for k in klines:
                rows.append((
                    SYMBOL,
                    INTERVAL,
                    datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc),
                    k[1], k[2], k[3], k[4],
                    k[5],
                    datetime.fromtimestamp(k[6] / 1000, tz=timezone.utc),
                    k[8],
                ))

            conn = get_db_conn()
            cur = conn.cursor()
            execute_batch(
                cur,
                """
                INSERT INTO candles (
                    symbol, interval, open_time,
                    open, high, low, close,
                    volume, close_time, trades
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING;
                """,
                rows,
            )
            conn.commit()
            cur.close()
            conn.close()

        except Exception:
            logging.exception("RSI loop error")

        logging.info("RSI loop finished in %.3f s", time.perf_counter() - start)
        time.sleep(60)


if __name__ == "__main__":
    logging.info("Starting RSI bot %s %s", SYMBOL, INTERVAL)
    main_loop()
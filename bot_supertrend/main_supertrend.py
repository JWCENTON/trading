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

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "trading")
DB_USER = os.environ.get("DB_USER", "botuser")
DB_PASS = os.environ.get("DB_PASS", "botpass")

SYMBOL = os.environ.get("SYMBOL", "BTCUSDT")
INTERVAL = os.environ.get("INTERVAL", "1m")

TRADING_MODE = os.environ.get("TRADING_MODE", "PAPER").upper()  # PAPER | LIVE (na razie PAPER)

STRATEGY_NAME = os.environ.get("STRATEGY_NAME", "SUPER_TREND")

# parametry SuperTrend
ATR_PERIOD = int(os.environ.get("ATR_PERIOD", "14"))
ST_MULTIPLIER = float(os.environ.get("ST_MULTIPLIER", "3.0"))

PAPER_START_USDT = float(os.environ.get("PAPER_START_USDT", "100"))

STOP_LOSS_PCT = float(os.environ.get("STOP_LOSS_PCT", "0.8"))      # np. 0.8%
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "1.2"))  # np. 1.2%

# Filtr trendu po EMA21 (0.001 = 0.1%)
TREND_BUFFER = float(os.environ.get("TREND_BUFFER", "0.001"))

# Minimalna zmienność (ATR%) żeby w ogóle wchodzić w trade
# np. 0.25 = 0.25%
MIN_ATR_PCT = float(os.environ.get("MIN_ATR_PCT", "0.25"))

# Filtry RSI – blokady ekstremów i „zdrowe” zakresy dla LONG/SHORT
RSI_LONG_MAX = float(os.environ.get("RSI_LONG_MAX", "70.0"))        # powyżej tego nie bierz LONG
RSI_SHORT_MIN = float(os.environ.get("RSI_SHORT_MIN", "30.0"))      # poniżej tego nie bierz SHORT
RSI_BLOCK_EXTREME_LOW = float(os.environ.get("RSI_BLOCK_EXTREME_LOW", "15.0"))
RSI_BLOCK_EXTREME_HIGH = float(os.environ.get("RSI_BLOCK_EXTREME_HIGH", "85.0"))

DISABLE_HOURS = os.environ.get("DISABLE_HOURS", "")
DISABLE_HOURS_SET = {int(h.strip()) for h in DISABLE_HOURS.split(",") if h.strip() != ""}

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")
client = Client(api_key=API_KEY, api_secret=API_SECRET)

# Max czas pozycji w minutach (timeout)
MAX_POSITION_MINUTES = int(os.environ.get("MAX_POSITION_MINUTES", "90"))

# Dzienny stop-loss (jeśli <= 0 -> wyłączony)
DAILY_MAX_LOSS_PCT = float(os.environ.get("DAILY_MAX_LOSS_PCT", "0.5"))

# Ilość na trade (PAPER)
ORDER_QTY_BTC = float(os.environ.get("ORDER_QTY_BTC", "0.0001"))

# =================
# Regime
# =================

from common.db import get_latest_regime
from datetime import datetime, timezone

REGIME_ENABLED = os.environ.get("REGIME_ENABLED", "0") == "1"
REGIME_MODE = os.environ.get("REGIME_MODE", "DRY_RUN").strip().upper()  # DRY_RUN | ENFORCE
REGIME_MAX_AGE_SECONDS = int(os.environ.get("REGIME_MAX_AGE_SECONDS", "180"))

def regime_allows(strategy_name: str, symbol: str, interval: str):
    """
    Zwraca: (allow: bool, meta: dict)
    DRY_RUN: zawsze allow=True, ale meta mówi czy 'would_block'.
    """
    if not REGIME_ENABLED:
        return True, {"enabled": False}

    r = get_latest_regime(symbol, interval)
    if not r:
        # fail-open (zależnie od preferencji); na etapie PAPER OK
        return True, {"enabled": True, "reason": "no_regime"}

    # świeżość
    ts = r["ts"]
    if ts is None:
        return True, {"enabled": True, "reason": "regime_ts_null"}

    now = datetime.now(timezone.utc)
    age = (now - ts).total_seconds()
    if age > REGIME_MAX_AGE_SECONDS:
        return True, {"enabled": True, "reason": f"regime_stale age={age:.0f}s", "age_s": age, "regime": r.get("regime")}

    regime = r.get("regime")

    # Polityka blokowania (v1):
    # - RSI/BBRANGE: blokuj w TREND_UP/TREND_DOWN i SHOCK
    # - TREND: blokuj w RANGE_* i SHOCK
    # - SUPER_TREND: blokuj w SHOCK (opcjonalnie też RANGE_LOWVOL, ale zostawmy tylko SHOCK na start)
    would_block = False
    why = "ok"

    if strategy_name in ("RSI", "BBRANGE"):
        if regime in ("TREND_UP", "TREND_DOWN", "SHOCK"):
            would_block = True
            why = f"{strategy_name} blocked in {regime}"
    elif strategy_name in ("TREND",):
        if regime in ("RANGE_LOWVOL", "RANGE_HIGHVOL", "SHOCK"):
            would_block = True
            why = f"TREND blocked in {regime}"
    elif strategy_name in ("SUPER_TREND", "SUPER_TREND", "ST"):
        if regime in ("SHOCK",):
            would_block = True
            why = f"SUPER_TREND blocked in {regime}"

    # DRY_RUN: nie blokujemy, tylko logujemy
    if REGIME_MODE == "DRY_RUN":
        return True, {"enabled": True, "mode": "DRY_RUN", "would_block": would_block, "why": why, **r, "age_s": age}

    # ENFORCE: blokujemy jeśli would_block
    allow = not would_block
    return allow, {"enabled": True, "mode": "ENFORCE", "would_block": would_block, "why": why, **r, "age_s": age}


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


def log_regime_gate_event(
    symbol: str,
    interval: str,
    strategy: str,
    decision: str,
    allow: bool,
    rmeta: dict,
    extra_meta: dict = None,
):
    """
    Zapisuje event tylko gdy:
    - ENFORCE blokuje (allow=False)
    - lub DRY_RUN mówi would_block=True
    """
    if not rmeta:
        return

    mode = rmeta.get("mode")
    would_block = rmeta.get("would_block", False)

    should_log = (allow is False) or (mode == "DRY_RUN" and would_block)
    if not should_log:
        return

    meta = {}
    if isinstance(rmeta, dict):
        meta["rmeta"] = rmeta
    if extra_meta:
        meta.update(extra_meta)

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO regime_gate_events
          (symbol, interval, strategy, decision, allow, regime, mode, would_block, why, meta)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb);
        """,
        (
            symbol,
            interval,
            strategy,
            decision,
            bool(allow),
            rmeta.get("regime"),
            mode,
            bool(would_block) if would_block is not None else None,
            rmeta.get("why"),
            json.dumps(meta),
        ),
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
        SELECT 1
        FROM positions
        WHERE symbol=%s AND strategy=%s AND interval=%s AND status='OPEN'
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL),
    )
    if cur.fetchone():
        cur.close()
        conn.close()
        logging.info("SUPER_TREND: open_position skipped - position already OPEN.")
        return False

    cur.execute(
        """
        INSERT INTO positions(symbol, strategy, interval, status, side, qty, entry_price, entry_time)
        VALUES (%s, %s, %s, 'OPEN', %s, %s, %s, now())
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL, side, float(qty), float(entry_price)),
    )
    conn.commit()
    cur.close()
    conn.close()
    logging.info("SUPER_TREND: position OPENED side=%s qty=%.8f entry=%.2f", side, float(qty), float(entry_price))
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
        (float(exit_price), reason, SYMBOL, STRATEGY_NAME, INTERVAL),
    )
    closed = cur.fetchone() is not None
    conn.commit()
    cur.close()
    conn.close()
    if closed:
        logging.info("SUPER_TREND: position CLOSED reason=%s exit=%.2f", reason, float(exit_price))
    else:
        logging.info("SUPER_TREND: close_position skipped - no OPEN position found.")
    return closed


def init_db():
    conn = get_db_conn()
    cur = conn.cursor()

    # candles – kolumny pod ATR / SuperTrend + (opcjonalnie) EMA/RSI
    cur.execute(
        """
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
            atr_14 NUMERIC,
            supertrend NUMERIC,
            supertrend_direction INTEGER,
            UNIQUE(symbol, interval, open_time)
        );
        """
    )

    # Strategy params
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_params (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            strategy TEXT NOT NULL,
            param_name TEXT NOT NULL,
            param_value NUMERIC NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE(symbol, strategy, param_name)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_params_history (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            strategy TEXT NOT NULL,
            param_name TEXT NOT NULL,
            old_value NUMERIC,
            new_value NUMERIC NOT NULL,
            changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            source TEXT NOT NULL
        );
        """
    )

    # Bot control / heartbeat / positions
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
      status TEXT NOT NULL,        -- OPEN | CLOSED
      side TEXT NOT NULL,          -- LONG | SHORT
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
        ON CONFLICT (symbol, strategy) DO NOTHING;
        """,
        (SYMBOL, STRATEGY_NAME),
    )

    conn.commit()

    seed_default_params_from_env(conn)

    cur.close()
    conn.close()
    logging.info("DB initialized for SUPER_TREND.")


def seed_default_params_from_env(conn):
    """
    Seed parametrów z ENV do strategy_params tylko jeśli nie istnieją.
    """
    global ATR_PERIOD, ST_MULTIPLIER, STOP_LOSS_PCT, TAKE_PROFIT_PCT
    global TREND_BUFFER, MIN_ATR_PCT
    global RSI_LONG_MAX, RSI_SHORT_MIN, RSI_BLOCK_EXTREME_LOW, RSI_BLOCK_EXTREME_HIGH
    global MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT, ORDER_QTY_BTC

    cur = conn.cursor()
    cur.execute(
        """
        SELECT param_name, param_value
        FROM strategy_params
        WHERE symbol=%s AND strategy=%s
        """,
        (SYMBOL, STRATEGY_NAME),
    )
    existing_rows = cur.fetchall()
    existing = {name: float(value) for (name, value) in existing_rows}

    defaults = {
        "ATR_PERIOD": float(ATR_PERIOD),
        "ST_MULTIPLIER": float(ST_MULTIPLIER),
        "STOP_LOSS_PCT": float(STOP_LOSS_PCT),
        "TAKE_PROFIT_PCT": float(TAKE_PROFIT_PCT),
        "TREND_BUFFER": float(TREND_BUFFER),
        "MIN_ATR_PCT": float(MIN_ATR_PCT),
        "RSI_LONG_MAX": float(RSI_LONG_MAX),
        "RSI_SHORT_MIN": float(RSI_SHORT_MIN),
        "RSI_BLOCK_EXTREME_LOW": float(RSI_BLOCK_EXTREME_LOW),
        "RSI_BLOCK_EXTREME_HIGH": float(RSI_BLOCK_EXTREME_HIGH),
        "MAX_POSITION_MINUTES": float(MAX_POSITION_MINUTES),
        "DAILY_MAX_LOSS_PCT": float(DAILY_MAX_LOSS_PCT),
        "ORDER_QTY_BTC": float(ORDER_QTY_BTC),
    }

    inserted_any = False
    for name, value in defaults.items():
        if name in existing:
            continue

        cur.execute(
            """
            INSERT INTO strategy_params(symbol, strategy, param_name, param_value)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, strategy, param_name) DO NOTHING;
            """,
            (SYMBOL, STRATEGY_NAME, name, value),
        )
        cur.execute(
            """
            INSERT INTO strategy_params_history(symbol, strategy, param_name, old_value, new_value, source)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (SYMBOL, STRATEGY_NAME, name, None, value, "MANUAL"),
        )
        inserted_any = True

    if inserted_any:
        conn.commit()
        logging.info("Seeded default SUPER_TREND params from ENV for %s/%s.", SYMBOL, STRATEGY_NAME)

    cur.close()


def load_runtime_params():
    global ATR_PERIOD, ST_MULTIPLIER
    global STOP_LOSS_PCT, TAKE_PROFIT_PCT
    global MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT
    global MIN_ATR_PCT, TREND_BUFFER
    global RSI_LONG_MAX, RSI_SHORT_MIN, RSI_BLOCK_EXTREME_LOW, RSI_BLOCK_EXTREME_HIGH
    global ORDER_QTY_BTC

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT param_name, param_value
        FROM strategy_params
        WHERE symbol=%s AND strategy=%s
        """,
        (SYMBOL, STRATEGY_NAME),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    params = {name: float(value) for (name, value) in rows} if rows else {}

    def clamp(val, lo, hi):
        return max(lo, min(hi, val))

    if "ATR_PERIOD" in params:
        ATR_PERIOD = int(clamp(params["ATR_PERIOD"], 5, 100))
    if "ST_MULTIPLIER" in params:
        ST_MULTIPLIER = clamp(params["ST_MULTIPLIER"], 1.0, 10.0)

    if "STOP_LOSS_PCT" in params:
        min_sl = 0.8 if SYMBOL == "BTCUSDT" else 0.3
        STOP_LOSS_PCT = clamp(params["STOP_LOSS_PCT"], min_sl, 5.0)
    if "TAKE_PROFIT_PCT" in params:
        TAKE_PROFIT_PCT = clamp(params["TAKE_PROFIT_PCT"], 0.1, 10.0)

    if "MAX_POSITION_MINUTES" in params:
        MAX_POSITION_MINUTES = int(clamp(params["MAX_POSITION_MINUTES"], 5, 24 * 60))
    if "DAILY_MAX_LOSS_PCT" in params:
        DAILY_MAX_LOSS_PCT = clamp(params["DAILY_MAX_LOSS_PCT"], 0.0, 10.0)

    if "MIN_ATR_PCT" in params:
        MIN_ATR_PCT = clamp(params["MIN_ATR_PCT"], 0.05, 5.0)
    if "TREND_BUFFER" in params:
        TREND_BUFFER = clamp(params["TREND_BUFFER"], 0.0001, 0.02)

    if "RSI_LONG_MAX" in params:
        RSI_LONG_MAX = clamp(params["RSI_LONG_MAX"], 50.0, 95.0)
    if "RSI_SHORT_MIN" in params:
        RSI_SHORT_MIN = clamp(params["RSI_SHORT_MIN"], 5.0, 50.0)
    if "RSI_BLOCK_EXTREME_LOW" in params:
        RSI_BLOCK_EXTREME_LOW = clamp(params["RSI_BLOCK_EXTREME_LOW"], 0.0, 30.0)
    if "RSI_BLOCK_EXTREME_HIGH" in params:
        RSI_BLOCK_EXTREME_HIGH = clamp(params["RSI_BLOCK_EXTREME_HIGH"], 70.0, 100.0)

    if "ORDER_QTY_BTC" in params:
        ORDER_QTY_BTC = clamp(params["ORDER_QTY_BTC"], 0.00001, 1.0)

    logging.info(
        "RUNTIME_PARAMS|symbol=%s|strategy=%s|ATR_PERIOD=%d|ST_MULTIPLIER=%.3f|"
        "STOP_LOSS_PCT=%.3f|TAKE_PROFIT_PCT=%.3f|MAX_POSITION_MINUTES=%d|"
        "DAILY_MAX_LOSS_PCT=%.3f|TREND_BUFFER=%.5f|MIN_ATR_PCT=%.3f|"
        "RSI_LONG_MAX=%.2f|RSI_SHORT_MIN=%.2f|RSI_BLOCK_EXTREME_LOW=%.2f|RSI_BLOCK_EXTREME_HIGH=%.2f|"
        "ORDER_QTY_BTC=%.8f",
        SYMBOL, STRATEGY_NAME,
        ATR_PERIOD, ST_MULTIPLIER,
        STOP_LOSS_PCT, TAKE_PROFIT_PCT,
        MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT,
        TREND_BUFFER, MIN_ATR_PCT,
        RSI_LONG_MAX, RSI_SHORT_MIN, RSI_BLOCK_EXTREME_LOW, RSI_BLOCK_EXTREME_HIGH,
        ORDER_QTY_BTC,
    )


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
    strategy: str = STRATEGY_NAME,
):
    """
    DB-guard: max 1 trade na świecę per (symbol, interval, strategy, candle_open_time)
    """
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO simulated_orders (
            symbol, interval, side, price, quantity_btc,
            reason, rsi_14, ema_21, candle_open_time, strategy
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, interval, strategy, candle_open_time) DO NOTHING
        RETURNING id;
        """,
        (
            symbol,
            interval,
            side,
            float(price),
            float(qty_btc),
            reason,
            float(rsi_14) if rsi_14 is not None else None,
            float(ema_21) if ema_21 is not None else None,
            candle_open_time,
            strategy,
        ),
    )
    inserted = cur.fetchone() is not None
    conn.commit()
    cur.close()
    conn.close()

    if inserted:
        logging.info(
            "SUPER_TREND: Simulated %s at %.2f qty=%.8f (reason=%s)",
            side, float(price), float(qty_btc), reason,
        )
    else:
        logging.info(
            "SUPER_TREND: Simulated order skipped by DB guard (symbol=%s interval=%s strategy=%s candle_open_time=%s).",
            symbol, interval, strategy, candle_open_time,
        )
    return inserted


def safe_close_if_open(current_price: float, candle_open_time):
    pos = get_open_position()
    if not pos:
        return False

    _, side, qty, _, _ = pos
    side_u = str(side).upper()

    if side_u == "LONG":
        exit_side = "SELL"
        reason = "PANIC CLOSE LONG"
    else:
        exit_side = "BUY"
        reason = "PANIC CLOSE SHORT"

    inserted = insert_simulated_order(
        symbol=SYMBOL,
        interval=INTERVAL,
        side=exit_side,
        price=current_price,
        qty_btc=float(qty),
        reason=reason,
        rsi_14=None,
        ema_21=None,
        candle_open_time=candle_open_time,
    )
    if not inserted:
        logging.info("SUPER_TREND: panic close blocked by DB guard.")
        return False

    return close_position(exit_price=current_price, reason="PANIC")


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

    if not rows:
        return 0.0

    cash = PAPER_START_USDT
    btc = 0.0
    equity_start_today = None

    for created_at, side, price, qty_btc in rows:
        price_f = float(price)
        qty_f = float(qty_btc)

        created_utc = created_at.astimezone(timezone.utc)
        if equity_start_today is None and created_utc.date() >= today:
            equity_start_today = cash + btc * price_f

        if side.upper() == "BUY":
            cash -= qty_f * price_f
            btc += qty_f
        elif side.upper() == "SELL":
            cash += qty_f * price_f
            btc -= qty_f

    if equity_start_today is None:
        return 0.0

    equity_now = cash + btc * current_price
    return (equity_now - equity_start_today) / equity_start_today * 100.0


def get_trend(close_price: float, ema21: float, buffer_pct: float = TREND_BUFFER) -> str:
    if ema21 is None:
        return "FLAT"
    if close_price > ema21 * (1.0 + buffer_pct):
        return "UP"
    elif close_price < ema21 * (1.0 - buffer_pct):
        return "DOWN"
    else:
        return "FLAT"


def fetch_klines():
    logging.info("Fetching klines for %s, interval %s", SYMBOL, INTERVAL)
    start = time.perf_counter()
    klines = client.get_klines(symbol=SYMBOL, interval=INTERVAL, limit=50)
    elapsed = time.perf_counter() - start
    logging.info("Fetched %d klines in %.3f s", len(klines), elapsed)

    rows = []
    for k in klines:
        open_time_ms = k[0]
        open_time = datetime.fromtimestamp(open_time_ms / 1000.0, tz=timezone.utc)

        open_price = k[1]
        high = k[2]
        low = k[3]
        close = k[4]
        volume = k[5]

        close_time_ms = k[6]
        close_time = datetime.fromtimestamp(close_time_ms / 1000.0, tz=timezone.utc)

        trades = k[8]

        rows.append(
            (
                SYMBOL,
                INTERVAL,
                open_time,
                open_price,
                high,
                low,
                close,
                volume,
                close_time,
                trades,
            )
        )

    return rows


def save_klines(rows):
    if not rows:
        logging.info("No klines to save.")
        return

    conn = get_db_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO candles (
            symbol, interval, open_time,
            open, high, low, close,
            volume, close_time, trades
        )
        VALUES (
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (symbol, interval, open_time)
        DO NOTHING;
    """

    start = time.perf_counter()
    execute_batch(cur, sql, rows, page_size=50)
    conn.commit()
    elapsed = time.perf_counter() - start

    cur.close()
    conn.close()
    logging.info("Saved %d klines in %.3f s", len(rows), elapsed)


def update_indicators():
    """
    Liczy ATR + SuperTrend.
    Do DB zapisuje ostatnie ~50 świec.
    """
    conn = get_db_conn()
    df = pd.read_sql_query(
        """
        SELECT id, open_time, high, low, close
        FROM candles
        WHERE symbol = %s AND interval = %s
        ORDER BY open_time
        """,
        conn,
        params=(SYMBOL, INTERVAL),
    )

    if df.empty:
        logging.info("No data for indicators yet.")
        conn.close()
        return

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    # ATR
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(span=ATR_PERIOD, adjust=False).mean()

    # SuperTrend
    hl2 = (high + low) / 2.0
    basic_ub = hl2 + ST_MULTIPLIER * atr
    basic_lb = hl2 - ST_MULTIPLIER * atr

    final_ub = pd.Series(index=df.index, dtype=float)
    final_lb = pd.Series(index=df.index, dtype=float)
    st_dir = pd.Series(index=df.index, dtype=int)
    st_val = pd.Series(index=df.index, dtype=float)

    final_ub.iloc[0] = basic_ub.iloc[0]
    final_lb.iloc[0] = basic_lb.iloc[0]
    st_dir.iloc[0] = 1
    st_val.iloc[0] = final_lb.iloc[0]

    for i in range(1, len(df)):
        if (basic_ub.iloc[i] < final_ub.iloc[i - 1]) or (close.iloc[i - 1] > final_ub.iloc[i - 1]):
            final_ub.iloc[i] = basic_ub.iloc[i]
        else:
            final_ub.iloc[i] = final_ub.iloc[i - 1]

        if (basic_lb.iloc[i] > final_lb.iloc[i - 1]) or (close.iloc[i - 1] < final_lb.iloc[i - 1]):
            final_lb.iloc[i] = basic_lb.iloc[i]
        else:
            final_lb.iloc[i] = final_lb.iloc[i - 1]

        if close.iloc[i] > final_ub.iloc[i - 1]:
            st_dir.iloc[i] = 1
        elif close.iloc[i] < final_lb.iloc[i - 1]:
            st_dir.iloc[i] = -1
        else:
            st_dir.iloc[i] = st_dir.iloc[i - 1]

        st_val.iloc[i] = final_lb.iloc[i] if st_dir.iloc[i] == 1 else final_ub.iloc[i]

    df["atr_14"] = atr
    df["supertrend"] = st_val
    df["supertrend_direction"] = st_dir

    last = df.tail(50)

    cur = conn.cursor()
    sql = """
        UPDATE candles
        SET atr_14 = %s,
            supertrend = %s,
            supertrend_direction = %s
        WHERE id = %s;
    """

    data = [
        (row["atr_14"], row["supertrend"], int(row["supertrend_direction"]), int(row["id"]))
        for _, row in last.iterrows()
    ]

    start = time.perf_counter()
    cur.executemany(sql, data)
    conn.commit()
    elapsed = time.perf_counter() - start

    cur.close()
    conn.close()
    logging.info("Updated ATR/SuperTrend for %d candles in %.3f s", len(data), elapsed)


def run_strategy():
    """
    Wejście:
    - BUY  gdy ST dir flip -1 -> +1
    - SELL gdy ST dir flip +1 -> -1

    Exit:
    - SL/TP/Timeout oparte o positions (hard truth)
    """
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT open_time, close, supertrend_direction, ema_21, rsi_14, atr_14
        FROM candles
        WHERE symbol=%s AND interval=%s
        ORDER BY open_time DESC
        LIMIT 2
        """,
        (SYMBOL, INTERVAL),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        logging.info("SUPER_TREND: no candles yet.")
        return

    latest = rows[0]
    prev = rows[1] if len(rows) > 1 else None

    open_time, close_price, st_dir_curr, ema_val, rsi_val, atr_val = latest
    price = float(close_price)

    # --- SAFE conversions for heartbeat (must exist before heartbeat) ---
    ema_val_f = float(ema_val) if ema_val is not None else None
    rsi_val_f = float(rsi_val) if rsi_val is not None else None

    st_dir_prev = None  # default

    st_dir_curr_i = int(st_dir_curr) if st_dir_curr is not None else None
    st_dir_prev_i = int(prev[2]) if (prev is not None and prev[2] is not None) else None

    st_dir_prev = st_dir_prev_i if st_dir_prev_i is not None else None
    st_dir_curr = st_dir_curr_i if st_dir_curr_i is not None else None

    atr_pct = float(atr_val) / price * 100.0 if atr_val is not None and price > 0 else None

    allow_gate, rmeta_gate = regime_allows(STRATEGY_NAME, SYMBOL, INTERVAL)
    heartbeat({
        "price": float(price),
        "open_time": str(open_time),
        "st_dir_curr": int(st_dir_curr) if st_dir_curr is not None else None,
        "st_dir_prev": int(st_dir_prev) if st_dir_prev is not None else None,
        "atr_14": float(atr_val) if atr_val is not None else None,
        "atr_pct": float(atr_pct) if atr_pct is not None else None,
        "ema_21": ema_val_f,
        "rsi_14": rsi_val_f,
        "regime_enabled": bool(rmeta_gate.get("enabled", False)),
        "regime": rmeta_gate.get("regime"),
        "regime_mode": rmeta_gate.get("mode"),
        "regime_would_block": rmeta_gate.get("would_block"),
        "regime_why": rmeta_gate.get("why"),          
        "regime_reason": rmeta_gate.get("reason"), 
        "regime_ts": str(rmeta_gate.get("ts")),
        "regime_age_s": rmeta_gate.get("age_s"),
    })


    mode = get_mode()
    if mode == "HALT":
        return
    if mode == "PANIC":
        safe_close_if_open(current_price=price, candle_open_time=open_time)
        set_mode("HALT", reason="Panic executed; halting.")
        return

    if st_dir_curr is None or prev is None or prev[2] is None:
        logging.info("SUPER_TREND: SuperTrend not ready yet.")
        return

    st_dir_curr = int(st_dir_curr)
    st_dir_prev = int(prev[2])

    # =========================
    # 1) HARD-TRUTH: pozycja z positions
    # =========================
    pos = get_open_position()
    has_position = pos is not None

    rsi_val_f = float(rsi_val) if rsi_val is not None else None
    ema_val_f = float(ema_val) if ema_val is not None else None

    if has_position:
        pos_id, pos_side, pos_qty, pos_entry_price, pos_entry_time = pos
        pos_side_u = str(pos_side).upper()
        pos_qty_f = float(pos_qty)
        pos_entry_price_f = float(pos_entry_price)

        # SL/TP
        if pos_side_u == "LONG":
            change_pct = (price - pos_entry_price_f) / pos_entry_price_f * 100.0

            if TAKE_PROFIT_PCT > 0 and change_pct >= TAKE_PROFIT_PCT:
                reason = f"SUPER_TREND TAKE PROFIT LONG {change_pct:.2f}% >= {TAKE_PROFIT_PCT:.2f}%"
                inserted = insert_simulated_order(SYMBOL, INTERVAL, "SELL", price, pos_qty_f, reason,
                                                  rsi_val_f, ema_val_f, open_time)
                if inserted:
                    close_position(exit_price=price, reason="TAKE_PROFIT")
                return

            drop_pct = -change_pct
            if drop_pct >= STOP_LOSS_PCT:
                reason = f"SUPER_TREND STOP LOSS LONG {drop_pct:.2f}% >= {STOP_LOSS_PCT:.2f}%"
                inserted = insert_simulated_order(SYMBOL, INTERVAL, "SELL", price, pos_qty_f, reason,
                                                  rsi_val_f, ema_val_f, open_time)
                if inserted:
                    close_position(exit_price=price, reason="STOP_LOSS")
                return

        elif pos_side_u == "SHORT":
            change_pct = (pos_entry_price_f - price) / pos_entry_price_f * 100.0

            if TAKE_PROFIT_PCT > 0 and change_pct >= TAKE_PROFIT_PCT:
                reason = f"SUPER_TREND TAKE PROFIT SHORT {change_pct:.2f}% >= {TAKE_PROFIT_PCT:.2f}%"
                inserted = insert_simulated_order(SYMBOL, INTERVAL, "BUY", price, pos_qty_f, reason,
                                                  rsi_val_f, ema_val_f, open_time)
                if inserted:
                    close_position(exit_price=price, reason="TAKE_PROFIT_SHORT")
                return

            rise_pct = -change_pct
            if rise_pct >= STOP_LOSS_PCT:
                reason = f"SUPER_TREND STOP LOSS SHORT {rise_pct:.2f}% >= {STOP_LOSS_PCT:.2f}%"
                inserted = insert_simulated_order(SYMBOL, INTERVAL, "BUY", price, pos_qty_f, reason,
                                                  rsi_val_f, ema_val_f, open_time)
                if inserted:
                    close_position(exit_price=price, reason="STOP_LOSS_SHORT")
                return

        # TIMEOUT od entry_time
        if MAX_POSITION_MINUTES > 0 and pos_entry_time is not None:
            if pos_entry_time.tzinfo is None:
                pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)

            age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0
            if age_minutes >= MAX_POSITION_MINUTES:
                side_timeout = "SELL" if pos_side_u == "LONG" else "BUY"
                reason_timeout = f"SUPER_TREND TIMEOUT {pos_side_u} {age_minutes:.1f}m >= {MAX_POSITION_MINUTES}m"
                inserted = insert_simulated_order(SYMBOL, INTERVAL, side_timeout, price, pos_qty_f, reason_timeout,
                                                  rsi_val_f, ema_val_f, open_time)
                if inserted:
                    close_position(exit_price=price, reason="TIMEOUT")
                return

        return  # pozycja otwarta -> nie otwieramy nowej

    # =========================
    # 2) FLAT – filtry nowych wejść
    # =========================

    # ATR minimalny
    if atr_val is None or float(atr_val) <= 0:
        logging.info("SUPER_TREND: ATR not ready (atr_14 is NULL).")
        return

    atr_pct = float(atr_val) / price * 100.0
    if atr_pct < MIN_ATR_PCT:
        logging.info("SUPER_TREND: ATR=%.4f%% < MIN_ATR_PCT=%.4f%% -> skip entry.", atr_pct, MIN_ATR_PCT)
        return

    # filtr godzinowy
    hour_utc = open_time.hour
    if hour_utc in DISABLE_HOURS_SET:
        logging.info("SUPER_TREND: hour %s UTC in DISABLE_HOURS=%s -> skip entries.", hour_utc, DISABLE_HOURS_SET)
        return

    # dzienny SL
    if DAILY_MAX_LOSS_PCT > 0:
        daily_pct = compute_daily_pnl_pct(SYMBOL, INTERVAL, price)
        if daily_pct <= -DAILY_MAX_LOSS_PCT:
            logging.info(
                "SUPER_TREND %s: daily PnL %.2f%% <= -%.2f%% -> blocking new entries.",
                SYMBOL, daily_pct, DAILY_MAX_LOSS_PCT,
            )
            return

    # sygnał flip
    decision = "HOLD"
    reason = None
    if st_dir_prev == -1 and st_dir_curr == 1:
        decision = "BUY"
        reason = f"SUPER_TREND flip DOWN -> UP (dir {st_dir_prev} -> {st_dir_curr})"
    elif st_dir_prev == 1 and st_dir_curr == -1:
        decision = "SELL"
        reason = f"SUPER_TREND flip UP -> DOWN (dir {st_dir_prev} -> {st_dir_curr})"

    if decision == "HOLD":
        return
    
    log_regime_gate_event(
        symbol=SYMBOL,
        interval=INTERVAL,
        strategy=STRATEGY_NAME,
        decision=decision,
        allow=allow_gate,
        rmeta=rmeta_gate,
        extra_meta={
            "price": float(price),
            "open_time": str(open_time),
            "st_dir_prev": int(st_dir_prev),
            "st_dir_curr": int(st_dir_curr),
            "atr_pct": float(atr_pct) if atr_pct is not None else None,
            "ema_21": float(ema_val_f) if ema_val_f is not None else None,
            "rsi_14": float(rsi_val_f) if rsi_val_f is not None else None,
            "reason": reason,
        },
    )

    # --- REGIME GATE (ENTRY ONLY) ---
    if rmeta_gate.get("mode") == "DRY_RUN" and rmeta_gate.get("would_block"):
        logging.info(
            "REGIME_GATE|dry_run would_block|strategy=%s|symbol=%s|interval=%s|decision=%s|regime=%s|why=%s",
            STRATEGY_NAME, SYMBOL, INTERVAL, decision,
            rmeta_gate.get("regime"), rmeta_gate.get("why"),
        )

    if not allow_gate:
        logging.info(
            "REGIME_GATE|blocked entry|strategy=%s|symbol=%s|interval=%s|decision=%s|regime=%s|why=%s|mode=%s",
            STRATEGY_NAME, SYMBOL, INTERVAL, decision,
            rmeta_gate.get("regime"), rmeta_gate.get("why"), rmeta_gate.get("mode"),
        )
        return

    # filtr trendu po EMA21
    if ema_val_f is not None:
        trend = get_trend(price, ema_val_f)
        if trend == "UP" and decision == "SELL":
            logging.info("SUPER_TREND: skip SELL – trend=UP (price=%.2f ema_21=%.2f).", price, ema_val_f)
            return
        if trend == "DOWN" and decision == "BUY":
            logging.info("SUPER_TREND: skip BUY – trend=DOWN (price=%.2f ema_21=%.2f).", price, ema_val_f)
            return

    # filtr RSI
    if rsi_val_f is not None:
        if rsi_val_f <= RSI_BLOCK_EXTREME_LOW or rsi_val_f >= RSI_BLOCK_EXTREME_HIGH:
            logging.info(
                "SUPER_TREND: skip %s – RSI=%.2f outside (%.1f, %.1f).",
                decision, rsi_val_f, RSI_BLOCK_EXTREME_LOW, RSI_BLOCK_EXTREME_HIGH,
            )
            return
        if decision == "BUY" and rsi_val_f > RSI_LONG_MAX:
            logging.info("SUPER_TREND: skip BUY – RSI=%.2f > RSI_LONG_MAX=%.1f.", rsi_val_f, RSI_LONG_MAX)
            return
        if decision == "SELL" and rsi_val_f < RSI_SHORT_MIN:
            logging.info("SUPER_TREND: skip SELL – RSI=%.2f < RSI_SHORT_MIN=%.1f.", rsi_val_f, RSI_SHORT_MIN)
            return

    qty_btc = ORDER_QTY_BTC
    logging.info("SUPER_TREND: opening %s at %.2f (%s)", decision, price, reason)

    opened = False
    if decision == "BUY":
        opened = open_position("LONG", qty_btc, price)
    elif decision == "SELL":
        opened = open_position("SHORT", qty_btc, price)

    if not opened:
        logging.info("SUPER_TREND: entry aborted (position could not be opened).")
        return

    inserted = insert_simulated_order(
        symbol=SYMBOL,
        interval=INTERVAL,
        side=decision,
        price=price,
        qty_btc=qty_btc,
        reason=reason,
        rsi_14=rsi_val_f,
        ema_21=ema_val_f,
        candle_open_time=open_time,
    )
    if not inserted:
        logging.info("SUPER_TREND: entry blocked by DB guard -> reverting OPEN position to keep consistency.")
        closed = close_position(exit_price=price, reason="ENTRY_DB_GUARD_REVERT")
        if not closed:
            logging.info("SUPER_TREND: revert requested but no OPEN position found (race/restart).")
        return


def main_loop():
    init_db()
    while True:
        loop_start = time.perf_counter()
        try:
            load_runtime_params()
            rows = fetch_klines()
            save_klines(rows)
            update_indicators()
            run_strategy()
        except Exception as e:
            logging.exception("Error in supertrend loop: %s", e)

        logging.info("SuperTrend loop finished in %.3f s", time.perf_counter() - loop_start)
        time.sleep(60)


if __name__ == "__main__":
    logging.info(
        "Starting SUPER_TREND trading bot worker for %s %s (strategy=%s)...",
        SYMBOL,
        INTERVAL,
        STRATEGY_NAME,
    )
    main_loop()
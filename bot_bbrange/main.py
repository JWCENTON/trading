import os
import time
import json
import psycopg2
import logging
import pandas as pd
from datetime import datetime, timezone, date
from common.db import get_latest_regime
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

# <<< NAZWA STRATEGII >>>
STRATEGY_NAME = os.environ.get("STRATEGY_NAME", "BBRANGE")

TRADING_MODE = os.environ.get("TRADING_MODE", "PAPER").upper()  # PAPER | LIVE (na razie PAPER)

SYMBOL = os.environ.get("SYMBOL", "BTCUSDT")
INTERVAL = os.environ.get("INTERVAL", "1m")

RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))
EMA_PERIOD = int(os.environ.get("EMA_PERIOD", "21"))

PAPER_START_USDT = float(os.environ.get("PAPER_START_USDT", "100"))

STOP_LOSS_PCT = float(os.environ.get("STOP_LOSS_PCT", "0.8"))       # 0.8%
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "1.0"))   # 1.0%

# Dla zgodności z innymi strategiami (mogą być nadpisywane przez strategy_params)
RSI_OVERSOLD = float(os.environ.get("RSI_OVERSOLD", "25"))
RSI_OVERBOUGHT = float(os.environ.get("RSI_OVERBOUGHT", "75"))

# 🔹 Specyficzne filtry RSI dla BBRANGE
RSI_LONG_MAX = float(os.environ.get("RSI_LONG_MAX", "45"))                  # max RSI do LONG
RSI_SHORT_MIN = float(os.environ.get("RSI_SHORT_MIN", "55"))                # min RSI do SHORT
RSI_BLOCK_EXTREME_LOW = float(os.environ.get("RSI_BLOCK_EXTREME_LOW", "10"))   # blokada przy ultra low
RSI_BLOCK_EXTREME_HIGH = float(os.environ.get("RSI_BLOCK_EXTREME_HIGH", "90")) # blokada przy ultra high

DISABLE_HOURS = os.environ.get("DISABLE_HOURS", "")
DISABLE_HOURS_SET = {int(h.strip()) for h in DISABLE_HOURS.split(",") if h.strip() != ""}

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")
client = Client(api_key=API_KEY, api_secret=API_SECRET)

# <<< Trend buffer – jak daleko od EMA21 uznajemy trend >>>
TREND_BUFFER = float(os.environ.get("TREND_BUFFER", "0.001"))  # 0.1%

# Max czas pozycji
MAX_POSITION_MINUTES = int(os.environ.get("MAX_POSITION_MINUTES", "90"))

# Dzienny SL (jeśli <= 0 -> wyłączony)
DAILY_MAX_LOSS_PCT = float(os.environ.get("DAILY_MAX_LOSS_PCT", "0.5"))

# Ilość na trade (PAPER)
ORDER_QTY_BTC = float(os.environ.get("ORDER_QTY_BTC", "0.0001"))

# <<< Parametry Bollingera >>>
BB_PERIOD = int(os.environ.get("BB_PERIOD", "20"))
BB_STD = float(os.environ.get("BB_STD", "2.0"))
# Minimalna szerokość pasma (jako ułamek, np 0.0015 = 0.15%)
MIN_BB_WIDTH_PCT = float(os.environ.get("MIN_BB_WIDTH_PCT", "0.0015"))

# ====================
# Regime
# ====================

REGIME_ENABLED = os.environ.get("REGIME_ENABLED", "0") == "1"
REGIME_MODE = os.environ.get("REGIME_MODE", "DRY_RUN").strip().upper()  # DRY_RUN | ENFORCE
REGIME_MAX_AGE_SECONDS = int(os.environ.get("REGIME_MAX_AGE_SECONDS", "180"))


def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


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
            json.dumps(meta, default=_json_default),
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
        logging.info("BBRANGE: open_position skipped - position already OPEN.")
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
    logging.info("BBRANGE: position OPENED side=%s qty=%.8f entry=%.2f", side, float(qty), float(entry_price))
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
        logging.info("BBRANGE: position CLOSED reason=%s exit=%.2f", reason, float(exit_price))
    else:
        logging.info("BBRANGE: close_position skipped - no OPEN position found.")
    return closed


def init_db():
    conn = get_db_conn()
    cur = conn.cursor()

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
            UNIQUE(symbol, interval, open_time)
        );
        """
    )    

    # 🔹 Strategy params
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

    # seed parametrów z ENV (jak TREND) – nie nadpisuje DB
    seed_default_params_from_env(conn)

    cur.close()
    conn.close()
    logging.info("DB initialized for BBRANGE.")


def seed_default_params_from_env(conn):
    global STOP_LOSS_PCT, TAKE_PROFIT_PCT, MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT, TREND_BUFFER
    global RSI_LONG_MAX, RSI_SHORT_MIN, RSI_BLOCK_EXTREME_LOW, RSI_BLOCK_EXTREME_HIGH
    global BB_PERIOD, BB_STD, MIN_BB_WIDTH_PCT, ORDER_QTY_BTC

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
        "STOP_LOSS_PCT": float(STOP_LOSS_PCT),
        "TAKE_PROFIT_PCT": float(TAKE_PROFIT_PCT),
        "MAX_POSITION_MINUTES": float(MAX_POSITION_MINUTES),
        "DAILY_MAX_LOSS_PCT": float(DAILY_MAX_LOSS_PCT),
        "TREND_BUFFER": float(TREND_BUFFER),
        "ORDER_QTY_BTC": float(ORDER_QTY_BTC),
        "BB_PERIOD": float(BB_PERIOD),
        "BB_STD": float(BB_STD),
        "MIN_BB_WIDTH_PCT": float(MIN_BB_WIDTH_PCT),
        "RSI_LONG_MAX": float(RSI_LONG_MAX),
        "RSI_SHORT_MIN": float(RSI_SHORT_MIN),
        "RSI_BLOCK_EXTREME_LOW": float(RSI_BLOCK_EXTREME_LOW),
        "RSI_BLOCK_EXTREME_HIGH": float(RSI_BLOCK_EXTREME_HIGH),
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
        logging.info("Seeded default BBRANGE params from ENV for %s/%s.", SYMBOL, STRATEGY_NAME)
    cur.close()


def load_runtime_params():
    global STOP_LOSS_PCT, TAKE_PROFIT_PCT, MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT, TREND_BUFFER
    global RSI_LONG_MAX, RSI_SHORT_MIN, RSI_BLOCK_EXTREME_LOW, RSI_BLOCK_EXTREME_HIGH
    global BB_PERIOD, BB_STD, MIN_BB_WIDTH_PCT, ORDER_QTY_BTC

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

    if "STOP_LOSS_PCT" in params:
        STOP_LOSS_PCT = clamp(params["STOP_LOSS_PCT"], 0.1, 5.0)
    if "TAKE_PROFIT_PCT" in params:
        TAKE_PROFIT_PCT = clamp(params["TAKE_PROFIT_PCT"], 0.1, 10.0)
    if "MAX_POSITION_MINUTES" in params:
        MAX_POSITION_MINUTES = int(clamp(params["MAX_POSITION_MINUTES"], 5, 24 * 60))
    if "DAILY_MAX_LOSS_PCT" in params:
        # tutaj dopuszczamy 0.0 jako "wyłączone"
        DAILY_MAX_LOSS_PCT = clamp(params["DAILY_MAX_LOSS_PCT"], 0.0, 10.0)
    if "TREND_BUFFER" in params:
        TREND_BUFFER = clamp(params["TREND_BUFFER"], 0.0001, 0.02)

    if "ORDER_QTY_BTC" in params:
        ORDER_QTY_BTC = clamp(params["ORDER_QTY_BTC"], 0.00001, 1.0)

    if "BB_PERIOD" in params:
        BB_PERIOD = int(clamp(params["BB_PERIOD"], 10, 200))
    if "BB_STD" in params:
        BB_STD = clamp(params["BB_STD"], 1.0, 4.0)
    if "MIN_BB_WIDTH_PCT" in params:
        MIN_BB_WIDTH_PCT = clamp(params["MIN_BB_WIDTH_PCT"], 0.0005, 0.01)

    if "RSI_LONG_MAX" in params:
        RSI_LONG_MAX = clamp(params["RSI_LONG_MAX"], 5.0, 60.0)
    if "RSI_SHORT_MIN" in params:
        RSI_SHORT_MIN = clamp(params["RSI_SHORT_MIN"], 40.0, 95.0)
    if "RSI_BLOCK_EXTREME_LOW" in params:
        RSI_BLOCK_EXTREME_LOW = clamp(params["RSI_BLOCK_EXTREME_LOW"], 0.0, 30.0)
    if "RSI_BLOCK_EXTREME_HIGH" in params:
        RSI_BLOCK_EXTREME_HIGH = clamp(params["RSI_BLOCK_EXTREME_HIGH"], 70.0, 100.0)

    logging.info(
        "RUNTIME_PARAMS|symbol=%s|strategy=%s|STOP_LOSS_PCT=%.4f|TAKE_PROFIT_PCT=%.4f|"
        "MAX_POSITION_MINUTES=%d|DAILY_MAX_LOSS_PCT=%.4f|TREND_BUFFER=%.5f|"
        "ORDER_QTY_BTC=%.8f|BB_PERIOD=%d|BB_STD=%.3f|MIN_BB_WIDTH_PCT=%.5f|"
        "RSI_LONG_MAX=%.2f|RSI_SHORT_MIN=%.2f|RSI_BLOCK_EXTREME_LOW=%.2f|RSI_BLOCK_EXTREME_HIGH=%.2f",
        SYMBOL, STRATEGY_NAME,
        STOP_LOSS_PCT, TAKE_PROFIT_PCT,
        MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT, TREND_BUFFER,
        ORDER_QTY_BTC, BB_PERIOD, BB_STD, MIN_BB_WIDTH_PCT,
        RSI_LONG_MAX, RSI_SHORT_MIN, RSI_BLOCK_EXTREME_LOW, RSI_BLOCK_EXTREME_HIGH,
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
        logging.info("BBRANGE: Simulated %s at %.2f qty=%.8f (reason=%s)", side, float(price), float(qty_btc), reason)
    else:
        logging.info(
            "BBRANGE: Simulated order skipped by DB guard (symbol=%s interval=%s strategy=%s candle_open_time=%s).",
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
        logging.info("BBRANGE: panic close blocked by DB guard (already traded this candle).")
        return False

    return close_position(exit_price=current_price, reason="PANIC")


def get_trend(close: float, ema21: float, buffer_pct: float = TREND_BUFFER) -> str:
    if close > ema21 * (1.0 + buffer_pct):
        return "UP"
    elif close < ema21 * (1.0 - buffer_pct):
        return "DOWN"
    else:
        return "FLAT"


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


def get_latest_candles(limit=200):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, interval, open_time, close, ema_21, rsi_14
        FROM candles
        WHERE symbol=%s AND interval=%s
        ORDER BY open_time DESC
        LIMIT %s
        """,
        (SYMBOL, INTERVAL, limit),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def run_strategy():
    rows = get_latest_candles(limit=max(BB_PERIOD + 30, 120))
    if not rows or len(rows) < BB_PERIOD + 5:
        logging.info("BBRANGE: not enough candles yet (have %d).", len(rows) if rows else 0)
        return

    rows_asc = list(reversed(rows))

    df = pd.DataFrame(
        [
            {
                "open_time": r[2],
                "close": float(r[3]),
                "ema_21": float(r[4]) if r[4] is not None else None,
                "rsi_14": float(r[5]) if r[5] is not None else None,
            }
            for r in rows_asc
        ]
    )

    if df["ema_21"].isna().iloc[-1]:
        logging.info("BBRANGE: EMA not ready yet, skipping.")
        return

    closes = df["close"]
    mid = closes.rolling(window=BB_PERIOD).mean()
    std = closes.rolling(window=BB_PERIOD).std()

    df["bb_mid"] = mid
    df["bb_upper"] = mid + BB_STD * std
    df["bb_lower"] = mid - BB_STD * std

    last = df.iloc[-1]
    open_time = last["open_time"]
    price = float(last["close"])
    ema_val = float(last["ema_21"])
    rsi_val = last["rsi_14"]

    bb_mid = last["bb_mid"]
    bb_upper = last["bb_upper"]
    bb_lower = last["bb_lower"]

    # --- Guard: bands must be ready before computing width ---
    if pd.isna(bb_mid) or pd.isna(bb_upper) or pd.isna(bb_lower) or float(bb_mid) == 0.0:
        logging.info("BBRANGE: Bollinger bands not ready yet (pre-heartbeat).")
        return
    
    bb_width_pct = None
    if bb_lower is not None and bb_mid is not None and bb_upper is not None:
        if not pd.isna(bb_lower) and not pd.isna(bb_mid) and not pd.isna(bb_upper):
            mid = float(bb_mid)
            if mid != 0:
                bb_width_pct = (float(bb_upper) - float(bb_lower)) / float(bb_mid)  # fraction, e.g. 0.003

    trend = get_trend(price, ema_val)

    allow_gate, rmeta_gate = regime_allows(STRATEGY_NAME, SYMBOL, INTERVAL)
    heartbeat({
        "price": float(price),
        "open_time": str(open_time),
        "ema_21": float(ema_val),
        "rsi_14": float(rsi_val) if (rsi_val is not None and not pd.isna(rsi_val)) else None,
        "bb_lower": float(bb_lower),
        "bb_mid": float(bb_mid),
        "bb_upper": float(bb_upper),
        "bb_width_pct": float(bb_width_pct) if bb_width_pct is not None else None,
        "trend": trend,
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

    if pd.isna(bb_mid) or pd.isna(bb_upper) or pd.isna(bb_lower):
        logging.info("BBRANGE: Bollinger bands not ready yet.")
        return

    if rsi_val is None or pd.isna(rsi_val):
        logging.info("BBRANGE: RSI not ready yet, skipping.")
        return
    rsi_val = float(rsi_val)

    if bb_width_pct < MIN_BB_WIDTH_PCT:
        logging.info(
            "BBRANGE: band too tight (width=%.4f%% < %.4f%%), skipping.",
            bb_width_pct * 100.0,
            MIN_BB_WIDTH_PCT * 100.0,
        )
        return

    trend = get_trend(price, ema_val)
    logging.info(
        "BBRANGE: price=%.2f ema=%.2f trend=%s bb_lower=%.2f bb_mid=%.2f bb_upper=%.2f width=%.3f%% rsi=%.2f",
        price, ema_val, trend, bb_lower, bb_mid, bb_upper, bb_width_pct * 100.0, rsi_val,
    )

    # =========================
    # 1) HARD-TRUTH: pozycja z positions (jak TREND/RSI)
    # =========================
    pos = get_open_position()
    has_position = pos is not None

    if has_position:
        pos_id, pos_side, pos_qty, pos_entry_price, pos_entry_time = pos
        pos_side_u = str(pos_side).upper()
        pos_qty_f = float(pos_qty)
        pos_entry_price_f = float(pos_entry_price)

        # --- SL/TP
        if pos_side_u == "LONG":
            change_pct = (price - pos_entry_price_f) / pos_entry_price_f * 100.0

            if TAKE_PROFIT_PCT > 0 and change_pct >= TAKE_PROFIT_PCT:
                reason = f"BBRANGE TAKE PROFIT LONG {change_pct:.2f}% >= {TAKE_PROFIT_PCT:.2f}%"
                inserted = insert_simulated_order(
                    SYMBOL, INTERVAL, "SELL", price, pos_qty_f, reason,
                    rsi_val, ema_val, open_time
                )
                if inserted:
                    close_position(exit_price=price, reason="TAKE_PROFIT")
                return

            drop_pct = -change_pct
            if drop_pct >= STOP_LOSS_PCT:
                reason = f"BBRANGE STOP LOSS LONG {drop_pct:.2f}% >= {STOP_LOSS_PCT:.2f}%"
                inserted = insert_simulated_order(
                    SYMBOL, INTERVAL, "SELL", price, pos_qty_f, reason,
                    rsi_val, ema_val, open_time
                )
                if inserted:
                    close_position(exit_price=price, reason="STOP_LOSS")
                return

        elif pos_side_u == "SHORT":
            change_pct = (pos_entry_price_f - price) / pos_entry_price_f * 100.0

            if TAKE_PROFIT_PCT > 0 and change_pct >= TAKE_PROFIT_PCT:
                reason = f"BBRANGE TAKE PROFIT SHORT {change_pct:.2f}% >= {TAKE_PROFIT_PCT:.2f}%"
                inserted = insert_simulated_order(
                    SYMBOL, INTERVAL, "BUY", price, pos_qty_f, reason,
                    rsi_val, ema_val, open_time
                )
                if inserted:
                    close_position(exit_price=price, reason="TAKE_PROFIT_SHORT")
                return

            rise_pct = -change_pct
            if rise_pct >= STOP_LOSS_PCT:
                reason = f"BBRANGE STOP LOSS SHORT {rise_pct:.2f}% >= {STOP_LOSS_PCT:.2f}%"
                inserted = insert_simulated_order(
                    SYMBOL, INTERVAL, "BUY", price, pos_qty_f, reason,
                    rsi_val, ema_val, open_time
                )
                if inserted:
                    close_position(exit_price=price, reason="STOP_LOSS_SHORT")
                return

        # --- TIMEOUT: liczony od entry_time
        if MAX_POSITION_MINUTES > 0 and pos_entry_time is not None:
            if pos_entry_time.tzinfo is None:
                pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)

            age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0
            if age_minutes >= MAX_POSITION_MINUTES:
                side_timeout = "SELL" if pos_side_u == "LONG" else "BUY"
                reason_timeout = f"BBRANGE TIMEOUT {pos_side_u} {age_minutes:.1f}m >= {MAX_POSITION_MINUTES}m"
                inserted = insert_simulated_order(
                    SYMBOL, INTERVAL, side_timeout, price, pos_qty_f, reason_timeout,
                    rsi_val, ema_val, open_time
                )
                if inserted:
                    close_position(exit_price=price, reason="TIMEOUT")
                return

        # jak jest pozycja i nic nie zaszło -> nie otwieramy nowej
        return

    # =========================
    # 2) Jesteśmy FLAT – filtry wejścia
    # =========================

    # filtr godzinowy
    hour_utc = open_time.hour
    if hour_utc in DISABLE_HOURS_SET:
        logging.info("BBRANGE: hour %s UTC in DISABLE_HOURS=%s -> skip entries.", hour_utc, DISABLE_HOURS_SET)
        return

    # dzienny SL
    if DAILY_MAX_LOSS_PCT > 0:
        daily_pct = compute_daily_pnl_pct(SYMBOL, INTERVAL, price)
        if daily_pct <= -DAILY_MAX_LOSS_PCT:
            logging.info(
                "BBRANGE %s: daily PnL %.2f%% <= -%.2f%% -> blocking new entries.",
                SYMBOL, daily_pct, DAILY_MAX_LOSS_PCT,
            )
            return

    # BBRANGE: tylko w FLAT
    if trend != "FLAT":
        logging.info("BBRANGE: trend=%s (not FLAT) -> no entries.", trend)
        return

    # sygnał pasma
    decision = "HOLD"
    reason = None
    if price < bb_lower:
        decision = "BUY"
        reason = f"BBRANGE LONG: price {price:.2f} < lower {bb_lower:.2f} (trend=FLAT)"
    elif price > bb_upper:
        decision = "SELL"
        reason = f"BBRANGE SHORT: price {price:.2f} > upper {bb_upper:.2f} (trend=FLAT)"

    if decision == "HOLD":
        return

    # RSI filtry
    if rsi_val <= RSI_BLOCK_EXTREME_LOW or rsi_val >= RSI_BLOCK_EXTREME_HIGH:
        logging.info(
            "BBRANGE: skip %s – RSI=%.2f outside (%.1f, %.1f).",
            decision, rsi_val, RSI_BLOCK_EXTREME_LOW, RSI_BLOCK_EXTREME_HIGH,
        )
        return

    if decision == "BUY" and rsi_val > RSI_LONG_MAX:
        logging.info("BBRANGE: skip BUY – RSI=%.2f > RSI_LONG_MAX=%.1f.", rsi_val, RSI_LONG_MAX)
        return

    if decision == "SELL" and rsi_val < RSI_SHORT_MIN:
        logging.info("BBRANGE: skip SELL – RSI=%.2f < RSI_SHORT_MIN=%.1f.", rsi_val, RSI_SHORT_MIN)
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
            "ema_21": float(ema_val),
            "rsi_14": float(rsi_val),
            "open_time": str(open_time),
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

    qty_btc = ORDER_QTY_BTC
    logging.info("BBRANGE: opening %s at %.2f (%s)", decision, price, reason)

    opened = False
    if decision == "BUY":
        opened = open_position("LONG", qty_btc, price)
    elif decision == "SELL":
        opened = open_position("SHORT", qty_btc, price)

    if not opened:
        logging.info("BBRANGE: entry aborted (could not open position).")
        return

    inserted = insert_simulated_order(
        symbol=SYMBOL,
        interval=INTERVAL,
        side=decision,
        price=price,
        qty_btc=qty_btc,
        reason=reason,
        rsi_14=rsi_val,
        ema_21=ema_val,
        candle_open_time=open_time,
    )
    if not inserted:
        logging.info("BBRANGE: entry blocked by DB guard -> reverting OPEN position to keep consistency.")
        closed = close_position(exit_price=price, reason="ENTRY_DB_GUARD_REVERT")
        if not closed:
            logging.info("BBRANGE: revert requested but no OPEN position found (race/restart).")
        return


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
            (SYMBOL, INTERVAL, open_time, open_price, high, low, close, volume, close_time, trades)
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    conn = get_db_conn()
    df = pd.read_sql_query(
        """
        SELECT id, open_time, close
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

    df["ema_21"] = df["close"].astype(float).ewm(span=EMA_PERIOD, adjust=False).mean()

    delta = df["close"].astype(float).diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    roll_up = gain.rolling(window=RSI_PERIOD).mean()
    roll_down = loss.rolling(window=RSI_PERIOD).mean()

    rs = roll_up / roll_down
    df["rsi_14"] = 100.0 - (100.0 / (1.0 + rs))

    last = df.tail(50)

    cur = conn.cursor()
    sql = """
        UPDATE candles
        SET ema_21 = %s,
            rsi_14 = %s
        WHERE id = %s;
    """

    data = [(row["ema_21"], row["rsi_14"], int(row["id"])) for _, row in last.iterrows()]

    start = time.perf_counter()
    cur.executemany(sql, data)
    conn.commit()
    elapsed = time.perf_counter() - start

    cur.close()
    conn.close()

    logging.info("Updated indicators for %d candles in %.3f s", len(data), elapsed)


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
            logging.exception("Error in main loop: %s", e)

        logging.info("Loop finished in %.3f s", time.perf_counter() - loop_start)
        time.sleep(60)


if __name__ == "__main__":
    logging.info(
        "Starting trading bot worker for %s %s (strategy=%s)...",
        SYMBOL,
        INTERVAL,
        STRATEGY_NAME,
    )
    main_loop()

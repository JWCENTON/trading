import os
import time
import json
import logging
import psycopg2
import pandas as pd
from datetime import datetime, timezone, date
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
TRADING_MODE = os.environ.get("TRADING_MODE", "PAPER").upper()  # PAPER | LIVE (na razie PAPER)

SYMBOL = os.environ.get("SYMBOL", "BTCUSDT")
INTERVAL = os.environ.get("INTERVAL", "1m")

RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))
EMA_PERIOD = int(os.environ.get("EMA_PERIOD", "21"))

RSI_OVERSOLD = float(os.environ.get("RSI_OVERSOLD", "25"))
RSI_OVERBOUGHT = float(os.environ.get("RSI_OVERBOUGHT", "75"))

PAPER_START_USDT = float(os.environ.get("PAPER_START_USDT", "100"))

STOP_LOSS_PCT = float(os.environ.get("STOP_LOSS_PCT", "0.8"))        # % (np 0.8 = 0.8%)
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "1.2"))     # % (np 1.2 = 1.2%)

DISABLE_HOURS = os.environ.get("DISABLE_HOURS", "")
DISABLE_HOURS_SET = {int(h.strip()) for h in DISABLE_HOURS.split(",") if h.strip() != ""}

TREND_BUFFER = float(os.environ.get("TREND_BUFFER", "0.001"))         # ułamek (np 0.001 = 0.1%)
MAX_POSITION_MINUTES = int(os.environ.get("MAX_POSITION_MINUTES", "90"))
DAILY_MAX_LOSS_PCT = float(os.environ.get("DAILY_MAX_LOSS_PCT", "0.5"))  # % (0 wyłącza)

ORDER_QTY_BTC = float(os.environ.get("ORDER_QTY_BTC", "0.0001"))
MAX_DIST_FROM_EMA_PCT = float(os.environ.get("MAX_DIST_FROM_EMA_PCT", "0.5"))  # % (np 0.5 = 0.5%)

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

client = Client(api_key=API_KEY, api_secret=API_SECRET)

# ========================
# Regime gating
# ========================

from common.db import get_latest_regime

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
        return True, {"enabled": True, "reason": "no_regime"}  # fail-open

    ts = r.get("ts")
    if ts is None:
        return True, {"enabled": True, "reason": "regime_ts_null"}

    now = datetime.now(timezone.utc)
    age = (now - ts).total_seconds()
    if age > REGIME_MAX_AGE_SECONDS:
        return True, {"enabled": True, "reason": f"regime_stale age={age:.0f}s", "age_s": age, "regime": r.get("regime")}

    regime = r.get("regime")

    # Polityka blokowania (v1):
    # - RSI/BBRANGE: blokuj w TREND_UP/TREND_DOWN i SHOCK
    would_block = False
    why = "ok"
    if strategy_name in ("RSI", "BBRANGE"):
        if regime in ("TREND_UP", "TREND_DOWN", "SHOCK"):
            would_block = True
            why = f"{strategy_name} blocked in {regime}"

    if REGIME_MODE == "DRY_RUN":
        return True, {"enabled": True, "mode": "DRY_RUN", "would_block": would_block, "why": why, **r, "age_s": age}

    allow = not would_block
    return allow, {"enabled": True, "mode": "ENFORCE", "would_block": would_block, "why": why, **r, "age_s": age}

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
        (SYMBOL, STRATEGY_NAME, INTERVAL, side, float(qty), float(entry_price)),
    )
    conn.commit()
    cur.close()
    conn.close()

    logging.info("RSI: position OPENED %s qty=%.8f entry=%.2f", side, float(qty), float(entry_price))
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
        logging.info("RSI: position CLOSED exit=%.2f reason=%s", float(exit_price), reason)
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
            symbol, interval, STRATEGY_NAME, side, float(price), float(qty_btc),
            reason,
            float(rsi_14) if rsi_14 is not None else None,
            float(ema_21) if ema_21 is not None else None,
            candle_open_time,
        ),
    )
    inserted = cur.fetchone() is not None
    conn.commit()
    cur.close()
    conn.close()

    if inserted:
        logging.info("RSI simulated %s @ %.2f qty=%.8f (%s)", side, float(price), float(qty_btc), reason)
    else:
        logging.info("RSI simulated order skipped by DB guard.")
    return inserted


# =========================
# PNL (daily)
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

        if str(side).upper() == "BUY":
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
# CANDLES + INDICATORS
# =========================


def fetch_klines(limit=50):
    start = time.perf_counter()
    klines = client.get_klines(symbol=SYMBOL, interval=INTERVAL, limit=limit)
    logging.info("Fetched %d klines in %.3f s", len(klines), time.perf_counter() - start)

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
    return rows


def save_klines(rows):
    if not rows:
        return
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
        page_size=50,
    )
    conn.commit()
    cur.close()
    conn.close()


def update_indicators():
    conn = get_db_conn()
    df = pd.read_sql_query(
        """
        SELECT id, open_time, close
        FROM candles
        WHERE symbol=%s AND interval=%s
        ORDER BY open_time
        """,
        conn,
        params=(SYMBOL, INTERVAL),
    )

    if df.empty:
        conn.close()
        logging.info("RSI: no candles yet for indicators.")
        return

    close_f = df["close"].astype(float)

    df["ema_21"] = close_f.ewm(span=EMA_PERIOD, adjust=False).mean()

    delta = close_f.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    roll_up = gain.rolling(window=RSI_PERIOD).mean()
    roll_down = loss.rolling(window=RSI_PERIOD).mean()

    rs = roll_up / roll_down
    df["rsi_14"] = 100.0 - (100.0 / (1.0 + rs))

    last = df.tail(60)  # update tylko końcówkę

    cur = conn.cursor()
    data = [(row["ema_21"], row["rsi_14"], int(row["id"])) for _, row in last.iterrows()]
    cur.executemany(
        """
        UPDATE candles
        SET ema_21=%s, rsi_14=%s
        WHERE id=%s;
        """,
        data,
    )
    conn.commit()
    cur.close()
    conn.close()
    logging.info("RSI: updated indicators for %d candles.", len(data))


def get_latest_candle():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT open_time, close, ema_21, rsi_14
        FROM candles
        WHERE symbol=%s AND interval=%s
        ORDER BY open_time DESC
        LIMIT 1
        """,
        (SYMBOL, INTERVAL),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row  # (open_time, close, ema_21, rsi_14) or None


# =========================
# STRATEGY LOGIC
# =========================


def run_strategy():
    row = get_latest_candle()
    if not row:
        return

    open_time, close_px, ema_21, rsi_14 = row
    if open_time is None:
        return

    price = float(close_px) if close_px is not None else None
    if price is None:
        return

    if ema_21 is None or rsi_14 is None:
        logging.info("RSI: indicators not ready yet.")
        heartbeat({"price": price, "open_time": str(open_time), "status": "indicators_not_ready"})
        return

    ema_val = float(ema_21)
    rsi_val = float(rsi_14)

    # heartbeat (w tym meta o regime, jeśli jest)
    allow_gate, rmeta_gate = regime_allows(STRATEGY_NAME, SYMBOL, INTERVAL)
    heartbeat({
        "price": price,
        "open_time": str(open_time),
        "ema_21": ema_val,
        "rsi_14": rsi_val,
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

    # --- jeżeli PANIC -> zamknij pozycję i przejdź w HALT
    if mode == "PANIC":
        pos = get_open_position()
        if pos:
            _, side, qty, _entry_price, _entry_time = pos
            side_u = str(side).upper()
            qty_f = float(qty)

            exit_side = "SELL" if side_u == "LONG" else "BUY"
            inserted = insert_simulated_order(
                SYMBOL, INTERVAL, exit_side, price, qty_f,
                "PANIC CLOSE",
                rsi_val, ema_val, open_time
            )
            if inserted:
                close_position(exit_price=price, reason="PANIC")
        set_mode("HALT", reason="Panic executed; halting.")
        return

    # =========================
    # 1) EXIT (zawsze dozwolony)
    # =========================
    pos = get_open_position()
    if pos:
        _pos_id, pos_side, pos_qty, pos_entry_price, pos_entry_time = pos
        pos_side_u = str(pos_side).upper()
        qty_f = float(pos_qty)
        entry_f = float(pos_entry_price)

        # SL/TP
        if pos_side_u == "LONG":
            change_pct = (price - entry_f) / entry_f * 100.0
            if TAKE_PROFIT_PCT > 0 and change_pct >= TAKE_PROFIT_PCT:
                reason = f"RSI TAKE PROFIT LONG {change_pct:.2f}% >= {TAKE_PROFIT_PCT:.2f}%"
                inserted = insert_simulated_order(SYMBOL, INTERVAL, "SELL", price, qty_f, reason, rsi_val, ema_val, open_time)
                if inserted:
                    close_position(exit_price=price, reason="TAKE_PROFIT")
                return

            drop_pct = -change_pct
            if STOP_LOSS_PCT > 0 and drop_pct >= STOP_LOSS_PCT:
                reason = f"RSI STOP LOSS LONG {drop_pct:.2f}% >= {STOP_LOSS_PCT:.2f}%"
                inserted = insert_simulated_order(SYMBOL, INTERVAL, "SELL", price, qty_f, reason, rsi_val, ema_val, open_time)
                if inserted:
                    close_position(exit_price=price, reason="STOP_LOSS")
                return

        else:  # SHORT
            change_pct = (entry_f - price) / entry_f * 100.0
            if TAKE_PROFIT_PCT > 0 and change_pct >= TAKE_PROFIT_PCT:
                reason = f"RSI TAKE PROFIT SHORT {change_pct:.2f}% >= {TAKE_PROFIT_PCT:.2f}%"
                inserted = insert_simulated_order(SYMBOL, INTERVAL, "BUY", price, qty_f, reason, rsi_val, ema_val, open_time)
                if inserted:
                    close_position(exit_price=price, reason="TAKE_PROFIT_SHORT")
                return

            rise_pct = -change_pct
            if STOP_LOSS_PCT > 0 and rise_pct >= STOP_LOSS_PCT:
                reason = f"RSI STOP LOSS SHORT {rise_pct:.2f}% >= {STOP_LOSS_PCT:.2f}%"
                inserted = insert_simulated_order(SYMBOL, INTERVAL, "BUY", price, qty_f, reason, rsi_val, ema_val, open_time)
                if inserted:
                    close_position(exit_price=price, reason="STOP_LOSS_SHORT")
                return

        # TIMEOUT
        if MAX_POSITION_MINUTES > 0 and pos_entry_time is not None:
            if pos_entry_time.tzinfo is None:
                pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)
            age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0
            if age_minutes >= MAX_POSITION_MINUTES:
                side_timeout = "SELL" if pos_side_u == "LONG" else "BUY"
                reason_timeout = f"RSI TIMEOUT {pos_side_u} {age_minutes:.1f}m >= {MAX_POSITION_MINUTES}m"
                inserted = insert_simulated_order(SYMBOL, INTERVAL, side_timeout, price, qty_f, reason_timeout, rsi_val, ema_val, open_time)
                if inserted:
                    close_position(exit_price=price, reason="TIMEOUT")
                return

        # jeśli jest pozycja i nic nie zamyka -> nie wchodzimy w nową
        return

    # =========================
    # 2) ENTRY (tu stosujemy filtry + REGIME)
    # =========================

    # filtr godzinowy
    hour_utc = open_time.hour
    if hour_utc in DISABLE_HOURS_SET:
        logging.info("RSI: hour %s UTC in DISABLE_HOURS=%s -> skip entries.", hour_utc, DISABLE_HOURS_SET)
        return

    # daily max loss
    if DAILY_MAX_LOSS_PCT > 0:
        daily_pct = compute_daily_pnl_pct(SYMBOL, INTERVAL, price)
        if daily_pct <= -DAILY_MAX_LOSS_PCT:
            logging.info("RSI %s: daily PnL %.2f%% <= -%.2f%% -> blocking new entries.",
                         SYMBOL, daily_pct, DAILY_MAX_LOSS_PCT)
            return

    # filtr dystansu od EMA (jako %)
    dist_from_ema_pct = abs(price - ema_val) / ema_val * 100.0
    if MAX_DIST_FROM_EMA_PCT > 0 and dist_from_ema_pct > MAX_DIST_FROM_EMA_PCT:
        logging.info(
            "RSI: skip entries – price too far from EMA (dist=%.2f%% > max=%.2f%%).",
            dist_from_ema_pct, MAX_DIST_FROM_EMA_PCT
        )
        return

    # decyzja RSI
    decision = "HOLD"
    reason = None

    if rsi_val <= RSI_OVERSOLD:
        decision = "BUY"
        reason = f"RSI {rsi_val:.2f} <= oversold {RSI_OVERSOLD:.2f}"
    elif rsi_val >= RSI_OVERBOUGHT:
        decision = "SELL"
        reason = f"RSI {rsi_val:.2f} >= overbought {RSI_OVERBOUGHT:.2f}"

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

    # open position + order
    qty_btc = ORDER_QTY_BTC
    if decision == "BUY":
        opened = open_position("LONG", qty_btc, price)
    else:
        opened = open_position("SHORT", qty_btc, price)

    if not opened:
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
        logging.info("RSI: entry blocked by DB guard -> reverting OPEN position to keep consistency.")
        close_position(exit_price=price, reason="ENTRY_DB_GUARD_REVERT")


# =========================
# MAIN LOOP
# =========================


def main_loop():
    init_db()
    while True:
        loop_start = time.perf_counter()
        try:
            rows = fetch_klines(limit=50)
            save_klines(rows)
            update_indicators()
            run_strategy()
        except Exception:
            logging.exception("RSI loop error")

        logging.info("RSI loop finished in %.3f s", time.perf_counter() - loop_start)
        time.sleep(60)


if __name__ == "__main__":
    logging.info("Starting RSI bot %s %s", SYMBOL, INTERVAL)
    main_loop()
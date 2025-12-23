import os
import time
import json
import logging
import psycopg2
import pandas as pd
from binance.client import Client
from common.db import get_latest_regime
from datetime import datetime, timezone, date
from psycopg2.extras import execute_batch
from common.execution import place_live_order


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

STRATEGY_NAME = os.environ.get("STRATEGY_NAME", "TREND")
TRADING_MODE = os.environ.get("TRADING_MODE", "PAPER").upper()  # PAPER | LIVE
LIVE_ORDERS_ENABLED = os.environ.get("LIVE_ORDERS_ENABLED", "0") == "1"

EMA_FAST = int(os.environ.get("EMA_FAST", "21"))
EMA_SLOW = int(os.environ.get("EMA_SLOW", "55"))

RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))
EMA_PERIOD = int(os.environ.get("EMA_PERIOD", "21"))

TREND_FILTER_PCT = float(os.environ.get("TREND_FILTER_PCT", "0.001"))  # 0.1%
ENTRY_BUFFER_PCT = float(os.environ.get("ENTRY_BUFFER_PCT", "0.0015"))  # 0.15%

RSI_OVERSOLD = float(os.environ.get("RSI_OVERSOLD", "25"))
RSI_OVERBOUGHT = float(os.environ.get("RSI_OVERBOUGHT", "75"))

PAPER_START_USDT = float(os.environ.get("PAPER_START_USDT", "100"))

STOP_LOSS_PCT = float(os.environ.get("STOP_LOSS_PCT", "0.8"))      # %
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "1.2"))  # %

DISABLE_HOURS = os.environ.get("DISABLE_HOURS", "")
DISABLE_HOURS_SET = {
    int(h.strip())
    for h in DISABLE_HOURS.split(",")
    if h.strip() != ""
}

QUOTE_ASSET = os.environ.get("QUOTE_ASSET", "USDC").upper()
if not SYMBOL.endswith(QUOTE_ASSET):
    raise RuntimeError(f"SYMBOL={SYMBOL} does not match QUOTE_ASSET={QUOTE_ASSET}")

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

client = Client(api_key=API_KEY, api_secret=API_SECRET)

TREND_BUFFER = float(os.environ.get("TREND_BUFFER", "0.001"))  # 0.1%
MAX_POSITION_MINUTES = int(os.environ.get("MAX_POSITION_MINUTES", "90"))

# jeśli DAILY_MAX_LOSS_PCT <= 0 -> dzienny SL wyłączony
DAILY_MAX_LOSS_PCT = float(os.environ.get("DAILY_MAX_LOSS_PCT", "0.5"))

ORDER_QTY_BTC = float(os.environ.get("ORDER_QTY_BTC", "0.0001"))
MAX_DIST_FROM_EMA_FAST_PCT = float(os.environ.get("MAX_DIST_FROM_EMA_FAST_PCT", "0.6"))

def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)

# =================
# Regime
# =================

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
        INSERT INTO public.bot_heartbeat(symbol, strategy, interval, last_seen, info)
        VALUES (%s, %s, %s, now(), %s::jsonb)
        ON CONFLICT ON CONSTRAINT bot_heartbeat_symbol_strategy_interval_key
        DO UPDATE SET last_seen=now(), info=EXCLUDED.info;
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL, json.dumps(info)),
    )
    conn.commit()
    cur.close()
    conn.close()


def log_regime_gate_on_exit(decision: str, rmeta_gate: dict, allow_gate: bool, extra: dict):
    # Zapisujmy EXIT zawsze, ale tylko jeśli reżim jest włączony i ma sensowne meta
    # (i tak meta jest małe, a to Ci daje audyt trail)
    log_regime_gate_event(
        symbol=SYMBOL,
        interval=INTERVAL,
        strategy=STRATEGY_NAME,
        decision=decision,
        allow=allow_gate,
        rmeta=rmeta_gate,
        extra_meta={"event": "EXIT", **(extra or {})},
    )


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
        logging.info("TREND: open_position skipped - position already OPEN.")
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
    logging.info("TREND: position OPENED side=%s qty=%.8f entry=%.2f", side, float(qty), float(entry_price))
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
        logging.info("TREND: position CLOSED reason=%s exit=%.2f", reason, float(exit_price))
    else:
        logging.info("TREND: close_position skipped - no OPEN position found.")
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

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_control (
          id SERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          strategy TEXT NOT NULL,
          mode TEXT NOT NULL DEFAULT 'NORMAL',
          reason TEXT,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE(symbol, strategy)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_heartbeat (
          id SERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          strategy TEXT NOT NULL,
          last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
          info JSONB,
          UNIQUE(symbol, strategy, interval)
        );
        """
    )

    cur.execute(
        """
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
        """
    )

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_positions_open
        ON positions(symbol, strategy, interval)
        WHERE status='OPEN';
        """
    )

    cur.execute(
        """
        INSERT INTO bot_control(symbol, strategy, interval, mode)
        VALUES (%s, %s, %s, 'NORMAL')
        ON CONFLICT (symbol, strategy, interval) DO NOTHING;
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL),
    )

    conn.commit()

    seed_default_params_from_env(conn)

    cur.close()
    conn.close()
    logging.info("DB initialized (candles, simulated_orders, strategy tables).")


def seed_default_params_from_env(conn):
    global EMA_FAST, EMA_SLOW
    global RSI_OVERSOLD, RSI_OVERBOUGHT
    global STOP_LOSS_PCT, TAKE_PROFIT_PCT
    global MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT
    global TREND_BUFFER, TREND_FILTER_PCT, ENTRY_BUFFER_PCT
    global ORDER_QTY_BTC, MAX_DIST_FROM_EMA_FAST_PCT

    cur = conn.cursor()
    cur.execute(
        """
        SELECT param_name, param_value
        FROM strategy_params
        WHERE symbol = %s AND strategy = %s
        """,
        (SYMBOL, STRATEGY_NAME),
    )
    existing_rows = cur.fetchall()
    existing = {name: float(value) for (name, value) in existing_rows}

    defaults = {
        "EMA_FAST": float(EMA_FAST),
        "EMA_SLOW": float(EMA_SLOW),
        "RSI_OVERSOLD": float(RSI_OVERSOLD),
        "RSI_OVERBOUGHT": float(RSI_OVERBOUGHT),
        "STOP_LOSS_PCT": float(STOP_LOSS_PCT),
        "TAKE_PROFIT_PCT": float(TAKE_PROFIT_PCT),
        "MAX_POSITION_MINUTES": float(MAX_POSITION_MINUTES),
        "DAILY_MAX_LOSS_PCT": float(DAILY_MAX_LOSS_PCT),
        "TREND_BUFFER": float(TREND_BUFFER),
        "TREND_FILTER_PCT": float(TREND_FILTER_PCT),
        "ENTRY_BUFFER_PCT": float(ENTRY_BUFFER_PCT),
        "ORDER_QTY_BTC": float(ORDER_QTY_BTC),
        "MAX_DIST_FROM_EMA_FAST_PCT": float(MAX_DIST_FROM_EMA_FAST_PCT),
    }

    inserted_any = False

    for name, value in defaults.items():
        if name in existing:
            continue

        cur.execute(
            """
            INSERT INTO strategy_params (symbol, strategy, param_name, param_value)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, strategy, param_name) DO NOTHING
            """,
            (SYMBOL, STRATEGY_NAME, name, value),
        )

        cur.execute(
            """
            INSERT INTO strategy_params_history
            (symbol, strategy, param_name, old_value, new_value, source)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (SYMBOL, STRATEGY_NAME, name, None, value, "MANUAL"),
        )
        inserted_any = True

    if inserted_any:
        conn.commit()
        logging.info(
            "Seeded default TREND params from ENV for %s / %s: %s",
            SYMBOL,
            STRATEGY_NAME,
            ", ".join(defaults.keys()),
        )
    else:
        logging.info(
            "TREND params already exist in DB for %s / %s – skipping ENV seed.",
            SYMBOL,
            STRATEGY_NAME,
        )

    cur.close()


def load_runtime_params():
    global RSI_OVERSOLD, RSI_OVERBOUGHT, STOP_LOSS_PCT, TAKE_PROFIT_PCT
    global MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT, TREND_BUFFER
    global TREND_FILTER_PCT, ENTRY_BUFFER_PCT, EMA_FAST, EMA_SLOW
    global ORDER_QTY_BTC, MAX_DIST_FROM_EMA_FAST_PCT

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT param_name, param_value
        FROM strategy_params
        WHERE symbol = %s AND strategy = %s
        """,
        (SYMBOL, STRATEGY_NAME),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        logging.info("TREND %s: no strategy_params rows found, using ENV/defaults.", SYMBOL)
        return

    params = {name: float(value) for (name, value) in rows}

    def clamp(val, lo, hi):
        return max(lo, min(hi, val))

    if "EMA_FAST" in params:
        EMA_FAST = int(clamp(params["EMA_FAST"], 2, 200))
    if "EMA_SLOW" in params:
        EMA_SLOW = int(clamp(params["EMA_SLOW"], max(EMA_FAST + 1, 3), 400))

    if "RSI_OVERSOLD" in params:
        RSI_OVERSOLD = clamp(params["RSI_OVERSOLD"], 5.0, 40.0)
    if "RSI_OVERBOUGHT" in params:
        RSI_OVERBOUGHT = clamp(params["RSI_OVERBOUGHT"], 60.0, 95.0)

    if "STOP_LOSS_PCT" in params:
        STOP_LOSS_PCT = clamp(params["STOP_LOSS_PCT"], 0.1, 5.0)
    if "TAKE_PROFIT_PCT" in params:
        TAKE_PROFIT_PCT = clamp(params["TAKE_PROFIT_PCT"], 0.1, 10.0)

    if "MAX_POSITION_MINUTES" in params:
        MAX_POSITION_MINUTES = int(clamp(params["MAX_POSITION_MINUTES"], 5, 24 * 60))

    if "DAILY_MAX_LOSS_PCT" in params:
        DAILY_MAX_LOSS_PCT = clamp(params["DAILY_MAX_LOSS_PCT"], 0.0, 10.0)

    if "TREND_BUFFER" in params:
        TREND_BUFFER = clamp(params["TREND_BUFFER"], 0.0001, 0.02)
    if "TREND_FILTER_PCT" in params:
        TREND_FILTER_PCT = clamp(params["TREND_FILTER_PCT"], 0.0001, 0.05)
    if "ENTRY_BUFFER_PCT" in params:
        ENTRY_BUFFER_PCT = clamp(params["ENTRY_BUFFER_PCT"], 0.0005, 0.05)

    if "ORDER_QTY_BTC" in params:
        ORDER_QTY_BTC = clamp(params["ORDER_QTY_BTC"], 0.00001, 1.0)
    if "MAX_DIST_FROM_EMA_FAST_PCT" in params:
        MAX_DIST_FROM_EMA_FAST_PCT = clamp(params["MAX_DIST_FROM_EMA_FAST_PCT"], 0.05, 5.0)

    min_needed = ENTRY_BUFFER_PCT * 100.0
    if MAX_DIST_FROM_EMA_FAST_PCT < min_needed:
        logging.info(
            "TREND: MAX_DIST_FROM_EMA_FAST_PCT %.2f%% < ENTRY_BUFFER_PCT(min) %.2f%% -> adjusting to %.2f%%",
            MAX_DIST_FROM_EMA_FAST_PCT, min_needed, min_needed
        )
        MAX_DIST_FROM_EMA_FAST_PCT = min_needed

    logging.info(
        "RUNTIME_PARAMS|symbol=%s|strategy=%s|EMA_FAST=%d|EMA_SLOW=%d|"
        "TREND_FILTER_PCT=%.4f|ENTRY_BUFFER_PCT=%.4f|TREND_BUFFER=%.4f|"
        "STOP_LOSS_PCT=%.2f|TAKE_PROFIT_PCT=%.2f|MAX_POSITION_MINUTES=%d|"
        "DAILY_MAX_LOSS_PCT=%.2f|RSI_OVERSOLD=%.1f|RSI_OVERBOUGHT=%.1f"
        "|ORDER_QTY_BTC=%.8f|MAX_DIST_FROM_EMA_FAST_PCT=%.2f",
        SYMBOL,
        STRATEGY_NAME,
        EMA_FAST,
        EMA_SLOW,
        TREND_FILTER_PCT,
        ENTRY_BUFFER_PCT,
        TREND_BUFFER,
        STOP_LOSS_PCT,
        TAKE_PROFIT_PCT,
        MAX_POSITION_MINUTES,
        DAILY_MAX_LOSS_PCT,
        RSI_OVERSOLD,
        RSI_OVERBOUGHT,
        ORDER_QTY_BTC,
        MAX_DIST_FROM_EMA_FAST_PCT,
    )


def get_latest_candles(limit: int):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, interval, open_time, close, ema_21, rsi_14
        FROM candles
        WHERE symbol = %s AND interval = %s
        ORDER BY open_time DESC
        LIMIT %s
        """,
        (SYMBOL, INTERVAL, limit),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


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
    if inserted:
        logging.info(
            "Simulated %s order at %.2f BTC qty=%.8f (reason=%s, strategy=%s)",
            side, float(price), float(qty_btc), reason, strategy,
        )
    else:
        logging.info(
            "Simulated order skipped by DB guard (symbol=%s interval=%s strategy=%s candle_open_time=%s).",
            symbol, interval, strategy, candle_open_time,
        )
    cur.close()
    conn.close()
    return inserted


def execute_and_record(side: str, price: float, qty_btc: float, reason: str, candle_open_time):
    """
    Jeden punkt prawdy:
    - jeśli LIVE: najpierw giełda (place_live_order)
    - potem zapis do simulated_orders (na razie jako ledger dla UI)
    Zwraca: inserted(bool)
    """
    if TRADING_MODE == "LIVE":
        # jeśli LIVE_ORDERS_ENABLED=0 -> place_live_order zwróci None i to jest OK (tryb "shadow")
        resp = place_live_order(client, SYMBOL, side, qty_btc)

        # jeżeli włączyłeś LIVE_ORDERS_ENABLED=1, a resp=None -> traktuj to jako błąd
        if LIVE_ORDERS_ENABLED and resp is None:
            raise RuntimeError("LIVE_ORDERS_ENABLED=1 but no exchange response")

    inserted = insert_simulated_order(
        symbol=SYMBOL,
        interval=INTERVAL,
        side=side,
        price=price,
        qty_btc=qty_btc,
        reason=reason,
        rsi_14=None,
        ema_21=None,
        candle_open_time=candle_open_time,
    )
    return inserted


def safe_close_if_open(current_price: float, candle_open_time):
    pos = get_open_position()
    if not pos:
        return False

    _, side, qty, _, _ = pos

    if side == "LONG":
        exit_side = "SELL"
        reason = "PANIC CLOSE LONG"
    else:
        exit_side = "BUY"
        reason = "PANIC CLOSE SHORT"

    inserted = execute_and_record(
        side=exit_side,
        price=current_price,
        qty_btc=float(qty),
        reason=reason,
        candle_open_time=candle_open_time,
    )
    if not inserted:
        logging.info("TREND: exit blocked by DB guard (already traded this candle) -> skipping close.")
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
        WHERE symbol = %s AND interval = %s AND strategy = %s
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

    equity_now = cash + btc * current_price

    if equity_start_today is None:
        return 0.0

    return (equity_now - equity_start_today) / equity_start_today * 100.0


def run_trend_strategy():
    rows = get_latest_candles(limit=max(EMA_SLOW + 20, 100))
    if not rows or len(rows) < EMA_SLOW + 5:
        logging.info("TREND: not enough candles yet (have %d).", len(rows) if rows else 0)
        return

    rows_asc = list(reversed(rows))

    df = pd.DataFrame(
        [
            {"symbol": r[0], "interval": r[1], "open_time": r[2], "close": float(r[3])}
            for r in rows_asc
        ]
    )

    closes = df["close"]
    df["ema_fast"] = closes.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = closes.ewm(span=EMA_SLOW, adjust=False).mean()

    if df["ema_slow"].isna().iloc[-1]:
        logging.info("TREND: ema_slow not ready yet, skipping.")
        return

    last = df.iloc[-1]
    prev = df.iloc[-2]

    price = float(last["close"])
    prev_price = float(prev["close"])
    ema_fast = float(last["ema_fast"])
    ema_slow = float(last["ema_slow"])
    ema_fast_prev = float(prev["ema_fast"])
    open_time = last["open_time"]

    mode = get_mode()
    if mode == "HALT":
        return

    if mode == "PANIC":
        safe_close_if_open(current_price=float(price), candle_open_time=open_time)
        set_mode("HALT", reason="Panic executed; halting.")
        return

    trend = "FLAT"
    if ema_fast > ema_slow * (1.0 + TREND_FILTER_PCT):
        trend = "UP"
    elif ema_fast < ema_slow * (1.0 - TREND_FILTER_PCT):
        trend = "DOWN"

    logging.info("TREND: price=%.2f ema_fast=%.2f ema_slow=%.2f trend=%s", price, ema_fast, ema_slow, trend)

    allow_gate, rmeta_gate = regime_allows(STRATEGY_NAME, SYMBOL, INTERVAL)
    pos = get_open_position()
    heartbeat({
        "price": float(price),
        "open_time": str(open_time),
        "trend": trend,
        "ema_fast": float(ema_fast),
        "ema_slow": float(ema_slow),
        "has_position": bool(pos is not None),
        "regime_enabled": bool(rmeta_gate.get("enabled", False)),
        "regime": rmeta_gate.get("regime"),
        "regime_mode": rmeta_gate.get("mode"),
        "regime_would_block": rmeta_gate.get("would_block"),
        "regime_why": rmeta_gate.get("why"),         
        "regime_reason": rmeta_gate.get("reason"), 
        "regime_ts": str(rmeta_gate.get("ts")),
        "regime_age_s": rmeta_gate.get("age_s"),
    })

    pos = get_open_position()
    has_position = pos is not None

    if has_position:
        _, pos_side, pos_qty, pos_entry_price, pos_entry_time = pos
        pos_side = str(pos_side).upper()
        pos_qty = float(pos_qty)
        pos_entry_price = float(pos_entry_price)

        # --- EXIT LOGIC ---
        if pos_side == "LONG":
            change_pct = (price - pos_entry_price) / pos_entry_price * 100.0

            if TAKE_PROFIT_PCT > 0 and change_pct >= TAKE_PROFIT_PCT:
                reason = f"TREND TAKE PROFIT LONG {change_pct:.2f}% >= {TAKE_PROFIT_PCT:.2f}%"
                inserted = execute_and_record(
                    side="SELL",
                    price=price,
                    qty_btc=pos_qty,
                    reason=reason,
                    candle_open_time=open_time,
                )
                if not inserted:
                    logging.info("TREND: exit blocked by DB guard -> skipping close.")
                    return

                closed = close_position(exit_price=price, reason="TAKE_PROFIT")
                if not closed:
                    logging.info("TREND: exit inserted but position was not OPEN (race/restart).")
                return

            drop_pct = -change_pct
            if STOP_LOSS_PCT > 0 and drop_pct >= STOP_LOSS_PCT:
                reason = f"TREND STOP LOSS LONG {drop_pct:.2f}% >= {STOP_LOSS_PCT:.2f}%"
                inserted = execute_and_record(
                    side="SELL",
                    price=price,
                    qty_btc=pos_qty,
                    reason=reason,
                    candle_open_time=open_time,
                )
                if not inserted:
                    logging.info("TREND: exit blocked by DB guard -> skipping close.")
                    return

                closed = close_position(exit_price=price, reason="TAKE_PROFIT")
                if not closed:
                    logging.info("TREND: exit inserted but position was not OPEN (race/restart).")
                return

        elif pos_side == "SHORT":
            change_pct = (pos_entry_price - price) / pos_entry_price * 100.0

            if TAKE_PROFIT_PCT > 0 and change_pct >= TAKE_PROFIT_PCT:
                reason = f"TREND TAKE PROFIT SHORT {change_pct:.2f}% >= {TAKE_PROFIT_PCT:.2f}%"
                inserted = execute_and_record(
                    side="BUY",
                    price=price,
                    qty_btc=pos_qty,
                    reason=reason,
                    candle_open_time=open_time,
                )
                if not inserted:
                    logging.info("TREND: exit blocked by DB guard -> skipping close.")
                    return
                close_position(exit_price=price, reason="TAKE_PROFIT_SHORT")
                return

            rise_pct = -change_pct
            if STOP_LOSS_PCT > 0 and rise_pct >= STOP_LOSS_PCT:
                reason = f"TREND STOP LOSS SHORT {rise_pct:.2f}% >= {STOP_LOSS_PCT:.2f}%"
                inserted = execute_and_record(
                    side="BUY",
                    price=price,
                    qty_btc=pos_qty,
                    reason=reason,
                    candle_open_time=open_time,
                )
                if not inserted:
                    logging.info("TREND: exit blocked by DB guard -> skipping close.")
                    return
                close_position(exit_price=price, reason="STOP_LOSS_SHORT")
                return

        # TIMEOUT
        if MAX_POSITION_MINUTES > 0 and pos_entry_time is not None:
            if pos_entry_time.tzinfo is None:
                pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)

            age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0
            if age_minutes >= MAX_POSITION_MINUTES:
                side_timeout = "SELL" if pos_side == "LONG" else "BUY"
                reason_timeout = f"TREND TIMEOUT {pos_side} {age_minutes:.1f}m >= {MAX_POSITION_MINUTES}m"
                allow_gate, rmeta_gate = regime_allows(STRATEGY_NAME, SYMBOL, INTERVAL)
                log_regime_gate_on_exit(
                    decision=side_timeout,
                    rmeta_gate=rmeta_gate,
                    allow_gate=allow_gate,
                    extra={
                        "exit_reason": "TIMEOUT",
                        "pos_side": pos_side,
                        "age_minutes": float(age_minutes),
                        "max_minutes": int(MAX_POSITION_MINUTES),
                        "price": float(price),
                        "open_time": str(open_time),
                    },
                )
                inserted = execute_and_record(
                    side=side_timeout,
                    price=price,
                    qty_btc=pos_qty,
                    reason=reason_timeout,
                    candle_open_time=open_time,
                )
                if not inserted:
                    logging.info("TREND: exit blocked by DB guard -> skipping close.")
                    return
                close_position(exit_price=price, reason="TIMEOUT")
                return

        return  # mamy pozycję -> brak nowych wejść

    # --- ENTRY FILTERS ---
    hour_utc = open_time.hour
    if hour_utc in DISABLE_HOURS_SET:
        logging.info("TREND: hour %s UTC in DISABLE_HOURS=%s -> skip opening new trades.", hour_utc, DISABLE_HOURS_SET)
        return

    if DAILY_MAX_LOSS_PCT > 0:
        daily_pct = compute_daily_pnl_pct(SYMBOL, INTERVAL, price)
        if daily_pct <= -DAILY_MAX_LOSS_PCT:
            logging.info("TREND %s: daily PnL %.2f%% <= -%.2f%% -> blocking new entries.", SYMBOL, daily_pct, DAILY_MAX_LOSS_PCT)
            return

    if trend == "FLAT":
        logging.info("TREND: trend=FLAT -> no new entries.")
        return

    decision = "HOLD"
    reason = None

    up_breakout_now = price > ema_fast * (1.0 + ENTRY_BUFFER_PCT)
    up_breakout_prev = prev_price > ema_fast_prev * (1.0 + ENTRY_BUFFER_PCT)

    down_breakout_now = price < ema_fast * (1.0 - ENTRY_BUFFER_PCT)
    down_breakout_prev = prev_price < ema_fast_prev * (1.0 - ENTRY_BUFFER_PCT)

    if trend == "UP":
        if up_breakout_now and not up_breakout_prev:
            decision = "BUY"
            reason = (
                f"TREND UP: breakout above EMA_FAST (price {price:.2f} > ema_fast "
                f"{ema_fast:.2f} * (1+{ENTRY_BUFFER_PCT:.4f}))"
            )
    elif trend == "DOWN":
        if down_breakout_now and not down_breakout_prev:
            decision = "SELL"
            reason = (
                f"TREND DOWN: breakout below EMA_FAST (price {price:.2f} < ema_fast "
                f"{ema_fast:.2f} * (1-{ENTRY_BUFFER_PCT:.4f}))"
            )

    if decision == "HOLD":
        logging.info("TREND: no entry signal (trend=%s).", trend)
        return
    
    # --- REGIME GATE (ENTRY ONLY) ---
    # logujemy zdarzenie (DRY_RUN would_block albo ENFORCE block)
    log_regime_gate_event(
        symbol=SYMBOL,
        interval=INTERVAL,
        strategy=STRATEGY_NAME,
        decision=decision,
        allow=allow_gate,
        rmeta=rmeta_gate,
        extra_meta={
            "price": float(price),
            "ema_fast": float(ema_fast),
            "ema_slow": float(ema_slow),
            "trend": trend,
            "open_time": str(open_time),
        },
    )

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

    dist_from_fast_pct = (price - ema_fast) / ema_fast * 100.0
    if abs(dist_from_fast_pct) > MAX_DIST_FROM_EMA_FAST_PCT:
        logging.info(
            "TREND: skip entry – price %.2f is %.2f%% away from EMA_FAST %.2f (max %.2f%%).",
            price, dist_from_fast_pct, ema_fast, MAX_DIST_FROM_EMA_FAST_PCT
        )
        return

    qty_btc = ORDER_QTY_BTC
    logging.info("TREND: opening %s at %.2f (%s)", decision, price, reason)

    inserted = execute_and_record(
        side=decision,
        price=price,
        qty_btc=qty_btc,
        reason=reason,
        candle_open_time=open_time,
    )
    if not inserted:
        logging.info("TREND: entry blocked by DB guard -> skipping open_position.")
        return

    opened = False
    if decision == "BUY":
        opened = open_position("LONG", qty_btc, price)
    elif decision == "SELL":
        opened = open_position("SHORT", qty_btc, price)

    if not opened:
        # Jeśli to kiedykolwiek wystąpi, to znaczy że DB ma już OPEN (race/restart).
        # W LIVE to oznacza potencjalny desync — dlatego chcemy to zobaczyć w logach.
        logging.warning("TREND: order executed/recorded but position not opened (race). Investigate immediately.")
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
            run_trend_strategy()
        except Exception as e:
            logging.exception("Error in trend loop: %s", e)

        logging.info("Trend loop finished in %.3f s", time.perf_counter() - loop_start)
        time.sleep(60)


if __name__ == "__main__":
    logging.info(
        "Starting trading bot worker for %s %s (strategy=%s)...",
        SYMBOL,
        INTERVAL,
        STRATEGY_NAME,
    )
    main_loop()
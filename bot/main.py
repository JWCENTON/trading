import os
import time
import json
import logging
import psycopg2
import pandas as pd
from dataclasses import replace
from datetime import datetime, timezone, date
from psycopg2.extras import execute_batch
from binance.client import Client
from common.execution import place_live_order
from common.runtime import RuntimeConfig
from common.permissions import can_trade
from common.bot_control import upsert_defaults, read as read_bot_control


SYMBOL = os.environ.get("SYMBOL", "BTCUSDT")

SPOT_MODE = os.environ.get("SPOT_MODE", "1") == "1"

QUOTE_ASSET = os.environ.get("QUOTE_ASSET", "USDC").upper()
if not SYMBOL.endswith(QUOTE_ASSET):
    raise RuntimeError(f"SYMBOL={SYMBOL} does not match QUOTE_ASSET={QUOTE_ASSET}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

STRATEGY_NAME = os.environ.get("STRATEGY_NAME", "RSI")

INTERVAL = os.environ.get("INTERVAL", "1m")

cfg = RuntimeConfig.from_env()

# =========================
# ENV / CONFIG
# =========================

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "trading")
DB_USER = os.environ.get("DB_USER", "botuser")
DB_PASS = os.environ.get("DB_PASS", "botpass")



RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))
EMA_PERIOD = int(os.environ.get("EMA_PERIOD", "21"))

RSI_OVERSOLD = float(os.environ.get("RSI_OVERSOLD", "30"))
RSI_OVERBOUGHT = float(os.environ.get("RSI_OVERBOUGHT", "70"))

PAPER_START_USDT = float(os.environ.get("PAPER_START_USDT", "100"))

STOP_LOSS_PCT = float(os.environ.get("STOP_LOSS_PCT", "0.8"))        # % (np 0.8 = 0.8%)
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "1.2"))     # % (np 1.2 = 1.2%)

DISABLE_HOURS = os.environ.get("DISABLE_HOURS", "")
DISABLE_HOURS_SET = {int(h.strip()) for h in DISABLE_HOURS.split(",") if h.strip() != ""}

TREND_BUFFER = float(os.environ.get("TREND_BUFFER", "0.001"))         # ułamek (np 0.001 = 0.1%)
MAX_POSITION_MINUTES = int(os.environ.get("MAX_POSITION_MINUTES", "450"))
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

REGIME_MAX_AGE_SECONDS = int(os.environ.get("REGIME_MAX_AGE_SECONDS", "180"))

logging.info(
  "CONFIG|SYMBOL=%s|INTERVAL=%s|SPOT_MODE=%s|REGIME_MAX_AGE_SECONDS=%s|cfg_trading_mode=%s",
  SYMBOL, INTERVAL, SPOT_MODE, REGIME_MAX_AGE_SECONDS, cfg.trading_mode
)

def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


def execute_and_record(
    side: str,
    price: float,
    qty_btc: float,
    reason: str,
    candle_open_time,
    *,
    cfg_used: RuntimeConfig,
    allow_live_orders: bool,
    allow_meta: dict,
):
    """
    Jeden punkt prawdy: wykonanie orderów.
    - PAPER -> ledger
    - LIVE + allow_live_orders=True -> giełda + ledger
    - LIVE + allow_live_orders=False -> suppress (ledger-only albo tylko log)
    """
    if cfg_used.trading_mode == "LIVE":
        if allow_live_orders:
            resp = place_live_order(
                client,
                cfg_used.symbol,
                side,
                qty_btc,
                trading_mode=cfg_used.trading_mode,
                live_orders_enabled=cfg_used.live_orders_enabled,
                quote_asset=cfg_used.quote_asset,
                panic_disable_trading=(os.environ.get("PANIC_DISABLE_TRADING", "0") == "1"),
                live_max_notional=float(os.environ.get("LIVE_MAX_NOTIONAL", "0")),
            )
            logging.info("LIVE_ORDER|resp=%s", json.dumps(resp, default=_json_default) if resp else None)
            if resp is None:
                logging.error(
                    "LIVE order failed/blocked -> skipping record (symbol=%s side=%s qty=%.8f reason=%s)",
                    cfg_used.symbol, side, float(qty_btc), reason
                )
                return False
        else:
            logging.warning(
                "LIVE ORDER SUPPRESSED symbol=%s side=%s qty=%.8f allow=false why=%s",
                cfg_used.symbol, side, float(qty_btc), (allow_meta or {}).get("why")
            )
            return False

    # PAPER lub LIVE (po ack) -> zapis ledger
    return insert_simulated_order(
        symbol=cfg_used.symbol,
        interval=cfg_used.interval,
        side=side,
        price=price,
        qty_btc=qty_btc,
        reason=reason,
        rsi_14=None,
        ema_21=None,
        candle_open_time=candle_open_time,
    )


def regime_allows(strategy_name: str, symbol: str, interval: str, bc):
    """
    Zwraca: (allow: bool, meta: dict)
    DRY_RUN: zawsze allow=True, ale meta mówi czy 'would_block'.
    """
    if not bc.regime_enabled:
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
        return True, {"enabled": True, "reason": f"regime_stale age={age:.0f}s", "age_s": age, "regime": r.get("regime"), "ts": ts}

    regime = r.get("regime")

    # Polityka blokowania (v1):
    # - RSI/BBRANGE: blokuj w TREND_UP/TREND_DOWN i SHOCK
    would_block = False
    why = "ok"
    if strategy_name in ("RSI", "BBRANGE"):
        if regime in ("TREND_UP", "TREND_DOWN", "SHOCK"):
            would_block = True
            why = f"{strategy_name} blocked in {regime}"

    if bc.regime_mode == "DRY_RUN":
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


def get_runtime_snapshot(price: float, open_time):
    """
    Jedno miejsce prawdy dla runtime:
    - bot_control
    - regime gate (ENTRY gate)
    - permissions (LIVE order send)
    - heartbeat meta
    """
    bc = read_bot_control(SYMBOL, STRATEGY_NAME, INTERVAL)

    cfg_effective = replace(
        cfg,
        live_orders_enabled=bc.live_orders_enabled,
        regime_enabled=bc.regime_enabled,
        regime_mode=bc.regime_mode,
    )

    # ENTRY gate: zależny od reżimu
    allow_gate_entry, rmeta_gate = regime_allows(STRATEGY_NAME, SYMBOL, INTERVAL, bc)

    # Czy wolno wysłać LIVE order (uwzględnia TRADING_MODE + LIVE_ORDERS_ENABLED + PANIC etc)
    allowed_orders_entry, allow_meta_entry = can_trade(cfg_effective, regime_allows_trade=allow_gate_entry)

    # EXIT: zawsze dozwolony (regime nie może blokować zamknięcia pozycji)
    allowed_orders_exit, allow_meta_exit = can_trade(cfg_effective, regime_allows_trade=True)

    hb = {
        "price": float(price),
        "open_time": str(open_time),
        "trading_mode": cfg_effective.trading_mode,
        "live_orders_enabled": bool(cfg_effective.live_orders_enabled),
        "bot_enabled": bool(getattr(bc, "enabled", True)),
        "bot_mode": getattr(bc, "mode", None),

        "regime_enabled": bool(bc.regime_enabled),
        "regime_mode": bc.regime_mode,
        "regime": (rmeta_gate or {}).get("regime"),
        "regime_would_block": (rmeta_gate or {}).get("would_block"),
        "regime_why": (rmeta_gate or {}).get("why"),
        "regime_reason": (rmeta_gate or {}).get("reason"),
        "regime_ts": str((rmeta_gate or {}).get("ts")),
        "regime_age_s": (rmeta_gate or {}).get("age_s"),

        "allow_entry_gate": bool(allow_gate_entry),
        "allow_live_orders_entry": bool(allowed_orders_entry),
        "allow_live_orders_exit": bool(allowed_orders_exit),
        "allow_meta_entry": allow_meta_entry,
        "allow_meta_exit": allow_meta_exit,

        "symbol": cfg_effective.symbol,
        "interval": cfg_effective.interval,
        "strategy": STRATEGY_NAME,
        "quote_asset": cfg_effective.quote_asset,
    }

    return {
        "bc": bc,
        "cfg_effective": cfg_effective,
        "allow_gate_entry": allow_gate_entry,
        "rmeta_gate": rmeta_gate,
        "allowed_orders_entry": allowed_orders_entry,
        "allow_meta_entry": allow_meta_entry,
        "allowed_orders_exit": allowed_orders_exit,
        "allow_meta_exit": allow_meta_exit,
        "heartbeat": hb,
    }


def get_mode() -> str:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT mode FROM bot_control WHERE symbol=%s AND strategy=%s AND interval=%s",
        (SYMBOL, STRATEGY_NAME, INTERVAL),
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
        INSERT INTO bot_control(symbol, strategy, interval, mode, reason, updated_at)
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (symbol, strategy, interval)
        DO UPDATE SET mode=EXCLUDED.mode, reason=EXCLUDED.reason, updated_at=now();
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL, mode, reason),
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
        CREATE TABLE IF NOT EXISTS bot_heartbeat (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            strategy TEXT NOT NULL,
            interval TEXT NOT NULL,
            last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
            info JSONB,
            UNIQUE(symbol, strategy, interval)
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
        INSERT INTO bot_control(symbol, strategy, interval, mode)
        VALUES (%s, %s, %s, 'NORMAL')
        ON CONFLICT (symbol, strategy, interval) DO NOTHING;
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL),
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
        ON CONFLICT (symbol, interval, strategy, candle_open_time) DO NOTHING
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
        SELECT open_time, open, high, low, close, ema_21, rsi_14
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
    return row  # (open_time, open, high, low, close, ema_21, rsi_14)


# =========================
# STRATEGY LOGIC
# =========================


def run_strategy(row):
    if not row:
        logging.info("RSI: no candle data available. (ROW==none)")
        return

    open_time, open_px, high_px, low_px, close_px, ema_21, rsi_14 = row

    close_price = float(close_px) if close_px is not None else None
    high_price  = float(high_px) if high_px is not None else None
    low_price   = float(low_px) if low_px is not None else None
    price = close_price  # jednoznacznie: price = close świecy

    if close_price is None or high_price is None or low_price is None:
        return

    if ema_21 is None or rsi_14 is None:
        logging.info("RSI: indicators not ready yet.")
        heartbeat({"price": price, "open_time": str(open_time), "status": "indicators_not_ready"})
        return

    ema_val = float(ema_21)
    rsi_val = float(rsi_14)

    snap = get_runtime_snapshot(price=price, open_time=open_time)
    bc = snap["bc"]
    cfg_effective = snap["cfg_effective"]

    # HARD stop
    if bc.mode == "HALT":
        heartbeat({**snap["heartbeat"], "status": "HALT"})
        return

    # heartbeat zawsze
    heartbeat({
        **snap["heartbeat"],
        "ema_21": float(ema_val),
        "rsi_14": float(rsi_val),
    })

    # PANIC zachowujemy jak masz (panic close + HALT)
    if bc.mode == "PANIC":
        pos = get_open_position()
        if pos:
            _, side, qty, _entry_price, _entry_time = pos
            side_u = str(side).upper()
            qty_f = float(qty)

            exit_side = "SELL" if side_u == "LONG" else "BUY"

            inserted = execute_and_record(
                side=exit_side,
                price=price,
                qty_btc=qty_f,
                reason="PANIC CLOSE",
                candle_open_time=open_time,
                cfg_used=cfg_effective,
                allow_live_orders=snap["allowed_orders_exit"],
                allow_meta=snap["allow_meta_exit"],
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

        # --- LONG ---
        if pos_side_u == "LONG":
            tp_level = entry_f * (1.0 + TAKE_PROFIT_PCT / 100.0)
            sl_level = entry_f * (1.0 - STOP_LOSS_PCT / 100.0)

            # TP intrabar
            if TAKE_PROFIT_PCT > 0 and high_price >= tp_level:
                exec_px = tp_level  # dla ledger sensowniejsze niż close
                reason = f"RSI TAKE PROFIT LONG intrabar high={high_price:.2f} >= tp={tp_level:.2f}"
                inserted = execute_and_record("SELL", exec_px, qty_f, reason, open_time, cfg_used=cfg_effective, allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],)
                if inserted:
                    close_position(exit_price=exec_px, reason="TAKE_PROFIT")
                return

            # SL intrabar
            if STOP_LOSS_PCT > 0 and low_price <= sl_level:
                exec_px = sl_level
                reason = f"RSI STOP LOSS LONG intrabar low={low_price:.2f} <= sl={sl_level:.2f}"
                inserted = execute_and_record("SELL", exec_px, qty_f, reason, open_time, cfg_used=cfg_effective, allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],)
                if inserted:
                    close_position(exit_price=exec_px, reason="STOP_LOSS")
                return

        # --- SHORT ---
        else:
            tp_level = entry_f * (1.0 - TAKE_PROFIT_PCT / 100.0)
            sl_level = entry_f * (1.0 + STOP_LOSS_PCT / 100.0)

            # TP intrabar
            if TAKE_PROFIT_PCT > 0 and low_price <= tp_level:
                exec_px = tp_level
                reason = f"RSI TAKE PROFIT SHORT intrabar low={low_price:.2f} <= tp={tp_level:.2f}"
                inserted = execute_and_record("BUY", exec_px, qty_f, reason, open_time, cfg_used=cfg_effective, allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],)
                if inserted:
                    close_position(exit_price=exec_px, reason="TAKE_PROFIT_SHORT")
                return

            # SL intrabar
            if STOP_LOSS_PCT > 0 and high_price >= sl_level:
                exec_px = sl_level
                reason = f"RSI STOP LOSS SHORT intrabar high={high_price:.2f} >= sl={sl_level:.2f}"
                inserted = execute_and_record("BUY", exec_px, qty_f, reason, open_time, cfg_used=cfg_effective, allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],)
                if inserted:
                    close_position(exit_price=exec_px, reason="STOP_LOSS_SHORT")
                return

        # TIMEOUT (dla obu stron)
        if MAX_POSITION_MINUTES > 0 and pos_entry_time is not None:
            if pos_entry_time.tzinfo is None:
                pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)
            age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0
            if age_minutes >= MAX_POSITION_MINUTES:
                side_timeout = "SELL" if pos_side_u == "LONG" else "BUY"
                reason_timeout = f"RSI TIMEOUT {pos_side_u} {age_minutes:.1f}m >= {MAX_POSITION_MINUTES}m"
                inserted = execute_and_record(
                    side=side_timeout,
                    price=price,  # close_price
                    qty_btc=qty_f,
                    reason=reason_timeout,
                    candle_open_time=open_time,
                    cfg_used=cfg_effective, 
                    allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],
                )
                if inserted:
                    close_position(exit_price=price, reason="TIMEOUT")
                return

        return  # OPEN i nic nie zamyka

    # =========================
    # 2) ENTRY (tu stosujemy filtry + REGIME)
    # =========================

    # filtr godzinowy
    hour_utc = open_time.hour
    if hour_utc in DISABLE_HOURS_SET:
        logging.info("RSI: hour %s UTC in DISABLE_HOURS=%s -> skip entries.", hour_utc, DISABLE_HOURS_SET)
        return
    
    if not bc.enabled:
        logging.info("RSI: bot_control.enabled=false -> blocking new entries.")
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

    # SPOT: nie otwieramy shortów
    if decision == "SELL" and SPOT_MODE:
        logging.info("RSI: SELL entry ignored (SPOT_MODE=1).")
        return

    # --- REGIME GATE (ENTRY ONLY) ---
    log_regime_gate_event(
        symbol=SYMBOL,
        interval=INTERVAL,
        strategy=STRATEGY_NAME,
        decision=decision,
        allow=snap["allow_gate_entry"],
        rmeta=snap["rmeta_gate"],
        extra_meta={
            "price": float(price),
            "ema_21": float(ema_val),
            "rsi_14": float(rsi_val),
            "open_time": str(open_time),
            "reason": reason,
        },
    )

    if not snap["allow_gate_entry"]:
        logging.info(
            "REGIME_GATE|blocked entry|strategy=%s|symbol=%s|interval=%s|decision=%s|regime=%s|why=%s|mode=%s",
            STRATEGY_NAME, SYMBOL, INTERVAL, decision,
            snap["rmeta_gate"].get("regime"), snap["rmeta_gate"].get("why"), snap["rmeta_gate"].get("mode"),
        )
        return

    qty_btc = ORDER_QTY_BTC

    # 1) giełda (LIVE) + ledger
    inserted = execute_and_record(
        side=decision,
        price=price,
        qty_btc=qty_btc,
        reason=reason,
        candle_open_time=open_time,
        cfg_used=cfg_effective, 
        allow_live_orders=snap["allowed_orders_entry"],
        allow_meta=snap["allow_meta_entry"],
    )
    if not inserted:
        logging.info("RSI: entry blocked/failed -> not opening position.")
        return

    # 2) po udanym record otwieramy pozycję w DB
    if decision == "BUY":
        opened = open_position("LONG", qty_btc, price)
    else:
        opened = open_position("SHORT", qty_btc, price)

    if not opened:
        logging.warning("RSI: order recorded but position not opened (race/desync). Investigate.")
        return


# =========================
# MAIN LOOP
# =========================


LAST_PROCESSED_OPEN_TIME = None

def main_loop():
    global LAST_PROCESSED_OPEN_TIME

    init_db()
    upsert_defaults(SYMBOL, STRATEGY_NAME, INTERVAL, cfg=cfg)
    while True:
        loop_start = time.perf_counter()
        try:
            rows = fetch_klines(limit=200)   # patrz Zmiana 3
            save_klines(rows)
            update_indicators()

            latest = get_latest_candle()
            if latest:
                open_time = latest[0]  # (open_time, open, high, low, close, ema_21, rsi_14)

                # Uruchamiaj logikę tylko raz na nową świecę
                if LAST_PROCESSED_OPEN_TIME != open_time:
                    LAST_PROCESSED_OPEN_TIME = open_time
                    run_strategy(latest)
                else:
                    logging.info("RSI: no new candle yet (%s) -> skip strategy.", str(open_time))

        except Exception:
            logging.exception("RSI loop error")

        logging.info("RSI loop finished in %.3f s", time.perf_counter() - loop_start)
        time.sleep(60)


if __name__ == "__main__":
    logging.info("Starting RSI bot %s %s", SYMBOL, INTERVAL)
    main_loop()
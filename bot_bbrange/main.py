import os
import time
import json
import math
import hashlib
import logging
import psycopg2
import pandas as pd
from common.execution import build_live_client_order_id
from dataclasses import replace
from datetime import datetime, timezone, date
from psycopg2.extras import execute_batch
from binance.client import Client
from common.execution import place_live_exit_maker_then_market
from common.daily_loss import should_emit_daily_loss_shadow
from common.alerts import emit_alert_throttled
from common.binance_ingest_trades import ingest_my_trades
from common.flags import binance_mytrades_enabled
from common.db import get_db_conn
from common.schema import ensure_schema
from common.runtime import RuntimeConfig
from common.permissions import can_trade
from common.execution import place_live_order
from common.bot_control import upsert_defaults, read as read_bot_control
from common.regime_gate import decide_regime_gate, emit_regime_gate_event
from common.sizing import compute_qty_from_notional
from common.daily_loss import compute_daily_loss_pct_positions, should_block_daily_loss_positions
from common.user_settings import SYSTEM_MIN_ENTRY_USDC, get_user_settings_snapshot


# =========================
# BASICS
# =========================

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

SYMBOL = os.environ.get("SYMBOL", "BTCUSDC")
QUOTE_ASSET = os.environ.get("QUOTE_ASSET", "USDC").upper()
if not SYMBOL.endswith(QUOTE_ASSET):
    raise RuntimeError(f"SYMBOL={SYMBOL} does not match QUOTE_ASSET={QUOTE_ASSET}")

STRATEGY_NAME = os.environ.get("STRATEGY_NAME", "BBRANGE").upper()
INTERVAL = os.environ.get("INTERVAL", "1m")
LIVE_TARGET_NOTIONAL = float(os.environ.get("LIVE_TARGET_NOTIONAL", "6.0"))

cfg = RuntimeConfig.from_env()

# =========================
# ENV / DEFAULTS
# =========================

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "trading")
DB_USER = os.environ.get("DB_USER", "botuser")
DB_PASS = os.environ.get("DB_PASS", "botpass")

RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))
EMA_PERIOD = int(os.environ.get("EMA_PERIOD", "21"))

PAPER_START_USDC = float(os.environ.get("PAPER_START_USDC", "100"))

STOP_LOSS_PCT = float(os.environ.get("STOP_LOSS_PCT", "0.8"))      # %
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "1.0"))   # %

DISABLE_HOURS = os.environ.get("DISABLE_HOURS", "")
DISABLE_HOURS_SET = {int(h.strip()) for h in DISABLE_HOURS.split(",") if h.strip() != ""}

TREND_BUFFER = float(os.environ.get("TREND_BUFFER", "0.001"))  # fraction (0.001 = 0.1%)
MAX_POSITION_MINUTES = int(os.environ.get("MAX_POSITION_MINUTES", "90"))
DAILY_MAX_LOSS_PCT = float(os.environ.get("DAILY_MAX_LOSS_PCT", "0.5"))
ORDER_NOTIONAL_USDC = float(os.environ.get("ORDER_NOTIONAL_USDC", "10"))
MIN_NOTIONAL_BUFFER_PCT = float(os.environ.get("MIN_NOTIONAL_BUFFER_PCT", "0.05"))
MIN_QTY_BTC = float(os.environ.get("MIN_QTY_BTC", "0.00001"))
QTY_STEP_BTC = float(os.environ.get("QTY_STEP_BTC", "0.00001"))
DAILY_MAX_LOSS_BASE_USDC = float(os.environ.get("DAILY_MAX_LOSS_BASE_USDC", str(PAPER_START_USDC)))

BB_PERIOD = int(os.environ.get("BB_PERIOD", "20"))
BB_STD = float(os.environ.get("BB_STD", "2.0"))
MIN_BB_WIDTH_PCT = float(os.environ.get("MIN_BB_WIDTH_PCT", "0.0015"))  # fraction

# RSI filters for BBRANGE
RSI_LONG_MAX = float(os.environ.get("RSI_LONG_MAX", "45"))
RSI_BLOCK_EXTREME_LOW = float(os.environ.get("RSI_BLOCK_EXTREME_LOW", "10"))
RSI_BLOCK_EXTREME_HIGH = float(os.environ.get("RSI_BLOCK_EXTREME_HIGH", "90"))

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")
client = Client(api_key=API_KEY, api_secret=API_SECRET) if cfg.trading_mode == "LIVE" else Client()

logging.info(
    "CONFIG|SYMBOL=%s|INTERVAL=%s|SPOT_MODE=%s|cfg_trading_mode=%s",
    SYMBOL, INTERVAL, cfg.spot_mode, cfg.trading_mode
)

# =========================
# JSON helper
# =========================

def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)

# =========================
# EVENTS
# =========================

def emit_strategy_event(
    *,
    event_type: str,
    decision: str | None = None,
    reason: str | None = None,
    price: float | None = None,
    candle_open_time=None,
    info: dict | None = None,
    symbol=None,
    interval=None,
    strategy=None,
):
    try:
        sym = symbol or SYMBOL
        itv = interval or INTERVAL
        strat = strategy or STRATEGY_NAME

        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.strategy_events
                    (symbol, interval, strategy, event_type, decision, reason, price, candle_open_time, info)
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        symbol or SYMBOL,
                        interval or INTERVAL,
                        strategy or STRATEGY_NAME,
                        event_type,
                        decision,
                        reason,
                        float(price) if price is not None else None,
                        candle_open_time,
                        json.dumps(info or {}, default=_json_default),
                    ),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        logging.exception("strategy_events insert failed")


def emit_strategy_event_with_conn(
    *,
    conn,
    event_type: str,
    decision: str | None = None,
    reason: str | None = None,
    price: float | None = None,
    candle_open_time=None,
    info: dict | None = None,
    symbol=None,
    interval=None,
    strategy=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.strategy_events
            (symbol, interval, strategy, event_type, decision, reason, price, candle_open_time, info)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                symbol or SYMBOL,
                interval or INTERVAL,
                strategy or STRATEGY_NAME,
                event_type,
                decision,
                reason,
                float(price) if price is not None else None,
                candle_open_time,
                json.dumps(info or {}, default=_json_default),
            ),
        )
    conn.commit()


def emit_blocked(*, reason: str, decision: str | None, price: float | None, candle_open_time, info: dict | None = None):
    hard_block = reason in {
        "REGIME_BLOCK",
        "BOT_DISABLED",
        "BOT_MODE_HALT",
        "DAILY_MAX_LOSS_POSITIONS",
        "DB_GUARD_DUPLICATE",
        "LIVE_ORDER_FAILED",
        "EXIT_BLOCKED",
        "LIVE_ENTRY_NOT_ATTEMPTED",
        "LIVE_EXIT_NOT_ATTEMPTED",
        "LIVE_ENTRY_NOT_FILLED",
        "POSITION_OPEN_NO_EXIT",
    }
    et = "BLOCKED" if hard_block else "SKIP"
    emit_strategy_event(
        event_type=et,
        decision=decision,
        reason=reason,
        price=price,
        candle_open_time=candle_open_time,
        info=info or {},
    )

# =========================
# HEARTBEAT
# =========================

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
        (SYMBOL, STRATEGY_NAME, INTERVAL, json.dumps(info, default=_json_default)),
    )
    conn.commit()
    cur.close()
    conn.close()

# =========================
# BOT CONTROL MODE (per interval)
# =========================

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

# =========================
# POSITIONS (SPOT => LONG only)
# =========================

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


def attach_entry_order_id(pos_id: int, order_id: str, client_order_id: str) -> None:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE positions
        SET entry_order_id = COALESCE(entry_order_id, %s),
            entry_client_order_id = COALESCE(entry_client_order_id, %s)
        WHERE id = %s
        """,
        (str(order_id), (str(client_order_id) if client_order_id else None), int(pos_id)),
    )
    conn.commit()
    cur.close()
    conn.close()


def attach_exit_order_id(pos_id: int, order_id: str, client_order_id: str) -> None:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE positions
        SET exit_order_id = COALESCE(exit_order_id, %s),
            exit_client_order_id = COALESCE(exit_client_order_id, %s)
        WHERE id = %s
        """,
        (str(order_id), (str(client_order_id) if client_order_id else None), int(pos_id)),
    )
    conn.commit()
    cur.close()
    conn.close()


def attach_entry_order_id_with_conn(cur, pos_id: int, order_id: str | None, client_order_id: str | None) -> None:
    cur.execute(
        """
        UPDATE positions
        SET entry_order_id = COALESCE(entry_order_id, %s),
            entry_client_order_id = COALESCE(entry_client_order_id, %s)
        WHERE id = %s
        """,
        (str(order_id) if order_id else None, (str(client_order_id) if client_order_id else None), int(pos_id)),
    )


def attach_exit_order_id_with_conn(cur, pos_id: int, order_id: str | None, client_order_id: str | None) -> None:
    cur.execute(
        """
        UPDATE positions
        SET exit_order_id = COALESCE(exit_order_id, %s),
            exit_client_order_id = COALESCE(exit_client_order_id, %s)
        WHERE id = %s
        """,
        (str(order_id) if order_id else None, (str(client_order_id) if client_order_id else None), int(pos_id)),
    )


def open_position(side: str, qty: float, entry_price: float, entry_client_order_id: str | None) -> int | None:
    # SPOT => tylko LONG
    if str(side).upper() != "LONG":
        return None

    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id FROM positions
        WHERE symbol=%s AND strategy=%s AND interval=%s AND status='OPEN'
        ORDER BY entry_time DESC
        LIMIT 1
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL),
    )
    row = cur.fetchone()
    if row:
        pos_id = int(row[0])
        cur.close()
        conn.close()
        logging.info("BBRANGE: open_position skipped – already OPEN pos_id=%s.", pos_id)
        return None

    cur.execute(
        """
        INSERT INTO positions(
          symbol, strategy, interval, status, side, qty, entry_price, entry_time, entry_client_order_id
        )
        VALUES (%s, %s, %s, 'OPEN', %s, %s, %s, now(), %s)
        RETURNING id;
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL, side, float(qty), float(entry_price),
         (str(entry_client_order_id) if entry_client_order_id else None)),
    )
    pos_id = int(cur.fetchone()[0])
    conn.commit()
    cur.close()
    conn.close()

    logging.info("BBRANGE: position OPENED pos_id=%s LONG qty=%.8f entry=%.2f", pos_id, float(qty), float(entry_price))
    return pos_id


def close_position(exit_price: float, reason: str, candle_open_time) -> bool:
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
        emit_strategy_event(
            event_type="POSITION_CLOSED",
            decision=None,
            reason=reason,
            price=float(exit_price),
            candle_open_time=candle_open_time,
            info={"exit_reason": reason, "exit_price": float(exit_price)},
        )
    return closed


# =========================
# PARAMS (per interval)
# =========================

def seed_default_params_from_env(conn):
    defaults = {
        "STOP_LOSS_PCT": float(STOP_LOSS_PCT),
        "TAKE_PROFIT_PCT": float(TAKE_PROFIT_PCT),
        "MAX_POSITION_MINUTES": float(MAX_POSITION_MINUTES),
        "DAILY_MAX_LOSS_PCT": float(DAILY_MAX_LOSS_PCT),
        "TREND_BUFFER": float(TREND_BUFFER),
        "ORDER_NOTIONAL_USDC": float(ORDER_NOTIONAL_USDC),
        "MIN_NOTIONAL_BUFFER_PCT": float(MIN_NOTIONAL_BUFFER_PCT),
        "MIN_QTY_BTC": float(MIN_QTY_BTC),
        "QTY_STEP_BTC": float(QTY_STEP_BTC),

        "BB_PERIOD": float(BB_PERIOD),
        "BB_STD": float(BB_STD),
        "MIN_BB_WIDTH_PCT": float(MIN_BB_WIDTH_PCT),

        "RSI_LONG_MAX": float(RSI_LONG_MAX),
        "RSI_BLOCK_EXTREME_LOW": float(RSI_BLOCK_EXTREME_LOW),
        "RSI_BLOCK_EXTREME_HIGH": float(RSI_BLOCK_EXTREME_HIGH),
    }

    cur = conn.cursor()
    cur.execute(
        """
        SELECT param_name
        FROM strategy_params
        WHERE symbol=%s AND strategy=%s AND interval=%s
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL),
    )
    existing = {r[0] for r in cur.fetchall()}

    inserted_any = False
    for name, value in defaults.items():
        if name in existing:
            continue
        cur.execute(
            """
            INSERT INTO strategy_params (symbol, strategy, interval, param_name, param_value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (symbol, strategy, interval, param_name) DO NOTHING
            """,
            (SYMBOL, STRATEGY_NAME, INTERVAL, name, value),
        )
        inserted_any = True

    if inserted_any:
        conn.commit()
        logging.info("Seeded default BBRANGE params from ENV for %s/%s/%s.", SYMBOL, STRATEGY_NAME, INTERVAL)
    cur.close()

def load_runtime_params():
    global STOP_LOSS_PCT, TAKE_PROFIT_PCT, MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT, TREND_BUFFER
    global ORDER_NOTIONAL_USDC, MIN_QTY_BTC, QTY_STEP_BTC
    global BB_PERIOD, BB_STD, MIN_BB_WIDTH_PCT
    global RSI_LONG_MAX, RSI_BLOCK_EXTREME_LOW, RSI_BLOCK_EXTREME_HIGH
    global MIN_NOTIONAL_BUFFER_PCT


    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT param_name, param_value
        FROM strategy_params
        WHERE symbol=%s AND strategy=%s AND interval=%s
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        logging.info("BBRANGE %s: no strategy_params rows found, using ENV/defaults.", SYMBOL)
        return

    params = {name: float(value) for (name, value) in rows}

    def clamp(v, lo, hi):
        return max(lo, min(hi, v))

    STOP_LOSS_PCT = clamp(params.get("STOP_LOSS_PCT", STOP_LOSS_PCT), 0.05, 5.0)
    TAKE_PROFIT_PCT = clamp(params.get("TAKE_PROFIT_PCT", TAKE_PROFIT_PCT), 0.05, 10.0)
    MAX_POSITION_MINUTES = int(clamp(params.get("MAX_POSITION_MINUTES", MAX_POSITION_MINUTES), 1, 24*60))
    DAILY_MAX_LOSS_PCT = clamp(params.get("DAILY_MAX_LOSS_PCT", DAILY_MAX_LOSS_PCT), 0.0, 10.0)
    TREND_BUFFER = clamp(params.get("TREND_BUFFER", TREND_BUFFER), 0.0001, 0.05)

    ORDER_NOTIONAL_USDC = clamp(params.get("ORDER_NOTIONAL_USDC", ORDER_NOTIONAL_USDC), 1.0, 1_000_000.0)
    MIN_NOTIONAL_BUFFER_PCT = clamp(params.get("MIN_NOTIONAL_BUFFER_PCT", MIN_NOTIONAL_BUFFER_PCT), 0.0, 0.50)
    MIN_QTY_BTC = clamp(params.get("MIN_QTY_BTC", MIN_QTY_BTC), 0.0, 10.0)
    QTY_STEP_BTC = clamp(params.get("QTY_STEP_BTC", QTY_STEP_BTC), 0.0, 10.0)

    BB_PERIOD = int(clamp(params.get("BB_PERIOD", BB_PERIOD), 10, 200))
    BB_STD = clamp(params.get("BB_STD", BB_STD), 1.0, 4.0)
    MIN_BB_WIDTH_PCT = clamp(params.get("MIN_BB_WIDTH_PCT", MIN_BB_WIDTH_PCT), 0.0005, 0.02)

    RSI_LONG_MAX = clamp(params.get("RSI_LONG_MAX", RSI_LONG_MAX), 5.0, 60.0)
    RSI_BLOCK_EXTREME_LOW = clamp(params.get("RSI_BLOCK_EXTREME_LOW", RSI_BLOCK_EXTREME_LOW), 0.0, 30.0)
    RSI_BLOCK_EXTREME_HIGH = clamp(params.get("RSI_BLOCK_EXTREME_HIGH", RSI_BLOCK_EXTREME_HIGH), 70.0, 100.0)

    logging.info(
        "RUNTIME_PARAMS|symbol=%s|strategy=%s|STOP_LOSS_PCT=%.3f|TAKE_PROFIT_PCT=%.3f|MAX_POSITION_MINUTES=%d|"
        "DAILY_MAX_LOSS_PCT=%.3f|TREND_BUFFER=%.5f|ORDER_NOTIONAL_USDC=%.2f|MIN_QTY_BTC=%.8f|QTY_STEP_BTC=%.8f|BB_PERIOD=%d|BB_STD=%.2f|MIN_BB_WIDTH_PCT=%.5f|"
        "RSI_LONG_MAX=%.2f|RSI_BLOCK_EXTREME_LOW=%.2f|RSI_BLOCK_EXTREME_HIGH=%.2f|MIN_NOTIONAL_BUFFER_PCT=%.3f",
        SYMBOL, STRATEGY_NAME,
        STOP_LOSS_PCT, TAKE_PROFIT_PCT, MAX_POSITION_MINUTES,
        DAILY_MAX_LOSS_PCT, TREND_BUFFER, ORDER_NOTIONAL_USDC, MIN_QTY_BTC, QTY_STEP_BTC,
        BB_PERIOD, BB_STD, MIN_BB_WIDTH_PCT,
        RSI_LONG_MAX, RSI_BLOCK_EXTREME_LOW, RSI_BLOCK_EXTREME_HIGH, MIN_NOTIONAL_BUFFER_PCT,
    )

# =========================
# ORDERS (IDEMPOTENT, RSI-style: is_exit)
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
    *,
    is_exit: bool,
    strategy: str = STRATEGY_NAME,
):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO simulated_orders (
            symbol, interval, strategy, side, price, quantity_btc,
            reason, rsi_14, ema_21, candle_open_time, is_exit
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, interval, strategy, candle_open_time, is_exit) DO NOTHING
        RETURNING id;
        """,
        (
            symbol, interval, strategy, side,
            float(price), float(qty_btc),
            reason,
            float(rsi_14) if rsi_14 is not None else None,
            float(ema_21) if ema_21 is not None else None,
            candle_open_time,
            bool(is_exit),
        ),
    )
    inserted = cur.fetchone() is not None
    conn.commit()
    cur.close()
    conn.close()
    return inserted


def make_client_order_id(symbol: str, strategy: str, interval: str, side: str, candle_open_time, *, pos_id: int, tag: str) -> str:
    return build_live_client_order_id(symbol, pos_id, tag)


def execute_and_record(
    side: str,
    price: float,
    qty_btc: float,
    reason: str,
    candle_open_time,
    *,
    is_exit: bool,
    cfg_used: RuntimeConfig,
    allow_live_orders: bool,
    allow_meta: dict,
    rsi_14: float | None,
    ema_21: float | None,
):
    """
    Guard-first (Model A jak RSI):
    1) Rezerwuj slot w DB (simulated_orders) -> idempotencja per candle + is_exit.
    2) Dopiero potem (opcjonalnie) LIVE.
    3) Ledger zostaje jako audyt, nawet jeśli LIVE suppressed/failed.

    Zwraca dict: ledger_ok/live_attempted/live_ok/blocked_reason/client_order_id/resp
    """
    inserted = insert_simulated_order(
        symbol=cfg_used.symbol,
        interval=cfg_used.interval,
        side=side,
        price=price,
        qty_btc=qty_btc,
        reason=reason,
        rsi_14=rsi_14,
        ema_21=ema_21,
        candle_open_time=candle_open_time,
        is_exit=is_exit,
    )

    if not inserted:
        emit_strategy_event(
            event_type="BLOCKED",
            decision=side,
            reason="DB_GUARD_DUPLICATE",
            price=price,
            candle_open_time=candle_open_time,
            info={"is_exit": bool(is_exit), "qty_btc": float(qty_btc), "reason_text": reason},
        )
        return {
            "ledger_ok": False,
            "live_attempted": False,
            "live_ok": False,
            "blocked_reason": "DB_GUARD_DUPLICATE",
            "client_order_id": None,
            "resp": None,
        }

    emit_strategy_event(
        event_type="SIM_ORDER_CREATED",
        decision=side,
        reason="LEDGER_OK",
        price=price,
        candle_open_time=candle_open_time,
        info={"is_exit": bool(is_exit), "qty_btc": float(qty_btc), "reason_text": reason},
    )

    # PAPER => traktujemy jako wykonane
    if cfg_used.trading_mode != "LIVE":
        return {
            "ledger_ok": True,
            "live_attempted": False,
            "live_ok": True,
            "blocked_reason": None,
            "client_order_id": None,
            "resp": None,
        }

    if not allow_live_orders:
        logging.warning(
            "LIVE ORDER NOT ATTEMPTED (live disabled/policy) symbol=%s side=%s qty=%.8f is_exit=%s why=%s",
            cfg_used.symbol, side, float(qty_btc), bool(is_exit), (allow_meta or {}).get("why")
        )

        reason_code = "LIVE_EXIT_NOT_ATTEMPTED" if is_exit else "LIVE_ENTRY_NOT_ATTEMPTED"

        emit_strategy_event(
            event_type="BLOCKED",
            decision=side,
            reason=reason_code,
            price=price,
            candle_open_time=candle_open_time,
            info={
                "allow_meta": allow_meta,
                "is_exit": bool(is_exit),
                "reason_text": reason,
                # diagnostyka wewnętrzna (opcjonalnie)
                "blocked_reason": "LIVE_ORDER_SUPPRESSED",
            },
        )
        return {
            "ledger_ok": True,
            "live_attempted": False,
            "live_ok": False,
            "blocked_reason": "LIVE_ORDER_SUPPRESSED",  # zostaje w res jako diagnostyka
            "client_order_id": None,
            "resp": None,
        }
    
    side_u = str(side).upper()

    pos_id = None
    client_order_id = None

    if not is_exit:
        # ENTRY: utwórz pozycję aby mieć pos_id -> potem CID zawiera pos_id
        pos_id = open_position(
            side="LONG",
            qty=float(qty_btc),
            entry_price=float(price),
            entry_client_order_id=None,
        )
        if pos_id is None:
            return {
                "ledger_ok": True,
                "live_attempted": False,
                "live_ok": False,
                "blocked_reason": "ALREADY_OPEN",
                "client_order_id": None,
                "resp": None,
            }

        client_order_id = make_client_order_id(
            cfg_used.symbol, STRATEGY_NAME, cfg_used.interval, side, candle_open_time, pos_id=pos_id, tag="E"
        )
        # --- DB: pre-attach client_order_id (single-conn) ---
        conn_exec = get_db_conn()
        cur_exec = conn_exec.cursor()
        try:
            if pos_id:
                if is_exit:
                    attach_exit_order_id_with_conn(cur_exec, int(pos_id), None, client_order_id)
                else:
                    attach_entry_order_id_with_conn(cur_exec, int(pos_id), None, client_order_id)
            conn_exec.commit()
        except Exception:
            conn_exec.rollback()
            logging.exception("BBRANGE: pre-attach client_order_id failed pos_id=%s", pos_id)
        finally:
            cur_exec.close()
            conn_exec.close()

    else:
        # EXIT: użyj istniejącej OPEN pozycji
        open_row = get_open_position()
        pos_id = int(open_row[0]) if open_row else None
        if not pos_id:
            emit_strategy_event(
                event_type="BLOCKED",
                decision=side,
                reason="EXIT_NO_OPEN_POSITION",
                price=price,
                candle_open_time=candle_open_time,
                info={"is_exit": True},
            )
            return {
                "ledger_ok": True,
                "live_attempted": False,
                "live_ok": False,
                "blocked_reason": "EXIT_NO_OPEN_POSITION",
                "client_order_id": None,
                "resp": None,
            }

        client_order_id = make_client_order_id(
            cfg_used.symbol, STRATEGY_NAME, cfg_used.interval, side, candle_open_time, pos_id=pos_id, tag="X"
        )
        # --- DB: pre-attach client_order_id (single-conn) ---
        conn_exec = get_db_conn()
        cur_exec = conn_exec.cursor()
        try:
            if pos_id:
                if is_exit:
                    attach_exit_order_id_with_conn(cur_exec, int(pos_id), None, client_order_id)
                else:
                    attach_entry_order_id_with_conn(cur_exec, int(pos_id), None, client_order_id)
            conn_exec.commit()
        except Exception:
            conn_exec.rollback()
            logging.exception("BBRANGE: pre-attach client_order_id failed pos_id=%s", pos_id)
        finally:
            cur_exec.close()
            conn_exec.close()

    resp = place_live_order(
        client,
        cfg_used.symbol,
        side,
        qty_btc,
        trading_mode=cfg_used.trading_mode,
        live_orders_enabled=(cfg_used.live_orders_enabled or is_exit),
        quote_asset=cfg_used.quote_asset,
        client_order_id=client_order_id,
        panic_disable_trading=(os.environ.get("PANIC_DISABLE_TRADING", "0") == "1"),
        live_max_notional=float(os.environ.get("LIVE_MAX_NOTIONAL", "0")),
        skip_balance_precheck=is_exit,
    )

    if not resp or not resp.get("ok"):
        emit_strategy_event(
            event_type="BLOCKED",
            decision=side,
            reason="LIVE_ORDER_FAILED",
            price=price,
            candle_open_time=candle_open_time,
            info={"is_exit": bool(is_exit), "client_order_id": client_order_id, "resp": (resp or {}).get("resp")},
        )
        return {
            "ledger_ok": True,
            "live_attempted": True,
            "live_ok": False,
            "blocked_reason": "LIVE_ORDER_FAILED",
            "client_order_id": client_order_id,
            "resp": (resp or {}).get("resp"),
        }

    # Wyznacz live_ok bezpiecznie
    live_ok = resp.get("live_ok")
    raw = (resp or {}).get("resp") or {}
    if live_ok is None:
        status = str(raw.get("status", "")).upper()
        executed = raw.get("executedQty")
        try:
            executed_f = float(executed) if executed is not None else 0.0
        except Exception:
            executed_f = 0.0
        live_ok = executed_f > 0.0 or status == "FILLED"
    live_ok = bool(live_ok)

    status_raw = str(raw.get("status", "")).upper()
    executed_raw = raw.get("executedQty")
    try:
        executed_f = float(executed_raw) if executed_raw is not None else 0.0
    except Exception:
        executed_f = 0.0

    emit_strategy_event(
        event_type="LIVE_ORDER_SENT",
        decision=side,
        reason="OK" if live_ok else "ACK_NO_FILL",
        price=price,
        candle_open_time=candle_open_time,
        info={
            "is_exit": bool(is_exit),
            "client_order_id": client_order_id,
            "live_ok": bool(live_ok),
            "status": status_raw,
            "executed_qty": executed_f,
            "resp": raw,
        },
    )

    order_id = raw.get("orderId")

    if pos_id:
        conn_exec = get_db_conn()
        cur_exec = conn_exec.cursor()
        try:
            if is_exit:
                attach_exit_order_id_with_conn(cur_exec, int(pos_id), str(order_id) if order_id else None, client_order_id)
            else:
                attach_entry_order_id_with_conn(cur_exec, int(pos_id), str(order_id) if order_id else None, client_order_id)
            conn_exec.commit()
        except Exception:
            conn_exec.rollback()
            logging.exception("BBRANGE: attach order ids failed pos_id=%s is_exit=%s order_id=%s", pos_id, bool(is_exit), order_id)
        finally:
            cur_exec.close()
            conn_exec.close()
    else:
        logging.error("BBRANGE: LIVE ACK missing orderId pos_id=%s is_exit=%s resp=%s", pos_id, bool(is_exit), raw)

    return {
        "ledger_ok": True,
        "live_attempted": True,
        "live_ok": live_ok,
        "blocked_reason": None if live_ok else "ACK_NO_FILL",
        "client_order_id": client_order_id,
        "resp": (resp or {}).get("resp"),
    }


# =========================
# DAILY PNL (paper ledger)
# =========================

def compute_daily_pnl_pct(symbol: str, interval: str, current_price: float) -> float:
    # Legacy sim-ledger daily pnl is ONLY meaningful for LIVE shadow.
    # In PAPER we use positions-based SSOT and do not compute this.
    if cfg.trading_mode != "LIVE":
        return 0.0
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

    cash = PAPER_START_USDC
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
        rows.append(
            (
                SYMBOL,
                INTERVAL,
                datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc),
                k[1], k[2], k[3], k[4],
                k[5],
                datetime.fromtimestamp(k[6] / 1000, tz=timezone.utc),
                k[8],
            )
        )
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
        ORDER BY open_time DESC
        LIMIT 2000;
        """,
        conn,
        params=(SYMBOL, INTERVAL),
    )

    if df.empty:
        conn.close()
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

    last = df.tail(60)

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

def get_last_closed_candle():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT open_time, open, high, low, close, ema_21, rsi_14
        FROM candles
        WHERE symbol=%s AND interval=%s
        ORDER BY open_time DESC
        OFFSET 1
        LIMIT 1
        """,
        (SYMBOL, INTERVAL),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

# =========================
# RUNTIME SNAPSHOT 
# =========================

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

    panic = (os.environ.get("PANIC_DISABLE_TRADING", "0") == "1")

    gate = decide_regime_gate(
        symbol=SYMBOL,
        interval=INTERVAL,
        strategy=STRATEGY_NAME,
        decision="ENTRY_CHECK",
        regime_enabled=bc.regime_enabled,
        regime_mode=bc.regime_mode,
    )

    allow_gate_entry = bool(gate.allow)

    rmeta_gate = {
        "enabled": bool(bc.regime_enabled),
        "mode": bc.regime_mode,
        "regime": gate.regime,
        "would_block": bool(gate.would_block) if gate.would_block is not None else None,
        "why": gate.why,
        "meta": gate.meta,
    }

    allowed_orders_entry, allow_meta_entry = can_trade(
        cfg_effective,
        regime_allows_trade=allow_gate_entry,
        is_exit=False,
        panic_disable_trading=panic,
    )
    allowed_orders_exit, allow_meta_exit = can_trade(
        cfg_effective,
        regime_allows_trade=True,
        is_exit=True,
        panic_disable_trading=panic,
    )

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

        "allow_entry_gate": bool(allow_gate_entry),
        "allow_live_orders_entry": bool(allowed_orders_entry),
        "allow_live_orders_exit": bool(allowed_orders_exit),
        "allow_meta_entry": allow_meta_entry,
        "allow_meta_exit": allow_meta_exit,

        "symbol": cfg_effective.symbol,
        "interval": cfg_effective.interval,
        "strategy": STRATEGY_NAME,
        "quote_asset": cfg_effective.quote_asset,
        "spot_mode": bool(cfg_effective.spot_mode),
        "bot_version": os.environ.get("BOT_VERSION"),
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
    

# =========================
# BBRANGE LOGIC (SPOT LONG ONLY)
# =========================

def get_trend(close: float, ema21: float, buffer_pct: float = TREND_BUFFER) -> str:
    if close > ema21 * (1.0 + buffer_pct):
        return "UP"
    elif close < ema21 * (1.0 - buffer_pct):
        return "DOWN"
    return "FLAT"

def run_strategy(row):
    open_time = (row[0] if row else None)
    price_for_events = float(row[4]) if row and row[4] is not None else None

    emit_strategy_event(
        event_type="RUN_START",
        decision=None,
        reason="ENTER",
        price=price_for_events,
        candle_open_time=open_time,
        info={"has_row": bool(row), "bot_version": os.environ.get("BOT_VERSION")},
    )
    try:
        if not row:
            emit_blocked(reason="NO_ROW", decision=None, price=None, candle_open_time=None)
            return

        open_time, open_px, high_px, low_px, close_px, ema_21, rsi_14 = row
        price = float(close_px) if close_px is not None else None
        if price is None:
            emit_blocked(reason="CANDLE_MISSING_CLOSE", decision=None, price=None, candle_open_time=open_time)
            return

        if ema_21 is None or rsi_14 is None:
            emit_blocked(
                reason="INDICATORS_NOT_READY",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"ema_21": ema_21, "rsi_14": rsi_14},
            )
            return

        ema_val = float(ema_21)
        rsi_val = float(rsi_14)

        snap = get_runtime_snapshot(price=price, open_time=open_time)
        bc = snap["bc"]
        # Telemetry baseline per candle: zawsze zapisujemy gate status (tak jak TREND)
        emit_regime_gate_event(
            symbol=SYMBOL,
            interval=INTERVAL,
            strategy=STRATEGY_NAME,
            decision="TICK",
            d=decide_regime_gate(
                symbol=SYMBOL,
                interval=INTERVAL,
                strategy=STRATEGY_NAME,
                decision="TICK",
                regime_enabled=bc.regime_enabled,
                regime_mode=bc.regime_mode,
            ),
        )
        
        cfg_effective = snap["cfg_effective"]
        time_exit_enabled = True
        max_pos_minutes = int(MAX_POSITION_MINUTES)

        # heartbeat always
        pos = get_open_position()
        heartbeat({
            **snap["heartbeat"],
            "ema_21": float(ema_val),
            "rsi_14": float(rsi_val),
            "has_position": bool(pos is not None),
            "pos_side": (str(pos[1]).upper() if pos else None),
            "pos_qty": (float(pos[2]) if pos else None),
            "pos_entry_price": (float(pos[3]) if pos else None),
            "order_notional_usdc": float(ORDER_NOTIONAL_USDC),
            "min_qty_btc": float(MIN_QTY_BTC),
            "qty_step_btc": float(QTY_STEP_BTC),
            "min_notional_buffer_pct": float(MIN_NOTIONAL_BUFFER_PCT),
        })

        # hard stop
        if bc.mode == "HALT":
            emit_blocked(reason="BOT_MODE_HALT", decision=None, price=price, candle_open_time=open_time, info={})
            return

        # PANIC: close if open + halt
        if bc.mode == "PANIC":
            if pos:
                _pos_id, pos_side, pos_qty, _ep, _et = pos
                side_u = str(pos_side).upper()
                if side_u != "LONG":
                    emit_strategy_event(
                        event_type="ERROR",
                        decision=None,
                        reason="PANIC_SHORT_IN_SPOT",
                        price=price,
                        candle_open_time=open_time,
                        info={"pos_side": side_u},
                    )
                    set_mode("HALT", reason="SHORT found in SPOT mode")
                    return

                res = execute_and_record(
                    side="SELL",
                    price=price,
                    qty_btc=float(pos_qty),
                    reason="PANIC CLOSE",
                    candle_open_time=open_time,
                    is_exit=True,
                    cfg_used=cfg_effective,
                    allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],
                    rsi_14=rsi_val,
                    ema_21=ema_val,
                )
                if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                    close_position(exit_price=price, reason="PANIC", candle_open_time=open_time)
            set_mode("HALT", reason="Panic executed; halting.")
            return

        # =========================
        # EXIT (only LONG exists)
        # =========================
        if pos:
            _pos_id, pos_side, pos_qty, pos_entry_price, pos_entry_time = pos
            side_u = str(pos_side).upper()
            qty_f = float(pos_qty)
            entry_f = float(pos_entry_price)

            if side_u != "LONG":
                emit_strategy_event(
                    event_type="ERROR",
                    decision=None,
                    reason="SHORT_POSITION_IN_SPOT",
                    price=price,
                    candle_open_time=open_time,
                    info={"pos_side": side_u},
                )
                set_mode("HALT", reason="SHORT found in SPOT mode")
                return

            # TP/SL intrabar based on high/low
            high_price = float(high_px) if high_px is not None else price
            low_price = float(low_px) if low_px is not None else price

            tp_level = entry_f * (1.0 + TAKE_PROFIT_PCT / 100.0)
            sl_level = entry_f * (1.0 - STOP_LOSS_PCT / 100.0)

            if TAKE_PROFIT_PCT > 0 and high_price >= tp_level:
                reason = f"BBRANGE TAKE PROFIT LONG intrabar high={high_price:.2f} >= tp={tp_level:.2f}"
                res = execute_and_record(
                    side="SELL",
                    price=price,
                    qty_btc=qty_f,
                    reason=reason,
                    candle_open_time=open_time,
                    is_exit=True,
                    cfg_used=cfg_effective,
                    allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],
                    rsi_14=rsi_val,
                    ema_21=ema_val,
                )
                if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                    close_position(exit_price=price, reason="TAKE_PROFIT", candle_open_time=open_time)
                else:
                    emit_blocked(
                        reason="EXIT_BLOCKED",
                        decision="SELL",
                        price=price,
                        candle_open_time=open_time,
                        info={"res": res},
                    )
                return

            if STOP_LOSS_PCT > 0 and low_price <= sl_level:
                reason = f"BBRANGE STOP LOSS LONG intrabar low={low_price:.2f} <= sl={sl_level:.2f}"
                res = execute_and_record(
                    side="SELL",
                    price=sl_level if cfg_effective.trading_mode == "PAPER" else price,
                    qty_btc=qty_f,
                    reason=reason,
                    candle_open_time=open_time,
                    is_exit=True,
                    cfg_used=cfg_effective,
                    allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],
                    rsi_14=rsi_val,
                    ema_21=ema_val,
                )
                if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                    close_position(exit_price=price, reason="STOP_LOSS", candle_open_time=open_time)
                else:
                    emit_blocked(
                        reason="EXIT_BLOCKED",
                        decision="SELL",
                        price=price,
                        candle_open_time=open_time,
                        info={"res": res},
                    )
                return

            # TIME EXIT
            if time_exit_enabled and max_pos_minutes > 0 and pos_entry_time is not None:
                if pos_entry_time.tzinfo is None:
                    pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)
                age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0
                if age_minutes >= max_pos_minutes:
                    emit_strategy_event(
                        event_type="EXIT_TIME",
                        decision="SELL",
                        reason="TIME_EXIT",
                        price=price,
                        candle_open_time=open_time,
                        info={
                            "pos_side": "LONG",
                            "age_minutes": float(age_minutes),
                            "max_minutes": int(max_pos_minutes),
                        },
                    )
                    reason = f"BBRANGE TIME_EXIT LONG {age_minutes:.1f}m >= {max_pos_minutes}m"
                    res = execute_and_record(
                        side="SELL",
                        price=price,
                        qty_btc=qty_f,
                        reason=reason,
                        candle_open_time=open_time,
                        is_exit=True,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        rsi_14=rsi_val,
                        ema_21=ema_val,
                    )
                    if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                        close_position(exit_price=price, reason="TIME_EXIT", candle_open_time=open_time)
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision="SELL",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return

            emit_blocked(reason="POSITION_OPEN_NO_EXIT", decision=None, price=price, candle_open_time=open_time, info={"pos_side": "LONG"})
            return

        # =========================
        # ENTRY (SPOT LONG ONLY)
        # =========================

        # disable hours
        if open_time.hour in DISABLE_HOURS_SET:
            emit_blocked(
                reason="DISABLE_HOURS",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"hour_utc": int(open_time.hour), "disable_hours": sorted(list(DISABLE_HOURS_SET))},
            )
            return

        if not bc.enabled:
            emit_blocked(reason="BOT_DISABLED", decision=None, price=price, candle_open_time=open_time, info={})
            return

        # Daily loss gate — SSOT = positions.
        # PAPER: only positions shadow telemetry (no legacy sim shadow, no block).
        # LIVE: positions shadow telemetry + optional legacy sim shadow + hard block by positions.
        if DAILY_MAX_LOSS_PCT > 0:
            pos_payload = compute_daily_loss_pct_positions(
                SYMBOL, INTERVAL, STRATEGY_NAME,
                base_usdc=float(DAILY_MAX_LOSS_BASE_USDC),
            )

            conn = get_db_conn()
            try:
                if should_emit_daily_loss_shadow(strategy=STRATEGY_NAME):
                    # Always emit positions shadow telemetry (throttled)
                    emit_alert_throttled(
                        conn=conn,
                        symbol=SYMBOL,
                        interval=INTERVAL,
                        strategy=STRATEGY_NAME,
                        reason="DAILY_MAX_LOSS_POSITIONS_SHADOW",
                        open_time=open_time,
                        price=price,
                        info={**pos_payload, "limit_pct": float(DAILY_MAX_LOSS_PCT)},
                    )

                # Legacy sim-ledger shadow: LIVE only (optional)
                #if cfg_effective.trading_mode == "LIVE":
                #    daily_pct = compute_daily_pnl_pct(symbol=SYMBOL, interval=INTERVAL, current_price=price)
                #    if daily_pct <= -DAILY_MAX_LOSS_PCT:
                #        emit_alert_throttled(
                #            conn=conn,
                #            symbol=SYMBOL,
                #            interval=INTERVAL,
                #            strategy=STRATEGY_NAME,
                #            reason="DAILY_MAX_LOSS_SHADOW",
                #            open_time=open_time,
                #            price=price,
                #            info={"daily_pct": float(daily_pct), "limit_pct": float(DAILY_MAX_LOSS_PCT)},
                #        )
            finally:
                conn.close()

            # LIVE hard block only by positions-based DML
            if cfg_effective.trading_mode == "LIVE":
                if should_block_daily_loss_positions(
                    daily_pct=float(pos_payload["daily_pct"]),
                    limit_pct=float(DAILY_MAX_LOSS_PCT),
                ):
                    emit_strategy_event(
                        event_type="BLOCKED",
                        decision=None,
                        reason="DAILY_MAX_LOSS_POSITIONS",
                        price=price,
                        candle_open_time=open_time,
                        info={**pos_payload, "limit_pct": float(DAILY_MAX_LOSS_PCT)},
                    )
                    return

        # Build BB on recent closes (need at least BB_PERIOD)
        conn = get_db_conn()
        df = pd.read_sql_query(
            """
            SELECT open_time, close
            FROM candles
            WHERE symbol=%s AND interval=%s AND open_time <= %s
            ORDER BY open_time DESC
            LIMIT %s
            """,
            conn,
            params=(SYMBOL, INTERVAL, open_time, max(BB_PERIOD + 30, 120)),
        )
        conn.close()

        if df.empty or len(df) < BB_PERIOD + 5:
            emit_blocked(reason="NOT_ENOUGH_CANDLES", decision=None, price=price, candle_open_time=open_time, info={"have": int(len(df))})
            return

        df = df.sort_values("open_time")
        closes = df["close"].astype(float)
        mid = closes.rolling(window=BB_PERIOD).mean()
        std = closes.rolling(window=BB_PERIOD).std()
        bb_mid = float(mid.iloc[-1]) if not pd.isna(mid.iloc[-1]) else None
        bb_upper = float((mid + BB_STD * std).iloc[-1]) if not pd.isna((mid + BB_STD * std).iloc[-1]) else None
        bb_lower = float((mid - BB_STD * std).iloc[-1]) if not pd.isna((mid - BB_STD * std).iloc[-1]) else None

        if bb_mid is None or bb_upper is None or bb_lower is None or bb_mid == 0:
            emit_blocked(reason="BB_NOT_READY", decision=None, price=price, candle_open_time=open_time, info={})
            return

        bb_width_pct = (bb_upper - bb_lower) / bb_mid
        trend = get_trend(price, ema_val)

        if bb_width_pct < MIN_BB_WIDTH_PCT:
            emit_blocked(
                reason="BB_WIDTH_TOO_LOW",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"bb_width_pct": float(bb_width_pct), "min": float(MIN_BB_WIDTH_PCT)},
            )
            return

        # BBRANGE entries only in FLAT trend
        if trend != "FLAT":
            emit_blocked(reason="TREND_NOT_FLAT", decision=None, price=price, candle_open_time=open_time, info={"trend": trend})
            return

        # Long entry signal only: price < lower
        if price >= bb_lower:
            emit_blocked(reason="NO_SIGNAL", decision=None, price=price, candle_open_time=open_time, info={"bb_lower": bb_lower})
            return

        # RSI filters
        if rsi_val <= RSI_BLOCK_EXTREME_LOW or rsi_val >= RSI_BLOCK_EXTREME_HIGH:
            emit_blocked(
                reason="RSI_EXTREME_BLOCK",
                decision="BUY",
                price=price,
                candle_open_time=open_time,
                info={"rsi": float(rsi_val), "low": float(RSI_BLOCK_EXTREME_LOW), "high": float(RSI_BLOCK_EXTREME_HIGH)},
            )
            return
        if rsi_val > RSI_LONG_MAX:
            emit_blocked(
                reason="RSI_LONG_MAX_BLOCK",
                decision="BUY",
                price=price,
                candle_open_time=open_time,
                info={"rsi": float(rsi_val), "rsi_long_max": float(RSI_LONG_MAX)},
            )
            return

        decision = "BUY"
        reason = f"BBRANGE LONG: price {price:.2f} < lower {bb_lower:.2f} (trend=FLAT)"

        # SPOT short block (defensive; here decision is BUY anyway)
        if decision == "SELL" and cfg_effective.spot_mode:
            emit_blocked(reason="SPOT_SHORT_BLOCK", decision="SELL", price=price, candle_open_time=open_time, info={"spot_mode": True})
            return

        # REGIME gate (ENTRY only) — standard: ENTRY_CHECK
        gate_entry = decide_regime_gate(
            symbol=SYMBOL,
            interval=INTERVAL,
            strategy=STRATEGY_NAME,
            decision="ENTRY_CHECK",
            regime_enabled=bc.regime_enabled,
            regime_mode=bc.regime_mode,
        )

        emit_regime_gate_event(
            symbol=SYMBOL,
            interval=INTERVAL,
            strategy=STRATEGY_NAME,
            decision="ENTRY_CHECK",
            d=gate_entry,
        )

        if not gate_entry.allow:
            emit_blocked(
                reason="REGIME_BLOCK",
                decision=decision,
                price=price,
                candle_open_time=open_time,
                info={"why": gate_entry.why, "regime": gate_entry.regime, "meta": gate_entry.meta},
            )
            return

        emit_strategy_event(
            event_type="SIGNAL",
            decision=decision,
            reason="OK",
            price=price,
            candle_open_time=open_time,
            info={"reason_text": reason, "bb_lower": bb_lower, "bb_mid": bb_mid, "bb_upper": bb_upper, "bb_width_pct": bb_width_pct},
        )
            
        qty_btc, sizing_info = compute_qty_from_notional(
            client,
            symbol=SYMBOL,
            px=price,                 # albo price / close_price - ten sam px, którego używasz do ordera
            target_notional=ORDER_NOTIONAL_USDC,
            min_notional_buffer_pct=MIN_NOTIONAL_BUFFER_PCT,
        )

        emit_strategy_event(
            event_type="SIZING",
            decision="BUY",
            reason="NOTIONAL",
            price=float(price),
            candle_open_time=open_time,
            info=sizing_info,
        )

        settings_snapshot = get_user_settings_snapshot()
        manual_entry_addon_usdc = float(settings_snapshot.get("manual_entry_addon_usdc", 0.0) or 0.0)
        base_target_notional = float(ORDER_NOTIONAL_USDC)
        final_target_notional = base_target_notional + manual_entry_addon_usdc

        if manual_entry_addon_usdc > 0:
            qty_btc, sizing_info = compute_qty_from_notional(
                client,
                symbol=SYMBOL,
                px=price,
                target_notional=final_target_notional,
                min_notional_buffer_pct=MIN_NOTIONAL_BUFFER_PCT,
            )

        order_notional_usdc = float(qty_btc) * float(price)
        emit_strategy_event(
            event_type="SIZING",
            decision="BUY",
            reason="FINAL_NOTIONAL",
            price=float(price),
            candle_open_time=open_time,
            info={
                **sizing_info,
                "base_target_notional": base_target_notional,
                "manual_entry_addon_usdc": manual_entry_addon_usdc,
                "configured_three_win_boost_usdc": float(settings_snapshot.get("three_win_boost_usdc", 10.0) or 10.0),
                "three_win_boost_active": base_target_notional > float(SYSTEM_MIN_ENTRY_USDC),
                "final_target_notional": float(final_target_notional),
            },
        )

        if qty_btc <= 0:
            emit_strategy_event(
                event_type="BLOCKED",
                decision="BUY",
                reason="SIZING_QTY_ZERO",
                price=float(price),
                candle_open_time=open_time,
                info=sizing_info,
            )
            return

        res = execute_and_record(
            side="BUY",
            price=price,
            qty_btc=float(qty_btc),
            reason=reason,
            candle_open_time=open_time,
            is_exit=False,
            cfg_used=cfg_effective,
            allow_live_orders=snap["allowed_orders_entry"],
            allow_meta=snap["allow_meta_entry"],
            rsi_14=rsi_val,
            ema_21=ema_val,
        )
        if not res["ledger_ok"]:
            logging.info("BBRANGE: entry blocked/failed -> not opening position.")
            return

        if cfg_effective.trading_mode == "LIVE" and not res["live_ok"]:
            # NOT_ATTEMPTED jest już emitowane w execute_and_record() (SSOT)
            if not res.get("live_attempted", False):
                return

            # attempted, ale brak fill -> logujemy tutaj
            emit_strategy_event(
                event_type="BLOCKED",
                decision=decision,
                reason="LIVE_ENTRY_NOT_FILLED",
                price=price,
                candle_open_time=open_time,
                info={"res": res},
            )
            return

        # Position OPEN is created inside execute_and_record() (SSOT).
        emit_strategy_event(
            event_type="POSITION_OPENED",
            decision="BUY",
            reason="SSOT_EXECUTE_AND_RECORD",
            price=price,
            candle_open_time=open_time,
            info={"qty_btc": float(qty_btc)},
        )
        return

    finally:
        emit_strategy_event(
            event_type="RUN_END",
            decision=None,
            reason="DONE",
            price=price_for_events,
            candle_open_time=open_time,
            info={},
        )
    

# =========================
# MAIN LOOP
# =========================

LAST_PROCESSED_OPEN_TIME = None

def main_loop():
    global LAST_PROCESSED_OPEN_TIME

    ensure_schema()
    upsert_defaults(SYMBOL, STRATEGY_NAME, INTERVAL)

    conn = get_db_conn()
    try:
        seed_default_params_from_env(conn)
        last_ingest_ts = 0.0
    finally:
        conn.close()

    while True:
        loop_start = time.perf_counter()
        try:
            # --- Binance fills ingest (LIVE ONLY) ---
            # co 60s: pobierz myTrades i zasil binance_order_fills + wyceń fee w USDC przez BNBUSDC candles
            if binance_mytrades_enabled() and (time.time() - last_ingest_ts >= 60):
                n_trades, n_priced = ingest_my_trades(
                    client=client,
                    symbols=[SYMBOL],
                    db_host=DB_HOST,
                    db_port=DB_PORT,
                    db_name=DB_NAME,
                    db_user=DB_USER,
                    db_pass=DB_PASS,
                    lookback_ms_default=7 * 24 * 3600 * 1000,
                )
                last_ingest_ts = time.time()

                emit_strategy_event(
                    event_type="INGEST",
                    decision=None,
                    reason="BINANCE_MYTRADES",
                    price=None,
                    candle_open_time=None,
                    info={"symbol": SYMBOL, "n_trades": int(n_trades), "n_fee_priced": int(n_priced)},
                )
            else:
                pass
            load_runtime_params()

            rows = fetch_klines(limit=200)
            save_klines(rows)
            update_indicators()

            latest = get_last_closed_candle()
            if latest:
                open_time = latest[0]
                price = float(latest[4]) if latest[4] is not None else None

                emit_strategy_event(
                    event_type="TICK",
                    decision=None,
                    reason="LOOP",
                    price=price,
                    candle_open_time=open_time,
                    info={"last_processed": str(LAST_PROCESSED_OPEN_TIME), "open_time": str(open_time)},
                )

                if LAST_PROCESSED_OPEN_TIME != open_time:
                    LAST_PROCESSED_OPEN_TIME = open_time
                    run_strategy(latest)
                else:
                    emit_strategy_event(
                        event_type="IDLE",
                        decision=None,
                        reason="NO_NEW_CANDLE",
                        price=float(latest[4]) if latest[4] is not None else None,
                        candle_open_time=open_time,
                        info={"open_time": str(open_time), "last_processed": str(LAST_PROCESSED_OPEN_TIME)},
                    )
                    logging.info("BBRANGE: no new candle yet (%s) -> skip strategy.", str(open_time))

        except Exception as e:
            logging.exception("BBRANGE loop error")
            emit_strategy_event(event_type="ERROR", decision=None, reason="EXCEPTION", price=None, candle_open_time=None, info={"error": str(e)})

        logging.info("BBRANGE loop finished in %.3f s", time.perf_counter() - loop_start)
        time.sleep(60)

if __name__ == "__main__":
    logging.info("Starting BBRANGE bot %s %s", SYMBOL, INTERVAL)
    main_loop()
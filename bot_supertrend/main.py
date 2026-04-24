# main_supertrend.py
import os
import time
import json
import logging
import hashlib
from decimal import Decimal, ROUND_DOWN
from dataclasses import replace
from datetime import datetime, timezone, date
from common.flags import binance_mytrades_enabled
from common.execution import place_live_exit_maker_then_market
from common.daily_loss import should_emit_daily_loss_shadow
from common.alerts import emit_alert_throttled
from common.binance_ingest_trades import ingest_my_trades
from common.execution import build_live_client_order_id, build_live_entry_intent_client_order_id
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from binance.client import Client
from common.db import get_db_conn
from common.schema import ensure_schema
from common.bot_control import upsert_defaults, read as read_bot_control
from common.runtime import RuntimeConfig
from common.permissions import can_trade
from common.regime_gate import decide_regime_gate, emit_regime_gate_event
from common.execution import place_live_order
from common.sizing import compute_qty_from_notional as common_compute_qty_from_notional
from common.daily_loss import compute_daily_loss_pct_positions, should_block_daily_loss_positions
from common.user_settings import SYSTEM_MIN_ENTRY_USDC, get_user_settings_snapshot


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# =========================
# ENV / Runtime
# =========================

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "trading")
DB_USER = os.environ.get("DB_USER", "botuser")
DB_PASS = os.environ.get("DB_PASS", "botpass")

SYMBOL = os.environ.get("SYMBOL", "BTCUSDC")
INTERVAL = os.environ.get("INTERVAL", "1m")
STRATEGY_NAME = os.environ.get("STRATEGY_NAME", "SUPERTREND").upper()

QUOTE_ASSET = os.environ.get("QUOTE_ASSET", "USDC").upper()
if not SYMBOL.endswith(QUOTE_ASSET):
    raise RuntimeError(f"SYMBOL={SYMBOL} does not match QUOTE_ASSET={QUOTE_ASSET}")

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

cfg = RuntimeConfig.from_env()
client = Client(api_key=API_KEY, api_secret=API_SECRET) if cfg.trading_mode == "LIVE" else Client()

# =========================
# Strategy Params (defaults)
# =========================
ATR_PERIOD = int(os.environ.get("ATR_PERIOD", "14"))
ST_MULTIPLIER = float(os.environ.get("ST_MULTIPLIER", "3.0"))

EMA_PERIOD = int(os.environ.get("EMA_PERIOD", "21"))
RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))

# ATR% minimum volatility gate (percent, e.g. 0.25 = 0.25%)
MIN_ATR_PCT = float(os.environ.get("MIN_ATR_PCT", "0.25"))

STOP_LOSS_PCT = float(os.environ.get("STOP_LOSS_PCT", "0.8"))      # %
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "1.2"))  # %

MAX_POSITION_MINUTES = int(os.environ.get("MAX_POSITION_MINUTES", "90"))

# Daily loss gate on PAPER ledger (if <=0 -> disabled)
DAILY_MAX_LOSS_PCT = float(os.environ.get("DAILY_MAX_LOSS_PCT", "0.5"))
PAPER_START_USDC = float(os.environ.get("PAPER_START_USDC", "100"))
DAILY_MAX_LOSS_BASE_USDC = float(os.environ.get("DAILY_MAX_LOSS_BASE_USDC", str(PAPER_START_USDC)))

# Trade size (BTC qty for BTCUSDC spot market BUY/SELL)
ORDER_QTY_BTC = float(os.environ.get("ORDER_QTY_BTC", "0.0001"))

# Optional: exit on SuperTrend flip back down (recommended)
EXIT_ON_FLIP_DOWN = os.environ.get("EXIT_ON_FLIP_DOWN", "1") == "1"

# Optional: disable entries on certain UTC hours (comma separated)
DISABLE_HOURS = os.environ.get("DISABLE_HOURS", "")
DISABLE_HOURS_SET = {int(h.strip()) for h in DISABLE_HOURS.split(",") if h.strip() != ""}

# Regime freshness
LIVE_TARGET_NOTIONAL = float(os.environ.get("LIVE_TARGET_NOTIONAL", "6.0"))
MIN_NOTIONAL_BUFFER_PCT = float(os.environ.get("MIN_NOTIONAL_BUFFER_PCT", "0.05"))
_SYMBOL_FILTERS_CACHE = None

# =========================
# Helpers
# =========================
def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


def compute_qty_from_notional_safe(
    client,
    *,
    symbol: str,
    px: float,
    target_notional: float,
    min_notional_buffer_pct: float,
):
    """
    Adapter kompatybilny wstecz:
    - wspiera common.sizing.compute_qty_from_notional z różnymi sygnaturami
    - zawsze zwraca (qty_btc, sizing_info)
    """
    try:
        # wariant "nowy" (jak w BBRANGE): (client, symbol=..., px=..., target_notional=..., min_notional_buffer_pct=...)
        return common_compute_qty_from_notional(
            client,
            symbol=symbol,
            px=px,
            target_notional=target_notional,
            min_notional_buffer_pct=min_notional_buffer_pct,
        )
    except TypeError:
        # wariant "stary": (client, px=..., target_notional=..., min_notional_buffer_pct=...)
        return common_compute_qty_from_notional(
            client,
            px=px,
            target_notional=target_notional,
            min_notional_buffer_pct=min_notional_buffer_pct,
        )


def _get_symbol_filters():
    global _SYMBOL_FILTERS_CACHE
    if _SYMBOL_FILTERS_CACHE is not None:
        return _SYMBOL_FILTERS_CACHE

    info = client.get_symbol_info(SYMBOL)
    if not info:
        raise RuntimeError(f"Cannot fetch symbol_info for {SYMBOL}")

    lot = next((f for f in info["filters"] if f["filterType"] == "LOT_SIZE"), None)
    min_notional = next((f for f in info["filters"] if f["filterType"] in ("MIN_NOTIONAL", "NOTIONAL")), None)

    step = float(lot["stepSize"]) if lot else 0.0
    min_qty = float(lot["minQty"]) if lot else 0.0
    min_not = float(min_notional.get("minNotional", 0.0)) if min_notional else 0.0

    _SYMBOL_FILTERS_CACHE = {"step": step, "min_qty": min_qty, "min_notional": min_not}
    return _SYMBOL_FILTERS_CACHE

def _floor_to_step(qty: float, step: float) -> float:
    if step is None or step <= 0:
        return float(qty)
    q = Decimal(str(qty))
    s = Decimal(str(step))
    floored = (q / s).to_integral_value(rounding=ROUND_DOWN) * s
    return float(floored)


def make_client_order_id(symbol: str, strategy: str, interval: str, side: str, candle_open_time, *, pos_id: int, tag: str) -> str:
    return build_live_client_order_id(symbol, pos_id, tag)


def set_entry_client_order_id(pos_id: int, client_order_id: str) -> None:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE positions
        SET entry_client_order_id =
            CASE
              WHEN entry_client_order_id IS NULL OR entry_client_order_id = '' OR entry_client_order_id = 'PENDING'
                THEN %s
              ELSE entry_client_order_id
            END
        WHERE id = %s
        """,
        (str(client_order_id), int(pos_id)),
    )
    conn.commit()
    cur.close()
    conn.close()


def set_exit_client_order_id(pos_id: int, client_order_id: str) -> None:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE positions
        SET exit_client_order_id =
            CASE
              WHEN exit_client_order_id IS NULL OR exit_client_order_id = '' OR exit_client_order_id = 'PENDING'
                THEN %s
              ELSE exit_client_order_id
            END
        WHERE id = %s
        """,
        (str(client_order_id), int(pos_id)),
    )
    conn.commit()
    cur.close()
    conn.close()
# =========================
# Telemetry (strategy_events)
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

# =========================
# Heartbeat
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
# Regime Gate (entry only)
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
# Positions (hard-truth)
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


def open_position(side: str, qty: float, entry_price: float, entry_client_order_id: str | None) -> int | None:
    # SPOT-only: LONG only
    if str(side).upper() != "LONG":
        return None

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM positions
        WHERE symbol=%s AND strategy=%s AND interval=%s AND status='OPEN'
        ORDER BY entry_time DESC
        LIMIT 1
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL),
    )
    row = cur.fetchone()
    if row:
        cur.close()
        conn.close()
        logging.info("SUPERTREND: open_position skipped - position already OPEN (pos_id=%s).", int(row[0]))
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

    logging.info("SUPERTREND: position OPENED pos_id=%s side=LONG qty=%.8f entry=%.2f", pos_id, float(qty), float(entry_price))
    return pos_id


def open_position_from_live_ack(
    *,
    side: str,
    qty: float,
    entry_price: float,
    entry_client_order_id: str,
    entry_order_id: str,
) -> int | None:
    if str(side).upper() != "LONG":
        return None

    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id
        FROM positions
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
        logging.info("SUPERTREND: open_position_from_live_ack skipped - already OPEN pos_id=%s.", pos_id)
        return None

    cur.execute(
        """
        INSERT INTO positions(
          symbol, strategy, interval, status, side, qty, entry_price, entry_time,
          entry_client_order_id, entry_order_id
        )
        VALUES (%s, %s, %s, 'OPEN', %s, %s, %s, now(), %s, %s)
        RETURNING id;
        """,
        (
            SYMBOL, STRATEGY_NAME, INTERVAL,
            side, float(qty), float(entry_price),
            str(entry_client_order_id), str(entry_order_id),
        ),
    )
    pos_id = int(cur.fetchone()[0])
    conn.commit()
    cur.close()
    conn.close()
    logging.info("SUPERTREND: position OPENED FROM LIVE ACK pos_id=%s LONG qty=%.8f entry=%.2f", pos_id, float(qty), float(entry_price))
    return pos_id


def close_position(exit_price: float, reason: str,  candle_open_time) -> bool:
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
        logging.info("SUPERTREND: position CLOSED reason=%s exit=%.2f", reason, float(exit_price))
        emit_strategy_event(
            event_type="POSITION_CLOSED",
            decision=None,
            reason=reason,
            price=float(exit_price),
            candle_open_time= candle_open_time,
            info={"exit_reason": reason, "exit_price": float(exit_price)},
        )
    else:
        logging.info("SUPERTREND: close_position skipped - no OPEN position found.")
    return closed

# =========================
# Params in DB (strategy_params)
# =========================
def seed_default_params_from_env(conn):
    """
    Insert defaults into strategy_params if missing (per symbol/strategy/interval).
    """
    defaults = {
        "ATR_PERIOD": float(ATR_PERIOD),
        "ST_MULTIPLIER": float(ST_MULTIPLIER),
        "EMA_PERIOD": float(EMA_PERIOD),
        "RSI_PERIOD": float(RSI_PERIOD),
        "MIN_ATR_PCT": float(MIN_ATR_PCT),
        "STOP_LOSS_PCT": float(STOP_LOSS_PCT),
        "TAKE_PROFIT_PCT": float(TAKE_PROFIT_PCT),
        "MAX_POSITION_MINUTES": float(MAX_POSITION_MINUTES),
        "DAILY_MAX_LOSS_PCT": float(DAILY_MAX_LOSS_PCT),
        "ORDER_QTY_BTC": float(ORDER_QTY_BTC),
        "EXIT_ON_FLIP_DOWN": 1.0 if EXIT_ON_FLIP_DOWN else 0.0,
    }

    cur = conn.cursor()
    cur.execute(
        """
        SELECT param_name, param_value
        FROM strategy_params
        WHERE symbol=%s AND strategy=%s AND interval=%s
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL),
    )
    existing_rows = cur.fetchall()
    existing = {name for (name, _) in existing_rows}

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
        cur.execute(
            """
            INSERT INTO strategy_params_history
            (symbol, strategy, interval, param_name, old_value, new_value, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (SYMBOL, STRATEGY_NAME, INTERVAL, name, None, value, "MANUAL"),
        )
        inserted_any = True

    if inserted_any:
        conn.commit()
        logging.info("Seeded default SUPERTREND params from ENV for %s/%s/%s.", SYMBOL, STRATEGY_NAME, INTERVAL)
    else:
        logging.info("SUPERTREND params already exist in DB for %s/%s/%s.", SYMBOL, STRATEGY_NAME, INTERVAL)

    cur.close()

def load_runtime_params():
    global ATR_PERIOD, ST_MULTIPLIER, EMA_PERIOD, RSI_PERIOD
    global MIN_ATR_PCT, STOP_LOSS_PCT, TAKE_PROFIT_PCT
    global MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT, ORDER_QTY_BTC
    global EXIT_ON_FLIP_DOWN

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

    params = {name: float(value) for (name, value) in rows} if rows else {}

    def clamp(v, lo, hi):
        return max(lo, min(hi, v))

    if "ATR_PERIOD" in params:
        ATR_PERIOD = int(clamp(params["ATR_PERIOD"], 5, 100))
    if "ST_MULTIPLIER" in params:
        ST_MULTIPLIER = clamp(params["ST_MULTIPLIER"], 1.0, 10.0)

    if "EMA_PERIOD" in params:
        EMA_PERIOD = int(clamp(params["EMA_PERIOD"], 5, 200))
    if "RSI_PERIOD" in params:
        RSI_PERIOD = int(clamp(params["RSI_PERIOD"], 5, 100))

    if "MIN_ATR_PCT" in params:
        MIN_ATR_PCT = clamp(params["MIN_ATR_PCT"], 0.01, 10.0)

    if "STOP_LOSS_PCT" in params:
        STOP_LOSS_PCT = clamp(params["STOP_LOSS_PCT"], 0.1, 10.0)
    if "TAKE_PROFIT_PCT" in params:
        TAKE_PROFIT_PCT = clamp(params["TAKE_PROFIT_PCT"], 0.1, 20.0)

    if "MAX_POSITION_MINUTES" in params:
        MAX_POSITION_MINUTES = int(clamp(params["MAX_POSITION_MINUTES"], 5, 24 * 60))
    if "DAILY_MAX_LOSS_PCT" in params:
        DAILY_MAX_LOSS_PCT = clamp(params["DAILY_MAX_LOSS_PCT"], 0.0, 20.0)

    if "ORDER_QTY_BTC" in params:
        ORDER_QTY_BTC = clamp(params["ORDER_QTY_BTC"], 0.00001, 1.0)
    
    if "EXIT_ON_FLIP_DOWN" in params:
        EXIT_ON_FLIP_DOWN = bool(int(clamp(params["EXIT_ON_FLIP_DOWN"], 0.0, 1.0)))

    logging.info(
        "RUNTIME_PARAMS|symbol=%s|strategy=%s|ATR_PERIOD=%d|ST_MULTIPLIER=%.3f|EMA_PERIOD=%d|RSI_PERIOD=%d|"
        "MIN_ATR_PCT=%.3f|STOP_LOSS_PCT=%.3f|TAKE_PROFIT_PCT=%.3f|MAX_POSITION_MINUTES=%d|DAILY_MAX_LOSS_PCT=%.3f|"
        "ORDER_QTY_BTC=%.8f|EXIT_ON_FLIP_DOWN=%s",
        SYMBOL, STRATEGY_NAME, ATR_PERIOD, ST_MULTIPLIER, EMA_PERIOD, RSI_PERIOD,
        MIN_ATR_PCT, STOP_LOSS_PCT, TAKE_PROFIT_PCT, MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT,
        ORDER_QTY_BTC, EXIT_ON_FLIP_DOWN,
    )

# =========================
# Ledger / Orders (guard-first)
# =========================
def insert_simulated_order(
    *,
    symbol: str,
    interval: str,
    side: str,
    price: float,
    qty_btc: float,
    reason: str,
    candle_open_time,
    is_exit: bool,
    strategy: str = STRATEGY_NAME,
):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO simulated_orders (
            symbol, interval, side, price, quantity_btc,
            reason, rsi_14, ema_21, candle_open_time, strategy, is_exit
        )
        VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL, %s, %s, %s)
        ON CONFLICT (symbol, interval, strategy, candle_open_time, is_exit) DO NOTHING
        RETURNING id;
        """,
        (
            symbol,
            interval,
            side,
            float(price),
            float(qty_btc),
            reason,
            candle_open_time,
            strategy,
            bool(is_exit),
        ),
    )
    inserted = cur.fetchone() is not None
    conn.commit()
    cur.close()
    conn.close()
    return inserted


def execute_and_record(
    *,
    side: str,                 # BUY or SELL
    price: float,
    qty_btc: float,
    reason: str,
    candle_open_time,
    is_exit: bool,
    cfg_used: RuntimeConfig,
    allow_live_orders: bool,
    allow_meta: dict,
):
    """
    Guard-first (jak RSI/BBRANGE):
    1) Rezerwuj slot w DB (simulated_orders) -> idempotencja per candle + is_exit
    2) Potem opcjonalnie LIVE
    3) Ledger zostaje jako audyt niezależnie od LIVE

    Zwraca dict:
      ledger_ok/live_attempted/live_ok/blocked_reason/client_order_id/resp
    """
    inserted = insert_simulated_order(
        symbol=cfg_used.symbol,
        interval=cfg_used.interval,
        side=side,
        price=price,
        qty_btc=qty_btc,
        reason=reason,
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

    # LIVE: permission gate
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
        existing_open = get_open_position()
        if existing_open:
            return {
                "ledger_ok": True,
                "live_attempted": False,
                "live_ok": False,
                "blocked_reason": "ALREADY_OPEN",
                "client_order_id": None,
                "resp": None,
            }

        client_order_id = build_live_entry_intent_client_order_id(
            cfg_used.symbol,
            STRATEGY_NAME,
            cfg_used.interval,
            candle_open_time,
        )

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
            cfg_used.symbol, STRATEGY_NAME, cfg_used.interval, side, candle_open_time, pos_id=int(pos_id), tag="X"
        )
        set_exit_client_order_id(int(pos_id), client_order_id)

    # Use single DB transaction for deterministic SSOT binding on ACK
    conn_exec = get_db_conn()
    try:
        resp = place_live_order(
            client,
            cfg_used.symbol,
            side_u,
            qty_btc,
            trading_mode=cfg_used.trading_mode,
            live_orders_enabled=(cfg_used.live_orders_enabled or is_exit),
            quote_asset=cfg_used.quote_asset,
            client_order_id=client_order_id,
            panic_disable_trading=(os.environ.get("PANIC_DISABLE_TRADING", "0") == "1"),
            live_max_notional=float(os.environ.get("LIVE_MAX_NOTIONAL", "0")),
            skip_balance_precheck=is_exit,
            # --- NEW: deterministic attach on ACK ---
            db_conn=conn_exec,
            position_id=int(pos_id) if pos_id is not None else None,
            leg=("EXIT" if is_exit else "ENTRY"),
        )
        conn_exec.commit()
    finally:
        try:
            conn_exec.close()
        except Exception:
            pass

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

    # Wyznacz live_ok defensywnie (jak BBRANGE)
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

    if not is_exit:
        if not order_id:
            logging.error("SUPERTREND: LIVE ENTRY ACK missing orderId pos_id=%s resp=%s", pos_id, raw)
            emit_strategy_event(
                event_type="BLOCKED",
                decision=side,
                reason="LIVE_ACK_MISSING_ORDER_ID",
                price=price,
                candle_open_time=candle_open_time,
                info={"is_exit": False, "client_order_id": client_order_id, "resp": raw},
            )
            return {
                "ledger_ok": True,
                "live_attempted": True,
                "live_ok": False,
                "blocked_reason": "LIVE_ACK_MISSING_ORDER_ID",
                "client_order_id": client_order_id,
                "resp": raw,
            }

        pos_id = open_position_from_live_ack(
            side="LONG",
            qty=float(qty_btc),
            entry_price=float(price),
            entry_client_order_id=str(client_order_id),
            entry_order_id=str(order_id),
        )

    if is_exit and not order_id:
        logging.error("SUPERTREND: LIVE EXIT ACK missing orderId pos_id=%s resp=%s", pos_id, raw)

    return {
        "ledger_ok": True,
        "live_attempted": True,
        "live_ok": live_ok,
        "blocked_reason": None if live_ok else "ACK_NO_FILL",
        "client_order_id": client_order_id,
        "resp": (resp or {}).get("resp"),
    }


# =========================
# PnL gate (paper ledger)
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
        WHERE symbol = %s AND interval = %s AND strategy = %s
            AND created_at >= date_trunc('day', now() AT TIME ZONE 'UTC')
        ORDER BY created_at ASC
        """,
        (symbol, interval, STRATEGY_NAME),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return 0.0

    cash = PAPER_START_USDC
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

# =========================
# Market data
# =========================
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
    execute_batch(cur, sql, rows, page_size=50)
    conn.commit()
    cur.close()
    conn.close()

def update_indicators():
    """
    Computes EMA, RSI, ATR and SuperTrend over full series, updates last ~50 candles.
    """
    conn = get_db_conn()
    df = pd.read_sql_query(
        """
        SELECT id, open_time, open, high, low, close
        FROM candles
        WHERE symbol = %s AND interval = %s
        ORDER BY open_time
        """,
        conn,
        params=(SYMBOL, INTERVAL),
    )

    if df.empty or len(df) < max(EMA_PERIOD, RSI_PERIOD, ATR_PERIOD) + 5:
        conn.close()
        return

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    # EMA
    df["ema_21"] = close.ewm(span=EMA_PERIOD, adjust=False).mean()

    # RSI
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    roll_up = gain.rolling(window=RSI_PERIOD).mean()
    roll_down = loss.rolling(window=RSI_PERIOD).mean()
    rs = roll_up / roll_down
    df["rsi_14"] = 100.0 - (100.0 / (1.0 + rs))

    # ATR (EWMA of TR)
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr_14"] = tr.ewm(span=ATR_PERIOD, adjust=False).mean()

    # SuperTrend
    hl2 = (high + low) / 2.0
    basic_ub = hl2 + ST_MULTIPLIER * df["atr_14"]
    basic_lb = hl2 - ST_MULTIPLIER * df["atr_14"]

    final_ub = pd.Series(index=df.index, dtype=float)
    final_lb = pd.Series(index=df.index, dtype=float)
    st_dir = pd.Series(index=df.index, dtype=int)
    st_val = pd.Series(index=df.index, dtype=float)

    final_ub.iloc[0] = float(basic_ub.iloc[0])
    final_lb.iloc[0] = float(basic_lb.iloc[0])
    st_dir.iloc[0] = 1
    st_val.iloc[0] = float(final_lb.iloc[0])

    for i in range(1, len(df)):
        c_prev = float(close.iloc[i - 1])

        bu = float(basic_ub.iloc[i])
        bl = float(basic_lb.iloc[i])

        fu_prev = float(final_ub.iloc[i - 1])
        fl_prev = float(final_lb.iloc[i - 1])

        final_ub.iloc[i] = bu if (bu < fu_prev) or (c_prev > fu_prev) else fu_prev
        final_lb.iloc[i] = bl if (bl > fl_prev) or (c_prev < fl_prev) else fl_prev

        c_now = float(close.iloc[i])
        if c_now > float(final_ub.iloc[i - 1]):
            st_dir.iloc[i] = 1
        elif c_now < float(final_lb.iloc[i - 1]):
            st_dir.iloc[i] = -1
        else:
            st_dir.iloc[i] = int(st_dir.iloc[i - 1])

        st_val.iloc[i] = float(final_lb.iloc[i]) if int(st_dir.iloc[i]) == 1 else float(final_ub.iloc[i])

    df["supertrend_direction"] = st_dir
    df["supertrend"] = st_val

    last = df.tail(50)

    cur = conn.cursor()
    sql = """
        UPDATE candles
        SET ema_21 = %s,
            rsi_14 = %s,
            atr_14 = %s,
            supertrend = %s,
            supertrend_direction = %s
        WHERE id = %s;
    """
    data = [
        (
            float(row["ema_21"]) if pd.notna(row["ema_21"]) else None,
            float(row["rsi_14"]) if pd.notna(row["rsi_14"]) else None,
            float(row["atr_14"]) if pd.notna(row["atr_14"]) else None,
            float(row["supertrend"]) if pd.notna(row["supertrend"]) else None,
            int(row["supertrend_direction"]) if pd.notna(row["supertrend_direction"]) else None,
            int(row["id"]),
        )
        for _, row in last.iterrows()
    ]
    cur.executemany(sql, data)
    conn.commit()
    cur.close()
    conn.close()

def get_last_closed_candle():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT open_time, close, ema_21, rsi_14, atr_14, supertrend, supertrend_direction
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

def get_prev_closed_candle():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT open_time, close, ema_21, rsi_14, atr_14, supertrend, supertrend_direction
        FROM candles
        WHERE symbol=%s AND interval=%s
        ORDER BY open_time DESC
        OFFSET 2
        LIMIT 1
        """,
        (SYMBOL, INTERVAL),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

# =========================
# Strategy Logic (SPOT LONG-only)
# =========================
def run_strategy(latest, prev):
    """
    Entry (LONG-only SPOT):
      - signal: SuperTrend flip -1 -> +1
      - volatility gate: ATR% >= MIN_ATR_PCT
      - hour gate, daily loss gate
      - regime gate (ENTRY only)
      - DB guard (is_exit=False)
      - execute_and_record(BUY) then open_position(LONG)

    Exit (LONG):
      - TP / SL / TIMEOUT
      - optional: flip +1 -> -1 (EXIT_ON_FLIP_DOWN)
      - DB guard (is_exit=True)
      - execute_and_record(SELL) then close_position
    """

    open_time, close_price, ema_21, rsi_14, atr_14, st_val, st_dir = latest
    _, prev_close, _, _, _, _, prev_st_dir = prev


    price = float(close_price)
    emit_strategy_event(
        event_type="RUN_START",
        decision=None,
        reason="ENTER",
        price=price,
        candle_open_time=open_time,
        info={"bot_version": os.environ.get("BOT_VERSION")},
    )

    try:
        ema_val = float(ema_21) if ema_21 is not None else None
        rsi_val = float(rsi_14) if rsi_14 is not None else None

        # snapshot + basic tick event
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
        time_exit_enabled = bool(getattr(cfg_effective, "time_exit_enabled", True))
        max_pos_minutes = int(getattr(cfg_effective, "max_position_minutes", MAX_POSITION_MINUTES))

        emit_strategy_event(
            event_type="TICK",
            decision=None,
            reason="LOOP",
            price=price,
            candle_open_time=open_time,
            info={},
        )

        if bc.mode == "HALT":
            emit_strategy_event(
                event_type="BLOCKED",
                reason="BOT_MODE_HALT",
                price=price,
                candle_open_time=open_time,
                info={"mode": "HALT"},
            )
            return

        if bc.mode == "PANIC":
            # close if open (SELL), then halt
            pos = get_open_position()
            if pos:
                _, pos_side, pos_qty, _, _ = pos
                if str(pos_side).upper() == "LONG":
                    res = execute_and_record(
                        side="SELL",
                        price=price,
                        qty_btc=float(pos_qty),
                        reason="PANIC CLOSE LONG",
                        candle_open_time=open_time,
                        is_exit=True,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                    )
                    if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                        close_position(exit_price=price, reason="PANIC", candle_open_time=open_time)
                    else:
                        emit_strategy_event(
                            event_type="BLOCKED",
                            decision="SELL",
                            reason="EXIT_BLOCKED",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return
            # after panic, HALT
            set_mode("HALT", reason="Panic executed; halting.")
            return

        # indicators readiness
        if st_dir is None or prev_st_dir is None or atr_14 is None:
            emit_strategy_event(
                event_type="BLOCKED",
                reason="INDICATORS_NOT_READY",
                price=price,
                candle_open_time=open_time,
                info={"st_dir": st_dir, "prev_st_dir": prev_st_dir, "atr_14": atr_14},
            )
            return

        st_dir_curr = int(st_dir)
        st_dir_prev = int(prev_st_dir)
        atr_pct = (float(atr_14) / price * 100.0) if price > 0 else None

        pos = get_open_position()
        pos_qty_hb = float(pos[2]) if pos else None
        pos_entry_price_hb = float(pos[3]) if pos else None
        pos_side_hb = str(pos[1]) if pos else None
        has_position = pos is not None

        heartbeat({
            **snap["heartbeat"],
            "has_position": bool(has_position),
            "st_dir_prev": int(st_dir_prev),
            "st_dir_curr": int(st_dir_curr),
            "atr_14": float(atr_14) if atr_14 is not None else None,
            "atr_pct": float(atr_pct) if atr_pct is not None else None,
            "ema_21": float(ema_21) if ema_21 is not None else None,
            "rsi_14": float(rsi_14) if rsi_14 is not None else None,
            "supertrend": float(st_val) if st_val is not None else None,
            "pos_qty": pos_qty_hb,
            "pos_entry_price": pos_entry_price_hb,
            "pos_side": pos_side_hb,
        })

        # =========================
        # EXIT if position OPEN (LONG)
        # =========================
        if has_position:
            _, pos_side, pos_qty, pos_entry_price, pos_entry_time = pos
            if str(pos_side).upper() != "LONG":
                # safety: we don't support shorts in SPOT
                emit_strategy_event(
                    event_type="ERROR",
                    reason="UNSUPPORTED_POSITION_SIDE",
                    price=price,
                    candle_open_time=open_time,
                    info={"pos_side": str(pos_side)},
                )
                return

            pos_qty = float(pos_qty)
            pos_entry_price = float(pos_entry_price)

            change_pct = (price - pos_entry_price) / pos_entry_price * 100.0

            # Take profit
            if TAKE_PROFIT_PCT > 0 and change_pct >= TAKE_PROFIT_PCT:
                reason = f"SUPERTREND TAKE PROFIT LONG {change_pct:.2f}% >= {TAKE_PROFIT_PCT:.2f}%"
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
                res = execute_and_record(
                    side="SELL",
                    price=price,
                    qty_btc=pos_qty,
                    reason=reason,
                    candle_open_time=open_time,
                    is_exit=True,
                    cfg_used=cfg_effective,
                    allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],
                )
                if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                    close_position(exit_price=price, reason="TAKE_PROFIT_LONG", candle_open_time=open_time)
                else:
                    emit_strategy_event(
                        event_type="BLOCKED",
                        decision="SELL",
                        reason="EXIT_BLOCKED",
                        price=price,
                        candle_open_time=open_time,
                        info={"res": res},
                    )
                return

            # Stop loss
            drop_pct = -change_pct
            if STOP_LOSS_PCT > 0 and drop_pct >= STOP_LOSS_PCT:
                reason = f"SUPERTREND STOP LOSS LONG {drop_pct:.2f}% >= {STOP_LOSS_PCT:.2f}%"
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
                res = execute_and_record(
                    side="SELL",
                    price=price,
                    qty_btc=pos_qty,
                    reason=reason,
                    candle_open_time=open_time,
                    is_exit=True,
                    cfg_used=cfg_effective,
                    allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],
                )
                if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                    close_position(exit_price=price, reason="STOP_LOSS_LONG", candle_open_time=open_time)
                else:
                    emit_strategy_event(
                        event_type="BLOCKED",
                        decision="SELL",
                        reason="EXIT_BLOCKED",
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
                    reason = f"SUPERTREND TIME_EXIT LONG {age_minutes:.1f}m >= {max_pos_minutes}m"
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
                    res = execute_and_record(
                        side="SELL",
                        price=price,
                        qty_btc=pos_qty,
                        reason=reason,
                        candle_open_time=open_time,
                        is_exit=True,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                    )
                    if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                        close_position(exit_price=price, reason="TIME_EXIT_LONG", candle_open_time=open_time)
                    else:
                        emit_strategy_event(
                            event_type="BLOCKED",
                            decision="SELL",
                            reason="EXIT_BLOCKED",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return

            # Optional: exit on flip down
            if EXIT_ON_FLIP_DOWN and st_dir_prev == 1 and st_dir_curr == -1:
                reason = f"SUPERTREND EXIT ON FLIP DOWN (dir {st_dir_prev}->{st_dir_curr})"
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
                res = execute_and_record(
                    side="SELL",
                    price=price,
                    qty_btc=pos_qty,
                    reason=reason,
                    candle_open_time=open_time,
                    is_exit=True,
                    cfg_used=cfg_effective,
                    allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],
                )
                if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                    close_position(exit_price=price, reason="FLIP_DOWN_EXIT", candle_open_time=open_time)
                else:
                    emit_strategy_event(
                        event_type="BLOCKED",
                        decision="SELL",
                        reason="EXIT_BLOCKED",
                        price=price,
                        candle_open_time=open_time,
                        info={"res": res},
                    )
                return

            return  # position open -> no new entries

        # =========================
        # ENTRY gates (no position)
        # =========================
        if not bc.enabled:
            emit_strategy_event(
                event_type="BLOCKED",
                reason="BOT_DISABLED",
                price=price,
                candle_open_time=open_time,
                info={},
            )
            return

        # Hour gate (UTC)
        hour_utc = open_time.hour
        if hour_utc in DISABLE_HOURS_SET:
            emit_strategy_event(
                event_type="BLOCKED",
                reason="DISABLE_HOURS",
                price=price,
                candle_open_time=open_time,
                info={"hour_utc": hour_utc, "disable_hours": sorted(DISABLE_HOURS_SET)},
            )
            return

        # Daily loss gate — SSOT = positions. PAPER: telemetry only. LIVE: hard-block by positions.
        if DAILY_MAX_LOSS_PCT > 0:
            pos_payload = compute_daily_loss_pct_positions(
                SYMBOL, INTERVAL, STRATEGY_NAME,
                base_usdc=float(DAILY_MAX_LOSS_BASE_USDC),
            )

            conn = get_db_conn()
            try:
                if should_emit_daily_loss_shadow(strategy=STRATEGY_NAME):
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

                # legacy sim-ledger shadow ONLY in LIVE (optional)
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

            # LIVE hard block only by positions-based DML (after telemetry)
            if cfg_effective.trading_mode == "LIVE":
                if should_block_daily_loss_positions(
                    daily_pct=float(pos_payload["daily_pct"]),
                    limit_pct=float(DAILY_MAX_LOSS_PCT),
                ):
                    emit_strategy_event(
                        event_type="BLOCKED",
                        reason="DAILY_MAX_LOSS_POSITIONS",
                        price=price,
                        candle_open_time=open_time,
                        info={**pos_payload, "limit_pct": float(DAILY_MAX_LOSS_PCT)},
                    )
                    return

        # Volatility gate
        if atr_pct is None or atr_pct < MIN_ATR_PCT:
            emit_strategy_event(
                event_type="BLOCKED",
                reason="ATR_TOO_LOW",
                price=price,
                candle_open_time=open_time,
                info={"atr_pct": float(atr_pct) if atr_pct is not None else None, "min": float(MIN_ATR_PCT)},
            )
            return

        # Signal: flip -1 -> +1 => BUY
        if not (st_dir_prev == -1 and st_dir_curr == 1):
            emit_strategy_event(
                event_type="BLOCKED",
                reason="NO_SIGNAL",
                price=price,
                candle_open_time=open_time,
                info={"st_dir_prev": int(st_dir_prev), "st_dir_curr": int(st_dir_curr)},
            )
            return

        decision = "BUY"
        reason = f"SUPERTREND flip DOWN->UP (dir {st_dir_prev}->{st_dir_curr})"

        # Regime gate (ENTRY only) — standard: ENTRY_CHECK
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
            emit_strategy_event(
                event_type="BLOCKED",
                decision=decision,
                reason="REGIME_BLOCK",
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
            info={"st_dir_prev": int(st_dir_prev), "st_dir_curr": int(st_dir_curr), "atr_pct": float(atr_pct)},
        )

        # --- SIZING (jak RSI/TREND) ---
        qty_btc, sizing_info = compute_qty_from_notional_safe(
            client,
            symbol=SYMBOL,
            px=price,
            target_notional=LIVE_TARGET_NOTIONAL,
            min_notional_buffer_pct=MIN_NOTIONAL_BUFFER_PCT,
        )

        emit_strategy_event(
            event_type="SIZING",
            decision="BUY",
            reason="LIVE_NOTIONAL",
            price=float(price),
            candle_open_time=open_time,
            info=sizing_info,
        )

        settings_snapshot = get_user_settings_snapshot()
        manual_entry_addon_usdc = float(settings_snapshot.get("manual_entry_addon_usdc", 0.0) or 0.0)
        base_target_notional = float(LIVE_TARGET_NOTIONAL)
        final_target_notional = base_target_notional + manual_entry_addon_usdc

        if manual_entry_addon_usdc > 0:
            qty_btc, sizing_info = compute_qty_from_notional_safe(
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

        # 1) ledger + live (if enabled)
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
        )
        if not res["ledger_ok"]:
            logging.info("SUPERTREND: entry blocked/failed -> not opening position.")
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

        # 2) positions hard-truth
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
            price=price,
            candle_open_time=open_time,
            info={},
        )

LAST_PROCESSED_OPEN_TIME = None
# =========================
# Main Loop
# =========================
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

    if cfg.trading_mode == "LIVE" and cfg.regime_enabled and cfg.regime_mode == "DRY_RUN":
        logging.info("LIVE + REGIME_ENABLED but REGIME_MODE=DRY_RUN. Consider ENFORCE for profitability.")

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
            rows = fetch_klines()
            save_klines(rows)
            update_indicators()
            latest = get_last_closed_candle()
            prev = get_prev_closed_candle()
            if latest and prev:
                open_time = latest[0]
                if LAST_PROCESSED_OPEN_TIME != open_time:
                    LAST_PROCESSED_OPEN_TIME = open_time
                    run_strategy(latest, prev)
                else:
                    logging.info("SUPERTREND: no new candle yet (%s) -> skip strategy.", str(open_time))
        except Exception as e:
            logging.exception("SUPERTREND loop error")
            emit_strategy_event(
                event_type="ERROR",
                decision=None,
                reason="EXCEPTION",
                price=None,
                candle_open_time=None,
                info={"error": str(e)},
            )

        logging.info("SUPERTREND loop finished in %.3f s", time.perf_counter() - loop_start)
        time.sleep(60)

if __name__ == "__main__":
    logging.info(
        "Starting SUPERTREND bot for %s %s (strategy=%s)...",
        SYMBOL, INTERVAL, STRATEGY_NAME,
    )
    main_loop()
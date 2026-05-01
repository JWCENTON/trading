import os
import time
import json
import logging
import psycopg2
import hashlib
import pandas as pd
from common.daily_loss import should_emit_daily_loss_shadow
from common.alerts import emit_alert_throttled
from common.flags import binance_mytrades_enabled
from common.schema import ensure_schema
from dataclasses import replace
from common.bot_control import upsert_defaults, read as read_bot_control
from binance.client import Client
from common.runtime import RuntimeConfig
from common.permissions import can_trade
from common.regime_gate import decide_regime_gate, emit_regime_gate_event
from datetime import datetime, timezone, date
from psycopg2.extras import execute_batch
from common.sizing import compute_qty_from_notional
from common.telemetry_throttle import should_emit_throttled_event
from common.daily_loss import compute_daily_loss_pct_positions, should_block_daily_loss_positions
from common.binance_ingest_trades import ingest_my_trades
from common.db import get_db_conn
from common.guarded_params import guarded_profit_defaults_map, parse_guarded_profit_config
from common.position_path import load_position_path_snapshot
from common.exit_guards.guarded_profit import evaluate_guarded_profit
from common.exit_guards.early_cut_adaptive import AdaptiveEarlyCutConfig, evaluate_adaptive_early_cut_long
from common.user_settings import SYSTEM_MIN_ENTRY_USDC, get_user_settings_snapshot
from common.win_streak import get_recent_win_streak
from common.execution import (
    place_live_order,
    build_live_client_order_id,
    build_live_entry_intent_client_order_id,
    preflight_live_order,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "trading")
DB_USER = os.environ.get("DB_USER", "botuser")
DB_PASS = os.environ.get("DB_PASS", "botpass")

SYMBOL = os.environ.get("SYMBOL", "BTCUSDC")
INTERVAL = os.environ.get("INTERVAL", "1m")

STRATEGY_NAME = os.environ.get("STRATEGY_NAME", "TREND").upper()

EMA_FAST = int(os.environ.get("EMA_FAST", "21"))
EMA_SLOW = int(os.environ.get("EMA_SLOW", "55"))

RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))
EMA_PERIOD = int(os.environ.get("EMA_PERIOD", "21"))

TREND_FILTER_PCT = float(os.environ.get("TREND_FILTER_PCT", "0.001"))  # 0.1%
ENTRY_BUFFER_PCT = float(os.environ.get("ENTRY_BUFFER_PCT", "0.0015"))  # 0.15%

RSI_OVERSOLD = float(os.environ.get("RSI_OVERSOLD", "25"))
RSI_OVERBOUGHT = float(os.environ.get("RSI_OVERBOUGHT", "75"))

PAPER_START_USDC = float(os.environ.get("PAPER_START_USDC", "100"))

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

cfg = RuntimeConfig.from_env()
client = Client(api_key=API_KEY, api_secret=API_SECRET) if cfg.trading_mode == "LIVE" else Client()

TREND_BUFFER = float(os.environ.get("TREND_BUFFER", "0.001"))  # 0.1%
MAX_POSITION_MINUTES = int(os.environ.get("MAX_POSITION_MINUTES", "90"))
KEEP_PROFIT_MAX_EXTRA_MINUTES = int(os.environ.get("KEEP_PROFIT_MAX_EXTRA_MINUTES", "60"))
TIME_EXIT_MIN_PROFIT_PROTECT_PCT = float(os.environ.get("TIME_EXIT_MIN_PROFIT_PROTECT_PCT", "0.20"))

# jeśli DAILY_MAX_LOSS_PCT <= 0 -> dzienny SL wyłączony
DAILY_MAX_LOSS_PCT = float(os.environ.get("DAILY_MAX_LOSS_PCT", "0.5"))
DAILY_MAX_LOSS_BASE_USDC = float(os.environ.get("DAILY_MAX_LOSS_BASE_USDC", str(PAPER_START_USDC)))

ORDER_QTY_BTC = float(os.environ.get("ORDER_QTY_BTC", "0.0001"))
MAX_DIST_FROM_EMA_FAST_PCT = float(os.environ.get("MAX_DIST_FROM_EMA_FAST_PCT", "0.6")) # percent (e.g. 0.6 = 0.6%)

# ===== Profitability hotfix params =====
ALLOW_SHORT = os.environ.get("ALLOW_SHORT", "0") == "1"  # default OFF
EMA_SLOPE_MIN_PCT = float(os.environ.get("EMA_SLOPE_MIN_PCT", "0.0000"))  # 0.00% by default; set later if needed

EARLY_EXIT_MINUTES = int(os.environ.get("EARLY_EXIT_MINUTES", "30"))
EARLY_EXIT_MAX_LOSS_PCT = float(os.environ.get("EARLY_EXIT_MAX_LOSS_PCT", "0.25"))  # cut losers earlier
MIN_PROFIT_TO_KEEP_PCT = float(os.environ.get("MIN_PROFIT_TO_KEEP_PCT", "0.05"))  # if timeout and profit too small -> exit

ADAPTIVE_EARLY_CUT_SHADOW_ENABLED = os.environ.get("ADAPTIVE_EARLY_CUT_SHADOW_ENABLED", "1") == "1"
ADAPTIVE_EARLY_CUT_MIN_AGE_MINUTES = int(os.environ.get("ADAPTIVE_EARLY_CUT_MIN_AGE_MINUTES", "45"))
ADAPTIVE_EARLY_CUT_ATR_MULT = float(os.environ.get("ADAPTIVE_EARLY_CUT_ATR_MULT", "1.0"))
ADAPTIVE_EARLY_CUT_WEAK_MFE_ATR_MULT = float(os.environ.get("ADAPTIVE_EARLY_CUT_WEAK_MFE_ATR_MULT", "0.5"))

GUARDED_PROFIT_ENABLED = os.environ.get("GUARDED_PROFIT_ENABLED", "0") == "1"
GUARDED_MIN_AGE_MINUTES = int(os.environ.get("GUARDED_MIN_AGE_MINUTES", "10"))
GUARDED_MFE_ARM_090_ABS = float(os.environ.get("GUARDED_MFE_ARM_090_ABS", "0.90"))
GUARDED_FLOOR_090_ABS = float(os.environ.get("GUARDED_FLOOR_090_ABS", "0.70"))
GUARDED_MFE_ARM_110_ABS = float(os.environ.get("GUARDED_MFE_ARM_110_ABS", "1.10"))
GUARDED_FLOOR_110_ABS = float(os.environ.get("GUARDED_FLOOR_110_ABS", "1.00"))

ORDER_NOTIONAL_USDC = float(os.environ.get("ORDER_NOTIONAL_USDC", "6"))
MIN_NOTIONAL_BUFFER_PCT = float(os.environ.get("MIN_NOTIONAL_BUFFER_PCT", "0.05"))
LIVE_TARGET_NOTIONAL = float(os.environ.get("LIVE_TARGET_NOTIONAL", "6.0"))

LAST_TREND_STATE = None

def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


def make_client_order_id(symbol: str, strategy: str, interval: str, side: str, candle_open_time, *, pos_id: int, tag: str) -> str:
    return build_live_client_order_id(symbol, pos_id, tag)

# =========================
# Events
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


def emit_blocked(
    *,
    reason: str,
    decision: str | None,
    price: float | None,
    candle_open_time,
    info: dict | None = None,
):
    # HARD blocks = to są “policy/safety” albo “realny blok orchestratora”
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
        "SPOT_SHORT_BLOCK",
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

# =================
# Regime
# =================


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

    # ENTRY gate: zależny od reżimu
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

    # Czy wolno wysłać LIVE order (ENTRY uwzględnia regime gate)
    allowed_orders_entry, allow_meta_entry = can_trade(
        cfg_effective, regime_allows_trade=allow_gate_entry, is_exit=False, panic_disable_trading=panic
    )
    # EXIT: zawsze dozwolony (regime nie może blokować zamknięcia pozycji)
    allowed_orders_exit, allow_meta_exit = can_trade(
        cfg_effective, regime_allows_trade=True, is_exit=True, panic_disable_trading=panic
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


def open_position(side: str, qty: float, entry_price: float, open_time, *, entry_client_order_id: str | None) -> int | None:
    if cfg.trading_mode == "LIVE" and not entry_client_order_id:
        logging.error("TREND: open_position refused in LIVE without entry_client_order_id")
        return None
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
        return None

    cur.execute(
        """
        INSERT INTO positions(
            symbol, strategy, interval, status, side, qty, entry_price, entry_time,
            entry_client_order_id
        )
        VALUES (%s, %s, %s, 'OPEN', %s, %s, %s, now(), %s)
        RETURNING id;
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL, side, float(qty), float(entry_price), (str(entry_client_order_id) if entry_client_order_id else None)),
    )
    row = cur.fetchone()
    conn.commit()
    pos_id = int(row[0]) if row else None
    cur.close()
    conn.close()
    logging.info(
        "TREND: position OPENED side=%s qty=%.8f entry=%.2f open_time=%s",
        side, float(qty), float(entry_price), str(open_time)
    )
    emit_strategy_event(
        event_type="POSITION_OPENED",
        decision="BUY" if str(side).upper() == "LONG" else "SELL",
        reason="OK",
        price=entry_price,
        candle_open_time=open_time,
        info={"side": side, "qty": float(qty), "entry_price": float(entry_price), "entry_client_order_id": entry_client_order_id},
    )
    return pos_id


def open_position_from_live_ack(
    *,
    side: str,
    qty: float,
    entry_price: float,
    entry_client_order_id: str,
    entry_order_id: str,
) -> int | None:
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
        logging.info("TREND: open_position_from_live_ack skipped – already OPEN pos_id=%s", pos_id)
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
            SYMBOL,
            STRATEGY_NAME,
            INTERVAL,
            side,
            float(qty),
            float(entry_price),
            str(entry_client_order_id),
            str(entry_order_id),
        ),
    )
    pos_id = int(cur.fetchone()[0])
    conn.commit()
    cur.close()
    conn.close()
    return pos_id


def close_position(exit_price: float, reason: str, open_time) -> bool:
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
        logging.info(
            "TREND: position CLOSED reason=%s exit=%.2f open_time=%s",
            reason, float(exit_price), str(open_time)
        )
        emit_strategy_event(
            event_type="POSITION_CLOSED",
            decision=None,
            reason=reason,
            price=exit_price,
            candle_open_time=open_time,
            info={"exit_reason": reason, "exit_price": float(exit_price)},
        )
    else:
        logging.info("TREND: close_position skipped - no OPEN position found.")
    return closed


def attach_entry_order(pos_id: int, *, entry_order_id: str | int | None, entry_client_order_id: str | None):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE positions
        SET entry_order_id=%s, entry_client_order_id=%s
        WHERE id=%s
        """,
        (entry_order_id, entry_client_order_id, int(pos_id)),
    )
    conn.commit()
    cur.close()
    conn.close()


def attach_exit_order(pos_id: int, *, exit_order_id: str | int | None, exit_client_order_id: str | None):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE positions
        SET exit_order_id=%s, exit_client_order_id=%s
        WHERE id=%s
        """,
        (exit_order_id, exit_client_order_id, int(pos_id)),
    )
    conn.commit()
    cur.close()
    conn.close()


def attach_exit_order_id_with_conn(cur, pos_id: int, order_id: str | None, client_order_id: str | None):
    cur.execute(
        """
        UPDATE positions
        SET exit_order_id = COALESCE(exit_order_id, %s),
            exit_client_order_id = COALESCE(exit_client_order_id, %s)
        WHERE id = %s
        """,
        (
            str(order_id) if order_id is not None else None,
            str(client_order_id) if client_order_id else None,
            int(pos_id),
        ),
    )


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


def seed_default_params_from_env(conn):
    global EMA_FAST, EMA_SLOW
    global RSI_OVERSOLD, RSI_OVERBOUGHT
    global STOP_LOSS_PCT, TAKE_PROFIT_PCT
    global MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT
    global TREND_BUFFER, TREND_FILTER_PCT, ENTRY_BUFFER_PCT
    global ORDER_QTY_BTC, MAX_DIST_FROM_EMA_FAST_PCT
    global ORDER_NOTIONAL_USDC, MIN_NOTIONAL_BUFFER_PCT
    global GUARDED_PROFIT_ENABLED, GUARDED_MIN_AGE_MINUTES
    global GUARDED_MFE_ARM_090_ABS, GUARDED_FLOOR_090_ABS, GUARDED_MFE_ARM_110_ABS, GUARDED_FLOOR_110_ABS

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
        "ALLOW_SHORT": 1.0 if ALLOW_SHORT else 0.0,
        "EARLY_EXIT_MINUTES": float(EARLY_EXIT_MINUTES),
        "EARLY_EXIT_MAX_LOSS_PCT": float(EARLY_EXIT_MAX_LOSS_PCT),
        "MIN_PROFIT_TO_KEEP_PCT": float(MIN_PROFIT_TO_KEEP_PCT),
        "KEEP_PROFIT_MAX_EXTRA_MINUTES": float(KEEP_PROFIT_MAX_EXTRA_MINUTES),
        "TIME_EXIT_MIN_PROFIT_PROTECT_PCT": float(TIME_EXIT_MIN_PROFIT_PROTECT_PCT),
        "ORDER_NOTIONAL_USDC": float(ORDER_NOTIONAL_USDC),
        "MIN_NOTIONAL_BUFFER_PCT": float(MIN_NOTIONAL_BUFFER_PCT),
        **guarded_profit_defaults_map(),
    }
    defaults["GUARDED_PROFIT_ENABLED"] = 1.0 if GUARDED_PROFIT_ENABLED else 0.0
    defaults["GUARDED_MIN_AGE_MINUTES"] = float(GUARDED_MIN_AGE_MINUTES)
    defaults["GUARDED_MFE_ARM_090_ABS"] = float(GUARDED_MFE_ARM_090_ABS)
    defaults["GUARDED_FLOOR_090_ABS"] = float(GUARDED_FLOOR_090_ABS)
    defaults["GUARDED_MFE_ARM_110_ABS"] = float(GUARDED_MFE_ARM_110_ABS)
    defaults["GUARDED_FLOOR_110_ABS"] = float(GUARDED_FLOOR_110_ABS)

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
    global ALLOW_SHORT, EARLY_EXIT_MINUTES, EARLY_EXIT_MAX_LOSS_PCT, MIN_PROFIT_TO_KEEP_PCT, KEEP_PROFIT_MAX_EXTRA_MINUTES, TIME_EXIT_MIN_PROFIT_PROTECT_PCT
    global GUARDED_PROFIT_ENABLED, GUARDED_MIN_AGE_MINUTES
    global GUARDED_MFE_ARM_090_ABS, GUARDED_FLOOR_090_ABS, GUARDED_MFE_ARM_110_ABS, GUARDED_FLOOR_110_ABS
    global ORDER_NOTIONAL_USDC, MIN_NOTIONAL_BUFFER_PCT
    global GUARDED_PROFIT_ENABLED, GUARDED_MIN_AGE_MINUTES
    global GUARDED_MFE_ARM_090_ABS, GUARDED_FLOOR_090_ABS, GUARDED_MFE_ARM_110_ABS, GUARDED_FLOOR_110_ABS

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
        RSI_OVERSOLD = clamp(params["RSI_OVERSOLD"], 5.0, 95.0)
    if "RSI_OVERBOUGHT" in params:
        RSI_OVERBOUGHT = clamp(params["RSI_OVERBOUGHT"], 5.0, 95.0)

    if RSI_OVERSOLD >= RSI_OVERBOUGHT:
        logging.warning(
            "RSI params invalid (oversold>=overbought). Adjusting oversold to overbought-1. "
            "oversold=%.2f overbought=%.2f", RSI_OVERSOLD, RSI_OVERBOUGHT
        )
        RSI_OVERSOLD = max(5.0, RSI_OVERBOUGHT - 1.0)

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

    if "ALLOW_SHORT" in params:
        ALLOW_SHORT = bool(int(clamp(params["ALLOW_SHORT"], 0.0, 1.0)))

    if "EARLY_EXIT_MINUTES" in params:
        EARLY_EXIT_MINUTES = int(clamp(params["EARLY_EXIT_MINUTES"], 5, 240))

    if "EARLY_EXIT_MAX_LOSS_PCT" in params:
        EARLY_EXIT_MAX_LOSS_PCT = clamp(params["EARLY_EXIT_MAX_LOSS_PCT"], 0.05, 5.0)

    if "MIN_PROFIT_TO_KEEP_PCT" in params:
        MIN_PROFIT_TO_KEEP_PCT = clamp(params["MIN_PROFIT_TO_KEEP_PCT"], 0.0, 5.0)

    if "KEEP_PROFIT_MAX_EXTRA_MINUTES" in params:
        KEEP_PROFIT_MAX_EXTRA_MINUTES = int(clamp(params["KEEP_PROFIT_MAX_EXTRA_MINUTES"], 0, 24 * 60))

    if "TIME_EXIT_MIN_PROFIT_PROTECT_PCT" in params:
        TIME_EXIT_MIN_PROFIT_PROTECT_PCT = clamp(params["TIME_EXIT_MIN_PROFIT_PROTECT_PCT"], -5.0, 5.0)

    guarded_cfg = parse_guarded_profit_config(params)
    GUARDED_PROFIT_ENABLED = guarded_cfg.enabled
    GUARDED_MIN_AGE_MINUTES = guarded_cfg.min_age_minutes
    GUARDED_MFE_ARM_090_ABS = guarded_cfg.mfe_arm_090_abs
    GUARDED_FLOOR_090_ABS = guarded_cfg.floor_090_abs
    GUARDED_MFE_ARM_110_ABS = guarded_cfg.mfe_arm_110_abs
    GUARDED_FLOOR_110_ABS = guarded_cfg.floor_110_abs

    if "ORDER_NOTIONAL_USDC" in params:
        ORDER_NOTIONAL_USDC = clamp(params["ORDER_NOTIONAL_USDC"], 1.0, 10000.0)

    if "MIN_NOTIONAL_BUFFER_PCT" in params:
        MIN_NOTIONAL_BUFFER_PCT = clamp(params["MIN_NOTIONAL_BUFFER_PCT"], 0.0, 0.50)

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
        "DAILY_MAX_LOSS_PCT=%.2f|RSI_OVERSOLD=%.1f|RSI_OVERBOUGHT=%.1f|"
        "ORDER_QTY_BTC=%.8f|ORDER_NOTIONAL_USDC=%.2f|MIN_NOTIONAL_BUFFER_PCT=%.3f|MAX_DIST_FROM_EMA_FAST_PCT=%.2f|"
        "ALLOW_SHORT=%s|EARLY_EXIT_MINUTES=%d|EARLY_EXIT_MAX_LOSS_PCT=%.2f|MIN_PROFIT_TO_KEEP_PCT=%.2f|"
        "KEEP_PROFIT_MAX_EXTRA_MINUTES=%d|TIME_EXIT_MIN_PROFIT_PROTECT_PCT=%.2f|"
        "ADAPTIVE_EARLY_CUT_SHADOW_ENABLED=%s|ADAPTIVE_EARLY_CUT_MIN_AGE_MINUTES=%d|"
        "ADAPTIVE_EARLY_CUT_ATR_MULT=%.2f|ADAPTIVE_EARLY_CUT_WEAK_MFE_ATR_MULT=%.2f|"
        "GUARDED_PROFIT_ENABLED=%s|GUARDED_MIN_AGE_MINUTES=%d|GUARDED_MFE_ARM_090_ABS=%.4f|"
        "GUARDED_FLOOR_090_ABS=%.4f|GUARDED_MFE_ARM_110_ABS=%.4f|GUARDED_FLOOR_110_ABS=%.4f",
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
        ORDER_NOTIONAL_USDC,
        MIN_NOTIONAL_BUFFER_PCT,
        MAX_DIST_FROM_EMA_FAST_PCT,
        ALLOW_SHORT,
        EARLY_EXIT_MINUTES,
        EARLY_EXIT_MAX_LOSS_PCT,
        MIN_PROFIT_TO_KEEP_PCT,
        KEEP_PROFIT_MAX_EXTRA_MINUTES,
        TIME_EXIT_MIN_PROFIT_PROTECT_PCT,
        ADAPTIVE_EARLY_CUT_SHADOW_ENABLED,
        ADAPTIVE_EARLY_CUT_MIN_AGE_MINUTES,
        ADAPTIVE_EARLY_CUT_ATR_MULT,
        ADAPTIVE_EARLY_CUT_WEAK_MFE_ATR_MULT,
        GUARDED_PROFIT_ENABLED,
        GUARDED_MIN_AGE_MINUTES,
        GUARDED_MFE_ARM_090_ABS,
        GUARDED_FLOOR_090_ABS,
        GUARDED_MFE_ARM_110_ABS,
        GUARDED_FLOOR_110_ABS,
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


def load_recent_atr_abs(
    *,
    symbol: str,
    interval: str,
    asof_open_time: datetime,
    period: int = 14,
) -> float | None:
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT high, low, close
            FROM candles
            WHERE symbol = %s
              AND interval = %s
              AND open_time <= %s
            ORDER BY open_time DESC
            LIMIT %s
            """,
            (symbol, interval, asof_open_time, period + 1),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not rows or len(rows) < period:
        return None

    rows = list(reversed(rows))
    trs: list[float] = []
    prev_close = None

    for idx, row in enumerate(rows):
        high = float(row[0])
        low = float(row[1])
        close = float(row[2])

        if idx == 0 or prev_close is None:
            tr = high - low
        else:
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close),
            )
        trs.append(tr)
        prev_close = close

    if len(trs) < period:
        return None

    recent = trs[-period:]
    return sum(recent) / len(recent)


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
            symbol, interval, side, price, quantity_btc,
            reason, rsi_14, ema_21, candle_open_time, strategy, is_exit
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            float(rsi_14) if rsi_14 is not None else None,
            float(ema_21) if ema_21 is not None else None,
            candle_open_time,
            strategy,
            bool(is_exit),
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
            "Simulated order skipped by DB guard (symbol=%s interval=%s strategy=%s candle_open_time=%s is_exit=%s).",
            symbol, interval, strategy, candle_open_time, is_exit,
        )
    cur.close()
    conn.close()
    return inserted


def ssot_apply_positions_paper(
    *,
    side: str,
    price: float,
    qty_btc: float,
    candle_open_time,
    is_exit: bool,
) -> dict:
    """
    PAPER SSOT:
    - ENTRY -> open_position()
    - EXIT  -> close_position()
    Zwraca meta do diagnostyki.
    """
    side_u = str(side).upper()

    if not is_exit:
        pos_side = "LONG" if side_u == "BUY" else "SHORT"

        pos_id = open_position(
            side=str(pos_side),
            qty=float(qty_btc),
            entry_price=float(price),
            open_time=candle_open_time,
            entry_client_order_id=None,
        )
        if pos_id is None:
            return {"paper_pos_action": "ENTRY_SKIPPED_ALREADY_OPEN", "paper_pos_id": None}

        return {"paper_pos_action": "ENTRY_OPENED", "paper_pos_id": int(pos_id)}

    # EXIT
    pos = get_open_position()
    if not pos:
        return {"paper_pos_action": "EXIT_SKIPPED_NO_OPEN", "paper_pos_id": None}

    close_position(exit_price=float(price), reason="PAPER_EXIT", open_time=candle_open_time)
    return {"paper_pos_action": "EXIT_CLOSED", "paper_pos_id": int(pos[0])}


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
    rsi_14: float | None = None,
    ema_21: float | None = None,
    pos_id: int | None = None,
):
    # 1) DB guard FIRST
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
        strategy=STRATEGY_NAME,
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

    # 2) PAPER SSOT: update positions even in PAPER (Variant A)
    if cfg_used.trading_mode != "LIVE":
        meta = ssot_apply_positions_paper(
            side=side,
            price=float(price),
            qty_btc=float(qty_btc),
            candle_open_time=candle_open_time,
            is_exit=bool(is_exit),
        )

        emit_strategy_event(
            event_type="SSOT_PAPER_APPLY",
            decision=side,
            reason="OK",
            price=price,
            candle_open_time=candle_open_time,
            info={"is_exit": bool(is_exit), **(meta or {})},
        )

        # jeśli to był EXIT i nie było OPEN -> nie blokuj ledger (ledger już jest), tylko audyt
        if meta.get("paper_pos_action") == "EXIT_SKIPPED_NO_OPEN":
            return {
                "ledger_ok": True,
                "live_attempted": False,
                "live_ok": False,
                "blocked_reason": "EXIT_NO_OPEN_POSITION",
                "client_order_id": None,
                "resp": meta,
            }

        # jeśli ENTRY i już była OPEN -> traktuj jako "ALREADY_OPEN" (ledger zostaje)
        if meta.get("paper_pos_action") == "ENTRY_SKIPPED_ALREADY_OPEN":
            return {
                "ledger_ok": True,
                "live_attempted": False,
                "live_ok": False,
                "blocked_reason": "ALREADY_OPEN",
                "client_order_id": None,
                "resp": meta,
            }

        return {
            "ledger_ok": True,
            "live_attempted": False,
            "live_ok": True,
            "blocked_reason": None,
            "client_order_id": None,
            "resp": meta,
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

    # ENTRY
    if not is_exit:
        pos_side = "LONG" if side_u == "BUY" else "SHORT"

        if cfg_used.trading_mode != "LIVE":
            # PAPER: local OPEN immediately
            pos_id_new = open_position(
                side=str(pos_side),
                qty=float(qty_btc),
                entry_price=float(price),
                open_time=candle_open_time,
                entry_client_order_id=None,
            )
            if pos_id_new is None:
                return {
                    "ledger_ok": True,
                    "live_attempted": False,
                    "live_ok": False,
                    "blocked_reason": "ALREADY_OPEN",
                    "client_order_id": None,
                    "resp": None,
                }
            pos_id = pos_id_new
        else:
            # LIVE: do NOT open locally before exchange ACK/fill
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
            pos_id = None

        tag = "E"

    # EXIT
    else:
        if pos_id is None:
            # fallback: take currently open pos id
            p = get_open_position()
            if not p:
                return {
                    "ledger_ok": True,
                    "live_attempted": False,
                    "live_ok": False,
                    "blocked_reason": "NO_OPEN_POSITION",
                    "client_order_id": None,
                    "resp": None,
                }
            pos_id = int(p[0])
        tag = "X"

    if not is_exit:
        client_order_id = build_live_entry_intent_client_order_id(
            cfg_used.symbol,
            STRATEGY_NAME,
            cfg_used.interval,
            candle_open_time,
        )
    else:
        if pos_id is None:
            p = get_open_position()
            if not p:
                return {
                    "ledger_ok": True,
                    "live_attempted": False,
                    "live_ok": False,
                    "blocked_reason": "NO_OPEN_POSITION",
                    "client_order_id": None,
                    "resp": None,
                }
            pos_id = int(p[0])

        client_order_id = make_client_order_id(
            cfg_used.symbol,
            STRATEGY_NAME,
            cfg_used.interval,
            side,
            candle_open_time,
            pos_id=int(pos_id),
            tag="X",
        )

        set_exit_client_order_id(int(pos_id), client_order_id)

    conn_exec = get_db_conn()
    try:
        pre = preflight_live_order(
            client,
            cfg_used.symbol,
            side,
            qty_btc,
            trading_mode=cfg_used.trading_mode,
            live_orders_enabled=(cfg_used.live_orders_enabled or is_exit),
            quote_asset=cfg_used.quote_asset,
            panic_disable_trading=(os.environ.get("PANIC_DISABLE_TRADING", "0") == "1"),
            live_max_notional=float(os.environ.get("LIVE_MAX_NOTIONAL", "0")),
            skip_balance_precheck=bool(is_exit),
        )

        if not pre or not pre.get("ok"):
            emit_strategy_event(
                event_type="BLOCKED",
                decision=side,
                reason=(pre or {}).get("reason") or "LIVE_PREFLIGHT_FAILED",
                price=price,
                candle_open_time=candle_open_time,
                info={
                    "is_exit": bool(is_exit),
                    "pos_id": int(pos_id) if pos_id else None,
                    "client_order_id": client_order_id,
                    "resp": pre,
                },
            )
            return {
                "ledger_ok": True,
                "live_attempted": False,
                "live_ok": False,
                "blocked_reason": (pre or {}).get("reason") or "LIVE_PREFLIGHT_FAILED",
                "client_order_id": client_order_id,
                "resp": pre,
            }
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
        if not is_exit and pos_id is not None:
            conn = get_db_conn()
            cur = conn.cursor()
            cur.execute("DELETE FROM positions WHERE id=%s AND status='OPEN'", (int(pos_id),))
            conn.commit()
            cur.close()
            conn.close()
        return {
            "ledger_ok": True,
            "live_attempted": True,
            "live_ok": False,
            "blocked_reason": "LIVE_ORDER_FAILED",
            "client_order_id": client_order_id,
            "resp": (resp or {}).get("resp"),
        }

    raw = (resp or {}).get("resp") or {}
    status_raw = str(raw.get("status", "")).upper()
    executed_raw = raw.get("executedQty")
    try:
        executed_f = float(executed_raw) if executed_raw is not None else 0.0
    except Exception:
        executed_f = 0.0

    # compute live_ok safely
    live_ok = resp.get("live_ok")
    if live_ok is None:
        status = str(raw.get("status", "")).upper()
        executed = raw.get("executedQty")
        try:
            executed_f = float(executed) if executed is not None else 0.0
        except Exception:
            executed_f = 0.0
        live_ok = executed_f > 0.0 or status == "FILLED"
    live_ok = bool(live_ok)

    emit_strategy_event(
        event_type="LIVE_ORDER_SENT",
        decision=side,
        reason="OK" if live_ok else "ACK_NO_FILL",
        price=price,
        candle_open_time=candle_open_time,
        info={
            "is_exit": bool(is_exit),
            "client_order_id": client_order_id,
            "live_ok": live_ok,
            "status": status_raw,
            "executed_qty": executed_f,
            "resp": raw,
        },
    )


    if (
        cfg_used.trading_mode == "LIVE"
        and not is_exit
        and live_ok
        and executed_f > 0.0
    ):
        order_id = raw.get("orderId")

        avg_px = None
        try:
            fills = raw.get("fills") or []
            if fills:
                total_qty = 0.0
                total_quote = 0.0
                for fl in fills:
                    q = float(fl.get("qty") or 0.0)
                    p = float(fl.get("price") or 0.0)
                    total_qty += q
                    total_quote += q * p
                if total_qty > 0.0:
                    avg_px = total_quote / total_qty
        except Exception:
            avg_px = None

        if avg_px is None:
            try:
                avg_px = float(raw.get("price") or price)
            except Exception:
                avg_px = float(price)

        pos_id_new = open_position_from_live_ack(
            side=("LONG" if str(side).upper() == "BUY" else "SHORT"),
            qty=float(executed_f),
            entry_price=float(avg_px),
            entry_order_id=str(order_id) if order_id is not None else None,
            entry_client_order_id=str(client_order_id),
        )

        if not pos_id_new:
            emit_strategy_event(
                event_type="BLOCKED",
                decision=side,
                reason="LIVE_ENTRY_ACK_BUT_POSITION_NOT_OPENED",
                price=price,
                candle_open_time=candle_open_time,
                info={
                    "client_order_id": client_order_id,
                    "order_id": order_id,
                    "executed_qty": executed_f,
                    "avg_px": avg_px,
                    "resp": raw,
                },
            )
            raise RuntimeError(
                "TREND live entry ACK but position not opened: "
                f"symbol={cfg_used.symbol} interval={cfg_used.interval} "
                f"client_order_id={client_order_id} order_id={order_id}"
            )

    return {
        "ledger_ok": True,
        "live_attempted": True,
        "live_ok": live_ok,
        "blocked_reason": None if live_ok else "ACK_NO_FILL",
        "client_order_id": client_order_id,
        "resp": raw,
    }


def safe_close_if_open(current_price: float, candle_open_time, bc, cfg_effective):
    pos = get_open_position()
    if not pos:
        return False

    _, side, qty, _, _ = pos

    exit_side = "SELL" if str(side).upper() == "LONG" else "BUY"
    reason = "PANIC CLOSE LONG" if exit_side == "SELL" else "PANIC CLOSE SHORT"

    panic = (os.environ.get("PANIC_DISABLE_TRADING", "0") == "1")
    allowed_orders, allow_meta = can_trade(
        cfg_effective, regime_allows_trade=True, is_exit=True, panic_disable_trading=panic
    )

    res = execute_and_record(
        side=exit_side,
        price=current_price,
        qty_btc=float(qty),
        reason=reason,
        candle_open_time=candle_open_time,
        cfg_used=cfg_effective,
        allow_live_orders=allowed_orders,
        allow_meta=allow_meta,
        is_exit=True,
        pos_id=int(pos[0]),
        rsi_14=None,
        ema_21=None,
    )
    if not res["ledger_ok"]:
        logging.info("TREND: exit blocked by DB guard (already traded this candle) -> skipping close.")
        return False
    # w LIVE wymagaj live_ok
    if cfg_effective.trading_mode == "LIVE" and not res["live_ok"]:
        logging.info("TREND: exit suppressed/failed -> not closing position.")
        return False

    return close_position(exit_price=current_price, reason="PANIC", open_time=candle_open_time)


def compute_daily_pnl_pct(symbol: str, interval: str, current_price: float) -> float:
    # Legacy sim-ledger daily pnl is ONLY meaningful for LIVE shadow.
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

    equity_now = cash + btc * current_price

    if equity_start_today is None:
        return 0.0

    return (equity_now - equity_start_today) / equity_start_today * 100.0


def run_trend_strategy():
    global LAST_TREND_STATE
    
    rows = get_latest_candles(limit=max(EMA_SLOW + 20, 100))
    if not rows or len(rows) < EMA_SLOW + 5:
        logging.info("TREND: not enough candles yet (have %d).", len(rows) if rows else 0)
        return

    rows_asc = list(reversed(rows))

    df = pd.DataFrame(
        [
            {
                "symbol": r[0],
                "interval": r[1],
                "open_time": r[2],
                "close": float(r[3]),
                "ema_21": (float(r[4]) if r[4] is not None else None),
                "rsi_14": (float(r[5]) if r[5] is not None else None),
            }
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
    ema_21 = float(last["ema_21"]) if last.get("ema_21") is not None else None
    rsi_14 = float(last["rsi_14"]) if last.get("rsi_14") is not None else None

    price = float(last["close"])
    prev_price = float(prev["close"])
    ema_fast = float(last["ema_fast"])
    ema_slow = float(last["ema_slow"])
    ema_fast_prev = float(prev["ema_fast"])
    ema_fast_slope = ema_fast - ema_fast_prev
    open_time = last["open_time"]

    ema_fast_slope_pct = 0.0 if ema_fast_prev == 0 else (ema_fast - ema_fast_prev) / ema_fast_prev

    snap = get_runtime_snapshot(price=price, open_time=open_time)
    emit_strategy_event(
        event_type="RUN_START",
        decision=None,
        reason="ENTER",
        price=float(price),
        candle_open_time=open_time,
        info={"bot_version": os.environ.get("BOT_VERSION")},
    )
    # Telemetry: zapisuj status reżimu raz na świecę (baseline), nawet bez sygnału
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
            regime_enabled=snap["bc"].regime_enabled,
            regime_mode=snap["bc"].regime_mode,
        ),
    )
    try:
        bc = snap["bc"]
        cfg_effective = snap["cfg_effective"]
        time_exit_enabled = bool(getattr(cfg_effective, "time_exit_enabled", True))
        max_pos_minutes = int(getattr(cfg_effective, "max_position_minutes", MAX_POSITION_MINUTES))

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
            safe_close_if_open(current_price=float(price), candle_open_time=open_time, bc=bc, cfg_effective=cfg_effective)
            emit_strategy_event(
                event_type="CONFIG_APPLIED",
                reason="PANIC_TRIGGERED",
                price=price,
                candle_open_time=open_time,
                info={"mode": "PANIC"},
            )
            set_mode("HALT", reason="Panic executed; halting.")
            return


        trend = "FLAT"
        if ema_fast > ema_slow * (1.0 + TREND_FILTER_PCT + TREND_BUFFER):
            trend = "UP"
        elif ema_fast < ema_slow * (1.0 - TREND_FILTER_PCT - TREND_BUFFER):
            trend = "DOWN"

        logging.info(
            "TREND: price=%.2f ema_fast=%.2f ema_slow=%.2f trend=%s ema_fast_slope=%.4f",
            price, ema_fast, ema_slow, trend, ema_fast_slope
        )

        trend_changed = (LAST_TREND_STATE != trend)
        LAST_TREND_STATE = trend

        pos = get_open_position()

        heartbeat({
            **snap["heartbeat"],
            "trend": trend,
            "ema_fast": float(ema_fast),
            "ema_slow": float(ema_slow),
            "ema_fast_slope": float(ema_fast_slope),
            "has_position": bool(pos is not None),
        })

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
                        cfg_used=cfg_effective,
                        rsi_14=rsi_14,
                        ema_21=ema_21,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        is_exit=True,
                        pos_id=int(pos[0]),
                    )
                    if not res["ledger_ok"]:
                        logging.info("TREND: exit blocked by DB guard -> skipping close.")
                        return
                    if cfg_effective.trading_mode == "LIVE" and not res["live_ok"]:
                        return

                    close_position(exit_price=price, reason="TAKE_PROFIT_LONG", open_time=open_time)
                    return

                drop_pct = -change_pct
                if STOP_LOSS_PCT > 0 and drop_pct >= STOP_LOSS_PCT:
                    reason = f"TREND STOP LOSS LONG {drop_pct:.2f}% >= {STOP_LOSS_PCT:.2f}%"
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
                        cfg_used=cfg_effective,
                        rsi_14=rsi_14,
                        ema_21=ema_21,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        is_exit=True,
                        pos_id=int(pos[0]),
                    )
                    if not res["ledger_ok"]:
                        logging.info("TREND: exit blocked by DB guard -> skipping close.")
                        return

                    if cfg_effective.trading_mode == "LIVE" and not res["live_ok"]:
                        logging.info("TREND: exit suppressed/failed -> not closing position.")
                        return

                    close_position(exit_price=price, reason="STOP_LOSS_LONG", open_time=open_time)
                    return

            elif pos_side == "SHORT":
                change_pct = (pos_entry_price - price) / pos_entry_price * 100.0

                if TAKE_PROFIT_PCT > 0 and change_pct >= TAKE_PROFIT_PCT:
                    reason = f"TREND TAKE PROFIT SHORT {change_pct:.2f}% >= {TAKE_PROFIT_PCT:.2f}%"
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
                        side="BUY",
                        price=price,
                        qty_btc=pos_qty,
                        reason=reason,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        rsi_14=rsi_14,
                        ema_21=ema_21,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        is_exit=True,
                        pos_id=int(pos[0]),
                    )
                    if not res["ledger_ok"]:
                        logging.info("TREND: exit blocked by DB guard -> skipping close.")
                        return
                    if cfg_effective.trading_mode == "LIVE" and not res["live_ok"]:
                        logging.info("TREND: exit suppressed/failed -> not closing position.")
                        return
                    close_position(exit_price=price, reason="TAKE_PROFIT_SHORT", open_time=open_time)
                    return

                rise_pct = -change_pct
                if STOP_LOSS_PCT > 0 and rise_pct >= STOP_LOSS_PCT:
                    reason = f"TREND STOP LOSS SHORT {rise_pct:.2f}% >= {STOP_LOSS_PCT:.2f}%"
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
                        side="BUY",
                        price=price,
                        qty_btc=pos_qty,
                        reason=reason,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        rsi_14=rsi_14,
                        ema_21=ema_21,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        is_exit=True,
                        pos_id=int(pos[0]),
                    )
                    if not res["ledger_ok"]:
                        logging.info("TREND: exit blocked by DB guard -> skipping close.")
                        return
                    if cfg_effective.trading_mode == "LIVE" and not res["live_ok"]:
                        logging.info("TREND: exit suppressed/failed -> not closing position.")
                        return
                    close_position(exit_price=price, reason="STOP_LOSS_SHORT", open_time=open_time)
                    return
                
            # --- GUARDED PROFIT: shared decision layer, local TREND execution ---
            if pos_side == "LONG" and pos_entry_time is not None and GUARDED_PROFIT_ENABLED:
                if pos_entry_time.tzinfo is None:
                    pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)

                age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0
                path = load_position_path_snapshot(
                    symbol=SYMBOL,
                    interval=INTERVAL,
                    entry_time=pos_entry_time,
                    asof_open_time=open_time,
                    entry_price=pos_entry_price,
                )
                guarded_cfg = parse_guarded_profit_config({
                    "GUARDED_PROFIT_ENABLED": 1.0 if GUARDED_PROFIT_ENABLED else 0.0,
                    "GUARDED_MIN_AGE_MINUTES": float(GUARDED_MIN_AGE_MINUTES),
                    "GUARDED_MFE_ARM_090_ABS": float(GUARDED_MFE_ARM_090_ABS),
                    "GUARDED_FLOOR_090_ABS": float(GUARDED_FLOOR_090_ABS),
                    "GUARDED_MFE_ARM_110_ABS": float(GUARDED_MFE_ARM_110_ABS),
                    "GUARDED_FLOOR_110_ABS": float(GUARDED_FLOOR_110_ABS),
                })
                guarded_decision = evaluate_guarded_profit(
                    side=pos_side,
                    age_minutes=age_minutes,
                    entry_price=pos_entry_price,
                    path=path,
                    config=guarded_cfg,
                )
                if guarded_decision.triggered:
                    reason_guarded = (
                        f"TREND GUARDED LONG bucket={guarded_decision.guard_bucket} "
                        f"mfe_abs={guarded_decision.mfe_abs:.4f} current_move_abs={guarded_decision.current_move_abs:.4f} "
                        f"floor_abs={guarded_decision.floor_abs:.4f} age={guarded_decision.age_minutes:.1f}m"
                    )
                    emit_strategy_event(
                        event_type="EXIT_SIGNAL",
                        decision="SELL",
                        reason="GUARDED_PROFIT_LONG",
                        price=price,
                        candle_open_time=open_time,
                        info={
                            "guard_bucket": guarded_decision.guard_bucket,
                            "mfe_abs": float(guarded_decision.mfe_abs or 0.0),
                            "floor_abs": float(guarded_decision.floor_abs or 0.0),
                            "current_move_abs": float(guarded_decision.current_move_abs or 0.0),
                            "age_minutes": float(guarded_decision.age_minutes),
                            "bars_seen": int(path.bars_seen) if path is not None else 0,
                        },
                    )
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
                        reason=reason_guarded,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        rsi_14=rsi_14,
                        ema_21=ema_21,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        is_exit=True,
                        pos_id=int(pos[0]),
                    )
                    if not res["ledger_ok"]:
                        logging.info("TREND: guarded exit blocked by DB guard -> skipping close.")
                        return
                    if cfg_effective.trading_mode == "LIVE" and not res["live_ok"]:
                        logging.info("TREND: guarded exit suppressed/failed -> not closing position.")
                        return
                    close_position(exit_price=price, reason="GUARDED_PROFIT_LONG", open_time=open_time)
                    return

            # --- ADAPTIVE EARLY CUT (SHADOW ONLY): telemetry, no real exit ---
            if (
                ADAPTIVE_EARLY_CUT_SHADOW_ENABLED
                and pos_side == "LONG"
                and pos_entry_time is not None
            ):
                if pos_entry_time.tzinfo is None:
                    pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)

                age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0
                path = load_position_path_snapshot(
                    symbol=SYMBOL,
                    interval=INTERVAL,
                    entry_time=pos_entry_time,
                    asof_open_time=open_time,
                    entry_price=pos_entry_price,
                )
                atr_abs = load_recent_atr_abs(
                    symbol=SYMBOL,
                    interval=INTERVAL,
                    asof_open_time=open_time,
                    period=14,
                )
                adaptive_cfg = AdaptiveEarlyCutConfig(
                    enabled=ADAPTIVE_EARLY_CUT_SHADOW_ENABLED,
                    min_age_minutes=ADAPTIVE_EARLY_CUT_MIN_AGE_MINUTES,
                    atr_mult=ADAPTIVE_EARLY_CUT_ATR_MULT,
                    weak_mfe_atr_mult=ADAPTIVE_EARLY_CUT_WEAK_MFE_ATR_MULT,
                )
                adaptive_decision = evaluate_adaptive_early_cut_long(
                    age_minutes=age_minutes,
                    entry_price=pos_entry_price,
                    current_price=price,
                    mfe_abs=path.mfe_abs,
                    mae_abs=path.mae_abs,
                    atr_abs=atr_abs,
                    config=adaptive_cfg,
                )
                throttle_conn = get_db_conn()
                try:
                    should_emit_shadow = should_emit_throttled_event(
                        conn=throttle_conn,
                        symbol=SYMBOL,
                        interval=INTERVAL,
                        strategy=STRATEGY_NAME,
                        event_type="SHADOW",
                        reason="EARLY_CUT_ADAPTIVE_SHADOW",
                        throttle_seconds=300,
                    )
                finally:
                    throttle_conn.close()

                if should_emit_shadow:
                    emit_strategy_event(
                        event_type="SHADOW",
                        decision="SELL" if adaptive_decision.would_cut else "HOLD",
                        reason="EARLY_CUT_ADAPTIVE_SHADOW",
                        price=price,
                        candle_open_time=open_time,
                        info={
                            "symbol": SYMBOL,
                            "interval": INTERVAL,
                            "age_minutes": float(adaptive_decision.age_minutes),
                            "entry_price": float(pos_entry_price),
                            "current_price": float(price),
                            "current_move_abs": float(adaptive_decision.current_move_abs),
                            "current_move_pct": float(adaptive_decision.current_move_pct),
                            "mfe_abs": float(adaptive_decision.mfe_abs),
                            "mae_abs": float(adaptive_decision.mae_abs),
                            "max_high": float(path.max_high),
                            "min_low": float(path.min_low),
                            "bars_seen": int(path.bars_seen),
                            "atr_abs": float(adaptive_decision.atr_abs),
                            "atr_pct": float(adaptive_decision.atr_pct),
                            "adaptive_min_age_minutes": int(adaptive_decision.min_age_minutes),
                            "adaptive_loss_abs": float(adaptive_decision.adaptive_loss_abs),
                            "adaptive_loss_pct": float(adaptive_decision.adaptive_loss_pct),
                            "weak_mfe_threshold_abs": float(adaptive_decision.weak_mfe_threshold_abs),
                            "weak_trade": bool(adaptive_decision.weak_trade),
                            "loss_breach": bool(adaptive_decision.loss_breach),
                            "would_cut_adaptive": bool(adaptive_decision.would_cut),
                            "reason_code": adaptive_decision.reason_code,
                        },
                    )

            # --- EARLY EXIT: cut losers earlier than TIMEOUT/SL to make TIMEOUT non-negative on average ---
            if EARLY_EXIT_MINUTES > 0 and pos_entry_time is not None:
                if pos_entry_time.tzinfo is None:
                    pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)

                age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0

                if age_minutes >= EARLY_EXIT_MINUTES:
                    if pos_side == "LONG":
                        pnl_pct = (price - pos_entry_price) / pos_entry_price * 100.0
                        if pnl_pct <= -EARLY_EXIT_MAX_LOSS_PCT:
                            reason_early = f"TREND EARLY_CUT LONG {pnl_pct:.2f}% <= -{EARLY_EXIT_MAX_LOSS_PCT:.2f}% after {age_minutes:.1f}m"
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
                                reason=reason_early,
                                candle_open_time=open_time,
                                cfg_used=cfg_effective,
                                rsi_14=rsi_14,
                                ema_21=ema_21,
                                allow_live_orders=snap["allowed_orders_exit"],
                                allow_meta=snap["allow_meta_exit"],
                                is_exit=True,
                                pos_id=int(pos[0]),
                            )
                            if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                                close_position(exit_price=price, reason="EARLY_CUT_LONG", open_time=open_time)
                            return

                    elif pos_side == "SHORT":
                        pnl_pct = (pos_entry_price - price) / pos_entry_price * 100.0
                        if pnl_pct <= -EARLY_EXIT_MAX_LOSS_PCT:
                            reason_early = f"TREND EARLY_CUT SHORT {pnl_pct:.2f}% <= -{EARLY_EXIT_MAX_LOSS_PCT:.2f}% after {age_minutes:.1f}m"
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
                                side="BUY",
                                price=price,
                                qty_btc=pos_qty,
                                reason=reason_early,
                                candle_open_time=open_time,
                                cfg_used=cfg_effective,
                                rsi_14=rsi_14,
                                ema_21=ema_21,
                                allow_live_orders=snap["allowed_orders_exit"],
                                allow_meta=snap["allow_meta_exit"],
                                is_exit=True,
                                pos_id=int(pos[0]),
                            )
                            if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                                close_position(exit_price=price, reason="EARLY_CUT_SHORT", open_time=open_time)
                            return

            # =========================
            # TIMEOUT (optional) — keep winners, cut stagnation
            # =========================
            if time_exit_enabled and max_pos_minutes > 0 and pos_entry_time is not None:
                if pos_entry_time.tzinfo is None:
                    pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)

                age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0

                if age_minutes >= max_pos_minutes:
                    # PnL% depends on side
                    if pos_side == "LONG":
                        pnl_pct = (price - pos_entry_price) / pos_entry_price * 100.0
                    else:
                        pnl_pct = (pos_entry_price - price) / pos_entry_price * 100.0

                    # If position is doing well enough, we keep it (avoid cutting trend winners)
                    extra_age_minutes = float(age_minutes - max_pos_minutes)

                    # 1) still profitable enough and inside limited extension window -> hold temporarily
                    if (
                        pnl_pct >= MIN_PROFIT_TO_KEEP_PCT
                        and extra_age_minutes < float(KEEP_PROFIT_MAX_EXTRA_MINUTES)
                    ):
                        emit_strategy_event(
                            event_type="BLOCKED",
                            decision=None,
                            reason="TIME_EXIT_SKIPPED_KEEP_PROFIT_WINDOW",
                            price=price,
                            candle_open_time=open_time,
                            info={
                                "pos_side": pos_side,
                                "age_minutes": float(age_minutes),
                                "max_minutes": int(max_pos_minutes),
                                "extra_age_minutes": float(extra_age_minutes),
                                "keep_profit_max_extra_minutes": int(KEEP_PROFIT_MAX_EXTRA_MINUTES),
                                "pnl_pct": float(pnl_pct),
                                "min_profit_to_keep_pct": float(MIN_PROFIT_TO_KEEP_PCT),
                            },
                        )
                        return

                    # 2) after timeout, if profit has faded below protect floor -> force exit
                    if extra_age_minutes >= 0 and pnl_pct < float(TIME_EXIT_MIN_PROFIT_PROTECT_PCT):
                        side_timeout = "SELL" if pos_side == "LONG" else "BUY"
                        reason_timeout = (
                            f"TREND TIMEOUT_PROFIT_FADED {pos_side} age={age_minutes:.1f}m "
                            f"extra={extra_age_minutes:.1f}m pnl={pnl_pct:.2f}% "
                            f"< protect={TIME_EXIT_MIN_PROFIT_PROTECT_PCT:.2f}%"
                        )

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
                            side=side_timeout,
                            price=price,
                            qty_btc=pos_qty,
                            reason=reason_timeout,
                            candle_open_time=open_time,
                            cfg_used=cfg_effective,
                            allow_live_orders=snap["allowed_orders_exit"],
                            allow_meta=snap["allow_meta_exit"],
                            is_exit=True,
                        )
                        if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                            close_position(exit_price=price, reason="TIME_EXIT_PROFIT_FADED", open_time=open_time)
                        return

                    # 3) hard final timeout after extension window
                    if extra_age_minutes >= float(KEEP_PROFIT_MAX_EXTRA_MINUTES):
                        side_timeout = "SELL" if pos_side == "LONG" else "BUY"
                        reason_timeout = (
                            f"TREND TIMEOUT_HARD {pos_side} age={age_minutes:.1f}m "
                            f"extra={extra_age_minutes:.1f}m >= keep_window={KEEP_PROFIT_MAX_EXTRA_MINUTES}m "
                            f"(pnl={pnl_pct:.2f}%)"
                        )

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
                            side=side_timeout,
                            price=price,
                            qty_btc=pos_qty,
                            reason=reason_timeout,
                            candle_open_time=open_time,
                            cfg_used=cfg_effective,
                            allow_live_orders=snap["allowed_orders_exit"],
                            allow_meta=snap["allow_meta_exit"],
                            is_exit=True,
                        )
                        if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                            close_position(exit_price=price, reason="TIME_EXIT_HARD", open_time=open_time)
                        return

                    side_timeout = "SELL" if pos_side == "LONG" else "BUY"
                    reason_timeout = f"TREND TIMEOUT {pos_side} {age_minutes:.1f}m >= {max_pos_minutes}m (pnl={pnl_pct:.2f}%)"

                    # Optional: audit regime meta on EXIT (does not block exit)
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
                        side=side_timeout,
                        price=price,
                        qty_btc=pos_qty,
                        reason=reason_timeout,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        rsi_14=rsi_14,
                        ema_21=ema_21,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        is_exit=True,
                        pos_id=int(pos[0]),
                    )

                    if not res["ledger_ok"]:
                        emit_strategy_event(
                            event_type="BLOCKED",
                            decision=side_timeout,
                            reason="EXIT_BLOCKED",
                            price=price,
                            candle_open_time=open_time,
                            info={
                                "exit_reason": "TIMEOUT",
                                "pos_side": pos_side,
                                "age_minutes": float(age_minutes),
                                "max_minutes": int(max_pos_minutes),
                            },
                        )
                        logging.info("TREND: exit blocked by DB guard -> skipping close.")
                        return
                    if cfg_effective.trading_mode == "LIVE" and not res["live_ok"]:
                        return
                    close_position(exit_price=price, reason="TIMEOUT", open_time=open_time)
                    return

            return  # mamy pozycję -> brak nowych wejść

        if not bc.enabled:
            emit_blocked(
                reason="BOT_DISABLED",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"trend": trend, "ema_fast": float(ema_fast), "ema_slow": float(ema_slow)},
            )
            return

        # --- ENTRY FILTERS ---
        hour_utc = open_time.hour
        if hour_utc in DISABLE_HOURS_SET:
            emit_blocked(
                reason="DISABLE_HOURS",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"hour_utc": int(hour_utc), "disable_hours": sorted(list(DISABLE_HOURS_SET)), "trend": trend},
            )
            return


        # Daily loss gate — SSOT = positions. PAPER: telemetry only. LIVE: hard-block by positions.
        if DAILY_MAX_LOSS_PCT > 0:
            # 1) positions-based realized-only (SSOT)
            pos_payload = compute_daily_loss_pct_positions(
                SYMBOL, INTERVAL, STRATEGY_NAME,
                base_usdc=float(DAILY_MAX_LOSS_BASE_USDC),
            )

            # 2) telemetry: positions shadow always (throttled) + legacy sim shadow ONLY in LIVE (optional)
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
                #        if should_emit_throttled_event(
                #            conn=conn,
                #            symbol=SYMBOL,
                #            interval=INTERVAL,
                #            strategy=STRATEGY_NAME,
                #            event_type="ALERT",
                #            reason="DAILY_MAX_LOSS_SHADOW",
                #            throttle_seconds=15 * 60,
                #        ):
                #            emit_alert_throttled(
                #                conn=conn,
                #                symbol=SYMBOL,
                #                interval=INTERVAL,
                #                strategy=STRATEGY_NAME,
                #                reason="DAILY_MAX_LOSS_SHADOW",
                #                open_time=open_time,
                #                price=price,
                #                info={"daily_pct": float(daily_pct), "limit_pct": float(DAILY_MAX_LOSS_PCT)},
                #            )
            finally:
                conn.close()

            # 3) LIVE: hard-block by positions after telemetry
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

        if trend == "FLAT":
            emit_blocked(
                reason="TREND_NOT_ACTIVE_FLAT",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"trend": trend, "ema_fast": float(ema_fast), "ema_slow": float(ema_slow)},
            )
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
            emit_blocked(
                reason="TREND_DOWN_LONG_ONLY",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"trend": trend},
            )
            return

        # Momentum confirmation: EMA_FAST slope must agree with direction
        if decision == "BUY" and ema_fast_slope_pct <= EMA_SLOPE_MIN_PCT:
            emit_strategy_event(
                event_type="BLOCKED",
                reason="EMA_SLOPE_REJECT",
                price=price,
                candle_open_time=open_time,
                info={"trend": trend, "up_breakout_now": up_breakout_now, "down_breakout_now": down_breakout_now, "slope_pct": float(ema_fast_slope_pct), "slope_abs": float(ema_fast_slope)},
            )
            return
        if decision == "SELL" and ema_fast_slope_pct >= -EMA_SLOPE_MIN_PCT:
            emit_strategy_event(
                event_type="BLOCKED",
                reason="EMA_SLOPE_REJECT",
                price=price,
                candle_open_time=open_time,
                info={"trend": trend, "up_breakout_now": up_breakout_now, "down_breakout_now": down_breakout_now, "slope_pct": float(ema_fast_slope_pct), "slope_abs": float(ema_fast_slope)},
            )
            return

        # Simple price momentum confirmation
        if decision == "BUY" and price <= prev_price:
            emit_strategy_event(
                event_type="BLOCKED",
                reason="PRICE_MOMENTUM_REJECT",
                price=price,
                candle_open_time=open_time,
                info={"trend": trend, "up_breakout_now": up_breakout_now, "down_breakout_now": down_breakout_now, "prev_price": prev_price},
            )
            return
        if decision == "SELL" and price >= prev_price:
            emit_strategy_event(
                event_type="BLOCKED",
                reason="PRICE_MOMENTUM_REJECT",
                price=price,
                candle_open_time=open_time,
                info={"trend": trend, "up_breakout_now": up_breakout_now, "down_breakout_now": down_breakout_now, "prev_price": prev_price},
            )
            return

        if decision == "HOLD":
            emit_blocked(
                reason="NO_SIGNAL",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"trend": trend, "ema_fast": float(ema_fast), "ema_slow": float(ema_slow)},
            )
            return

        # --- REGIME GATE (ENTRY ONLY) ---
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
                info={"why": gate_entry.why, "regime": gate_entry.regime, "meta": gate_entry.meta, "trend": trend},
            )
            return
        
        emit_strategy_event(
            event_type="SIGNAL",
            decision=decision,
            reason="OK",
            price=price,
            candle_open_time=open_time,
            info={"trend": trend, "ema_fast": ema_fast, "ema_slow": ema_slow, "ema_fast_slope": ema_fast_slope},
        )

        # ===== MAX DIST FILTER (po sygnale, przed sizingiem jest OK, ale qty jeszcze nie istnieje) =====
        dist_from_fast_pct = (price - ema_fast) / ema_fast * 100.0
        if abs(dist_from_fast_pct) > MAX_DIST_FROM_EMA_FAST_PCT:
            emit_strategy_event(
                event_type="BLOCKED",
                decision=decision,
                reason="MAX_DIST_FROM_EMA",
                price=price,
                candle_open_time=open_time,
                info={
                    "trend": trend,
                    "dist_from_fast_pct": float(dist_from_fast_pct),
                    "max_dist_pct": float(MAX_DIST_FROM_EMA_FAST_PCT),
                    "ema_fast": float(ema_fast),
                },
            )
            return

        # ===== SIZING (MUSI być przed użyciem qty_btc) =====
        if cfg_effective.trading_mode == "LIVE":
            qty_btc, sizing_info = compute_qty_from_notional(
                client,
                symbol=SYMBOL,
                px=price,
                target_notional=LIVE_TARGET_NOTIONAL,
                min_notional_buffer_pct=MIN_NOTIONAL_BUFFER_PCT,
            )
        else:
            # PAPER sizing: stała ilość (albo możesz też policzyć z notional — jak w RSI)
            qty_btc = float(ORDER_QTY_BTC)
            sizing_info = {
                "mode": "PAPER_FIXED_QTY",
                "qty": float(qty_btc),
                "px": float(price),
            }

        emit_strategy_event(
            event_type="SIZING",
            decision=decision,
            reason="NOTIONAL" if cfg_effective.trading_mode == "LIVE" else "FIXED_QTY",
            price=float(price),
            candle_open_time=open_time,
            info=sizing_info,
        )

        settings_snapshot = get_user_settings_snapshot()
        manual_entry_addon_usdc = float(settings_snapshot.get("manual_entry_addon_usdc", 0.0) or 0.0)
        configured_three_win_boost_usdc = float(settings_snapshot.get("three_win_boost_usdc", 10.0) or 10.0)
        recent_win_streak = get_recent_win_streak(strategy=STRATEGY_NAME, symbol=SYMBOL, interval=INTERVAL, required_wins=3)
        applied_three_win_boost_usdc = configured_three_win_boost_usdc if recent_win_streak.eligible else 0.0
        base_target_notional = float(LIVE_TARGET_NOTIONAL)
        final_target_notional = base_target_notional + manual_entry_addon_usdc + applied_three_win_boost_usdc

        if cfg_effective.trading_mode == "LIVE" and (manual_entry_addon_usdc > 0 or applied_three_win_boost_usdc > 0):
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
            decision=decision,
            reason="FINAL_NOTIONAL",
            price=float(price),
            candle_open_time=open_time,
            info={
                **sizing_info,
                "base_target_notional": base_target_notional,
                "manual_entry_addon_usdc": manual_entry_addon_usdc,
                "configured_three_win_boost_usdc": float(configured_three_win_boost_usdc),
                "recent_closed_trades_checked": int(recent_win_streak.checked),
                "recent_win_streak_required": int(recent_win_streak.required),
                "recent_win_streak": int(recent_win_streak.streak),
                "three_win_boost_active": bool(recent_win_streak.eligible),
                "applied_three_win_boost_usdc": float(applied_three_win_boost_usdc),
                "win_streak_source": recent_win_streak.source,
                "win_streak_error": recent_win_streak.error,
                "boost_candidate": bool(getattr(recent_win_streak, "boost_candidate", False)),
                "boost_allowed": bool(getattr(recent_win_streak, "boost_allowed", False)),
                "boost_block_reason": getattr(recent_win_streak, "boost_block_reason", None),
                "prev_net_1": getattr(recent_win_streak, "prev_net_1", None),
                "prev_net_2": getattr(recent_win_streak, "prev_net_2", None),
                "prev_net_3": getattr(recent_win_streak, "prev_net_3", None),
                "last_exit_reason": getattr(recent_win_streak, "last_exit_reason", None),
                "last_boost_exit_reason": getattr(recent_win_streak, "last_boost_exit_reason", None),
                "last_trade_gross_pct": getattr(recent_win_streak, "last_trade_gross_pct", None),
                "rolling_5_gross_pct_avg": getattr(recent_win_streak, "rolling_5_gross_pct_avg", None),
                "final_target_notional": float(final_target_notional),
            },
        )

        qty_btc = float(qty_btc)
        if qty_btc <= 0:
            emit_strategy_event(
                event_type="BLOCKED",
                decision=decision,
                reason="SIZING_QTY_ZERO",
                price=float(price),
                candle_open_time=open_time,
                info=sizing_info,
            )
            return

        logging.info(
            "TREND: opening %s at %.2f (%s) qty=%.8f",
            decision, price, reason, float(qty_btc)
        )

        res = execute_and_record(
            side=decision,
            price=price,
            qty_btc=float(qty_btc),
            reason=reason,
            candle_open_time=open_time,
            cfg_used=cfg_effective,
            rsi_14=rsi_14,
            ema_21=ema_21,
            allow_live_orders=snap["allowed_orders_entry"],
            allow_meta=snap["allow_meta_entry"],
            is_exit=False,
        )
        if not res["ledger_ok"]:
            logging.info("TREND: entry blocked/failed -> not opening position.")
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

    finally:
        emit_strategy_event(
            event_type="RUN_END",
            decision=None,
            reason="DONE",
            price=float(price),
            candle_open_time=open_time,
            info={},
        )


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
    if cfg.trading_mode == "LIVE" and cfg.regime_enabled and cfg.regime_mode == "DRY_RUN":
        logging.warning("LIVE + REGIME_ENABLED but REGIME_MODE=DRY_RUN. Consider ENFORCE for profitability.")
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
            if latest:
                open_time = latest[0]                
                if LAST_PROCESSED_OPEN_TIME != open_time:
                    LAST_PROCESSED_OPEN_TIME = open_time
                    run_trend_strategy()
                else:
                    emit_strategy_event(
                        event_type="IDLE",
                        decision=None,
                        reason="NO_NEW_CANDLE",
                        price=float(latest[4]) if latest[4] is not None else None,
                        candle_open_time=open_time,
                        info={"open_time": str(open_time), "last_processed": str(LAST_PROCESSED_OPEN_TIME)},
                    )                    
                    logging.info("TREND: no new candle yet (%s) -> skip strategy.", str(open_time))

        except Exception as e:
            logging.exception("TREND loop error")
            emit_strategy_event(
                event_type="ERROR",
                decision=None,
                reason="EXCEPTION",
                price=None,
                candle_open_time=None,
                info={"error": str(e)},
            )

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
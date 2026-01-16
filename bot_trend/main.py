import os
import time
import json
import logging
import psycopg2
import hashlib
import pandas as pd
from common.schema import ensure_schema
from dataclasses import replace
from common.bot_control import upsert_defaults, read as read_bot_control
from binance.client import Client
from common.runtime import RuntimeConfig
from common.permissions import can_trade
from common.db import get_latest_regime
from datetime import datetime, timezone, date
from psycopg2.extras import execute_batch
from common.execution import place_live_order
from common.sizing import compute_qty_from_notional
from common.telemetry_throttle import should_emit_throttled_event


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

client = Client(api_key=API_KEY, api_secret=API_SECRET)

TREND_BUFFER = float(os.environ.get("TREND_BUFFER", "0.001"))  # 0.1%
MAX_POSITION_MINUTES = int(os.environ.get("MAX_POSITION_MINUTES", "90"))

# jeśli DAILY_MAX_LOSS_PCT <= 0 -> dzienny SL wyłączony
DAILY_MAX_LOSS_PCT = float(os.environ.get("DAILY_MAX_LOSS_PCT", "0.5"))

ORDER_QTY_BTC = float(os.environ.get("ORDER_QTY_BTC", "0.0001"))
MAX_DIST_FROM_EMA_FAST_PCT = float(os.environ.get("MAX_DIST_FROM_EMA_FAST_PCT", "0.6")) # percent (e.g. 0.6 = 0.6%)

# ===== Profitability hotfix params =====
ALLOW_SHORT = os.environ.get("ALLOW_SHORT", "0") == "1"  # default OFF
EMA_SLOPE_MIN_PCT = float(os.environ.get("EMA_SLOPE_MIN_PCT", "0.0000"))  # 0.00% by default; set later if needed

EARLY_EXIT_MINUTES = int(os.environ.get("EARLY_EXIT_MINUTES", "30"))
EARLY_EXIT_MAX_LOSS_PCT = float(os.environ.get("EARLY_EXIT_MAX_LOSS_PCT", "0.25"))  # cut losers earlier
MIN_PROFIT_TO_KEEP_PCT = float(os.environ.get("MIN_PROFIT_TO_KEEP_PCT", "0.05"))  # if timeout and profit too small -> exit

ORDER_NOTIONAL_USDC = float(os.environ.get("ORDER_NOTIONAL_USDC", "6"))
MIN_NOTIONAL_BUFFER_PCT = float(os.environ.get("MIN_NOTIONAL_BUFFER_PCT", "0.05"))
LIVE_TARGET_NOTIONAL = float(os.environ.get("LIVE_TARGET_NOTIONAL", "6.0"))

LAST_TREND_STATE = None

cfg = RuntimeConfig.from_env()

def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


def emit_alert_throttled(*, open_time, price, info):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT MAX(created_at)
        FROM strategy_events
        WHERE symbol=%s AND interval=%s AND strategy=%s
          AND event_type='ALERT' AND reason='DAILY_MAX_LOSS_SHADOW'
          AND created_at >= now() - interval '15 minutes'
    """, (SYMBOL, INTERVAL, STRATEGY_NAME))
    last = cur.fetchone()[0]
    cur.close()
    conn.close()

    if last is not None:
        return  # throttle: był alert w ostatnich 15 minutach

    emit_strategy_event(
        event_type="ALERT",
        reason="DAILY_MAX_LOSS_SHADOW",
        price=price,
        candle_open_time=open_time,
        info=info,
    )


def make_client_order_id(symbol: str, strategy: str, interval: str, side: str, candle_open_time) -> str:
    raw = f"{symbol}|{strategy}|{interval}|{side}|{candle_open_time.isoformat()}"
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]
    return f"{symbol[:6]}-{strategy[:6]}-{interval}-{side}-{h}"[:36]

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
):
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.strategy_events
              (symbol, interval, strategy, event_type, decision, reason, price, candle_open_time, info)
            VALUES
              (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb);
            """,
            (
                SYMBOL,
                INTERVAL,
                STRATEGY_NAME,
                event_type,
                decision,
                reason,
                float(price) if price is not None else None,
                candle_open_time,
                json.dumps(info or {}, default=_json_default),
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        # nie zabijaj pętli bota przez telemetry
        logging.exception("strategy_events insert failed")

# =================
# Regime
# =================

REGIME_MAX_AGE_SECONDS = int(os.environ.get("REGIME_MAX_AGE_SECONDS", "180"))


def regime_allows(strategy_name: str, symbol: str, interval: str, bc):
    """
    Zwraca: (allow: bool, meta: dict)
    DRY_RUN: zawsze allow=True, ale meta mówi czy 'would_block'.
    """
    if not bc.regime_enabled:
        return True, {"enabled": False}

    r = get_latest_regime(symbol, interval)
    if not r:
        # fail-open (zależnie od preferencji); na etapie PAPER OK
        return True, {"enabled": True, "reason": "no_regime"}

    # świeżość
    ts = r.get("ts")
    if ts is None:
        return True, {"enabled": True, "reason": "regime_ts_null", **r}

    now = datetime.now(timezone.utc)
    age = (now - ts).total_seconds()
    if age > REGIME_MAX_AGE_SECONDS:
        return True, {
            "enabled": True,
            "reason": f"regime_stale age={age:.0f}s",
            "age_s": age,
            "regime": r.get("regime"),
            "ts": ts,
            **r,
        }

    regime = r.get("regime")

    # Polityka blokowania (v1):
    # - RSI/BBRANGE: blokuj w TREND_UP/TREND_DOWN i SHOCK
    # - TREND: blokuj w RANGE_* i SHOCK
    # - SUPER_TREND: blokuj w SHOCK (opcjonalnie też RANGE_LOWVOL, ale zostawmy tylko SHOCK na start)
    would_block = False
    why = "ok"

    if strategy_name in ("RSI", "BBRANGE"):
        would_block = False
        why = "ok"
    elif strategy_name in ("TREND",):
        if regime in ("RANGE_LOWVOL", "SHOCK"):
            would_block = True
            why = f"TREND blocked in {regime}"
    elif strategy_name in ("SUPERTREND", "SUPER_TREND", "ST"):
        if regime in ("SHOCK",):
            would_block = True
            why = f"SUPERTREND blocked in {regime}"

    # DRY_RUN: nie blokujemy, tylko logujemy
    if bc.regime_mode == "DRY_RUN":
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
    Zapisuje event ZAWSZE gdy mamy rmeta (telemetry baseline).
    Telemetry nie może zabijać pętli bota.
    """
    if not rmeta:
        return

    try:
        mode = rmeta.get("mode")
        would_block = rmeta.get("would_block", False)

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

    except Exception:
        logging.exception("regime_gate_events insert failed")


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
    allow_gate_entry, rmeta_gate = regime_allows(STRATEGY_NAME, SYMBOL, INTERVAL, bc)

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


def open_position(side: str, qty: float, entry_price: float, open_time) -> bool:
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
        info={"side": side, "qty": float(qty), "entry_price": float(entry_price)},
    )
    return True


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


def seed_default_params_from_env(conn):
    global EMA_FAST, EMA_SLOW
    global RSI_OVERSOLD, RSI_OVERBOUGHT
    global STOP_LOSS_PCT, TAKE_PROFIT_PCT
    global MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT
    global TREND_BUFFER, TREND_FILTER_PCT, ENTRY_BUFFER_PCT
    global ORDER_QTY_BTC, MAX_DIST_FROM_EMA_FAST_PCT
    global ORDER_NOTIONAL_USDC, MIN_NOTIONAL_BUFFER_PCT

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
        "ORDER_NOTIONAL_USDC": float(ORDER_NOTIONAL_USDC),
        "MIN_NOTIONAL_BUFFER_PCT": float(MIN_NOTIONAL_BUFFER_PCT),
    }

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
    global ALLOW_SHORT, EARLY_EXIT_MINUTES, EARLY_EXIT_MAX_LOSS_PCT, MIN_PROFIT_TO_KEEP_PCT
    global ORDER_NOTIONAL_USDC, MIN_NOTIONAL_BUFFER_PCT

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
        "ALLOW_SHORT=%s|EARLY_EXIT_MINUTES=%d|EARLY_EXIT_MAX_LOSS_PCT=%.2f|MIN_PROFIT_TO_KEEP_PCT=%.2f",
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
):
    # 1) DB guard FIRST
    inserted = insert_simulated_order(
        symbol=cfg_used.symbol,
        interval=cfg_used.interval,
        side=side,
        price=price,
        qty_btc=qty_btc,
        reason=reason,
        rsi_14=None,
        ema_21=None,
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

    # 2) LIVE AFTER ledger
    if cfg_used.trading_mode != "LIVE":
        return {
            "ledger_ok": True,
            "live_attempted": False,
            "live_ok": True,  # PAPER traktujemy jako wykonane
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

    client_order_id = make_client_order_id(cfg_used.symbol, STRATEGY_NAME, cfg_used.interval, side, candle_open_time)

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

    # ok -> wylicz live_ok bezpiecznie
    live_ok = resp.get("live_ok")
    if live_ok is None:
        raw = (resp or {}).get("resp") or {}
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
            "status": resp.get("status"),
            "executed_qty": resp.get("executed_qty"),
            "resp": (resp or {}).get("resp"),
        },
    )

    return {
        "ledger_ok": True,
        "live_attempted": True,
        "live_ok": live_ok,
        "blocked_reason": None if live_ok else "ACK_NO_FILL",
        "client_order_id": client_order_id,
        "resp": (resp or {}).get("resp"),
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


def size_qty_from_notional(symbol: str, target_notional: float, price: float, min_notional_buffer_pct: float):
    """
    Zwraca: (qty, meta)
    Meta pod event SIZING: step, min_qty, min_notional, notional, target_notional, buffer.
    Wymaga filtrów LOT_SIZE/MIN_NOTIONAL z Binance.
    """
    info = client.get_symbol_info(symbol)
    if not info:
        raise RuntimeError(f"Symbol info not found for {symbol}")

    step = None
    min_qty = None
    min_notional = None

    for f in info.get("filters", []):
        if f.get("filterType") == "LOT_SIZE":
            step = float(f["stepSize"])
            min_qty = float(f["minQty"])
        if f.get("filterType") in ("MIN_NOTIONAL", "NOTIONAL"):
            # Binance bywa różny zależnie od rynku / wersji
            mn = f.get("minNotional") or f.get("notional") or f.get("min_notional")
            if mn is not None:
                min_notional = float(mn)

    if step is None or min_qty is None:
        raise RuntimeError(f"LOT_SIZE filter missing for {symbol}")
    if min_notional is None:
        # konserwatywnie: jeśli brak, nie blokuj, ale loguj min_notional=null
        min_notional = 0.0

    target = float(target_notional)
    px = float(price)

    raw_qty = target / px if px > 0 else 0.0

    # dopasowanie do step
    steps = int(raw_qty / step)
    qty = steps * step

    # enforce min_qty
    if qty < min_qty:
        qty = 0.0

    notional = qty * px
    min_required = min_notional * (1.0 + float(min_notional_buffer_pct))

    meta = {
        "px": px,
        "qty": float(qty),
        "step": str(step),
        "min_qty": float(min_qty),
        "notional": float(notional),
        "min_notional": float(min_notional),
        "target_notional": float(target),
        "min_notional_buffer_pct": float(min_notional_buffer_pct),
        "min_required_notional": float(min_required),
    }

    ok = (qty > 0.0) and (notional >= min_required)
    return ok, qty, meta


def run_trend_strategy():
    global LAST_TREND_STATE
    
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
    log_regime_gate_event(
        symbol=SYMBOL,
        interval=INTERVAL,
        strategy=STRATEGY_NAME,
        decision="TICK",
        allow=snap["allow_gate_entry"],
        rmeta=snap["rmeta_gate"],
        extra_meta={
            "price": float(price),
            "ema_fast": float(ema_fast) if "ema_fast" in locals() else None,
            "ema_slow": float(ema_slow) if "ema_slow" in locals() else None,
            "open_time": str(open_time),
            "note": "baseline per candle",
            "stage": "BASELINE",
        },
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
                    res = execute_and_record(
                        side="SELL",
                        price=price,
                        qty_btc=pos_qty,
                        reason=reason,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        is_exit=True,
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
                    res = execute_and_record(
                        side="SELL",
                        price=price,
                        qty_btc=pos_qty,
                        reason=reason,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        is_exit=True,
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
                    res = execute_and_record(
                        side="BUY",
                        price=price,
                        qty_btc=pos_qty,
                        reason=reason,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        is_exit=True,
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
                    res = execute_and_record(
                        side="BUY",
                        price=price,
                        qty_btc=pos_qty,
                        reason=reason,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        is_exit=True,
                    )
                    if not res["ledger_ok"]:
                        logging.info("TREND: exit blocked by DB guard -> skipping close.")
                        return
                    if cfg_effective.trading_mode == "LIVE" and not res["live_ok"]:
                        logging.info("TREND: exit suppressed/failed -> not closing position.")
                        return
                    close_position(exit_price=price, reason="STOP_LOSS_SHORT", open_time=open_time)
                    return
                
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
                            res = execute_and_record(
                                side="SELL",
                                price=price,
                                qty_btc=pos_qty,
                                reason=reason_early,
                                candle_open_time=open_time,
                                cfg_used=cfg_effective,
                                allow_live_orders=snap["allowed_orders_exit"],
                                allow_meta=snap["allow_meta_exit"],
                                is_exit=True,
                            )
                            if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                                close_position(exit_price=price, reason="EARLY_CUT_LONG", open_time=open_time)
                            return

                    elif pos_side == "SHORT":
                        pnl_pct = (pos_entry_price - price) / pos_entry_price * 100.0
                        if pnl_pct <= -EARLY_EXIT_MAX_LOSS_PCT:
                            reason_early = f"TREND EARLY_CUT SHORT {pnl_pct:.2f}% <= -{EARLY_EXIT_MAX_LOSS_PCT:.2f}% after {age_minutes:.1f}m"
                            res = execute_and_record(
                                side="BUY",
                                price=price,
                                qty_btc=pos_qty,
                                reason=reason_early,
                                candle_open_time=open_time,
                                cfg_used=cfg_effective,
                                allow_live_orders=snap["allowed_orders_exit"],
                                allow_meta=snap["allow_meta_exit"],
                                is_exit=True,
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
                    if pnl_pct >= MIN_PROFIT_TO_KEEP_PCT:
                        emit_strategy_event(
                            event_type="BLOCKED",
                            decision=None,
                            reason="TIME_EXIT_SKIPPED_KEEP_PROFIT",
                            price=price,
                            candle_open_time=open_time,
                            info={
                                "pos_side": pos_side,
                                "age_minutes": float(age_minutes),
                                "max_minutes": int(max_pos_minutes),
                                "pnl_pct": float(pnl_pct),
                                "min_profit_to_keep_pct": float(MIN_PROFIT_TO_KEEP_PCT),
                            },
                        )
                        return

                    side_timeout = "SELL" if pos_side == "LONG" else "BUY"
                    reason_timeout = f"TREND TIMEOUT {pos_side} {age_minutes:.1f}m >= {max_pos_minutes}m (pnl={pnl_pct:.2f}%)"

                    # Optional: audit regime meta on EXIT (does not block exit)
                    allow_gate_exit, rmeta_gate_exit = regime_allows(STRATEGY_NAME, SYMBOL, INTERVAL, bc)
                    log_regime_gate_on_exit(
                        decision=side_timeout,
                        rmeta_gate=rmeta_gate_exit,
                        allow_gate=allow_gate_exit,
                        extra={
                            "exit_reason": "TIMEOUT",
                            "pos_side": pos_side,
                            "age_minutes": float(age_minutes),
                            "max_minutes": int(max_pos_minutes),
                            "price": float(price),
                            "open_time": str(open_time),
                            "pnl_pct": float(pnl_pct),
                            "min_profit_to_keep_pct": float(MIN_PROFIT_TO_KEEP_PCT),
                        },
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
            emit_strategy_event(
                event_type="BLOCKED",
                reason="BOT_DISABLED",
                price=price,
                candle_open_time=open_time,
                info={"trend": trend, "ema_fast": ema_fast, "ema_slow": ema_slow},
            )
            return

        # --- ENTRY FILTERS ---
        hour_utc = open_time.hour
        if hour_utc in DISABLE_HOURS_SET:
            emit_strategy_event(
                event_type="BLOCKED",
                reason="DISABLE_HOURS",
                price=price,
                candle_open_time=open_time,
                info={"trend": trend, "ema_fast": ema_fast, "ema_slow": ema_slow},
            )
            return

        # Daily loss gate (ledger-based) — PAPER blocks, LIVE alerts (throttled)
        if DAILY_MAX_LOSS_PCT > 0:
            daily_pct = compute_daily_pnl_pct(symbol=SYMBOL, interval=INTERVAL, current_price=price)
            if daily_pct <= -DAILY_MAX_LOSS_PCT:
                if cfg_effective.trading_mode == "LIVE":
                    # Throttle: max 1 alert / 15 min per symbol/interval/strategy
                    conn = get_db_conn()
                    try:
                        if should_emit_throttled_event(
                            conn=conn,
                            symbol=SYMBOL,
                            interval=INTERVAL,
                            strategy=STRATEGY_NAME,
                            event_type="ALERT",
                            reason="DAILY_MAX_LOSS_SHADOW",
                            throttle_seconds=15 * 60,
                        ):
                            emit_alert_throttled(open_time=open_time, price=price, info={"daily_pct": float(daily_pct), "limit_pct": float(DAILY_MAX_LOSS_PCT)})
                    finally:
                        conn.close()
                    # LIVE: nie blokujemy
                else:
                    emit_strategy_event(
                        event_type="BLOCKED",
                        reason="DAILY_MAX_LOSS",
                        price=price,
                        candle_open_time=open_time,
                        info={"daily_pct": float(daily_pct), "limit_pct": float(DAILY_MAX_LOSS_PCT)},
                    )
                    return

        if trend == "FLAT":
            if trend_changed:
                emit_strategy_event(
                    event_type="BLOCKED",
                    decision=None,
                    reason="TREND_FLAT_STATE",
                    price=price,
                    candle_open_time=open_time,
                    info={"trend": trend, "ema_fast": ema_fast, "ema_slow": ema_slow},
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
            # LONG-only (SPOT). No new entries in downtrend.
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
            return
        
                # --- REGIME GATE (ENTRY ONLY) ---
        log_regime_gate_event(
            symbol=SYMBOL,
            interval=INTERVAL,
            strategy=STRATEGY_NAME,
            decision="ENTRY_CHECK",
            allow=snap["allow_gate_entry"],
            rmeta=snap["rmeta_gate"],
            extra_meta={
                "stage": "ENTRY",
                "side": decision,
                "price": float(price),
                "ema_fast": float(ema_fast),
                "ema_slow": float(ema_slow),
                "trend": trend,
                "open_time": str(open_time),
                "reason": reason,
            },
        )

        if not snap["allow_gate_entry"]:
            emit_strategy_event(
                event_type="BLOCKED",
                decision=decision,
                reason="REGIME_BLOCK",
                price=price,
                candle_open_time=open_time,
                info={"rmeta": snap["rmeta_gate"], "trend": trend},
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
            allow_live_orders=snap["allowed_orders_entry"],
            allow_meta=snap["allow_meta_entry"],
            is_exit=False,
        )
        if not res["ledger_ok"]:
            logging.info("RSI: entry blocked/failed -> not opening position.")
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

        opened = False
        if decision == "BUY":
            opened = open_position("LONG", qty_btc, price, candle_open_time=open_time)
        elif decision == "SELL":
            opened = open_position("SHORT", qty_btc, price, candle_open_time=open_time)

        emit_strategy_event(
            event_type="RUN_END",
            decision=None,
            reason="EXIT",
            price=float(price) if price is not None else None,
            candle_open_time=open_time,
            info={},
        )

        if not opened:
            logging.error("TREND DESYNC: order recorded but position not opened -> HALT for safety.")
            set_mode("HALT", reason="DESYNC: order recorded but position not opened")
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
    finally:
        conn.close()
    if cfg.trading_mode == "LIVE" and cfg.regime_enabled and cfg.regime_mode == "DRY_RUN":
        logging.warning("LIVE + REGIME_ENABLED but REGIME_MODE=DRY_RUN. Consider ENFORCE for profitability.")
    while True:
        loop_start = time.perf_counter()
        try:
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
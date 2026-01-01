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

LAST_TREND_STATE = None

cfg = RuntimeConfig.from_env()

def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


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
        if regime in ("RANGE_LOWVOL", "SHOCK"):
            would_block = True
            why = f"TREND blocked in {regime}"
    elif strategy_name in ("SUPER_TREND", "ST"):
        if regime in ("SHOCK",):
            would_block = True
            why = f"SUPER_TREND blocked in {regime}"

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

    panic = (os.environ.get("PANIC_DISABLE_TRADING", "0") == "1")

    # ENTRY gate: zależny od reżimu
    allow_gate_entry, rmeta_gate = regime_allows(STRATEGY_NAME, SYMBOL, INTERVAL, bc)

    # Czy wolno wysłać LIVE order (ENTRY uwzględnia regime gate)
    allowed_orders_entry, allow_meta_entry = can_trade(cfg_effective, regime_allows_trade=allow_gate_entry, panic_disable_trading=panic)

    # EXIT: zawsze dozwolony (regime nie może blokować zamknięcia pozycji)
    allowed_orders_exit, allow_meta_exit   = can_trade(cfg_effective, regime_allows_trade=True, panic_disable_trading=panic)

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
    emit_strategy_event(
        event_type="POSITION_OPENED",
        decision="BUY" if str(side).upper() == "LONG" else "SELL",
        reason="OK",
        price=entry_price,
        candle_open_time=None,
        info={"side": side, "qty": float(qty), "entry_price": float(entry_price)},
    )
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
        emit_strategy_event(
            event_type="POSITION_CLOSED",
            decision=None,
            reason=reason,
            price=exit_price,
            candle_open_time=None,
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
        "ORDER_QTY_BTC=%.8f|MAX_DIST_FROM_EMA_FAST_PCT=%.2f|"
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
    """
    Guard-first:
    1) Rezerwuj slot w DB (simulated_orders) -> idempotencja per candle.
    2) Dopiero potem (opcjonalnie) wysyłaj LIVE.
    3) Jeśli LIVE się nie uda, loguj, ale slot DB zostaje jako audyt (attempt).
       (Docelowo: warto rozbudować ledger o status, ale to minimalny bezpieczny fix.)
    """

    # 1) DB guard / ledger reservation FIRST
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
    )

    if not inserted:
        # Nic więcej nie rób: to jest mutex i ochrona przed duplikatem
        emit_strategy_event(
            event_type="BLOCKED",
            decision=side,
            reason="DB_GUARD_DUPLICATE",
            price=price,
            candle_open_time=candle_open_time,
            info={"is_exit": bool(is_exit), "qty_btc": float(qty_btc), "reason_text": reason},
        )
        return False

    emit_strategy_event(
        event_type="SIM_ORDER_CREATED",
        decision=side,
        reason="LEDGER_OK",
        price=price,
        candle_open_time=candle_open_time,
        info={"is_exit": bool(is_exit), "qty_btc": float(qty_btc), "reason_text": reason},
    )

    # 2) LIVE order AFTER successful DB reservation
    if cfg_used.trading_mode == "LIVE":
        if not allow_live_orders:
            logging.warning(
                "LIVE ORDER SUPPRESSED symbol=%s side=%s qty=%.8f allow=false why=%s",
                cfg_used.symbol, side, float(qty_btc), (allow_meta or {}).get("why")
            )
            emit_strategy_event(
                event_type="BLOCKED",
                decision=side,
                reason="LIVE_ORDER_SUPPRESSED",
                price=price,
                candle_open_time=candle_open_time,
                info={"allow_meta": allow_meta, "is_exit": bool(is_exit)},
            )
            # Zwracamy False, bo LIVE nie poszło (ale ledger mamy)
            return False

        client_order_id = make_client_order_id(
            cfg_used.symbol, STRATEGY_NAME, cfg_used.interval, side, candle_open_time
        )
        resp = place_live_order(
            client,
            cfg_used.symbol,
            side,
            qty_btc,
            trading_mode=cfg_used.trading_mode,
            live_orders_enabled=cfg_used.live_orders_enabled,
            quote_asset=cfg_used.quote_asset,
            client_order_id=client_order_id,
            panic_disable_trading=(os.environ.get("PANIC_DISABLE_TRADING", "0") == "1"),
            live_max_notional=float(os.environ.get("LIVE_MAX_NOTIONAL", "0")),
            skip_balance_precheck=is_exit,
        )

        if resp is None:
            logging.error(
                "LIVE order failed/blocked AFTER ledger reservation (symbol=%s side=%s qty=%.8f).",
                cfg_used.symbol, side, float(qty_btc),
            )
            emit_strategy_event(
                event_type="BLOCKED",
                decision=side,
                reason="LIVE_ORDER_FAILED",
                price=price,
                candle_open_time=candle_open_time,
                info={"is_exit": bool(is_exit), "client_order_id": client_order_id},
            )
            return False

        emit_strategy_event(
            event_type="LIVE_ORDER_SENT",
            decision=side,
            reason="OK",
            price=price,
            candle_open_time=candle_open_time,
            info={"is_exit": bool(is_exit), "client_order_id": client_order_id, "resp": resp},
        )

    # Jeżeli PAPER -> inserted=True oznacza sukces; jeżeli LIVE -> sukces oznacza też, że poszło lub było suppressed/failed
    return True


def safe_close_if_open(current_price: float, candle_open_time, bc, cfg_effective):
    pos = get_open_position()
    if not pos:
        return False

    _, side, qty, _, _ = pos

    exit_side = "SELL" if str(side).upper() == "LONG" else "BUY"
    reason = "PANIC CLOSE LONG" if exit_side == "SELL" else "PANIC CLOSE SHORT"

    panic = (os.environ.get("PANIC_DISABLE_TRADING", "0") == "1")
    allowed_orders, allow_meta = can_trade(cfg_effective, regime_allows_trade=True, panic_disable_trading=panic)

    inserted = execute_and_record(
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
    bc = snap["bc"]
    cfg_effective = snap["cfg_effective"]

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
                inserted = execute_and_record(
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
                if not inserted:
                    logging.info("TREND: exit blocked by DB guard -> skipping close.")
                    return

                closed = close_position(exit_price=price, reason="TAKE_PROFIT_LONG")
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
                    cfg_used=cfg_effective,
                    allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],
                    is_exit=True,
                )
                if not inserted:
                    logging.info("TREND: exit blocked by DB guard -> skipping close.")
                    return

                closed = close_position(exit_price=price, reason="STOP_LOSS_LONG")
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
                    cfg_used=cfg_effective,
                    allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],
                    is_exit=True,
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
                    cfg_used=cfg_effective,
                    allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],
                    is_exit=True,
                )
                if not inserted:
                    logging.info("TREND: exit blocked by DB guard -> skipping close.")
                    return
                close_position(exit_price=price, reason="STOP_LOSS_SHORT")
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
                        inserted = execute_and_record(
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
                        if inserted:
                            close_position(exit_price=price, reason="EARLY_CUT_LONG")
                        return

                elif pos_side == "SHORT":
                    pnl_pct = (pos_entry_price - price) / pos_entry_price * 100.0
                    if pnl_pct <= -EARLY_EXIT_MAX_LOSS_PCT:
                        reason_early = f"TREND EARLY_CUT SHORT {pnl_pct:.2f}% <= -{EARLY_EXIT_MAX_LOSS_PCT:.2f}% after {age_minutes:.1f}m"
                        inserted = execute_and_record(
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
                        if inserted:
                            close_position(exit_price=price, reason="EARLY_CUT_SHORT")
                        return

        # TIMEOUT
        if MAX_POSITION_MINUTES > 0 and pos_entry_time is not None:
            if pos_entry_time.tzinfo is None:
                pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)

            age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0
            if age_minutes >= MAX_POSITION_MINUTES:
                # If profit is too small, do not keep holding; exit to avoid negative TIMEOUT expectancy
                if pos_side == "LONG":
                    pnl_pct = (price - pos_entry_price) / pos_entry_price * 100.0
                else:
                    pnl_pct = (pos_entry_price - price) / pos_entry_price * 100.0

                if pnl_pct < MIN_PROFIT_TO_KEEP_PCT:
                    logging.info(
                        "TREND: TIMEOUT exit - pnl_pct %.2f%% < MIN_PROFIT_TO_KEEP_PCT %.2f%%",
                        pnl_pct, MIN_PROFIT_TO_KEEP_PCT
                    )
                side_timeout = "SELL" if pos_side == "LONG" else "BUY"
                reason_timeout = f"TREND TIMEOUT {pos_side} {age_minutes:.1f}m >= {MAX_POSITION_MINUTES}m"
                allow_gate_exit, rmeta_gate_exit = regime_allows(STRATEGY_NAME, SYMBOL, INTERVAL, bc)
                log_regime_gate_on_exit(
                    decision=side_timeout,
                    rmeta_gate=rmeta_gate_exit,
                    allow_gate=allow_gate_exit,
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
                    cfg_used=cfg_effective,
                    allow_live_orders=snap["allowed_orders_exit"],
                    allow_meta=snap["allow_meta_exit"],
                    is_exit=True,
                )
                if not inserted:
                    logging.info("TREND: exit blocked by DB guard -> skipping close.")
                    return
                close_position(exit_price=price, reason="TIMEOUT")
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

    if DAILY_MAX_LOSS_PCT > 0:
        daily_pct = compute_daily_pnl_pct(SYMBOL, INTERVAL, price)
        if daily_pct <= -DAILY_MAX_LOSS_PCT:
            emit_strategy_event(
                event_type="BLOCKED",
                reason="DAILY_MAX_LOSS",
                price=price,
                candle_open_time=open_time,
                info={"trend": trend, "ema_fast": ema_fast, "ema_slow": ema_slow},
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
    # logujemy zdarzenie (DRY_RUN would_block albo ENFORCE block)
    log_regime_gate_event(
        symbol=SYMBOL,
        interval=INTERVAL,
        strategy=STRATEGY_NAME,
        decision=decision,
        allow=snap["allow_gate_entry"],
        rmeta=snap["rmeta_gate"],
        extra_meta={
            "price": float(price),
            "ema_fast": float(ema_fast),
            "ema_slow": float(ema_slow),
            "trend": trend,
            "open_time": str(open_time),
        },
    )

    rmeta = snap["rmeta_gate"] or {}

    if rmeta.get("mode") == "DRY_RUN" and rmeta.get("would_block"):
        logging.info(
            "REGIME_GATE|dry_run would_block|strategy=%s|symbol=%s|interval=%s|decision=%s|regime=%s|why=%s",
            STRATEGY_NAME, SYMBOL, INTERVAL, decision,
            rmeta.get("regime"), rmeta.get("why"),
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
        logging.info(
            "REGIME_GATE|blocked entry|strategy=%s|symbol=%s|interval=%s|decision=%s|regime=%s|why=%s|mode=%s",
            STRATEGY_NAME, SYMBOL, INTERVAL, decision,
            rmeta.get("regime"), rmeta.get("why"), rmeta.get("mode"),
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

    qty_btc = ORDER_QTY_BTC
    logging.info("TREND: opening %s at %.2f (%s)", decision, price, reason)

    inserted = execute_and_record(
        side=decision,
        price=price,
        qty_btc=qty_btc,
        reason=reason,
        candle_open_time=open_time,
        cfg_used=cfg_effective,
        allow_live_orders=snap["allowed_orders_entry"],
        allow_meta=snap["allow_meta_entry"],
        is_exit=False,
    )
    if not inserted:
        logging.info("TREND: entry blocked by DB guard -> skipping open_position.")
        return

    opened = False
    if decision == "BUY":
        opened = open_position("LONG", qty_btc, price)
    elif decision == "SELL":
        opened = open_position("SHORT", qty_btc, price)

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
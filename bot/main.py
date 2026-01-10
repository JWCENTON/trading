import os
import time
import json
import hashlib
import logging
import psycopg2
import pandas as pd
from common.schema import ensure_schema
from dataclasses import replace
from datetime import datetime, timezone, date
from psycopg2.extras import execute_batch
from binance.client import Client
from common.execution import place_live_order
from common.runtime import RuntimeConfig
from common.permissions import can_trade
from common.bot_control import upsert_defaults, read as read_bot_control


SYMBOL = os.environ.get("SYMBOL", "BTCUSDC")

QUOTE_ASSET = os.environ.get("QUOTE_ASSET", "USDC").upper()
if not SYMBOL.endswith(QUOTE_ASSET):
    raise RuntimeError(f"SYMBOL={SYMBOL} does not match QUOTE_ASSET={QUOTE_ASSET}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

STRATEGY_NAME = os.environ.get("STRATEGY_NAME", "RSI").upper()

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

PAPER_START_USDC = float(os.environ.get("PAPER_START_USDC", "100"))

STOP_LOSS_PCT = float(os.environ.get("STOP_LOSS_PCT", "0.8"))        # % (np 0.8 = 0.8%)
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "1.2"))     # % (np 1.2 = 1.2%)

DISABLE_HOURS = os.environ.get("DISABLE_HOURS", "")
DISABLE_HOURS_SET = {int(h.strip()) for h in DISABLE_HOURS.split(",") if h.strip() != ""}

ENTRY_BUFFER_PCT = float(os.environ.get("ENTRY_BUFFER_PCT", "0.002"))
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
  SYMBOL, INTERVAL, cfg.spot_mode, REGIME_MAX_AGE_SECONDS, cfg.trading_mode
)

def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


def make_client_order_id(symbol: str, strategy: str, interval: str, side: str, candle_open_time) -> str:
    raw = f"{symbol}|{strategy}|{interval}|{side}|{candle_open_time.isoformat()}"
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]
    # 36 znaków max, czytelny prefix
    return f"{symbol[:6]}-{strategy[:6]}-{interval}-{side}-{h}"[:36]


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
    1) Rezerwuj slot w DB (simulated_orders) -> idempotencja per candle + is_exit.
    2) Dopiero potem (opcjonalnie) LIVE.
    3) Jeśli LIVE się nie uda / suppressed -> audyt zostaje w ledger.
    """

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
        return False

    emit_strategy_event(
        event_type="SIM_ORDER_CREATED",
        decision=side,
        reason="LEDGER_OK",
        price=price,
        candle_open_time=candle_open_time,
        info={"is_exit": bool(is_exit), "qty_btc": float(qty_btc), "reason_text": reason},
    )

    # 2) LIVE AFTER ledger reservation
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
                info={"allow_meta": allow_meta, "is_exit": bool(is_exit), "reason_text": reason},
            )
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
            live_orders_enabled=(cfg_used.live_orders_enabled or is_exit),
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

    return True


def regime_allows(strategy_name: str, symbol: str, interval: str, bc):
    """
    Zwraca: (allow: bool, meta: dict)
    DRY_RUN: zawsze allow=True, ale meta mówi czy 'would_block'.
    """
    would_block = False
    why = "ok"
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


def emit_blocked(*, reason: str, decision: str | None, price: float, candle_open_time, info: dict | None = None):
    emit_strategy_event(
        event_type="BLOCKED",
        decision=decision,
        reason=reason,
        price=price,
        candle_open_time=candle_open_time,
        info=info or {},
    )

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

    meta = {"rmeta": rmeta}
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

    # Czy wolno wysłać LIVE order (uwzględnia TRADING_MODE + LIVE_ORDERS_ENABLED + PANIC etc)
    allowed_orders_entry, allow_meta_entry = can_trade(cfg_effective, regime_allows_trade=allow_gate_entry, is_exit=False, panic_disable_trading=panic)

    # EXIT: zawsze dozwolony (regime nie może blokować zamknięcia pozycji)
    allowed_orders_exit, allow_meta_exit   = can_trade(cfg_effective, regime_allows_trade=True, is_exit=True, panic_disable_trading=panic)

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


def seed_default_params_from_env(conn):
    defaults = {
        "RSI_OVERSOLD": float(RSI_OVERSOLD),
        "RSI_OVERBOUGHT": float(RSI_OVERBOUGHT),
        "STOP_LOSS_PCT": float(STOP_LOSS_PCT),
        "TAKE_PROFIT_PCT": float(TAKE_PROFIT_PCT),
        "MAX_POSITION_MINUTES": float(MAX_POSITION_MINUTES),
        "DAILY_MAX_LOSS_PCT": float(DAILY_MAX_LOSS_PCT),
        "ORDER_QTY_BTC": float(ORDER_QTY_BTC),
        "MAX_DIST_FROM_EMA_PCT": float(MAX_DIST_FROM_EMA_PCT),
        "TREND_BUFFER": float(TREND_BUFFER),
        "ENTRY_BUFFER_PCT": float(ENTRY_BUFFER_PCT),
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
        logging.info("Seeded default RSI params from ENV for %s/%s.", SYMBOL, STRATEGY_NAME)
    else:
        logging.info("RSI params already exist in DB for %s/%s – skipping ENV seed.", SYMBOL, STRATEGY_NAME)

    cur.close()

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
            symbol, interval, strategy, side, float(price), float(qty_btc),
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

    if inserted:
        logging.info("RSI simulated %s @ %.2f qty=%.8f is_exit=%s (%s)", side, float(price), float(qty_btc), bool(is_exit), reason)
    else:
        logging.info("RSI simulated order skipped by DB guard (symbol=%s interval=%s strategy=%s candle_open_time=%s is_exit=%s).",
                     symbol, interval, strategy, candle_open_time, bool(is_exit))
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


def load_runtime_params():
    global RSI_OVERSOLD, RSI_OVERBOUGHT, STOP_LOSS_PCT, TAKE_PROFIT_PCT
    global MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT, ORDER_QTY_BTC
    global MAX_DIST_FROM_EMA_PCT, TREND_BUFFER, ENTRY_BUFFER_PCT

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
        logging.info("RSI %s: no strategy_params rows found, using ENV/defaults.", SYMBOL)
        return

    params = {name: float(value) for (name, value) in rows}

    def clamp(v, lo, hi):
        return max(lo, min(hi, v))

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
        STOP_LOSS_PCT = clamp(params["STOP_LOSS_PCT"], 0.05, 5.0)
    if "TAKE_PROFIT_PCT" in params:
        TAKE_PROFIT_PCT = clamp(params["TAKE_PROFIT_PCT"], 0.05, 10.0)

    if "MAX_POSITION_MINUTES" in params:
        MAX_POSITION_MINUTES = int(clamp(params["MAX_POSITION_MINUTES"], 1, 24*60))

    if "DAILY_MAX_LOSS_PCT" in params:
        DAILY_MAX_LOSS_PCT = clamp(params["DAILY_MAX_LOSS_PCT"], 0.0, 10.0)

    if "ORDER_QTY_BTC" in params:
        ORDER_QTY_BTC = clamp(params["ORDER_QTY_BTC"], 0.00001, 1.0)

    if "MAX_DIST_FROM_EMA_PCT" in params:
        MAX_DIST_FROM_EMA_PCT = clamp(params["MAX_DIST_FROM_EMA_PCT"], 0.01, 5.0)

    if "TREND_BUFFER" in params:
        TREND_BUFFER = clamp(params["TREND_BUFFER"], 0.0001, 0.05)

    if "ENTRY_BUFFER_PCT" in params:
        ENTRY_BUFFER_PCT = clamp(params["ENTRY_BUFFER_PCT"], 0.0001, 0.05)

    logging.info(
        "RUNTIME_PARAMS|symbol=%s|strategy=%s|RSI_OVERSOLD=%.2f|RSI_OVERBOUGHT=%.2f|"
        "STOP_LOSS_PCT=%.2f|TAKE_PROFIT_PCT=%.2f|MAX_POSITION_MINUTES=%d|DAILY_MAX_LOSS_PCT=%.2f|"
        "ORDER_QTY_BTC=%.8f|MAX_DIST_FROM_EMA_PCT=%.2f|TREND_BUFFER=%.4f|ENTRY_BUFFER_PCT=%.4f",
        SYMBOL, STRATEGY_NAME, RSI_OVERSOLD, RSI_OVERBOUGHT,
        STOP_LOSS_PCT, TAKE_PROFIT_PCT, MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT,
        ORDER_QTY_BTC, MAX_DIST_FROM_EMA_PCT, TREND_BUFFER, ENTRY_BUFFER_PCT
    )

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


# =========================
# STRATEGY LOGIC
# =========================


def run_strategy(row):
    open_time = (row[0] if row else None)
    price_for_events = float(row[4]) if row and row[4] is not None else None
    emit_strategy_event(
        event_type="RUN_START",
        decision=None,
        reason="ENTER",
        price=price_for_events,
        candle_open_time=(row[0] if row else None),
        info={"has_row": bool(row)},
    )
    try:
        if not row:
            logging.info("RSI: no candle data available. (ROW==none)")
            emit_strategy_event(
                event_type="BLOCKED",
                decision=None,
                reason="NO_ROW",
                price=None,
                candle_open_time=None,
                info={"has_row": False},
            )
            return

        open_time, open_px, high_px, low_px, close_px, ema_21, rsi_14 = row

        close_price = float(close_px) if close_px is not None else None
        high_price  = float(high_px) if high_px is not None else None
        low_price   = float(low_px) if low_px is not None else None
        price = close_price  # jednoznacznie: price = close świecy

        if close_price is None or high_price is None or low_price is None:
            fallback_price = float(open_px) if open_px is not None else None
            emit_strategy_event(
                event_type="BLOCKED",
                reason="CANDLE_MISSING_FIELDS",
                decision=None,
                price=fallback_price,
                candle_open_time=open_time,
                info={"open": open_px, "high": high_px, "low": low_px, "close": close_px},
            )
            return

        if ema_21 is None or rsi_14 is None:
            emit_blocked(
                reason="INDICATORS_NOT_READY",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"ema_21": ema_21, "rsi_14": rsi_14},
            )
            heartbeat({"price": price, "open_time": str(open_time), "status": "indicators_not_ready"})
            return

        ema_val = float(ema_21)
        rsi_val = float(rsi_14)

        snap = get_runtime_snapshot(price=price, open_time=open_time)
        # Telemetry baseline per candle: zawsze zapisujemy gate status (tak jak TREND)
        log_regime_gate_event(
            symbol=SYMBOL,
            interval=INTERVAL,
            strategy=STRATEGY_NAME,
            decision="TICK",
            allow=snap["allow_gate_entry"],
            rmeta=snap["rmeta_gate"],
            extra_meta={
                "price": float(price),
                "ema_21": float(ema_val) if ema_21 is not None else None,
                "rsi_14": float(rsi_val) if rsi_14 is not None else None,
                "open_time": str(open_time),
                "note": "baseline per candle",
            },
        )
        bc = snap["bc"]
        cfg_effective = snap["cfg_effective"]
        time_exit_enabled = bool(getattr(cfg_effective, "time_exit_enabled", True))
        max_pos_minutes = int(getattr(cfg_effective, "max_position_minutes", MAX_POSITION_MINUTES))

        # HARD stop
        if bc.mode == "HALT":
            emit_blocked(
                reason="BOT_MODE_HALT",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"mode": "HALT"},
            )
            heartbeat({**snap["heartbeat"], "status": "HALT"})
            return

        # heartbeat zawsze
        pos = get_open_position()
        heartbeat({
            **snap["heartbeat"],
            "ema_21": float(ema_val),
            "rsi_14": float(rsi_val),
            "has_position": bool(pos is not None),
            "pos_side": (str(pos[1]).upper() if pos else None),
            "pos_qty": (float(pos[2]) if pos else None),
            "pos_entry_price": (float(pos[3]) if pos else None),
        })

        # PANIC zachowujemy jak masz (panic close + HALT)
        if bc.mode == "PANIC":
            emit_strategy_event(
                event_type="CONFIG_APPLIED",
                decision=None,
                reason="PANIC_TRIGGERED",
                price=price,
                candle_open_time=open_time,
                info={"mode": "PANIC"},
            )
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
                    is_exit=True,
                )
                if inserted:
                    close_position(exit_price=price, reason="PANIC")
                    pos = None
                else:
                    emit_blocked(
                        reason="EXIT_BLOCKED",
                        decision=exit_side,
                        price=price,
                        candle_open_time=open_time,
                        info={"exit_reason": "PANIC"},
                    )
                    return
            if not pos:
                emit_blocked(
                    reason="PANIC_NO_POSITION",
                    decision=None,
                    price=price,
                    candle_open_time=open_time,
                    info={},
                )
            set_mode("HALT", reason="Panic executed; halting.")
            return


        # =========================
        # 1) EXIT (zawsze dozwolony)
        # =========================
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
                    exec_px = tp_level if cfg_effective.trading_mode == "PAPER" else price
                    reason = f"RSI TAKE PROFIT LONG intrabar high={high_price:.2f} >= tp={tp_level:.2f}"
                    inserted = execute_and_record("SELL", exec_px, qty_f, reason, open_time,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        is_exit=True,
                    )
                    if inserted:
                        close_position(exit_price=exec_px, reason="TAKE_PROFIT")
                    if not inserted:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision="SELL",
                            price=price,
                            candle_open_time=open_time,
                            info={"exit_reason": "TAKE_PROFIT"},
                        )
                        return
                    return

                # SL intrabar
                if STOP_LOSS_PCT > 0 and low_price <= sl_level:
                    exec_px = sl_level if cfg_effective.trading_mode == "PAPER" else price
                    reason = f"RSI STOP LOSS LONG intrabar low={low_price:.2f} <= sl={sl_level:.2f}"
                    inserted = execute_and_record("SELL", exec_px, qty_f, reason, open_time, cfg_used=cfg_effective, allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"], is_exit=True,)
                    if inserted:
                        close_position(exit_price=exec_px, reason="STOP_LOSS")
                    if not inserted:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision="SELL",
                            price=price,
                            candle_open_time=open_time,
                            info={"exit_reason": "STOP_LOSS"},
                        )
                        return
                    return

            # --- SHORT ---
            else:
                tp_level = entry_f * (1.0 - TAKE_PROFIT_PCT / 100.0)
                sl_level = entry_f * (1.0 + STOP_LOSS_PCT / 100.0)

                # TP intrabar
                if TAKE_PROFIT_PCT > 0 and low_price <= tp_level:
                    exec_px = tp_level if cfg_effective.trading_mode == "PAPER" else price
                    reason = f"RSI TAKE PROFIT SHORT intrabar low={low_price:.2f} <= tp={tp_level:.2f}"
                    inserted = execute_and_record("BUY", exec_px, qty_f, reason, open_time, cfg_used=cfg_effective, allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"], is_exit=True,)
                    if inserted:
                        close_position(exit_price=exec_px, reason="TAKE_PROFIT_SHORT")
                    if not inserted:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision="BUY",
                            price=price,
                            candle_open_time=open_time,
                            info={"exit_reason": "TAKE_PROFIT"},
                        )
                        return
                    return

                # SL intrabar
                if STOP_LOSS_PCT > 0 and high_price >= sl_level:
                    exec_px = sl_level if cfg_effective.trading_mode == "PAPER" else price
                    reason = f"RSI STOP LOSS SHORT intrabar high={high_price:.2f} >= sl={sl_level:.2f}"
                    inserted = execute_and_record("BUY", exec_px, qty_f, reason, open_time, cfg_used=cfg_effective, allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"], is_exit=True,)
                    if inserted:
                        close_position(exit_price=exec_px, reason="STOP_LOSS_SHORT")
                    if not inserted:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision="BUY",
                            price=price,
                            candle_open_time=open_time,
                            info={"exit_reason": "STOP_LOSS"},
                        )
                        return
                    return

            # TIME EXIT (dla obu stron)
            if time_exit_enabled and max_pos_minutes > 0 and pos_entry_time is not None:
                if pos_entry_time.tzinfo is None:
                    pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)
                age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0
                if age_minutes >= max_pos_minutes:
                    side_timeout = "SELL" if pos_side_u == "LONG" else "BUY"
                    reason_timeout = f"RSI TIME_EXIT {pos_side_u} {age_minutes:.1f}m >= {max_pos_minutes}m"

                    emit_strategy_event(
                        event_type="EXIT_TIME",
                        decision=side_timeout,
                        reason="TIME_EXIT",
                        price=price,
                        candle_open_time=open_time,
                        info={
                            "pos_side": pos_side_u,
                            "age_minutes": float(age_minutes),
                            "max_minutes": int(max_pos_minutes),
                        },
                    )

                    inserted = execute_and_record(
                        side=side_timeout,
                        price=price,
                        qty_btc=qty_f,
                        reason=reason_timeout,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        is_exit=True,
                    )
                    if inserted:
                        close_position(exit_price=price, reason="TIME_EXIT")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision=side_timeout,
                            price=price,
                            candle_open_time=open_time,
                            info={"exit_reason": "TIME_EXIT"},
                        )
                    return
            emit_strategy_event(
                event_type="BLOCKED",
                decision=None,
                reason="POSITION_OPEN_NO_EXIT",
                price=price,
                candle_open_time=open_time,
                info={
                    "pos_side": pos_side_u,
                    "pos_qty": float(qty_f),
                    "pos_entry_price": float(entry_f),
                },
            )        
            return  # OPEN i nic nie zamyka

        # =========================
        # 2) ENTRY (tu stosujemy filtry + REGIME)
        # =========================

        # filtr godzinowy
        hour_utc = open_time.hour
        if hour_utc in DISABLE_HOURS_SET:
            emit_blocked(
                reason="DISABLE_HOURS",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"hour_utc": int(hour_utc), "disable_hours": sorted(list(DISABLE_HOURS_SET))},
            )
            return
        
        if not bc.enabled:
            emit_blocked(
                reason="BOT_DISABLED",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"enabled": False},
            )
            return

        # daily max loss
        if DAILY_MAX_LOSS_PCT > 0:
            daily_pct = compute_daily_pnl_pct(SYMBOL, INTERVAL, price)
            if daily_pct <= -DAILY_MAX_LOSS_PCT:
                emit_blocked(
                    reason="DAILY_MAX_LOSS",
                    decision=None,
                    price=price,
                    candle_open_time=open_time,
                    info={"daily_pct": float(daily_pct), "limit_pct": float(DAILY_MAX_LOSS_PCT)},
                )
                return

        # filtr dystansu od EMA (jako %)
        dist_from_ema_pct = abs(price - ema_val) / ema_val * 100.0
        if MAX_DIST_FROM_EMA_PCT > 0 and dist_from_ema_pct > MAX_DIST_FROM_EMA_PCT:
            emit_blocked(
                reason="MAX_DIST_FROM_EMA",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"dist_pct": float(dist_from_ema_pct), "max_dist_pct": float(MAX_DIST_FROM_EMA_PCT), "ema_21": float(ema_val)},
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
            emit_blocked(
                reason="NO_SIGNAL",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"rsi_14": float(rsi_val), "oversold": float(RSI_OVERSOLD), "overbought": float(RSI_OVERBOUGHT)},
            )
            return
        
        # ENTRY_BUFFER vs EMA (chroni przed wejściem "za blisko EMA" / w złym miejscu)
        # BUY: wymagamy zejścia poniżej EMA o ENTRY_BUFFER_PCT
        if decision == "BUY":
            buy_level = ema_val * (1.0 - ENTRY_BUFFER_PCT)
            if price > buy_level:
                emit_blocked(
                    reason="ENTRY_BUFFER_BLOCK",
                    decision="BUY",
                    price=price,
                    candle_open_time=open_time,
                    info={"buy_level": float(buy_level), "ema_21": float(ema_val), "entry_buffer_pct": float(ENTRY_BUFFER_PCT)},
                )
                return

        # SELL (short): wymagamy wyjścia powyżej EMA o ENTRY_BUFFER_PCT
        if decision == "SELL":
            sell_level = ema_val * (1.0 + ENTRY_BUFFER_PCT)
            if price < sell_level:
                emit_blocked(
                    reason="ENTRY_BUFFER_BLOCK",
                    decision="SELL",
                    price=price,
                    candle_open_time=open_time,
                    info={"sell_level": float(sell_level), "ema_21": float(ema_val), "entry_buffer_pct": float(ENTRY_BUFFER_PCT)},
                )
                return

        # SPOT: nie otwieramy shortów
        if decision == "SELL" and cfg_effective.spot_mode:
            emit_blocked(
                reason="SPOT_SHORT_BLOCK",
                decision="SELL",
                price=price,
                candle_open_time=open_time,
                info={"spot_mode": True, "trading_mode": cfg_effective.trading_mode},
            )
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
            emit_blocked(
                reason="REGIME_BLOCK",
                decision=decision,
                price=price,
                candle_open_time=open_time,
                info={"rmeta": snap["rmeta_gate"]},
            )
            return
        
        emit_strategy_event(
            event_type="SIGNAL",
            decision=decision,
            reason="OK",
            price=price,
            candle_open_time=open_time,
            info={"rsi_14": float(rsi_val), "ema_21": float(ema_val), "reason_text": reason},
        )

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
            is_exit=False,
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
            logging.error("RSI DESYNC: order recorded but position not opened -> HALT for safety.")
            set_mode("HALT", reason="DESYNC: order recorded but position not opened")
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
    finally:
        conn.close()
    while True:
        loop_start = time.perf_counter()
        try:
            load_runtime_params()
            rows = fetch_klines(limit=200)   # patrz Zmiana 3
            save_klines(rows)
            update_indicators()

            latest = get_last_closed_candle()
            if latest:
                open_time = latest[0]  # (open_time, open, high, low, close, ema_21, rsi_14)
                emit_strategy_event(
                    event_type="TICK",
                    decision=None,
                    reason="LOOP",
                    price=float(latest[4]) if latest[4] is not None else None,  # close
                    candle_open_time=open_time,
                    info={"last_processed": str(LAST_PROCESSED_OPEN_TIME), "open_time": str(open_time)},
                )
                # Uruchamiaj logikę tylko raz na nową świecę
                if LAST_PROCESSED_OPEN_TIME != open_time:
                    LAST_PROCESSED_OPEN_TIME = open_time
                    run_strategy(latest)
                else:
                    emit_strategy_event(
                        event_type="BLOCKED",
                        decision=None,
                        reason="NO_NEW_CANDLE",
                        price=float(latest[4]) if latest[4] is not None else None,
                        candle_open_time=open_time,
                        info={"open_time": str(open_time), "last_processed": str(LAST_PROCESSED_OPEN_TIME)},
                    )
                    logging.info("RSI: no new candle yet (%s) -> skip strategy.", str(open_time))

        except Exception as e:
            logging.exception("RSI loop error")
            emit_strategy_event(
                event_type="ERROR",
                decision=None,
                reason="EXCEPTION",
                price=None,
                candle_open_time=None,
                info={"error": str(e)},
            )

        logging.info("RSI loop finished in %.3f s", time.perf_counter() - loop_start)
        time.sleep(60)


if __name__ == "__main__":
    logging.info("Starting RSI bot %s %s", SYMBOL, INTERVAL)
    main_loop()
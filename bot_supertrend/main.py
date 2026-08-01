# main_supertrend.py
import os
import time
import json
import logging
import hashlib
from decimal import Decimal, ROUND_DOWN
from dataclasses import replace
from datetime import datetime, timezone, date
from typing import Callable
from common.adaptive_time_exit import hard_time_exit_enabled, time_exit_policy_name
from common.safe_json import sanitize_json
from common.entry_trace import record_entry_trace_shadow
from common.flags import exchange_mytrades_enabled
from common.execution import place_live_exit_maker_then_market
from common.daily_loss import should_emit_daily_loss_shadow
from common.alerts import emit_alert_throttled
from common.exchange_ingest_trades import ingest_my_trades
from common.exchange_identity import normalize_exchange_source
from common.execution import build_live_client_order_id, build_live_entry_intent_client_order_id
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from common.db import db_write_conn, get_db_conn, read_only_db_conn
from common.bot_control import upsert_defaults, read as read_bot_control
from common.runtime import RuntimeConfig
from common.exchange_client import get_market_data_client
from common.permissions import can_trade
from common.regime_gate import decide_regime_gate, emit_regime_gate_event
from common.execution import place_live_order
from common.sizing import compute_qty_from_notional as common_compute_qty_from_notional
from common.daily_loss import compute_daily_loss_pct_positions, should_block_daily_loss_positions
from common.user_settings import SYSTEM_MIN_ENTRY_USDC, get_user_settings_snapshot
from common.win_streak import get_recent_win_streak
from common.exit_guards.profit_lock import ProfitLockConfig, evaluate_profit_lock
from common.exit_guards.profit_lock_events import emit_profit_lock_event_once
from common.position_path import load_position_path_snapshot
from common.exit_reason_context import build_exit_reason_context
from common.decision_contract import (
    DecisionReason,
    DecisionSubtype,
    EvaluationContext,
    ExecutionOutcome,
    ExecutionStage,
    FinalDecision,
    normalize_entry_execution_outcome,
)
from common.partial_exit import apply_partial_exit_result
from common.supertrend_terminal_outcome import (
    expire_paper_supertrend_slot_canaries,
    paper_supertrend_entries_enabled,
    persist_exit_intent,
    reconcile_terminal_compatibility_outcome,
)
from common.final_decision_observation_sink import finalize_decision_observation
from common.simulated_execution_evidence import (
    execute_paper_exit_after_preflight,
    paper_position_mutation_allowed_cursor,
    record_simulated_fill_evidence,
)


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
PROFIT_LOCK_CONFIG = ProfitLockConfig.from_env()

QUOTE_ASSET = os.environ.get("QUOTE_ASSET", "USDC").upper()
if not SYMBOL.endswith(QUOTE_ASSET):
    raise RuntimeError(f"SYMBOL={SYMBOL} does not match QUOTE_ASSET={QUOTE_ASSET}")

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

cfg = RuntimeConfig.from_env()
_exchange_client = None

INDICATOR_PROGRESS_INTERVAL_S = 90.0
INDICATOR_PROGRESS_ROW_STEP = 5000
IndicatorProgressCallback = Callable[[str, int, int], None]


def get_exchange_client():
    """Return the process-wide exchange client, creating it on first runtime use."""
    global _exchange_client
    if _exchange_client is None:
        try:
            _exchange_client = get_market_data_client()
        except Exception:
            logging.exception("SUPERTREND exchange client initialization failed")
            raise
    return _exchange_client

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

    info = get_exchange_client().get_symbol_info(SYMBOL)
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
                        json.dumps(sanitize_json(info or {}), default=_json_default, allow_nan=False),
                    ),
                )
            conn.commit()
        finally:
            conn.close()

        record_entry_trace_shadow(
            symbol=sym,
            interval=itv,
            strategy=strat,
            event_type=event_type,
            decision=decision,
            reason=reason,
            price=price,
            candle_open_time=candle_open_time,
            info=info or {},
        )
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
                json.dumps(sanitize_json(info or {}), default=_json_default, allow_nan=False),
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
        (SYMBOL, STRATEGY_NAME, INTERVAL, json.dumps(sanitize_json(info), default=_json_default, allow_nan=False)),
    )
    conn.commit()
    cur.close()
    conn.close()


def lifecycle_heartbeat(status: str, *, duration_s=None, error=None, **metadata):
    """Best-effort loop-boundary heartbeat without replacing strategy metadata."""
    info = {
        "lifecycle_status": str(status),
        "lifecycle_updated_at": datetime.now(timezone.utc).isoformat(),
        "cycle_duration_s": (
            round(float(duration_s), 3) if duration_s is not None else None
        ),
        "last_cycle_error": None if error is None else str(error)[:2000],
    }
    info.update(metadata)
    conn = None
    cur = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.bot_heartbeat(symbol, strategy, interval, last_seen, info)
            VALUES (%s, %s, %s, now(), %s::jsonb)
            ON CONFLICT ON CONSTRAINT bot_heartbeat_symbol_strategy_interval_key
            DO UPDATE SET
              last_seen=now(),
              info=COALESCE(bot_heartbeat.info, '{}'::jsonb) || EXCLUDED.info;
            """,
            (
                SYMBOL,
                STRATEGY_NAME,
                INTERVAL,
                json.dumps(sanitize_json(info), default=_json_default, allow_nan=False),
            ),
        )
        conn.commit()
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        logging.exception("SUPERTREND lifecycle heartbeat failed")
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


class IndicatorProgressHeartbeat:
    """Emit heartbeat only for time-gated progress made by the main thread."""

    def __init__(
        self,
        *,
        cycle_started_at: str,
        interval_s: float = INDICATOR_PROGRESS_INTERVAL_S,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.cycle_started_at = cycle_started_at
        self.interval_s = float(interval_s)
        self.monotonic = monotonic
        self.last_emit_at = monotonic()
        self.last_phase = None
        self.last_processed_rows = -1

    def __call__(self, phase: str, processed_rows: int, total_rows: int):
        phase = str(phase)
        processed_rows = int(processed_rows)
        total_rows = max(0, int(total_rows))
        made_progress = (
            phase != self.last_phase
            or processed_rows > self.last_processed_rows
        )
        if not made_progress:
            return

        self.last_phase = phase
        self.last_processed_rows = processed_rows
        now = self.monotonic()
        if now - self.last_emit_at < self.interval_s:
            return

        self.last_emit_at = now
        progress_pct = (
            round((processed_rows / total_rows) * 100.0, 3)
            if total_rows > 0
            else None
        )
        lifecycle_heartbeat(
            "INDICATOR_PROGRESS",
            phase=phase,
            processed_rows=processed_rows,
            total_rows=total_rows,
            progress_pct=progress_pct,
            cycle_started_at=self.cycle_started_at,
            progress_updated_at=datetime.now(timezone.utc).isoformat(),
        )

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


def open_position(
    side: str,
    qty: float,
    entry_price: float,
    entry_client_order_id: str | None,
    *,
    entry_time=None,
) -> int | None:
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
        VALUES (%s, %s, %s, 'OPEN', %s, %s, %s, COALESCE(%s, now()), %s)
        RETURNING id;
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL, side, float(qty), float(entry_price),
         entry_time,
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


def close_position(
    exit_price: float,
    reason: str,
    candle_open_time,
    *,
    expected_position_id: int | None = None,
) -> bool:
    conn = get_db_conn()
    cur = conn.cursor()

    if expected_position_id is None:
        cur.execute(
            """
            SELECT id, side, entry_price, entry_time
            FROM positions
            WHERE symbol=%s AND strategy=%s AND interval=%s AND status='OPEN'
            ORDER BY entry_time DESC
            LIMIT 1;
            """,
            (SYMBOL, STRATEGY_NAME, INTERVAL),
        )
    else:
        cur.execute(
            """
            SELECT id, side, entry_price, entry_time
            FROM positions
            WHERE id=%s AND symbol=%s AND strategy=%s AND interval=%s
              AND status='OPEN'
            """,
            (
                int(expected_position_id), SYMBOL, STRATEGY_NAME, INTERVAL,
            ),
        )
    row = cur.fetchone()

    if not row:
        conn.commit()
        cur.close()
        conn.close()
        logging.info("SUPERTREND: close_position skipped - no OPEN position found.")
        return False

    pos_id, pos_side, pos_entry_price, pos_entry_time = row

    if (
        str(os.getenv("TRADING_MODE", "")).upper() == "PAPER"
        and not paper_position_mutation_allowed_cursor(
            cur,
            position_id=int(pos_id),
            deployment_id=os.environ.get(
                "DEPLOYMENT_ID",
                os.environ.get("WALTRADE_DEPLOYMENT_ID", "local-paper"),
            ),
        )
    ):
        conn.rollback()
        cur.close()
        conn.close()
        return False

    enriched_reason = build_exit_reason_context(
        base_reason=reason,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        interval=INTERVAL,
        side=pos_side,
        entry_price=pos_entry_price,
        exit_price=exit_price,
        entry_time=pos_entry_time,
        asof_time=candle_open_time,
        profit_lock_config=PROFIT_LOCK_CONFIG,
    )

    cur.execute(
        """
        UPDATE positions
        SET status='CLOSED', exit_price=%s, exit_time=now(), exit_reason=%s
        WHERE id=%s AND status='OPEN'
        RETURNING id;
        """,
        (float(exit_price), enriched_reason, int(pos_id)),
    )
    closed = cur.fetchone() is not None
    conn.commit()
    cur.close()
    conn.close()

    if closed:
        logging.info("SUPERTREND: position CLOSED reason=%s exit=%.2f", enriched_reason, float(exit_price))
        emit_strategy_event(
            event_type="POSITION_CLOSED",
            decision=None,
            reason=enriched_reason,
            price=float(exit_price),
            candle_open_time= candle_open_time,
            info={"exit_reason": enriched_reason, "exit_price": float(exit_price)},
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
        "ORDER_QTY_BTC=%.8f|EXIT_ON_FLIP_DOWN=%s|"
        "PROFIT_LOCK_ENABLED=%s|PROFIT_LOCK_ARM_PCT=%.3f|PROFIT_LOCK_FLOOR_PCT=%.3f|PROFIT_LOCK_TRAIL_DROP_PCT=%.3f|PROFIT_LOCK_MIN_AGE_MINUTES=%.1f|PROFIT_LOCK_STRATEGIES=%s",
        SYMBOL, STRATEGY_NAME, ATR_PERIOD, ST_MULTIPLIER, EMA_PERIOD, RSI_PERIOD,
        MIN_ATR_PCT, STOP_LOSS_PCT, TAKE_PROFIT_PCT, MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT,
        ORDER_QTY_BTC, EXIT_ON_FLIP_DOWN,
        bool(PROFIT_LOCK_CONFIG.enabled), float(PROFIT_LOCK_CONFIG.arm_pct), float(PROFIT_LOCK_CONFIG.floor_pct),
        float(PROFIT_LOCK_CONFIG.trail_drop_pct), float(PROFIT_LOCK_CONFIG.min_age_minutes),
        ",".join(sorted(PROFIT_LOCK_CONFIG.strategies)),
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
    inserted_row = cur.fetchone()
    inserted = inserted_row[0] if inserted_row else None
    conn.commit()
    cur.close()
    conn.close()
    return inserted


def execute_and_record(
    *,
    side: str,
    price: float,
    qty_btc: float,
    reason: str,
    candle_open_time,
    is_exit: bool,
    cfg_used: RuntimeConfig,
    allow_live_orders: bool,
    allow_meta: dict,
):
    def action(preflight):
        return _execute_and_record_after_paper_exit_preflight(
            side=side, price=price, qty_btc=qty_btc, reason=reason,
            candle_open_time=candle_open_time, is_exit=is_exit,
            cfg_used=cfg_used, allow_live_orders=allow_live_orders,
            allow_meta=allow_meta,
            paper_position_id=(preflight.position_id if preflight else None),
        )

    if str(cfg_used.trading_mode).upper() == "PAPER" and is_exit:
        return execute_paper_exit_after_preflight(
            get_db_conn,
            deployment_id=os.environ.get(
                "DEPLOYMENT_ID",
                os.environ.get("WALTRADE_DEPLOYMENT_ID", "local-paper"),
            ),
            symbol=cfg_used.symbol,
            strategy=STRATEGY_NAME,
            interval=cfg_used.interval,
            exit_trigger=reason,
            decision=side,
            price=price,
            candle_open_time=candle_open_time,
            emit_event=emit_strategy_event,
            action=action,
        )
    return action(None)


def _execute_and_record_after_paper_exit_preflight(
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
    paper_position_id: int | None = None,
):
    """
    Guard-first (jak RSI/BBRANGE):
    1) Rezerwuj slot w DB (simulated_orders) -> idempotencja per candle + is_exit
    2) Potem opcjonalnie LIVE
    3) Ledger zostaje jako audyt niezależnie od LIVE

    Zwraca dict:
      ledger_ok/live_attempted/live_ok/blocked_reason/client_order_id/resp
    """
    trading_mode = str(cfg_used.trading_mode).upper()
    if trading_mode not in {"PAPER", "LIVE"}:
        logging.error(
            "SUPERTREND: invalid trading mode; execution fail-closed mode=%r",
            cfg_used.trading_mode,
        )
        return {
            "ledger_ok": False,
            "live_attempted": False,
            "live_ok": False,
            "paper_executed": False,
            "blocked_reason": "INVALID_TRADING_MODE",
            "client_order_id": None,
            "resp": None,
        }

    deployment_id = os.environ.get(
        "DEPLOYMENT_ID", os.environ.get("WALTRADE_DEPLOYMENT_ID", "local-paper")
    )
    if trading_mode == "PAPER" and not is_exit:
        try:
            entries_enabled, gate_reason = paper_supertrend_entries_enabled(
                get_db_conn, deployment_id=deployment_id,
                symbol=cfg_used.symbol, interval=cfg_used.interval,
            )
        except Exception:
            logging.exception("PAPER SUPERTREND entry gate unavailable; fail closed")
            entries_enabled, gate_reason = False, "ENTRY_GATE_UNAVAILABLE"
        if not entries_enabled:
            emit_strategy_event(
                event_type="BLOCKED", decision=side,
                reason="PAPER_SUPERTREND_ENTRY_CONTAINED", price=price,
                candle_open_time=candle_open_time,
                info={"operator_reason": gate_reason, "entry_only": True},
            )
            return {
                "ledger_ok": True, "live_attempted": False, "live_ok": False,
                "paper_executed": False,
                "blocked_reason": "PAPER_SUPERTREND_ENTRY_CONTAINED",
                "client_order_id": None, "resp": None,
            }

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

    # PAPER: materialize the standard position lifecycle after ledger success.
    # LIVE remains below and never creates a position before confirmed execution.
    if trading_mode == "PAPER":
        if is_exit:
            open_row = get_open_position() if paper_position_id is None else None
            position_id = (
                int(paper_position_id)
                if paper_position_id is not None
                else int(open_row[0]) if open_row else None
            )
            if position_id is None:
                return {
                    "ledger_ok": False,
                    "live_attempted": False,
                    "live_ok": False,
                    "paper_executed": False,
                    "blocked_reason": "EXIT_NO_OPEN_POSITION",
                    "client_order_id": None,
                    "resp": None,
                    "position_id": None,
                    "simulated_order_id": int(inserted),
                }
        else:
            position_id = open_position(
                "LONG",
                float(qty_btc),
                float(price),
                None,
                entry_time=candle_open_time,
            )
            if position_id is None:
                return {
                    "ledger_ok": False,
                    "live_attempted": False,
                    "live_ok": False,
                    "paper_executed": False,
                    "blocked_reason": "PAPER_POSITION_OPEN_FAILED",
                    "client_order_id": None,
                    "resp": None,
                    "position_id": None,
                    "simulated_order_id": int(inserted),
                }
        if is_exit:
            try:
                persist_exit_intent(
                    get_db_conn, position_id=int(position_id),
                    simulated_order_id=int(inserted), deployment_id=deployment_id,
                    symbol=cfg_used.symbol, interval=cfg_used.interval,
                    canonical_reason_code=(
                        "FLIP_DOWN_EXIT"
                        if "FLIP DOWN" in str(reason).upper()
                        else "PROFIT_LOCK_EXIT"
                        if "PROFIT_LOCK" in str(reason).upper()
                        else "PANIC"
                        if "PANIC" in str(reason).upper()
                        else "SUPERTREND_EXIT"
                    ),
                    raw_reason=str(reason), exit_decision_at=candle_open_time,
                )
            except Exception:
                logging.exception(
                    "SUPERTREND exit intent persistence unavailable; safe exit continues"
                )
        try:
            record_simulated_fill_evidence(
                get_db_conn,
                client=get_exchange_client(),
                simulated_order_id=int(inserted),
                position_id=int(position_id),
                environment="paper",
                deployment_id=os.environ.get(
                    "DEPLOYMENT_ID",
                    os.environ.get("WALTRADE_DEPLOYMENT_ID", "local-paper"),
                ),
            )
        except Exception:
            logging.exception(
                "FINANCIAL_TRUTH_EVIDENCE|SUPERTREND paper persistence unavailable"
            )
        reconciliation = None
        if is_exit:
            try:
                reconciliation = reconcile_terminal_compatibility_outcome(
                    get_db_conn, position_id=int(position_id),
                    simulated_order_id=int(inserted), deployment_id=deployment_id,
                )
                if not reconciliation.applied and reconciliation.reason != "ALREADY_RECONCILED":
                    emit_strategy_event(
                        event_type="SUPERTREND_OUTCOME_UNRESOLVED",
                        decision=side, reason=reconciliation.reason, price=price,
                        candle_open_time=candle_open_time,
                        info={"position_id": int(position_id),
                              "simulated_order_id": int(inserted)},
                    )
            except Exception:
                logging.exception("SUPERTREND terminal compatibility reconciliation failed")
        return {
            "ledger_ok": True,
            "live_attempted": False,
            "live_ok": True,
            "paper_executed": True,
            "blocked_reason": None,
            "client_order_id": None,
            "resp": None,
            "position_id": int(position_id),
            "simulated_order_id": int(inserted),
            "terminal_outcome_reconciled": bool(
                reconciliation and
                (reconciliation.applied or reconciliation.reason == "ALREADY_RECONCILED")
            ),
        }

    # Explicit LIVE branch: unknown modes returned before any mutation above.
    assert trading_mode == "LIVE"
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
            get_exchange_client(),
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
            order_purpose=("EXIT" if is_exit else "ENTRY"),
            strategy=STRATEGY_NAME,
            interval=cfg_used.interval,
            exchange_source=normalize_exchange_source(
                os.environ.get("EXCHANGE")
                or os.environ.get("EXCHANGE_PROVIDER")
                or "BINANCE"
            ),
        )
        conn_exec.commit()
    finally:
        try:
            conn_exec.close()
        except Exception:
            pass

    if not resp or not resp.get("ok"):
        failed_entry_outcome = (
            normalize_entry_execution_outcome(
                resp or {},
                requested_qty=qty_btc,
                client_order_id=client_order_id,
            )
            if not is_exit
            else None
        )
        emit_strategy_event(
            event_type="BLOCKED",
            decision=side,
            reason="LIVE_ORDER_FAILED",
            price=price,
            candle_open_time=candle_open_time,
            info={"is_exit": bool(is_exit), "client_order_id": client_order_id, "resp": (resp or {}).get("resp")},
        )
        result = {
            "ledger_ok": True,
            "live_attempted": (
                failed_entry_outcome.attempted if failed_entry_outcome else True
            ),
            "live_ok": False,
            "blocked_reason": "LIVE_ORDER_FAILED",
            "client_order_id": client_order_id,
            "resp": (resp or {}).get("resp"),
        }
        if failed_entry_outcome is not None:
            result.update({
                "order_accepted": failed_entry_outcome.order_accepted,
                "executed": failed_entry_outcome.executed,
                "fully_executed": failed_entry_outcome.fully_executed,
                "executed_qty": failed_entry_outcome.executed_qty,
                "requested_qty": failed_entry_outcome.requested_qty,
                "order_id": failed_entry_outcome.order_id,
                "exchange_status": failed_entry_outcome.exchange_status,
            })
        return result

    raw = (resp or {}).get("resp") or {}
    entry_outcome = None
    if not is_exit:
        entry_outcome = normalize_entry_execution_outcome(
            resp,
            requested_qty=qty_btc,
            client_order_id=client_order_id,
        )
        live_ok = bool(entry_outcome.executed)
        status_raw = str(entry_outcome.exchange_status or "").upper()
        executed_f = float(entry_outcome.executed_qty or 0.0)
    else:
        # Existing exit interpretation remains unchanged.
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
        status_raw = str(raw.get("status", "")).upper()
        executed_raw = raw.get("executedQty")
        try:
            executed_f = float(executed_raw) if executed_raw is not None else 0.0
        except Exception:
            executed_f = 0.0

    emit_strategy_event(
        event_type="LIVE_ORDER_SENT",
        decision=side,
        reason=(
            "OK" if live_ok
            else "ORDER_ACCEPTED_PENDING_FILL"
            if entry_outcome is not None and entry_outcome.order_accepted
            else "ORDER_REJECTED"
            if entry_outcome is not None
            else "ACK_NO_FILL"
        ),
        price=price,
        candle_open_time=candle_open_time,
        info={
            "is_exit": bool(is_exit),
            "client_order_id": client_order_id,
            "live_ok": bool(live_ok),
            "order_accepted": bool(
                entry_outcome.order_accepted
                if entry_outcome is not None
                else resp.get("order_accepted", False)
            ),
            "status": status_raw,
            "executed_qty": executed_f,
            "requested_qty": float(qty_btc),
            "order_purpose": "EXIT" if is_exit else "ENTRY",
            "exchange_source": normalize_exchange_source(
                os.environ.get("EXCHANGE")
                or os.environ.get("EXCHANGE_PROVIDER")
                or "BINANCE"
            ),
            "resp": raw,
        },
    )
    order_id = entry_outcome.order_id if entry_outcome is not None else raw.get("orderId")

    if not is_exit:
        if entry_outcome.executed and not order_id:
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
                "live_attempted": entry_outcome.attempted,
                "order_accepted": entry_outcome.order_accepted,
                "executed": entry_outcome.executed,
                "fully_executed": entry_outcome.fully_executed,
                "executed_qty": entry_outcome.executed_qty,
                "requested_qty": entry_outcome.requested_qty,
                "order_id": entry_outcome.order_id,
                "exchange_status": entry_outcome.exchange_status,
                "live_ok": False,
                "blocked_reason": "LIVE_ACK_MISSING_ORDER_ID",
                "client_order_id": entry_outcome.client_order_id,
                "resp": raw,
            }

        if entry_outcome.executed:
            try:
                pos_id = open_position_from_live_ack(
                    side="LONG",
                    qty=float(entry_outcome.executed_qty),
                    entry_price=float(price),
                    entry_client_order_id=str(client_order_id),
                    entry_order_id=str(order_id),
                )
            except Exception:
                pos_id = None
                logging.exception(
                    "SUPERTREND live entry fill position write failed cid=%s order_id=%s",
                    client_order_id, order_id,
                )
            if pos_id is None:
                emit_strategy_event(
                    event_type="BLOCKED",
                    decision=side,
                    reason="LIVE_ENTRY_FILL_BUT_POSITION_NOT_OPENED",
                    price=price,
                    candle_open_time=candle_open_time,
                    info={"client_order_id": client_order_id, "order_id": order_id,
                          "executed_qty": entry_outcome.executed_qty, "resp": raw},
                )
                return {
                    "ledger_ok": False,
                    "live_attempted": entry_outcome.attempted,
                    "order_accepted": entry_outcome.order_accepted,
                    "executed": entry_outcome.executed,
                    "fully_executed": entry_outcome.fully_executed,
                    "executed_qty": entry_outcome.executed_qty,
                    "requested_qty": entry_outcome.requested_qty,
                    "order_id": entry_outcome.order_id,
                    "exchange_status": entry_outcome.exchange_status,
                    "live_ok": True,
                    "blocked_reason": "LIVE_ENTRY_FILL_POSITION_WRITE_FAILED",
                    "client_order_id": entry_outcome.client_order_id,
                    "resp": raw,
                }

        return {
            "ledger_ok": True,
            "live_attempted": entry_outcome.attempted,
            "order_accepted": entry_outcome.order_accepted,
            "executed": entry_outcome.executed,
            "fully_executed": entry_outcome.fully_executed,
            "executed_qty": entry_outcome.executed_qty,
            "requested_qty": entry_outcome.requested_qty,
            "order_id": entry_outcome.order_id,
            "exchange_status": entry_outcome.exchange_status,
            "live_ok": entry_outcome.executed,
            "blocked_reason": (
                None if entry_outcome.executed
                else "ORDER_ACCEPTED_PENDING_FILL" if entry_outcome.order_accepted
                else "ORDER_REJECTED"
            ),
            "client_order_id": entry_outcome.client_order_id,
            "resp": raw,
        }

    if is_exit and not order_id:
        logging.error("SUPERTREND: LIVE EXIT ACK missing orderId pos_id=%s resp=%s", pos_id, raw)

    result = {
        "ledger_ok": True,
        "live_attempted": True,
        "order_accepted": bool(resp.get("order_accepted", False)),
        "executed": bool(resp.get("executed", False)),
        "fully_executed": bool(resp.get("fully_executed", False)),
        "executed_qty": float(resp.get("executed_qty") or executed_f),
        "requested_qty": float(resp.get("requested_qty") or qty_btc),
        "order_id": resp.get("order_id") or order_id,
        "exchange_status": resp.get("exchange_status") or status_raw,
        "live_ok": live_ok,
        "blocked_reason": None if live_ok else "ACK_NO_FILL",
        "client_order_id": client_order_id,
        "resp": (resp or {}).get("resp"),
    }
    mutation = apply_partial_exit_result(
        get_db_conn, result=result, position_id=int(pos_id),
        exchange_source=normalize_exchange_source(
            os.environ.get("EXCHANGE") or os.environ.get("EXCHANGE_PROVIDER") or "BINANCE"
        ),
        symbol=cfg_used.symbol, strategy=STRATEGY_NAME,
        interval=cfg_used.interval, side=side_u, exit_price=price,
        exit_reason=reason,
    )
    if mutation is not None:
        emit_strategy_event(
            event_type="POSITION_REDUCED", decision=side_u,
            reason="PARTIAL_EXECUTION", price=price,
            candle_open_time=candle_open_time,
            info={"execution_status": "PARTIAL",
                  "executed_qty": result["executed_qty"],
                  "applied_qty": result["position_qty_applied"],
                  "remaining_qty": result["position_remaining_qty"],
                  "fully_executed": False, "exit_reason": reason},
        )
    return result


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
    klines = get_exchange_client().get_klines(
        symbol=SYMBOL, interval=INTERVAL, limit=50
    )
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

def update_indicators(
    progress_callback: IndicatorProgressCallback | None = None,
):
    """
    Computes EMA, RSI, ATR and SuperTrend over full series, updates last ~50 candles.
    """
    progress_failed = False

    def report_progress(phase: str, processed_rows: int, total_rows: int):
        nonlocal progress_failed
        if progress_callback is None or progress_failed:
            return
        try:
            progress_callback(phase, processed_rows, total_rows)
        except Exception:
            progress_failed = True
            logging.exception("SUPERTREND indicator progress callback failed")

    with read_only_db_conn(get_db_conn) as read_conn:
        df = pd.read_sql_query(
            """
            SELECT id, open_time, open, high, low, close
            FROM candles
            WHERE symbol = %s AND interval = %s
            ORDER BY open_time
            """,
            read_conn,
            params=(SYMBOL, INTERVAL),
        )
    total_rows = len(df)
    report_progress("LOAD_HISTORY", total_rows, total_rows)

    if df.empty or len(df) < max(EMA_PERIOD, RSI_PERIOD, ATR_PERIOD) + 5:
        return

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    # EMA
    df["ema_21"] = close.ewm(span=EMA_PERIOD, adjust=False).mean()
    report_progress("EMA", total_rows, total_rows)

    # RSI
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    roll_up = gain.rolling(window=RSI_PERIOD).mean()
    roll_down = loss.rolling(window=RSI_PERIOD).mean()
    rs = roll_up / roll_down
    df["rsi_14"] = 100.0 - (100.0 / (1.0 + rs))
    report_progress("RSI", total_rows, total_rows)

    # ATR (EWMA of TR)
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr_14"] = tr.ewm(span=ATR_PERIOD, adjust=False).mean()
    report_progress("ATR", total_rows, total_rows)

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
        if i % INDICATOR_PROGRESS_ROW_STEP == 0:
            report_progress("SUPERTREND_LOOP", i, total_rows)

    report_progress("SUPERTREND_LOOP", total_rows, total_rows)

    df["supertrend_direction"] = st_dir
    df["supertrend"] = st_val

    last = df.tail(50)

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
    report_progress("PERSIST_LATEST", 0, len(data))
    with db_write_conn(get_db_conn) as (conn, cur):
        cur.executemany(sql, data)
        conn.commit()
        report_progress("PERSIST_LATEST", len(data), len(data))

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
def _as_aware_utc(value):
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _supertrend_evaluation_context(open_time, evaluation_started_at, snap=None):
    cfg_effective = snap["cfg_effective"] if snap is not None else cfg
    bc = snap["bc"] if snap is not None else None
    return EvaluationContext(
        deployment_id=os.environ.get("WALTRADE_DEPLOYMENT_ID", "UNKNOWN"),
        environment=DB_NAME,
        symbol=SYMBOL,
        interval=INTERVAL,
        strategy=STRATEGY_NAME,
        candle_open_time=_as_aware_utc(open_time),
        evaluation_started_at=evaluation_started_at,
        engine_name=STRATEGY_NAME,
        engine_version=os.environ.get("BOT_VERSION"),
        runtime_enabled=(bool(bc.enabled) if bc is not None else None),
        live_orders_enabled=(
            bool(snap["allowed_orders_entry"]) if snap is not None else None
        ),
        paper_mode=cfg_effective.trading_mode != "LIVE",
        context={"contract_version": "FINAL_DECISION_V1"},
    )


def _supertrend_outcome(result, cfg_effective):
    if isinstance(result, ExecutionOutcome):
        return result
    if cfg_effective.trading_mode == "LIVE":
        return normalize_entry_execution_outcome(
            result,
            requested_qty=float(result.get("requested_qty") or 0.0),
            client_order_id=result.get("client_order_id"),
            ledger_ok=bool(result.get("ledger_ok", False)),
        )
    return ExecutionOutcome.from_legacy(
        result, paper_mode=True
    )


def _supertrend_execution_decision(evaluation, result, cfg_effective, *,
                                   reason_code, reason_text, side, price,
                                   position_id=None, is_exit=False):
    outcome = _supertrend_outcome(result, cfg_effective)
    details = {
        "blocked_reason": outcome.blocked_reason,
        "live_attempted": outcome.attempted,
        "order_accepted": outcome.order_accepted,
        "live_ok": outcome.operation_succeeded,
        "executed": outcome.executed,
        "fully_executed": outcome.fully_executed,
        "executed_qty": outcome.executed_qty,
        "requested_qty": outcome.requested_qty,
        "ledger_ok": outcome.ledger_ok,
        "execution_stage": outcome.stage.value,
        "execution_result": outcome.raw,
        "legacy_reason": reason_text,
    }
    common = dict(
        finished_at=datetime.now(timezone.utc),
        reference_price=Decimal(str(price)),
        side=side,
        reason_text=reason_text,
        details=details,
    )
    if (
        is_exit
        and result.get("position_close_succeeded") is False
    ):
        return FinalDecision.technical_failure_result(
            evaluation, DecisionReason.EXECUTION_FAILED,
            DecisionSubtype.LEDGER_FAILURE,
            signal_detected=True,
            entry_attempted=outcome.attempted,
            order_submitted=outcome.order_accepted,
            **{
                **common,
                "reason_text": "POSITION_CLOSE_FAILED",
                "details": {
                    **details,
                    "blocked_reason": "POSITION_CLOSE_FAILED",
                    "position_close_succeeded": False,
                },
            },
        )
    if outcome.ledger_ok and cfg_effective.trading_mode != "LIVE":
        factory = FinalDecision.exit_result if is_exit else FinalDecision.paper_simulation
        return factory(evaluation, reason_code, position_id=position_id, **common) \
            if is_exit else factory(evaluation, reason_code, **common)
    if outcome.ledger_ok and outcome.fully_executed:
        factory = FinalDecision.exit_result if is_exit else FinalDecision.trade_executed_result
        return factory(evaluation, reason_code, position_id=position_id, **common)
    if outcome.stage in {ExecutionStage.SUPPRESSED, ExecutionStage.NOT_ATTEMPTED}:
        return FinalDecision.action_suppressed(
            evaluation, DecisionReason.EXECUTION_NOT_ATTEMPTED,
            finished_at=common["finished_at"], reference_price=common["reference_price"],
            side=side, reason_text=outcome.blocked_reason or reason_text,
            details=details,
        )
    subtype = (
        DecisionSubtype.LEDGER_FAILURE if not outcome.ledger_ok else
        DecisionSubtype.PARTIAL_EXECUTION if outcome.executed else
        DecisionSubtype.ORDER_ACCEPTED_NOT_FILLED if outcome.order_accepted else
        DecisionSubtype.ORDER_REJECTED
    )
    return FinalDecision.technical_failure_result(
        evaluation, DecisionReason.EXECUTION_FAILED, subtype,
        signal_detected=True, entry_attempted=outcome.attempted,
        order_submitted=outcome.order_accepted, trade_executed=outcome.executed,
        **common,
    )


def _close_supertrend_exit(
    result: dict,
    *,
    exit_price: float,
    reason: str,
    candle_open_time,
) -> dict:
    """Bind an exit outcome to the exact conditional position close."""
    if not result.get("ledger_ok"):
        return result
    if result.get("terminal_outcome_reconciled"):
        return {
            **result,
            "position_close_succeeded": True,
            "blocked_reason": None,
        }
    try:
        closed = close_position(
            exit_price=exit_price,
            reason=reason,
            candle_open_time=candle_open_time,
            expected_position_id=result.get("position_id"),
        )
    except Exception:
        logging.exception(
            "POSITION_CLOSE_FAILED strategy=%s position_id=%s simulated_order_id=%s "
            "exit_reason=%s symbol=%s interval=%s",
            STRATEGY_NAME, result.get("position_id"),
            result.get("simulated_order_id"), reason, SYMBOL, INTERVAL,
        )
        return {
            **result,
            "position_close_succeeded": False,
            "blocked_reason": "POSITION_CLOSE_FAILED",
        }
    if not closed:
        logging.error(
            "POSITION_CLOSE_FAILED strategy=%s position_id=%s simulated_order_id=%s "
            "exit_reason=%s symbol=%s interval=%s",
            STRATEGY_NAME, result.get("position_id"),
            result.get("simulated_order_id"), reason, SYMBOL, INTERVAL,
        )
        emit_strategy_event(
            event_type="POSITION_CLOSE_FAILED",
            decision=None,
            reason="POSITION_CLOSE_FAILED",
            price=exit_price,
            candle_open_time=candle_open_time,
            info={
                "position_id": result.get("position_id"),
                "simulated_order_id": result.get("simulated_order_id"),
                "exit_reason": reason,
                "symbol": SYMBOL,
                "interval": INTERVAL,
            },
        )
    return {
        **result,
        "position_close_succeeded": bool(closed),
        "blocked_reason": None if closed else "POSITION_CLOSE_FAILED",
    }


def _run_strategy(latest, prev):
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
    if str(os.environ.get("TRADING_MODE", "")).upper() == "PAPER":
        try:
            expire_paper_supertrend_slot_canaries(
                get_db_conn,
                deployment_id=os.environ.get(
                    "DEPLOYMENT_ID",
                    os.environ.get("WALTRADE_DEPLOYMENT_ID", "local-paper"),
                ),
            )
        except Exception:
            # Maintenance must never block an existing safe exit. Any later
            # entry attempt still fails closed in execute_and_record().
            logging.exception("PAPER SUPERTREND canary expiry maintenance failed")

    open_time, close_price, ema_21, rsi_14, atr_14, st_val, st_dir = latest
    _, prev_close, _, _, _, _, prev_st_dir = prev


    evaluation_started_at = datetime.now(timezone.utc)
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
        evaluation = _supertrend_evaluation_context(
            open_time, evaluation_started_at, snap=snap
        )
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
        time_exit_enabled = bool(getattr(cfg_effective, "time_exit_enabled", True)) and hard_time_exit_enabled()
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
            return FinalDecision.entry_suppressed(
                evaluation, DecisionReason.BOT_MODE_HALT,
                DecisionSubtype.EXECUTION_DISABLED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)), reason_text="BOT_MODE_HALT",
            )

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
                        res = _close_supertrend_exit(
                            res,
                            exit_price=price, reason="PANIC",
                            candle_open_time=open_time,
                        )
                    else:
                        emit_strategy_event(
                            event_type="BLOCKED",
                            decision="SELL",
                            reason="EXIT_BLOCKED",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return _supertrend_execution_decision(
                        evaluation, res, cfg_effective,
                        reason_code=DecisionReason.STRATEGY_EXIT,
                        reason_text="PANIC CLOSE LONG", side="SELL", price=price,
                        position_id=int(pos[0]), is_exit=True,
                    )
            # after panic, HALT
            set_mode("HALT", reason="Panic executed; halting.")
            return FinalDecision.entry_suppressed(
                evaluation, DecisionReason.BOT_MODE_HALT,
                DecisionSubtype.EXECUTION_DISABLED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)), reason_text="PANIC_HALTED",
            )

        # indicators readiness
        if st_dir is None or prev_st_dir is None or atr_14 is None:
            emit_strategy_event(
                event_type="BLOCKED",
                reason="INDICATORS_NOT_READY",
                price=price,
                candle_open_time=open_time,
                info={"st_dir": st_dir, "prev_st_dir": prev_st_dir, "atr_14": atr_14},
            )
            return FinalDecision.system_not_evaluated(
                evaluation, DecisionReason.INDICATORS_NOT_READY,
                finished_at=datetime.now(timezone.utc),
                reason_text="INDICATORS_NOT_READY",
                details={"st_dir": st_dir, "prev_st_dir": prev_st_dir, "atr_14": atr_14},
            )

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
                return FinalDecision.technical_failure_result(
                    evaluation, DecisionReason.UNKNOWN, DecisionSubtype.DATA_NOT_READY,
                    finished_at=datetime.now(timezone.utc),
                    reference_price=Decimal(str(price)), reason_text="UNSUPPORTED_POSITION_SIDE",
                    details={"pos_side": str(pos_side)},
                )

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
                    res = _close_supertrend_exit(
                        res,
                        exit_price=price, reason="TAKE_PROFIT_LONG",
                        candle_open_time=open_time,
                    )
                else:
                    emit_strategy_event(
                        event_type="BLOCKED",
                        decision="SELL",
                        reason="EXIT_BLOCKED",
                        price=price,
                        candle_open_time=open_time,
                        info={"res": res},
                    )
                return _supertrend_execution_decision(
                    evaluation, res, cfg_effective,
                    reason_code=DecisionReason.TAKE_PROFIT, reason_text=reason,
                    side="SELL", price=price, position_id=int(pos[0]), is_exit=True,
                )

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
                    res = _close_supertrend_exit(
                        res,
                        exit_price=price, reason="STOP_LOSS_LONG",
                        candle_open_time=open_time,
                    )
                else:
                    emit_strategy_event(
                        event_type="BLOCKED",
                        decision="SELL",
                        reason="EXIT_BLOCKED",
                        price=price,
                        candle_open_time=open_time,
                        info={"res": res},
                    )
                return _supertrend_execution_decision(
                    evaluation, res, cfg_effective,
                    reason_code=DecisionReason.STOP_LOSS, reason_text=reason,
                    side="SELL", price=price, position_id=int(pos[0]), is_exit=True,
                )

            # PROFIT LOCK: percent high-watermark guard for RSI/TREND/SUPERTREND/BBRANGE.
            if pos_entry_time is not None:
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
                profit_lock_decision = evaluate_profit_lock(
                    strategy=STRATEGY_NAME,
                    side=pos_side,
                    age_minutes=age_minutes,
                    entry_price=pos_entry_price,
                    current_price=price,
                    path=path,
                    config=PROFIT_LOCK_CONFIG,
                )

                profit_lock_info = {
                    "enabled": bool(PROFIT_LOCK_CONFIG.enabled),
                    "strategies": sorted(PROFIT_LOCK_CONFIG.strategies),
                    "arm_pct": float(PROFIT_LOCK_CONFIG.arm_pct),
                    "floor_pct": float(PROFIT_LOCK_CONFIG.floor_pct),
                    "trail_drop_pct": float(PROFIT_LOCK_CONFIG.trail_drop_pct),
                    "min_age_minutes": float(PROFIT_LOCK_CONFIG.min_age_minutes),
                    "reason_code": profit_lock_decision.reason_code,
                    "trigger_type": profit_lock_decision.trigger_type,
                    "peak_move_pct": float(profit_lock_decision.peak_move_pct),
                    "current_move_pct": float(profit_lock_decision.current_move_pct),
                    "age_minutes": float(profit_lock_decision.age_minutes),
                    "bars_seen": int(path.bars_seen),
                    "max_high": float(path.max_high),
                    "min_low": float(path.min_low),
                }
                profit_lock_event_type = None
                profit_lock_event_decision = "HOLD"
                if profit_lock_decision.triggered:
                    profit_lock_event_type = "PROFIT_LOCK_TRIGGERED"
                    profit_lock_event_decision = "EXIT"
                elif profit_lock_decision.reason_code == "ARMED_WAITING":
                    profit_lock_event_type = "PROFIT_LOCK_ARMED"
                elif profit_lock_decision.reason_code == "NOT_ARMED" and profit_lock_decision.peak_move_pct > 0:
                    profit_lock_event_type = "PROFIT_LOCK_PEAK_UPDATED"

                if profit_lock_event_type:
                    emit_profit_lock_event_once(
                        symbol=SYMBOL,
                        interval=INTERVAL,
                        strategy=STRATEGY_NAME,
                        event_type=profit_lock_event_type,
                        decision=profit_lock_event_decision,
                        reason=profit_lock_decision.reason_code,
                        price=price,
                        candle_open_time=open_time,
                        position_entry_time=pos_entry_time,
                        info=profit_lock_info,
                    )
                if profit_lock_decision.triggered:
                    exit_kind = str(profit_lock_decision.reason_code or "PROFIT_LOCK_LONG")
                    reason_profit_lock = (
                        f"SUPERTREND {exit_kind} {profit_lock_decision.trigger_type} "
                        f"peak={profit_lock_decision.peak_move_pct:.3f}% "
                        f"current={profit_lock_decision.current_move_pct:.3f}% "
                        f"floor={profit_lock_decision.floor_pct:.3f}% "
                        f"trail_drop={profit_lock_decision.trail_drop_pct:.3f}% "
                        f"age={profit_lock_decision.age_minutes:.1f}m"
                    )
                    emit_strategy_event(
                        event_type="EXIT_SIGNAL",
                        decision="SELL",
                        reason=exit_kind,
                        price=price,
                        candle_open_time=open_time,
                        info={
                            "trigger_type": profit_lock_decision.trigger_type,
                            "peak_move_pct": float(profit_lock_decision.peak_move_pct),
                            "current_move_pct": float(profit_lock_decision.current_move_pct),
                            "floor_pct": float(profit_lock_decision.floor_pct),
                            "trail_drop_pct": float(profit_lock_decision.trail_drop_pct),
                            "age_minutes": float(profit_lock_decision.age_minutes),
                            "bars_seen": int(path.bars_seen),
                            "max_high": float(path.max_high),
                            "min_low": float(path.min_low),
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
                        reason=reason_profit_lock,
                        candle_open_time=open_time,
                        is_exit=True,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                    )
                    if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                        res = _close_supertrend_exit(
                            res,
                            exit_price=price, reason=exit_kind,
                            candle_open_time=open_time,
                        )
                    else:
                        emit_strategy_event(
                            event_type="BLOCKED",
                            decision="SELL",
                            reason="EXIT_BLOCKED",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res, "exit_kind": exit_kind},
                        )
                    return _supertrend_execution_decision(
                        evaluation, res, cfg_effective,
                        reason_code=DecisionReason.PROFIT_LOCK,
                        reason_text=reason_profit_lock, side="SELL", price=price,
                        position_id=int(pos[0]), is_exit=True,
                    )

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
                        res = _close_supertrend_exit(
                            res,
                            exit_price=price, reason="TIME_EXIT_LONG",
                            candle_open_time=open_time,
                        )
                    else:
                        emit_strategy_event(
                            event_type="BLOCKED",
                            decision="SELL",
                            reason="EXIT_BLOCKED",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return _supertrend_execution_decision(
                        evaluation, res, cfg_effective,
                        reason_code=DecisionReason.TIME_EXIT, reason_text=reason,
                        side="SELL", price=price, position_id=int(pos[0]), is_exit=True,
                    )

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
                    res = _close_supertrend_exit(
                        res,
                        exit_price=price, reason="FLIP_DOWN_EXIT",
                        candle_open_time=open_time,
                    )
                else:
                    emit_strategy_event(
                        event_type="BLOCKED",
                        decision="SELL",
                        reason="EXIT_BLOCKED",
                        price=price,
                        candle_open_time=open_time,
                        info={"res": res},
                    )
                return _supertrend_execution_decision(
                    evaluation, res, cfg_effective,
                    reason_code=DecisionReason.STRATEGY_EXIT, reason_text=reason,
                    side="SELL", price=price, position_id=int(pos[0]), is_exit=True,
                )

            return FinalDecision.position_hold(
                evaluation, DecisionReason.POSITION_HOLD,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)), side=str(pos_side).upper(),
                position_id=int(pos[0]), reason_text="POSITION_HOLD",
                details={"change_pct": change_pct},
            )  # position open -> no new entries

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
            return FinalDecision.entry_suppressed(
                evaluation, DecisionReason.BOT_DISABLED,
                DecisionSubtype.EXECUTION_DISABLED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)), reason_text="BOT_DISABLED",
            )

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
            return FinalDecision.entry_suppressed(
                evaluation, DecisionReason.DISABLE_HOURS,
                DecisionSubtype.EXECUTION_DISABLED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)), reason_text="DISABLE_HOURS",
                details={"hour_utc": hour_utc, "disable_hours": sorted(DISABLE_HOURS_SET)},
            )

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
                    return FinalDecision.entry_blocked(
                        evaluation, DecisionReason.DAILY_MAX_LOSS_POSITIONS,
                        DecisionSubtype.RISK_BLOCKED,
                        finished_at=datetime.now(timezone.utc),
                        reference_price=Decimal(str(price)), side="BUY",
                        reason_text="DAILY_MAX_LOSS_POSITIONS",
                        signal_detected=False,
                        details={**pos_payload, "limit_pct": float(DAILY_MAX_LOSS_PCT)},
                    )

        # Volatility gate
        if atr_pct is None or atr_pct < MIN_ATR_PCT:
            emit_strategy_event(
                event_type="BLOCKED",
                reason="ATR_TOO_LOW",
                price=price,
                candle_open_time=open_time,
                info={"atr_pct": float(atr_pct) if atr_pct is not None else None, "min": float(MIN_ATR_PCT)},
            )
            return FinalDecision.signal_rejected(
                evaluation, DecisionReason.UNKNOWN,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)), side="BUY",
                reason_text="ATR_TOO_LOW",
                details={"atr_pct": atr_pct, "min": float(MIN_ATR_PCT)},
            )

        # Signal: flip -1 -> +1 => BUY
        if not (st_dir_prev == -1 and st_dir_curr == 1):
            emit_strategy_event(
                event_type="BLOCKED",
                reason="NO_SIGNAL",
                price=price,
                candle_open_time=open_time,
                info={"st_dir_prev": int(st_dir_prev), "st_dir_curr": int(st_dir_curr)},
            )
            return FinalDecision.no_trade(
                evaluation, DecisionReason.NO_SIGNAL,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)), reason_text="NO_SIGNAL",
                details={"st_dir_prev": st_dir_prev, "st_dir_curr": st_dir_curr},
            )

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
            return FinalDecision.entry_blocked(
                evaluation, DecisionReason.REGIME_BLOCK,
                DecisionSubtype.REGIME_BLOCKED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)), side="BUY",
                reason_text="REGIME_BLOCK",
                details={"why": gate_entry.why, "regime": gate_entry.regime, "meta": gate_entry.meta},
            )

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
            get_exchange_client(),
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
        raw_manual_entry_addon_usdc = settings_snapshot.get("manual_entry_addon_usdc")
        manual_entry_addon_usdc = 0.0 if raw_manual_entry_addon_usdc is None else float(raw_manual_entry_addon_usdc)

        raw_three_win_boost_usdc = settings_snapshot.get("three_win_boost_usdc")
        configured_three_win_boost_usdc = 0.0 if raw_three_win_boost_usdc is None else float(raw_three_win_boost_usdc)
        recent_win_streak = get_recent_win_streak(strategy=STRATEGY_NAME, symbol=SYMBOL, interval=INTERVAL, required_wins=3)
        applied_three_win_boost_usdc = configured_three_win_boost_usdc if recent_win_streak.eligible else 0.0
        base_target_notional = float(LIVE_TARGET_NOTIONAL)
        final_target_notional = base_target_notional + manual_entry_addon_usdc + applied_three_win_boost_usdc

        if (manual_entry_addon_usdc > 0 or applied_three_win_boost_usdc > 0):
            qty_btc, sizing_info = compute_qty_from_notional_safe(
                get_exchange_client(),
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
            return _supertrend_execution_decision(
                evaluation, res, cfg_effective,
                reason_code=DecisionReason.SSOT_EXECUTE_AND_RECORD,
                reason_text=reason, side="BUY", price=price,
            )

        if cfg_effective.trading_mode == "LIVE" and not res["live_ok"]:
            # NOT_ATTEMPTED jest już emitowane w execute_and_record() (SSOT)
            if not res.get("live_attempted", False):
                return _supertrend_execution_decision(
                    evaluation, res, cfg_effective,
                    reason_code=DecisionReason.EXECUTION_NOT_ATTEMPTED,
                    reason_text=reason, side="BUY", price=price,
                )

            # attempted, ale brak fill -> logujemy tutaj
            emit_strategy_event(
                event_type="BLOCKED",
                decision=decision,
                reason="LIVE_ENTRY_NOT_FILLED",
                price=price,
                candle_open_time=open_time,
                info={"res": res},
            )
            return _supertrend_execution_decision(
                evaluation, res, cfg_effective,
                reason_code=DecisionReason.EXECUTION_FAILED,
                reason_text=reason, side="BUY", price=price,
            )

        # 2) positions hard-truth
        # Position OPEN is created inside execute_and_record() (SSOT).
        emit_strategy_event(
            event_type="POSITION_OPENED",
            decision="BUY",
            reason="SSOT_EXECUTE_AND_RECORD",
            price=price,
            candle_open_time=open_time,
            info={
                "qty_btc": (
                    float(res["executed_qty"])
                    if cfg_effective.trading_mode == "LIVE"
                    else float(qty_btc)
                ),
                "position_id": res.get("position_id"),
                "simulated_order_id": res.get("simulated_order_id"),
            },
        )
        return _supertrend_execution_decision(
            evaluation, res, cfg_effective,
            reason_code=DecisionReason.SSOT_EXECUTE_AND_RECORD,
            reason_text=reason, side="BUY", price=price,
        )

    finally:
        emit_strategy_event(
            event_type="RUN_END",
            decision=None,
            reason="DONE",
            price=price,
            candle_open_time=open_time,
            info={},
        )


def run_strategy(latest, prev):
    return finalize_decision_observation(
        _run_strategy(latest, prev), source_service="bot-supertrend",
    )


LAST_PROCESSED_OPEN_TIME = None
# =========================
# Main Loop
# =========================
def run_loop_iteration(runtime_client, last_ingest_ts, progress_callback=None):
    global LAST_PROCESSED_OPEN_TIME
    # --- Exchange fills ingest (LIVE ONLY) ---
    # co 60s: pobierz exchange trades i zasil fills table + wyceń fee w USDC przez BNBUSDC candles
    if exchange_mytrades_enabled() and (time.time() - last_ingest_ts >= 60):
        n_trades, n_priced = ingest_my_trades(
            client=runtime_client,
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
            reason="EXCHANGE_MYTRADES",
            price=None,
            candle_open_time=None,
            info={"symbol": SYMBOL, "n_trades": int(n_trades), "n_fee_priced": int(n_priced)},
        )

    load_runtime_params()
    rows = fetch_klines()
    save_klines(rows)
    update_indicators(progress_callback=progress_callback)
    latest = get_last_closed_candle()
    prev = get_prev_closed_candle()
    if latest and prev:
        open_time = latest[0]
        if LAST_PROCESSED_OPEN_TIME != open_time:
            LAST_PROCESSED_OPEN_TIME = open_time
            run_strategy(latest, prev)
        else:
            logging.info("SUPERTREND: no new candle yet (%s) -> skip strategy.", str(open_time))
    return last_ingest_ts


def run_loop_cycle(runtime_client, last_ingest_ts):
    """Run one real worker iteration and record progress only at its boundaries."""
    loop_start = time.perf_counter()
    cycle_started_at = datetime.now(timezone.utc).isoformat()
    lifecycle_heartbeat("RUNNING", cycle_started_at=cycle_started_at)
    progress_heartbeat = IndicatorProgressHeartbeat(
        cycle_started_at=cycle_started_at,
    )
    error = None
    try:
        last_ingest_ts = run_loop_iteration(
            runtime_client,
            last_ingest_ts,
            progress_callback=progress_heartbeat,
        )
    except Exception as exc:
        error = exc
        logging.exception("SUPERTREND loop error")
        emit_strategy_event(
            event_type="ERROR",
            decision=None,
            reason="EXCEPTION",
            price=None,
            candle_open_time=None,
            info={"error": str(exc)},
        )
    finally:
        duration_s = time.perf_counter() - loop_start
        lifecycle_heartbeat(
            "ERROR" if error is not None else "CYCLE_OK",
            duration_s=duration_s,
            error=error,
        )
        logging.info("SUPERTREND loop finished in %.3f s", duration_s)
    return last_ingest_ts


def main_loop():
    runtime_client = get_exchange_client()
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
        last_ingest_ts = run_loop_cycle(runtime_client, last_ingest_ts)
        time.sleep(60)

if __name__ == "__main__":
    logging.info(
        "Starting SUPERTREND bot for %s %s (strategy=%s)...",
        SYMBOL, INTERVAL, STRATEGY_NAME,
    )
    main_loop()

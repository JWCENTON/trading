import os
import time
import json
import hashlib
import logging
import psycopg2
import pandas as pd
from common.flags import binance_mytrades_enabled
from common.schema import ensure_schema
from common.binance_ingest_trades import ingest_my_trades
from dataclasses import replace
from datetime import datetime, timezone, date
from common.daily_loss import should_emit_daily_loss_shadow
from common.alerts import emit_alert_throttled
from psycopg2.extras import execute_batch
from binance.client import Client
from common.execution import place_live_order, compute_live_qty_from_notional
from common.runtime import RuntimeConfig
from common.permissions import can_trade
from common.regime_gate import decide_regime_gate, emit_regime_gate_event
from common.bot_control import upsert_defaults, read as read_bot_control
from common.daily_loss import compute_daily_loss_pct_positions, should_block_daily_loss_positions
from common.db import get_db_conn
from common.execution import build_live_client_order_id

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


# =========================
# EXIT EXECUTION (Maker -> Market fallback) for RSI_SOFT_EXIT
# =========================
RSI_SOFT_EXIT_EXEC_MODE = os.environ.get("RSI_SOFT_EXIT_EXEC_MODE", "MAKER_THEN_MARKET").upper()
RSI_SOFT_EXIT_MAKER_OFFSET_BPS = float(os.environ.get("RSI_SOFT_EXIT_MAKER_OFFSET_BPS", "2"))  # 2 bps = 0.02%
RSI_SOFT_EXIT_MAKER_TIMEOUT_SEC = int(os.environ.get("RSI_SOFT_EXIT_MAKER_TIMEOUT_SEC", "7"))
RSI_SOFT_EXIT_MAKER_POLL_SEC = float(os.environ.get("RSI_SOFT_EXIT_MAKER_POLL_SEC", "1.0"))

# Soft exit (mean-reversion) — produkcyjnie:
# LONG: zamknij gdy RSI >= RSI_EXIT_OVERBOUGHT
# SHORT: zamknij gdy RSI <= RSI_EXIT_OVERSOLD
RSI_SOFT_EXIT_ENABLED = int(os.environ.get("RSI_SOFT_EXIT_ENABLED", "1"))
RSI_EXIT_OVERBOUGHT = float(os.environ.get("RSI_EXIT_OVERBOUGHT", str(RSI_OVERBOUGHT)))
RSI_EXIT_OVERSOLD = float(os.environ.get("RSI_EXIT_OVERSOLD", str(RSI_OVERSOLD)))

EMA_SLOPE_FILTER = int(os.environ.get("EMA_SLOPE_FILTER", "1"))  # 1=enabled, 0=disabled

PAPER_START_USDC = float(os.environ.get("PAPER_START_USDC", "100"))

STOP_LOSS_PCT = float(os.environ.get("STOP_LOSS_PCT", "0.8"))        # % (np 0.8 = 0.8%)
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "1.2"))     # % (np 1.2 = 1.2%)

DISABLE_HOURS = os.environ.get("DISABLE_HOURS", "")
DISABLE_HOURS_SET = {int(h.strip()) for h in DISABLE_HOURS.split(",") if h.strip() != ""}

ENTRY_BUFFER_PCT = float(os.environ.get("ENTRY_BUFFER_PCT", "0.002"))
TREND_BUFFER = float(os.environ.get("TREND_BUFFER", "0.001"))         # ułamek (np 0.001 = 0.1%)
MAX_POSITION_MINUTES = int(os.environ.get("MAX_POSITION_MINUTES", "450"))
DAILY_MAX_LOSS_PCT = float(os.environ.get("DAILY_MAX_LOSS_PCT", "0.5"))  # % (0 wyłącza)
DAILY_MAX_LOSS_BASE_USDC = float(os.environ.get("DAILY_MAX_LOSS_BASE_USDC", str(PAPER_START_USDC)))

ORDER_QTY_BTC = float(os.environ.get("ORDER_QTY_BTC", "0.0001"))
MAX_DIST_FROM_EMA_PCT = float(os.environ.get("MAX_DIST_FROM_EMA_PCT", "0.5"))  # % (np 0.5 = 0.5%)

# Rebound entry + vol/trend filters
RSI_REBOUND_DELTA = float(os.environ.get("RSI_REBOUND_DELTA", "3.0"))     # pkt RSI
ATR_MIN_PCT = float(os.environ.get("ATR_MIN_PCT", "0.10"))                # % (np 0.10 = 0.10%)
EMA_SLOPE_BLOCK = int(os.environ.get("EMA_SLOPE_BLOCK", "1"))             # 1=blokuj BUY gdy EMA spada

# Profit-protect / exit quality
MIN_PROFIT_FOR_SOFT_EXIT_PCT = float(os.environ.get("MIN_PROFIT_FOR_SOFT_EXIT_PCT", "0.12"))  # %
BE_TRIGGER_PCT = float(os.environ.get("BE_TRIGGER_PCT", "0.15"))          # %
BE_OFFSET_PCT  = float(os.environ.get("BE_OFFSET_PCT", "0.03"))           # %

# (opcjonalnie) ogranicz churn: minimalny edge (jeśli chcesz twardo)
MIN_EDGE_PCT = float(os.environ.get("MIN_EDGE_PCT", "0.12"))              # % (np. 0.12)

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

TIME_EXIT_ENABLED = 1

ORDER_NOTIONAL_USDC = float(os.environ.get("ORDER_NOTIONAL_USDC", "6.0"))
MIN_NOTIONAL_BUFFER_PCT = float(os.environ.get("MIN_NOTIONAL_BUFFER_PCT", "0.05"))
LIVE_TARGET_NOTIONAL = float(os.environ.get("LIVE_TARGET_NOTIONAL", "6.0"))

client = Client(api_key=API_KEY, api_secret=API_SECRET) if cfg.trading_mode == "LIVE" else Client()

# ========================
# Regime gating
# ========================

logging.info(
  "CONFIG|SYMBOL=%s|INTERVAL=%s|SPOT_MODE=%s|cfg_trading_mode=%s",
  SYMBOL, INTERVAL, cfg.spot_mode, cfg.trading_mode
)


def get_last_n_closed_candles(n: int = 2):
    """
    Zwraca listę n ostatnich ZAMKNIĘTYCH świec (offset 1), najnowsza pierwsza.
    Każdy wiersz: (open_time, open, high, low, close, ema_21, rsi_14, atr_14)
    """
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT open_time, open, high, low, close, ema_21, rsi_14, atr_14
        FROM candles
        WHERE symbol=%s AND interval=%s
        ORDER BY open_time DESC
        OFFSET 1
        LIMIT %s
        """,
        (SYMBOL, INTERVAL, int(n)),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


def is_live_mode() -> bool:
    return str(cfg.trading_mode).upper() == "LIVE"


def make_client_order_id(symbol: str, strategy: str, interval: str, side: str, candle_open_time, *, pos_id: int, tag: str) -> str:
    return build_live_client_order_id(symbol, pos_id, tag)


def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def get_best_bid_ask(sym: str):
    ob = client.get_order_book(symbol=sym, limit=5)
    best_bid = _safe_float(ob["bids"][0][0]) if ob.get("bids") else None
    best_ask = _safe_float(ob["asks"][0][0]) if ob.get("asks") else None
    return best_bid, best_ask


def _mk_child_client_order_id(base_id: str, suffix: str) -> str:
    # Binance max 36 chars for newClientOrderId
    s = str(suffix).upper()[:3]
    if len(base_id) <= 32:
        return f"{base_id}-{s}"[:36]
    return f"{base_id[:32]}-{s}"[:36]


def place_live_exit_maker_then_market(
    *,
    symbol: str,
    side: str,
    qty_btc: float,
    maker_offset_bps: float,
    timeout_sec: int,
    poll_sec: float,
    base_client_order_id: str,
):
    """
    LIVE EXIT execution:
    1) LIMIT_MAKER (post-only) near top-of-book
    2) poll up to timeout_sec
    3) if not fully filled -> cancel + MARKET for remaining
    Returns dict: {ok, live_ok, status, executed_qty, filled_as, resp, maker_price, best_bid, best_ask}
    """

    side_u = str(side).upper()
    qty_f = float(qty_btc)

    best_bid, best_ask = get_best_bid_ask(symbol)
    if best_bid is None or best_ask is None:
        return {
            "ok": False,
            "live_ok": False,
            "status": "NO_BOOK",
            "executed_qty": 0.0,
            "filled_as": None,
            "resp": {"error": "order_book_empty"},
            "maker_price": None,
            "best_bid": best_bid,
            "best_ask": best_ask,
        }

    # For EXIT:
    # SELL (close LONG): set maker price slightly ABOVE best_bid to avoid taking (remain maker)
    # BUY (close SHORT): set maker price slightly BELOW best_ask to avoid taking
    if side_u == "SELL":
        maker_price = best_bid * (1.0 + maker_offset_bps / 10_000.0)
    else:
        maker_price = best_ask * (1.0 - maker_offset_bps / 10_000.0)

    maker_cid = _mk_child_client_order_id(base_client_order_id, "MKR")

    # Spot post-only: type='LIMIT_MAKER'
    try:
        resp_maker = client.create_order(
            symbol=symbol,
            side=side_u,
            type="LIMIT_MAKER",
            quantity=f"{qty_f:.8f}",
            price=f"{maker_price:.2f}",
            newClientOrderId=maker_cid,
        )
    except Exception as e:
        return {
            "ok": False,
            "live_ok": False,
            "status": "MAKER_CREATE_FAILED",
            "executed_qty": 0.0,
            "filled_as": None,
            "resp": {"error": str(e)},
            "maker_price": float(maker_price),
            "best_bid": best_bid,
            "best_ask": best_ask,
        }

    order_id = resp_maker.get("orderId")
    executed_qty = 0.0
    status = None

    # poll
    deadline = time.time() + max(0, int(timeout_sec))
    while time.time() < deadline:
        time.sleep(max(0.1, float(poll_sec)))
        try:
            o = client.get_order(symbol=symbol, orderId=order_id)
        except Exception:
            continue

        status = str(o.get("status", "")).upper()
        executed_qty = _safe_float(o.get("executedQty"), 0.0)

        # If FILLED -> done
        if status == "FILLED":
            return {
                "ok": True,
                "live_ok": True,
                "status": status,
                "executed_qty": float(executed_qty),
                "filled_as": "MAKER",
                "resp": {"maker_create": resp_maker, "maker_final": o},
                "maker_price": float(maker_price),
                "best_bid": best_bid,
                "best_ask": best_ask,
            }

        # If already fully executed but status lagging
        if executed_qty >= qty_f * 0.999:
            return {
                "ok": True,
                "live_ok": True,
                "status": status or "FILLED_BY_QTY",
                "executed_qty": float(executed_qty),
                "filled_as": "MAKER",
                "resp": {"maker_create": resp_maker, "maker_final": o},
                "maker_price": float(maker_price),
                "best_bid": best_bid,
                "best_ask": best_ask,
            }

    # timeout -> cancel
    try:
        cancel_resp = client.cancel_order(symbol=symbol, orderId=order_id)
    except Exception as e:
        cancel_resp = {"error": str(e)}

    remaining = max(0.0, qty_f - float(executed_qty))

    # If partially filled, close remainder by MARKET
    if remaining > 0.0:
        mkt_cid = _mk_child_client_order_id(base_client_order_id, "MKT")
        try:
            resp_mkt = client.create_order(
                symbol=symbol,
                side=side_u,
                type="MARKET",
                quantity=f"{remaining:.8f}",
                newClientOrderId=mkt_cid,
            )
            mkt_status = str(resp_mkt.get("status", "")).upper()
            mkt_exec = _safe_float(resp_mkt.get("executedQty"), 0.0)
            live_ok = (mkt_status == "FILLED") or (mkt_exec > 0.0)
            return {
                "ok": True,
                "live_ok": bool(live_ok),
                "status": "FALLBACK_MARKET",
                "executed_qty": float(executed_qty) + float(mkt_exec),
                "filled_as": "MARKET_FALLBACK",
                "resp": {"maker_create": resp_maker, "cancel": cancel_resp, "market": resp_mkt},
                "maker_price": float(maker_price),
                "best_bid": best_bid,
                "best_ask": best_ask,
            }
        except Exception as e:
            return {
                "ok": False,
                "live_ok": False,
                "status": "FALLBACK_MARKET_FAILED",
                "executed_qty": float(executed_qty),
                "filled_as": None,
                "resp": {"maker_create": resp_maker, "cancel": cancel_resp, "error": str(e)},
                "maker_price": float(maker_price),
                "best_bid": best_bid,
                "best_ask": best_ask,
            }

    # nothing remaining (rare) but not marked filled
    return {
        "ok": True,
        "live_ok": float(executed_qty) > 0.0,
        "status": "MAKER_TIMEOUT_NO_REMAIN",
        "executed_qty": float(executed_qty),
        "filled_as": "MAKER_TIMEOUT",
        "resp": {"maker_create": resp_maker, "cancel": cancel_resp},
        "maker_price": float(maker_price),
        "best_bid": best_bid,
        "best_ask": best_ask,
    }


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

    WAŻNE (semantyka Model A):
    - Zwracamy True, jeśli ledger został zapisany (inserted=True),
      nawet jeżeli LIVE order jest suppressed/disabled.
    - Dzięki temu strategia aktualizuje positions i może testować EXIT/TP/SL/TIME_EXIT.
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

    # 2) LIVE AFTER ledger reservation
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
                "blocked_reason": "LIVE_ORDER_SUPPRESSED",
            },
        )

        return {
            "ledger_ok": True,
            "live_attempted": False,
            "live_ok": False,
            "blocked_reason": "LIVE_ORDER_SUPPRESSED",
            "client_order_id": None,
            "resp": None,
        }

    # SSOT: create position FIRST (ENTRY) to get pos_id, then build clientOrderId that includes pos_id
    pos_id = None
    client_order_id = None

    if not is_exit:
        # ENTRY: zapisujemy side jako LONG/SHORT w positions (bo reszta kodu tak oczekuje)
        side_u = str(side).upper()
        pos_side = "LONG" if side_u == "BUY" else "SHORT"

        pos_id = open_position(
            side=str(pos_side),
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
            if not is_exit and pos_id:
                attach_entry_order_id_with_conn(cur_exec, int(pos_id), None, client_order_id)
            if is_exit and pos_id:
                attach_exit_order_id_with_conn(cur_exec, int(pos_id), None, client_order_id)
            conn_exec.commit()
        except Exception:
            conn_exec.rollback()
            logging.exception("RSI: pre-attach client_order_id failed pos_id=%s", pos_id)
        finally:
            cur_exec.close()
            conn_exec.close()

    else:
        # EXIT: użyj istniejącej OPEN pozycji
        open_row = get_open_position()
        pos_id = int(open_row[0]) if open_row else None
        if not pos_id:
            logging.error("EXIT requested but no OPEN position found (symbol=%s interval=%s strategy=%s)", cfg_used.symbol, cfg_used.interval, STRATEGY_NAME)
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
            if not is_exit and pos_id:
                attach_entry_order_id_with_conn(cur_exec, int(pos_id), None, client_order_id)
            if is_exit and pos_id:
                attach_exit_order_id_with_conn(cur_exec, int(pos_id), None, client_order_id)
            conn_exec.commit()
        except Exception:
            conn_exec.rollback()
            logging.exception("RSI: pre-attach client_order_id failed pos_id=%s", pos_id)
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
            info={
                "is_exit": bool(is_exit),
                "pos_id": int(pos_id) if pos_id else None,
                "client_order_id": client_order_id,
                "resp": (resp or {}).get("resp"),
            },
        )
        # NIE zamykaj pozycji w DB na ślepo (zostaw OPEN, reconciliation/ingest to ogarnie)
        return {
            "ledger_ok": True,
            "live_attempted": True,
            "live_ok": False,
            "blocked_reason": "LIVE_ORDER_FAILED",
            "client_order_id": client_order_id,
            "resp": (resp or {}).get("resp"),
        }

    raw = (resp or {}).get("resp") or {}
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
            logging.exception("RSI: attach order ids failed pos_id=%s is_exit=%s order_id=%s", pos_id, bool(is_exit), order_id)
        finally:
            cur_exec.close()
            conn_exec.close()
    else:
        logging.error("RSI: LIVE ACK missing orderId pos_id=%s is_exit=%s resp=%s", pos_id, bool(is_exit), raw)

    # resp ok -> ustal live_ok
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
    cquote_raw = raw.get("cummulativeQuoteQty")

    try:
        executed_f = float(executed_raw) if executed_raw is not None else 0.0
    except Exception:
        executed_f = 0.0

    try:
        cquote_f = float(cquote_raw) if cquote_raw is not None else 0.0
    except Exception:
        cquote_f = 0.0

    avg_price = None
    if executed_f > 0 and cquote_f > 0:
        avg_price = cquote_f / executed_f

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
            "cummulative_quote_qty": cquote_f,
            "avg_price": float(avg_price) if avg_price is not None else None,
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


def execute_and_record_soft_exit_maker_then_market(
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
    Ledger-first jak execute_and_record(), ale LIVE wykonanie dla RSI_SOFT_EXIT:
    LIMIT_MAKER -> cancel -> MARKET fallback.
    """

    # 1) DB guard FIRST (idempotencja per candle + is_exit)
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
        is_exit=True,
    )

    if not inserted:
        emit_strategy_event(
            event_type="BLOCKED",
            decision=side,
            reason="DB_GUARD_DUPLICATE",
            price=price,
            candle_open_time=candle_open_time,
            info={"is_exit": True, "qty_btc": float(qty_btc), "reason_text": reason},
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
        info={"is_exit": True, "qty_btc": float(qty_btc), "reason_text": reason},
    )

    # 2) PAPER -> traktujemy jako wykonane
    if cfg_used.trading_mode != "LIVE":
        return {
            "ledger_ok": True,
            "live_attempted": False,
            "live_ok": True,
            "blocked_reason": None,
            "client_order_id": None,
            "resp": None,
        }

    # 3) LIVE permissions gate (EXIT always allowed, but still respects allow_live_orders from can_trade)
    if not allow_live_orders:
        emit_strategy_event(
            event_type="BLOCKED",
            decision=side,
            reason="LIVE_EXIT_NOT_ATTEMPTED",
            price=price,
            candle_open_time=candle_open_time,
            info={"allow_meta": allow_meta, "is_exit": True, "blocked_reason": "LIVE_ORDER_SUPPRESSED"},
        )
        return {
            "ledger_ok": True,
            "live_attempted": False,
            "live_ok": False,
            "blocked_reason": "LIVE_ORDER_SUPPRESSED",
            "client_order_id": None,
            "resp": None,
        }
    
    open_row = get_open_position()
    pos_id = int(open_row[0]) if open_row else None
    if not pos_id:
        emit_strategy_event(
            event_type="BLOCKED",
            decision=side,
            reason="EXIT_NO_OPEN_POSITION",
            price=price,
            candle_open_time=candle_open_time,
            info={"is_exit": True, "exit_kind": "RSI_SOFT_EXIT"},
        )
        return {
            "ledger_ok": True,
            "live_attempted": False,
            "live_ok": False,
            "blocked_reason": "EXIT_NO_OPEN_POSITION",
            "client_order_id": None,
            "resp": None,
        }

    base_client_order_id = make_client_order_id(
        cfg_used.symbol, STRATEGY_NAME, cfg_used.interval, side, candle_open_time, pos_id=pos_id, tag="X"
    )

    # IMPORTANT: persist exit_client_order_id BEFORE sending orders
    # reconcile_positions uses origClientOrderId -> must match a REAL Binance order CID
    maker_cid = _mk_child_client_order_id(base_client_order_id, "MKR")
    try:
        conn2 = get_db_conn()
        cur2 = conn2.cursor()
        cur2.execute(
            "UPDATE positions SET exit_client_order_id=%s WHERE id=%s",
            (maker_cid, int(pos_id)),
        )
        conn2.commit()
        cur2.close()
        conn2.close()
    except Exception:
        logging.exception("RSI: failed to set exit_client_order_id (soft exit) pos_id=%s", pos_id)

    # telemetry: maker attempt
    emit_strategy_event(
        event_type="LIVE_ORDER_SENT",
        decision=side,
        reason="EXIT_MAKER_ATTEMPT",
        price=price,
        candle_open_time=candle_open_time,
        info={
            "is_exit": True,
            "client_order_id": base_client_order_id,
            "mode": "MAKER_THEN_MARKET",
            "maker_offset_bps": float(RSI_SOFT_EXIT_MAKER_OFFSET_BPS),
            "timeout_sec": int(RSI_SOFT_EXIT_MAKER_TIMEOUT_SEC),
            "exit_kind": "RSI_SOFT_EXIT",
        },
    )

    out = place_live_exit_maker_then_market(
        symbol=cfg_used.symbol,
        side=side,
        qty_btc=float(qty_btc),
        maker_offset_bps=float(RSI_SOFT_EXIT_MAKER_OFFSET_BPS),
        timeout_sec=int(RSI_SOFT_EXIT_MAKER_TIMEOUT_SEC),
        poll_sec=float(RSI_SOFT_EXIT_MAKER_POLL_SEC),
        base_client_order_id=base_client_order_id,
    )

    if not out.get("ok") or not out.get("live_ok"):
        emit_strategy_event(
            event_type="BLOCKED",
            decision=side,
            reason="LIVE_ORDER_FAILED",
            price=price,
            candle_open_time=candle_open_time,
            info={"is_exit": True, "client_order_id": base_client_order_id, "resp": out},
        )
        return {
            "ledger_ok": True,
            "live_attempted": True,
            "live_ok": False,
            "blocked_reason": "LIVE_ORDER_FAILED",
            "client_order_id": base_client_order_id,
            "resp": out,
        }

    # classify result
    filled_as = str(out.get("filled_as") or "").upper()
    if filled_as == "MAKER":
        reason_code = f"EXIT_MAKER_FILLED|{out['resp']['maker_create']['orderId']}"
    elif filled_as == "MARKET_FALLBACK":
        reason_code = f"EXIT_MARKET_FALLBACK|{out['resp']['market']['orderId']}"
    else:
        reason_code = "EXIT_MAKER_TIMEOUT"

    emit_strategy_event(
        event_type="LIVE_ORDER_SENT",
        decision=side,
        reason=reason_code,
        price=price,
        candle_open_time=candle_open_time,
        info={
            "is_exit": True,
            "client_order_id": base_client_order_id,
            "status": str(out.get("status")),
            "executed_qty": float(out.get("executed_qty") or 0.0),
            "filled_as": out.get("filled_as"),
            "maker_price": out.get("maker_price"),
            "best_bid": out.get("best_bid"),
            "best_ask": out.get("best_ask"),
            "resp": out.get("resp"),
            "exit_kind": "RSI_SOFT_EXIT",
        },
    )

    if pos_id:
        try:
            filled_as = str(out.get("filled_as") or "").upper()
            resp_blob = out.get("resp") or {}
            exit_order_id = None

            if filled_as == "MAKER":
                exit_order_id = ((resp_blob.get("maker_create") or {}).get("orderId")) or ((resp_blob.get("maker_final") or {}).get("orderId"))
            elif filled_as == "MARKET_FALLBACK":
                exit_order_id = (resp_blob.get("market") or {}).get("orderId")
            else:
                # fallback: try maker_create
                exit_order_id = (resp_blob.get("maker_create") or {}).get("orderId")

            if exit_order_id:
                attach_exit_order_id(pos_id, str(exit_order_id), base_client_order_id)
            else:
                logging.error("RSI: EXIT missing orderId pos_id=%s out=%s", pos_id, out)
        except Exception:
            logging.exception("RSI: failed to attach exit order id pos_id=%s", pos_id)

    return {
        "ledger_ok": True,
        "live_attempted": True,
        "live_ok": True,
        "blocked_reason": None,
        "client_order_id": base_client_order_id,
        "resp": out,
    }


def execute_exit_safe(
    *,
    exit_side: str,
    price: float,
    qty_btc: float,
    reason_text: str,
    candle_open_time,
    cfg_used: RuntimeConfig,
    allow_live_orders: bool,
    allow_meta: dict,
    exit_kind: str,   # <-- NOWE: "RSI_SOFT_EXIT" | "TAKE_PROFIT" | "TIME_EXIT" | "BE_PROTECT" | ...
):
    """
    Exit executor (routing):
    - RSI_SOFT_EXIT: może używać maker->market (jeśli włączone)
    - TAKE_PROFIT / TIME_EXIT / BE_PROTECT: zawsze standard (bez maker wait)
    - SL/PANIC: nie używają tej funkcji (zostają direct execute_and_record)
    """

    ek = str(exit_kind or "").upper()

    # maker->market WYŁĄCZNIE dla RSI_SOFT_EXIT
    if ek == "RSI_SOFT_EXIT" and cfg_used.trading_mode == "LIVE" and RSI_SOFT_EXIT_EXEC_MODE == "MAKER_THEN_MARKET":
        return execute_and_record_soft_exit_maker_then_market(
            side=exit_side,
            price=price,
            qty_btc=float(qty_btc),
            reason=reason_text,
            candle_open_time=candle_open_time,
            cfg_used=cfg_used,
            allow_live_orders=allow_live_orders,
            allow_meta=allow_meta,
        )

    # wszystko inne -> standardowy exit (bez maker wait)
    return execute_and_record(
        side=exit_side,
        price=price,
        qty_btc=float(qty_btc),
        reason=reason_text,
        candle_open_time=candle_open_time,
        cfg_used=cfg_used,
        allow_live_orders=allow_live_orders,
        allow_meta=allow_meta,
        is_exit=True,
    )


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


def emit_blocked(*, reason: str, decision: str | None, price: float, candle_open_time, info: dict | None = None):
    # klasyfikacja: BLOCKED tylko dla twardych gate’ów
    hard_block = reason in {
        "REGIME_BLOCK",
        "BOT_DISABLED",
        "BOT_MODE_HALT",
        "DAILY_MAX_LOSS",
        "DAILY_MAX_LOSS_POSITIONS",
        "DB_GUARD_DUPLICATE",
        "LIVE_ORDER_FAILED",
        "EXIT_BLOCKED",
        "LIVE_ENTRY_NOT_ATTEMPTED",
        "LIVE_EXIT_NOT_ATTEMPTED",
        "LIVE_ENTRY_NOT_FILLED",
        "LIVE_EXIT_NOT_FILLED",
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
# DB HELPERS
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
        (SYMBOL, STRATEGY_NAME, INTERVAL, json.dumps(info)),
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

    # meta dla can_trade/heartbeat — trzymamy w tej samej strukturze co dotychczas,
    # ale już kanonicznej (z helpera).
    rmeta_gate = {
        "enabled": bool(bc.regime_enabled),
        "mode": bc.regime_mode,
        "regime": gate.regime,
        "would_block": bool(gate.would_block) if gate.would_block is not None else None,
        "why": gate.why,          # TERAZ: krótki ENUM, nie długi opis
        "meta": gate.meta,        # tu jest pełna notatka/diagnostyka
    }

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

        "allow_entry_gate": bool(allow_gate_entry),
        "allow_live_orders_entry": bool(allowed_orders_entry),
        "allow_live_orders_exit": bool(allowed_orders_exit),
        "allow_meta_entry": allow_meta_entry,
        "allow_meta_exit": allow_meta_exit,
        "rsi_soft_exit_exec_mode": str(RSI_SOFT_EXIT_EXEC_MODE),
        "rsi_soft_exit_maker_enabled": bool(str(RSI_SOFT_EXIT_EXEC_MODE).upper() == "MAKER_THEN_MARKET"),

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
        logging.info("RSI: open_position skipped – already OPEN pos_id=%s.", pos_id)
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

    logging.info("RSI: position OPENED pos_id=%s %s qty=%.8f entry=%.2f", pos_id, side, float(qty), float(entry_price))
    return pos_id


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
        "TIME_EXIT_ENABLED": 1.0,
        "ORDER_NOTIONAL_USDC": float(ORDER_NOTIONAL_USDC),
        "MIN_NOTIONAL_BUFFER_PCT": float(MIN_NOTIONAL_BUFFER_PCT),
        "RSI_SOFT_EXIT_ENABLED": float(RSI_SOFT_EXIT_ENABLED),
        "RSI_EXIT_OVERBOUGHT": float(RSI_EXIT_OVERBOUGHT),
        "RSI_EXIT_OVERSOLD": float(RSI_EXIT_OVERSOLD),
        "RSI_REBOUND_DELTA": float(RSI_REBOUND_DELTA),
        "ATR_MIN_PCT": float(ATR_MIN_PCT),
        "EMA_SLOPE_BLOCK": float(EMA_SLOPE_BLOCK),
        "MIN_PROFIT_FOR_SOFT_EXIT_PCT": float(MIN_PROFIT_FOR_SOFT_EXIT_PCT),
        "BE_TRIGGER_PCT": float(BE_TRIGGER_PCT),
        "BE_OFFSET_PCT": float(BE_OFFSET_PCT),
        "MIN_EDGE_PCT": float(MIN_EDGE_PCT),
        "EMA_SLOPE_FILTER": float(EMA_SLOPE_FILTER),
        "RSI_SOFT_EXIT_MAKER_OFFSET_BPS": float(RSI_SOFT_EXIT_MAKER_OFFSET_BPS),
        "RSI_SOFT_EXIT_MAKER_TIMEOUT_SEC": float(RSI_SOFT_EXIT_MAKER_TIMEOUT_SEC),
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
    global TIME_EXIT_ENABLED
    global ORDER_NOTIONAL_USDC, MIN_NOTIONAL_BUFFER_PCT
    global RSI_SOFT_EXIT_ENABLED, RSI_EXIT_OVERBOUGHT, RSI_EXIT_OVERSOLD
    global RSI_REBOUND_DELTA, ATR_MIN_PCT, EMA_SLOPE_BLOCK
    global MIN_PROFIT_FOR_SOFT_EXIT_PCT, BE_TRIGGER_PCT, BE_OFFSET_PCT
    global MIN_EDGE_PCT
    global EMA_SLOPE_FILTER
    global RSI_SOFT_EXIT_MAKER_OFFSET_BPS, RSI_SOFT_EXIT_MAKER_TIMEOUT_SEC
    
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

    if "TIME_EXIT_ENABLED" in params:
        TIME_EXIT_ENABLED = bool(int(clamp(params["TIME_EXIT_ENABLED"], 0, 1)))

    if "ORDER_NOTIONAL_USDC" in params:
        ORDER_NOTIONAL_USDC = clamp(params["ORDER_NOTIONAL_USDC"], 1.0, 1000.0)

    if "MIN_NOTIONAL_BUFFER_PCT" in params:
        MIN_NOTIONAL_BUFFER_PCT = clamp(params["MIN_NOTIONAL_BUFFER_PCT"], 0.0, 0.50)

    if "RSI_SOFT_EXIT_ENABLED" in params:
        RSI_SOFT_EXIT_ENABLED = int(clamp(params["RSI_SOFT_EXIT_ENABLED"], 0, 1))

    # domyślnie = progi wejścia, ale pozwalamy rozdzielić exit
    if "RSI_EXIT_OVERBOUGHT" in params:
        RSI_EXIT_OVERBOUGHT = clamp(params["RSI_EXIT_OVERBOUGHT"], 5.0, 95.0)
    else:
        RSI_EXIT_OVERBOUGHT = float(RSI_OVERBOUGHT)

    if "RSI_EXIT_OVERSOLD" in params:
        RSI_EXIT_OVERSOLD = clamp(params["RSI_EXIT_OVERSOLD"], 5.0, 95.0)
    else:
        RSI_EXIT_OVERSOLD = float(RSI_OVERSOLD)

    if "RSI_REBOUND_DELTA" in params:
        RSI_REBOUND_DELTA = clamp(params["RSI_REBOUND_DELTA"], 0.0, 20.0)

    if "ATR_MIN_PCT" in params:
        ATR_MIN_PCT = clamp(params["ATR_MIN_PCT"], 0.0, 5.0)

    if "EMA_SLOPE_BLOCK" in params:
        EMA_SLOPE_BLOCK = int(clamp(params["EMA_SLOPE_BLOCK"], 0, 1))

    if "MIN_PROFIT_FOR_SOFT_EXIT_PCT" in params:
        MIN_PROFIT_FOR_SOFT_EXIT_PCT = clamp(params["MIN_PROFIT_FOR_SOFT_EXIT_PCT"], 0.0, 5.0)

    if "BE_TRIGGER_PCT" in params:
        BE_TRIGGER_PCT = clamp(params["BE_TRIGGER_PCT"], 0.0, 5.0)

    if "BE_OFFSET_PCT" in params:
        BE_OFFSET_PCT = clamp(params["BE_OFFSET_PCT"], 0.0, 2.0)

    if "MIN_EDGE_PCT" in params:
        MIN_EDGE_PCT = clamp(params["MIN_EDGE_PCT"], 0.0, 5.0)

    if "EMA_SLOPE_FILTER" in params:
        EMA_SLOPE_FILTER = bool(int(clamp(params["EMA_SLOPE_FILTER"], 0, 1)))

    if "RSI_SOFT_EXIT_MAKER_OFFSET_BPS" in params:
        RSI_SOFT_EXIT_MAKER_OFFSET_BPS = clamp(params["RSI_SOFT_EXIT_MAKER_OFFSET_BPS"], 0.0, 50.0)

    if "RSI_SOFT_EXIT_MAKER_TIMEOUT_SEC" in params:
        RSI_SOFT_EXIT_MAKER_TIMEOUT_SEC = int(clamp(params["RSI_SOFT_EXIT_MAKER_TIMEOUT_SEC"], 1, 60))


    logging.info(
    "RUNTIME_PARAMS|symbol=%s|strategy=%s|RSI_OVERSOLD=%.2f|RSI_OVERBOUGHT=%.2f|"
    "STOP_LOSS_PCT=%.2f|TAKE_PROFIT_PCT=%.2f|MAX_POSITION_MINUTES=%d|DAILY_MAX_LOSS_PCT=%.2f|"
    "ORDER_QTY_BTC=%.8f|ORDER_NOTIONAL_USDC=%.2f|MIN_NOTIONAL_BUFFER_PCT=%.3f|"
    "MAX_DIST_FROM_EMA_PCT=%.2f|TREND_BUFFER=%.4f|ENTRY_BUFFER_PCT=%.4f|TIME_EXIT_ENABLED=%s|"
    "RSI_SOFT_EXIT_ENABLED=%s|RSI_EXIT_OVERBOUGHT=%.2f|RSI_EXIT_OVERSOLD=%.2f|RSI_REBOUND_DELTA=%.2f|ATR_MIN_PCT=%.3f|EMA_SLOPE_BLOCK=%s|"
    "MIN_PROFIT_FOR_SOFT_EXIT_PCT=%.3f|BE_TRIGGER_PCT=%.3f|BE_OFFSET_PCT=%.3f|MIN_EDGE_PCT=%.3f|EMA_SLOPE_FILTER=%s",
    SYMBOL, STRATEGY_NAME, RSI_OVERSOLD, RSI_OVERBOUGHT,
    STOP_LOSS_PCT, TAKE_PROFIT_PCT, MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT,
    ORDER_QTY_BTC, float(ORDER_NOTIONAL_USDC), float(MIN_NOTIONAL_BUFFER_PCT),
    MAX_DIST_FROM_EMA_PCT, TREND_BUFFER, ENTRY_BUFFER_PCT, bool(TIME_EXIT_ENABLED),
    bool(RSI_SOFT_EXIT_ENABLED), float(RSI_EXIT_OVERBOUGHT), float(RSI_EXIT_OVERSOLD),
    float(RSI_REBOUND_DELTA), float(ATR_MIN_PCT), bool(EMA_SLOPE_BLOCK),
    float(MIN_PROFIT_FOR_SOFT_EXIT_PCT), float(BE_TRIGGER_PCT), float(BE_OFFSET_PCT), float(MIN_EDGE_PCT), bool(EMA_SLOPE_FILTER),
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
        SELECT id, open_time, high, low, close
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

    # ATR(14) w % i absolutnie (atr_14)
    high_f = df["high"].astype(float)
    low_f = df["low"].astype(float)
    prev_close = close_f.shift(1)

    tr = pd.concat(
        [
            (high_f - low_f),
            (high_f - prev_close).abs(),
            (low_f - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    df["atr_14"] = tr.rolling(window=14).mean()
    df["rsi_14"] = 100.0 - (100.0 / (1.0 + rs))

    last = df.tail(60)  # update tylko końcówkę

    cur = conn.cursor()
    data = [
        (row["ema_21"], row["rsi_14"], row["atr_14"], int(row["id"]))
        for _, row in last.iterrows()
    ]
    cur.executemany(
        """
        UPDATE candles
        SET ema_21=%s, rsi_14=%s, atr_14=%s
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


def run_strategy(row, prev_row=None):
    open_time = (row[0] if row else None)
    price_for_events = float(row[4]) if row and row[4] is not None else None
    emit_strategy_event(
        event_type="RUN_START",
        decision=None,
        reason="ENTER",
        price=price_for_events,
        candle_open_time=(row[0] if row else None),
        info={"has_row": bool(row), "bot_version": os.environ.get("BOT_VERSION")},
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

        open_time, open_px, high_px, low_px, close_px, ema_21, rsi_14, atr_14 = row

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

        if ema_21 is None or rsi_14 is None or atr_14 is None:            
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

        atr_val = float(atr_14) if atr_14 is not None else None

        prev_ema_val = None
        prev_rsi_val = None
        if prev_row:
            _ot2, _o2, _h2, _l2, _c2, _ema2, _rsi2, _atr2 = prev_row
            prev_ema_val = float(_ema2) if _ema2 is not None else None
            prev_rsi_val = float(_rsi2) if _rsi2 is not None else None

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
        time_exit_enabled = TIME_EXIT_ENABLED
        max_pos_minutes = int(MAX_POSITION_MINUTES)

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

                res = execute_and_record(
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
                if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                    close_position(exit_price=price, reason="PANIC")
                    pos = None
                else:
                    emit_blocked(
                        reason="EXIT_BLOCKED",
                        decision=exit_side,
                        price=price,
                        candle_open_time=open_time,
                        info={"res": res},
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

            # PnL% pozycji (dla long/short)
            if pos_side_u == "LONG":
                move_pct = (price - entry_f) / entry_f * 100.0
            else:
                move_pct = (entry_f - price) / entry_f * 100.0

            # --- LONG ---
            if pos_side_u == "LONG":
                tp_level = entry_f * (1.0 + TAKE_PROFIT_PCT / 100.0)
                sl_level = entry_f * (1.0 - STOP_LOSS_PCT / 100.0)

                # TP intrabar
                if TAKE_PROFIT_PCT > 0 and high_price >= tp_level:
                    exec_px = price
                    reason = f"RSI TAKE PROFIT LONG intrabar high={high_price:.2f} >= tp={tp_level:.2f}"
                    res = execute_exit_safe(
                        exit_side="SELL",
                        price=exec_px,
                        qty_btc=qty_f,
                        reason_text=reason,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        exit_kind="TAKE_PROFIT",
                    )
                    if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                        close_position(exit_price=exec_px, reason="TAKE_PROFIT")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision="SELL",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                        return
                    return

                # SL intrabar
                if STOP_LOSS_PCT > 0 and low_price <= sl_level:
                    exec_px = price
                    reason = f"RSI STOP LOSS LONG intrabar low={low_price:.2f} <= sl={sl_level:.2f}"
                    res = execute_and_record("SELL", exec_px, qty_f, reason, open_time, cfg_used=cfg_effective, allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"], is_exit=True,)
                    if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                        close_position(exit_price=exec_px, reason="STOP_LOSS")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision="SELL",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                        return
                    return

            # --- SHORT ---
            else:
                tp_level = entry_f * (1.0 - TAKE_PROFIT_PCT / 100.0)
                sl_level = entry_f * (1.0 + STOP_LOSS_PCT / 100.0)

                # TP intrabar
                if TAKE_PROFIT_PCT > 0 and low_price <= tp_level:
                    exec_px = price
                    reason = f"RSI TAKE PROFIT SHORT intrabar low={low_price:.2f} <= tp={tp_level:.2f}"
                    res = execute_exit_safe(
                        exit_side="BUY",
                        price=exec_px,
                        qty_btc=qty_f,
                        reason_text=reason,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        exit_kind="TAKE_PROFIT",
                    )
                    if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                        close_position(exit_price=exec_px, reason="TAKE_PROFIT_SHORT")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision="BUY",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                        return
                    return

                # SL intrabar
                if STOP_LOSS_PCT > 0 and high_price >= sl_level:
                    exec_px = price
                    reason = f"RSI STOP LOSS SHORT intrabar high={high_price:.2f} >= sl={sl_level:.2f}"
                    res = execute_and_record("BUY", exec_px, qty_f, reason, open_time, cfg_used=cfg_effective, allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"], is_exit=True,)
                    if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                        close_position(exit_price=exec_px, reason="STOP_LOSS_SHORT")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision="BUY",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                        return
                    return
            
            # Profit-protect (stateless): jeśli był zysk >= BE_TRIGGER,
            # a teraz wróciliśmy blisko entry (BE_OFFSET) -> zamknij, żeby nie oddawać.
            if BE_TRIGGER_PCT > 0 and move_pct >= float(BE_TRIGGER_PCT):
                if pos_side_u == "LONG":
                    be_level = entry_f * (1.0 + float(BE_OFFSET_PCT) / 100.0)
                    if price <= be_level:
                        exit_side = "SELL"
                        reason_exit = f"BE_PROTECT LONG move_pct={move_pct:.3f} price={price:.2f} <= be={be_level:.2f}"
                        res = execute_exit_safe(
                            exit_side=exit_side,
                            price=price,
                            qty_btc=qty_f,
                            reason_text=reason_exit,
                            candle_open_time=open_time,
                            cfg_used=cfg_effective,
                            allow_live_orders=snap["allowed_orders_exit"],
                            allow_meta=snap["allow_meta_exit"],
                            exit_kind="BE_PROTECT",
                        )
                        if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                            close_position(exit_price=price, reason="BE_PROTECT")
                        else:
                            emit_blocked(reason="EXIT_BLOCKED", decision=exit_side, price=price, candle_open_time=open_time, info={"res": res})
                        return
                else:
                    be_level = entry_f * (1.0 - float(BE_OFFSET_PCT) / 100.0)
                    if price >= be_level:
                        exit_side = "BUY"
                        reason_exit = f"BE_PROTECT SHORT move_pct={move_pct:.3f} price={price:.2f} >= be={be_level:.2f}"
                        res = execute_exit_safe(
                            exit_side=exit_side,
                            price=price,
                            qty_btc=qty_f,
                            reason_text=reason_exit,
                            candle_open_time=open_time,
                            cfg_used=cfg_effective,
                            allow_live_orders=snap["allowed_orders_exit"],
                            allow_meta=snap["allow_meta_exit"],
                            exit_kind="BE_PROTECT",
                        )
                        if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                            close_position(exit_price=price, reason="BE_PROTECT")
                        else:
                            emit_blocked(reason="EXIT_BLOCKED", decision=exit_side, price=price, candle_open_time=open_time, info={"res": res})
                        return
                
            # SOFT EXIT (mean reversion) — jeśli włączone
            if int(RSI_SOFT_EXIT_ENABLED) == 1:
                # LONG: zamykamy gdy RSI wraca wysoko (overbought/exit threshold)
                if pos_side_u == "LONG" and rsi_val >= float(RSI_EXIT_OVERBOUGHT) and move_pct >= float(MIN_PROFIT_FOR_SOFT_EXIT_PCT):
                    exit_side = "SELL"
                    reason_exit = f"RSI SOFT_EXIT LONG rsi={rsi_val:.2f} >= exit_overbought={float(RSI_EXIT_OVERBOUGHT):.2f}"
                    emit_strategy_event(
                        event_type="EXIT_SIGNAL",
                        decision=exit_side,
                        reason="RSI_SOFT_EXIT",
                        price=price,
                        candle_open_time=open_time,
                        info={
                            "pos_side": pos_side_u,
                            "rsi_14": float(rsi_val),
                            "exit_overbought": float(RSI_EXIT_OVERBOUGHT),
                        },
                    )
                    res = execute_exit_safe(
                        exit_side=exit_side,
                        price=price,
                        qty_btc=qty_f,
                        reason_text=reason_exit,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        exit_kind="RSI_SOFT_EXIT",
                    )
                    if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                        close_position(exit_price=price, reason="RSI_SOFT_EXIT")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision=exit_side,
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return

                # SHORT: zamykamy gdy RSI wraca nisko (oversold/exit threshold)
                if pos_side_u != "LONG" and rsi_val <= float(RSI_EXIT_OVERSOLD) and move_pct >= float(MIN_PROFIT_FOR_SOFT_EXIT_PCT):
                    exit_side = "BUY"
                    reason_exit = f"RSI SOFT_EXIT SHORT rsi={rsi_val:.2f} <= exit_oversold={float(RSI_EXIT_OVERSOLD):.2f}"
                    emit_strategy_event(
                        event_type="EXIT_SIGNAL",
                        decision=exit_side,
                        reason="RSI_SOFT_EXIT",
                        price=price,
                        candle_open_time=open_time,
                        info={
                            "pos_side": pos_side_u,
                            "rsi_14": float(rsi_val),
                            "exit_oversold": float(RSI_EXIT_OVERSOLD),
                        },
                    )
                    res = execute_exit_safe(
                        exit_side=exit_side,
                        price=price,
                        qty_btc=qty_f,
                        reason_text=reason_exit,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        exit_kind="RSI_SOFT_EXIT",
                    )
                    if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                        close_position(exit_price=price, reason="RSI_SOFT_EXIT")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision=exit_side,
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
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
                    res = execute_exit_safe(
                        exit_side=side_timeout,
                        price=price,
                        qty_btc=qty_f,
                        reason_text=reason_timeout,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        exit_kind="TIME_EXIT",
                    )
                    if res["ledger_ok"] and (cfg_effective.trading_mode != "LIVE" or res["live_ok"]):
                        close_position(exit_price=price, reason="TIME_EXIT")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision=side_timeout,
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return
                        # OPEN: brak sygnału exit w tym ticku (TP/SL/TIME/soft-exit nie zaszły)
            age_minutes = None
            if pos_entry_time is not None:
                if pos_entry_time.tzinfo is None:
                    pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)
                age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0

            emit_strategy_event(
                event_type="POSITION_HOLD",
                decision=None,
                reason="NO_EXIT_SIGNAL",
                price=price,
                candle_open_time=open_time,
                info={
                    "pos_side": pos_side_u,
                    "pos_qty": float(qty_f),
                    "pos_entry_price": float(entry_f),
                    "age_minutes": float(age_minutes) if age_minutes is not None else None,
                    "tp_pct": float(TAKE_PROFIT_PCT),
                    "sl_pct": float(STOP_LOSS_PCT),
                    "time_exit_enabled": bool(time_exit_enabled),
                    "max_position_minutes": int(max_pos_minutes),
                    "rsi_14": float(rsi_val),
                    "ema_21": float(ema_val),
                    "soft_exit_enabled": bool(int(RSI_SOFT_EXIT_ENABLED) == 1),
                    "exit_overbought": float(RSI_EXIT_OVERBOUGHT),
                    "exit_oversold": float(RSI_EXIT_OVERSOLD),
                },
            )
            return        

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
                # if cfg_effective.trading_mode == "LIVE":
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
        # ===== ENTRY SIGNAL: REBOUND (z poprzedniej świecy) =====
        if prev_rsi_val is None or prev_ema_val is None:
            emit_blocked(
                reason="PREV_INDICATORS_NOT_READY",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"prev_rsi": prev_rsi_val, "prev_ema": prev_ema_val},
            )
            return

        # ATR gate (fee-aware): nie handluj, gdy zmienność zbyt mała
        atr_pct = (float(atr_val) / float(price)) * 100.0 if atr_val and price else 0.0
        if float(ATR_MIN_PCT) > 0 and atr_pct < float(ATR_MIN_PCT):
            emit_blocked(
                reason="ATR_TOO_LOW",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"atr_14": float(atr_val), "atr_pct": float(atr_pct), "min_atr_pct": float(ATR_MIN_PCT)},
            )
            return

        # Trend filter: nie kupuj, gdy EMA spada (opcjonalnie)
        if int(EMA_SLOPE_BLOCK) == 1 and ema_val < prev_ema_val:
            emit_blocked(
                reason="EMA_SLOPE_DOWN",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"ema_now": float(ema_val), "ema_prev": float(prev_ema_val)},
            )
            return

        # Minimalny edge gate (opcjonalnie)
        if float(MIN_EDGE_PCT) > 0 and float(TAKE_PROFIT_PCT) < float(MIN_EDGE_PCT):
            emit_blocked(
                reason="EDGE_TOO_LOW",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"tp_pct": float(TAKE_PROFIT_PCT), "min_edge_pct": float(MIN_EDGE_PCT)},
            )
            return

        # Rebound: poprzednio RSI poniżej oversold, teraz powrót powyżej oversold + delta
        if not (prev_rsi_val < float(RSI_OVERSOLD) and rsi_val > (float(RSI_OVERSOLD) + float(RSI_REBOUND_DELTA))):
            emit_blocked(
                reason="NO_SIGNAL_REBOUND",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={
                    "rsi_prev": float(prev_rsi_val),
                    "rsi_now": float(rsi_val),
                    "oversold": float(RSI_OVERSOLD),
                    "rebound_delta": float(RSI_REBOUND_DELTA),
                },
            )
            return

        decision = "BUY"
        reason = f"RSI_REBOUND prev={prev_rsi_val:.2f} now={rsi_val:.2f} > {RSI_OVERSOLD + RSI_REBOUND_DELTA:.2f}"
        
        # === ENTRY_CHECK telemetry (MUST) ===
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

        # ENFORCE: gate_entry.allow może być False
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
            info={"rsi_14": float(rsi_val), "ema_21": float(ema_val), "reason_text": reason},
        )

        # === SIZING (Model A) ===
        if cfg_effective.trading_mode == "LIVE":
            qty_btc, px_live, notional_live, step, min_qty, min_notional = compute_live_qty_from_notional(
                client,
                SYMBOL,
                target_notional=float(ORDER_NOTIONAL_USDC),
                quote_asset=QUOTE_ASSET,
                min_notional_buffer_pct=float(MIN_NOTIONAL_BUFFER_PCT),
            )

            emit_strategy_event(
                event_type="SIZING",
                decision=decision,
                reason="NOTIONAL",
                price=price,
                candle_open_time=open_time,
                info={
                    "target_notional": float(ORDER_NOTIONAL_USDC),
                    "min_notional_buffer_pct": float(MIN_NOTIONAL_BUFFER_PCT),
                    "px": float(px_live),
                    "qty": float(qty_btc),
                    "notional": float(notional_live),
                    "step": str(step),
                    "min_qty": float(min_qty),
                    "min_notional": float(min_notional),
                },
            )
        else:
            qty_btc = float(ORDER_QTY_BTC)

        qty_btc = float(qty_btc)
        if qty_btc <= 0:
            emit_strategy_event(
                event_type="BLOCKED",
                decision=decision,
                reason="SIZING_QTY_ZERO",
                price=float(price),
                candle_open_time=open_time,
                info={"qty": float(qty_btc)},
            )
            return

        # 1) giełda (LIVE) + ledger
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

            # attempted, ale brak fill -> to logujemy tutaj
            emit_strategy_event(
                event_type="BLOCKED",
                decision=decision,
                reason="LIVE_ENTRY_NOT_FILLED",
                price=price,
                candle_open_time=open_time,
                info={"res": res},
            )
            return

        # 2) Position OPEN is created inside execute_and_record() (SSOT).
        # Do not open it again here.
        emit_strategy_event(
            event_type="POSITION_OPENED",
            decision=decision,
            reason="SSOT_EXECUTE_AND_RECORD",
            price=price,
            candle_open_time=open_time,
            info={"qty_btc": float(qty_btc)},
        )
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
            rows = fetch_klines(limit=200)   # patrz Zmiana 3
            save_klines(rows)
            update_indicators()

            closed = get_last_n_closed_candles(2)
            if closed:
                latest = closed[0]
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
                    run_strategy(latest, prev_row=(closed[1] if len(closed) > 1 else None))
                else:
                    emit_strategy_event(
                        event_type="IDLE",
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
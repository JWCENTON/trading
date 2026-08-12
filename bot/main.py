import os
import time
import json
import logging
import psycopg2
import pandas as pd
from common.adaptive_time_exit import hard_time_exit_enabled, time_exit_policy_name
from common.safe_json import sanitize_json
from common.entry_trace import record_entry_trace_shadow
from common.flags import exchange_mytrades_enabled
from common.exchange_ingest_trades import ingest_my_trades
from common.exchange_identity import normalize_exchange_source
from dataclasses import replace
from datetime import datetime, timezone, date
from decimal import Decimal
from common.daily_loss import should_emit_daily_loss_shadow
from common.alerts import emit_alert_throttled
from psycopg2.extras import execute_batch
from common.runtime import RuntimeConfig
from common.exchange_client import get_market_data_client
from common.simulated_execution_evidence import (
    create_simulated_order_cursor,
    execute_paper_exit_after_preflight,
    paper_position_mutation_allowed_cursor,
    record_simulated_fill_evidence,
    simulated_order_write_status,
)
from common.permissions import can_trade
from common.regime_gate import attach_regime_gate_event, decide_regime_gate, emit_regime_gate_event
from common.bot_control import upsert_defaults, read as read_bot_control
from common.daily_loss import compute_daily_loss_pct_positions, should_block_daily_loss_positions
from common.db import db_write_conn, get_db_conn, read_only_db_conn
from common.user_settings import SYSTEM_MIN_ENTRY_USDC, get_user_settings_snapshot
from common.win_streak import get_recent_win_streak
from common.exit_guards.profit_lock import ProfitLockConfig, evaluate_profit_lock
from common.exit_guards.profit_lock_events import emit_profit_lock_event_once
from common.position_path import load_position_path_snapshot
from common.exit_reason_context import build_exit_reason_context
from common.decision_contract import (
    DecisionReason,
    DecisionSubtype,
    ExecutionOutcome,
    ExecutionStage,
    EvaluationContext,
    FinalDecision,
    normalize_entry_execution_outcome,
)
from common.canonical_regime import evaluation_regime_fields, frozen_regime_provenance
from common.partial_exit import apply_partial_exit_result
from common.final_decision_observation_sink import finalize_decision_observation
from common.execution import (
    place_live_order,
    place_live_exit_maker_then_market as exchange_place_live_exit_maker_then_market,
    compute_live_qty_from_notional,
    build_live_client_order_id,
    build_live_entry_intent_client_order_id,
    preflight_live_order,
)

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

PROFIT_LOCK_CONFIG = ProfitLockConfig.from_env()

ORDER_NOTIONAL_USDC = float(os.environ.get("ORDER_NOTIONAL_USDC", "6.0"))
MIN_NOTIONAL_BUFFER_PCT = float(os.environ.get("MIN_NOTIONAL_BUFFER_PCT", "0.05"))
LIVE_TARGET_NOTIONAL = float(os.environ.get("LIVE_TARGET_NOTIONAL", "6.0"))

_exchange_client = None


def get_exchange_client():
    """Return the process-wide exchange client, creating it on first runtime use."""
    global _exchange_client
    if _exchange_client is None:
        try:
            _exchange_client = get_market_data_client()
        except Exception:
            logging.exception("RSI exchange client initialization failed")
            raise
    return _exchange_client

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
    return get_exchange_client().get_best_bid_ask(symbol=sym)



def _mk_child_client_order_id(base_id: str, suffix: str) -> str:
    # Exchange client order id limit
    s = str(suffix).upper()[:3]
    if len(base_id) <= 32:
        return f"{base_id}-{s}"[:36]
    return f"{base_id[:32]}-{s}"[:36]


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
    return exchange_place_live_exit_maker_then_market(
        get_exchange_client(),
        symbol=symbol,
        side=side,
        qty=float(qty_btc),
        maker_offset_bps=float(maker_offset_bps),
        timeout_sec=int(timeout_sec),
        poll_sec=float(poll_sec),
        base_client_order_id=base_client_order_id,
    )


def _apply_rsi_partial_exit(result, *, pos_id, cfg_used, side, price, reason,
                            candle_open_time):
    if pos_id is None:
        return None
    mutation = apply_partial_exit_result(
        get_db_conn, result=result, position_id=int(pos_id),
        exchange_source=normalize_exchange_source(
            os.environ.get("EXCHANGE") or os.environ.get("EXCHANGE_PROVIDER") or "BINANCE"
        ),
        symbol=cfg_used.symbol, strategy=STRATEGY_NAME,
        interval=cfg_used.interval, side=side, exit_price=price,
        exit_reason=reason,
    )
    if mutation is not None:
        emit_strategy_event(
            event_type="POSITION_REDUCED", decision=side,
            reason="PARTIAL_EXECUTION", price=price,
            candle_open_time=candle_open_time,
            info={"execution_status": "PARTIAL",
                  "executed_qty": result["executed_qty"],
                  "applied_qty": result["position_qty_applied"],
                  "remaining_qty": result["position_remaining_qty"],
                  "fully_executed": False, "exit_reason": reason},
        )
    return mutation

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
    evaluation: EvaluationContext | None = None,
):
    def action(preflight):
        return _execute_and_record_after_paper_exit_preflight(
            side, price, qty_btc, reason, candle_open_time,
            is_exit=is_exit, cfg_used=cfg_used,
            allow_live_orders=allow_live_orders, allow_meta=allow_meta,
            evaluation=evaluation,
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
    evaluation: EvaluationContext | None = None,
    paper_position_id: int | None = None,
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
    trading_mode = str(cfg_used.trading_mode).upper()
    if trading_mode not in {"PAPER", "LIVE"}:
        logging.error(
            "RSI: invalid trading mode; execution fail-closed mode=%r",
            cfg_used.trading_mode,
        )
        return {
            "ledger_ok": False, "live_attempted": False,
            "order_accepted": False, "live_ok": False,
            "blocked_reason": "INVALID_TRADING_MODE",
            "client_order_id": None, "resp": None,
        }

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
        market_regime=(evaluation.market_regime if evaluation is not None else None),
        regime_source_provenance=(
            frozen_regime_provenance(evaluation)
            if evaluation is not None and trading_mode == "PAPER" and not is_exit
            else None
        ),
    )

    if not inserted:
        slot_result = simulated_order_write_status(inserted)
        emit_strategy_event(
            event_type="BLOCKED",
            decision=side,
            reason=slot_result,
            price=price,
            candle_open_time=candle_open_time,
            info={"is_exit": bool(is_exit), "qty_btc": float(qty_btc), "reason_text": reason},
        )
        return {
            "ledger_ok": False,
            "live_attempted": False,
            "order_accepted": False,
            "live_ok": False,
            "blocked_reason": slot_result,
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

    # 2) PAPER: also write SSOT positions (hard truth), but do not send exchange orders
    if trading_mode == "PAPER":
        paper_res = ssot_apply_positions_paper(
            side=side,
            price=float(price),
            qty_btc=float(qty_btc),
            candle_open_time=candle_open_time,
            is_exit=bool(is_exit),
            cfg_used=cfg_used,
            reason_text=str(reason or ""),
            evaluation=evaluation,
            expected_position_id=paper_position_id,
        )

        evidence_persisted = None
        if (
            paper_res.get("ok")
            and paper_res.get("pos_id")
            and isinstance(inserted, int)
            and not isinstance(inserted, bool)
        ):
            try:
                evidence_persisted = record_simulated_fill_evidence(
                    get_db_conn,
                    client=get_exchange_client(),
                    simulated_order_id=int(inserted),
                    position_id=int(paper_res["pos_id"]),
                    environment="paper",
                    deployment_id=os.environ.get(
                        "DEPLOYMENT_ID",
                        os.environ.get("WALTRADE_DEPLOYMENT_ID", "local-paper"),
                    ),
                    exit_reason=str(reason or "PAPER_EXIT") if is_exit else None,
                    require_terminal_close=bool(is_exit),
                )
            except Exception:
                logging.exception(
                    "FINANCIAL_TRUTH_EVIDENCE|paper persistence unavailable"
                )
                if is_exit:
                    paper_res = {
                        **paper_res,
                        "ok": False,
                        "blocked_reason": "POSITION_CLOSE_FAILED",
                        "position_close_succeeded": False,
                    }

        if is_exit and paper_res.get("ok"):
            if not evidence_persisted:
                paper_res = {
                    **paper_res,
                    "ok": False,
                    "blocked_reason": "POSITION_CLOSE_FAILED",
                    "position_close_succeeded": False,
                }
            else:
                paper_res = {
                    **paper_res,
                    "paper_pos_action": "EXIT_CLOSED",
                    "position_close_succeeded": True,
                }
                emit_strategy_event(
                    event_type="POSITION_CLOSED",
                    decision=side,
                    reason=str(reason or "PAPER_EXIT"),
                    price=float(price),
                    candle_open_time=candle_open_time,
                    info={
                        "position_id": int(paper_res["pos_id"]),
                        "simulated_order_id": int(inserted),
                        "symbol": cfg_used.symbol,
                        "interval": cfg_used.interval,
                        "strategy": STRATEGY_NAME,
                        "exit_reason": str(reason or "PAPER_EXIT"),
                        "exit_price": float(price),
                        "quantity": float(qty_btc),
                        "timestamp": candle_open_time,
                    },
                )

        if not paper_res.get("ok", False):
            emit_strategy_event(
                event_type="BLOCKED",
                decision=side,
                reason=paper_res.get("blocked_reason") or "PAPER_POSITIONS_FAILED",
                price=price,
                candle_open_time=candle_open_time,
                info={
                    "is_exit": bool(is_exit),
                    "qty_btc": float(qty_btc),
                    "reason_text": reason,
                    "paper_res": paper_res,
                },
            )
            return {
                "ledger_ok": False,
                "live_attempted": False,
                "order_accepted": False,
                "live_ok": False,
                "paper_executed": False,
                "blocked_reason": paper_res.get("blocked_reason") or "PAPER_POSITIONS_FAILED",
                "client_order_id": paper_res.get("client_order_id"),
                "resp": paper_res,
                "position_close_succeeded": (
                    False if is_exit else None
                ),
            }

        return {
            "ledger_ok": True,
            "live_attempted": False,
            "order_accepted": False,
            "live_ok": False,
            "paper_executed": True,
            "blocked_reason": None,
            "client_order_id": paper_res.get("client_order_id"),
            "resp": paper_res,
            "position_close_succeeded": True if is_exit else None,
        }

    # 3) LIVE AFTER ledger reservation

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
            "order_accepted": False,
            "live_ok": False,
            "blocked_reason": "LIVE_ORDER_SUPPRESSED",
            "client_order_id": None,
            "resp": None,
        }

    # SSOT: create position FIRST (ENTRY) to get pos_id, then build clientOrderId that includes pos_id
    pos_id = None
    client_order_id = None

    if not is_exit:
        side_u = str(side).upper()
        pos_side = "LONG" if side_u == "BUY" else "SHORT"

        # do NOT create OPEN before exchange ACK
        existing_open = get_open_position()
        if existing_open:
            return {
                "ledger_ok": True,
                "live_attempted": False,
                "order_accepted": False,
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
        open_row = get_open_position()
        pos_id = int(open_row[0]) if open_row else None
        if not pos_id:
            logging.error(
                "EXIT requested but no OPEN position found (symbol=%s interval=%s strategy=%s)",
                cfg_used.symbol, cfg_used.interval, STRATEGY_NAME
            )
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
                "order_accepted": False,
                "live_ok": False,
                "blocked_reason": "EXIT_NO_OPEN_POSITION",
                "client_order_id": None,
                "resp": None,
            }

        client_order_id = make_client_order_id(
            cfg_used.symbol, STRATEGY_NAME, cfg_used.interval, side, candle_open_time, pos_id=pos_id, tag="X"
        )

        # EXIT CID can still be pre-attached after we know there is a real OPEN
        conn_exec = get_db_conn()
        cur_exec = conn_exec.cursor()
        try:
            attach_exit_order_id_with_conn(cur_exec, int(pos_id), None, client_order_id)
            conn_exec.commit()
        except Exception:
            conn_exec.rollback()
            logging.exception("pre-attach exit_client_order_id failed pos_id=%s", pos_id)
        finally:
            cur_exec.close()
            conn_exec.close()

    pre = preflight_live_order(
        get_exchange_client(),
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
            "order_accepted": False,
            "live_ok": False,
            "blocked_reason": (pre or {}).get("reason") or "LIVE_PREFLIGHT_FAILED",
            "client_order_id": client_order_id,
            "resp": pre,
        }

    resp = place_live_order(
        get_exchange_client(),
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
        order_purpose=("EXIT" if is_exit else "ENTRY"),
        strategy=STRATEGY_NAME,
        interval=cfg_used.interval,
        exchange_source=normalize_exchange_source(
            os.environ.get("EXCHANGE")
            or os.environ.get("EXCHANGE_PROVIDER")
            or "BINANCE"
        ),
    )

    if not resp or not resp.get("ok"):
        logging.error(
            "LIVE order failed/blocked AFTER ledger reservation (symbol=%s side=%s qty=%.8f is_exit=%s).",
            cfg_used.symbol, side, float(qty_btc), bool(is_exit),
        )
        emit_strategy_event(
            event_type="BLOCKED",
            decision=side,
            reason=(resp or {}).get("reason") or "LIVE_ORDER_FAILED",
            price=price,
            candle_open_time=candle_open_time,
            info={
                "is_exit": bool(is_exit),
                "pos_id": int(pos_id) if pos_id else None,
                "client_order_id": client_order_id,
                "resp": resp,
            },
        )

        # ENTRY fail => no OPEN row exists, by design
        # EXIT fail  => existing OPEN stays OPEN
        return {
            "ledger_ok": True,
            "live_attempted": True,
            "order_accepted": bool((resp or {}).get("order_accepted", False)),
            "executed": bool((resp or {}).get("executed", False)),
            "fully_executed": bool((resp or {}).get("fully_executed", False)),
            "executed_qty": float((resp or {}).get("executed_qty") or 0.0),
            "requested_qty": float((resp or {}).get("requested_qty") or qty_btc),
            "order_id": (resp or {}).get("order_id"),
            "exchange_status": (resp or {}).get("exchange_status"),
            "live_ok": False,
            "blocked_reason": (resp or {}).get("reason") or "LIVE_ORDER_FAILED",
            "client_order_id": client_order_id,
            "resp": resp,
        }

    raw = (resp or {}).get("resp") or {}
    order_id = raw.get("orderId")
    entry_outcome = None

    if not is_exit:
        entry_outcome = normalize_entry_execution_outcome(
            resp,
            requested_qty=qty_btc,
            client_order_id=client_order_id,
        )
        order_id = entry_outcome.order_id
        if entry_outcome.executed and not order_id:
            logging.error("LIVE ENTRY ACK missing orderId symbol=%s resp=%s", cfg_used.symbol, raw)
            emit_strategy_event(
                event_type="BLOCKED",
                decision=side,
                reason="LIVE_ACK_MISSING_ORDER_ID",
                price=price,
                candle_open_time=candle_open_time,
                info={
                    "is_exit": False,
                    "client_order_id": client_order_id,
                    "resp": raw,
                },
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
            side_u = str(side).upper()
            pos_side = "LONG" if side_u == "BUY" else "SHORT"
            try:
                pos_id = open_position_from_live_ack(
                    side=str(pos_side),
                    qty=float(entry_outcome.executed_qty),
                    entry_price=float(price),
                    entry_client_order_id=str(client_order_id),
                    entry_order_id=str(order_id),
                )
            except Exception:
                pos_id = None
                logging.exception(
                    "RSI live entry fill position write failed symbol=%s cid=%s order_id=%s",
                    cfg_used.symbol, client_order_id, order_id,
                )

            if pos_id is None:
                emit_strategy_event(
                    event_type="BLOCKED",
                    decision=side,
                    reason="LIVE_ENTRY_FILL_BUT_POSITION_NOT_OPENED",
                    price=price,
                    candle_open_time=candle_open_time,
                    info={
                        "client_order_id": client_order_id,
                        "order_id": order_id,
                        "executed_qty": entry_outcome.executed_qty,
                        "resp": raw,
                    },
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

    if is_exit and pos_id:
        conn_exec = get_db_conn()
        cur_exec = conn_exec.cursor()
        try:
            attach_exit_order_id_with_conn(
                cur_exec,
                int(pos_id),
                str(order_id) if order_id else None,
                client_order_id,
            )
            conn_exec.commit()
        except Exception:
            conn_exec.rollback()
            logging.exception("attach exit order ids failed pos_id=%s order_id=%s", pos_id, order_id)
        finally:
            cur_exec.close()
            conn_exec.close()
    elif is_exit and not order_id:
        logging.error("RSI: LIVE EXIT ACK missing orderId pos_id=%s resp=%s", pos_id, raw)

    if entry_outcome is not None:
        live_ok = bool(entry_outcome.executed)
        status_raw = str(entry_outcome.exchange_status or "").upper()
        executed_raw = entry_outcome.executed_qty
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
            "cummulative_quote_qty": cquote_f,
            "avg_price": float(avg_price) if avg_price is not None else None,
        },
    )

    if entry_outcome is not None:
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

    result = {
        "ledger_ok": True,
        "live_attempted": True,
        "order_accepted": bool((resp or {}).get("order_accepted", False)),
        "executed": bool((resp or {}).get("executed", live_ok)),
        "fully_executed": bool(
            (resp or {}).get("fully_executed", False)
            or status_raw == "FILLED"
            or executed_f >= float(qty_btc) * 0.999
        ),
        "executed_qty": float((resp or {}).get("executed_qty") or executed_f),
        "requested_qty": float((resp or {}).get("requested_qty") or qty_btc),
        "order_id": (resp or {}).get("order_id") or order_id,
        "exchange_status": (resp or {}).get("exchange_status") or status_raw,
        "live_ok": live_ok,
        "blocked_reason": None if live_ok else "ACK_NO_FILL",
        "client_order_id": client_order_id,
        "resp": (resp or {}).get("resp"),
    }
    if is_exit:
        _apply_rsi_partial_exit(
            result, pos_id=pos_id, cfg_used=cfg_used, side=side, price=price,
            reason=reason, candle_open_time=candle_open_time,
        )
    return result


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
        slot_result = simulated_order_write_status(inserted)
        emit_strategy_event(
            event_type="BLOCKED",
            decision=side,
            reason=slot_result,
            price=price,
            candle_open_time=candle_open_time,
            info={"is_exit": True, "qty_btc": float(qty_btc), "reason_text": reason},
        )
        return {
            "ledger_ok": False,
            "live_attempted": False,
            "order_accepted": False,
            "live_ok": False,
            "blocked_reason": slot_result,
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
            "order_accepted": False,
            "live_ok": False,
            "paper_executed": True,
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
            "order_accepted": False,
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
            "order_accepted": False,
            "live_ok": False,
            "blocked_reason": "EXIT_NO_OPEN_POSITION",
            "client_order_id": None,
            "resp": None,
        }

    base_client_order_id = make_client_order_id(
        cfg_used.symbol, STRATEGY_NAME, cfg_used.interval, side, candle_open_time, pos_id=pos_id, tag="X"
    )

    # IMPORTANT: persist exit_client_order_id BEFORE sending orders
    # reconcile_positions uses origClientOrderId -> must match a real exchange order client id
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
        result = {
            "ledger_ok": True,
            "live_attempted": bool(out.get("attempted", True)),
            "order_accepted": bool(out.get("order_accepted", False)),
            "executed": bool(out.get("executed", False)),
            "fully_executed": bool(out.get("fully_executed", False)),
            "executed_qty": float(out.get("executed_qty") or 0.0),
            "requested_qty": float(out.get("requested_qty") or qty_btc),
            "order_id": out.get("order_id"),
            "exchange_status": out.get("exchange_status"),
            "live_ok": False,
            "blocked_reason": "LIVE_ORDER_FAILED",
            "client_order_id": base_client_order_id,
            "resp": out,
        }
        _apply_rsi_partial_exit(
            result, pos_id=pos_id, cfg_used=cfg_used, side=side, price=price,
            reason=reason, candle_open_time=candle_open_time,
        )
        return result

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

    result = {
        "ledger_ok": True,
        "live_attempted": True,
        "order_accepted": bool(out.get("order_accepted", out.get("live_ok") is True)),
        "executed": bool(out.get("executed", out.get("live_ok") is True)),
        "fully_executed": bool(out.get("fully_executed", False)),
        "executed_qty": float(out.get("executed_qty") or 0.0),
        "requested_qty": float(out.get("requested_qty") or qty_btc),
        "order_id": out.get("order_id"),
        "exchange_status": out.get("exchange_status"),
        "live_ok": True,
        "blocked_reason": None,
        "client_order_id": base_client_order_id,
        "resp": out,
    }
    _apply_rsi_partial_exit(
        result, pos_id=pos_id, cfg_used=cfg_used, side=side, price=price,
        reason=reason, candle_open_time=candle_open_time,
    )
    return result


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
        (SYMBOL, STRATEGY_NAME, INTERVAL, json.dumps(sanitize_json(info), allow_nan=False)),
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


def open_position(side: str, qty: float, entry_price: float, entry_client_order_id: str | None,
                  *, market_regime: str | None = None) -> int | None:
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
          symbol, strategy, interval, status, side, qty, entry_price, entry_time,
          entry_client_order_id, market_regime
        )
        VALUES (%s, %s, %s, 'OPEN', %s, %s, %s, now(), %s, %s)
        RETURNING id;
        """,
        (SYMBOL, STRATEGY_NAME, INTERVAL, side, float(qty), float(entry_price),
         (str(entry_client_order_id) if entry_client_order_id else None), market_regime),
    )
    pos_id = int(cur.fetchone()[0])
    conn.commit()
    cur.close()
    conn.close()

    logging.info("RSI: position OPENED pos_id=%s %s qty=%.8f entry=%.2f", pos_id, side, float(qty), float(entry_price))
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
        logging.info("open_position_from_live_ack skipped – already OPEN pos_id=%s", pos_id)
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


def close_position(exit_price: float, reason: str) -> bool:
    conn = get_db_conn()
    cur = conn.cursor()

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
    row = cur.fetchone()

    if not row:
        conn.commit()
        cur.close()
        conn.close()
        logging.info("RSI: close_position skipped – no OPEN position.")
        return False

    pos_id, pos_side, pos_entry_price, pos_entry_time = row

    enriched_reason = build_exit_reason_context(
        base_reason=reason,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        interval=INTERVAL,
        side=pos_side,
        entry_price=pos_entry_price,
        exit_price=exit_price,
        entry_time=pos_entry_time,
        asof_time=datetime.now(timezone.utc),
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
        logging.info("RSI: position CLOSED exit=%.2f reason=%s", float(exit_price), enriched_reason)
    else:
        logging.info("RSI: close_position skipped – no OPEN position.")
    return closed


def ssot_apply_positions_paper(
    *,
    side: str,
    price: float,
    qty_btc: float,
    candle_open_time,
    is_exit: bool,
    cfg_used: RuntimeConfig,
    reason_text: str,
    evaluation: EvaluationContext | None = None,
    expected_position_id: int | None = None,
) -> dict:
    """
    PAPER mode: mirror LIVE SSOT behavior into `positions` (hard truth),
    but without sending a real exchange order.
    """
    pos_id = None
    client_order_id = None

    if not is_exit:
        # ENTRY
        side_u = str(side).upper()
        pos_side = "LONG" if side_u == "BUY" else "SHORT"

        pos_id = open_position(
            side=str(pos_side),
            qty=float(qty_btc),
            entry_price=float(price),
            entry_client_order_id=None,
            market_regime=(evaluation.market_regime if evaluation is not None else None),
        )
        if pos_id is None:
            return {"ok": False, "blocked_reason": "ALREADY_OPEN", "pos_id": None, "client_order_id": None}

        client_order_id = make_client_order_id(
            cfg_used.symbol, STRATEGY_NAME, cfg_used.interval, side, candle_open_time, pos_id=int(pos_id), tag="E"
        )

        # attach entry_client_order_id (order_id remains NULL in PAPER)
        conn_exec = get_db_conn()
        cur_exec = conn_exec.cursor()
        try:
            attach_entry_order_id_with_conn(cur_exec, int(pos_id), None, client_order_id)
            conn_exec.commit()
        except Exception:
            conn_exec.rollback()
            logging.exception("PAPER: attach entry_client_order_id failed pos_id=%s", pos_id)
        finally:
            cur_exec.close()
            conn_exec.close()

        return {"ok": True, "blocked_reason": None, "pos_id": int(pos_id), "client_order_id": client_order_id}

    # EXIT
    open_row = get_open_position() if expected_position_id is None else None
    pos_id = (
        int(expected_position_id)
        if expected_position_id is not None
        else int(open_row[0]) if open_row else None
    )
    if not pos_id:
        return {"ok": False, "blocked_reason": "EXIT_NO_OPEN_POSITION", "pos_id": None, "client_order_id": None}
    client_order_id = make_client_order_id(
        cfg_used.symbol, STRATEGY_NAME, cfg_used.interval, side, candle_open_time, pos_id=int(pos_id), tag="X"
    )

    # Resolve/attach the PAPER exit identity only. The canonical simulated-fill
    # writer owns inventory mutation, lifecycle, Financial Truth, and CLOSED.
    conn_exec = get_db_conn()
    cur_exec = conn_exec.cursor()
    try:
        deployment_id = os.environ.get(
            "DEPLOYMENT_ID",
            os.environ.get("WALTRADE_DEPLOYMENT_ID", "local-paper"),
        )
        if not paper_position_mutation_allowed_cursor(
            cur_exec, position_id=pos_id, deployment_id=deployment_id
        ):
            conn_exec.rollback()
            return {
                "ok": False,
                "blocked_reason": "PAPER_ADOPTION_GENERATION_BLOCKED",
                "pos_id": pos_id,
                "client_order_id": client_order_id,
            }
        attach_exit_order_id_with_conn(cur_exec, int(pos_id), None, client_order_id)
        conn_exec.commit()
    except Exception:
        conn_exec.rollback()
        logging.exception("PAPER: exit identity attach failed pos_id=%s", pos_id)
        return {
            "ok": False,
            "blocked_reason": "PAPER_EXIT_IDENTITY_FAILED",
            "pos_id": int(pos_id),
            "client_order_id": client_order_id,
        }
    finally:
        cur_exec.close()
        conn_exec.close()

    return {
        "ok": True,
        "blocked_reason": None,
        "pos_id": int(pos_id),
        "client_order_id": client_order_id,
        "paper_pos_action": "EXIT_PENDING_CANONICAL_PERSISTENCE",
        "position_close_succeeded": None,
    }


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
    market_regime: str | None = None,
    regime_source_provenance: dict | None = None,
):
    conn = get_db_conn()
    cur = conn.cursor()
    inserted = create_simulated_order_cursor(
        cur, symbol=symbol, interval=interval, strategy=strategy, side=side,
        price=Decimal(str(price)), quantity=Decimal(str(qty_btc)),
        reason=reason, candle_open_time=candle_open_time, is_exit=is_exit,
        rsi_14=None if rsi_14 is None else Decimal(str(rsi_14)),
        ema_21=None if ema_21 is None else Decimal(str(ema_21)),
        market_regime=market_regime,
        regime_source_provenance=regime_source_provenance,
    )
    if inserted:
        conn.commit()
    else:
        conn.rollback()
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
    "MIN_PROFIT_FOR_SOFT_EXIT_PCT=%.3f|BE_TRIGGER_PCT=%.3f|BE_OFFSET_PCT=%.3f|MIN_EDGE_PCT=%.3f|EMA_SLOPE_FILTER=%s|"
    "PROFIT_LOCK_ENABLED=%s|PROFIT_LOCK_ARM_PCT=%.3f|PROFIT_LOCK_FLOOR_PCT=%.3f|PROFIT_LOCK_TRAIL_DROP_PCT=%.3f|PROFIT_LOCK_MIN_AGE_MINUTES=%.1f|PROFIT_LOCK_STRATEGIES=%s",
    SYMBOL, STRATEGY_NAME, RSI_OVERSOLD, RSI_OVERBOUGHT,
    STOP_LOSS_PCT, TAKE_PROFIT_PCT, MAX_POSITION_MINUTES, DAILY_MAX_LOSS_PCT,
    ORDER_QTY_BTC, float(ORDER_NOTIONAL_USDC), float(MIN_NOTIONAL_BUFFER_PCT),
    MAX_DIST_FROM_EMA_PCT, TREND_BUFFER, ENTRY_BUFFER_PCT, bool(TIME_EXIT_ENABLED),
    bool(RSI_SOFT_EXIT_ENABLED), float(RSI_EXIT_OVERBOUGHT), float(RSI_EXIT_OVERSOLD),
    float(RSI_REBOUND_DELTA), float(ATR_MIN_PCT), bool(EMA_SLOPE_BLOCK),
    float(MIN_PROFIT_FOR_SOFT_EXIT_PCT), float(BE_TRIGGER_PCT), float(BE_OFFSET_PCT), float(MIN_EDGE_PCT), bool(EMA_SLOPE_FILTER),
    bool(PROFIT_LOCK_CONFIG.enabled), float(PROFIT_LOCK_CONFIG.arm_pct), float(PROFIT_LOCK_CONFIG.floor_pct), float(PROFIT_LOCK_CONFIG.trail_drop_pct), float(PROFIT_LOCK_CONFIG.min_age_minutes), ",".join(sorted(PROFIT_LOCK_CONFIG.strategies)),
)

# =========================
# CANDLES + INDICATORS
# =========================


def fetch_klines(limit=50):
    start = time.perf_counter()
    klines = get_exchange_client().get_klines(
        symbol=SYMBOL, interval=INTERVAL, limit=limit
    )
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
    with read_only_db_conn(get_db_conn) as read_conn:
        df = pd.read_sql_query(
            """
            SELECT id, open_time, high, low, close
            FROM candles
            WHERE symbol=%s AND interval=%s
            ORDER BY open_time
            """,
            read_conn,
            params=(SYMBOL, INTERVAL),
        )

    if df.empty:
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

    with db_write_conn(get_db_conn) as (conn, cur):
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


def _as_aware_utc(value):
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _rsi_evaluation_context(open_time, evaluation_started_at, snap=None):
    cfg_effective = snap["cfg_effective"] if snap is not None else cfg
    bc = snap["bc"] if snap is not None else None
    candle_time = _as_aware_utc(open_time)
    paper_mode = cfg_effective.trading_mode != "LIVE"
    market_regime, regime_confidence, regime_context = evaluation_regime_fields(
        get_db_conn, symbol=SYMBOL, interval=INTERVAL,
        decision_candle_timestamp=candle_time, paper_mode=paper_mode,
    )
    return EvaluationContext(
        deployment_id=os.environ.get(
            "DEPLOYMENT_ID",
            os.environ.get("WALTRADE_DEPLOYMENT_ID", "UNKNOWN"),
        ),
        environment=DB_NAME,
        symbol=SYMBOL,
        interval=INTERVAL,
        strategy=STRATEGY_NAME,
        candle_open_time=candle_time,
        evaluation_started_at=evaluation_started_at,
        engine_name=STRATEGY_NAME,
        engine_version=os.environ.get("BOT_VERSION"),
        runtime_enabled=(bool(bc.enabled) if bc is not None else None),
        live_orders_enabled=(
            bool(snap["allowed_orders_entry"]) if snap is not None else None
        ),
        market_regime=market_regime,
        regime_confidence=regime_confidence,
        paper_mode=paper_mode,
        context={"contract_version": "FINAL_DECISION_V1", **regime_context},
    )


def _build_no_new_candle_decision(row):
    """Return the non-persisted terminal result for main-loop candle deduplication."""
    open_time = row[0]
    now = datetime.now(timezone.utc)
    evaluation = _rsi_evaluation_context(open_time, now)
    return FinalDecision.idle(
        evaluation,
        DecisionReason.NO_NEW_CANDLE,
        finished_at=now,
        reason_text="NO_NEW_CANDLE",
        details={"last_processed": str(LAST_PROCESSED_OPEN_TIME)},
    )


def build_no_new_candle_decision(row):
    return finalize_decision_observation(
        _build_no_new_candle_decision(row), source_service="bot-rsi",
    )


def _rsi_execution_outcome(result, cfg_effective):
    if isinstance(result, ExecutionOutcome):
        return result
    return ExecutionOutcome.from_legacy(
        result, paper_mode=cfg_effective.trading_mode != "LIVE"
    )


def _rsi_execution_succeeded(result, cfg_effective):
    outcome = _rsi_execution_outcome(result, cfg_effective)
    return outcome.ledger_ok and (
        cfg_effective.trading_mode != "LIVE" or outcome.operation_succeeded
    )


def _rsi_exit_decision(
    evaluation,
    res,
    cfg_effective,
    *,
    reason_code,
    reason_text,
    side,
    price,
    position_id,
):
    outcome = _rsi_execution_outcome(res, cfg_effective)
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
    }

    if not outcome.ledger_ok:
        failure_subtype = DecisionSubtype.LEDGER_FAILURE
    elif cfg_effective.trading_mode != "LIVE":
        return FinalDecision.exit_result(
            evaluation,
            reason_code,
            finished_at=datetime.now(timezone.utc),
            reference_price=Decimal(str(price)),
            side=side,
            position_id=position_id,
            reason_text=reason_text,
            details=details,
        )

    elif outcome.fully_executed:
        return FinalDecision.exit_result(
            evaluation,
            reason_code,
            finished_at=datetime.now(timezone.utc),
            reference_price=Decimal(str(price)),
            side=side,
            position_id=position_id,
            reason_text=reason_text,
            details=details,
        )

    elif outcome.stage in {ExecutionStage.SUPPRESSED, ExecutionStage.NOT_ATTEMPTED}:
        if outcome.blocked_reason == "EXIT_NO_OPEN_POSITION":
            return FinalDecision.no_position(
                evaluation,
                DecisionReason.NO_OPEN_POSITION,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)),
                side=side,
                reason_text=outcome.blocked_reason,
                details=details,
            )
        return FinalDecision.action_suppressed(
            evaluation,
            DecisionReason.EXECUTION_NOT_ATTEMPTED,
            finished_at=datetime.now(timezone.utc),
            reference_price=Decimal(str(price)),
            side=side,
            reason_text=outcome.blocked_reason or "LIVE_EXIT_NOT_ATTEMPTED",
            details=details,
        )
    elif outcome.executed:
        failure_subtype = DecisionSubtype.PARTIAL_EXECUTION
    elif outcome.order_accepted:
        failure_subtype = DecisionSubtype.ORDER_ACCEPTED_NOT_FILLED
    else:
        failure_subtype = DecisionSubtype.ORDER_REJECTED

    return FinalDecision.technical_failure_result(
        evaluation,
        DecisionReason.EXECUTION_FAILED,
        failure_subtype,
        finished_at=datetime.now(timezone.utc),
        reference_price=Decimal(str(price)),
        side=side,
        reason_text=outcome.blocked_reason or "EXIT_BLOCKED",
        signal_detected=True,
        entry_attempted=outcome.attempted,
        order_submitted=outcome.order_accepted,
        trade_executed=outcome.executed,
        details={
            **details,
            "exit_reason": reason_text,
        },
    )


def _rsi_entry_decision(
    evaluation,
    res,
    cfg_effective,
    *,
    side,
    price,
    reason_text,
):
    outcome = _rsi_execution_outcome(res, cfg_effective)
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
    }
    common = {
        "finished_at": datetime.now(timezone.utc),
        "reference_price": Decimal(str(price)),
        "side": side,
        "details": details,
    }

    if not outcome.ledger_ok:
        return FinalDecision.technical_failure_result(
            evaluation,
            DecisionReason.EXECUTION_FAILED,
            DecisionSubtype.LEDGER_FAILURE,
            reason_text=outcome.blocked_reason or "ENTRY_LEDGER_FAILED",
            signal_detected=True,
            entry_attempted=outcome.attempted,
            order_submitted=outcome.order_accepted,
            trade_executed=outcome.executed,
            **common,
        )
    if cfg_effective.trading_mode != "LIVE":
        return FinalDecision.paper_simulation(
            evaluation,
            DecisionReason.SSOT_EXECUTE_AND_RECORD,
            reason_text=reason_text,
            **common,
        )
    if outcome.stage in {ExecutionStage.SUPPRESSED, ExecutionStage.NOT_ATTEMPTED}:
        return FinalDecision.action_suppressed(
            evaluation,
            DecisionReason.EXECUTION_NOT_ATTEMPTED,
            reason_text=outcome.blocked_reason or "LIVE_ENTRY_NOT_ATTEMPTED",
            **common,
        )
    if outcome.fully_executed:
        return FinalDecision.trade_executed_result(
            evaluation,
            DecisionReason.SSOT_EXECUTE_AND_RECORD,
            reason_text=reason_text,
            **common,
        )

    if outcome.executed:
        failure_subtype = DecisionSubtype.PARTIAL_EXECUTION
    elif outcome.order_accepted:
        failure_subtype = DecisionSubtype.ORDER_ACCEPTED_NOT_FILLED
    else:
        failure_subtype = DecisionSubtype.ORDER_REJECTED
    return FinalDecision.technical_failure_result(
        evaluation,
        DecisionReason.EXECUTION_FAILED,
        failure_subtype,
        reason_text=outcome.blocked_reason or "LIVE_ENTRY_NOT_FILLED",
        signal_detected=True,
        entry_attempted=outcome.attempted,
        order_submitted=outcome.order_accepted,
        trade_executed=outcome.executed,
        **common,
    )


def _run_strategy(row, prev_row=None):
    evaluation_started_at = datetime.now(timezone.utc)
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
        pre_evaluation = _rsi_evaluation_context(
            open_time, evaluation_started_at
        )

        close_price = float(close_px) if close_px is not None else None
        high_price  = float(high_px) if high_px is not None else None
        low_price   = float(low_px) if low_px is not None else None
        price = close_price  # jednoznacznie: price = close świecy

        if close_price is None or high_price is None or low_price is None:
            fallback_price = float(open_px) if open_px is not None else None
            missing_fields = [
                name for name, value in (
                    ("close", close_px), ("high", high_px), ("low", low_px)
                )
                if value is None
            ]
            emit_strategy_event(
                event_type="BLOCKED",
                reason="CANDLE_MISSING_FIELDS",
                decision=None,
                price=fallback_price,
                candle_open_time=open_time,
                info={"open": open_px, "high": high_px, "low": low_px, "close": close_px},
            )
            return FinalDecision.system_not_evaluated(
                pre_evaluation, DecisionReason.CANDLE_MISSING_FIELDS,
                finished_at=datetime.now(timezone.utc),
                reason_text="CANDLE_MISSING_FIELDS",
                details={"missing_fields": missing_fields},
            )

        if ema_21 is None or rsi_14 is None or atr_14 is None:            
            emit_blocked(
                reason="INDICATORS_NOT_READY",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"ema_21": ema_21, "rsi_14": rsi_14},
            )
            heartbeat({"price": price, "open_time": str(open_time), "status": "indicators_not_ready"})
            return FinalDecision.system_not_evaluated(
                pre_evaluation, DecisionReason.INDICATORS_NOT_READY,
                finished_at=datetime.now(timezone.utc),
                reason_text="INDICATORS_NOT_READY",
            )

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
        evaluation = _rsi_evaluation_context(
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
        time_exit_enabled = bool(TIME_EXIT_ENABLED) and hard_time_exit_enabled()
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
            return FinalDecision.entry_suppressed(
                evaluation,
                DecisionReason.BOT_MODE_HALT,
                DecisionSubtype.EXECUTION_DISABLED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)),
                reason_text="BOT_MODE_HALT",
                signal_detected=False,
            )

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
            panic_result = None
            panic_side = None
            panic_position_id = None
            emit_strategy_event(
                event_type="CONFIG_APPLIED",
                decision=None,
                reason="PANIC_TRIGGERED",
                price=price,
                candle_open_time=open_time,
                info={"mode": "PANIC"},
            )
            if pos:
                panic_position_id, side, qty, _entry_price, _entry_time = pos
                side_u = str(side).upper()
                qty_f = float(qty)

                exit_side = "SELL" if side_u == "LONG" else "BUY"
                panic_side = exit_side

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
                if _rsi_execution_succeeded(res, cfg_effective):
                    if cfg_effective.trading_mode == "LIVE":
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
                    return _rsi_exit_decision(
                        evaluation, res, cfg_effective,
                        reason_code=DecisionReason.STRATEGY_EXIT,
                        reason_text="PANIC CLOSE", side=exit_side, price=price,
                        position_id=panic_position_id,
                    )
                panic_result = res
            if not pos:
                emit_blocked(
                    reason="PANIC_NO_POSITION",
                    decision=None,
                    price=price,
                    candle_open_time=open_time,
                    info={},
                )
            set_mode("HALT", reason="Panic executed; halting.")
            if panic_result is not None:
                return _rsi_exit_decision(
                    evaluation, panic_result, cfg_effective,
                    reason_code=DecisionReason.STRATEGY_EXIT,
                    reason_text="PANIC CLOSE", side=panic_side, price=price,
                    position_id=panic_position_id,
                )
            return FinalDecision.no_position(
                evaluation, DecisionReason.NO_OPEN_POSITION,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)),
                reason_text="PANIC_NO_POSITION",
                details={"operation": "PANIC_CLOSE"},
            )


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
                    if _rsi_execution_succeeded(res, cfg_effective):
                        if cfg_effective.trading_mode == "LIVE":
                            close_position(exit_price=exec_px, reason="TAKE_PROFIT")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision="SELL",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return _rsi_exit_decision(
                        evaluation, res, cfg_effective,
                        reason_code=DecisionReason.TAKE_PROFIT,
                        reason_text=reason, side="SELL", price=exec_px,
                        position_id=_pos_id,
                    )

                # SL intrabar
                if STOP_LOSS_PCT > 0 and low_price <= sl_level:
                    exec_px = price
                    reason = f"RSI STOP LOSS LONG intrabar low={low_price:.2f} <= sl={sl_level:.2f}"
                    res = execute_and_record("SELL", exec_px, qty_f, reason, open_time, cfg_used=cfg_effective, allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"], is_exit=True,)
                    if _rsi_execution_succeeded(res, cfg_effective):
                        if cfg_effective.trading_mode == "LIVE":
                            close_position(exit_price=exec_px, reason="STOP_LOSS")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision="SELL",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return _rsi_exit_decision(
                        evaluation, res, cfg_effective,
                        reason_code=DecisionReason.STOP_LOSS,
                        reason_text=reason, side="SELL", price=exec_px,
                        position_id=_pos_id,
                    )

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
                    if _rsi_execution_succeeded(res, cfg_effective):
                        if cfg_effective.trading_mode == "LIVE":
                            close_position(exit_price=exec_px, reason="TAKE_PROFIT_SHORT")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision="BUY",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return _rsi_exit_decision(
                        evaluation, res, cfg_effective,
                        reason_code=DecisionReason.TAKE_PROFIT,
                        reason_text=reason, side="BUY", price=exec_px,
                        position_id=_pos_id,
                    )

                # SL intrabar
                if STOP_LOSS_PCT > 0 and high_price >= sl_level:
                    exec_px = price
                    reason = f"RSI STOP LOSS SHORT intrabar high={high_price:.2f} >= sl={sl_level:.2f}"
                    res = execute_and_record("BUY", exec_px, qty_f, reason, open_time, cfg_used=cfg_effective, allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"], is_exit=True,)
                    if _rsi_execution_succeeded(res, cfg_effective):
                        if cfg_effective.trading_mode == "LIVE":
                            close_position(exit_price=exec_px, reason="STOP_LOSS_SHORT")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision="BUY",
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return _rsi_exit_decision(
                        evaluation, res, cfg_effective,
                        reason_code=DecisionReason.STOP_LOSS,
                        reason_text=reason, side="BUY", price=exec_px,
                        position_id=_pos_id,
                    )
            
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
                        if _rsi_execution_succeeded(res, cfg_effective):
                            if cfg_effective.trading_mode == "LIVE":
                                close_position(exit_price=price, reason="BE_PROTECT")
                        else:
                            emit_blocked(reason="EXIT_BLOCKED", decision=exit_side, price=price, candle_open_time=open_time, info={"res": res})
                        return _rsi_exit_decision(
                            evaluation, res, cfg_effective,
                            reason_code=DecisionReason.BREAK_EVEN_PROTECT,
                            reason_text=reason_exit, side=exit_side, price=price,
                            position_id=_pos_id,
                        )
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
                        if _rsi_execution_succeeded(res, cfg_effective):
                            if cfg_effective.trading_mode == "LIVE":
                                close_position(exit_price=price, reason="BE_PROTECT")
                        else:
                            emit_blocked(reason="EXIT_BLOCKED", decision=exit_side, price=price, candle_open_time=open_time, info={"res": res})
                        return _rsi_exit_decision(
                            evaluation, res, cfg_effective,
                            reason_code=DecisionReason.BREAK_EVEN_PROTECT,
                            reason_text=reason_exit, side=exit_side, price=price,
                            position_id=_pos_id,
                        )
                
            # PROFIT LOCK: stateful high-watermark guard based on candle path since entry.
            # Captures profits for RSI/TREND/SUPERTREND/BBRANGE and intentionally excludes legacy SUPER_TREND config rows.
            if pos_entry_time is not None:
                if pos_entry_time.tzinfo is None:
                    pos_entry_time = pos_entry_time.replace(tzinfo=timezone.utc)
                age_minutes = (datetime.now(timezone.utc) - pos_entry_time).total_seconds() / 60.0
                path = load_position_path_snapshot(
                    symbol=SYMBOL,
                    interval=INTERVAL,
                    entry_time=pos_entry_time,
                    asof_open_time=open_time,
                    entry_price=entry_f,
                )
                profit_lock_decision = evaluate_profit_lock(
                    strategy=STRATEGY_NAME,
                    side=pos_side_u,
                    age_minutes=age_minutes,
                    entry_price=entry_f,
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
                    exit_side = "SELL" if pos_side_u == "LONG" else "BUY"
                    exit_kind = str(profit_lock_decision.reason_code or "PROFIT_LOCK")
                    reason_exit = (
                        f"{exit_kind} {profit_lock_decision.trigger_type} "
                        f"peak={profit_lock_decision.peak_move_pct:.3f}% "
                        f"current={profit_lock_decision.current_move_pct:.3f}% "
                        f"floor={profit_lock_decision.floor_pct:.3f}% "
                        f"trail_drop={profit_lock_decision.trail_drop_pct:.3f}% "
                        f"age={profit_lock_decision.age_minutes:.1f}m"
                    )
                    emit_strategy_event(
                        event_type="EXIT_SIGNAL",
                        decision=exit_side,
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
                    res = execute_exit_safe(
                        exit_side=exit_side,
                        price=price,
                        qty_btc=qty_f,
                        reason_text=reason_exit,
                        candle_open_time=open_time,
                        cfg_used=cfg_effective,
                        allow_live_orders=snap["allowed_orders_exit"],
                        allow_meta=snap["allow_meta_exit"],
                        exit_kind=exit_kind,
                    )
                    if _rsi_execution_succeeded(res, cfg_effective):
                        if cfg_effective.trading_mode == "LIVE":
                            close_position(exit_price=price, reason=exit_kind)
                    else:
                        emit_blocked(reason="EXIT_BLOCKED", decision=exit_side, price=price, candle_open_time=open_time, info={"res": res})
                    return _rsi_exit_decision(
                        evaluation, res, cfg_effective,
                        reason_code=DecisionReason.PROFIT_LOCK,
                        reason_text=reason_exit, side=exit_side, price=price,
                        position_id=_pos_id,
                    )

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
                    if _rsi_execution_succeeded(res, cfg_effective):
                        if cfg_effective.trading_mode == "LIVE":
                            close_position(exit_price=price, reason="RSI_SOFT_EXIT")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision=exit_side,
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return _rsi_exit_decision(
                        evaluation, res, cfg_effective,
                        reason_code=DecisionReason.STRATEGY_EXIT,
                        reason_text=reason_exit, side=exit_side, price=price,
                        position_id=_pos_id,
                    )

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
                    if _rsi_execution_succeeded(res, cfg_effective):
                        if cfg_effective.trading_mode == "LIVE":
                            close_position(exit_price=price, reason="RSI_SOFT_EXIT")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision=exit_side,
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return _rsi_exit_decision(
                        evaluation, res, cfg_effective,
                        reason_code=DecisionReason.STRATEGY_EXIT,
                        reason_text=reason_exit, side=exit_side, price=price,
                        position_id=_pos_id,
                    )

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
                    if _rsi_execution_succeeded(res, cfg_effective):
                        if cfg_effective.trading_mode == "LIVE":
                            close_position(exit_price=price, reason="TIME_EXIT")
                    else:
                        emit_blocked(
                            reason="EXIT_BLOCKED",
                            decision=side_timeout,
                            price=price,
                            candle_open_time=open_time,
                            info={"res": res},
                        )
                    return _rsi_exit_decision(
                        evaluation, res, cfg_effective,
                        reason_code=DecisionReason.TIME_EXIT,
                        reason_text=reason_timeout, side=side_timeout, price=price,
                        position_id=_pos_id,
                    )
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
                    "time_exit_policy": time_exit_policy_name(),
                    "max_position_minutes": int(max_pos_minutes),
                    "rsi_14": float(rsi_val),
                    "ema_21": float(ema_val),
                    "soft_exit_enabled": bool(int(RSI_SOFT_EXIT_ENABLED) == 1),
                    "exit_overbought": float(RSI_EXIT_OVERBOUGHT),
                    "exit_oversold": float(RSI_EXIT_OVERSOLD),
                },
            )
            return FinalDecision.position_hold(
                evaluation,
                DecisionReason.POSITION_HOLD,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)),
                side=pos_side_u,
                position_id=_pos_id,
                reason_text="NO_EXIT_SIGNAL",
                details={"age_minutes": age_minutes},
            )

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
            return FinalDecision.entry_suppressed(
                evaluation, DecisionReason.DISABLE_HOURS,
                DecisionSubtype.EXECUTION_DISABLED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)),
                reason_text="DISABLE_HOURS",
            )
        
        if not bc.enabled:
            emit_blocked(
                reason="BOT_DISABLED",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"enabled": False},
            )
            return FinalDecision.entry_suppressed(
                evaluation, DecisionReason.BOT_DISABLED,
                DecisionSubtype.LIVE_DISABLED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)),
                reason_text="BOT_DISABLED",
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
                    return FinalDecision.entry_blocked(
                        evaluation, DecisionReason.DAILY_MAX_LOSS_POSITIONS,
                        DecisionSubtype.RISK_BLOCKED,
                        finished_at=datetime.now(timezone.utc),
                        reference_price=Decimal(str(price)),
                        reason_text="DAILY_MAX_LOSS_POSITIONS",
                        signal_detected=False,
                        details=pos_payload,
                    )

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
            return FinalDecision.entry_suppressed(
                evaluation, DecisionReason.POLICY_BLOCK,
                DecisionSubtype.READINESS_BLOCKED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)),
                reason_text="MAX_DIST_FROM_EMA",
                details={"dist_pct": dist_from_ema_pct},
            )

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
            return FinalDecision.system_not_evaluated(
                evaluation, DecisionReason.INDICATORS_NOT_READY,
                finished_at=datetime.now(timezone.utc),
                reason_text="PREV_INDICATORS_NOT_READY",
            )

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
            return FinalDecision.entry_suppressed(
                evaluation, DecisionReason.POLICY_BLOCK,
                DecisionSubtype.READINESS_BLOCKED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)),
                reason_text="ATR_TOO_LOW",
                details={"atr_pct": atr_pct},
            )

        # Trend filter: nie kupuj, gdy EMA spada (opcjonalnie)
        if int(EMA_SLOPE_BLOCK) == 1 and ema_val < prev_ema_val:
            emit_blocked(
                reason="EMA_SLOPE_DOWN",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"ema_now": float(ema_val), "ema_prev": float(prev_ema_val)},
            )
            return FinalDecision.entry_suppressed(
                evaluation, DecisionReason.POLICY_BLOCK,
                DecisionSubtype.READINESS_BLOCKED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)),
                reason_text="EMA_SLOPE_DOWN",
            )

        # Minimalny edge gate (opcjonalnie)
        if float(MIN_EDGE_PCT) > 0 and float(TAKE_PROFIT_PCT) < float(MIN_EDGE_PCT):
            emit_blocked(
                reason="EDGE_TOO_LOW",
                decision=None,
                price=price,
                candle_open_time=open_time,
                info={"tp_pct": float(TAKE_PROFIT_PCT), "min_edge_pct": float(MIN_EDGE_PCT)},
            )
            return FinalDecision.entry_suppressed(
                evaluation, DecisionReason.POLICY_BLOCK,
                DecisionSubtype.READINESS_BLOCKED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)),
                reason_text="EDGE_TOO_LOW",
            )

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
            return FinalDecision.no_trade(
                evaluation, DecisionReason.NO_SIGNAL,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)),
                reason_text="NO_SIGNAL_REBOUND",
                details={"rsi_prev": prev_rsi_val, "rsi_now": rsi_val},
            )

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
                return FinalDecision.signal_rejected(
                    evaluation, DecisionReason.POLICY_BLOCK,
                    finished_at=datetime.now(timezone.utc),
                    reference_price=Decimal(str(price)), side="BUY",
                    reason_text="ENTRY_BUFFER_BLOCK",
                    details={"buy_level": buy_level},
                )

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
                return FinalDecision.signal_rejected(
                    evaluation, DecisionReason.POLICY_BLOCK,
                    finished_at=datetime.now(timezone.utc),
                    reference_price=Decimal(str(price)), side="SELL",
                    reason_text="ENTRY_BUFFER_BLOCK",
                    details={"sell_level": sell_level},
                )

        # SPOT: nie otwieramy shortów
        if decision == "SELL" and cfg_effective.spot_mode:
            emit_blocked(
                reason="SPOT_SHORT_BLOCK",
                decision="SELL",
                price=price,
                candle_open_time=open_time,
                info={"spot_mode": True, "trading_mode": cfg_effective.trading_mode},
            )
            return FinalDecision.signal_rejected(
                evaluation, DecisionReason.SPOT_SHORT_BLOCK,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)), side="SELL",
                reason_text="SPOT_SHORT_BLOCK",
            )

        # --- REGIME GATE (ENTRY ONLY) ---
        gate_entry = decide_regime_gate(
            symbol=SYMBOL,
            interval=INTERVAL,
            strategy=STRATEGY_NAME,
            decision="ENTRY_CHECK",
            regime_enabled=bc.regime_enabled,
            regime_mode=bc.regime_mode,
        )

        gate_event_id = emit_regime_gate_event(
            symbol=SYMBOL,
            interval=INTERVAL,
            strategy=STRATEGY_NAME,
            decision="ENTRY_CHECK",
            d=gate_entry,
        )
        evaluation = attach_regime_gate_event(
            evaluation, gate_event_id=gate_event_id, decision=gate_entry,
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
            return FinalDecision.entry_blocked(
                evaluation, DecisionReason.REGIME_BLOCK,
                DecisionSubtype.REGIME_BLOCKED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)), side=decision,
                reason_text="REGIME_BLOCK",
                details={"why": gate_entry.why, "regime": gate_entry.regime},
            )
        
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
                get_exchange_client(),
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

        settings_snapshot = get_user_settings_snapshot()
        raw_manual_entry_addon_usdc = settings_snapshot.get("manual_entry_addon_usdc")
        manual_entry_addon_usdc = 0.0 if raw_manual_entry_addon_usdc is None else float(raw_manual_entry_addon_usdc)

        raw_three_win_boost_usdc = settings_snapshot.get("three_win_boost_usdc")
        configured_three_win_boost_usdc = 0.0 if raw_three_win_boost_usdc is None else float(raw_three_win_boost_usdc)
        recent_win_streak = get_recent_win_streak(strategy=STRATEGY_NAME, symbol=SYMBOL, interval=INTERVAL, required_wins=3)
        applied_three_win_boost_usdc = configured_three_win_boost_usdc if recent_win_streak.eligible else 0.0
        base_target_notional = float(ORDER_NOTIONAL_USDC)
        final_target_notional = base_target_notional + manual_entry_addon_usdc + applied_three_win_boost_usdc

        if manual_entry_addon_usdc > 0 or applied_three_win_boost_usdc > 0:
            qty_btc, px_live, notional_live, step, min_qty, min_notional = compute_live_qty_from_notional(
                get_exchange_client(),
                SYMBOL,
                target_notional=float(final_target_notional),
                quote_asset=QUOTE_ASSET,
                min_notional_buffer_pct=float(MIN_NOTIONAL_BUFFER_PCT),
            )

        order_notional_usdc = float(notional_live if cfg_effective.trading_mode == "LIVE" else (float(qty_btc) * float(price)))
        emit_strategy_event(
            event_type="SIZING",
            decision=decision,
            reason="FINAL_NOTIONAL",
            price=float(price),
            candle_open_time=open_time,
            info={
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
                "order_notional": float(order_notional_usdc),
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
                info={"qty": float(qty_btc)},
            )
            return FinalDecision.entry_suppressed(
                evaluation, DecisionReason.SIZING_QTY_ZERO,
                DecisionSubtype.EXECUTION_DISABLED,
                finished_at=datetime.now(timezone.utc),
                reference_price=Decimal(str(price)), side=decision,
                reason_text="SIZING_QTY_ZERO",
                signal_detected=True,
            )

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
            evaluation=evaluation,
        )
        outcome = _rsi_execution_outcome(res, cfg_effective)
        if not outcome.ledger_ok:
            logging.info("RSI: entry blocked/failed -> not opening position.")
            return _rsi_entry_decision(
                evaluation, outcome, cfg_effective,
                side=decision, price=price, reason_text=reason,
            )

        if cfg_effective.trading_mode == "LIVE" and not outcome.operation_succeeded:
            # NOT_ATTEMPTED jest już emitowane w execute_and_record() (SSOT)
            if outcome.stage in {
                ExecutionStage.SUPPRESSED, ExecutionStage.NOT_ATTEMPTED
            }:
                return _rsi_entry_decision(
                    evaluation, outcome, cfg_effective,
                    side=decision, price=price, reason_text=reason,
                )

            # attempted, ale brak fill -> to logujemy tutaj
            emit_strategy_event(
                event_type="BLOCKED",
                decision=decision,
                reason="LIVE_ENTRY_NOT_FILLED",
                price=price,
                candle_open_time=open_time,
                info={"res": res},
            )
            return _rsi_entry_decision(
                evaluation, outcome, cfg_effective,
                side=decision, price=price, reason_text=reason,
            )

        # 2) Position OPEN is created inside execute_and_record() (SSOT).
        # Do not open it again here.
        emit_strategy_event(
            event_type="POSITION_OPENED",
            decision=decision,
            reason="SSOT_EXECUTE_AND_RECORD",
            price=price,
            candle_open_time=open_time,
            info={
                "qty_btc": (
                    float(outcome.executed_qty)
                    if cfg_effective.trading_mode == "LIVE"
                    else float(qty_btc)
                )
            },
        )
        return _rsi_entry_decision(
            evaluation, outcome, cfg_effective,
            side=decision, price=price, reason_text=reason,
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


def run_strategy(row, prev_row=None):
    return finalize_decision_observation(
        _run_strategy(row, prev_row=prev_row), source_service="bot-rsi",
    )


# =========================
# MAIN LOOP
# =========================


LAST_PROCESSED_OPEN_TIME = None

def main_loop():
    global LAST_PROCESSED_OPEN_TIME

    runtime_client = get_exchange_client()
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
                    build_no_new_candle_decision(latest)
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

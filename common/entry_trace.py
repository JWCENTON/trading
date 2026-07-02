import json
import logging
from typing import Any, Dict

from common.db import get_db_conn
from common.safe_json import sanitize_json
from common.realtime_engine import compute_realtime_snapshot


TRACE_EVENT_TYPES = {
    "BLOCKED",
    "ENTRY_CHECK",
    "BUY_READY",
    "SELL_READY",
    "POSITION_HOLD",
}


def _safe_float(v):
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def _should_trace(event_type: str | None, reason: str | None) -> bool:
    et = str(event_type or "").upper()
    rs = str(reason or "").upper()

    if et in TRACE_EVENT_TYPES:
        return True

    return (
        "NO_SIGNAL" in rs
        or "ATR_TOO_LOW" in rs
        or "REBOUND" in rs
        or "MOMENTUM" in rs
        or "EMA" in rs
        or "BREAKOUT" in rs
        or "REGIME_BLOCK" in rs
    )


def record_entry_trace_shadow(
    *,
    symbol: str,
    interval: str,
    strategy: str,
    event_type: str | None,
    decision: str | None,
    reason: str | None,
    price,
    candle_open_time,
    info: Dict[str, Any] | None,
) -> None:
    """
    Shadow-only.
    Nie rzuca wyjątku do strategii.
    Nie wpływa na decyzję.
    """
    if not _should_trace(event_type, reason):
        return

    try:
        rt = compute_realtime_snapshot(
            symbol=str(symbol),
            interval=str(interval),
            candle_open_time=candle_open_time,
        )

        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO entry_trace_events (
                      symbol, interval, strategy,
                      candle_open_time, event_type, decision, reason, price,
                      realtime_score, realtime_status, primary_driver,
                      atr_pct, ema_slope_pct, volume_ratio,
                      momentum_3_pct, momentum_5_pct, range_pct,
                      breakout_up, breakout_down,
                      atr_component, volume_component, ema_component,
                      momentum_component, breakout_component,
                      realtime_weights_json,
                      input_info, realtime_json
                    )
                    VALUES (
                      %s,%s,%s,
                      %s,%s,%s,%s,%s,
                      %s,%s,%s,
                      %s,%s,%s,
                      %s,%s,%s,
                      %s,%s,
                      %s,%s,%s,
                      %s,%s,
                      %s::jsonb,
                      %s::jsonb,%s::jsonb
                    )
                    """,
                    (
                        str(symbol),
                        str(interval),
                        str(strategy),
                        candle_open_time,
                        event_type,
                        decision,
                        reason,
                        _safe_float(price),
                        _safe_float(rt.get("realtime_score")),
                        rt.get("realtime_status"),
                        rt.get("primary_driver"),
                        _safe_float(rt.get("atr_pct")),
                        _safe_float(rt.get("ema_slope_pct")),
                        _safe_float(rt.get("volume_ratio")),
                        _safe_float(rt.get("momentum_3_pct")),
                        _safe_float(rt.get("momentum_5_pct")),
                        _safe_float(rt.get("range_pct")),
                        bool(rt.get("breakout_up")) if rt.get("breakout_up") is not None else None,
                        bool(rt.get("breakout_down")) if rt.get("breakout_down") is not None else None,
                        _safe_float(rt.get("atr_component")),
                        _safe_float(rt.get("volume_component")),
                        _safe_float(rt.get("ema_component")),
                        _safe_float(rt.get("momentum_component")),
                        _safe_float(rt.get("breakout_component")),
                        json.dumps(sanitize_json(rt.get("weights") or {}), allow_nan=False),
                        json.dumps(sanitize_json(info or {}), allow_nan=False),
                        json.dumps(sanitize_json(rt or {}), allow_nan=False),
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    except Exception:
        logging.exception("entry_trace shadow insert failed")

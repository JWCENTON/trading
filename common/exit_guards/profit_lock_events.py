import json
import logging
from datetime import datetime
from typing import Any, Optional

from common.db import get_db_conn
from common.exit_guards.profit_lock import normalize_strategy_name


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def emit_profit_lock_event_once(
    *,
    symbol: str,
    interval: str,
    strategy: str,
    event_type: str,
    decision: Optional[str],
    reason: Optional[str],
    price: Optional[float],
    candle_open_time,
    position_entry_time,
    info: dict | None = None,
) -> bool:
    """
    Insert a Profit Lock telemetry event once per position/candle/reason.

    This keeps the strategy_events table useful for debugging without flooding it
    when a runner loops multiple times over the same candle.
    """
    payload = dict(info or {})
    if position_entry_time is not None:
        payload["position_entry_time"] = position_entry_time.isoformat() if hasattr(position_entry_time, "isoformat") else str(position_entry_time)

    position_entry_key = str(payload.get("position_entry_time", ""))
    strategy_u = normalize_strategy_name(strategy)

    conn = None
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM public.strategy_events
                WHERE symbol = %s
                  AND interval = %s
                  AND strategy = %s
                  AND event_type = %s
                  AND reason IS NOT DISTINCT FROM %s
                  AND candle_open_time IS NOT DISTINCT FROM %s
                  AND COALESCE(info->>'position_entry_time', '') = %s
                LIMIT 1;
                """,
                (symbol, interval, strategy_u, event_type, reason, candle_open_time, position_entry_key),
            )
            if cur.fetchone():
                conn.rollback()
                return False

            cur.execute(
                """
                INSERT INTO public.strategy_events
                (symbol, interval, strategy, event_type, decision, reason, price, candle_open_time, info)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    symbol,
                    interval,
                    strategy_u,
                    event_type,
                    decision,
                    reason,
                    float(price) if price is not None else None,
                    candle_open_time,
                    json.dumps(payload, default=_json_default),
                ),
            )
        conn.commit()
        return True
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        logging.exception("profit_lock telemetry insert failed")
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

import json
from datetime import datetime, date

def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)

def emit_alert_throttled(
    *,
    conn,
    symbol: str,
    interval: str,
    strategy: str,
    reason: str,
    open_time,
    price,
    info,
    throttle_minutes: int = 15,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM strategy_events
            WHERE symbol=%s AND interval=%s AND strategy=%s
              AND event_type='ALERT' AND reason=%s
              AND created_at >= now() - (%s || ' minutes')::interval
            LIMIT 1
            """,
            (symbol, interval, strategy, reason, throttle_minutes),
        )
        if cur.fetchone() is not None:
            return

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.strategy_events
            (symbol, interval, strategy, event_type, decision, reason, price, candle_open_time, info)
            VALUES
            (%s, %s, %s, 'ALERT', NULL, %s, %s, %s, %s);
            """,
            (
                symbol,
                interval,
                strategy,
                reason,
                float(price) if price is not None else None,
                open_time,
                json.dumps(info or {}, default=_json_default),
            ),
        )
    conn.commit()

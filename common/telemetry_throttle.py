# common/telemetry_throttle.py
import psycopg2

def should_emit_throttled_event(
    *,
    conn,
    symbol: str,
    interval: str,
    strategy: str,
    event_type: str,
    reason: str,
    throttle_seconds: int,
) -> bool:
    """
    True => emituj event teraz
    False => NIE emituj (bo był niedawno)
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM strategy_events
        WHERE symbol=%s AND interval=%s AND strategy=%s
          AND event_type=%s AND reason=%s
          AND created_at >= now() - make_interval(secs => %s)
        LIMIT 1;
        """,
        (symbol, interval, strategy, event_type, reason, int(throttle_seconds)),
    )
    exists = cur.fetchone() is not None
    cur.close()
    return not exists
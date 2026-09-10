"""Bounded, immutable LOCAL PAPER TREND input; no trading authority."""
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json

VERSION = "LOCAL_PAPER_TREND_FINAL_CANDLE_SET_V1"
CONTRACT = {"version": VERSION, "source": "candles", "finality": "close_time < evaluation_started_at",
            "identity": "same immutable rows in loop and evaluator", "indicators": "causal prefix only"}
FINGERPRINT = hashlib.sha256(json.dumps(CONTRACT, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class CandleSnapshot:
    evaluated_at: datetime
    rows: tuple

    @property
    def source_id(self):
        return self.rows[0][9] if self.rows else None

    @property
    def open_time(self):
        return self.rows[0][2] if self.rows else None


def load_snapshot(connection_factory, symbol, interval, evaluated_at, limit):
    """One SELECT supplies prices, persisted causal-prefix indicators and OHLC.

    EMA/RSI values at a row are computed by update_indicators using the prefix
    ending at that row, never a suffix. Fetch them atomically with that row.
    """
    conn = connection_factory()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT symbol,interval,open_time,close,ema_21,rsi_14,
                                  high,low,close_time,id
                           FROM candles WHERE symbol=%s AND interval=%s
                             AND close_time < %s
                           ORDER BY open_time DESC LIMIT %s""",
                        (symbol, interval, evaluated_at, limit))
            rows = tuple(tuple(r) for r in cur.fetchall())
    finally:
        conn.close()
    if any(r[8] >= evaluated_at or r[2] > r[8] for r in rows):
        raise ValueError("TREND_NON_FINAL_SOURCE")
    return CandleSnapshot(evaluated_at, rows)


def snapshot_atr(snapshot, period=14):
    rows = list(reversed(snapshot.rows[:period + 1]))
    if len(rows) < period + 1:
        return None
    tr = [max(float(b[6])-float(b[7]), abs(float(b[6])-float(a[3])),
              abs(float(b[7])-float(a[3]))) for a, b in zip(rows, rows[1:])]
    return sum(tr) / period

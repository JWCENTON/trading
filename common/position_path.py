from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from common.db import get_db_conn


@dataclass(frozen=True)
class PositionPathSnapshot:
    max_high: float
    min_low: float
    mfe_abs: float
    mae_abs: float
    current_close: float
    bars_seen: int
    last_open_time: Optional[datetime]


def load_position_path_snapshot(
    symbol: str,
    interval: str,
    entry_time: datetime,
    asof_open_time: datetime,
    entry_price: float,
) -> PositionPathSnapshot:
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                COALESCE(MAX(high), %s) AS max_high,
                COALESCE(MIN(low), %s) AS min_low,
                COALESCE(
                    (
                        SELECT c2.close
                        FROM candles c2
                        WHERE c2.symbol = %s
                          AND c2.interval = %s
                          AND c2.open_time >= %s
                          AND c2.open_time <= %s
                        ORDER BY c2.open_time DESC
                        LIMIT 1
                    ),
                    %s
                ) AS current_close,
                COUNT(*) AS bars_seen,
                MAX(open_time) AS last_open_time
            FROM candles c
            WHERE c.symbol = %s
              AND c.interval = %s
              AND c.open_time >= %s
              AND c.open_time <= %s
            """,
            (
                float(entry_price),
                float(entry_price),
                symbol,
                interval,
                entry_time,
                asof_open_time,
                float(entry_price),
                symbol,
                interval,
                entry_time,
                asof_open_time,
            ),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    max_high = float(row[0]) if row and row[0] is not None else float(entry_price)
    min_low = float(row[1]) if row and row[1] is not None else float(entry_price)
    current_close = float(row[2]) if row and row[2] is not None else float(entry_price)
    bars_seen = int(row[3]) if row and row[3] is not None else 0
    last_open_time = row[4] if row else None

    return PositionPathSnapshot(
        max_high=max_high,
        min_low=min_low,
        mfe_abs=max_high - float(entry_price),
        mae_abs=min_low - float(entry_price),
        current_close=current_close,
        bars_seen=bars_seen,
        last_open_time=last_open_time,
    )

# common/win_streak.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from common.db import get_db_conn


@dataclass(frozen=True)
class RecentWinStreak:
    """Recent CLOSED win-streak for one strategy/symbol/interval slot."""

    checked: int
    required: int
    streak: int
    eligible: bool
    source: str = "positions_closed_pnl_v1"
    error: Optional[str] = None


def get_recent_win_streak(
    *,
    strategy: str,
    symbol: str,
    interval: str,
    required_wins: int = 3,
) -> RecentWinStreak:
    """
    Count consecutive winning CLOSED positions for exactly one slot:
    strategy + symbol + interval.

    A win is defined as positions.pnl > 0. If fewer than required_wins
    CLOSED rows exist, eligibility is false. On DB/query errors, fail closed:
    return eligible=false and expose the error in telemetry.
    """
    required_wins = int(required_wins or 3)
    if required_wins <= 0:
        required_wins = 3

    conn = None
    cur = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT pnl
            FROM positions
            WHERE status = 'CLOSED'
              AND exit_time IS NOT NULL
              AND strategy = %s
              AND symbol = %s
              AND interval = %s
            ORDER BY exit_time DESC
            LIMIT %s;
            """,
            (strategy, symbol, interval, required_wins),
        )
        rows = cur.fetchall()
        checked = len(rows)

        streak = 0
        for (pnl,) in rows:
            if pnl is not None and Decimal(str(pnl)) > Decimal("0"):
                streak += 1
            else:
                break

        return RecentWinStreak(
            checked=checked,
            required=required_wins,
            streak=streak,
            eligible=(checked >= required_wins and streak >= required_wins),
        )
    except Exception as exc:
        return RecentWinStreak(
            checked=0,
            required=required_wins,
            streak=0,
            eligible=False,
            error=f"{type(exc).__name__}: {exc}",
        )
    finally:
        try:
            if cur is not None:
                cur.close()
        finally:
            if conn is not None:
                conn.close()

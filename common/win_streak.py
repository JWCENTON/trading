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
    source: str = "positions_closed_calc_pnl_v2"
    error: Optional[str] = None


def _calc_gross_pnl_usdc(*, side, qty, entry_price, exit_price) -> Decimal:
    """
    Calculate gross PnL from positions columns.

    LONG:  (exit_price - entry_price) * qty
    SHORT: (entry_price - exit_price) * qty

    This intentionally avoids positions.pnl because that column does not exist.
    Commission-aware net PnL can be added later using binance_order_fills,
    but win-streak boost must first be schema-safe and fail-closed.
    """
    side_u = str(side or "").upper()
    qty_d = Decimal(str(qty))
    entry_d = Decimal(str(entry_price))
    exit_d = Decimal(str(exit_price))

    if side_u == "LONG":
        return (exit_d - entry_d) * qty_d
    if side_u == "SHORT":
        return (entry_d - exit_d) * qty_d

    return Decimal("0")


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

    A win is defined as calculated gross PnL > 0 from positions:
      LONG  => (exit_price - entry_price) * qty
      SHORT => (entry_price - exit_price) * qty

    If fewer than required_wins CLOSED rows exist, eligibility is false.
    On DB/query/data errors, fail closed: eligible=false and expose error.
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
            SELECT id, side, qty, entry_price, exit_price
            FROM positions
            WHERE status = 'CLOSED'
              AND exit_time IS NOT NULL
              AND qty IS NOT NULL
              AND entry_price IS NOT NULL
              AND exit_price IS NOT NULL
              AND strategy = %s
              AND symbol = %s
              AND interval = %s
            ORDER BY exit_time DESC, id DESC
            LIMIT %s;
            """,
            (strategy, symbol, interval, required_wins),
        )
        rows = cur.fetchall()
        checked = len(rows)

        streak = 0
        for _pos_id, side, qty, entry_price, exit_price in rows:
            pnl = _calc_gross_pnl_usdc(
                side=side,
                qty=qty,
                entry_price=entry_price,
                exit_price=exit_price,
            )
            if pnl > Decimal("0"):
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

# common/win_streak.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Any

from common.db import get_db_conn


@dataclass(frozen=True)
class RecentWinStreak:
    """BOOST v1 decision object for one strategy/symbol/interval slot."""

    checked: int
    required: int
    streak: int
    eligible: bool
    source: str = "boost_v1_net_edge_positions_ssot"
    error: Optional[str] = None

    boost_candidate: bool = False
    boost_allowed: bool = False
    boost_block_reason: Optional[str] = None
    prev_net_1: Optional[float] = None
    prev_net_2: Optional[float] = None
    prev_net_3: Optional[float] = None
    last_exit_reason: Optional[str] = None
    last_boost_exit_reason: Optional[str] = None
    last_trade_gross_pct: Optional[float] = None
    rolling_5_gross_pct_avg: Optional[float] = None


def _d(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _f(v: Any) -> Optional[float]:
    d = _d(v)
    return float(d) if d is not None else None


def _ctx_get(ctx: Any, key: str) -> Any:
    if not isinstance(ctx, dict):
        return None
    return ctx.get(key)


def _is_boosted(ctx: Any) -> bool:
    """
    Detect historical boosted trades from entry_context_json.
    Fail conservative: no context => not treated as boosted.
    """
    addon = _d(_ctx_get(ctx, "applied_three_win_boost_usdc"))
    active = _ctx_get(ctx, "three_win_boost_active")
    return bool(active) or (addon is not None and addon > Decimal("0"))


def get_recent_win_streak(
    *,
    strategy: str,
    symbol: str,
    interval: str,
    required_wins: int = 3,
) -> RecentWinStreak:
    """
    BOOST v1:
    - no boost after 2+ net wins,
    - boost only after RSI_SOFT_EXIT,
    - cooldown: 3 closed trades after bad boost STOP_LOSS/TIME_EXIT,
    - last trade must have gross edge > 0.

    Keeps old function name/API so all bots get the same central logic.
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
            SELECT
                p.id,
                p.exit_reason,
                p.entry_context_json,
                COALESCE(v.pnl_net_real_usdc, p.net_pnl_usdc) AS net_pnl_usdc,
                COALESCE(v.pnl_gross_real_usdc, p.gross_pnl_usdc) AS gross_pnl_usdc,
                v.pnl_gross_real_pct AS gross_pct
            FROM positions p
            LEFT JOIN v_positions_pnl_net_real_ssot v ON v.id = p.id
            WHERE p.status = 'CLOSED'
              AND p.exit_time IS NOT NULL
              AND p.strategy = %s
              AND p.symbol = %s
              AND p.interval = %s
            ORDER BY p.exit_time DESC, p.id DESC
            LIMIT 8;
            """,
            (strategy, symbol, interval),
        )
        rows = cur.fetchall()
        checked = len(rows)

        if checked == 0:
            return RecentWinStreak(
                checked=0,
                required=required_wins,
                streak=0,
                eligible=False,
                boost_candidate=False,
                boost_allowed=False,
                boost_block_reason="INSUFFICIENT_HISTORY",
            )

        prev_net = [_d(r[3]) for r in rows[:3]]
        prev_gross = [_d(r[4]) for r in rows[:5]]
        prev_gross_pct = [_d(r[5]) for r in rows[:5]]

        streak = 0
        for n in prev_net:
            if n is not None and n > Decimal("0"):
                streak += 1
            else:
                break

        last_exit_reason = str(rows[0][1] or "")
        last_gross = prev_gross[0]
        last_gross_pct = prev_gross_pct[0]

        rolling_vals = [x for x in prev_gross_pct if x is not None]
        rolling_5_gross_pct_avg = (
            sum(rolling_vals, Decimal("0")) / Decimal(str(len(rolling_vals)))
            if rolling_vals else None
        )

        boost_candidate = True
        block_reason = None

        # Rule 1: hard block after 2+ net wins.
        if len(prev_net) >= 2 and prev_net[0] is not None and prev_net[1] is not None:
            if prev_net[0] > Decimal("0") and prev_net[1] > Decimal("0"):
                block_reason = "BLOCKED_AFTER_2_NET_WINS"

        # Rule 2: only after qualitative RSI exit.
        if block_reason is None and last_exit_reason != "RSI_SOFT_EXIT":
            block_reason = f"LAST_EXIT_NOT_RSI_SOFT_EXIT:{last_exit_reason or 'NULL'}"

        # Rule 3: cooldown after bad boost.
        last_boost_exit_reason = None
        if block_reason is None:
            for idx, row in enumerate(rows):
                ctx = row[2]
                if _is_boosted(ctx):
                    last_boost_exit_reason = str(row[1] or "")
                    if last_boost_exit_reason in ("STOP_LOSS", "TIME_EXIT") and idx < 3:
                        block_reason = f"BOOST_COOLDOWN_AFTER_{last_boost_exit_reason}"
                    break
        else:
            for row in rows:
                if _is_boosted(row[2]):
                    last_boost_exit_reason = str(row[1] or "")
                    break

        # Rule 4: last gross edge must be positive.
        if block_reason is None:
            if last_gross is None or last_gross <= Decimal("0"):
                block_reason = "LAST_GROSS_EDGE_NOT_POSITIVE"

        allowed = block_reason is None

        return RecentWinStreak(
            checked=checked,
            required=required_wins,
            streak=streak,
            eligible=allowed,
            boost_candidate=boost_candidate,
            boost_allowed=allowed,
            boost_block_reason=block_reason,
            prev_net_1=_f(prev_net[0]) if len(prev_net) > 0 else None,
            prev_net_2=_f(prev_net[1]) if len(prev_net) > 1 else None,
            prev_net_3=_f(prev_net[2]) if len(prev_net) > 2 else None,
            last_exit_reason=last_exit_reason or None,
            last_boost_exit_reason=last_boost_exit_reason,
            last_trade_gross_pct=_f(last_gross_pct),
            rolling_5_gross_pct_avg=_f(rolling_5_gross_pct_avg),
        )
    except Exception as exc:
        return RecentWinStreak(
            checked=0,
            required=required_wins,
            streak=0,
            eligible=False,
            boost_candidate=False,
            boost_allowed=False,
            boost_block_reason="ERROR_FAIL_CLOSED",
            error=f"{type(exc).__name__}: {exc}",
        )
    finally:
        try:
            if cur is not None:
                cur.close()
        finally:
            if conn is not None:
                conn.close()

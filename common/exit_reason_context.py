from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from common.position_path import load_position_path_snapshot


def _safe_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _age_minutes(entry_time, asof_time=None) -> Optional[float]:
    try:
        if entry_time is None:
            return None
        if getattr(entry_time, "tzinfo", None) is None:
            entry_time = entry_time.replace(tzinfo=timezone.utc)

        if asof_time is None:
            asof_time = datetime.now(timezone.utc)
        elif getattr(asof_time, "tzinfo", None) is None:
            asof_time = asof_time.replace(tzinfo=timezone.utc)

        return max(0.0, (asof_time - entry_time).total_seconds() / 60.0)
    except Exception:
        return None


def build_exit_reason_context(
    *,
    base_reason: str,
    strategy: str,
    symbol: str,
    interval: str,
    side: str | None,
    entry_price,
    exit_price,
    entry_time,
    asof_time=None,
    profit_lock_config=None,
) -> str:
    """
    Shadow/diagnostic-only formatter for positions.exit_reason.

    It does not change exit decisions. It only enriches the reason text with
    deterministic context available at close time.
    """
    reason = str(base_reason or "EXIT").strip()

    # Do not double-enrich reasons that already contain the standard context.
    if " peak=" in reason and " current=" in reason and " age=" in reason:
        return reason

    entry_f = _safe_float(entry_price)
    exit_f = _safe_float(exit_price)
    side_u = (side or "LONG").upper()

    if entry_f is None or exit_f is None or entry_f <= 0:
        return reason

    age = _age_minutes(entry_time, asof_time)

    peak_pct = None
    current_pct = None
    mae_pct = None
    bars_seen = None

    try:
        path = load_position_path_snapshot(
            symbol=symbol,
            interval=interval,
            entry_time=entry_time,
            asof_open_time=asof_time or datetime.now(timezone.utc),
            entry_price=entry_f,
        )

        bars_seen = int(path.bars_seen)

        if side_u == "LONG":
            peak_pct = ((float(path.max_high) - entry_f) / entry_f) * 100.0
            mae_pct = ((float(path.min_low) - entry_f) / entry_f) * 100.0
            current_pct = ((exit_f - entry_f) / entry_f) * 100.0
        else:
            peak_pct = ((entry_f - float(path.min_low)) / entry_f) * 100.0
            mae_pct = ((entry_f - float(path.max_high)) / entry_f) * 100.0
            current_pct = ((entry_f - exit_f) / entry_f) * 100.0
    except Exception:
        if side_u == "LONG":
            current_pct = ((exit_f - entry_f) / entry_f) * 100.0
        else:
            current_pct = ((entry_f - exit_f) / entry_f) * 100.0

    giveback_pct = None
    if peak_pct is not None and current_pct is not None:
        giveback_pct = peak_pct - current_pct

    strategy_u = str(strategy or "").upper()
    reason_text = str(reason or "").strip()

    if strategy_u and reason_text.upper().startswith(strategy_u + " "):
        prefix_parts = [reason_text]
    else:
        prefix_parts = [strategy_u, reason_text]

    parts = [
        *prefix_parts,
        str(side_u),
        f"entry={entry_f:.8f}",
        f"exit={exit_f:.8f}",
    ]

    if peak_pct is not None:
        parts.append(f"peak={peak_pct:.3f}%")

    if current_pct is not None:
        parts.append(f"current={current_pct:.3f}%")

    if peak_pct is not None:
        parts.append(f"mfe={peak_pct:.3f}%")

    if mae_pct is not None:
        parts.append(f"mae={mae_pct:.3f}%")

    if giveback_pct is not None:
        parts.append(f"giveback={giveback_pct:.3f}%")

    if profit_lock_config is not None:
        try:
            parts.append(f"floor={float(profit_lock_config.floor_pct):.3f}%")
            parts.append(f"trail_drop={float(profit_lock_config.trail_drop_pct):.3f}%")
        except Exception:
            pass

    if age is not None:
        parts.append(f"age={age:.1f}m")

    if bars_seen is not None:
        parts.append(f"bars={bars_seen}")

    return " ".join(p for p in parts if p)

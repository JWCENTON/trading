from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping


@dataclass(frozen=True)
class PaperAccountBridge:
    account_value: Decimal | None
    account_value_status: str
    realized_coverage_count: int
    closed_positions_count: int
    realized_coverage_pct: Decimal
    realized_source_breakdown: Mapping[str, int]
    realized_net_pnl: Decimal | None
    unrealized_pnl: Decimal
    calculation_method: str


def reconstruct_paper_account(
    *,
    initial_equity: Decimal,
    realized_net_pnl: Decimal | None,
    unrealized_pnl: Decimal,
    resolved_count: int,
    closed_count: int,
    source_breakdown: Mapping[str, int],
    external_adjustments: Decimal | None = None,
) -> PaperAccountBridge:
    if min(resolved_count, closed_count) < 0 or resolved_count > closed_count:
        raise ValueError("invalid realized coverage")
    coverage = (
        Decimal(resolved_count) / Decimal(closed_count) * Decimal("100")
        if closed_count else Decimal("100")
    )
    complete = resolved_count == closed_count
    adjustments_known = external_adjustments is not None
    if realized_net_pnl is None and resolved_count:
        status = "UNAVAILABLE"
        value = None
    else:
        status = (
            "RECONSTRUCTED_COMPLETE"
            if complete and adjustments_known else "RECONSTRUCTED_PARTIAL"
        )
        value = (
            initial_equity
            + (realized_net_pnl if realized_net_pnl is not None else Decimal("0"))
            + unrealized_pnl
            + (external_adjustments or Decimal("0"))
        )
    return PaperAccountBridge(
        value, status, resolved_count, closed_count, coverage,
        dict(source_breakdown), realized_net_pnl, unrealized_pnl,
        "INITIAL_PLUS_RESOLVED_REALIZED_PLUS_UNREALIZED_PLUS_ADJUSTMENTS",
    )

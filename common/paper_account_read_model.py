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
    resolved_outcome_count: int
    unresolved_outcome_count: int
    resolved_coverage_pct: Decimal
    high_assurance_count: int
    high_assurance_coverage_pct: Decimal
    legacy_compatible_count: int
    legacy_compatible_coverage_pct: Decimal
    quality_breakdown: Mapping[str, int]


def reconstruct_paper_account(
    *,
    initial_equity: Decimal,
    realized_net_pnl: Decimal | None,
    unrealized_pnl: Decimal,
    resolved_count: int,
    closed_count: int,
    source_breakdown: Mapping[str, int],
    high_assurance_count: int | None = None,
    legacy_compatible_count: int | None = None,
    quality_breakdown: Mapping[str, int] | None = None,
    external_adjustments: Decimal | None = None,
) -> PaperAccountBridge:
    if min(resolved_count, closed_count) < 0 or resolved_count > closed_count:
        raise ValueError("invalid realized coverage")
    high_assurance = (
        high_assurance_count if high_assurance_count is not None else
        sum(source_breakdown.get(source, 0) for source in
            ("FINANCIAL_TRUTH", "PAPER_SIMULATED_FILLS"))
    )
    legacy_compatible = (
        legacy_compatible_count if legacy_compatible_count is not None else
        source_breakdown.get("VERIFIED_LEGACY_STORED", 0)
    )
    if min(high_assurance, legacy_compatible) < 0:
        raise ValueError("invalid outcome quality counts")
    if high_assurance + legacy_compatible > resolved_count:
        raise ValueError("quality counts exceed resolved coverage")
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
        if complete and adjustments_known:
            status = (
                "RECONSTRUCTED_COMPLETE_HIGH_ASSURANCE"
                if high_assurance == closed_count
                else "RECONSTRUCTED_COMPLETE_MIXED"
            )
        else:
            status = (
                "RECONSTRUCTED_PARTIAL_MIXED"
                if legacy_compatible else "RECONSTRUCTED_PARTIAL_HIGH_ASSURANCE"
            )
        value = (
            initial_equity
            + (realized_net_pnl if realized_net_pnl is not None else Decimal("0"))
            + unrealized_pnl
            + (external_adjustments or Decimal("0"))
        )
    quality = dict(quality_breakdown or {
        "HIGH_ASSURANCE": high_assurance,
        "LEGACY_COMPATIBLE": legacy_compatible,
        "UNRESOLVED": closed_count - resolved_count,
    })
    high_pct = Decimal(high_assurance) / Decimal(closed_count) * Decimal("100") if closed_count else Decimal("100")
    legacy_pct = Decimal(legacy_compatible) / Decimal(closed_count) * Decimal("100") if closed_count else Decimal("0")
    return PaperAccountBridge(
        value, status, resolved_count, closed_count, coverage,
        dict(source_breakdown), realized_net_pnl, unrealized_pnl,
        "INITIAL_PLUS_RESOLVED_REALIZED_PLUS_UNREALIZED_PLUS_ADJUSTMENTS",
        resolved_count, closed_count - resolved_count, coverage,
        high_assurance, high_pct, legacy_compatible, legacy_pct, quality,
    )

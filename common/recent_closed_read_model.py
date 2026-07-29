from __future__ import annotations

from decimal import Decimal
from typing import Any


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    parsed = Decimal(str(value))
    return parsed if parsed != 0 else None


def resolve_entry_notional_usdc(
    *,
    real_execution_notional: Any = None,
    simulated_execution_notional: Any = None,
    estimated_notional: Any = None,
    legacy_price_qty_notional: Any = None,
) -> Decimal | None:
    """Resolve the gross executed entry-notional denominator.

    Net PnL already includes the applicable fee model, so entry fees are not
    subtracted again from this gross execution-notional value.
    """
    for candidate in (
        real_execution_notional,
        simulated_execution_notional,
        estimated_notional,
        legacy_price_qty_notional,
    ):
        resolved = _decimal_or_none(candidate)
        if resolved is not None:
            return resolved
    return None


def calculate_net_pnl_pct(
    net_pnl_usdc: Any,
    entry_notional_usdc: Any,
) -> Decimal | None:
    denominator = _decimal_or_none(entry_notional_usdc)
    if net_pnl_usdc is None or denominator is None:
        return None
    return Decimal(str(net_pnl_usdc)) / denominator * Decimal("100")

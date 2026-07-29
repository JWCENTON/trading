from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from enum import Enum
from typing import Iterable, Mapping


ZERO = Decimal("0")


class InventoryEvidenceStatus(str, Enum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"


class ExitInventoryStatus(str, Enum):
    FULLY_EXECUTED_CLOSE = "FULLY_EXECUTED_CLOSE"
    TERMINAL_DUST_CLOSE = "TERMINAL_DUST_CLOSE"
    PARTIAL_REDUCTION = "PARTIAL_REDUCTION"
    NO_NEW_FILL = "NO_NEW_FILL"
    INCOMPLETE_EVIDENCE = "INCOMPLETE_EVIDENCE"


def decimal(value: object) -> Decimal:
    return Decimal(str(value or 0))


def base_asset(symbol: str, quote_asset: str = "USDC") -> str:
    value = str(symbol or "").upper()
    quote = str(quote_asset or "").upper()
    return value[: -len(quote)] if quote and value.endswith(quote) else value


@dataclass(frozen=True)
class InventoryQuantity:
    gross_entry_executed_qty: Decimal
    entry_base_fee_qty: Decimal
    net_entry_inventory_qty: Decimal
    cumulative_exit_executed_qty: Decimal
    exit_inventory_reduction_qty: Decimal
    remaining_inventory_qty: Decimal
    evidence_status: InventoryEvidenceStatus
    incomplete_reasons: tuple[str, ...] = ()

    @classmethod
    def from_fills(
        cls,
        *,
        symbol: str,
        entry_fills: Iterable[Mapping[str, object]],
        exit_fills: Iterable[Mapping[str, object]] = (),
        quote_asset: str = "USDC",
    ) -> "InventoryQuantity":
        entries = tuple(entry_fills)
        exits = tuple(exit_fills)
        base = base_asset(symbol, quote_asset)
        reasons: list[str] = []

        if not entries:
            reasons.append("MISSING_ENTRY_FILLS")

        gross_entry = sum((decimal(row.get("executed_qty")) for row in entries), ZERO)
        gross_exit = sum((decimal(row.get("executed_qty")) for row in exits), ZERO)
        entry_base_fee = ZERO
        exit_base_fee = ZERO

        for purpose, rows in (("ENTRY", entries), ("EXIT", exits)):
            for row in rows:
                fee_asset = str(row.get("commission_asset") or "").upper()
                fee_value = row.get("commission_amount")
                if not fee_asset or fee_value in (None, ""):
                    reasons.append(f"MISSING_{purpose}_FEE_EVIDENCE")
                    continue
                if fee_asset == base:
                    if purpose == "ENTRY":
                        entry_base_fee += decimal(fee_value)
                    else:
                        exit_base_fee += decimal(fee_value)

        net_entry = gross_entry - entry_base_fee
        exit_reduction = gross_exit + exit_base_fee
        remaining = net_entry - exit_reduction
        if min(gross_entry, net_entry, remaining) < ZERO:
            reasons.append("INVENTORY_QUANTITY_CONFLICT")

        reasons = list(dict.fromkeys(reasons))
        return cls(
            gross_entry,
            entry_base_fee,
            net_entry,
            gross_exit,
            exit_reduction,
            remaining,
            (
                InventoryEvidenceStatus.COMPLETE
                if not reasons
                else InventoryEvidenceStatus.INCOMPLETE
            ),
            tuple(reasons),
        )


def project_inventory_from_execution_evidence(
    *,
    symbol: str,
    entry_fills: Iterable[Mapping[str, object]],
    exit_fills: Iterable[Mapping[str, object]] = (),
    quote_asset: str = "USDC",
) -> InventoryQuantity:
    """Mode-neutral canonical projector for LIVE and PAPER fill adapters."""
    return InventoryQuantity.from_fills(
        symbol=symbol,
        entry_fills=entry_fills,
        exit_fills=exit_fills,
        quote_asset=quote_asset,
    )


@dataclass(frozen=True)
class InstrumentExecutionLimits:
    lot_size: Decimal | None
    min_size: Decimal | None
    min_notional: Decimal | None
    price: Decimal | None
    price_fresh: bool


@dataclass(frozen=True)
class ExitInventoryClassification:
    status: ExitInventoryStatus
    remaining_inventory_qty: Decimal
    executable_inventory_qty: Decimal
    dust_qty: Decimal
    terminal_reason: str | None


def floor_to_lot(quantity: Decimal, lot_size: Decimal | None) -> Decimal:
    if lot_size is None or lot_size <= ZERO:
        return quantity
    return (quantity / lot_size).to_integral_value(rounding=ROUND_DOWN) * lot_size


def classify_exit_inventory(
    *,
    previous_remaining_qty: object,
    cumulative_exit_inventory_reduction_qty: object,
    previous_cumulative_exit_inventory_reduction_qty: object,
    inventory: InventoryQuantity,
    limits: InstrumentExecutionLimits,
    tolerance: object = "0.000000000001",
) -> ExitInventoryClassification:
    previous = decimal(previous_remaining_qty)
    cumulative = decimal(cumulative_exit_inventory_reduction_qty)
    previous_cumulative = decimal(previous_cumulative_exit_inventory_reduction_qty)
    numeric_tolerance = max(decimal(tolerance), ZERO)

    if inventory.evidence_status is not InventoryEvidenceStatus.COMPLETE:
        return ExitInventoryClassification(
            ExitInventoryStatus.INCOMPLETE_EVIDENCE,
            max(inventory.remaining_inventory_qty, ZERO),
            ZERO,
            ZERO,
            "INCOMPLETE_INVENTORY_EVIDENCE",
        )
    if cumulative <= previous_cumulative:
        return ExitInventoryClassification(
            ExitInventoryStatus.NO_NEW_FILL, previous,
            floor_to_lot(previous, limits.lot_size), ZERO, None,
        )

    remaining = max(inventory.remaining_inventory_qty, ZERO)
    if remaining <= numeric_tolerance:
        return ExitInventoryClassification(
            ExitInventoryStatus.FULLY_EXECUTED_CLOSE,
            remaining, ZERO, remaining, None,
        )

    executable = floor_to_lot(remaining, limits.lot_size)
    below_lot = limits.lot_size is not None and executable < limits.lot_size
    below_size = limits.min_size is not None and executable < limits.min_size
    notional_known = (
        limits.price is not None
        and limits.price_fresh
        and limits.min_notional is not None
    )
    below_notional = bool(
        notional_known
        and executable * limits.price < limits.min_notional
    )
    requires_price = bool(limits.min_notional and limits.min_notional > ZERO)
    if requires_price and not notional_known:
        return ExitInventoryClassification(
            ExitInventoryStatus.INCOMPLETE_EVIDENCE,
            remaining, executable, ZERO, "PRICE_EVIDENCE_INCOMPLETE",
        )
    if below_lot or below_size or below_notional:
        reason = (
            "BELOW_LOT_SIZE" if below_lot
            else "BELOW_MIN_SIZE" if below_size
            else "BELOW_MIN_NOTIONAL"
        )
        return ExitInventoryClassification(
            ExitInventoryStatus.TERMINAL_DUST_CLOSE,
            remaining, ZERO, remaining, reason,
        )
    return ExitInventoryClassification(
        ExitInventoryStatus.PARTIAL_REDUCTION,
        remaining, executable, ZERO, None,
    )

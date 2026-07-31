from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Iterable


NORMALIZATION_VERSION = "PAPER_OUTCOME_NORMALIZATION_V1"
MONEY_DISPLAY_SCALE = Decimal("0.00000001")
FEE_SERIALIZATION_SCALE = Decimal("0.000000000001")
PERCENTAGE_SERIALIZATION_SCALE = Decimal("0.0000000001")
ROUNDING_MODE = ROUND_HALF_EVEN


@dataclass(frozen=True)
class OutcomeNormalization:
    status: str
    stored_value: Decimal | None
    resolved_value: Decimal | None
    delta: Decimal | None
    normalization_version: str = NORMALIZATION_VERSION


def _stored_rounding_unit(value: Decimal) -> Decimal:
    scale = max(0, -value.as_tuple().exponent)
    return Decimal(1).scaleb(-min(scale, 18))


def compare_outcome_values(
    stored_value: Decimal | None, resolved_value: Decimal | None,
) -> OutcomeNormalization:
    if stored_value is None or resolved_value is None:
        return OutcomeNormalization(
            "SOURCE_NOT_COMPARABLE", stored_value, resolved_value, None
        )
    delta = resolved_value - stored_value
    if delta == 0:
        status = "EXACT_MATCH"
    elif abs(delta) <= _stored_rounding_unit(stored_value) / Decimal(2):
        status = "ROUNDING_ONLY"
    else:
        status = "MATERIAL_CONFLICT"
    return OutcomeNormalization(status, stored_value, resolved_value, delta)


def aggregate_normalization_status(
    diagnostics: Iterable[OutcomeNormalization],
) -> str:
    statuses = {item.status for item in diagnostics}
    if "MATERIAL_CONFLICT" in statuses:
        return "MATERIAL_CONFLICT"
    if "ROUNDING_ONLY" in statuses:
        return "ROUNDING_ONLY"
    if "EXACT_MATCH" in statuses:
        return "EXACT_MATCH"
    return "SOURCE_NOT_COMPARABLE"


def serialize_money(value: Decimal) -> str:
    return format(value.quantize(MONEY_DISPLAY_SCALE, rounding=ROUNDING_MODE), "f")


def serialize_fee(value: Decimal) -> str:
    return format(value.quantize(FEE_SERIALIZATION_SCALE, rounding=ROUNDING_MODE), "f")


def serialize_percentage(value: Decimal) -> str:
    return format(
        value.quantize(PERCENTAGE_SERIALIZATION_SCALE, rounding=ROUNDING_MODE), "f"
    )

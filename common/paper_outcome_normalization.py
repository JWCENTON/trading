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
    gross_delta: Decimal | None = None
    fee_delta: Decimal | None = None
    net_delta: Decimal | None = None
    gross_rounding_bound: Decimal | None = None
    fee_rounding_bound: Decimal | None = None
    net_serialization_bound: Decimal | None = None
    maximum_explainable_net_delta: Decimal | None = None
    reconstructed_net_delta: Decimal | None = None
    normalization_version: str = NORMALIZATION_VERSION


def _stored_rounding_unit(value: Decimal) -> Decimal:
    scale = max(0, -value.as_tuple().exponent)
    return Decimal(1).scaleb(-scale)


def _half_unit(value: Decimal) -> Decimal:
    return _stored_rounding_unit(value) / Decimal(2)


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


def compare_outcome_components(
    *,
    stored_gross: Decimal | None,
    stored_fees: Decimal | None,
    stored_net: Decimal | None,
    resolved_gross: Decimal | None,
    resolved_fees: Decimal | None,
    resolved_net: Decimal | None,
) -> OutcomeNormalization:
    values = (
        stored_gross, stored_fees, stored_net,
        resolved_gross, resolved_fees, resolved_net,
    )
    if any(value is None for value in values):
        return OutcomeNormalization(
            "SOURCE_NOT_COMPARABLE", stored_net, resolved_net, None
        )

    assert stored_gross is not None and stored_fees is not None
    assert stored_net is not None and resolved_gross is not None
    assert resolved_fees is not None and resolved_net is not None
    # Deltas use stored-minus-resolved direction so net = gross - fees remains
    # directly auditable without sign reversal.
    gross_delta = stored_gross - resolved_gross
    fee_delta = stored_fees - resolved_fees
    net_delta = stored_net - resolved_net
    reconstructed = gross_delta - fee_delta
    gross_bound = _half_unit(stored_gross)
    fee_bound = _half_unit(stored_fees)
    net_bound = _half_unit(stored_net)
    maximum = gross_bound + fee_bound + net_bound
    formulas_hold = (
        stored_gross - stored_fees == stored_net
        and resolved_gross - resolved_fees == resolved_net
    )
    components_within_bounds = (
        abs(gross_delta) <= gross_bound and abs(fee_delta) <= fee_bound
    )
    direction_explained = abs(net_delta - reconstructed) <= net_bound

    if not formulas_hold or not components_within_bounds or not direction_explained:
        status = "MATERIAL_CONFLICT"
    elif gross_delta == 0 and fee_delta == 0 and net_delta == 0:
        status = "EXACT_MATCH"
    elif abs(net_delta) <= net_bound:
        status = "ROUNDING_ONLY"
    elif abs(net_delta) <= maximum:
        status = "COMPONENT_ROUNDING_ACCUMULATION"
    else:
        status = "MATERIAL_CONFLICT"
    return OutcomeNormalization(
        status=status,
        stored_value=stored_net,
        resolved_value=resolved_net,
        delta=resolved_net - stored_net,
        gross_delta=gross_delta,
        fee_delta=fee_delta,
        net_delta=net_delta,
        gross_rounding_bound=gross_bound,
        fee_rounding_bound=fee_bound,
        net_serialization_bound=net_bound,
        maximum_explainable_net_delta=maximum,
        reconstructed_net_delta=reconstructed,
    )


def aggregate_normalization_status(
    diagnostics: Iterable[OutcomeNormalization],
) -> str:
    statuses = {item.status for item in diagnostics}
    if "MATERIAL_CONFLICT" in statuses:
        return "MATERIAL_CONFLICT"
    if "COMPONENT_ROUNDING_ACCUMULATION" in statuses:
        return "NON_MATERIAL_NORMALIZATION"
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

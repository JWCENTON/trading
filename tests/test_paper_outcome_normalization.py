from decimal import Decimal

from common.paper_outcome_normalization import (
    NORMALIZATION_VERSION,
    aggregate_normalization_status,
    compare_outcome_components,
    compare_outcome_values,
    serialize_fee,
    serialize_money,
    serialize_percentage,
)


def test_versioned_decimal_serialization_contract():
    assert NORMALIZATION_VERSION == "PAPER_OUTCOME_NORMALIZATION_V1"
    assert serialize_money(Decimal("1.234567885")) == "1.23456788"
    assert serialize_fee(Decimal("0.1234567890125")) == "0.123456789012"
    assert serialize_percentage(Decimal("1.23456789015")) == "1.2345678902"


def test_rounding_only_is_derived_from_stored_scale():
    diagnostic = compare_outcome_values(
        Decimal("0.04000000"), Decimal("0.0399999996")
    )
    assert diagnostic.status == "ROUNDING_ONLY"
    assert diagnostic.delta == Decimal("-0.0000000004")


def test_material_conflict_and_aggregate_precedence():
    exact = compare_outcome_values(Decimal("1.00"), Decimal("1.00"))
    rounding = compare_outcome_values(Decimal("1.00000000"), Decimal("1.000000004"))
    conflict = compare_outcome_values(Decimal("1.00"), Decimal("1.02"))
    assert conflict.status == "MATERIAL_CONFLICT"
    assert aggregate_normalization_status([exact, rounding]) == "ROUNDING_ONLY"
    assert aggregate_normalization_status([exact, conflict]) == "MATERIAL_CONFLICT"


def test_missing_source_is_not_comparable():
    assert compare_outcome_values(None, Decimal("0")).status == "SOURCE_NOT_COMPARABLE"


def _components(**overrides):
    values = {
        "stored_gross": Decimal("1.00000000"),
        "stored_fees": Decimal("0.10000000"),
        "stored_net": Decimal("0.90000000"),
        "resolved_gross": Decimal("0.999999996"),
        "resolved_fees": Decimal("0.100000004"),
        "resolved_net": Decimal("0.899999992"),
    }
    values.update(overrides)
    return compare_outcome_components(**values)


def test_component_rounding_accumulation_is_directionally_explained():
    diagnostic = _components()
    assert diagnostic.status == "COMPONENT_ROUNDING_ACCUMULATION"
    assert diagnostic.gross_delta == Decimal("0.000000004")
    assert diagnostic.fee_delta == Decimal("-0.000000004")
    assert diagnostic.net_delta == Decimal("0.000000008")
    assert diagnostic.reconstructed_net_delta == diagnostic.net_delta
    assert diagnostic.maximum_explainable_net_delta == Decimal("0.000000015")
    assert aggregate_normalization_status([diagnostic]) == "NON_MATERIAL_NORMALIZATION"


def test_component_rounding_preserves_material_conflict_boundaries():
    assert _components(resolved_gross=Decimal("0.999999994"), resolved_net=Decimal("0.899999990")).status == "MATERIAL_CONFLICT"
    assert _components(resolved_fees=Decimal("0.100000006"), resolved_net=Decimal("0.899999990")).status == "MATERIAL_CONFLICT"
    assert _components(resolved_net=Decimal("0.899999990")).status == "MATERIAL_CONFLICT"
    assert _components(stored_net=Decimal("0.89999999")).status == "MATERIAL_CONFLICT"
    assert _components(resolved_net=Decimal("0.899999993")).status == "MATERIAL_CONFLICT"

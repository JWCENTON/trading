from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from common.pre_entry_risk import (
    calculate_pre_entry_risk,
    fingerprint,
    quantity_fingerprint,
    reference_price_fingerprint,
)


NOW = datetime(2026, 8, 22, 12, tzinfo=timezone.utc)


def calculate(**changes):
    values = dict(
        reference_entry_price=Decimal("100"),
        boundary_distance_pct=Decimal("0.8"),
        proposed_inventory_qty=Decimal("2"),
        canonical_exit_fee_rate=Decimal("0.0035"),
        reference_price_timestamp=NOW,
        effective_at=NOW + timedelta(minutes=1),
    )
    values.update(changes)
    return calculate_pre_entry_risk(**values)


@pytest.mark.parametrize("strategy", ["RSI", "TREND", "SUPERTREND", "BBRANGE"])
def test_all_four_strategies_share_exact_frozen_risk_semantics(strategy):
    result = calculate()
    assert strategy in {"RSI", "TREND", "SUPERTREND", "BBRANGE"}
    assert result.proposed_boundary_price == Decimal("99.2")
    assert result.pre_entry_core_price_risk == Decimal("1.6")
    assert result.pre_entry_exit_fee_estimate == Decimal("0.69440")
    assert result.total_pre_entry_risk == Decimal("2.29440")
    assert result.evidence_status == "CANONICAL"


@pytest.mark.parametrize(
    ("changes", "status"),
    [
        ({"boundary_policy_status": "MISSING"}, "MISSING_BOUNDARY_POLICY"),
        ({"reference_entry_price": None}, "MISSING_REFERENCE_PRICE"),
        ({"reference_price_timestamp": NOW - timedelta(hours=1)}, "STALE_REFERENCE_PRICE"),
        ({"proposed_inventory_qty": None}, "MISSING_PROPOSED_QUANTITY"),
        ({"proposed_inventory_qty": Decimal("0")}, "INVALID_QUANTITY"),
        ({"proposed_inventory_qty": Decimal("-1")}, "INVALID_QUANTITY"),
        ({"canonical_exit_fee_rate": None}, "MISSING_EXIT_COST_AUTHORITY"),
        ({"exit_cost_status": "STALE_EXIT_COST_AUTHORITY"}, "STALE_EXIT_COST_AUTHORITY"),
        ({"boundary_distance_pct": Decimal("100")}, "INVALID_BOUNDARY"),
        ({"identity_status": "ACCOUNT_IDENTITY_MISMATCH"}, "ACCOUNT_IDENTITY_MISMATCH"),
    ],
)
def test_incomplete_evidence_is_typed_and_never_fabricates_zero(changes, status):
    result = calculate(**changes)
    assert result.evidence_status == status
    assert result.pre_entry_core_price_risk is None
    assert result.pre_entry_exit_fee_estimate is None
    assert result.total_pre_entry_risk is None


def test_decimal_and_fingerprint_contract_rejects_float_inputs():
    floated = calculate_pre_entry_risk(
        reference_entry_price=100.0,
        boundary_distance_pct=Decimal("0.8"),
        proposed_inventory_qty=Decimal("1"),
        canonical_exit_fee_rate=Decimal("0.0035"),
        reference_price_timestamp=NOW,
        effective_at=NOW,
    )
    assert floated.evidence_status == "EVIDENCE_INCOMPLETE"
    assert floated.total_pre_entry_risk is None
    assert quantity_fingerprint(source="x", quantity="1.00") == quantity_fingerprint(
        source="x", quantity=Decimal("1.00")
    )
    assert len(reference_price_fingerprint(
        symbol="BTCUSDC", interval="1m", candle_open_time=NOW,
        reference_price=Decimal("100.123456789123456789"),
    )) == 64
    with pytest.raises(ValueError, match="FLOAT_FORBIDDEN"):
        fingerprint({"forbidden": 0.1})


def test_fee_authority_changes_risk_without_hidden_constant():
    paper = calculate(canonical_exit_fee_rate=Decimal("0.0035"))
    live = calculate(canonical_exit_fee_rate=Decimal("0.0010"))
    assert paper.pre_entry_core_price_risk == live.pre_entry_core_price_risk
    assert paper.pre_entry_exit_fee_estimate == Decimal("0.69440")
    assert live.pre_entry_exit_fee_estimate == Decimal("0.19840")


@pytest.mark.parametrize(
    ("strategy", "reference", "quantity", "distance", "expected"),
    [
        ("BBRANGE", "93.36", "0.214", "0.8", "0.22919954688"),
        ("RSI", "77156.14", "0.00026", "0.45", "0.1601688168067"),
        ("TREND", "100.16", "0.199", "0.8", "0.22865806848"),
    ],
)
def test_approved_formula_vectors_are_exact(strategy, reference, quantity, distance, expected):
    result = calculate(
        reference_entry_price=Decimal(reference),
        proposed_inventory_qty=Decimal(quantity),
        boundary_distance_pct=Decimal(distance),
    )
    assert strategy in {"RSI", "TREND", "BBRANGE"}
    assert result.total_pre_entry_risk == Decimal(expected)


def test_deterministic_identity_is_independent_per_commitment():
    from common.pre_entry_risk import deterministic_pre_entry_risk_id

    reservations = [uuid for uuid in (
        "11111111-1111-4111-8111-111111111111",
        "22222222-2222-4222-8222-222222222222",
        "33333333-3333-4333-8333-333333333333",
    )]
    identities = [deterministic_pre_entry_risk_id(value) for value in reservations]
    assert len(set(identities)) == 3
    assert identities == [deterministic_pre_entry_risk_id(value) for value in reservations]

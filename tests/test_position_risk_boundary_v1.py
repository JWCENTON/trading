from datetime import datetime, timedelta, timezone
from decimal import Decimal
import uuid

import pytest

from common.position_risk_boundary import (
    BOUNDARY_TYPE,
    EXECUTION_PRICE_GUARANTEE,
    PositionRiskEvidence,
    RiskBoundaryProjection,
    boundary_price_from_basis,
    evaluate_position_risk,
    policy_fingerprint,
)
from common.portfolio_state import (
    OpenInventoryMark, PortfolioBaseline, RealizedEvidence,
    build_portfolio_state,
)


NOW = datetime(2026, 8, 21, 12, tzinfo=timezone.utc)


def projection(**changes):
    values = dict(
        boundary_id=uuid.UUID("11111111-1111-4111-8111-111111111111"),
        position_id=1, environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint="a" * 64, side="LONG",
        state="BOUNDARY_ACTIVATED", boundary_distance_pct=Decimal("0.8"),
        entry_basis_price=Decimal("100"),
        entry_basis_authority="CANONICAL_WEIGHTED_ENTRY_FILL_EVIDENCE",
        boundary_price=Decimal("99.2"), boundary_type=BOUNDARY_TYPE,
        execution_price_guarantee=EXECUTION_PRICE_GUARANTEE,
        policy_fingerprint="b" * 64, effective_at=NOW,
        exit_fee_rate=Decimal("0.0035"), exit_fee_model="PAPER_V2",
    )
    values.update(changes)
    return RiskBoundaryProjection(**values)


def evaluate(**changes):
    values = dict(
        position_id=1, side="LONG",
        remaining_inventory_qty=Decimal("2"),
        mark_price=Decimal("101"),
        mark_status="CANONICAL", projection=projection(),
    )
    values.update(changes)
    return evaluate_position_risk(**values)


def mark(position_id=1):
    return OpenInventoryMark(
        position_id, "BTCUSDC", "RSI", "1m", "LONG", Decimal("100"),
        Decimal("2"), "COMPLETE", Decimal("101"), NOW, "TREND", NOW,
    )


def test_policy_fingerprint_and_boundary_are_decimal_deterministic():
    first = policy_fingerprint(
        strategy="RSI", interval="1m", boundary_distance_pct=Decimal("0.8"),
    )
    assert first == policy_fingerprint(
        strategy="RSI", interval="1m", boundary_distance_pct="0.8",
    )
    assert boundary_price_from_basis(
        Decimal("123.456789123456789"), Decimal("0.8"),
    ) == Decimal("122.469134810469134688")
    with pytest.raises(ValueError, match="INVALID_DECIMAL"):
        policy_fingerprint(
            strategy="RSI", interval="1m", boundary_distance_pct=0.8,
        )


def test_current_mark_to_boundary_risk_and_fee_are_exact():
    result = evaluate()
    assert result.core_price_risk == Decimal("3.6")
    assert result.exit_fee_estimate == Decimal("0.69440")
    assert result.open_risk_to_trigger == Decimal("4.29440")
    assert result.status == "CANONICAL"
    assert result.execution_price_guarantee == "NO"
    assert result.tail_risk_status == "UNBOUNDED_BY_TRIGGER"


def test_partial_exit_uses_remaining_quantity_without_boundary_revision():
    full = evaluate(remaining_inventory_qty=Decimal("2"))
    partial = evaluate(remaining_inventory_qty=Decimal("0.75"))
    assert partial.boundary_price == full.boundary_price == Decimal("99.2")
    assert partial.open_risk_to_trigger == full.open_risk_to_trigger * Decimal("0.375")


@pytest.mark.parametrize("mark_price", [Decimal("99.2"), Decimal("98")])
def test_boundary_breach_is_null_not_zero(mark_price):
    result = evaluate(mark_price=mark_price)
    assert result.open_risk_to_trigger is None
    assert result.status == "BOUNDARY_BREACHED_UNRESOLVED"


@pytest.mark.parametrize(
    ("changes", "status"),
    [
        ({"projection": None}, "MISSING_BOUNDARY"),
        ({"mark_price": None, "mark_status": "PRICE_UNAVAILABLE"}, "MISSING_MARK"),
        ({"mark_status": "PRICE_STALE"}, "STALE_MARK"),
        ({"projection": projection(entry_basis_price=None)}, "MISSING_ENTRY_BASIS"),
        ({"projection": projection(boundary_price=Decimal("101"))}, "BOUNDARY_INVALID"),
        ({"side": "SHORT"}, "BOUNDARY_INVALID"),
        ({"projection": projection(exit_fee_rate=None)}, "MISSING_COST_AUTHORITY"),
    ],
)
def test_typed_fail_closed_statuses(changes, status):
    result = evaluate(**changes)
    assert result.status == status
    assert result.open_risk_to_trigger is None


def test_empty_and_mixed_portfolio_aggregation():
    common = dict(
        environment="PAPER", deployment_id="local-paper", as_of=NOW,
        baseline=PortfolioBaseline(
            NOW - timedelta(days=1), Decimal("1000"), Decimal("0"),
            "TEST", "a" * 64,
        ), realized=RealizedEvidence(0, 0, None, None),
        historical_peak_managed_equity=Decimal("1000"),
    )
    empty = build_portfolio_state(open_marks=(), risk_boundaries={}, **common)
    assert empty.open_risk == Decimal("0")
    assert empty.open_risk_status == "CANONICAL_EMPTY"

    mixed = build_portfolio_state(
        open_marks=(mark(1), mark(2)), risk_boundaries={1: projection()}, **common,
    )
    assert mixed.open_risk is None
    assert mixed.open_risk_status == "INCOMPLETE"
    assert mixed.canonical_risk_position_count == 1
    assert mixed.material_risk_position_count == 2
    assert mixed.partial_risk_sum == Decimal("4.29440")
    assert mixed.total_capital == Decimal("1004")


def test_boundary_does_not_change_existing_portfolio_capital_semantics():
    baseline = PortfolioBaseline(NOW, Decimal("1000"), Decimal("0"), "TEST", "a" * 64)
    inputs = dict(
        environment="PAPER", deployment_id="local-paper", as_of=NOW,
        baseline=baseline, realized=RealizedEvidence(0, 0, None, None),
        open_marks=(mark(),), historical_peak_managed_equity=Decimal("1000"),
    )
    missing = build_portfolio_state(risk_boundaries={}, **inputs)
    canonical = build_portfolio_state(risk_boundaries={1: projection()}, **inputs)
    for field in (
        "total_capital", "available_capital", "reserved_capital",
        "deployed_capital", "realized_pnl", "unrealized_pnl", "drawdown",
        "open_exposure_notional",
    ):
        assert getattr(missing, field) == getattr(canonical, field)

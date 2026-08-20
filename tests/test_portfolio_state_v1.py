from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.portfolio_state import (
    OpenInventoryMark,
    PortfolioBaseline,
    RealizedEvidence,
    build_portfolio_state,
    validate_identity,
)
from scripts.portfolio_state_v1_fingerprint import contract_fingerprint


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 8, 20, 20, 0, tzinfo=timezone.utc)
ZERO = Decimal("0")
BASELINE = PortfolioBaseline(
    NOW - timedelta(days=9), Decimal("925.50"), Decimal("1.25"),
    "CANONICAL_PAPER_ACCOUNT_READ_MODEL_V1", "a" * 64,
)


def mark(**changes):
    values = {
        "position_id": 1, "symbol": "BTCUSDC", "strategy": "RSI",
        "interval": "1m", "side": "LONG", "entry_price": Decimal("100"),
        "remaining_inventory_qty": Decimal("0.2"),
        "inventory_evidence_status": "COMPLETE",
        "mark_price": Decimal("110"), "mark_timestamp": NOW,
        "regime": "TREND", "regime_timestamp": NOW,
    }
    values.update(changes)
    return OpenInventoryMark(**values)


def state(*, realized=Decimal("10"), closed=1, marks=None, baseline=BASELINE,
          peak=Decimal("940")):
    return build_portfolio_state(
        environment="PAPER", deployment_id="local-paper", as_of=NOW,
        baseline=baseline,
        realized=RealizedEvidence(closed, closed, realized, NOW),
        open_marks=(mark(),) if marks is None else marks,
        historical_peak_managed_equity=peak, runtime_revision="b" * 40,
    )


def test_pre_baseline_unresolved_history_is_not_an_input_to_managed_equity():
    first = state()
    second = state()
    assert first.total_capital == second.total_capital == Decimal("936.25")
    assert first.source_authorities["account_reporting_excluded"] == "RECONSTRUCTED_PARTIAL_MIXED"


def test_post_baseline_complete_ft_changes_managed_equity_exactly():
    before = state(realized=Decimal("10.000000000000000001"))
    after = state(realized=Decimal("10.000000000000000002"))
    assert after.total_capital - before.total_capital == Decimal("0.000000000000000001")
    assert after.realized_pnl_status == "CANONICAL"


@pytest.mark.parametrize(
    ("price", "timestamp", "expected"),
    [(None, None, "PRICE_UNAVAILABLE"),
     (Decimal("110"), NOW - timedelta(minutes=21), "PRICE_STALE")],
)
def test_missing_or_stale_mark_never_fabricates_zero(price, timestamp, expected):
    result = state(marks=(mark(mark_price=price, mark_timestamp=timestamp),))
    assert result.unrealized_pnl is None
    assert result.deployed_capital is None
    assert result.unrealized_pnl_status == expected
    assert result.total_capital is None


def test_remaining_inventory_quantity_is_the_only_quantity_authority():
    result = state(marks=(mark(remaining_inventory_qty=Decimal("0.123456789")),))
    assert result.deployed_capital == Decimal("13.580246790")
    assert result.exposure_by_symbol[0].quantity == Decimal("0.123456789")


def test_inventory_is_deployed_and_never_reserved_capital():
    result = state()
    assert result.deployed_capital == Decimal("22.0")
    assert result.reserved_capital is None
    assert result.reserved_capital_status == "NOT_YET_CANONICAL"


def test_available_is_not_inferred_from_baseline_or_inventory():
    result = state()
    assert result.available_capital is None
    assert result.available_capital_status == "INCOMPLETE"
    assert "AVAILABLE_BASELINE_DEPENDS_ON_FORBIDDEN_PRICE_FALLBACK" in result.incomplete_reasons


def test_incomplete_post_baseline_ft_is_null_not_partial_sum_or_zero():
    result = build_portfolio_state(
        environment="PAPER", deployment_id="local-paper", as_of=NOW,
        baseline=BASELINE,
        realized=RealizedEvidence(2, 1, Decimal("7"), NOW),
        open_marks=(mark(),), historical_peak_managed_equity=None,
    )
    assert result.realized_pnl is None
    assert result.realized_pnl_status == "INCOMPLETE"
    assert result.total_capital is None


def test_empty_state_has_exact_decimal_zero_economics():
    result = state(realized=None, closed=0, marks=(), peak=None)
    assert result.realized_pnl == ZERO
    assert result.unrealized_pnl == ZERO
    assert result.deployed_capital == ZERO
    assert result.total_capital == Decimal("924.25")


def test_exposure_dimensions_sum_once_and_unknown_regime_is_explicit():
    result = state(marks=(
        mark(position_id=1, remaining_inventory_qty=Decimal("0.1")),
        mark(position_id=2, strategy="TREND", regime=None,
             remaining_inventory_qty=Decimal("0.2")),
    ))
    assert sum(bucket.market_value_usdc for bucket in result.exposure_by_symbol) == result.open_exposure_notional
    assert sum(bucket.market_value_usdc for bucket in result.exposure_by_strategy) == result.open_exposure_notional
    assert sum(bucket.market_value_usdc for bucket in result.exposure_by_regime) == result.open_exposure_notional
    assert {bucket.key for bucket in result.exposure_by_regime} == {"TREND", "UNKNOWN"}
    assert all(bucket.quantity is None for bucket in result.exposure_by_strategy)
    assert all(bucket.quantity is None for bucket in result.exposure_by_regime)


def test_drawdown_uses_accepted_baseline_aligned_managed_equity():
    result = state(peak=Decimal("950"))
    assert result.drawdown == (result.total_capital - Decimal("950")) / Decimal("950") * Decimal("100")
    assert result.drawdown_status == "CANONICAL"


@pytest.mark.parametrize("deployment,mode", [
    ("local-paper", "PAPER"), ("local-live", "LIVE"),
    ("vps-paper", "PAPER"), ("vps-live", "LIVE"),
])
def test_environment_fencing_accepts_only_matching_pairs(deployment, mode):
    assert validate_identity(mode, deployment) == (mode, deployment)
    with pytest.raises(ValueError, match="ENVIRONMENT_DEPLOYMENT_MISMATCH"):
        validate_identity("LIVE" if mode == "PAPER" else "PAPER", deployment)


def test_same_inputs_are_deterministic_and_serialization_has_no_floats():
    first = state().serializable()
    second = state().serializable()
    assert first == second
    assert isinstance(first["total_capital"], str)
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)


def test_contract_manifest_and_fingerprint_are_stable():
    contract = json.loads((ROOT / "contracts/portfolio_state_v1_contract.json").read_text())
    assert contract["version"] == "PORTFOLIO_STATE_V1"
    assert contract["equations"]["reserved_is_not_inventory"] is True
    expected = (ROOT / "contracts/portfolio_state_v1_contract.sha256").read_text().strip()
    assert contract_fingerprint() == expected


def test_read_entrypoint_uses_database_enforced_read_only_transaction():
    source = (ROOT / "scripts/portfolio_state_v1_read.py").read_text()
    assert "with read_only_db_conn()" in source
    implementation = (ROOT / "common/portfolio_state.py").read_text().upper()
    for mutation in ("INSERT INTO", "UPDATE POSITIONS", "DELETE FROM", "CREATE ORDER"):
        assert mutation not in implementation

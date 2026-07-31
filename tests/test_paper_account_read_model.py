from decimal import Decimal

from common.paper_account_read_model import reconstruct_paper_account


def test_complete_account_bridge_reconciles():
    bridge = reconstruct_paper_account(
        initial_equity=Decimal("1000"), realized_net_pnl=Decimal("5"),
        unrealized_pnl=Decimal("2"), resolved_count=10, closed_count=10,
        source_breakdown={"PAPER_SIMULATED_FILLS": 10},
        external_adjustments=Decimal("1"),
    )
    assert bridge.account_value == Decimal("1008")
    assert bridge.account_value_status == "RECONSTRUCTED_COMPLETE"
    assert bridge.realized_coverage_pct == Decimal("100")


def test_local_like_partial_history_is_explicit():
    bridge = reconstruct_paper_account(
        initial_equity=Decimal("1000"), realized_net_pnl=Decimal("-1.8"),
        unrealized_pnl=Decimal("1.2"), resolved_count=201,
        closed_count=10396, source_breakdown={"UNRESOLVED": 10195},
    )
    assert bridge.account_value == Decimal("999.4")
    assert bridge.account_value_status == "RECONSTRUCTED_PARTIAL"
    assert bridge.realized_coverage_pct.quantize(Decimal("0.01")) == Decimal("1.93")


def test_vps_qty_zero_bridge_includes_stored_realized_history():
    bridge = reconstruct_paper_account(
        initial_equity=Decimal("1000"),
        realized_net_pnl=Decimal("-69.88382562"),
        unrealized_pnl=Decimal("0.956527521"), resolved_count=6432,
        closed_count=6432, source_breakdown={"STORED_PROVEN": 6432},
        external_adjustments=Decimal("0"),
    )
    assert bridge.account_value == Decimal("931.072701901")
    assert bridge.account_value_status == "RECONSTRUCTED_COMPLETE"


def test_numeric_zero_is_a_real_complete_account_value():
    bridge = reconstruct_paper_account(
        initial_equity=Decimal("0"), realized_net_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"), resolved_count=1, closed_count=1,
        source_breakdown={"FINANCIAL_TRUTH": 1},
        external_adjustments=Decimal("0"),
    )
    assert bridge.account_value == Decimal("0")
    assert bridge.account_value_status == "RECONSTRUCTED_COMPLETE"

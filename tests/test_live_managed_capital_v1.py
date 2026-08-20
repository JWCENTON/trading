from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from common.live_managed_capital import (
    ACCOUNT_SCOPE,
    CONTRACT_VERSION,
    InventoryLimit,
    LiveManagedCapitalBaseline,
    RawOkxAccountSnapshot,
    RawOkxBalance,
    evaluate_live_managed_capital,
    parse_okx_balance_response,
    activate_live_managed_capital_baseline,
)
from common.portfolio_state import OpenInventoryMark, RealizedEvidence, build_portfolio_state


NOW = datetime(2026, 8, 20, 22, 0, tzinfo=timezone.utc)
IDENTITY = "a" * 64
BASELINE = LiveManagedCapitalBaseline(NOW - timedelta(days=1), IDENTITY, Decimal("100"), "b" * 64)
LIMIT = InventoryLimit(Decimal("0.000001"), Decimal("0.00001"), Decimal("1"))


def balance(asset, total, available=None, frozen="0", order_frozen="0"):
    total = Decimal(total)
    return RawOkxBalance(
        asset, total, total if available is None else Decimal(available),
        Decimal(frozen), Decimal(order_frozen), {},
    )


def snapshot(*balances):
    return RawOkxAccountSnapshot(IDENTITY, NOW, tuple(balances))


def evaluate(selected, **changes):
    args = dict(
        marks={"BTC": (Decimal("50000"), NOW)},
        inventory_quantities={}, inventory_limits={"BTC": LIMIT},
        baseline=BASELINE, as_of=NOW,
    )
    args.update(changes)
    return evaluate_live_managed_capital(selected, **args)


def test_decimal_account_equity_and_inventory_is_not_added_twice():
    evidence = evaluate(
        snapshot(balance("USDC", "10.000000000000000001"), balance("BTC", "0.002")),
        inventory_quantities={"BTC": Decimal("0.002")},
    )
    assert evidence.managed_equity == Decimal("110.000000000000000001")
    mark = OpenInventoryMark(
        1, "BTCUSDC", "RSI", "1m", "LONG", Decimal("40000"),
        Decimal("0.002"), "COMPLETE", Decimal("50000"), NOW, "TREND", NOW,
    )
    state = build_portfolio_state(
        environment="LIVE", deployment_id="local-live", as_of=NOW,
        baseline=None, realized=RealizedEvidence(0, 0, None, None),
        open_marks=(mark,), historical_peak_managed_equity=None,
        live_capital=evidence, live_baseline_managed_equity=Decimal("100"),
        live_baseline_at=BASELINE.accepted_at,
    )
    assert state.total_capital == evidence.managed_equity
    assert state.deployed_capital == Decimal("100.000")
    assert state.total_capital != evidence.managed_equity + state.deployed_capital


def test_missing_non_usdc_mark_is_incomplete_not_zero():
    evidence = evaluate(snapshot(balance("BTC", "0.002")), marks={})
    assert evidence.managed_equity is None
    assert evidence.managed_equity_status == "INCOMPLETE"
    assert "PRICE_UNAVAILABLE:BTC" in evidence.incomplete_reasons


def test_availbal_missing_never_falls_back_to_cashbal():
    payload = {"data": [{"details": [{
        "ccy": "USDC", "cashBal": "10", "availBal": "",
        "frozenBal": "0", "ordFrozen": "0",
    }]}]}
    with pytest.raises(ValueError, match="USDC.availBal"):
        parse_okx_balance_response(payload, account_identity_fingerprint=IDENTITY, observed_at=NOW)


def test_owner_flows_change_raw_but_not_flow_adjusted_performance():
    deposited = evaluate(
        snapshot(balance("USDC", "125")),
        cumulative_deposits_and_transfer_in=Decimal("25"),
    )
    withdrawn = evaluate(
        snapshot(balance("USDC", "90")),
        cumulative_withdrawals_and_transfer_out=Decimal("10"),
    )
    gain = evaluate(snapshot(balance("USDC", "107")))
    assert deposited.managed_equity == Decimal("125")
    assert deposited.flow_adjusted_equity == Decimal("100")
    assert withdrawn.flow_adjusted_equity == Decimal("100")
    assert gain.flow_adjusted_equity == Decimal("107")


def test_unknown_asset_and_material_inventory_residual_fail_closed():
    unknown = evaluate(snapshot(balance("USDC", "100"), balance("DOGE", "1")))
    mismatch = evaluate(snapshot(balance("BTC", "0.002")))
    assert unknown.managed_equity is None
    assert any(reason.startswith("UNCLASSIFIED_ACCOUNT_ASSET") for reason in unknown.incomplete_reasons)
    assert mismatch.managed_equity is None
    assert mismatch.inventory_reconciliation_status == "INCOMPLETE"


def test_prebaseline_history_does_not_change_live_current_equity_and_ft_stays_realized_authority():
    evidence = evaluate(snapshot(balance("USDC", "107")))
    common = dict(
        environment="LIVE", deployment_id="local-live", as_of=NOW,
        baseline=None, open_marks=(), historical_peak_managed_equity=None,
        live_capital=evidence, live_baseline_managed_equity=Decimal("100"),
        live_baseline_at=BASELINE.accepted_at,
    )
    first = build_portfolio_state(
        **common, realized=RealizedEvidence(1, 1, Decimal("2"), NOW),
    )
    second = build_portfolio_state(
        **common, realized=RealizedEvidence(999, 999, Decimal("900"), NOW),
    )
    assert first.total_capital == second.total_capital == Decimal("107")
    assert first.realized_pnl == Decimal("2")
    assert first.source_authorities["realized_pnl"] == "canonical_financial_truth_v1.COMPLETE"


def test_reserved_capital_is_not_guessed_zero_and_contract_identity_is_single_source():
    evidence = evaluate(snapshot(balance("USDC", "100")))
    assert evidence.available_capital is None
    assert evidence.available_capital_status == "INCOMPLETE"
    assert evidence.reserved_capital is None
    assert evidence.reserved_capital_status == "NOT_YET_CANONICAL"
    assert ACCOUNT_SCOPE == "DEDICATED_WALTRADE_MANAGED_ACCOUNT"
    assert CONTRACT_VERSION == "LIVE_MANAGED_CAPITAL_AUTHORITY_V1"


def test_baseline_writer_requires_exact_approval_before_any_sql():
    class Cursor:
        def __init__(self):
            self.calls = []
        def execute(self, *args):
            self.calls.append(args)

    cur = Cursor()
    with pytest.raises(ValueError, match="FINGERPRINT_MISMATCH"):
        activate_live_managed_capital_baseline(
            cur, plan={"activation_fingerprint": "bad"},
            expected_fingerprint="different", approved_by="PO",
            approval_reference={"decision": "approved"},
        )
    assert cur.calls == []

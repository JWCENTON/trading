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
    LiveManagedCapitalReadContext,
    evaluate_live_managed_capital,
    parse_okx_balance_response,
    activate_live_managed_capital_baseline,
    build_live_baseline_plan,
    canonical_json,
    plan_artifact_fingerprint,
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


def test_owner_flow_watermark_missing_blocks_adjusted_equity_not_raw_equity():
    evidence = evaluate(
        snapshot(balance("USDC", "125")),
        cumulative_deposits_and_transfer_in=Decimal("0"),
        flow_history_status="NO_SYNC",
    )
    assert evidence.managed_equity == Decimal("125")
    assert evidence.managed_equity_status == "CANONICAL"
    assert evidence.flow_adjusted_equity is None
    assert evidence.flow_history_status == "NO_SYNC"
    assert "OWNER_FLOW_HISTORY_NO_SYNC" in evidence.incomplete_reasons


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


def baseline_plan(*, raw=None, total="205.1128544594105", btc=None, btc_mark=None):
    balances = [RawOkxBalance(
        "USDC", Decimal(total), Decimal(total), Decimal("0"), Decimal("0"), raw or {},
    )]
    marks = {}
    inventory = {}
    limits = {}
    if btc is not None:
        balances.append(RawOkxBalance(
            "BTC", Decimal(btc), Decimal(btc), Decimal("0"), Decimal("0"), {},
        ))
        marks["BTC"] = (Decimal(btc_mark), NOW - timedelta(minutes=1))
        inventory["BTC"] = Decimal(btc)
        limits["BTC"] = LIMIT
    context = LiveManagedCapitalReadContext(
        snapshot=RawOkxAccountSnapshot(
            IDENTITY, NOW, tuple(balances),
        ),
        marks=marks, inventory_quantities=inventory, inventory_limits=limits,
    )
    return build_live_baseline_plan(
        context, deployment_id="local-live", plan_created_at=NOW,
        accepted_at_candidate=NOW - timedelta(minutes=1),
        runtime_revision="6" * 40,
    )


class ApplyCursor:
    def __init__(self, *, duplicate=False):
        self.calls = []
        self.duplicate = duplicate
        self.last_sql = ""

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.calls.append((sql, params))

    def fetchone(self):
        if "SELECT baseline_id" in self.last_sql:
            return (91,) if self.duplicate else None
        if "INSERT INTO" in self.last_sql:
            return (17,)
        return None


def apply_plan(cur, plan, **changes):
    values = dict(
        artifact=plan, expected_fingerprint=plan["artifact_fingerprint"],
        approved_by="Product Owner", approval_reference={"approval": "YES"},
        fresh_environment="LIVE", fresh_deployment_id="local-live",
        fresh_account_identity_fingerprint=IDENTITY,
        fresh_runtime_revision="6" * 40,
    )
    values.update(changes)
    return activate_live_managed_capital_baseline(cur, **values)


def test_immutable_artifact_excludes_mutable_eq_usd_and_uses_decimal_strings():
    first = baseline_plan(raw={"eqUsd": "205.11", "eq": "205.11"})
    second = baseline_plan(raw={"eqUsd": "999.99", "eq": "999.99"})
    assert first == second
    assert first["managed_equity"] == "205.1128544594105"
    assert "eqUsd" not in canonical_json(first["raw_balance_snapshot"])
    assert plan_artifact_fingerprint(first) == first["artifact_fingerprint"]
    with pytest.raises(ValueError, match="FLOAT_FORBIDDEN"):
        canonical_json({"money": 1.5})


def test_later_market_or_balance_changes_cannot_mutate_approved_artifact_on_apply():
    plan = baseline_plan(total="155.1128544594105", btc="0.001", btc_mark="50000")
    frozen = canonical_json(plan)
    # The apply API deliberately has no fresh balance or mark arguments.
    changed_live_balance = Decimal("999.999")
    changed_later_mark = Decimal("123456.78")
    assert changed_live_balance and changed_later_mark
    rebuilt_later = baseline_plan(
        total="155.1128544594105", btc="0.001", btc_mark=str(changed_later_mark),
    )
    assert rebuilt_later["artifact_fingerprint"] != plan["artifact_fingerprint"]
    approved_fingerprint = plan["artifact_fingerprint"]
    cur = ApplyCursor()
    assert apply_plan(cur, plan) == 17
    assert canonical_json(plan) == frozen
    assert plan["artifact_fingerprint"] == approved_fingerprint
    insert_params = next(params for sql, params in cur.calls if "INSERT INTO" in sql)
    assert insert_params[9] == "205.1128544594105"
    assert plan["valuation_snapshot"]["BTC"] == {
        "price": "50000",
        "candle_timestamp": (NOW - timedelta(minutes=1)).isoformat(),
        "mark_source": "candles.close/1m",
        "mark_freshness": "CANONICAL",
        "mark_selection_rule": (
            "LATEST_FULLY_CLOSED_CANONICAL_1M_CANDLE_BEFORE_PLAN_CREATED_AT"
        ),
    }
    assert plan["historical_anchor_policy"] == (
        "APPLY_APPROVED_HISTORICAL_SNAPSHOT_NO_BALANCE_RECONSTRUCTION"
    )


def test_artifact_tamper_and_fresh_fence_mismatches_fail_before_sql():
    plan = baseline_plan()
    tampered = dict(plan, managed_equity="1")
    cases = [
        (tampered, {}, "FINGERPRINT_MISMATCH"),
        (plan, {"fresh_environment": "PAPER"}, "ENVIRONMENT_MISMATCH"),
        (plan, {"fresh_deployment_id": "vps-live"}, "DEPLOYMENT_MISMATCH"),
        (plan, {"fresh_account_identity_fingerprint": "b" * 64}, "IDENTITY_MISMATCH"),
        (plan, {"fresh_runtime_revision": "7" * 40}, "REVISION_MISMATCH"),
    ]
    for artifact, changes, reason in cases:
        cur = ApplyCursor()
        with pytest.raises(ValueError, match=reason):
            apply_plan(cur, artifact, **changes)
        assert cur.calls == []


def test_duplicate_baseline_is_explicit_and_plan_timestamps_are_distinct():
    plan = baseline_plan()
    assert plan["plan_created_at"] == NOW.isoformat()
    assert plan["accepted_at_candidate"] == (NOW - timedelta(minutes=1)).isoformat()
    assert plan["applied_at"] is None
    cur = ApplyCursor(duplicate=True)
    with pytest.raises(ValueError, match="ALREADY_ACCEPTED"):
        apply_plan(cur, plan)
    assert not any("INSERT INTO" in sql for sql, _params in cur.calls)


def test_baseline_writer_requires_exact_approval_before_any_sql():
    plan = baseline_plan()
    cur = ApplyCursor()
    with pytest.raises(ValueError, match="PRODUCT_OWNER_APPROVAL_REQUIRED"):
        apply_plan(cur, plan, approved_by="")
    assert cur.calls == []

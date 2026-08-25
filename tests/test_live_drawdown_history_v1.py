from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.live_drawdown_history import (
    CONTRACT_VERSION,
    DrawdownObservation,
    calculate_drawdown_history,
    capture_observation_candidate,
)
from common.live_managed_capital import (
    InventoryLimit,
    LiveManagedCapitalBaseline,
    LiveManagedCapitalEvidence,
    LiveManagedCapitalReadContext,
    RawOkxAccountSnapshot,
    RawOkxBalance,
)


ROOT = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 8, 23, 0, 0, tzinfo=timezone.utc)
D = Decimal


def observation(minutes, managed, adjusted=None, *, status="CANONICAL", trigger="CADENCE_15M"):
    at = T0 + timedelta(minutes=minutes)
    return DrawdownObservation(
        at, at, trigger, D(managed), D(adjusted if adjusted is not None else managed), status,
    )


def history(*rows, baseline="100", as_of_minutes=60):
    return calculate_drawdown_history(
        baseline_managed_equity=D(baseline), baseline_at=T0,
        observations=rows, as_of=T0 + timedelta(minutes=as_of_minutes),
    )


def test_first_observation_and_new_peak_are_canonical():
    first = history(observation(15, "100"), as_of_minutes=15)
    assert first.history_status == "CANONICAL"
    assert first.recovery_status == "NO_DRAWDOWN"
    assert first.current_drawdown_abs == D("0")
    peak = history(observation(15, "100"), observation(30, "120"), as_of_minutes=30)
    assert peak.peak_flow_adjusted_equity == D("120")
    assert peak.current_drawdown_pct == D("0")
    assert peak.peak_timestamp == T0 + timedelta(minutes=30)


def test_drawdown_deeper_max_recovery_and_later_new_peak():
    rows = (
        observation(15, "120"), observation(30, "110"),
        observation(45, "90"), observation(60, "120"),
        observation(75, "130"),
    )
    falling = history(*rows[:2], as_of_minutes=30)
    assert falling.recovery_status == "IN_DRAWDOWN"
    assert falling.current_drawdown_abs == D("-10")
    assert falling.current_drawdown_pct == D("-8.333333333333333333333333333")
    deeper = history(*rows[:3], as_of_minutes=45)
    assert deeper.max_drawdown_abs == D("-30")
    assert deeper.max_drawdown_pct == D("-25.00")
    recovered = history(*rows[:4], as_of_minutes=60)
    assert recovered.recovery_status == "RECOVERED"
    assert recovered.recovery_timestamp == T0 + timedelta(minutes=60)
    assert recovered.drawdown_duration == timedelta(minutes=30)
    later_peak = history(*rows, as_of_minutes=75)
    assert later_peak.peak_flow_adjusted_equity == D("130")
    assert later_peak.current_drawdown_abs == D("0")
    assert later_peak.max_drawdown_abs == D("-30")


def test_owner_flow_in_and_out_are_neutral_and_decimal_exact():
    deposit = history(observation(15, "110.000000000000000001", "100"), as_of_minutes=15)
    assert deposit.current_managed_equity == D("110.000000000000000001")
    assert deposit.current_flow_adjusted_equity == D("100")
    assert deposit.current_drawdown_abs == D("0")
    withdrawal = history(observation(15, "90.000000000000000001", "100"), as_of_minutes=15)
    assert withdrawal.current_managed_equity == D("90.000000000000000001")
    assert withdrawal.current_flow_adjusted_equity == D("100")
    assert withdrawal.current_drawdown_abs == D("0")


def test_fail_closed_gap_zero_peak_and_incomplete_rows():
    gap = history(observation(15, "100"), observation(45, "99"), as_of_minutes=45)
    assert gap.history_status == "OBSERVATION_GAP"
    assert gap.current_drawdown_abs is None
    zero = history(observation(15, "0"), baseline="0", as_of_minutes=15)
    assert zero.history_status == "ZERO_PEAK_PERCENT_UNAVAILABLE"
    assert zero.current_drawdown_pct is None
    for status in (
        "INCOMPLETE_PORTFOLIO_STATE", "INCOMPLETE_MARK",
        "INCOMPLETE_FINANCIAL_TRUTH", "INCOMPLETE_CAPITAL_FLOW",
        "ACCOUNT_IDENTITY_MISMATCH", "SOURCE_FINGERPRINT_MISMATCH",
    ):
        failed = history(observation(15, "100", status=status), as_of_minutes=15)
        assert failed.history_status == status
        assert failed.current_drawdown_abs is None


def test_live_decimal_validation_error_contract_is_unchanged():
    with pytest.raises(ValueError, match="LIVE_DRAWDOWN_DECIMAL_REQUIRED"):
        calculate_drawdown_history(
            baseline_managed_equity=100.0, baseline_at=T0,
            observations=(), as_of=T0,
        )


class State:
    environment = "LIVE"
    deployment_id = "local-live"
    total_capital = D("100")
    total_capital_status = "CANONICAL"
    realized_pnl = D("1")
    realized_pnl_status = "CANONICAL"
    unrealized_pnl = D("2")
    unrealized_pnl_status = "CANONICAL"
    deployed_capital = D("10")
    deployed_capital_status = "CANONICAL"
    reserved_capital = D("3")
    reserved_capital_status = "CANONICAL"
    available_capital = D("87")
    available_capital_status = "CANONICAL"

    def __init__(self, **overrides):
        for key, value in overrides.items():
            setattr(self, key, value)

    def serializable(self):
        return {
            "portfolio_state_version": "PORTFOLIO_STATE_V1",
            "total_capital": "100",
            "total_capital_status": "CANONICAL",
        }


def capture_inputs():
    identity = "a" * 64
    balance = RawOkxBalance("USDC", D("100"), D("87"), D("3"), D("3"), {"ccy": "USDC"})
    context = LiveManagedCapitalReadContext(
        RawOkxAccountSnapshot(identity, T0 + timedelta(minutes=15), (balance,)),
        {}, {}, {"USDC": InventoryLimit(D("0.01"), D("0"), D("0"))},
    )
    capital = LiveManagedCapitalEvidence(
        D("100"), "CANONICAL", D("87"), D("87"), "CANONICAL",
        D("3"), "CANONICAL", None, D("0"), D("0"), "CANONICAL",
        T0 + timedelta(minutes=15), None, (), flow_history_status="STALE_SYNC",
        flow_sync_through=T0,
    )
    baseline = LiveManagedCapitalBaseline(T0, identity, D("100"), "b" * 64)
    return context, capital, baseline


def test_capture_fail_closed_statuses_and_deterministic_idempotency():
    context, capital, baseline = capture_inputs()
    common = dict(
        live_capital=capital, context=context, baseline_id=1, baseline=baseline,
        observed_at=T0 + timedelta(minutes=15, seconds=4),
        observation_trigger="CADENCE_15M", trigger_reference="2026-08-23T00:15:00+00:00",
        producer_identity="test", git_revision="c" * 40,
    )
    first = capture_observation_candidate(state=State(), **common)
    second = capture_observation_candidate(state=State(), **common)
    assert first.status == "READY_FOR_FLOW_WATERMARK"
    assert first.candidate.observation_identity == second.candidate.observation_identity
    stale = State(unrealized_pnl_status="PRICE_STALE")
    assert capture_observation_candidate(state=stale, **common).status == "INCOMPLETE_MARK"
    wrong_context = replace(
        context, snapshot=replace(context.snapshot, account_identity_fingerprint="d" * 64)
    )
    assert capture_observation_candidate(
        state=State(), **{**common, "context": wrong_context}
    ).status == "ACCOUNT_IDENTITY_MISMATCH"


def test_contract_manifest_and_migration_are_bounded_forward_only():
    contract_path = ROOT / "contracts/live_drawdown_history_authority_v1_contract.json"
    contract = json.loads(contract_path.read_text())
    assert contract["contract_version"] == CONTRACT_VERSION
    assert contract["historical_policy"] == "FORWARD_ONLY_NO_BACKFILL"
    expected = (
        ROOT / "contracts/live_drawdown_history_authority_v1_contract.sha256"
    ).read_text().strip()
    assert hashlib.sha256(contract_path.read_bytes()).hexdigest() == expected
    migration = (ROOT / "db/migrations/20260823_live_drawdown_history_authority_v1.sql").read_text().upper()
    assert "INSERT INTO LIVE_MANAGED_EQUITY_OBSERVATION_V1" not in migration
    assert "UPDATE LIVE_MANAGED_EQUITY_OBSERVATION_V1" not in migration
    assert "DELETE FROM LIVE_MANAGED_EQUITY_OBSERVATION_V1" not in migration
    assert "DROP TABLE" not in migration

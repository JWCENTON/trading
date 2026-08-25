from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.drawdown_history import (
    DrawdownObservation,
    calculate_drawdown_history,
    canonical_numeric_38_18,
)
from common.paper_drawdown_history import (
    CADENCE,
    CONTRACT_VERSION,
    FAILURE_PRIORITY,
    STALE_AFTER,
    ActivationEvidence,
    PaperDrawdownHistory,
    calibration_readiness,
    capture_observation_candidate,
)
from common.paper_equity_baseline_v2 import PaperEquityBaselineV2


ROOT = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 8, 25, 10, 0, tzinfo=timezone.utc)
D = Decimal


def observation(minutes: int, equity: str, *, trigger: str = "CADENCE_15M"):
    at = T0 + timedelta(minutes=minutes)
    return DrawdownObservation(at, at, trigger, D(equity), D(equity))


def history(*rows, as_of_minutes: int, baseline: str = "100"):
    return calculate_drawdown_history(
        baseline_managed_equity=D(baseline), baseline_at=T0,
        observations=rows, as_of=T0 + timedelta(minutes=as_of_minutes),
        cadence=CADENCE, stale_after=STALE_AFTER,
        failure_priority=FAILURE_PRIORITY, cadence_anchor_at=T0,
        timestamp_error="PAPER_DRAWDOWN_AS_OF_REQUIRED",
    )


def baseline() -> PaperEquityBaselineV2:
    return PaperEquityBaselineV2(
        baseline_id=7, deployment_id="local-paper", baseline_timestamp=T0,
        baseline_account_total=D("120"), baseline_managed_equity=D("100"),
        baseline_external_manual=D("20"), baseline_available=D("100"),
        baseline_inventory_value=D("20"), baseline_realized_net_pnl=D("0"),
        baseline_unrealized_pnl=D("1"), baseline_fees=D("0"),
        baseline_open_positions=1, frozen_pre_baseline_unresolved_count=2,
        evidence_status="COMPLETE", source_authority="canonical",
        approved_by="Product Owner", approval_provenance={"approved": True},
        activation_fingerprint="b" * 64, created_at=T0,
    )


def activation() -> ActivationEvidence:
    return ActivationEvidence(
        3, 7, "local-paper", T0, T0, "b" * 64, "a" * 64, "c" * 64,
    )


class State:
    environment = "PAPER"
    deployment_id = "local-paper"
    total_capital = D("106")
    total_capital_status = "CANONICAL"
    realized_pnl = D("5")
    realized_pnl_status = "CANONICAL"
    unrealized_pnl = D("2")
    unrealized_pnl_status = "CANONICAL"

    def __init__(self, **values):
        self.__dict__.update(values)

    def serializable(self):
        return {
            "portfolio_state_version": "PORTFOLIO_STATE_V1",
            "environment": self.environment,
            "deployment_id": self.deployment_id,
            "as_of": (T0 + timedelta(minutes=15)).isoformat(),
            "total_capital": str(self.total_capital),
            "total_capital_status": self.total_capital_status,
            "realized_pnl": str(self.realized_pnl),
            "realized_pnl_status": self.realized_pnl_status,
            "unrealized_pnl": str(self.unrealized_pnl),
            "unrealized_pnl_status": self.unrealized_pnl_status,
            "source_authorities": {
                "total_capital": "PAPER_EQUITY_BASELINE_V2_PLUS_POST_BASELINE_CANONICAL_FINANCIAL_TRUTH_COMPLETE_PLUS_FRESH_OPEN_MARK"
            },
        }


def capture(state=None):
    return capture_observation_candidate(
        state=state or State(), baseline=baseline(), activation=activation(),
        observed_at=T0 + timedelta(minutes=15),
        observation_trigger="CADENCE_15M",
        trigger_reference=(T0 + timedelta(minutes=15)).isoformat(),
        producer_identity="test", git_revision="d" * 40,
    )


def test_numeric_38_18_canonicalization_matches_frozen_postgres_semantics():
    exact = D("785.153275660783716231")
    assert canonical_numeric_38_18(exact) == exact
    assert canonical_numeric_38_18(
        D("785.153275660783716231082848")
    ) == D("785.153275660783716231")
    assert canonical_numeric_38_18(D("1.0000000000000000005")) == D(
        "1.000000000000000001"
    )
    assert canonical_numeric_38_18(D("-1.0000000000000000005")) == D(
        "-1.000000000000000001"
    )
    assert canonical_numeric_38_18(D("0.0000000000000000004")) == D("0")
    assert canonical_numeric_38_18(D("-0.0000000000000000005")) == D(
        "-0.000000000000000001"
    )
    with pytest.raises(ValueError, match="DRAWDOWN_DECIMAL_REQUIRED"):
        canonical_numeric_38_18(1.25)


def test_confirmed_natural_capital_basis_canonicalizes_before_exact_equality():
    natural_baseline = replace(
        baseline(),
        baseline_managed_equity=D("925.729373904606433753"),
        baseline_unrealized_pnl=D("0.197552812000000000"),
    )
    raw_total = D("785.153275660783716231082848")
    raw_realized = D("-139.783285143822717521917152")
    raw_unrealized = D("-0.595260288")

    def natural_capture(total):
        return capture_observation_candidate(
            state=State(
                total_capital=total,
                realized_pnl=raw_realized,
                unrealized_pnl=raw_unrealized,
            ),
            baseline=natural_baseline, activation=activation(),
            observed_at=T0 + timedelta(minutes=15),
            observation_trigger="CADENCE_15M",
            trigger_reference=(T0 + timedelta(minutes=15)).isoformat(),
            producer_identity="test", git_revision="d" * 40,
        )

    raw = natural_capture(raw_total)
    canonical = natural_capture(D("785.153275660783716231"))
    assert raw.status == canonical.status == "CANONICAL"
    assert raw.candidate.managed_equity == D("785.153275660783716231")
    assert raw.candidate.portfolio_state_evidence["total_capital"] == str(raw_total)
    assert raw.candidate.portfolio_state_fingerprint != (
        canonical.candidate.portfolio_state_fingerprint
    )
    assert raw.candidate.source_fingerprints["managed_equity_canonical"] == (
        canonical.candidate.source_fingerprints["managed_equity_canonical"]
    )
    assert raw.candidate.observation_identity == canonical.candidate.observation_identity
    assert natural_capture(D("785.153275660783716230")).status == (
        "SOURCE_FINGERPRINT_MISMATCH"
    )
    rounded_up = natural_capture(D("785.1532756607837162315"))
    assert rounded_up.status == "SOURCE_FINGERPRINT_MISMATCH"


def test_first_forward_observation_new_peak_and_decimal_exactness():
    first = history(observation(15, "100.000000000000000001"), as_of_minutes=15)
    assert first.history_status == "CANONICAL"
    assert first.current_drawdown_abs == D("0")
    peak = history(
        observation(15, "100.000000000000000001"),
        observation(30, "120.000000000000000002"), as_of_minutes=30,
    )
    assert peak.peak_flow_adjusted_equity == D("120.000000000000000002")
    assert peak.peak_timestamp == T0 + timedelta(minutes=30)


def test_drawdown_max_recovery_and_new_peak_after_recovery():
    rows = (
        observation(15, "120"), observation(30, "110"),
        observation(45, "90"), observation(60, "120"),
        observation(75, "130"),
    )
    falling = history(*rows[:2], as_of_minutes=30)
    assert falling.current_drawdown_abs == D("-10")
    assert falling.recovery_status == "IN_DRAWDOWN"
    deepest = history(*rows[:3], as_of_minutes=45)
    assert deepest.max_drawdown_abs == D("-30")
    assert deepest.max_drawdown_pct == D("-25")
    recovered = history(*rows[:4], as_of_minutes=60)
    assert recovered.recovery_status == "RECOVERED"
    assert recovered.recovery_timestamp == T0 + timedelta(minutes=60)
    assert recovered.drawdown_duration == timedelta(minutes=30)
    new_peak = history(*rows, as_of_minutes=75)
    assert new_peak.peak_flow_adjusted_equity == D("130")
    assert new_peak.max_drawdown_abs == D("-30")


def test_gap_is_fail_closed_including_first_required_bucket():
    internal = history(
        observation(15, "100"), observation(45, "99"), as_of_minutes=45,
    )
    assert internal.history_status == "OBSERVATION_GAP"
    first_missing = history(
        observation(30, "100"), as_of_minutes=30,
    )
    assert first_missing.history_status == "OBSERVATION_GAP"


def test_capital_basis_identity_and_incomplete_exclusion():
    first = capture()
    second = capture()
    assert first.status == "CANONICAL"
    assert first.candidate.managed_equity == D("106")
    assert "drawdown" not in first.candidate.portfolio_state_evidence
    assert first.candidate.observation_identity == second.candidate.observation_identity
    assert capture(State(total_capital_status="INCOMPLETE")).status == "INCOMPLETE_PORTFOLIO_STATE"
    assert capture(State(realized_pnl_status="INCOMPLETE")).status == "INCOMPLETE_FINANCIAL_TRUTH"
    assert capture(State(unrealized_pnl_status="PRICE_STALE")).status == "INCOMPLETE_MARK"
    assert capture(State(total_capital=D("106.01"))).status == "SOURCE_FINGERPRINT_MISMATCH"
    changed = capture(State(total_capital=D("107"), realized_pnl=D("6")))
    assert changed.candidate.observation_identity != first.candidate.observation_identity


def test_readiness_has_no_invented_numeric_sufficiency_thresholds():
    calculated = history(observation(15, "100"), as_of_minutes=15)
    paper = PaperDrawdownHistory(
        **calculated.__dict__, source_fingerprint="f" * 64
    )
    readiness = calibration_readiness(paper)
    assert readiness.authority_ready is True
    assert readiness.continuity_ready is True
    assert readiness.episode_ready is None
    assert readiness.market_coverage_ready is None
    assert readiness.shadow_outcome_ready is None
    assert readiness.financial_model_stable is None
    assert readiness.accounting_ready is True
    assert readiness.calibration_ready is False
    assert set(readiness.serializable()) == {
        "AUTHORITY_READY", "CONTINUITY_READY", "EPISODE_READY",
        "MARKET_COVERAGE_READY", "SHADOW_OUTCOME_READY",
        "FINANCIAL_MODEL_STABLE", "ACCOUNTING_READY", "CALIBRATION_READY",
    }
    assert set(paper.serializable()) == {
        "CURRENT_DRAWDOWN_ABS", "CURRENT_DRAWDOWN_PCT",
        "MAX_DRAWDOWN_ABS", "MAX_DRAWDOWN_PCT", "PEAK_TIMESTAMP",
        "DRAWDOWN_START", "RECOVERY_TIMESTAMP", "DRAWDOWN_DURATION",
        "RECOVERY_STATUS", "HISTORY_STATUS", "LATEST_OBSERVATION_AT",
        "SOURCE_FINGERPRINT",
    }


def test_contract_and_migration_are_forward_only_without_daily_snapshot_reuse():
    contract_path = ROOT / "contracts/paper_drawdown_history_authority_v1_contract.json"
    contract = json.loads(contract_path.read_text())
    assert contract["contract_version"] == CONTRACT_VERSION
    assert contract["historical_policy"] == "FORWARD_ONLY_NO_BACKFILL"
    assert contract["numeric_sufficiency_thresholds"] is False
    assert contract["managed_equity_precision"] == "NUMERIC(38,18)"
    assert contract["canonical_scale"] == 18
    assert contract["exact_equality_after_canonicalization"] is True
    assert contract["float_allowed"] is False
    assert contract["tolerance_comparison"] is False
    expected = (ROOT / "contracts/paper_drawdown_history_authority_v1_contract.sha256").read_text().strip()
    assert hashlib.sha256(contract_path.read_bytes()).hexdigest() == expected
    migration = (ROOT / "db/migrations/20260825_paper_drawdown_history_authority_v1.sql").read_text().upper()
    assert "EQUITY_DAILY_SNAPSHOT_V1" not in migration
    assert "INSERT INTO PUBLIC.PAPER_MANAGED_EQUITY_OBSERVATION_V1" not in migration
    assert "UPDATE PUBLIC.PAPER_MANAGED_EQUITY_OBSERVATION_V1" not in migration
    assert "DELETE FROM PUBLIC.PAPER_MANAGED_EQUITY_OBSERVATION_V1" not in migration
    assert "DROP TABLE" not in migration
    corrective = (
        ROOT / "db/migrations/20260825_paper_drawdown_history_numeric_scale_v1.sql"
    ).read_text().upper()
    assert "ROUND(" in corrective
    assert "NUMERIC(38,18)" in corrective
    assert "INSERT INTO" not in corrective
    assert "UPDATE PUBLIC.PAPER_MANAGED_EQUITY_OBSERVATION_V1" not in corrective
    assert "DELETE FROM" not in corrective

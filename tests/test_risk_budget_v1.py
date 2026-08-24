from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.risk_budget import (
    CONTRACT_VERSION,
    NumericPolicyEvidence,
    RiskBudgetEvidenceError,
    RiskBudgetInputs,
    account_scope_lock_identity,
    canonical_json,
    evaluate_account_scoped_shadow_gate_cursor,
    evaluate_pre_entry_gate,
    evaluate_state,
    fingerprint,
    paper_controlled_influence_readiness,
)


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 8, 24, 15, tzinfo=timezone.utc)
IDENTITY = "a" * 64
POLICY_FP = "b" * 64
CANDIDATE_FP = "c" * 64


def inputs(**changes):
    base = RiskBudgetInputs(
        environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint=IDENTITY, as_of=NOW,
        total_capital=Decimal("803.979895054397180231"),
        total_capital_status="CANONICAL",
        open_risk=Decimal("1.021791813582144"),
        open_risk_status="CANONICAL",
        pre_entry_committed_risk=Decimal("0.229434428049600"),
        pre_entry_risk_status="CANONICAL",
        current_drawdown_abs=Decimal("-121.5"),
        current_drawdown_pct=Decimal("-13.16"),
        max_drawdown_abs=Decimal("-140.1"),
        max_drawdown_pct=Decimal("-15.02"),
        recovery_status="IN_DRAWDOWN",
        drawdown_history_status="CANONICAL",
        source_fingerprints={
            "portfolio_state": "1" * 64,
            "open_risk": "2" * 64,
            "pre_entry_risk": "3" * 64,
            "drawdown_history": "4" * 64,
        },
    )
    return replace(base, **changes)


def policy(*, capacity=None, state=None, status="MISSING_POLICY"):
    return NumericPolicyEvidence(
        policy_version="RISK_BUDGET_PRODUCT_POLICY_V1",
        policy_fingerprint=POLICY_FP, status=status,
        policy_state=state, total_risk_capacity=capacity,
    )


def canonical_snapshot(*, capacity="3", state="NORMAL"):
    return evaluate_state(
        inputs(), policy(capacity=Decimal(capacity), state=state, status="CANONICAL")
    )


def test_active_risk_is_exact_decimal_sum_and_reservation_is_not_an_input():
    snapshot = evaluate_state(inputs(), policy())
    assert snapshot.used_risk == Decimal("1.251226241631744")
    assert snapshot.total_risk_capacity is None
    assert snapshot.available_risk_capacity is None
    assert snapshot.authority_status == "MISSING_POLICY"
    assert snapshot.policy_state is None
    assert not hasattr(inputs(), "reserved_capital")


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({"total_capital_status": "INCOMPLETE"}, "INCOMPLETE_PORTFOLIO_STATE"),
        ({"drawdown_history_status": "NO_HISTORY"}, "INCOMPLETE_DRAWDOWN_HISTORY"),
        ({"open_risk_status": "MISSING_MARK"}, "INCOMPLETE_OPEN_RISK"),
        ({"pre_entry_risk_status": "EVIDENCE_INCOMPLETE"}, "INCOMPLETE_PRE_ENTRY_RISK"),
        ({"identity_status": "ACCOUNT_IDENTITY_MISMATCH"}, "ACCOUNT_IDENTITY_MISMATCH"),
        ({"source_fingerprint_status": "MISMATCH"}, "SOURCE_FINGERPRINT_MISMATCH"),
        ({"freshness_status": "STALE"}, "STALE_AUTHORITY"),
    ],
)
def test_required_evidence_fails_closed_before_missing_policy(changes, expected):
    snapshot = evaluate_state(inputs(**changes), policy())
    assert snapshot.authority_status == expected
    decision = evaluate_pre_entry_gate(
        snapshot, candidate_pre_entry_risk=Decimal("0.1"),
        candidate_evidence_fingerprint=CANDIDATE_FP,
        environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint=IDENTITY,
    )
    assert decision.result == "BLOCK_NEW_RISK"
    assert decision.authority_status == expected


def test_capacity_formula_and_allow_reduce_block_advisories():
    snapshot = canonical_snapshot(capacity="3")
    assert snapshot.authority_status == "CANONICAL"
    assert snapshot.available_risk_capacity == Decimal("1.748773758368256")

    def gate(candidate):
        return evaluate_pre_entry_gate(
            snapshot, candidate_pre_entry_risk=Decimal(candidate),
            candidate_evidence_fingerprint=CANDIDATE_FP,
            environment="PAPER", deployment_id="local-paper",
            account_identity_fingerprint=IDENTITY,
        )

    assert gate("1").result == "ALLOW"
    reduced = gate("2")
    assert reduced.result == "REDUCE"
    assert reduced.admissible_risk_capacity == Decimal("1.748773758368256")

    no_new = canonical_snapshot(capacity="3", state="NO_NEW_RISK")
    assert evaluate_pre_entry_gate(
        no_new, candidate_pre_entry_risk=Decimal("0.1"),
        candidate_evidence_fingerprint=CANDIDATE_FP,
        environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint=IDENTITY,
    ).result == "BLOCK_NEW_RISK"


@pytest.mark.parametrize("state", ["NORMAL", "REDUCED", "NO_NEW_RISK", "PAUSED"])
def test_all_approved_policy_states_are_versioned_without_thresholds(state):
    snapshot = canonical_snapshot(capacity="3", state=state)
    assert snapshot.policy_state == state


def test_capacity_exhausted_and_missing_policy_are_explicit_blocks():
    exhausted = canonical_snapshot(capacity="1")
    assert exhausted.authority_status == "RISK_CAPACITY_EXHAUSTED"
    assert exhausted.available_risk_capacity == Decimal("0")
    missing = evaluate_state(inputs(), policy())
    decision = evaluate_pre_entry_gate(
        missing, candidate_pre_entry_risk=Decimal("0.1"),
        candidate_evidence_fingerprint=CANDIDATE_FP,
        environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint=IDENTITY,
    )
    assert decision.result == "BLOCK_NEW_RISK"
    assert decision.authority_status == "MISSING_POLICY"


def test_gate_identity_and_candidate_fingerprint_fail_closed():
    snapshot = canonical_snapshot()
    mismatch = evaluate_pre_entry_gate(
        snapshot, candidate_pre_entry_risk=Decimal("0.1"),
        candidate_evidence_fingerprint=CANDIDATE_FP,
        environment="LIVE", deployment_id="local-live",
        account_identity_fingerprint=IDENTITY,
    )
    assert mismatch.result == "BLOCK_NEW_RISK"
    assert mismatch.authority_status == "ACCOUNT_IDENTITY_MISMATCH"
    bad_fp = evaluate_pre_entry_gate(
        snapshot, candidate_pre_entry_risk=Decimal("0.1"),
        candidate_evidence_fingerprint="bad",
        environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint=IDENTITY,
    )
    assert bad_fp.authority_status == "SOURCE_FINGERPRINT_MISMATCH"


def test_account_scoped_api_locks_before_re_read_and_never_executes():
    calls = []

    class Cursor:
        def execute(self, query, params):
            calls.append((query, params))

    def loader(cur):
        assert calls and "pg_advisory_xact_lock" in calls[0][0]
        calls.append(("LOADED_OPEN_AND_PRE_ENTRY", cur))
        return inputs()

    snapshot, decision = evaluate_account_scoped_shadow_gate_cursor(
        Cursor(), environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint=IDENTITY, input_loader=loader,
        policy=policy(), candidate_pre_entry_risk=Decimal("0.1"),
        candidate_evidence_fingerprint=CANDIDATE_FP,
    )
    assert snapshot.used_risk == Decimal("1.251226241631744")
    assert decision.result == "BLOCK_NEW_RISK"
    assert len(calls) == 2
    assert account_scope_lock_identity(
        "PAPER", "local-paper", IDENTITY
    ).endswith(IDENTITY)


def test_paper_controlled_influence_is_hard_off_and_float_is_forbidden():
    assert paper_controlled_influence_readiness() == (
        False,
        "APPROVED_NUMERIC_POLICY_AND_HISTORY_SUFFICIENCY_CALIBRATION_REQUIRED",
    )
    with pytest.raises(RiskBudgetEvidenceError, match="BINARY_FLOAT_FORBIDDEN"):
        canonical_json({"risk": 0.1})


def test_canonical_drawdown_duration_is_exact_deterministic_and_not_omitted():
    duration = timedelta(days=2, seconds=3, microseconds=456789)
    payload = {
        "drawdown_duration": duration,
        "drawdown": Decimal("-1.2300"),
        "observed_at": NOW,
        "observation_date": date(2026, 8, 24),
    }
    rendered = canonical_json(payload)
    normalized = json.loads(rendered)
    assert normalized["drawdown_duration"] == {
        "duration_microseconds": 172803456789,
    }
    assert normalized["drawdown"] == "-1.2300"
    assert normalized["observed_at"] == "2026-08-24T15:00:00+00:00"
    assert normalized["observation_date"] == "2026-08-24"
    assert fingerprint(payload) == fingerprint(dict(reversed(tuple(payload.items()))))


def test_changed_duration_changes_fingerprint_without_float_seconds():
    base = {"drawdown_duration": timedelta(seconds=1, microseconds=1)}
    changed = {"drawdown_duration": timedelta(seconds=1, microseconds=2)}
    assert fingerprint(base) != fingerprint(changed)
    assert "1.000001" not in canonical_json(base)


def test_unsupported_canonical_value_fails_closed_explicitly():
    with pytest.raises(
        RiskBudgetEvidenceError,
        match="CANONICAL_SERIALIZATION_UNSUPPORTED_TYPE:object",
    ):
        canonical_json({"unsupported": object()})


def test_contract_manifest_and_checksum_are_stable():
    contract_path = ROOT / "contracts/risk_budget_authority_v1_contract.json"
    contract = json.loads(contract_path.read_text())
    assert contract["contract_version"] == CONTRACT_VERSION
    assert contract["execution_influence"] is False
    assert contract["paper"]["controlled_influence_ready"] is False
    expected = (
        ROOT / "contracts/risk_budget_authority_v1_contract.sha256"
    ).read_text().strip()
    assert hashlib.sha256(contract_path.read_bytes()).hexdigest() == expected


def test_migration_is_additive_shadow_only_and_has_no_backfill():
    migration = (
        ROOT / "db/migrations/20260824_risk_budget_authority_v1.sql"
    ).read_text().upper()
    assert "CREATE TABLE IF NOT EXISTS PUBLIC.RISK_BUDGET_EVENT_V1" in migration
    assert "CREATE OR REPLACE VIEW PUBLIC.V_RISK_BUDGET_CURRENT_V1" in migration
    assert "APPEND_ONLY" in migration
    assert "DROP TABLE" not in migration
    assert "DROP COLUMN" not in migration
    assert "TRUNCATE" not in migration
    assert "INSERT INTO PUBLIC.RISK_BUDGET_EVENT_V1" not in migration
    assert "PAPER_CONTROLLED_INFLUENCE_READY BOOLEAN NOT NULL DEFAULT FALSE" in migration

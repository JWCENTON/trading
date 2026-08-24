from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
import uuid

import pytest

from common.capital_reservation import paper_account_identity_fingerprint
from common.live_drawdown_history import LiveDrawdownHistory
from common.live_managed_capital import (
    LiveManagedCapitalBaseline,
    LiveManagedCapitalEvidence,
    LiveManagedCapitalReadContext,
    RawOkxAccountSnapshot,
)
from common.owner_capital_flow_sync import OwnerFlowHistoryAuthority
from common.pre_entry_risk import CommittedPreEntryRiskEvidence
from common.risk_budget import evaluate_state, missing_numeric_policy_evidence
import common.risk_budget_runtime as runtime


NOW = datetime(2026, 8, 24, 16, 7, tzinfo=timezone.utc)
REVISION = "a" * 40


class PeakCursor:
    def __init__(self):
        self.query = ""

    def execute(self, query, params=None):
        self.query = query

    def fetchone(self):
        return (Decimal("100"), Decimal("110"))


@dataclass(frozen=True)
class State:
    as_of: datetime = NOW
    total_capital: Decimal = Decimal("99")
    total_capital_status: str = "CANONICAL"
    open_risk: Decimal = Decimal("2.25")
    open_risk_status: str = "CANONICAL"
    drawdown: Decimal = Decimal("-10")
    drawdown_status: str = "CANONICAL"

    def serializable(self):
        return {
            "as_of": self.as_of.isoformat(), "total_capital": "99",
            "total_capital_status": self.total_capital_status,
            "open_risk": "2.25", "open_risk_status": self.open_risk_status,
            "drawdown": "-10", "drawdown_status": self.drawdown_status,
        }


def test_canonical_adapter_uses_open_and_pre_entry_risk_without_reservation(monkeypatch):
    seen = {}

    def committed(_cur, **kwargs):
        seen.update(kwargs)
        return CommittedPreEntryRiskEvidence(
            Decimal("0.75"), 1, "CANONICAL"
        )

    monkeypatch.setattr(
        runtime, "load_committed_pre_entry_risk_evidence_cursor", committed,
    )
    excluded = uuid.uuid4()
    adapted = runtime.load_canonical_risk_budget_inputs_cursor(
        PeakCursor(), deployment_id="local-paper", as_of=NOW,
        runtime_revision=REVISION,
        exclude_pre_entry_risk_id=excluded,
        portfolio_state_reader=lambda *args, **kwargs: State(),
    )
    snapshot = evaluate_state(adapted, missing_numeric_policy_evidence())
    assert adapted.total_capital == Decimal("99")
    assert adapted.open_risk == Decimal("2.25")
    assert adapted.pre_entry_committed_risk == Decimal("0.75")
    assert snapshot.used_risk == Decimal("3.00")
    assert seen["exclude_pre_entry_risk_id"] == excluded
    assert not hasattr(adapted, "reserved_capital")
    assert set(adapted.source_fingerprints) == {
        "portfolio_state", "open_risk", "pre_entry_risk", "drawdown_history",
    }


def test_paper_current_drawdown_is_exact_but_full_history_stays_incomplete(monkeypatch):
    monkeypatch.setattr(
        runtime, "load_committed_pre_entry_risk_evidence_cursor",
        lambda *args, **kwargs: CommittedPreEntryRiskEvidence(
            Decimal("0"), 0, "CANONICAL"
        ),
    )
    adapted = runtime.load_canonical_risk_budget_inputs_cursor(
        PeakCursor(), deployment_id="local-paper", as_of=NOW,
        runtime_revision=REVISION,
        portfolio_state_reader=lambda *args, **kwargs: State(),
    )
    assert adapted.current_drawdown_abs == Decimal("-11")
    assert adapted.current_drawdown_pct == Decimal("-10")
    assert adapted.max_drawdown_abs is None
    assert adapted.max_drawdown_pct is None
    assert adapted.recovery_status is None
    assert adapted.drawdown_history_status == "INCOMPLETE_DRAWDOWN_HISTORY"
    snapshot = evaluate_state(adapted, missing_numeric_policy_evidence())
    assert snapshot.authority_status == "INCOMPLETE_DRAWDOWN_HISTORY"
    assert snapshot.total_risk_capacity is None
    assert snapshot.available_risk_capacity is None


def test_fail_open_shadow_savepoint_never_propagates_or_changes_execution(monkeypatch):
    calls = []

    class Cursor:
        def execute(self, query, params=None):
            calls.append(query)

    monkeypatch.setattr(
        runtime, "record_paper_pre_entry_shadow_gate_cursor",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("evidence only")),
    )
    result = runtime.record_paper_pre_entry_shadow_gate_fail_open_cursor(
        Cursor(), pre_entry_risk_id=uuid.uuid4(), deployment_id="local-paper",
        as_of=NOW, git_revision=REVISION,
    )
    assert result.status == "EVIDENCE_FAILURE_EXECUTION_UNCHANGED"
    assert calls == [
        "SAVEPOINT risk_budget_shadow_gate_v1",
        "ROLLBACK TO SAVEPOINT risk_budget_shadow_gate_v1",
        "RELEASE SAVEPOINT risk_budget_shadow_gate_v1",
    ]
    assert runtime.RISK_BUDGET_EXECUTION_INFLUENCE is False


def test_natural_candidate_adapter_excludes_candidate_and_preserves_identity(monkeypatch):
    candidate_id = uuid.uuid4()
    identity = paper_account_identity_fingerprint("local-paper")
    candidate_fp = "c" * 64

    class Cursor:
        def __init__(self):
            self.rows = [
                ("risk_budget_event_v1",),
                (Decimal("0.4"), candidate_fp, identity, "decision-7", "CANONICAL"),
            ]

        def execute(self, query, params=None):
            pass

        def fetchone(self):
            return self.rows.pop(0)

    expected = runtime.ShadowGateResult("IDEMPOTENT")
    seen = {}

    def persist(*args, **kwargs):
        seen.update(kwargs)
        return expected

    monkeypatch.setattr(runtime, "persist_shadow_gate_evaluation_cursor", persist)
    result = runtime.record_paper_pre_entry_shadow_gate_cursor(
        Cursor(), pre_entry_risk_id=candidate_id, deployment_id="local-paper",
        as_of=NOW, git_revision=REVISION,
    )
    assert result == expected
    assert seen["pre_entry_risk_id"] == candidate_id
    assert seen["candidate_pre_entry_risk"] == Decimal("0.4")
    assert seen["candidate_evidence_fingerprint"] == candidate_fp
    assert seen["candidate_account_identity_fingerprint"] == identity
    assert seen["decision_identity"] == "decision-7"


def test_runtime_call_sites_are_shadow_only_and_packaged():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    automation = (root / "automation_runner/main.py").read_text()
    execution = (root / "common/simulated_execution_evidence.py").read_text()
    live_execution = (root / "common/execution.py").read_text()
    dockerfile = (root / "automation_runner/Dockerfile").read_text()
    assert "run_risk_budget_state_evaluation_cycle(" in automation
    assert "exchange_client=(" in automation
    assert "record_paper_pre_entry_shadow_gate_fail_open_cursor" in execution
    assert "execution_effect=NONE" in execution
    assert "record_live_pre_entry_shadow_gate_fail_open_cursor" in live_execution
    assert live_execution.index("freeze_live_pre_entry_risk_cursor(") < live_execution.index(
        "record_live_pre_entry_shadow_gate_fail_open_cursor("
    ) < live_execution.index("prepare_live_submission_cursor(")
    assert "risk_budget_live_pre_entry_shadow_v1" in live_execution
    assert "execution_effect=NONE" in live_execution
    assert "COPY common /app/common" in dockerfile


def test_shadow_hook_does_not_change_order_quantity_decision_or_reservation(
    monkeypatch,
):
    import common.simulated_execution_evidence as execution

    decision_id = uuid.uuid4()
    reservation_id = uuid.uuid4()
    risk_id = uuid.uuid4()
    calls = []

    class Cursor:
        connection = object()

        def __init__(self):
            self.next_row = None
            self.insert_params = None

        def execute(self, query, params=None):
            if "register_forward_entry_decision_v1" in query:
                self.next_row = (decision_id,)
            elif "INSERT INTO simulated_orders" in query:
                self.insert_params = params
                self.next_row = (42,)

        def fetchone(self):
            row, self.next_row = self.next_row, None
            return row

    cursor = Cursor()
    monkeypatch.setattr(
        execution, "detect_simulated_order_namespace",
        lambda _conn: SimpleNamespace(
            is_legacy=False, is_namespace_v1=True, issues=(),
        ),
    )

    def reserve(_cur, **kwargs):
        calls.append(("reservation", kwargs))
        return "INSERTED", reservation_id

    monkeypatch.setattr(execution, "accept_paper_simulated_order_cursor", reserve)
    monkeypatch.setattr(
        execution, "accept_paper_boundary_cursor",
        lambda _cur, **kwargs: calls.append(("boundary", kwargs)) or "INSERTED",
    )
    monkeypatch.setattr(
        execution, "freeze_paper_pre_entry_risk_cursor",
        lambda _cur, **kwargs: calls.append(("freeze", kwargs))
        or ("INSERTED", risk_id),
    )
    monkeypatch.setattr(
        runtime, "record_paper_pre_entry_shadow_gate_fail_open_cursor",
        lambda _cur, **kwargs: calls.append(("shadow", kwargs))
        or runtime.ShadowGateResult("INSERTED"),
    )
    monkeypatch.setattr(
        execution, "capture_entry_opportunity_snapshot_fail_open_cursor",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setenv("DEPLOYMENT_ID", "local-paper")
    monkeypatch.setenv("GIT_SHA", REVISION)

    result = execution.create_simulated_order_cursor(
        cursor, symbol="BTCUSDC", interval="1m", strategy="RSI",
        side="BUY", price=Decimal("100"), quantity=Decimal("0.25"),
        reason="FINAL_ALLOW", candle_open_time=NOW, is_exit=False,
        market_regime="TREND",
        regime_source_provenance={
            "regime_attribution_version": "CANONICAL_REGIME_ATTRIBUTION_V1",
            "regime_source": "market_regime",
            "regime_source_symbol": "BTCUSDC",
            "regime_source_interval": "1m",
            "regime_source_ts": NOW.isoformat(),
        },
    )
    assert result == 42
    assert cursor.insert_params[5] == Decimal("0.25")
    assert cursor.insert_params[-2] == decision_id
    assert calls[0][0] == "reservation"
    assert calls[0][1]["requested_notional"] == Decimal("25.00")
    assert [name for name, _ in calls] == [
        "reservation", "boundary", "freeze", "shadow",
    ]
    assert calls[-1][1]["pre_entry_risk_id"] == risk_id


def test_live_simulated_audit_order_skips_paper_reservation_wiring(monkeypatch):
    import common.simulated_execution_evidence as execution

    class Cursor:
        connection = object()

        def __init__(self):
            self.next_row = None

        def execute(self, query, params=None):
            if "INSERT INTO simulated_orders" in query:
                self.next_row = (43,)

        def fetchone(self):
            row, self.next_row = self.next_row, None
            return row

    monkeypatch.setattr(
        execution, "detect_simulated_order_namespace",
        lambda _conn: SimpleNamespace(
            is_legacy=False, is_namespace_v1=True, issues=(),
        ),
    )
    monkeypatch.setattr(
        execution, "accept_paper_simulated_order_cursor",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("LIVE must not invoke PAPER reservation wiring")
        ),
    )
    monkeypatch.setattr(
        execution, "capture_entry_opportunity_snapshot_fail_open_cursor",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setenv("TRADING_MODE", "LIVE")
    monkeypatch.setenv("DEPLOYMENT_ID", "local-live")

    assert execution.create_simulated_order_cursor(
        Cursor(), symbol="SOLUSDC", interval="5m", strategy="SUPERTREND",
        side="BUY", price=Decimal("100"), quantity=Decimal("0.25"),
        reason="LIVE_AUDIT", candle_open_time=NOW, is_exit=False,
        market_regime=None, regime_source_provenance=None,
    ) == 43


def test_paper_reservation_rejects_live_deployment_identity(monkeypatch):
    import common.simulated_execution_evidence as execution

    class Cursor:
        connection = object()

        def __init__(self):
            self.next_row = None

        def execute(self, query, params=None):
            if "INSERT INTO simulated_orders" in query:
                self.next_row = (44,)
            elif "to_regclass('public.capital_reservation_event_v1')" in query:
                self.next_row = (
                    "capital_reservation_event_v1",
                    "v_capital_reservation_current_v1",
                )

        def fetchone(self):
            row, self.next_row = self.next_row, None
            return row

    monkeypatch.setattr(
        execution, "detect_simulated_order_namespace",
        lambda _conn: SimpleNamespace(
            is_legacy=False, is_namespace_v1=True, issues=(),
        ),
    )
    monkeypatch.setattr(
        execution, "capture_entry_opportunity_snapshot_fail_open_cursor",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    monkeypatch.setenv("DEPLOYMENT_ID", "local-live")

    with pytest.raises(ValueError, match="CAPITAL_RESERVATION_PAPER_DEPLOYMENT_INVALID"):
        execution.create_simulated_order_cursor(
            Cursor(), symbol="SOLUSDC", interval="5m", strategy="SUPERTREND",
            side="BUY", price=Decimal("100"), quantity=Decimal("0.25"),
            reason="PAPER_ENTRY", candle_open_time=NOW, is_exit=False,
            market_regime=None, regime_source_provenance=None,
        )


def _live_bundle(account="b" * 64):
    capital = LiveManagedCapitalEvidence(
        managed_equity=Decimal("99"), managed_equity_status="CANONICAL",
        raw_usdc_avail_bal=Decimal("80"), available_capital=Decimal("80"),
        available_capital_status="CANONICAL", reserved_capital=Decimal("1"),
        reserved_capital_status="CANONICAL",
        flow_adjusted_equity=Decimal("97"),
        cumulative_flow_in_usdc=Decimal("3"),
        cumulative_flow_out_usdc=Decimal("1"),
        inventory_reconciliation_status="CANONICAL",
        balance_observed_at=NOW, mark_oldest_at=NOW,
        incomplete_reasons=(), flow_history_status="CANONICAL",
        flow_sync_through=NOW,
    )
    baseline = LiveManagedCapitalBaseline(
        accepted_at=NOW, account_identity_fingerprint=account,
        baseline_managed_equity=Decimal("100"),
        activation_fingerprint="d" * 64,
    )
    context = LiveManagedCapitalReadContext(
        snapshot=RawOkxAccountSnapshot(account, NOW, ()),
        marks={}, inventory_quantities={}, inventory_limits={},
    )
    return capital, baseline, Decimal("100"), context


def _live_history(**changes):
    value = LiveDrawdownHistory(
        current_managed_equity=Decimal("99"),
        current_flow_adjusted_equity=Decimal("97"),
        peak_flow_adjusted_equity=Decimal("110"),
        current_drawdown_abs=Decimal("-13"),
        current_drawdown_pct=Decimal("-11.81818181818181818181818182"),
        max_drawdown_abs=Decimal("-15"),
        max_drawdown_pct=Decimal("-13.63636363636363636363636364"),
        recovery_status="IN_DRAWDOWN", peak_timestamp=NOW,
        drawdown_start=NOW, recovery_timestamp=None,
        drawdown_duration=timedelta(hours=4, microseconds=7),
        history_status="CANONICAL",
        latest_observation_at=NOW,
    )
    return replace(value, **changes)


def test_live_adapter_uses_canonical_authorities_and_missing_policy_is_advisory(monkeypatch):
    account = "b" * 64
    seen = {}
    monkeypatch.setattr(
        runtime, "load_committed_pre_entry_risk_evidence_cursor",
        lambda _cur, **kwargs: seen.update(kwargs) or CommittedPreEntryRiskEvidence(
            Decimal("0.75"), 1, "CANONICAL"
        ),
    )
    adapted = runtime.load_canonical_risk_budget_inputs_cursor(
        object(), deployment_id="local-live", as_of=NOW,
        runtime_revision=REVISION, exchange_client=object(),
        live_managed_loader=lambda *args, **kwargs: _live_bundle(account),
        live_drawdown_reader=lambda *args, **kwargs: _live_history(),
        owner_flow_loader=lambda *args, **kwargs: OwnerFlowHistoryAuthority(
            Decimal("3"), Decimal("1"), NOW, "CANONICAL", "run-1"
        ),
        portfolio_state_reader=lambda *args, **kwargs: State(),
    )
    snapshot = evaluate_state(adapted, missing_numeric_policy_evidence())
    assert adapted.environment == "LIVE"
    assert adapted.account_identity_fingerprint == account
    assert adapted.current_drawdown_abs == Decimal("-13")
    assert adapted.max_drawdown_abs == Decimal("-15")
    assert adapted.drawdown_history_status == "CANONICAL"
    assert adapted.open_risk == Decimal("2.25")
    assert adapted.pre_entry_committed_risk == Decimal("0.75")
    assert snapshot.used_risk == Decimal("3.00")
    assert snapshot.authority_status == "MISSING_POLICY"
    assert snapshot.total_risk_capacity is None
    assert snapshot.available_risk_capacity is None
    assert seen["environment"] == "LIVE"
    assert set(adapted.source_fingerprints) == {
        "portfolio_state", "open_risk", "pre_entry_risk", "drawdown_history",
    }


def test_live_adapter_fails_closed_for_stale_and_identity_mismatch(monkeypatch):
    monkeypatch.setattr(
        runtime, "load_committed_pre_entry_risk_evidence_cursor",
        lambda *args, **kwargs: CommittedPreEntryRiskEvidence(
            Decimal("0"), 0, "CANONICAL"
        ),
    )
    bundle = _live_bundle("b" * 64)
    mismatched_context = LiveManagedCapitalReadContext(
        snapshot=RawOkxAccountSnapshot("c" * 64, NOW, ()),
        marks={}, inventory_quantities={}, inventory_limits={},
    )
    adapted = runtime.load_canonical_risk_budget_inputs_cursor(
        object(), deployment_id="local-live", as_of=NOW,
        exchange_client=object(),
        live_managed_loader=lambda *args, **kwargs: (*bundle[:3], mismatched_context),
        live_drawdown_reader=lambda *args, **kwargs: _live_history(
            history_status="STALE_HISTORY"
        ),
        owner_flow_loader=lambda *args, **kwargs: OwnerFlowHistoryAuthority(
            None, None, NOW, "STALE_SYNC", "run-1"
        ),
        portfolio_state_reader=lambda *args, **kwargs: State(),
    )
    snapshot = evaluate_state(adapted, missing_numeric_policy_evidence())
    assert adapted.identity_status == "ACCOUNT_IDENTITY_MISMATCH"
    assert adapted.freshness_status == "STALE_AUTHORITY"
    assert snapshot.authority_status == "ACCOUNT_IDENTITY_MISMATCH"
    assert snapshot.total_risk_capacity is None


def test_live_adapter_fails_closed_for_noncanonical_owner_flow(monkeypatch):
    monkeypatch.setattr(
        runtime, "load_committed_pre_entry_risk_evidence_cursor",
        lambda *args, **kwargs: CommittedPreEntryRiskEvidence(
            Decimal("0"), 0, "CANONICAL"
        ),
    )
    adapted = runtime.load_canonical_risk_budget_inputs_cursor(
        object(), deployment_id="local-live", as_of=NOW,
        exchange_client=object(),
        live_managed_loader=lambda *args, **kwargs: _live_bundle(),
        live_drawdown_reader=lambda *args, **kwargs: _live_history(),
        owner_flow_loader=lambda *args, **kwargs: OwnerFlowHistoryAuthority(
            None, None, NOW, "INCOMPLETE_CAPITAL_FLOW", "run-1"
        ),
        portfolio_state_reader=lambda *args, **kwargs: State(),
    )
    snapshot = evaluate_state(adapted, missing_numeric_policy_evidence())
    assert adapted.drawdown_history_status == "INCOMPLETE_CAPITAL_FLOW"
    assert snapshot.authority_status == "INCOMPLETE_DRAWDOWN_HISTORY"
    assert snapshot.available_risk_capacity is None


def test_all_live_incomplete_truth_states_block_advisory_without_capacity(monkeypatch):
    monkeypatch.setattr(
        runtime, "load_committed_pre_entry_risk_evidence_cursor",
        lambda *args, **kwargs: CommittedPreEntryRiskEvidence(
            Decimal("0"), 0, "CANONICAL"
        ),
    )
    canonical = runtime.load_canonical_risk_budget_inputs_cursor(
        object(), deployment_id="local-live", as_of=NOW,
        exchange_client=object(),
        live_managed_loader=lambda *args, **kwargs: _live_bundle(),
        live_drawdown_reader=lambda *args, **kwargs: _live_history(),
        owner_flow_loader=lambda *args, **kwargs: OwnerFlowHistoryAuthority(
            Decimal("3"), Decimal("1"), NOW, "CANONICAL", "run-1"
        ),
        portfolio_state_reader=lambda *args, **kwargs: State(),
    )
    cases = (
        (replace(canonical, total_capital_status="INCOMPLETE"), "INCOMPLETE_PORTFOLIO_STATE"),
        (replace(canonical, drawdown_history_status="INCOMPLETE"), "INCOMPLETE_DRAWDOWN_HISTORY"),
        (replace(canonical, open_risk_status="INCOMPLETE"), "INCOMPLETE_OPEN_RISK"),
        (replace(canonical, pre_entry_risk_status="INCOMPLETE"), "INCOMPLETE_PRE_ENTRY_RISK"),
        (replace(canonical, identity_status="MISMATCH"), "ACCOUNT_IDENTITY_MISMATCH"),
        (replace(canonical, source_fingerprint_status="MISMATCH"), "SOURCE_FINGERPRINT_MISMATCH"),
        (replace(canonical, freshness_status="STALE"), "STALE_AUTHORITY"),
    )
    for inputs, expected in cases:
        snapshot = evaluate_state(inputs, missing_numeric_policy_evidence())
        assert snapshot.authority_status == expected
        assert snapshot.total_risk_capacity is None
        assert snapshot.available_risk_capacity is None


def test_live_shadow_fail_open_isolated_by_savepoint(monkeypatch):
    calls = []

    class Cursor:
        def execute(self, query, params=None):
            calls.append(query)

    monkeypatch.setattr(
        runtime, "record_pre_entry_shadow_gate_cursor",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("evidence only")),
    )
    result = runtime.record_live_pre_entry_shadow_gate_fail_open_cursor(
        Cursor(), pre_entry_risk_id=uuid.uuid4(), deployment_id="local-live",
        as_of=NOW, git_revision=REVISION, exchange_client=object(),
    )
    assert result.status == "EVIDENCE_FAILURE_EXECUTION_UNCHANGED"
    assert calls == [
        "SAVEPOINT risk_budget_live_shadow_gate_v1",
        "ROLLBACK TO SAVEPOINT risk_budget_live_shadow_gate_v1",
        "RELEASE SAVEPOINT risk_budget_live_shadow_gate_v1",
    ]
    assert runtime.RISK_BUDGET_EXECUTION_INFLUENCE is False

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
import uuid

from common.capital_reservation import paper_account_identity_fingerprint
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
    dockerfile = (root / "automation_runner/Dockerfile").read_text()
    assert "run_paper_risk_budget_state_evaluation_cycle()" in automation
    assert "record_paper_pre_entry_shadow_gate_fail_open_cursor" in execution
    assert "execution_effect=NONE" in execution
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

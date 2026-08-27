from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import uuid

from common.joint_authority_epoch import AuthorityEpoch, RiskBudgetEpochBoundary
from common.pre_entry_risk import CommittedPreEntryRiskEvidence
from common.risk_budget import PersistResult, RiskBudgetInputs
import common.risk_budget_runtime as runtime


NOW = datetime(2026, 8, 27, 20, 45, tzinfo=timezone.utc)
REVISION = "a" * 40
IDENTITY = "b" * 64


def inputs(**changes):
    value = RiskBudgetInputs(
        environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint=IDENTITY, as_of=NOW,
        total_capital=Decimal("100"), total_capital_status="CANONICAL",
        open_risk=Decimal("2"), open_risk_status="CANONICAL",
        pre_entry_committed_risk=Decimal("1"),
        pre_entry_risk_status="CANONICAL",
        current_drawdown_abs=Decimal("-5"),
        current_drawdown_pct=Decimal("-5"),
        max_drawdown_abs=Decimal("-10"), max_drawdown_pct=Decimal("-10"),
        recovery_status="IN_DRAWDOWN", drawdown_history_status="CANONICAL",
        source_fingerprints={
            "portfolio_state": "1" * 64, "open_risk": "2" * 64,
            "pre_entry_risk": "3" * 64, "drawdown_history": "4" * 64,
        },
    )
    return replace(value, **changes)


def epoch(identity=1, generation=1):
    return AuthorityEpoch(
        identity, "local-paper", 1, "1" * 64, 1, NOW, "2" * 64,
        1, generation, "3" * 64, NOW, NOW, "4" * 64, "5" * 64,
    )


def boundary(identity=1, generation=1, observation="6" * 64):
    return RiskBudgetEpochBoundary(
        "CANONICAL", NOW, epoch(identity, generation), observation,
    )


class ExistingCursor:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def execute(self, query, params=None):
        self.queries.append((query, params))

    def fetchall(self):
        return list(self.rows)


def test_retry_reuses_frozen_accepted_event_without_rematerializing_mutable_state(
    monkeypatch,
):
    event_id = uuid.uuid4()
    cursor = ExistingCursor([(event_id, "7" * 64, "MISSING_POLICY")])
    monkeypatch.setattr(runtime, "risk_budget_schema_available_cursor", lambda _cur: True)

    def mutable_loader(*_args, **_kwargs):
        raise AssertionError("historical mutable tables must not be rematerialized")

    replay = runtime.persist_state_evaluation_cursor(
        cursor, deployment_id="local-paper", boundary=NOW, as_of=NOW,
        git_revision="f" * 40, input_loader=mutable_loader,
        paper_epoch_boundary=boundary(),
    )
    assert replay.status == "IDEMPOTENT"
    assert replay.authority_status == "MISSING_POLICY"
    assert replay.persisted == PersistResult("IDEMPOTENT", event_id, "7" * 64)
    assert "risk_budget_authority_epoch_binding_v1" in cursor.queries[0][0]


def test_new_generation_versions_same_boundary_identity(monkeypatch):
    cursor = ExistingCursor([])
    identities = []
    monkeypatch.setattr(runtime, "risk_budget_schema_available_cursor", lambda _cur: True)
    monkeypatch.setattr(
        runtime, "persist_event_cursor",
        lambda _cur, _snapshot, **kwargs: identities.append(
            kwargs["event_identity"]
        ) or PersistResult("INSERTED", uuid.uuid4(), "8" * 64),
    )
    monkeypatch.setattr(runtime, "bind_risk_budget_event_cursor", lambda *_a, **_k: "INSERTED")

    for upstream in (boundary(1, 1, "6" * 64), boundary(2, 2, "9" * 64)):
        result = runtime.persist_state_evaluation_cursor(
            cursor, deployment_id="local-paper", boundary=NOW, as_of=NOW,
            git_revision=REVISION, input_loader=lambda *_a, **_k: inputs(),
            paper_epoch_boundary=upstream,
        )
        assert result.status == "INSERTED"

    assert identities[0] != identities[1]
    assert identities[0].startswith("JOINT_AUTHORITY_EPOCH_V2:1:1:")
    assert identities[1].startswith("JOINT_AUTHORITY_EPOCH_V2:2:2:")


def test_runtime_revision_is_excluded_from_portfolio_semantic_fingerprint(monkeypatch):
    monkeypatch.setattr(
        runtime, "load_committed_pre_entry_risk_evidence_cursor",
        lambda *_a, **_k: CommittedPreEntryRiskEvidence(
            Decimal("1"), 1, "CANONICAL"
        ),
    )

    class State:
        as_of = NOW
        open_risk = Decimal("2")
        open_risk_status = "CANONICAL"

        def __init__(self, revision):
            self.runtime_revision = revision

        def serializable(self):
            return {
                "environment": "PAPER", "deployment_id": "local-paper",
                "as_of": NOW.isoformat(), "runtime_revision": self.runtime_revision,
                "total_capital": "100", "open_risk": "2",
            }

    first = runtime._source_fingerprints(
        State("a" * 40), State("a" * 40).serializable(),
        CommittedPreEntryRiskEvidence(Decimal("1"), 1, "CANONICAL"),
        None, {"history": "frozen"},
    )
    second = runtime._source_fingerprints(
        State("b" * 40), State("b" * 40).serializable(),
        CommittedPreEntryRiskEvidence(Decimal("1"), 1, "CANONICAL"),
        None, {"history": "frozen"},
    )
    assert first == second
    assert runtime.RISK_BUDGET_EXECUTION_INFLUENCE is False


class Connection:
    def __init__(self):
        self.rollbacks = 0
        self.commits = 0
        self.autocommit = True

    def cursor(self):
        class Context:
            def __enter__(self):
                return object()

            def __exit__(self, *_args):
                return False

        return Context()

    def rollback(self):
        self.rollbacks += 1

    def commit(self):
        self.commits += 1

    def close(self):
        pass


def test_true_runtime_conflict_fails_closed_and_cutoff_is_not_retried_forever(
    monkeypatch,
):
    from common.risk_budget import RiskBudgetIdempotencyConflict

    connection = Connection()
    upstream = boundary()
    calls = []
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    monkeypatch.setenv("DEPLOYMENT_ID", "local-paper")
    monkeypatch.setenv("GIT_SHA", REVISION)
    monkeypatch.setattr(runtime, "get_db_conn", lambda: connection)
    monkeypatch.setattr(
        runtime, "resolve_risk_budget_boundary_cursor",
        lambda *_a, **_k: upstream,
    )

    def conflict(*_args, **_kwargs):
        calls.append("attempt")
        raise RiskBudgetIdempotencyConflict(
            "RISK_BUDGET_EVENT_IDEMPOTENCY_CONFLICT"
        )

    monkeypatch.setattr(runtime, "persist_state_evaluation_cursor", conflict)
    monkeypatch.setattr(runtime, "_last_state_evaluation_cutoff", None)
    first = runtime.run_risk_budget_state_evaluation_cycle(now=NOW)
    second = runtime.run_risk_budget_state_evaluation_cycle(now=NOW)
    assert first.status == "CONFLICT"
    assert second.status == "ALREADY_ATTEMPTED_FOR_CUTOFF"
    assert calls == ["attempt"]
    assert connection.commits == 0
    assert connection.rollbacks >= 2

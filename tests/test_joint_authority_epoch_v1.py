from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import uuid

import pytest

from common.joint_authority_epoch import (
    AuthorityEpoch,
    JointAuthorityError,
    activation_attempt_identity,
    bind_risk_budget_event_cursor,
    calibration_eligible,
    ceil_15,
    drawdown_activation_boundary,
    fingerprint,
    is_canonical_boundary,
    resolve_risk_budget_boundary_cursor,
    validate_ordering,
    activate_drawdown_epoch_cursor,
)
from common.risk_budget import PersistResult, RiskBudgetInputs
import common.risk_budget_runtime as risk_runtime


ROOT = Path(__file__).resolve().parents[1]
UTC = timezone.utc
R = datetime(2026, 8, 25, 16, 1, 3, tzinfo=UTC)


def epoch() -> AuthorityEpoch:
    return AuthorityEpoch(
        9, "local-paper", 1, "b" * 64, 4, R, "r" * 64, 11, 3,
        "d" * 64, datetime(2026, 8, 25, 16, 15, tzinfo=UTC),
        datetime(2026, 8, 25, 16, 30, tzinfo=UTC), "e" * 64, "s" * 64,
    )


@pytest.mark.parametrize(
    ("replay", "expected"),
    [
        (datetime(2026, 8, 25, 16, 0, tzinfo=UTC), datetime(2026, 8, 25, 16, 0, tzinfo=UTC)),
        (datetime(2026, 8, 25, 16, 0, 1, tzinfo=UTC), datetime(2026, 8, 25, 16, 15, tzinfo=UTC)),
        (datetime(2026, 8, 25, 16, 14, 59, tzinfo=UTC), datetime(2026, 8, 25, 16, 15, tzinfo=UTC)),
    ],
)
def test_replay_to_drawdown_temporal_boundaries(replay, expected):
    assert ceil_15(replay) == expected
    assert drawdown_activation_boundary(replay) == expected
    assert is_canonical_boundary(expected)


def test_delayed_recovery_uses_strictly_future_boundary_and_never_backfills():
    recovery = datetime(2026, 8, 25, 17, 0, tzinfo=UTC)
    assert drawdown_activation_boundary(
        R, operational_recovery_at=recovery,
    ) == datetime(2026, 8, 25, 17, 15, tzinfo=UTC)
    between = recovery + timedelta(seconds=1)
    assert drawdown_activation_boundary(
        R, operational_recovery_at=between,
    ) == datetime(2026, 8, 25, 17, 15, tzinfo=UTC)


def test_vps_incident_shape_never_requires_pre_replay_1600_observation():
    d = drawdown_activation_boundary(R)
    c1 = d + timedelta(minutes=15)
    assert d == datetime(2026, 8, 25, 16, 15, tzinfo=UTC)
    assert c1 == datetime(2026, 8, 25, 16, 30, tzinfo=UTC)
    assert datetime(2026, 8, 25, 16, 0, tzinfo=UTC) < R


def test_ordering_rejects_historical_activation_q0_before_c1_and_dataset_before_q0():
    valid = epoch()
    validate_ordering(
        baseline_at=R - timedelta(days=1), replay_at=R,
        drawdown_at=valid.drawdown_activation_boundary,
        first_cadence_at=valid.first_required_cadence,
        q0_at=valid.first_required_cadence,
        calibration_dataset_as_of=valid.first_required_cadence,
    )
    with pytest.raises(JointAuthorityError, match="ORDERING_INVALID"):
        validate_ordering(
            baseline_at=R - timedelta(days=1), replay_at=R,
            drawdown_at=datetime(2026, 8, 25, 16, 0, tzinfo=UTC),
            first_cadence_at=datetime(2026, 8, 25, 16, 15, tzinfo=UTC),
        )
    with pytest.raises(JointAuthorityError, match="Q0_BEFORE_C1"):
        validate_ordering(
            baseline_at=R - timedelta(days=1), replay_at=R,
            drawdown_at=valid.drawdown_activation_boundary,
            first_cadence_at=valid.first_required_cadence, q0_at=R,
        )
    with pytest.raises(JointAuthorityError, match="DATASET_BEFORE_Q0"):
        validate_ordering(
            baseline_at=R - timedelta(days=1), replay_at=R,
            drawdown_at=valid.drawdown_activation_boundary,
            first_cadence_at=valid.first_required_cadence,
            q0_at=valid.first_required_cadence,
            calibration_dataset_as_of=valid.drawdown_activation_boundary,
        )


def test_attempt_identity_is_restart_deterministic_but_retry_is_new():
    kwargs = dict(
        deployment_id="local-paper", requested_boundary=epoch().drawdown_activation_boundary,
        prepared_at=datetime(2026, 8, 25, 16, 2, tzinfo=UTC),
        source_fingerprints={"baseline": "b" * 64}, status="FAILED",
        failure_reason="SEED_INCOMPLETE",
    )
    first = activation_attempt_identity(**kwargs)
    assert first == activation_attempt_identity(**kwargs)
    retry = activation_attempt_identity(
        **{**kwargs, "prepared_at": kwargs["prepared_at"] + timedelta(seconds=1)}
    )
    assert retry != first


class BoundaryCursor:
    def __init__(self, *, observation=True, heads=1):
        self.observation = observation
        self.heads = heads
        self.query = ""

    def execute(self, query, params=None):
        self.query = query

    def fetchone(self):
        if "to_regclass" in self.query:
            return ("joint_authority_epoch_v1",)
        if "paper_managed_equity_observation_v1" in self.query:
            return (
                (datetime(2026, 8, 25, 16, 30, tzinfo=UTC), "o" * 64)
                if self.observation else None
            )
        raise AssertionError(self.query)

    def fetchall(self):
        if "joint_authority_epoch_selection_v1" not in self.query:
            raise AssertionError(self.query)
        row = (
            9, "local-paper", 1, "b" * 64, 4, R, "r" * 64, 11, 3,
            "d" * 64, datetime(2026, 8, 25, 16, 15, tzinfo=UTC),
            datetime(2026, 8, 25, 16, 30, tzinfo=UTC), "e" * 64, "s" * 64,
        )
        return [row] * self.heads


def test_risk_budget_waits_before_c1_then_consumes_upstream_boundary_not_scheduler():
    waiting = resolve_risk_budget_boundary_cursor(
        BoundaryCursor(), deployment_id="local-paper",
        scheduler_time=datetime(2026, 8, 25, 16, 29, 59, tzinfo=UTC),
    )
    assert waiting.status == "EXPECTED_WAITING_FOR_FIRST_DRAWDOWN_CADENCE"
    assert waiting.as_of is None
    ready = resolve_risk_budget_boundary_cursor(
        BoundaryCursor(), deployment_id="local-paper",
        scheduler_time=datetime(2026, 8, 25, 16, 37, 44, tzinfo=UTC),
    )
    assert ready.status == "CANONICAL"
    assert ready.as_of == datetime(2026, 8, 25, 16, 30, tzinfo=UTC)
    assert ready.as_of != datetime(2026, 8, 25, 16, 35, tzinfo=UTC)


def test_missing_cadence_waits_and_ambiguous_epoch_fails_closed():
    missing = resolve_risk_budget_boundary_cursor(
        BoundaryCursor(observation=False), deployment_id="local-paper",
        scheduler_time=datetime(2026, 8, 25, 16, 40, tzinfo=UTC),
    )
    assert missing.status == "EXPECTED_WAITING_FOR_FIRST_DRAWDOWN_CADENCE"
    with pytest.raises(JointAuthorityError, match="AMBIGUOUS"):
        resolve_risk_budget_boundary_cursor(
            BoundaryCursor(heads=2), deployment_id="local-paper",
            scheduler_time=datetime(2026, 8, 25, 16, 40, tzinfo=UTC),
        )


def test_q0_eligibility_is_same_epoch_and_never_before_c1():
    current = epoch()
    assert not calibration_eligible(
        epoch=current, evaluation_as_of=current.first_required_cadence - timedelta(microseconds=1)
    )
    assert calibration_eligible(
        epoch=current, evaluation_as_of=current.first_required_cadence
    )
    other = AuthorityEpoch(**{
        **current.__dict__, "authority_epoch_id": 10, "epoch_fingerprint": "x" * 64,
    })
    assert other.authority_epoch_id != current.authority_epoch_id


class BindingCursor:
    def __init__(self):
        self.inserted = False
        self.value = None
        self.query = ""

    def execute(self, query, params=None):
        self.query = query
        if query.lstrip().startswith("INSERT"):
            self.value = params[-1]

    def fetchone(self):
        if self.query.lstrip().startswith("INSERT") and not self.inserted:
            self.inserted = True
            return (self.value,)
        if self.query.lstrip().startswith("SELECT binding_fingerprint"):
            return (self.value,)
        return None


def test_q0_binding_is_byte_identical_across_restart():
    cursor = BindingCursor()
    event_id = uuid.uuid4()
    kwargs = dict(
        event_id=event_id, epoch=epoch(),
        evaluation_as_of=epoch().first_required_cadence,
        risk_budget_source_fingerprint="q" * 64,
    )
    assert bind_risk_budget_event_cursor(cursor, **kwargs) == "INSERTED"
    assert bind_risk_budget_event_cursor(cursor, **kwargs) == "IDEMPOTENT"


def risk_inputs(*, complete=True):
    return RiskBudgetInputs(
        environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint="a" * 64,
        as_of=epoch().first_required_cadence,
        total_capital=100 if complete else None,
        total_capital_status="CANONICAL" if complete else "INCOMPLETE",
        open_risk=1, open_risk_status="CANONICAL",
        pre_entry_committed_risk=0, pre_entry_risk_status="CANONICAL",
        current_drawdown_abs=-1, current_drawdown_pct=-1,
        max_drawdown_abs=-2, max_drawdown_pct=-2,
        recovery_status="IN_DRAWDOWN", drawdown_history_status="CANONICAL",
        source_fingerprints={
            "portfolio_state": "1" * 64, "open_risk": "2" * 64,
            "pre_entry_risk": "3" * 64, "drawdown_history": "4" * 64,
        },
    )


def test_first_complete_state_event_becomes_q0_but_incomplete_truth_does_not(monkeypatch):
    boundary = risk_runtime.StateEvaluationResult
    upstream = type("Upstream", (), {
        "epoch": epoch(), "as_of": epoch().first_required_cadence,
    })()
    persisted = PersistResult("INSERTED", uuid.uuid4(), "9" * 64)
    calls = []
    monkeypatch.setattr(risk_runtime, "risk_budget_schema_available_cursor", lambda cur: True)
    monkeypatch.setattr(risk_runtime, "persist_event_cursor", lambda *a, **k: persisted)
    monkeypatch.setattr(
        risk_runtime, "bind_risk_budget_event_cursor",
        lambda *a, **k: calls.append(k) or "INSERTED",
    )
    complete = risk_runtime.persist_state_evaluation_cursor(
        object(), deployment_id="local-paper",
        boundary=epoch().first_required_cadence,
        as_of=epoch().first_required_cadence, git_revision="a" * 40,
        input_loader=lambda *a, **k: risk_inputs(), paper_epoch_boundary=upstream,
    )
    assert isinstance(complete, boundary)
    assert complete.authority_status == "MISSING_POLICY"
    assert len(calls) == 1
    calls.clear()
    incomplete = risk_runtime.persist_state_evaluation_cursor(
        object(), deployment_id="local-paper",
        boundary=epoch().first_required_cadence,
        as_of=epoch().first_required_cadence, git_revision="a" * 40,
        input_loader=lambda *a, **k: risk_inputs(complete=False),
        paper_epoch_boundary=upstream,
    )
    assert incomplete.status == "INCOMPLETE_PORTFOLIO_STATE"
    assert incomplete.persisted is None
    assert calls == []


class ActivationCursor:
    def __init__(self):
        self.commands = []
        self.query = ""

    def execute(self, query, params=None):
        self.query = query
        self.commands.append(query.strip().splitlines()[0])

    def fetchone(self):
        if "RETURNING authority_epoch_id" in self.query:
            return (9,)
        return None

    def fetchall(self):
        return []


def patch_activation_sources(monkeypatch, *, seed_status="CANONICAL"):
    from types import SimpleNamespace
    import common.paper_drawdown_history as drawdown
    import common.paper_equity_baseline_v2 as baseline_module
    import common.paper_portfolio_replay_cutover as replay_module

    baseline = SimpleNamespace(
        baseline_id=1, deployment_id="local-paper", evidence_status="COMPLETE",
        activation_fingerprint="b" * 64,
        baseline_timestamp=R - timedelta(days=10),
    )
    replay = SimpleNamespace(
        cutover_id=2, cutover_at=R, cutover_fingerprint="c" * 64,
    )
    activation = SimpleNamespace(
        activation_id=3, generation=2,
        activation_evidence_fingerprint="d" * 64,
    )
    monkeypatch.setattr(
        baseline_module, "fetch_paper_equity_baseline_v2", lambda *a, **k: baseline,
    )
    monkeypatch.setattr(
        replay_module, "load_replay_cutover_cursor", lambda *a, **k: replay,
    )
    monkeypatch.setattr(
        drawdown, "create_activation_generation_cursor", lambda *a, **k: activation,
    )
    monkeypatch.setattr(
        drawdown, "capture_observation_candidate",
        lambda *a, **k: SimpleNamespace(
            status=seed_status,
            candidate=object() if seed_status == "CANONICAL" else None,
        ),
    )
    monkeypatch.setattr(
        drawdown, "persist_observation_candidate",
        lambda *a, **k: SimpleNamespace(status="CANONICAL"),
    )


def activation_kwargs():
    return dict(
        deployment_id="local-paper",
        requested_boundary=datetime(2026, 8, 25, 16, 15, tzinfo=UTC),
        supersession_reason="UPSTREAM_REPLAY_CUTOVER_EPOCH_CHANGE",
        expected_previous_history_status="CANONICAL",
        approval_evidence={"approved": True}, producer_identity="test",
        git_revision="a" * 40, deployment_identity="test-local-paper",
        contract_versions={"joint": "JOINT_AUTHORITY_EPOCH_V1"},
        contract_fingerprints={"joint": "f" * 64},
        portfolio_state_reader=lambda *a, **k: object(),
        prepared_at=datetime(2026, 8, 25, 16, 2, tzinfo=UTC),
    )


def test_atomic_activation_selects_epoch_only_after_canonical_seed(monkeypatch):
    patch_activation_sources(monkeypatch)
    cursor = ActivationCursor()
    result = activate_drawdown_epoch_cursor(cursor, **activation_kwargs())
    assert result.status == "ACTIVATED"
    assert result.epoch.authority_epoch_id == 9
    assert any("INSERT INTO joint_authority_epoch_v1" in item for item in cursor.commands)
    assert any("INSERT INTO joint_authority_epoch_selection_v1" in item for item in cursor.commands)
    assert cursor.commands[-1] == "RELEASE SAVEPOINT joint_authority_activation_v1"


def test_seed_failure_rolls_back_generation_and_records_failed_attempt_without_epoch(monkeypatch):
    patch_activation_sources(monkeypatch, seed_status="INCOMPLETE_PORTFOLIO_STATE")
    cursor = ActivationCursor()
    result = activate_drawdown_epoch_cursor(cursor, **activation_kwargs())
    assert result.status == "FAILED"
    assert "JOINT_AUTHORITY_SEED_INCOMPLETE_PORTFOLIO_STATE" in result.failure_reason
    assert "ROLLBACK TO SAVEPOINT joint_authority_activation_v1" in cursor.commands
    assert not any("INSERT INTO joint_authority_epoch_v1" in item for item in cursor.commands)
    assert not any("INSERT INTO joint_authority_epoch_selection_v1" in item for item in cursor.commands)


def test_contract_and_migration_freeze_lifecycle_without_backfill_or_numeric_policy():
    contract_path = ROOT / "contracts/joint_authority_epoch_v1_contract.json"
    contract = json.loads(contract_path.read_text())
    assert contract["ordering_invariant"] == (
        "B <= R <= D < C1 <= Q0 <= CALIBRATION_DATASET_AS_OF"
    )
    assert contract["epoch_model"]["cross_generation_history_merge"] is False
    assert contract["risk_budget"]["independent_wall_clock_floor"] is False
    assert contract["numeric_risk_policy"] is False
    declared = (
        ROOT / "contracts/joint_authority_epoch_v1_contract.sha256"
    ).read_text().strip()
    assert hashlib.sha256(contract_path.read_bytes()).hexdigest() == declared
    migration = (
        ROOT / "db/migrations/20260825_pre_calibration_joint_authority_epoch_v1.sql"
    ).read_text().upper()
    assert "INSERT INTO PUBLIC.PAPER_MANAGED_EQUITY_OBSERVATION_V1" not in migration
    assert "UPDATE PUBLIC." not in migration
    assert "DELETE FROM PUBLIC." not in migration
    assert "DROP TABLE" not in migration
    assert "JOINT_AUTHORITY_EPOCH_V1_APPEND_ONLY" in migration
    assert hashlib.sha256(contract_path.read_bytes()).hexdigest()

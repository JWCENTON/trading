from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from pathlib import Path

import pytest

from common.decision_contract import (
    DecisionReason,
    DecisionSubtype,
    EvaluationContext,
    FinalDecision,
)
from common.decision_observation import event_from_final_decision
from common.paper_opportunity_observation import (
    CONTRACT_VERSION,
    FEATURE_FLAG,
    SUPPORTED_OUTCOME_HORIZONS,
    build_paper_opportunity_envelope,
)
from common.paper_opportunity_outcome_automation import (
    run_paper_opportunity_outcome_automation,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT / "db/migrations/20260826_full_paper_opportunity_observation_v1.sql"
).read_text()
TRANSPORT = (ROOT / "common/decision_observation_transport.py").read_text()
COMPOSE = (ROOT / "docker-compose.paper.override.yaml").read_text()
MIGRATION_SHA = (
    ROOT / "db/migrations/20260826_full_paper_opportunity_observation_v1.sha256"
).read_text().strip()
CONTRACT_PATH = ROOT / "contracts/full_paper_opportunity_observation_v1_contract.json"
CONTRACT_SHA = (
    ROOT / "contracts/full_paper_opportunity_observation_v1_contract.sha256"
).read_text().strip()


def env(**extra):
    return {
        FEATURE_FLAG: "1",
        "TRADING_MODE": "PAPER",
        "PAPER_SIMULATION_FEE_RATE": "0.0035",
        "GIT_SHA": "a" * 40,
        **extra,
    }


def context(*, deployment="local-paper", environment="trading_paper", strategy="RSI"):
    at = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    return EvaluationContext(
        deployment_id=deployment,
        environment=environment,
        symbol="BTCUSDC",
        interval="1m",
        strategy=strategy,
        candle_open_time=at,
        evaluation_started_at=at + timedelta(seconds=2),
        engine_name=strategy,
        engine_version="test",
        runtime_enabled=True,
        live_orders_enabled=False,
        paper_mode=environment == "trading_paper",
    )


def no_signal(ctx=None, *, finished_seconds=5):
    return FinalDecision.no_trade(
        ctx or context(),
        DecisionReason.NO_SIGNAL,
        finished_at=datetime(2026, 8, 26, 12, 0, finished_seconds, tzinfo=timezone.utc),
        reference_price=Decimal("50000"),
        reason_text="NO_SIGNAL",
    )


def test_paper_observation_is_explicitly_paper_only_and_cost_canonical():
    item = build_paper_opportunity_envelope(no_signal(), environ=env())
    assert item is not None
    assert item.contract_version == CONTRACT_VERSION
    assert item.observation_type == "NO_SIGNAL"
    assert item.opportunity_direction == "LONG"
    assert item.fee_model_version == "PAPER_SIMULATOR_FINANCIAL_MODEL_V2"
    assert item.full_cost_hurdle_pct == Decimal("0.7024586051179126944305067737")
    live = replace(
        no_signal(),
        evaluation=context(deployment="local-live", environment="trading_live"),
    )
    assert build_paper_opportunity_envelope(live, environ=env(TRADING_MODE="LIVE")) is None
    assert build_paper_opportunity_envelope(no_signal(), environ=env(**{FEATURE_FLAG: "0"})) is None


def test_observation_identity_deduplicates_same_logical_candle():
    first = build_paper_opportunity_envelope(no_signal(finished_seconds=5), environ=env())
    second = build_paper_opportunity_envelope(no_signal(finished_seconds=50), environ=env())
    assert first.observation_key == second.observation_key


@pytest.mark.parametrize(
    ("decision", "expected"),
    [
        (
            lambda: FinalDecision.paper_simulation(
                context(), DecisionReason.SSOT_EXECUTE_AND_RECORD,
                finished_at=datetime(2026, 8, 26, 12, 0, 5, tzinfo=timezone.utc),
                reference_price=Decimal("50000"), side="BUY",
            ),
            "EXECUTED",
        ),
        (
            lambda: FinalDecision.signal_rejected(
                context(), DecisionReason.UNKNOWN,
                finished_at=datetime(2026, 8, 26, 12, 0, 5, tzinfo=timezone.utc),
                reference_price=Decimal("50000"), side="BUY", reason_text="ATR_TOO_LOW",
            ),
            "SIGNAL_REJECTED",
        ),
        (
            lambda: FinalDecision.entry_blocked(
                context(), DecisionReason.POLICY_BLOCK, DecisionSubtype.READINESS_BLOCKED,
                finished_at=datetime(2026, 8, 26, 12, 0, 5, tzinfo=timezone.utc),
                reference_price=Decimal("50000"), side="BUY",
            ),
            "POLICY_BLOCK",
        ),
        (
            lambda: FinalDecision.position_hold(
                context(), DecisionReason.POSITION_HOLD,
                finished_at=datetime(2026, 8, 26, 12, 0, 5, tzinfo=timezone.utc),
                reference_price=Decimal("50000"),
            ),
            "ALREADY_OPEN_BLOCK",
        ),
        (
            lambda: FinalDecision.paper_simulation(
                context(strategy="SUPERTREND"), DecisionReason.SSOT_EXECUTE_AND_RECORD,
                finished_at=datetime(2026, 8, 26, 12, 0, 5, tzinfo=timezone.utc),
                reference_price=Decimal("50000"), side="BUY",
                details={"blocked_reason": "PAPER_SUPERTREND_ENTRY_CONTAINED"},
            ),
            "CONTAINMENT_BLOCK",
        ),
    ],
)
def test_required_observation_taxonomy(decision, expected):
    assert build_paper_opportunity_envelope(decision(), environ=env()).observation_type == expected


def test_bbrange_treatment_provenance_is_preserved_without_changing_decision():
    original = FinalDecision.entry_blocked(
        context(strategy="BBRANGE"), DecisionReason.POLICY_BLOCK,
        DecisionSubtype.READINESS_BLOCKED,
        finished_at=datetime(2026, 8, 26, 12, 0, 5, tzinfo=timezone.utc),
        reference_price=Decimal("50000"), side="BUY", signal_detected=True,
        details={
            "treatment_name": "BBRANGE_PAPER_TREATMENT_V1",
            "base_decision": "BUY",
            "treatment_decision": "NO_TRADE",
            "treatment_reason": "VOLUME_PRIMARY_DRIVER",
        },
    )
    item = build_paper_opportunity_envelope(
        original,
        environ=env(BBRANGE_PAPER_TREATMENT_V1_ENABLED="1"),
    )
    assert item.treatment_status == "ACTIVE"
    assert item.treatment_base_decision == "BUY"
    assert item.treatment_decision == "NO_TRADE"
    assert item.treatment_reason == "VOLUME_PRIMARY_DRIVER"
    assert original.trade_executed is False and original.order_submitted is False


def test_enriched_event_keeps_future_outcomes_out_of_decision_inputs(monkeypatch):
    monkeypatch.setenv(FEATURE_FLAG, "1")
    monkeypatch.setenv("PAPER_SIMULATION_FEE_RATE", "0.0035")
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    event = event_from_final_decision(
        no_signal(), event_id="11111111-1111-1111-1111-111111111111",
        decision_key="key", source_service="test", source_instance="test",
    )
    assert event.paper_opportunity is not None
    serialized = repr(event.paper_opportunity)
    for forbidden in ("mfe_pct", "mae_pct", "economic_label", "covered_full_costs"):
        assert forbidden not in serialized


def test_schema_is_compact_immutable_idempotent_and_has_maturity_view():
    for token in (
        "observation_key TEXT NOT NULL UNIQUE",
        "UNIQUE(observation_id,horizon_minutes)",
        "BEFORE UPDATE OR DELETE",
        "NOT_YET_MATURE",
        "EVIDENCE_INCOMPLETE",
        "ECONOMICALLY_VIABLE",
        "NEVER_COVERED_FULL_COSTS",
        "time_to_full_cost_cover_seconds",
        "time_to_mfe_seconds",
        "entry_trace_event_id",
        "entry_opportunity_snapshot_id",
    ):
        assert token.replace(" ", "") in MIGRATION.replace(" ", "")
    assert tuple(SUPPORTED_OUTCOME_HORIZONS) == (15, 30, 60, 240)
    assert "INSERT INTO positions" not in MIGRATION + TRANSPORT
    assert "INSERT INTO simulated_orders" not in MIGRATION + TRANSPORT
    assert 'FULL_PAPER_OPPORTUNITY_OBSERVATION_V1_ENABLED: "1"' in COMPOSE
    assert hashlib.sha256(SOURCE_BYTES := (
        ROOT / "db/migrations/20260826_full_paper_opportunity_observation_v1.sql"
    ).read_bytes()).hexdigest() == MIGRATION_SHA
    assert SOURCE_BYTES
    assert hashlib.sha256(CONTRACT_PATH.read_bytes()).hexdigest() == CONTRACT_SHA


class Cursor:
    def __init__(self):
        self.row = None
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params):
        normalized = " ".join(sql.split())
        self.calls.append(normalized)
        if normalized.startswith("SELECT refresh_paper_opportunity_outcomes_v1"):
            self.row = (3,)

    def fetchone(self):
        return self.row


class Connection:
    def __init__(self):
        self.cursor_value = Cursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_value

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_outcome_automation_is_paper_only_and_bounded():
    live = Connection()
    assert run_paper_opportunity_outcome_automation(
        live, environ=env(TRADING_MODE="LIVE", DEPLOYMENT_ID="local-live")
    ) is None
    assert live.cursor_value.calls == []
    paper = Connection()
    result = run_paper_opportunity_outcome_automation(
        paper, environ=env(DEPLOYMENT_ID="local-paper"), bounded_limit=200
    )
    assert result.status == "OK" and result.inserted == 3
    assert "SELECT refresh_paper_opportunity_outcomes_v1(%s,%s)" in paper.cursor_value.calls[0]
    assert paper.commits == 1

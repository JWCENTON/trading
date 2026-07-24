from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from common.decision_contract import DecisionReason, EvaluationContext, FinalDecision
from common.decision_observation_transport import (
    DurableDecisionObservationProducer, ProducerObservationStatus,
    TransportFlags, TransportMetrics,
    deterministic_decision_key,
)

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (ROOT / "db/migrations/20260717_causal_decision_observation_transport_v1.sql").read_text()
TRANSPORT = (ROOT / "common/decision_observation_transport.py").read_text()
RUNNER = (ROOT / "automation_runner/main.py").read_text()


def decision(deployment="local-paper", action="NO_TRADE"):
    ctx = EvaluationContext(
        deployment_id=deployment, environment="trading_paper", symbol="BTCUSDT",
        interval="1m", strategy="RSI", candle_open_time=datetime(2026, 7, 17, tzinfo=timezone.utc),
        evaluation_started_at=datetime(2026, 7, 17, tzinfo=timezone.utc), paper_mode=True,
        engine_name="RSI",
    )
    if action == "HOLD":
        return FinalDecision.position_hold(ctx, DecisionReason.NO_SIGNAL, finished_at=datetime.now(timezone.utc))
    if action == "EXIT":
        return FinalDecision.exit_result(ctx, DecisionReason.SSOT_EXECUTE_AND_RECORD, finished_at=datetime.now(timezone.utc))
    if action == "TRADE":
        return FinalDecision.paper_simulation(ctx, DecisionReason.SSOT_EXECUTE_AND_RECORD, finished_at=datetime.now(timezone.utc))
    return FinalDecision.no_trade(ctx, DecisionReason.NO_SIGNAL, finished_at=datetime.now(timezone.utc))


class NoConnect:
    def __init__(self): self.calls = 0
    def __call__(self): self.calls += 1; raise AssertionError("unexpected write")


def test_safe_defaults_and_disabled_producer_write_nothing():
    flags = TransportFlags()
    assert not flags.decision_observation_enabled and not flags.shadow_observation_enabled
    assert not flags.auto_apply and flags.kill_switch
    factory = NoConnect()
    original = decision()
    producer = DurableDecisionObservationProducer(factory, flags, source_service="test")
    assert producer.observe(original) is original
    assert producer.last_skip_reason == "OBSERVATION_DISABLED"
    assert factory.calls == 0


def test_real_automation_consumer_initialization_defaults_do_zero_db_operations(monkeypatch):
    for name in ("CAUSAL_DECISION_OBSERVATION_ENABLED", "CAUSAL_SHADOW_OBSERVATION_ENABLED",
                 "CAUSAL_LEARNING_AUTO_APPLY", "DEPLOYMENT_ID"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("CAUSAL_LEARNING_KILL_SWITCH", "1")
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    monkeypatch.setenv("EXCHANGE", "OKX")
    import automation_runner.main as runner
    calls = []
    monkeypatch.setattr(runner, "get_db_conn", lambda: calls.append("connect"))
    assert runner.run_causal_decision_observation_consumer() == 0
    assert calls == []


def test_kill_switch_leaves_producer_off_and_decision_unchanged():
    factory = NoConnect()
    original = decision()
    flags = TransportFlags(decision_observation_enabled=True, kill_switch=True, deployment_id="local-paper")
    producer = DurableDecisionObservationProducer(factory, flags, source_service="test")
    assert producer.observe(original) is original
    assert producer.last_skip_reason == "KILL_SWITCH_ACTIVE"
    assert factory.calls == 0


def test_missing_and_mismatched_deployment_fail_open():
    original = decision()
    missing = DurableDecisionObservationProducer(NoConnect(), TransportFlags(
        decision_observation_enabled=True, kill_switch=False), source_service="test")
    assert missing.observe(original) is original
    assert missing.last_error_code == "CONFIGURATION_INVALID"
    mismatch = DurableDecisionObservationProducer(NoConnect(), TransportFlags(
        decision_observation_enabled=True, kill_switch=False, deployment_id="vps-paper"), source_service="test")
    assert mismatch.observe(original) is original
    assert mismatch.last_error_code == "DEPLOYMENT_MISMATCH"

    wrong_environment = replace(
        original,
        evaluation=replace(original.evaluation, environment="trading_live"),
    )
    environment_mismatch = DurableDecisionObservationProducer(
        NoConnect(), TransportFlags(decision_observation_enabled=True,
                                    kill_switch=False,
                                    deployment_id="local-paper"),
        source_service="test",
    )
    assert environment_mismatch.observe(wrong_environment) is wrong_environment
    assert environment_mismatch.last_error_code == "ENVIRONMENT_MISMATCH"


def test_deterministic_key_and_semantics_cover_all_decision_kinds():
    for kind in ("NO_TRADE", "HOLD", "EXIT", "TRADE"):
        item = decision(action=kind)
        assert deterministic_decision_key(item) == deterministic_decision_key(item)
    original = decision()
    assert replace(original) == original


def test_transport_schema_is_durable_idempotent_and_immutable():
    for token in ("UNIQUE(deployment_id,decision_key)", "FOR UPDATE SKIP LOCKED",
                  "IDEMPOTENCY_CONFLICT", "DEAD_LETTER", "STALE_CLAIM"):
        assert token.replace(" ", "") in (MIGRATION + TRANSPORT).replace(" ", "")
    assert "BEFORE UPDATE OR DELETE" in MIGRATION
    assert "event payload is immutable" in MIGRATION


def test_consumer_is_automation_runner_single_writer_and_bounded():
    assert "DecisionObservationOutboxConsumer" in RUNNER
    assert "CAUSAL_OUTBOX_BATCH_SIZE" in TRANSPORT
    assert "ORDER BY decision_created_at,inserted_at,event_id" in TRANSPORT
    for bot in ("bot/main.py", "bot_trend/main.py", "bot_supertrend/main.py", "bot_bbrange/main.py"):
        source = (ROOT / bot).read_text()
        assert "causal_decision_observation_v1" not in source
        assert "causal_decision_observation_outbox_v1" not in source


def test_shadow_off_path_has_no_attribution_would_trade_or_promotion_write():
    persist = TRANSPORT[TRANSPORT.index("def _persist"):]
    assert "learning_would_trade_decisions_v1" not in persist
    assert "causal_promotion_consumption_v1" not in persist
    assert "learning_recommendation_activations_v1" not in persist


def test_baseline_projection_includes_no_trade_without_order_or_position():
    assert "OBSERVATION_ONLY" in TRANSPORT
    persist = TRANSPORT[TRANSPORT.index("def _persist"):]
    assert "INSERT INTO decision_replay_v1" in persist
    assert "INSERT INTO learning_feature_warehouse_v1" in persist
    assert "INSERT INTO positions" not in persist and "INSERT INTO orders" not in persist


def test_observability_contract_and_separation():
    for metric in ("outbox_events_created_total", "outbox_events_processed_total",
                   "outbox_events_retry_total", "outbox_events_dead_letter_total",
                   "outbox_idempotency_conflicts_total", "outbox_oldest_pending_age_seconds",
                   "consumer_last_poll_timestamp", "consumer_last_success_timestamp",
                   "consumer_batch_duration_seconds", "current_batch_in_progress",
                   "decision_observation_write_failures_total"):
        assert metric in TRANSPORT
    assert {"local-live", "local-paper", "vps-live", "vps-paper"}.issubset(set(MIGRATION.split("'")))


def test_migration_and_fingerprint_contracts_are_idempotent():
    assert "CREATE TABLE IF NOT EXISTS" in MIGRATION
    assert "CREATE INDEX IF NOT EXISTS" in MIGRATION
    assert "ON CONFLICT(key) DO NOTHING" in MIGRATION
    runner = (ROOT / "scripts/causal_learning_telemetry_fingerprint_v1.py").read_text()
    assert "causal_decision_observation_transport_v1" in runner


def test_producer_failure_does_not_mutate_trading_state():
    class Broken:
        def __call__(self): raise RuntimeError("db unavailable")
    original = decision()
    metrics = TransportMetrics()
    producer = DurableDecisionObservationProducer(Broken(), TransportFlags(
        decision_observation_enabled=True, kill_switch=False, deployment_id="local-paper"),
        source_service="test", metrics=metrics)
    assert producer.observe(original) is original
    assert metrics.counters["decision_observation_write_failures_total"] == 1


def test_invalid_configuration_diagnostic_is_bounded(caplog):
    original = decision()
    for _ in range(2):
        producer = DurableDecisionObservationProducer(
            NoConnect(),
            TransportFlags(decision_observation_enabled=True, kill_switch=False),
            source_service="bounded-diagnostic-test",
        )
        assert producer.observe(original) is original
    assert caplog.text.count("causal_outbox_producer_failure") == 1


class ProducerCursor:
    def __init__(self, *, mode, event_id):
        self.mode = mode
        self.event_id = event_id
        self.row = None
        self.status_updates = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params):
        normalized = " ".join(sql.split())
        if normalized.startswith("INSERT INTO causal_decision_observation_outbox_v1"):
            if self.mode == "db-failure":
                raise RuntimeError("database unavailable")
            self.insert_digest = params[5]
            self.row = (self.event_id,) if self.mode == "new" else None
        elif normalized.startswith("SELECT event_id,event_payload_hash"):
            digest = self.insert_digest if self.mode == "existing" else "different"
            self.row = (self.event_id, digest)
        elif normalized.startswith("UPDATE causal_decision_observation_outbox_v1"):
            self.status_updates.append(params)
        else:
            raise AssertionError(normalized)

    def fetchone(self):
        return self.row


class ProducerConnection:
    def __init__(self, mode, event_id):
        self.cursor_value = ProducerCursor(mode=mode, event_id=event_id)
        self.committed = 0
        self.rolled_back = 0
        self.closed = 0

    def cursor(self):
        return self.cursor_value

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1

    def close(self):
        self.closed += 1


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("new", ProducerObservationStatus.ACCEPTED),
        ("existing", ProducerObservationStatus.IDEMPOTENT_EXISTING),
        ("conflict", ProducerObservationStatus.IDEMPOTENCY_CONFLICT),
        ("db-failure", ProducerObservationStatus.OUTBOX_WRITE_FAILED),
    ],
)
def test_structured_producer_result_classifies_outbox_outcomes(mode, expected):
    import uuid

    event_id = uuid.uuid4()
    conn = ProducerConnection(mode, event_id)
    producer = DurableDecisionObservationProducer(
        lambda: conn,
        TransportFlags(
            decision_observation_enabled=True,
            kill_switch=False,
            deployment_id="local-paper",
        ),
        source_service="test",
    )
    result = producer.observe_with_result(decision())
    assert result.status is expected
    if expected in {
        ProducerObservationStatus.ACCEPTED,
        ProducerObservationStatus.IDEMPOTENT_EXISTING,
        ProducerObservationStatus.IDEMPOTENCY_CONFLICT,
    }:
        assert result.outbox_event_id == str(event_id)
    if expected is ProducerObservationStatus.IDEMPOTENCY_CONFLICT:
        assert result.conflict


def test_structured_producer_result_classifies_serialization(monkeypatch):
    producer = DurableDecisionObservationProducer(
        NoConnect(),
        TransportFlags(
            decision_observation_enabled=True,
            kill_switch=False,
            deployment_id="local-paper",
        ),
        source_service="serialization-test",
    )

    def broken(_event):
        raise TypeError("not serializable")

    monkeypatch.setattr(
        "common.decision_observation_transport._payload",
        broken,
    )
    result = producer.observe_with_result(decision())
    assert result.status is ProducerObservationStatus.SERIALIZATION_FAILED
    assert result.error_class == "TypeError"

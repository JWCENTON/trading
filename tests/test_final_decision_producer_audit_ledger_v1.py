from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from common.decision_contract import (
    DecisionReason,
    DecisionSubtype,
    EvaluationContext,
    FinalDecision,
)
from common.decision_observation_transport import (
    ProducerObservationResult,
    ProducerObservationStatus,
    TransportFlags,
    deterministic_decision_key,
)
from common.final_decision_producer_audit import (
    AUDIT_LEDGER_HEALTH,
    AuditDecisionContext,
    AuditIdentity,
    AuditLedgerFlags,
    FinalDecisionProducerAuditLedger,
    reset_audit_ledger_health_for_tests,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT / "db/migrations/20260724_final_decision_producer_audit_ledger_v1.sql"
).read_text()


def decision(kind="NO_TRADE", deployment="local-paper"):
    now = datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
    context = EvaluationContext(
        deployment_id=deployment,
        environment="trading_paper",
        symbol="BTCUSDC",
        interval="1m",
        strategy="RSI",
        candle_open_time=now,
        evaluation_started_at=now,
        engine_name="RSI",
        paper_mode=True,
    )
    if kind == "SYSTEM_NOT_EVALUATED":
        return FinalDecision.system_not_evaluated(
            context,
            DecisionReason.NO_NEW_CANDLE,
            finished_at=now,
        )
    if kind == "HOLD":
        return FinalDecision.position_hold(
            context,
            DecisionReason.NO_SIGNAL,
            finished_at=now,
        )
    if kind == "TRADE":
        return FinalDecision.paper_simulation(
            context,
            DecisionReason.SSOT_EXECUTE_AND_RECORD,
            finished_at=now,
        )
    if kind == "EXIT":
        return FinalDecision.exit_result(
            context,
            DecisionReason.STRATEGY_EXIT,
            finished_at=now,
        )
    if kind == "BLOCKED_BY_EXISTING_LOGIC":
        return FinalDecision.entry_blocked(
            context,
            DecisionReason.POLICY_BLOCK,
            DecisionSubtype.ORC_BLOCKED,
            finished_at=now,
        )
    return FinalDecision.no_trade(
        context,
        DecisionReason.NO_SIGNAL,
        finished_at=now,
    )


class MemoryCursor:
    def __init__(self, conn):
        self.conn = conn
        self.row = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params):
        normalized = " ".join(sql.split())
        if normalized.startswith("INSERT INTO final_decision_producer_audit_v1"):
            audit_event_id = params[0]
            event_digest = params[26]
            self.conn.rows.setdefault(audit_event_id, (params, event_digest))
        elif normalized.startswith("SELECT event_digest"):
            existing = self.conn.rows.get(params[0])
            self.row = None if existing is None else (existing[1],)
        else:
            raise AssertionError(normalized)

    def fetchone(self):
        return self.row


class MemoryConnection:
    def __init__(self):
        self.rows = {}
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0

    def cursor(self):
        return MemoryCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed += 1


def ledger_for(item, conn):
    key = deterministic_decision_key(item)
    context = AuditDecisionContext.from_decision(
        item,
        decision_key=key,
        source_service="test-rsi",
        source_instance="test-instance",
    )
    return FinalDecisionProducerAuditLedger(lambda: conn, context), context


def test_identity_is_deployment_safe_and_deterministic():
    first = AuditIdentity.build("local-paper", "key-1")
    same = AuditIdentity.build("local-paper", "key-1")
    other = AuditIdentity.build("vps-paper", "key-1")
    assert first == same
    assert first.finalized_event_id != other.finalized_event_id
    assert first.audit_event_id("FINALIZED") == same.audit_event_id("FINALIZED")
    assert first.audit_event_id("ACCEPTED") != first.audit_event_id("FINALIZED")


def test_append_only_lifecycle_is_idempotent_and_reuses_one_connection():
    conn = MemoryConnection()
    ledger, context = ledger_for(decision(), conn)
    attempted_at = datetime(2026, 7, 24, 12, 0, 1, tzinfo=timezone.utc)
    outbox_id = "6d77c699-9197-5550-86f0-bc959f5e3295"
    assert ledger.append("FINALIZED")
    assert ledger.append("FINALIZED")
    assert ledger.append("PRODUCER_ATTEMPTED", attempted_at=attempted_at)
    assert ledger.append(
        "ACCEPTED",
        attempted_at=attempted_at,
        outbox_event_id=outbox_id,
    )
    ledger.close()
    assert len(conn.rows) == 3
    assert conn.commits == 4
    assert conn.closed == 1
    finalized = conn.rows[context.identity.audit_event_id("FINALIZED")][0]
    assert finalized[4] == "FINALIZED"


def test_system_not_evaluated_remains_analytically_visible():
    conn = MemoryConnection()
    ledger, context = ledger_for(decision("SYSTEM_NOT_EVALUATED"), conn)
    assert context.original_decision_type == "SYSTEM_NOT_EVALUATED"
    assert context.decision_kind == "NO_TRADE"
    assert ledger.append("FINALIZED")


@pytest.mark.parametrize(
    ("kind", "original_type"),
    [
        ("TRADE", "PAPER_SIMULATION"),
        ("EXIT", "PAPER_SIMULATION"),
        ("HOLD", "NO_TRADE"),
        ("NO_TRADE", "NO_TRADE"),
        ("BLOCKED_BY_EXISTING_LOGIC", "ENTRY_BLOCKED"),
        ("SYSTEM_NOT_EVALUATED", "SYSTEM_NOT_EVALUATED"),
    ],
)
def test_ledger_contract_covers_all_shared_decision_kinds(kind, original_type):
    conn = MemoryConnection()
    ledger, context = ledger_for(decision(kind), conn)
    assert context.decision_kind == (
        "NO_TRADE" if kind == "SYSTEM_NOT_EVALUATED" else kind
    )
    assert context.original_decision_type == original_type
    assert ledger.append("FINALIZED")


def test_same_audit_identity_with_changed_content_is_detected():
    conn = MemoryConnection()
    ledger, context = ledger_for(decision(), conn)
    assert ledger.append("FINALIZED")
    changed = FinalDecisionProducerAuditLedger(
        lambda: conn,
        replace(context, action="CHANGED"),
    )
    reset_audit_ledger_health_for_tests()
    assert changed.append("FINALIZED") is False
    assert AUDIT_LEDGER_HEALTH.status == "DENOMINATOR_DEGRADED"


def test_ledger_failure_is_fail_open_and_degrades_health(caplog):
    reset_audit_ledger_health_for_tests()

    def broken():
        raise RuntimeError("database unavailable")

    item = decision()
    context = AuditDecisionContext.from_decision(
        item,
        decision_key=deterministic_decision_key(item),
        source_service="broken-test",
    )
    ledger = FinalDecisionProducerAuditLedger(broken, context)
    assert ledger.append("FINALIZED") is False
    assert AUDIT_LEDGER_HEALTH.status == "DENOMINATOR_DEGRADED"
    assert AUDIT_LEDGER_HEALTH.failures["RuntimeError"] == 1
    assert "final_decision_producer_audit_failure" in caplog.text


def test_flag_defaults_off(monkeypatch):
    monkeypatch.delenv("FINAL_DECISION_PRODUCER_AUDIT_LEDGER_ENABLED", raising=False)
    assert not AuditLedgerFlags.from_env().enabled
    monkeypatch.setenv("FINAL_DECISION_PRODUCER_AUDIT_LEDGER_ENABLED", "1")
    assert AuditLedgerFlags.from_env().enabled


class CapturingLedger:
    instances = []

    def __init__(self, _factory, context):
        self.context = context
        self.events = []
        self.closed = False
        self.__class__.instances.append(self)

    def append(self, event_type, **details):
        self.events.append((event_type, details))
        return True

    def close(self):
        self.closed = True


def _enable_ledger(monkeypatch):
    CapturingLedger.instances.clear()
    monkeypatch.setattr(
        AuditLedgerFlags,
        "from_env",
        classmethod(lambda cls: AuditLedgerFlags(enabled=True)),
    )
    monkeypatch.setattr(
        "common.final_decision_observation_sink.FinalDecisionProducerAuditLedger",
        CapturingLedger,
    )


@pytest.mark.parametrize(
    ("transport_flags", "expected"),
    [
        (
            TransportFlags(),
            ["FINALIZED", "SKIPPED_DISABLED"],
        ),
        (
            TransportFlags(
                decision_observation_enabled=True,
                kill_switch=True,
                deployment_id="local-paper",
            ),
            ["FINALIZED", "SKIPPED_KILL_SWITCH"],
        ),
    ],
)
def test_finalizer_records_exact_legal_skip_and_returns_original(
    monkeypatch, transport_flags, expected
):
    from common.final_decision_observation_sink import finalize_decision_observation

    _enable_ledger(monkeypatch)
    monkeypatch.setattr(
        TransportFlags,
        "from_env",
        classmethod(lambda cls: transport_flags),
    )
    original = decision()
    assert finalize_decision_observation(original, source_service="test") is original
    assert [event[0] for event in CapturingLedger.instances[0].events] == expected
    assert CapturingLedger.instances[0].closed


def test_none_emits_no_ledger_event(monkeypatch):
    from common.final_decision_observation_sink import finalize_decision_observation

    _enable_ledger(monkeypatch)
    assert finalize_decision_observation(None, source_service="test") is None
    assert CapturingLedger.instances == []


def test_transport_flag_parse_failure_is_durable_validation_and_fail_open(
    monkeypatch,
):
    from common.final_decision_observation_sink import finalize_decision_observation

    _enable_ledger(monkeypatch)

    def broken_flags(_cls):
        raise ValueError("invalid batch size")

    monkeypatch.setattr(
        TransportFlags,
        "from_env",
        classmethod(broken_flags),
    )
    original = decision()
    assert finalize_decision_observation(original, source_service="test") is original
    assert [event[0] for event in CapturingLedger.instances[0].events] == [
        "FINALIZED",
        "PRODUCER_ATTEMPTED",
        "VALIDATION_REJECTED",
    ]


@pytest.mark.parametrize(
    "result_status",
    [
        ProducerObservationStatus.ACCEPTED,
        ProducerObservationStatus.IDEMPOTENT_EXISTING,
        ProducerObservationStatus.IDEMPOTENCY_CONFLICT,
        ProducerObservationStatus.VALIDATION_REJECTED,
        ProducerObservationStatus.SERIALIZATION_FAILED,
        ProducerObservationStatus.OUTBOX_WRITE_FAILED,
    ],
)
def test_active_finalizer_has_attempt_and_exact_terminal(
    monkeypatch, result_status
):
    from common.final_decision_observation_sink import finalize_decision_observation

    _enable_ledger(monkeypatch)
    flags = TransportFlags(
        decision_observation_enabled=True,
        kill_switch=False,
        deployment_id="local-paper",
    )
    monkeypatch.setattr(
        TransportFlags,
        "from_env",
        classmethod(lambda cls: flags),
    )

    class Producer:
        def __init__(self, *_args, **_kwargs):
            pass

        def observe_with_result(self, _decision, *, decision_key):
            linked = result_status in {
                ProducerObservationStatus.ACCEPTED,
                ProducerObservationStatus.IDEMPOTENT_EXISTING,
                ProducerObservationStatus.IDEMPOTENCY_CONFLICT,
            }
            return ProducerObservationResult(
                result_status,
                decision_key,
                outbox_event_id=(
                    "6d77c699-9197-5550-86f0-bc959f5e3295" if linked else None
                ),
                error_class=(
                    result_status.value
                    if result_status in {
                        ProducerObservationStatus.IDEMPOTENCY_CONFLICT,
                        ProducerObservationStatus.VALIDATION_REJECTED,
                        ProducerObservationStatus.SERIALIZATION_FAILED,
                        ProducerObservationStatus.OUTBOX_WRITE_FAILED,
                    }
                    else None
                ),
            )

    monkeypatch.setattr(
        "common.final_decision_observation_sink.DurableDecisionObservationProducer",
        Producer,
    )
    original = decision()
    assert finalize_decision_observation(original, source_service="test") is original
    assert [event[0] for event in CapturingLedger.instances[0].events] == [
        "FINALIZED",
        "PRODUCER_ATTEMPTED",
        result_status.value,
    ]


def test_ledger_disabled_preserves_zero_ledger_work(monkeypatch):
    from common.final_decision_observation_sink import finalize_decision_observation

    monkeypatch.setattr(
        AuditLedgerFlags,
        "from_env",
        classmethod(lambda cls: AuditLedgerFlags(enabled=False)),
    )
    monkeypatch.setattr(
        TransportFlags,
        "from_env",
        classmethod(lambda cls: TransportFlags()),
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("ledger must not be constructed")

    monkeypatch.setattr(
        "common.final_decision_observation_sink.FinalDecisionProducerAuditLedger",
        forbidden,
    )
    original = decision()
    assert finalize_decision_observation(original, source_service="test") is original


def test_migration_is_additive_append_only_and_indexed():
    assert "CREATE TABLE IF NOT EXISTS final_decision_producer_audit_v1" in MIGRATION
    assert "BEFORE UPDATE OR DELETE" in MIGRATION
    assert "DROP " not in MIGRATION
    assert "DELETE FROM" not in MIGRATION
    assert "UPDATE final_decision_producer_audit_v1" not in MIGRATION
    assert "REFERENCES causal_decision_observation_outbox_v1(event_id)" in MIGRATION
    for token in (
        "ux_final_decision_producer_audit_finalized_v1",
        "ux_final_decision_producer_audit_attempt_event_v1",
        "ix_final_decision_producer_audit_deployment_time_v1",
        "ix_final_decision_producer_audit_event_time_v1",
        "ix_final_decision_producer_audit_source_time_v1",
        "ix_final_decision_producer_audit_slot_time_v1",
        "ix_final_decision_producer_audit_status_time_v1",
        "ix_final_decision_producer_audit_outbox_v1",
    ):
        assert token in MIGRATION


def test_all_strategy_families_keep_the_shared_hook_without_domain_changes():
    paths = {
        "RSI": "bot/main.py",
        "TREND": "bot_trend/main.py",
        "SUPERTREND": "bot_supertrend/main.py",
        "BBRANGE": "bot_bbrange/main.py",
    }
    for path in paths.values():
        source = (ROOT / path).read_text()
        assert "finalize_decision_observation(" in source
        assert "final_decision_producer_audit_v1" not in source

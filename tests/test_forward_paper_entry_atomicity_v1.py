from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from common import simulated_execution_evidence as evidence


BOUNDARIES = (
    "AFTER_DECISION_BEFORE_ORDER",
    "AFTER_ORDER_BEFORE_COMMITMENT",
    "AFTER_COMMITMENT_BEFORE_FILL",
    "AFTER_ORDER_BEFORE_POSITION",
    "AFTER_POSITION_PREPARED",
    "AFTER_ENTRY_FILL",
    "AFTER_POSITION_LINKAGE",
    "BEFORE_ENTRY_COMMIT",
)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class FakeConnection:
    def __init__(self, committed):
        self.committed = committed
        self.pending = []
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_args):
        if exc_type is None:
            self.committed.extend(self.pending)
        self.pending.clear()
        return False

    def close(self):
        self.closed = True


@pytest.mark.parametrize("crash_boundary", BOUNDARIES)
def test_crash_rolls_back_whole_entry_and_retry_commits_exactly_once(
    monkeypatch, crash_boundary
):
    committed = []
    connections = []

    def connection_factory():
        connection = FakeConnection(committed)
        connections.append(connection)
        return connection

    def inject(stage):
        if stage == crash_boundary:
            raise RuntimeError("injected-process-boundary")

    def create(cur, *, failure_injector, **_kwargs):
        for stage, mutation in (
            ("AFTER_DECISION_BEFORE_ORDER", "decision"),
            ("AFTER_ORDER_BEFORE_COMMITMENT", "order"),
            ("AFTER_COMMITMENT_BEFORE_FILL", "reservation"),
        ):
            cur.connection.pending.append(mutation)
            failure_injector(stage)
        return 41

    def record(*_args, connection, failure_injector, **_kwargs):
        for stage, mutation in (
            ("AFTER_ORDER_BEFORE_POSITION", "position-precheck"),
            ("AFTER_POSITION_PREPARED", "position"),
            ("AFTER_ENTRY_FILL", "fill-with-frozen-fee-v2"),
            ("AFTER_POSITION_LINKAGE", "position-fill-linkage"),
        ):
            connection.pending.append(mutation)
            failure_injector(stage)
        return evidence.PaperEntryAtomicResult(True, "INSERTED", 41, 77)

    monkeypatch.setattr(evidence, "create_simulated_order_cursor", create)
    monkeypatch.setattr(evidence, "record_simulated_fill_evidence", record)

    kwargs = dict(
        client=object(), symbol="BTCUSDC", interval="1m", strategy="RSI",
        side="BUY", price=Decimal("100"), quantity=Decimal("0.1"),
        reason="atomic-proof",
        candle_open_time=datetime(2026, 8, 27, tzinfo=timezone.utc),
        deployment_id="local-paper", market_regime="RANGE",
        regime_source_provenance={"proof": True},
    )
    with pytest.raises(RuntimeError, match="injected-process-boundary"):
        evidence.record_forward_paper_entry_atomic(
            connection_factory, failure_injector=inject, **kwargs
        )

    assert committed == []
    assert connections[-1].closed is True

    result = evidence.record_forward_paper_entry_atomic(
        connection_factory, failure_injector=lambda _stage: None, **kwargs
    )
    assert result == evidence.PaperEntryAtomicResult(True, "INSERTED", 41, 77)
    assert committed.count("decision") == 1
    assert committed.count("order") == 1
    assert committed.count("reservation") == 1
    assert committed.count("position") == 1
    assert committed.count("fill-with-frozen-fee-v2") == 1
    assert committed.count("position-fill-linkage") == 1


def test_conflicting_order_rolls_back_and_does_not_prepare_position(monkeypatch):
    committed = []
    called = []

    def create(cur, **_kwargs):
        cur.connection.pending.append("decision")
        return evidence.SimulatedOrderWriteBlocked(
            "PAPER_ORDER_SLOT_ALREADY_OCCUPIED", 12
        )

    def record(*_args, **_kwargs):
        called.append(True)

    monkeypatch.setattr(evidence, "create_simulated_order_cursor", create)
    monkeypatch.setattr(evidence, "record_simulated_fill_evidence", record)

    result = evidence.record_forward_paper_entry_atomic(
        lambda: FakeConnection(committed), client=object(), symbol="BTCUSDC",
        interval="1m", strategy="RSI", side="BUY", price=Decimal("100"),
        quantity=Decimal("0.1"), reason="conflict",
        candle_open_time=datetime(2026, 8, 27, tzinfo=timezone.utc),
        deployment_id="local-paper", market_regime="RANGE",
        regime_source_provenance={"proof": True},
    )
    assert result.persisted is False
    assert result.status == "PAPER_ORDER_SLOT_ALREADY_OCCUPIED"
    assert committed == []
    assert called == []


def test_all_four_strategies_use_one_shared_paper_entry_boundary():
    root = __import__("pathlib").Path(__file__).resolve().parents[1]
    for relative in (
        "bot/main.py", "bot_bbrange/main.py", "bot_trend/main.py",
        "bot_supertrend/main.py",
    ):
        source = (root / relative).read_text()
        assert "record_forward_paper_entry_atomic(" in source
        assert 'trading_mode == "PAPER" and not is_exit' in source

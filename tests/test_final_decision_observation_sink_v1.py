from datetime import datetime, timezone
from pathlib import Path

from common.decision_contract import DecisionReason, EvaluationContext, FinalDecision
from common.final_decision_observation_sink import finalize_decision_observation
from common.decision_observation_transport import TransportFlags


def decision():
    now = datetime(2026, 7, 17, tzinfo=timezone.utc)
    context = EvaluationContext(
        deployment_id="local-paper", environment="trading_paper",
        symbol="BTCUSDC", interval="1m", strategy="RSI",
        candle_open_time=now, evaluation_started_at=now,
        engine_name="RSI", paper_mode=True,
    )
    return FinalDecision.no_trade(
        context, DecisionReason.NO_SIGNAL, finished_at=now,
    )


class NoConnect:
    def __init__(self):
        self.calls = 0

    def __call__(self):
        self.calls += 1
        raise AssertionError("database access is forbidden")


def test_default_off_and_kill_switch_do_not_construct_db_work(monkeypatch):
    original = decision()
    factory = NoConnect()
    for flags in (
        TransportFlags(),
        TransportFlags(decision_observation_enabled=True, kill_switch=True,
                       deployment_id="local-paper"),
    ):
        monkeypatch.setattr(TransportFlags, "from_env", classmethod(lambda cls, value=flags: value))
        assert finalize_decision_observation(
            original, source_service="test", connection_factory=factory,
        ) is original
    assert factory.calls == 0


def test_enabled_sink_attempts_once_and_is_identity_preserving(monkeypatch):
    original = decision()
    attempts = []
    flags = TransportFlags(decision_observation_enabled=True, kill_switch=False,
                           deployment_id="local-paper")
    monkeypatch.setattr(TransportFlags, "from_env", classmethod(lambda cls: flags))

    class Producer:
        def __init__(self, _factory, observed_flags, *, source_service):
            assert observed_flags is flags
            assert source_service == "test"

        def observe(self, observed):
            attempts.append(observed)
            return observed

    monkeypatch.setattr(
        "common.final_decision_observation_sink.DurableDecisionObservationProducer",
        Producer,
    )
    returned = finalize_decision_observation(original, source_service="test")
    assert returned is original
    assert attempts == [original]


def test_sink_is_fail_open_even_if_producer_construction_fails(monkeypatch):
    original = decision()
    flags = TransportFlags(decision_observation_enabled=True, kill_switch=False,
                           deployment_id="local-paper")
    monkeypatch.setattr(TransportFlags, "from_env", classmethod(lambda cls: flags))

    def broken(*_args, **_kwargs):
        raise RuntimeError("producer unavailable")

    monkeypatch.setattr(
        "common.final_decision_observation_sink.DurableDecisionObservationProducer",
        broken,
    )
    assert finalize_decision_observation(original, source_service="test") is original


def test_none_without_canonical_identity_is_not_observed():
    assert finalize_decision_observation(None, source_service="test") is None


def test_all_four_runtime_entrypoints_use_only_the_shared_sink():
    root = Path(__file__).resolve().parents[1]
    contracts = {
        "bot/main.py": ("source_service=\"bot-rsi\"", 2),
        "bot_trend/main.py": ("source_service=\"bot-trend\"", 2),
        "bot_supertrend/main.py": ("source_service=\"bot-supertrend\"", 1),
        "bot_bbrange/main.py": ("source_service=\"bot-bbrange\"", 1),
    }
    for path, (call, expected_paths) in contracts.items():
        source = (root / path).read_text()
        assert source.count(call) == expected_paths
        assert "causal_decision_observation_outbox_v1" not in source

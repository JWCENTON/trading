from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from common.decision_contract import DecisionReason, EvaluationContext, FinalDecision
from common.decision_observation import (
    DecisionObservationAdapter, DecisionObservationEvent,
    FailureCode, IdempotencyConflict, InMemoryObservationRepository,
    RuntimeFlags, SkipReason, directional_status, event_from_final_decision,
)

NOW = datetime(2026, 7, 17, tzinfo=timezone.utc)


def final(deployment="local-paper"):
    context = EvaluationContext(
        deployment_id=deployment, environment="trading_paper", symbol="BTCUSDC",
        interval="1m", strategy="RSI", candle_open_time=NOW,
        evaluation_started_at=NOW, engine_name="RSI", market_regime="RANGE",
        regime_confidence=Decimal("0.8"), paper_mode=True,
    )
    return FinalDecision.no_trade(context, DecisionReason.NO_SIGNAL,
                                  finished_at=NOW, reference_price=Decimal("100"))


def event(deployment="local-paper", key="decision-1"):
    return event_from_final_decision(
        final(deployment), event_id="00000000-0000-0000-0000-000000000001",
        decision_key=key, source_service="bot-runner", source_instance="fixture",
        confidence=Decimal("0.7"), quantity_intent=Decimal("1"),
        stop_loss_intent=Decimal("90"), take_profit_intent=Decimal("110"),
    )


def enabled(deployment="local-paper"):
    return RuntimeFlags(True, True, False, False, deployment)


def test_deployment_required_and_validated():
    with pytest.raises(ValueError, match="deployment_id"):
        event("")


def test_event_and_final_decision_are_immutable():
    item, decision = event(), final()
    with pytest.raises(FrozenInstanceError):
        item.action = "BLOCK"
    before = decision
    DecisionObservationAdapter(InMemoryObservationRepository(), enabled()).observe(item)
    assert decision == before and decision.action is None


def test_digest_is_deterministic_and_semantic():
    first = event()
    assert first.semantic_digest == event().semantic_digest
    assert replace(first, source_instance="retry").semantic_digest == first.semantic_digest
    assert replace(first, action="BLOCK").semantic_digest != first.semantic_digest


def test_duplicate_identical_is_idempotent_but_changed_conflicts():
    repo = InMemoryObservationRepository()
    assert repo.record_decision_observation(event()) == "inserted"
    assert repo.record_decision_observation(event()) == "idempotent"
    with pytest.raises(IdempotencyConflict):
        repo.record_decision_observation(replace(event(), action="BLOCK"))


def test_local_and_vps_paper_do_not_mix():
    repo = InMemoryObservationRepository()
    repo.record_decision_observation(event("local-paper"))
    repo.record_decision_observation(event("vps-paper"))
    assert len(repo.observations) == 2


@pytest.mark.parametrize("flags,reason", [
    (RuntimeFlags(False, True, False, False, "local-paper"), SkipReason.TELEMETRY_DISABLED),
    (RuntimeFlags(True, True, False, True, "local-paper"), SkipReason.KILL_SWITCH_ACTIVE),
    (RuntimeFlags(True, False, False, False, "local-paper"), SkipReason.SHADOW_OBSERVATION_DISABLED),
    (RuntimeFlags(True, True, True, False, "local-paper"), SkipReason.AUTO_APPLY_NOT_ALLOWED),
])
def test_disabled_killed_shadow_off_or_auto_apply_produces_no_write(flags, reason):
    repo = InMemoryObservationRepository()
    adapter = DecisionObservationAdapter(repo, flags)
    assert not adapter.observe(event())
    assert adapter.last_skip_reason == reason.value
    assert not adapter.failures
    assert not repo.observations


def test_invalid_configuration_fails_open_and_counts_error():
    adapter = DecisionObservationAdapter(InMemoryObservationRepository(),
                                         RuntimeFlags(True, True, False, False, None))
    assert not adapter.observe(event())
    assert adapter.failures[FailureCode.CONFIGURATION_INVALID.value] == 1


def test_repository_exception_fails_open():
    class Broken(InMemoryObservationRepository):
        def record_decision_observation(self, value): raise RuntimeError("db down")
    adapter = DecisionObservationAdapter(Broken(), enabled())
    assert not adapter.observe(event())
    assert adapter.failures[FailureCode.DECISION_OBSERVATION_WRITE_FAILED.value] == 1


@pytest.mark.parametrize("pnl,linked,closed,status", [
    (Decimal("1"), True, True, "HARMFUL_DIRECTIONAL"),
    (Decimal("-1"), True, True, "BENEFICIAL_DIRECTIONAL"),
    (Decimal("0"), True, True, "NEUTRAL_DIRECTIONAL"),
    (None, True, False, "PENDING_OUTCOME"),
    (None, False, True, "NOT_EVALUABLE"),
])
def test_directional_outcomes(pnl, linked, closed, status):
    assert directional_status(pnl, linked, closed) == status


def test_repository_chain_supports_no_trade_and_is_deployment_safe():
    repo, item = InMemoryObservationRepository(), event()
    assert item.decision_kind == "NO_TRADE"
    repo.record_decision_observation(item)
    attribution = {"deployment_id": "local-paper", "decision_key": "decision-1",
                   "causal_linkage_status": "NOT_ELIGIBLE"}
    repo.record_attribution(attribution)
    would = {**attribution, "actual_action": "NO_ACTION",
             "actual_execution_eligible": False, "recommended_shadow_action": "BLOCK",
             "would_trade_without_recommendation": False,
             "would_trade_with_recommendation": False,
             "recommendation_effect_applied": False}
    repo.record_would_trade(would)
    repo.project(item, attribution)
    assert repo.replay[("local-paper", "decision-1")]["decision_kind"] == "NO_TRADE"
    assert repo.warehouse[("local-paper", "decision-1")]["causal_linkage_status"] == "NOT_ELIGIBLE"
    assert not would["recommendation_effect_applied"]


def test_promotion_consumption_is_append_only_idempotent():
    repo = InMemoryObservationRepository()
    value = {"deployment_id": "local-paper", "decision_key": "decision-1",
             "promotion_consumption_event_id": "promotion-1", "promotion_hash": "abc"}
    assert repo.record_promotion_consumption(value) == "inserted"
    assert repo.record_promotion_consumption(value) == "idempotent"
    with pytest.raises(IdempotencyConflict):
        repo.record_promotion_consumption({**value, "promotion_hash": "changed"})


def test_cross_deployment_adapter_rejects_event():
    repo = InMemoryObservationRepository()
    assert not DecisionObservationAdapter(repo, enabled("local-paper")).observe(event("vps-paper"))
    assert not repo.observations

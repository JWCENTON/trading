from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from common.causal_learning import (
    DecisionAttribution,
    LEGACY_NOT_ATTRIBUTABLE,
    NO_RECOMMENDATION,
    payload_hash,
    recommendation_id,
    slot_key,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT / "db/migrations/20260716_causal_learning_telemetry_v1.sql"
).read_text()


def test_recommendation_id_is_deterministic_and_versioned():
    kwargs = dict(
        environment="trading_paper",
        strategy="BBRANGE",
        symbol="SOLUSDC",
        interval="1m",
        regime="RANGE_LOWVOL",
        recommendation_type="EDGE",
        recommendation_action="BLOCK_CANDIDATE",
        evidence_cutoff_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
        policy_version="LEARNING_ENGINE_V1_4",
    )
    assert recommendation_id(**kwargs) == recommendation_id(**kwargs)
    assert recommendation_id(**kwargs) != recommendation_id(
        **{**kwargs, "policy_version": "LEARNING_ENGINE_V1_5"}
    )


def test_regime_specific_keys_do_not_collide():
    base = dict(
        environment="trading_paper",
        strategy="BBRANGE",
        symbol="SOLUSDC",
        interval="1m",
    )
    assert slot_key(**base, regime="RANGE_LOWVOL") != slot_key(
        **base, regime="TREND_UP"
    )


def test_environment_specific_keys_do_not_collide():
    base = dict(
        strategy="BBRANGE",
        symbol="SOLUSDC",
        interval="1m",
        regime="RANGE_LOWVOL",
    )
    assert slot_key(environment="trading_live", **base) != slot_key(
        environment="trading_paper", **base
    )


def test_payload_hash_is_order_independent():
    assert payload_hash({"a": 1, "b": 2}) == payload_hash({"b": 2, "a": 1})


def test_default_attribution_is_explicitly_unattributed():
    attribution = DecisionAttribution()
    assert attribution.recommendation_id is None
    assert attribution.experiment_arm == "BASELINE"
    assert attribution.causal_linkage_status == NO_RECOMMENDATION
    assert LEGACY_NOT_ATTRIBUTABLE != NO_RECOMMENDATION


@pytest.mark.parametrize(
    "fragment",
    [
        "evidence_cutoff_at TIMESTAMPTZ NOT NULL",
        "learning_recommendation_activations_v1",
        "causal activation identity is append-only",
        "a.effective_from <= NEW.decision_timestamp",
        "a.expires_at > NEW.decision_timestamp",
        "r.reset_at IS NULL",
        "historical causal attribution is immutable",
        "LEGACY_NOT_ATTRIBUTABLE",
        "NO_ACTIVE_RECOMMENDATION",
        "promotion_payload_hash",
        "promotion_policy_version",
        "SHADOW_COUNTERFACTUAL",
        "learning_would_trade_decisions_v1",
        "learning_counterfactual_outcomes_v1",
        "DIRECTIONAL_ONLY",
        "v_learning_experiment_readiness_v1",
    ],
)
def test_migration_contains_causal_safety_contract(fragment):
    assert fragment in MIGRATION


def test_blocked_would_trade_cannot_create_position_or_order():
    section = MIGRATION[
        MIGRATION.index("CREATE TABLE IF NOT EXISTS learning_would_trade_decisions_v1"):
        MIGRATION.index("CREATE TABLE IF NOT EXISTS learning_counterfactual_outcomes_v1")
    ]
    assert "INSERT INTO positions" not in section
    assert "INSERT INTO binance_orders" not in section
    assert "INSERT INTO simulated_orders" not in section


def test_counterfactual_outcome_links_by_decision_key():
    assert (
        "decision_key TEXT PRIMARY KEY REFERENCES "
        "learning_would_trade_decisions_v1(decision_key)"
    ) in MIGRATION


def test_auto_apply_is_forced_off():
    assert "('causal_learning_auto_apply_enabled', '0', now())" in MIGRATION
    assert "apply_mode <> 'PAPER_EXPERIMENT' OR environment = 'trading_paper'" in MIGRATION


def test_replay_and_feature_warehouse_receive_causal_fields():
    for table in ("decision_replay_v1", "learning_feature_warehouse_v1"):
        section = MIGRATION[MIGRATION.index(f"ALTER TABLE {table}"):]
        assert "ADD COLUMN IF NOT EXISTS recommendation_id TEXT" in section
        assert "ADD COLUMN IF NOT EXISTS activation_id UUID" in section
        assert "ADD COLUMN IF NOT EXISTS causal_linkage_status TEXT" in section


def test_all_strategy_final_decision_contracts_remain_present():
    for path in (
        "bot/main.py",
        "bot_trend/main.py",
        "bot_supertrend/main.py",
        "bot_bbrange/main.py",
    ):
        assert "FinalDecision" in (ROOT / path).read_text()


def test_migration_does_not_mutate_trading_state():
    upper = MIGRATION.upper()
    for statement in (
        "UPDATE BOT_CONTROL",
        "INSERT INTO BOT_CONTROL",
        "UPDATE POSITIONS",
        "INSERT INTO POSITIONS",
        "INSERT INTO BINANCE_ORDERS",
        "INSERT INTO SIMULATED_ORDERS",
    ):
        assert statement not in upper

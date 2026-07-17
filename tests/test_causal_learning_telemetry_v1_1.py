from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "db/migrations/20260717_causal_learning_telemetry_v1_1.sql").read_text()
FP = (ROOT / "tests/postgres/causal_learning_telemetry_fingerprint_v1_1.sql").read_text()


def test_v1_migration_is_untouched_and_v11_is_additive():
    assert "BEGIN;" in SQL and "COMMIT;" in SQL
    assert "legacy-unknown" in SQL
    assert "ALTER COLUMN deployment_id SET NOT NULL" in SQL


def test_deployment_is_end_to_end_and_lookup_safe():
    for table in (
        "learning_recommendation_snapshots_v1", "learning_recommendation_activations_v1",
        "learning_would_trade_decisions_v1", "learning_counterfactual_outcomes_v1",
        "decision_replay_v1", "learning_feature_warehouse_v1",
    ):
        assert f"ALTER TABLE {table}" in SQL
    assert "a.deployment_id=NEW.deployment_id" in SQL
    assert "o.deployment_id=w.deployment_id" in SQL


def test_observation_and_promotion_evidence_are_append_only():
    assert "CREATE TABLE IF NOT EXISTS causal_decision_observation_v1" in SQL
    assert "UNIQUE (deployment_id, decision_key)" in SQL
    assert "CREATE TABLE IF NOT EXISTS causal_promotion_consumption_v1" in SQL
    assert SQL.count("EXECUTE FUNCTION prevent_causal_v1_1_mutation()") == 2


def test_status_and_would_trade_contracts():
    for value in ("NOT_ELIGIBLE", "ATTRIBUTED_SHADOW_OBSERVATION", "ATTRIBUTED_EXPERIMENT",
                  "PENDING_OUTCOME", "BENEFICIAL_DIRECTIONAL", "HARMFUL_DIRECTIONAL",
                  "NEUTRAL_DIRECTIONAL", "NOT_EVALUABLE", "DIRECTIONAL_ONLY"):
        assert value in SQL
    assert "CHECK (recommendation_effect_applied IS FALSE)" in SQL
    assert "consumed_promotion_hash:=NULL" in SQL


def test_flags_remain_off_and_no_treatment_is_created():
    assert "('causal_shadow_observation_enabled','0',now())" in SQL
    assert "('causal_learning_auto_apply_enabled','0',now())" in SQL
    assert "INSERT INTO learning_recommendation_activations_v1" not in SQL
    assert "TREATMENT" not in SQL


def test_v11_manifest_is_explicit_and_versioned():
    assert "learning_%" not in FP and "decision_%" not in FP and "v_learning_%" not in FP
    assert "causal_decision_observation_v1" in FP
    assert "causal_promotion_consumption_v1" in FP

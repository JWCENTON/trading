from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

RUNNER = (ROOT / "automation_runner/main.py").read_text()
MIGRATION = (
    ROOT / "db/migrations/20260807_causal_post_refresh_reconciliation_v1_3.sql"
).read_text()


def test_reconciliation_runs_after_replay_refresh():
    warehouse = RUNNER.index("SELECT refresh_learning_feature_warehouse_v1")
    replay = RUNNER.index("SELECT refresh_decision_replay_v1")
    reconcile = RUNNER.index("SELECT reconcile_forward_causal_artifacts_v1_3")

    assert warehouse < replay < reconcile


def test_reconciliation_is_required_pipeline_function():
    anchor = RUNNER.index(
        '"refresh_learning_feedback_shadow_recommendations_v1"'
    )
    start = RUNNER.rfind("        funcs = [", 0, anchor)
    end = RUNNER.index("        ]", anchor) + 9
    funcs = RUNNER[start:end]

    assert "refresh_learning_feature_warehouse_v1" in funcs
    assert "refresh_decision_replay_v1" in funcs
    assert "reconcile_forward_causal_artifacts_v1_3" in funcs


def test_reconciliation_is_forward_attributed_only():
    assert "FINAL_DECISION_EXECUTION_EPILOG" in MIGRATION
    assert "FORWARD_DECISION_REGISTRY_CONTINUITY_V1" in MIGRATION
    assert "causal_linkage_status LIKE 'ATTRIBUTED_%'" in MIGRATION
    assert "LEGACY_NOT_ATTRIBUTABLE" in MIGRATION


def test_reconciliation_uses_canonical_warehouse_selection():
    assert (
        "exit_time IS NOT NULL" in MIGRATION
        and "net_pnl_usdc IS NOT NULL" in MIGRATION
    )
    assert "LIMIT 1" in MIGRATION


def test_reconciliation_is_idempotency_guarded():
    assert "recommendation_id IS DISTINCT FROM" in MIGRATION
    assert "observation_decision_key IS DISTINCT FROM" in MIGRATION

    # Canonical warehouse selection must not be restricted only to
    # LEGACY_NOT_ATTRIBUTABLE rows, otherwise a second run can select an older
    # snapshot and collide with the existing causal observation identity.
    canonical = MIGRATION[
        MIGRATION.index("SELECT id"):
        MIGRATION.index("IF v_warehouse_id IS NOT NULL")
    ]
    assert "causal_linkage_status = 'LEGACY_NOT_ATTRIBUTABLE'" not in canonical

    assert "id <> v_warehouse_id" in MIGRATION
    assert "observation_decision_key =" in MIGRATION

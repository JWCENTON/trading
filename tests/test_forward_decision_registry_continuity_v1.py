from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT / "db/migrations/20260805_forward_decision_registry_continuity_v1.sql"
).read_text()
EPILOG = (ROOT / "common/final_decision_observation_sink.py").read_text()
TRANSPORT = (ROOT / "common/decision_observation_transport.py").read_text()


def test_rca_existing_final_decision_epilog_is_flagged_fail_open_telemetry_only():
    assert "if not flags.decision_observation_enabled" in EPILOG
    assert "if flags.kill_switch" in EPILOG
    assert "trading result unchanged" in EPILOG
    persist = TRANSPORT[TRANSPORT.index("    def _persist(") :]
    assert "INSERT INTO causal_decision_observation_v1" in persist
    assert "INSERT INTO decision_replay_v1" in persist
    assert "INSERT INTO learning_feature_warehouse_v1" in persist
    assert "INSERT INTO decision_registry_v1" not in persist


def test_common_transaction_bound_order_writer_requires_registry_first():
    assert "BEFORE INSERT ON public.simulated_orders" in MIGRATION
    assert "register_forward_entry_decision_v1(" in MIGRATION
    assert "FINAL_DECISION_EXECUTION_EPILOG" in MIGRATION
    assert "COMMON_SIMULATED_ORDER_WRITER" in MIGRATION
    assert "FORWARD_DECISION_REGISTRY_REQUIRED" in MIGRATION
    assert "decision_payload_fingerprint" in MIGRATION
    assert "source_revision" in MIGRATION
    assert "producer_identity" in MIGRATION
    assert "'final_action','EXECUTE'" in MIGRATION
    assert "'execution_side',upper(p_side)" in MIGRATION


def test_order_fill_position_and_outcome_use_one_forward_identity():
    assert "NEW.decision_id := public.register_forward_entry_decision_v1" in MIGRATION
    assert "NEW.decision_id := v_order.decision_id" in MIGRATION
    assert "SET position_id=NEW.position_id" in MIGRATION
    assert "decision_type IN (''TRADE_EXECUTED'',''ENTRY_DECISION'')" in MIGRATION
    assert "financial_truth.financial_truth_status=''COMPLETE''" in MIGRATION
    assert "NOT EXISTS (" in MIGRATION
    assert "forward_decision.decision_type=''ENTRY_DECISION''" in MIGRATION


def test_patch_is_forward_only_and_preserves_protected_state():
    assert "history" not in MIGRATION.lower()
    assert "UPDATE public.positions" not in MIGRATION
    assert "UPDATE public.canonical_financial_truth_v1" not in MIGRATION
    assert "UPDATE public.decision_outcomes_v1" not in MIGRATION
    assert "DELETE FROM public.decision_registry_v1" not in MIGRATION
    for table in (
        "strategy_params",
        "bot_control",
        "learning_outcome_exclusion_v1",
    ):
        assert f"UPDATE public.{table}" not in MIGRATION
        assert f"DELETE FROM public.{table}" not in MIGRATION

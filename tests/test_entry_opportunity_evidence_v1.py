from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

import common.entry_opportunity_evidence as evidence
from common.paper_simulation_fee_config import PaperSimulationFeeConfig


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT / "db/migrations/20260814_entry_opportunity_evidence_v1.sql"
).read_text()
WRITER = (ROOT / "common/entry_opportunity_evidence.py").read_text()
EXECUTION_WRITER = (
    ROOT / "common/simulated_execution_evidence.py"
).read_text()


def test_a_fee_v2_cost_and_exact_break_even():
    result = evidence.cost_assumptions(
        Decimal("20"), Decimal("0.0035"), Decimal("0.0035"),
    )
    assert result["expected_round_trip_fee_usdc"] == Decimal("0.1400")
    assert result["expected_round_trip_fee_pct"] == Decimal("0.7000")
    assert result["break_even_move_pct"].quantize(Decimal("0.000001")) == Decimal(
        "0.702459"
    )


def test_b_snapshot_and_references_are_immutable_after_capture():
    assert "ENTRY_OPPORTUNITY_SNAPSHOT_IMMUTABLE" in MIGRATION
    assert "ENTRY_OPPORTUNITY_REFERENCE_IMMUTABLE" in MIGRATION
    assert "ON CONFLICT(decision_id) DO NOTHING" in WRITER
    assert "DO UPDATE SET" not in WRITER


def test_c_current_layer_refreshes_cannot_update_snapshot():
    immutable_trigger = MIGRATION.index(
        "CREATE TRIGGER trg_entry_opportunity_snapshot_immutable_v1"
    )
    outcome_view = MIGRATION.index(
        "CREATE OR REPLACE VIEW public.v_entry_opportunity_outcome_labels_v1"
    )
    assert immutable_trigger < outcome_view
    for relation in (
        "market_memory_sequence", "slot_brain_snapshot", "v_orc_picks_v5",
    ):
        assert relation in WRITER


def test_d_position_late_link_is_direct_and_deterministic():
    for relation in (
        "decision_registry_v1", "simulated_orders",
        "simulated_execution_fills_v1", "positions",
        "decision_replay_v1", "learning_feature_warehouse_v1",
    ):
        assert f"ALTER TABLE public.{relation}" in MIGRATION
    assert "link_entry_opportunity_position_fail_open_cursor" in EXECUTION_WRITER
    assert "WHERE id=%s AND entry_opportunity_snapshot_id IS NULL" in WRITER


def test_e_missing_layers_are_explicit_null_statuses():
    for status in (
        "MISSING_AT_ENTRY:NO_SIGNAL_EVENT",
        "MISSING_AT_ENTRY:NO_ACTIVE_MME_SEQUENCE",
        "MISSING_AT_ENTRY:NO_SLOT_BRAIN_SNAPSHOT",
        "MISSING_AT_ENTRY:NO_ORC_CONTROL_STATE",
        "MISSING_AT_ENTRY:NO_EXECUTION_QUALITY_MODEL",
    ):
        assert status in WRITER
    assert '"expected_move_pct": None' in WRITER
    assert '"expected_move_model_version": None' in WRITER


class _FailOpenCursor:
    def __init__(self):
        self.calls = []
        self.result = None

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split())
        self.calls.append((normalized, params))
        if "SELECT to_regclass" in normalized:
            self.result = [("entry_opportunity_evidence_v1",)]
        elif "SELECT environment,deployment_id" in normalized:
            self.result = [("trading_paper", "local-paper")]
        else:
            self.result = []

    def fetchone(self):
        return self.result[0] if self.result else None


def test_f_writer_failure_is_fail_open_and_marks_missing(monkeypatch):
    def fail(*_args, **_kwargs):
        raise RuntimeError("telemetry-only failure")

    monkeypatch.setattr(evidence, "capture_entry_opportunity_snapshot_cursor", fail)
    cur = _FailOpenCursor()
    assert evidence.capture_entry_opportunity_snapshot_fail_open_cursor(
        cur,
        decision_id="00000000-0000-0000-0000-000000000001",
        simulated_order_id=1,
        planned_entry_notional=Decimal("20"),
        fee_config=PaperSimulationFeeConfig(
            rate=Decimal("0.0035"), model_version="V2", config_source="test",
        ),
    ) is None
    rendered = "\n".join(query for query, _ in cur.calls)
    assert "ENTRY_OPPORTUNITY_EVIDENCE_MISSING" in rendered
    assert "ROLLBACK TO SAVEPOINT entry_opportunity_capture_v1" in rendered


def test_g_replay_and_warehouse_use_frozen_reference():
    assert "trg_decision_replay_entry_opportunity_propagate_v1" in MIGRATION
    assert "trg_learning_warehouse_entry_opportunity_propagate_v1" in MIGRATION
    assert "entry_opportunity_snapshot_id" in MIGRATION
    propagation = MIGRATION.split(
        "CREATE OR REPLACE FUNCTION public.propagate_entry_opportunity_reference_v1()",
        1,
    )[1].split("DROP TRIGGER IF EXISTS", 1)[0]
    assert "market_memory" not in propagation
    assert "slot_brain" not in propagation
    assert "v_orc_" not in propagation


def test_h_future_outcome_is_separate_from_entry_snapshot():
    table_contract = MIGRATION.split(
        "CREATE TABLE IF NOT EXISTS public.entry_opportunity_evidence_audit_v1", 1
    )[0]
    for field in (
        "actual_mfe_pct", "actual_mae_pct", "actual_net_pnl_usdc",
        "economic_viability_label",
    ):
        assert field not in table_contract
    assert "v_entry_opportunity_outcome_labels_v1" in MIGRATION
    assert "entry_opportunity_expected_move_v1_ck" in MIGRATION


def test_i_deployment_identity_is_validated_against_canonical_runtime():
    assert "decision_timestamp,environment," in WRITER
    assert "deployment_id,strategy" in WRITER
    assert "canonical_runtime_paper_provenance" in WRITER
    assert "validate_registry_runtime_provenance" in WRITER
    assert '"local-paper": ("trading_paper", "LOCAL")' in WRITER
    assert '"vps-paper": ("trading_paper", "VPS")' in WRITER


def test_j_hook_is_observational_and_does_not_change_order_result():
    assert "Evidence is observational" in EXECUTION_WRITER
    assert 'logging.exception("entry_opportunity_evidence_fail_open")' in EXECUTION_WRITER
    assert "return inserted_order_id" in EXECUTION_WRITER
    assert "return fill_id" in EXECUTION_WRITER


@pytest.mark.parametrize("bad_rate", [Decimal("-0.1"), Decimal("1")])
def test_invalid_fee_contract_is_not_fabricated(bad_rate):
    with pytest.raises(ValueError):
        evidence.cost_assumptions(Decimal("20"), bad_rate, Decimal("0.0035"))

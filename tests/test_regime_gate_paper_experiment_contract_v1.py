from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from common.decision_contract import DecisionReason, EvaluationContext, FinalDecision
from common.decision_observation import event_from_final_decision
from common.regime_gate import attach_regime_gate_event, decide_regime_gate


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "db/migrations/20260812_regime_gate_paper_experiment_contract_v1.sql").read_text()
TRANSPORT = (ROOT / "common/decision_observation_transport.py").read_text()


def _gate(mode: str):
    with patch("common.regime_gate.get_current_regime", return_value="TREND_UP"), patch(
        "common.regime_gate.get_policy", return_value=(False, "fixture")
    ):
        return decide_regime_gate(symbol="ETHUSDC", interval="1m", strategy="BBRANGE",
                                  decision="ENTRY_CHECK", regime_enabled=True, regime_mode=mode)


def test_01_dry_run_control_allowed_and_would_block():
    decision = _gate("DRY_RUN")
    assert decision.allow is True and decision.would_block is True
    assert decision.why == "POLICY_WOULD_BLOCK"


def test_02_shadow_enforce_is_blocked_by_treatment():
    assert _gate("ENFORCE").allow is False
    assert "BLOCKED_BY_TREATMENT" in SQL and "EXACT_DECISION_EFFECT" in SQL


def test_03_one_economic_owner_only():
    assert "CHECK (economic_owner_count=1)" in SQL
    assert SQL.count("CHECK (economic_owner_count=0)") >= 1


def test_04_no_duplicate_paper_exposure_contract():
    assert "shadow_economic_owner_count integer NOT NULL DEFAULT 0" in SQL
    assert "regime_gate_shadow_treatment_v1" in SQL
    assert "INSERT INTO positions" not in SQL and "INSERT INTO orders" not in SQL


def test_05_frozen_policy_fingerprint_is_exact():
    required = ("p_deployment_id", "p_environment", "p_symbol", "p_interval",
                "p_strategy", "p_regime", "p_allow_entry", "p_policy_version",
                "p_effective_from", "p_evidence_cutoff_at")
    assert all(item in SQL for item in required)
    assert "trg_regime_gate_snapshot_immutable_v1" in SQL


def test_06_historical_decision_not_attributed():
    assert "o.decision_created_at>=act.effective_from" in SQL
    assert "g.created_at>=act.effective_from" in SQL
    assert "o.decision_created_at>s.evidence_cutoff_at" in SQL


def test_07_future_decision_carries_exact_gate_identity():
    now = datetime.now(timezone.utc)
    ctx = EvaluationContext(deployment_id="local-paper", environment="trading_paper",
                            symbol="ETHUSDC", interval="1m", strategy="BBRANGE",
                            candle_open_time=now, evaluation_started_at=now,
                            engine_name="test", paper_mode=True)
    attached = attach_regime_gate_event(ctx, gate_event_id=17, decision=_gate("DRY_RUN"))
    final = FinalDecision.no_trade(attached, DecisionReason.NO_SIGNAL, finished_at=now)
    event = event_from_final_decision(final, event_id="4d06b3f7-f397-4553-960c-0ad963fa3836",
                                      decision_key="fixture", source_service="test", source_instance="test")
    assert event.regime_gate_event_id == 17


def test_08_wrong_deployment_rejected():
    assert "p_deployment_id NOT IN ('local-paper','vps-paper')" in SQL
    assert "REGIME_GATE_EXPERIMENT_SCOPE_MISMATCH" in SQL


def test_09_live_rejected_fail_closed():
    assert "environment text NOT NULL CHECK (environment='trading_paper')" in SQL
    assert "REGIME_GATE_EXPERIMENT_PAPER_ONLY" in SQL


def test_10_missing_approval_rejected():
    for field in ("approved_by", "approved_at", "approval_reference", "approval_reason"):
        assert f"s.{field} IS NULL" in SQL
    assert "REGIME_GATE_EXPERIMENT_APPROVAL_REQUIRED" in SQL


def test_11_overlapping_activation_rejected():
    assert "tstzrange" in SQL and "REGIME_GATE_EXPERIMENT_OVERLAP" in SQL


def test_12_replay_uses_frozen_policy_not_runtime_policy():
    body = SQL.split("CREATE OR REPLACE FUNCTION replay_regime_gate_experiment_v1", 1)[1]
    body = body.split("CREATE OR REPLACE FUNCTION persist_regime_gate_experiment_attribution_v1", 1)[0]
    assert "regime_gate_policy_snapshots_v1" in body
    assert "regime_policy" not in body


def test_13_policy_change_cannot_rewrite_history():
    assert "REGIME_GATE_EVIDENCE_APPEND_ONLY" in SQL
    assert "BEFORE UPDATE OR DELETE ON regime_gate_policy_snapshots_v1" in SQL


def test_14_deactivation_restores_future_unattributed_control():
    assert "deactivate_regime_gate_experiment_v1" in SQL
    assert "act.deactivated_at IS NULL OR o.decision_created_at<act.deactivated_at" in SQL


def test_15_equity_complete_permits_trusted_evaluation():
    assert "b.equity_status='COMPLETE'" in SQL and "THEN 'TRUSTED'" in SQL
    assert "paper_equity_baseline_v2" in SQL


def test_16_equity_incomplete_blocks_trusted_verdict():
    assert "ELSE 'BLOCKED' END effectiveness_verdict" in SQL
    assert "financial_truth_status IS DISTINCT FROM 'COMPLETE'" in SQL


def test_17_shadow_has_no_synthetic_financial_truth():
    shadow = SQL.split("CREATE TABLE IF NOT EXISTS regime_gate_shadow_treatment_v1", 1)[1]
    shadow = shadow.split("CREATE TABLE IF NOT EXISTS regime_gate_experiment_replay_v1", 1)[0]
    assert "financial_truth" not in shadow and "position_id" not in shadow
    assert "DIRECTIONAL_ONLY" in shadow and "NOT_EVALUABLE" in shadow


def test_18_attribution_and_replay_are_idempotent_and_projected():
    assert "ON CONFLICT(deployment_id,decision_key) DO NOTHING" in SQL
    assert "ON CONFLICT(attribution_id) DO NOTHING" in SQL
    assert "replay_regime_gate_experiment_v1(x_id)" in SQL
    assert TRANSPORT.index("INSERT INTO decision_replay_v1") < TRANSPORT.index(
        "persist_regime_gate_experiment_attribution_v1"
    )
    assert TRANSPORT.index("INSERT INTO learning_feature_warehouse_v1") < TRANSPORT.index(
        "persist_regime_gate_experiment_attribution_v1"
    )

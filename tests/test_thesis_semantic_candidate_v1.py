from __future__ import annotations

from pathlib import Path

from common.thesis_evidence_bundle import fingerprint
from common.thesis_semantic_candidate import (
    ACTIVE_RULE_FINGERPRINT,
    ACTIVE_RULE_SPEC,
    FORMING_RULE_FINGERPRINT,
    FORMING_RULE_SPEC,
    evaluate_active_candidate,
    evaluate_forming_candidate,
)


ROOT = Path(__file__).resolve().parents[1]


def _forming(**overrides):
    values = {
        "evidence_complete": True,
        "already_forming": False,
        "current_6h": 1,
        "current_24h": 2,
        "current_3d": 3,
        "previous_complete_6h": -1,
        "previous_complete_exists": True,
        "available_mme_directions": ("UP",),
    }
    values.update(overrides)
    return evaluate_forming_candidate(**values)


def _active(**overrides):
    values = {
        "evidence_complete": True,
        "forming_direction": "UP",
        "previous_bundle_exists": True,
        "previous_evidence_complete": True,
        "previous_semantic_evaluation_complete": True,
        "current_horizons": (1, 2, 3),
        "previous_horizons": (4, 5, 6),
        "available_mme_directions": ("UP",),
    }
    values.update(overrides)
    return evaluate_active_candidate(**values)


def test_forming_positive_case():
    result = _forming()
    assert result.evaluation_result == "MATCH"
    assert result.reason_code == "FORMING_ALIGNMENT_MATCH"
    assert result.direction == "UP"


def test_forming_rejects_opposite_mme():
    result = _forming(available_mme_directions=("UP", "DOWN"))
    assert result.evaluation_result == "NO_MATCH"
    assert result.reason_code == "MME_OPPOSITE_AVAILABLE"


def test_forming_rejects_previously_aligned_6h():
    result = _forming(previous_complete_6h=1)
    assert result.reason_code == "PREVIOUS_6H_ALREADY_ALIGNED"


def test_forming_rejects_24h_3d_disagreement():
    result = _forming(current_3d=-1)
    assert result.reason_code == "STRUCTURAL_24H_3D_DISAGREE"


def test_forming_rejects_zero_structural_direction():
    result = _forming(current_24h=0, current_3d=0)
    assert result.evaluation_result == "NO_MATCH"
    assert result.reason_code == "STRUCTURAL_DIRECTION_ZERO"


def test_forming_is_blocked_on_incomplete():
    result = _forming(evidence_complete=False)
    assert result.evaluation_result == "EVIDENCE_INCOMPLETE"
    assert result.reason_code == "EVIDENCE_INCOMPLETE"


def test_active_candidate_positive_adjacent_coherence():
    result = _active()
    assert result.evaluation_result == "MATCH"
    assert result.reason_code == "ACTIVE_ADJACENT_COHERENCE_MATCH"


def test_active_candidate_rejects_one_contradicting_horizon():
    result = _active(current_horizons=(1, -1, 3))
    assert result.reason_code == "CURRENT_HORIZONS_NOT_COHERENT"


def test_active_candidate_rejects_opposite_mme():
    result = _active(available_mme_directions=("UP", "DOWN"))
    assert result.reason_code == "MME_OPPOSITE_AVAILABLE"


def test_active_candidate_is_blocked_on_incomplete():
    result = _active(evidence_complete=False)
    assert result.evaluation_result == "EVIDENCE_INCOMPLETE"
    assert result.reason_code == "EVIDENCE_INCOMPLETE"


def test_rule_fingerprints_are_deterministic_and_environment_neutral():
    assert fingerprint(dict(FORMING_RULE_SPEC)) == FORMING_RULE_FINGERPRINT
    assert fingerprint(dict(ACTIVE_RULE_SPEC)) == ACTIVE_RULE_FINGERPRINT
    assert FORMING_RULE_FINGERPRINT != ACTIVE_RULE_FINGERPRINT
    assert all(key not in FORMING_RULE_SPEC for key in ("environment", "deployment_id"))
    assert all(key not in ACTIVE_RULE_SPEC for key in ("environment", "deployment_id"))


def test_contract_has_no_trading_final_decision_or_orc_sink():
    source = (ROOT / "common" / "thesis_semantic_candidate.py").read_text()
    migration = (
        ROOT / "db" / "migrations"
        / "20260819_thesis_semantic_candidate_freeze_v1.sql"
    ).read_text()
    combined = source + migration
    forbidden = (
        "FinalDecision", "INSERT INTO positions", "UPDATE positions",
        "INSERT INTO orders", "UPDATE orders", "INSERT INTO fills",
        "UPDATE fills", "UPDATE bot_control", "orc_apply", "ORC_ACTIONS",
        "THESIS_WEAKENING", "THESIS_INVALIDATED", "THESIS_ENDED",
    )
    assert all(token not in combined for token in forbidden)
    assert "ACTIVE_CANDIDATE" in combined
    assert "candidate_to_state = 'ACTIVE'" not in migration
    assert "BEFORE UPDATE OR DELETE" in migration

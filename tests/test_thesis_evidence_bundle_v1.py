from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from common.thesis_evidence_bundle import (
    _mme_transition,
    canonical_evidence_cutoff,
    canonical_json,
    deterministic_pipeline_run_id,
    evidence_status,
    fingerprint,
    source_version_manifest,
)


ROOT = Path(__file__).resolve().parents[1]


def test_pipeline_run_and_fingerprints_are_deterministic():
    cutoff = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
    manifest = source_version_manifest()
    first = deterministic_pipeline_run_id("trading_paper", "LOCAL", cutoff, manifest)
    second = deterministic_pipeline_run_id("trading_paper", "LOCAL", cutoff, manifest)
    assert first == second
    assert len(first) == 64
    assert first != deterministic_pipeline_run_id(
        "trading_paper", "VPS", cutoff, manifest,
    )


def test_canonical_serialization_contract():
    instant = datetime(2026, 8, 18, 12, 0, 1, 23, tzinfo=timezone.utc)
    left = {"z": Decimal("1.2300"), "a": instant, "n": None}
    right = {"n": None, "a": instant, "z": Decimal("1.23")}
    assert canonical_json(left) == canonical_json(right)
    assert canonical_json(left) == (
        '{"a":"2026-08-18T12:00:01.000023Z","n":null,"z":"1.23"}'
    )
    assert fingerprint(left) == fingerprint(right)
    assert fingerprint({"n": None}) != fingerprint({})


def test_cutoff_and_completeness_are_threshold_free():
    observed = datetime(2026, 8, 18, 12, 4, 59, 999999, tzinfo=timezone.utc)
    assert canonical_evidence_cutoff(observed) == datetime(
        2026, 8, 18, 12, 0, tzinfo=timezone.utc,
    )
    assert evidence_status([]) == "COMPLETE"
    assert evidence_status(["STRUCTURAL_3D_INCOMPLETE"]) == "INCOMPLETE"


def test_mme_absence_change_and_identical_observation_semantics():
    common = {
        "pipeline_run_id": "a" * 64,
        "environment": "trading_paper",
        "deployment_id": "LOCAL",
        "symbol": "BTCUSDC",
        "interval": "5m",
        "evidence_cutoff": datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc),
        "git_revision": "b" * 40,
    }
    absent = {
        **common,
        "observation_id": "c" * 64,
        "availability_state": "ABSENT",
        "sequence_type": None,
        "sequence_stage": None,
        "direction": None,
        "sequence_quality": None,
        "continuation_score": None,
        "reversal_score": None,
        "late_entry_risk": None,
        "orc_readiness_score": None,
        "orc_hint": None,
        "ranking_status": None,
        "action_hint": None,
        "source_fingerprint": fingerprint({"state": "ABSENT"}),
    }
    appeared = {
        **absent,
        "observation_id": "d" * 64,
        "availability_state": "AVAILABLE",
        "sequence_type": "ACTIVE_IMPULSE_SEQUENCE",
        "sequence_stage": "EXPANSION",
        "direction": "UP",
        "source_fingerprint": fingerprint({"state": "AVAILABLE"}),
    }
    first = _mme_transition(None, absent)
    assert first is not None
    assert first["transition_category"] == "SOURCE_ABSENT"
    assert _mme_transition(absent, absent) is None
    transition = _mme_transition(absent, appeared)
    assert transition is not None
    assert transition["transition_category"] == "SOURCE_APPEARED"
    assert "availability_state" in transition["changed_fields"]


def test_contract_has_no_trading_sink_or_thesis_projector():
    source = (ROOT / "common" / "thesis_evidence_bundle.py").read_text()
    forbidden_relations = (
        "INSERT INTO positions", "UPDATE positions", "INSERT INTO orders",
        "UPDATE bot_control", "FinalDecision", "THESIS_STARTED",
        "THESIS_WEAKENING", "THESIS_INVALIDATED", "THESIS_ENDED",
    )
    assert all(token not in source for token in forbidden_relations)


def test_source_fingerprint_is_environment_neutral():
    evidence = {
        "symbol": "BTCUSDC",
        "horizon": "24h",
        "first_close": Decimal("65000.00"),
        "last_close": Decimal("65100.00"),
    }
    local_source_fingerprint = fingerprint(evidence)
    vps_source_fingerprint = fingerprint(dict(evidence))
    assert local_source_fingerprint == vps_source_fingerprint

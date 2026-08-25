from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from common.paper_portfolio_replay_cutover import CONTRACT_VERSION, fingerprint


ROOT = Path(__file__).resolve().parents[1]


def test_contract_freezes_forward_only_non_economic_semantics():
    path = ROOT / "contracts/paper_portfolio_replay_cutover_v1_contract.json"
    contract = json.loads(path.read_text())
    assert contract["contract_version"] == CONTRACT_VERSION
    assert contract["forward_only"] is True
    assert contract["economic_baseline_reset"] is False
    assert contract["pre_cutover_replay"] == "UNSUPPORTED_FAIL_CLOSED"
    assert contract["legacy_reconstruction"] is False
    expected = (
        ROOT / "contracts/paper_portfolio_replay_cutover_v1_contract.sha256"
    ).read_text().strip()
    assert hashlib.sha256(path.read_bytes()).hexdigest() == expected


def test_migration_is_additive_append_only_and_has_no_backfill():
    migration = (
        ROOT / "db/migrations/20260825_paper_portfolio_replay_cutover_v1.sql"
    ).read_text().upper()
    assert "CREATE TABLE IF NOT EXISTS PUBLIC.PAPER_PORTFOLIO_REPLAY_CUTOVER_V1" in migration
    assert "PAPER_PORTFOLIO_REPLAY_CUTOVER_V1_APPEND_ONLY" in migration
    assert "DROP TABLE" not in migration
    assert "DROP COLUMN" not in migration
    assert "TRUNCATE" not in migration
    assert "INSERT INTO PUBLIC.POSITIONS" not in migration
    assert "INSERT INTO PUBLIC.SIMULATED_EXECUTION_FILLS_V1" not in migration


def test_cutover_fingerprint_forbids_float_and_is_deterministic():
    assert fingerprint({"qty": "0.123", "position_id": 1}) == fingerprint({
        "position_id": 1, "qty": "0.123",
    })
    with pytest.raises(ValueError, match="FLOAT_FORBIDDEN"):
        fingerprint({"qty": 0.123})

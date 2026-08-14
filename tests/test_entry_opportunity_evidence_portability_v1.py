from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
from decimal import Decimal

import pytest

import common.entry_opportunity_evidence as evidence
from common.entry_opportunity_evidence import (
    canonical_runtime_paper_provenance,
    cost_assumptions,
    validate_registry_runtime_provenance,
)


ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools/install_entry_opportunity_evidence_v1_portable.py"
SPEC = importlib.util.spec_from_file_location("entry_portability", TOOL_PATH)
assert SPEC and SPEC.loader
PORTABILITY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PORTABILITY)
ORIGINAL = ROOT / "db/migrations/20260814_entry_opportunity_evidence_v1.sql"


def _runtime(deployment: str, *, mode: str = "PAPER", environment: str = "paper"):
    return {
        "TRADING_MODE": mode,
        "ENVIRONMENT": environment,
        "DEPLOYMENT_ID": deployment,
    }


@pytest.mark.parametrize(
    ("runtime_deployment", "expected"),
    [
        ("local-paper", ("trading_paper", "LOCAL")),
        ("vps-paper", ("trading_paper", "VPS")),
    ],
)
def test_writer_maps_explicit_paper_runtime_provenance(runtime_deployment, expected):
    assert canonical_runtime_paper_provenance(_runtime(runtime_deployment)) == expected


@pytest.mark.parametrize(
    "runtime",
    [
        _runtime("local-live", mode="LIVE", environment="live"),
        _runtime("vps-paper", mode="LIVE", environment="live"),
        _runtime("UNKNOWN"),
        _runtime(""),
    ],
)
def test_writer_blocks_live_unknown_and_empty_runtime_identity(runtime):
    with pytest.raises(RuntimeError):
        canonical_runtime_paper_provenance(runtime)


def test_writer_rejects_registry_runtime_mismatch_without_fabrication():
    with pytest.raises(
        RuntimeError,
        match="ENTRY_OPPORTUNITY_RUNTIME_REGISTRY_IDENTITY_MISMATCH",
    ):
        validate_registry_runtime_provenance(
            "trading_paper",
            "LOCAL",
            runtime_provenance_provider=lambda: ("trading_paper", "VPS"),
        )


class _MismatchAuditCursor:
    def __init__(self):
        self.result = None
        self.calls = []

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split())
        self.calls.append((normalized, params))
        if "SELECT to_regclass" in normalized:
            self.result = [("entry_opportunity_evidence_v1",)]
        elif "SELECT environment,deployment_id" in normalized:
            self.result = [("trading_paper", "LOCAL")]
        else:
            self.result = []

    def fetchone(self):
        return self.result[0] if self.result else None


def test_inconsistent_writer_identity_is_audited_missing_and_fail_open(monkeypatch):
    def mismatch(*_args, **_kwargs):
        validate_registry_runtime_provenance(
            "trading_paper",
            "LOCAL",
            runtime_provenance_provider=lambda: ("trading_paper", "VPS"),
        )

    monkeypatch.setattr(evidence, "capture_entry_opportunity_snapshot_cursor", mismatch)
    cur = _MismatchAuditCursor()
    assert evidence.capture_entry_opportunity_snapshot_fail_open_cursor(
        cur,
        decision_id="00000000-0000-0000-0000-000000000002",
    ) is None
    rendered = "\n".join(query for query, _ in cur.calls)
    assert "ENTRY_OPPORTUNITY_EVIDENCE_MISSING" in rendered
    assert "INSERT INTO entry_opportunity_evidence_audit_v1" in rendered


@pytest.mark.parametrize(
    "target",
    [
        ("LIVE", "LOCAL", "local-live"),
        ("trading_live", "VPS", "vps-live"),
        ("PAPER", "UNKNOWN", "unknown"),
        ("PAPER", "", ""),
        ("PAPER", "LOCAL", "vps-paper"),
        ("PAPER", "VPS", "local-paper"),
    ],
)
def test_portable_installer_fails_closed_for_invalid_target_combinations(target):
    with pytest.raises(ValueError):
        PORTABILITY.validate_target_identity(*target)


def test_original_v1_checksum_and_schema_source_are_frozen():
    checksum = hashlib.sha256(ORIGINAL.read_bytes()).hexdigest()
    assert checksum == PORTABILITY.ORIGINAL_CHECKSUM
    assert checksum == (
        "ed6f0bd1f0ac22a0e540b960319a117e3850a858907d85b300e613677c28576d"
    )
    schema = PORTABILITY.original_schema_sql()
    assert "CREATE TABLE IF NOT EXISTS public.entry_opportunity_evidence_v1" in schema
    assert "INSERT INTO public.schema_migration_ledger_v1" not in schema


def test_fee_and_break_even_are_deployment_independent():
    local = cost_assumptions(20, "0.0035", "0.0035")
    vps = cost_assumptions(20, "0.0035", "0.0035")
    assert local == vps
    assert local["expected_round_trip_fee_pct"] == Decimal("0.7000")

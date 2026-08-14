from __future__ import annotations

import hashlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORRECTION = (
    ROOT
    / "db/migrations/20260814_entry_opportunity_evidence_v1_1_git_sha_provenance_correction.sql"
).read_text()
V1 = ROOT / "db/migrations/20260814_entry_opportunity_evidence_v1.sql"
V1_1 = (
    ROOT / "db/migrations/20260814_entry_opportunity_evidence_v1_1_portability.sql"
)


def test_original_entry_opportunity_migration_checksums_are_unchanged():
    assert hashlib.sha256(V1.read_bytes()).hexdigest() == (
        "ed6f0bd1f0ac22a0e540b960319a117e3850a858907d85b300e613677c28576d"
    )
    assert hashlib.sha256(V1_1.read_bytes()).hexdigest() == (
        "d95e976b434cde3facb7e35cc3e6bd05aa64d1d9248e3f9411e235ee58509c50"
    )


def test_correction_is_append_only_and_reuses_existing_provenance_contract():
    assert "migration_provenance_correction_v1" in CORRECTION
    assert "MIGRATION_PROVENANCE_CORRECTION_V1_IMMUTABLE" in CORRECTION
    assert "NON_CANONICAL_GIT_SHA_PROVENANCE_CORRECTION" in CORRECTION
    assert "UPDATE public.schema_migration_ledger_v1" not in CORRECTION
    assert "DELETE FROM public.schema_migration_ledger_v1" not in CORRECTION
    assert "DISABLE TRIGGER" not in CORRECTION
    assert "ALTER TABLE public.entry_opportunity_evidence_v1" not in CORRECTION


def test_correction_target_is_explicit_and_portable():
    assert "waltrade.target_environment" in CORRECTION
    assert "waltrade.target_deployment_id" in CORRECTION
    assert "waltrade.target_runtime_deployment_id" in CORRECTION
    assert "WHEN 'LOCAL' THEN 'local-paper'" in CORRECTION
    assert "WHEN 'VPS' THEN 'vps-paper'" in CORRECTION
    assert "ON CONFLICT (corrected_ledger_id) DO NOTHING" in CORRECTION

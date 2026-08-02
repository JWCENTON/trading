from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from common.legacy_fill_equivalence_proof import (
    APPLY_ENABLE_ENV,
    EXPECTED_INGESTION_IDS,
    EXPECTED_POSITION_IDS,
    LegacyFillEquivalenceProofService,
    ManifestProof,
    ProofManifest,
    RuntimeIdentity,
    canonical_fingerprint,
    canonical_json,
    decimal_text,
)


def _manifest_payload():
    positions = [3084, 3079, 3082, 3081, 3085, 3085, 3085, 3085]
    return {
        "proof_version": "LEGACY_FILL_EQUIVALENCE_PROOF_V1",
        "environment": "LIVE",
        "deployment_id": "local-live",
        "database": "trading_live",
        "proofs": [
            {
                "ingestion_id": ingestion_id,
                "position_id": position_id,
                "exchange_order_id": f"order-{position_id}",
                "exchange_trade_id": f"trade-{ingestion_id}",
                "canonical_local_fill_id": 1000 + ingestion_id,
                "correction_revision": 2,
                "latest_observed_fingerprint": f"{ingestion_id:064x}",
            }
            for ingestion_id, position_id in zip(
                sorted(EXPECTED_INGESTION_IDS), positions,
            )
        ],
    }


def test_canonical_fingerprint_is_stable_compact_utf8_and_decimal_exact():
    payload = {"z": None, "decimal": decimal_text("1.2300"), "a": "ą"}
    rendered = canonical_json(payload)
    assert rendered == '{"a":"\\u0105","decimal":"1.23","z":null}'
    assert canonical_fingerprint(payload) == hashlib.sha256(
        rendered.encode("utf-8")
    ).hexdigest()


def test_manifest_closes_exact_cohort_and_rejects_forbidden(tmp_path):
    payload = _manifest_payload()
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    manifest = ProofManifest.load(path)
    assert {row.ingestion_id for row in manifest.proofs} == EXPECTED_INGESTION_IDS
    assert {row.position_id for row in manifest.proofs} == EXPECTED_POSITION_IDS

    payload["proofs"][0]["ingestion_id"] = 22
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RuntimeError, match="UNEXPECTED_PROOF_COHORT"):
        ProofManifest.load(path)


def test_repository_manifest_is_exact_and_migration_is_additive():
    root = Path(__file__).resolve().parents[1]
    manifest = ProofManifest.load(
        root / "config/legacy_fill_equivalence_proof_v1.json"
    )
    assert {row.ingestion_id for row in manifest.proofs} == EXPECTED_INGESTION_IDS
    migration = (
        root / "db/migrations/20260802_legacy_fill_equivalence_proof_v1.sql"
    ).read_text(encoding="utf-8").upper()
    assert "LEGACY_FILL_EQUIVALENCE_PROOF_V1" in migration
    assert "BEFORE UPDATE OR DELETE" in migration
    assert "BEFORE TRUNCATE" in migration
    assert "UPDATE PUBLIC.EXCHANGE_FILL_INGESTION_STATE_V2" not in migration
    assert "UPDATE PUBLIC.BINANCE_ORDER_FILLS" not in migration
    assert "UPDATE PUBLIC.POSITIONS" not in migration


class NoConnection:
    def __call__(self):
        raise AssertionError("database must not be reached")


class NoExchange:
    place_order_calls = 0
    cancel_order_calls = 0

    def pending_spot_orders(self):
        raise AssertionError("exchange must not be reached")


def _service(runtime, *, database="trading_live"):
    manifest = ProofManifest(
        "LIVE", "local-live", "trading_live",
        tuple(
            ManifestProof(
                row["ingestion_id"], row["position_id"], row["exchange_order_id"],
                row["exchange_trade_id"], row["canonical_local_fill_id"], 2,
                row["latest_observed_fingerprint"],
            )
            for row in _manifest_payload()["proofs"]
        ),
    )
    return LegacyFillEquivalenceProofService(
        NoConnection(), NoExchange(), runtime, manifest,
        expected_git_sha="1" * 40, expected_database=database,
    )


@pytest.mark.parametrize(
    ("runtime", "error"),
    [
        (RuntimeIdentity("BINANCE", "LIVE", "local-live", "1" * 40, "PROCESS_SUPERVISOR"), "EXCHANGE_IDENTITY"),
        (RuntimeIdentity("OKX", "PAPER", "local-live", "1" * 40, "PROCESS_SUPERVISOR"), "TRADING_MODE_IDENTITY"),
        (RuntimeIdentity("OKX", "LIVE", "vps-live", "1" * 40, "PROCESS_SUPERVISOR"), "DEPLOYMENT_IDENTITY"),
        (RuntimeIdentity("OKX", "LIVE", "local-live", "2" * 40, "PROCESS_SUPERVISOR"), "GIT_SHA_IDENTITY"),
        (RuntimeIdentity("OKX", "LIVE", "local-live", "1" * 40, "WRITER"), "ORCHESTRATOR_ROLE"),
    ],
)
def test_runtime_identity_gates_fail_before_io(runtime, error):
    with pytest.raises(RuntimeError, match=error):
        _service(runtime).plan()


def test_apply_requires_independent_environment_flag(monkeypatch):
    monkeypatch.delenv(APPLY_ENABLE_ENV, raising=False)
    service = _service(
        RuntimeIdentity("OKX", "LIVE", "local-live", "1" * 40, "PROCESS_SUPERVISOR")
    )
    with pytest.raises(RuntimeError, match="APPLY_ENV_FLAG_DISABLED"):
        service.apply(
            apply_requested=True, environment="LIVE", deployment_id="local-live",
            database="trading_live", manifest_path="manifest.json",
        )

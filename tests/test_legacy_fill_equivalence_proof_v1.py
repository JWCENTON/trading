from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

import tools.legacy_fill_equivalence_proof_v1 as proof_cli
from common.legacy_fill_equivalence_proof import (
    APPLY_ENABLE_ENV,
    EXPECTED_INGESTION_IDS,
    EXPECTED_INGESTION_IDS_BY_DEPLOYMENT,
    EXPECTED_POSITION_IDS,
    EXPECTED_POSITION_IDS_BY_DEPLOYMENT,
    LegacyFillEquivalenceProofService,
    LOCAL_LIVE_DEPLOYMENT,
    ManifestProof,
    ProofManifest,
    RuntimeIdentity,
    SUPPORTED_DEPLOYMENTS,
    VPS_LIVE_DEPLOYMENT,
    canonical_fingerprint,
    canonical_json,
    decimal_text,
)


def _manifest_payload(deployment_id=LOCAL_LIVE_DEPLOYMENT):
    if deployment_id == VPS_LIVE_DEPLOYMENT:
        return {
            "proof_version": "LEGACY_FILL_EQUIVALENCE_PROOF_V1",
            "environment": "LIVE",
            "deployment_id": VPS_LIVE_DEPLOYMENT,
            "database": "trading_live",
            "proofs": [
                {
                    "ingestion_id": 41,
                    "position_id": 3096,
                    "exchange_order_id": "3759648872868290560",
                    "exchange_trade_id": "1171224",
                    "canonical_local_fill_id": 16123888,
                    "correction_revision": 2,
                    "latest_observed_fingerprint": (
                        "b215ec32a05577b0253c688408e725a916ced3312549d334a"
                        "cf3d275bdfbe14e"
                    ),
                },
                {
                    "ingestion_id": 47,
                    "position_id": 3094,
                    "exchange_order_id": "3758376674027315200",
                    "exchange_trade_id": "1167757",
                    "canonical_local_fill_id": 15451809,
                    "correction_revision": 2,
                    "latest_observed_fingerprint": (
                        "b53cc728a17371a15594d01261203e2ce0a5dcd5d93be7a8"
                        "6943f082b264e90c"
                    ),
                },
            ],
        }
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


def test_vps_manifest_is_closed_and_unknown_or_mixed_cohorts_are_rejected(
    tmp_path,
):
    path = tmp_path / "manifest.json"
    vps_payload = _manifest_payload(VPS_LIVE_DEPLOYMENT)
    path.write_text(json.dumps(vps_payload), encoding="utf-8")
    manifest = ProofManifest.load(path)
    assert {row.ingestion_id for row in manifest.proofs} == {41, 47}
    assert {row.position_id for row in manifest.proofs} == {3094, 3096}

    unknown = dict(vps_payload, deployment_id="unknown-live")
    path.write_text(json.dumps(unknown), encoding="utf-8")
    with pytest.raises(RuntimeError, match="DEPLOYMENT_IDENTITY_MISMATCH"):
        ProofManifest.load(path)

    mixed = json.loads(json.dumps(vps_payload))
    mixed["proofs"][0] = _manifest_payload()["proofs"][0]
    path.write_text(json.dumps(mixed), encoding="utf-8")
    with pytest.raises(RuntimeError, match="UNEXPECTED_PROOF_COHORT"):
        ProofManifest.load(path)


def test_repository_manifest_is_exact_and_migration_is_additive():
    root = Path(__file__).resolve().parents[1]
    local_manifest = ProofManifest.load(
        root / "config/legacy_fill_equivalence_proof_v1.json"
    )
    vps_manifest = ProofManifest.load(
        root / "config/legacy_fill_equivalence_proof_v1_vps_live.json"
    )
    assert {
        row.ingestion_id for row in local_manifest.proofs
    } == EXPECTED_INGESTION_IDS
    assert {
        row.position_id for row in local_manifest.proofs
    } == EXPECTED_POSITION_IDS
    assert [
        (
            row.ingestion_id, row.position_id, row.exchange_order_id,
            row.exchange_trade_id, row.canonical_local_fill_id,
            row.latest_observed_fingerprint, row.correction_revision,
        )
        for row in vps_manifest.proofs
    ] == [
        (
            41, 3096, "3759648872868290560", "1171224", 16123888,
            "b215ec32a05577b0253c688408e725a916ced3312549d334acf3d275bdfbe14e",
            2,
        ),
        (
            47, 3094, "3758376674027315200", "1167757", 15451809,
            "b53cc728a17371a15594d01261203e2ce0a5dcd5d93be7a86943f082b264e90c",
            2,
        ),
    ]
    assert SUPPORTED_DEPLOYMENTS == {"local-live", "vps-live"}
    assert EXPECTED_INGESTION_IDS_BY_DEPLOYMENT[VPS_LIVE_DEPLOYMENT] == {
        41, 47,
    }
    assert EXPECTED_POSITION_IDS_BY_DEPLOYMENT[VPS_LIVE_DEPLOYMENT] == {
        3094, 3096,
    }
    migration_v1 = (
        root / "db/migrations/20260802_legacy_fill_equivalence_proof_v1.sql"
    ).read_text(encoding="utf-8").upper()
    migration_v2 = (
        root / "db/migrations/"
        "20260802_legacy_fill_equivalence_proof_deployment_cohorts_v2.sql"
    ).read_text(encoding="utf-8").upper()
    assert "LEGACY_FILL_EQUIVALENCE_PROOF_V1" in migration_v1
    assert "BEFORE UPDATE OR DELETE" in migration_v1
    assert "BEFORE TRUNCATE" in migration_v1
    assert "DEPLOYMENT_ID='LOCAL-LIVE'" in migration_v1
    assert "DEPLOYMENT_ID IN ('LOCAL-LIVE', 'VPS-LIVE')" in migration_v2
    for migration in (migration_v1, migration_v2):
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


def _service(
    runtime, *, database="trading_live", deployment_id=LOCAL_LIVE_DEPLOYMENT,
):
    payload = _manifest_payload(deployment_id)
    manifest = ProofManifest(
        "LIVE", deployment_id, "trading_live",
        tuple(
            ManifestProof(
                row["ingestion_id"], row["position_id"], row["exchange_order_id"],
                row["exchange_trade_id"], row["canonical_local_fill_id"], 2,
                row["latest_observed_fingerprint"],
            )
            for row in payload["proofs"]
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


def test_vps_runtime_identity_selects_only_the_vps_manifest_before_io():
    runtime = RuntimeIdentity(
        "OKX", "LIVE", "vps-live", "1" * 40, "PROCESS_SUPERVISOR",
    )
    with pytest.raises(AssertionError, match="exchange must not be reached"):
        _service(runtime, deployment_id=VPS_LIVE_DEPLOYMENT).plan()


@pytest.mark.parametrize("deployment_id", ["unknown-live", "vps-live"])
def test_cli_rejects_unknown_or_manifest_mismatched_deployment_before_io(
    deployment_id,
):
    root = Path(__file__).resolve().parents[1]
    with pytest.raises(RuntimeError, match="DEPLOYMENT_IDENTITY_MISMATCH"):
        proof_cli.main([
            "--database", "trading_live",
            "--manifest",
            str(root / "config/legacy_fill_equivalence_proof_v1.json"),
            "--expected-git-sha", "1" * 40,
            "--deployment-id", deployment_id,
        ])


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

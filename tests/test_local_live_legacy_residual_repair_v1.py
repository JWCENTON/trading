from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from decimal import Decimal

import pytest

from common.inventory_quantity import (
    ExitInventoryStatus,
    InstrumentExecutionLimits,
    classify_exit_inventory,
    project_inventory_from_execution_evidence,
)
from common.local_live_legacy_residual_repair import (
    ALLOWED_POSITION_IDS,
    APPLY_ENABLE_ENV,
    BoundedResidualRepairService,
    EXPECTED_DATABASE,
    FINGERPRINT_CONTRACT_VERSION,
    FORBIDDEN_DB_ORDER_ROW_IDS,
    FORBIDDEN_EXCHANGE_ORDER_IDS,
    FORBIDDEN_INGESTION_IDS,
    MANIFEST_VERSION,
    ManifestPosition,
    PLACEHOLDER_FINGERPRINT,
    POSITION_UPDATE_ALLOWLIST,
    PROOF_CONTRACT_VERSION,
    RepairManifest,
    RunPlan,
    RuntimeIdentity,
    render_manifest_candidate,
    resolve_correction_trust,
    stable_equivalence_proof_evidence,
)
from common.legacy_recovery import semantic_repair_fingerprint
from tools.local_live_legacy_residual_repair import parser


EXPECTED = {
    3079: ("BTCUSDC", "0.00031545", "0.000001104075", "0.00031435", "0", "0.00000001", "0.0001"),
    3080: ("BNBUSDC", "0.035152", "0.000123032", "0.035029", "0", "0.000001", "0.001"),
    3081: ("ETHUSDC", "0.010623", "0.0000371805", "0.010585", "0.0000008195", "0.000001", "0.001"),
    3082: ("ETHUSDC", "0.010584", "0.000037044", "0.010547", "0", "0.000001", "0.001"),
    3083: ("SOLUSDC", "0.25921", "0.000907235", "0.25831", "0", "0.00001", "0.01"),
    3084: ("BTCUSDC", "0.00030191", "0.000001056685", "0.00030085", "0.000000003315", "0.00000001", "0.0001"),
    3085: ("SOLUSDC", "0.26924", "0.000942340", "0.26829", "0.000007660", "0.00001", "0.01"),
}


def fill(qty, fee, asset):
    return {
        "executed_qty": qty,
        "commission_amount": fee,
        "commission_asset": asset,
    }


@pytest.mark.parametrize("position_id", sorted(EXPECTED))
def test_seven_expected_inventory_shapes_use_canonical_c22(position_id):
    symbol, gross, fee, exit_qty, remaining, lot, minimum = EXPECTED[position_id]
    inventory = project_inventory_from_execution_evidence(
        symbol=symbol,
        entry_fills=[fill(gross, fee, symbol[:-4])],
        exit_fills=[fill(exit_qty, "0.01", "USDC")],
    )
    classification = classify_exit_inventory(
        previous_remaining_qty=inventory.net_entry_inventory_qty,
        cumulative_exit_inventory_reduction_qty=inventory.exit_inventory_reduction_qty,
        previous_cumulative_exit_inventory_reduction_qty=0,
        inventory=inventory,
        limits=InstrumentExecutionLimits(
            Decimal(lot), Decimal(minimum), Decimal("0"), None, True,
        ),
        tolerance=Decimal(lot),
    )
    assert classification.remaining_inventory_qty == Decimal(remaining)
    expected_status = (
        ExitInventoryStatus.FULLY_EXECUTED_CLOSE
        if Decimal(remaining) == 0
        else ExitInventoryStatus.TERMINAL_DUST_CLOSE
    )
    assert classification.status is expected_status


def _manifest_payload():
    return {
        "contract_version": "LOCAL_LIVE_LEGACY_RESIDUAL_REPAIR_V1",
        "environment": "LIVE",
        "deployment_id": "local-live",
        "positions": [
            {
                "position_id": position_id,
                "entry_order_id": f"entry-{position_id}",
                "exit_order_id": f"exit-{position_id}",
                "semantic_fingerprint": "0" * 64,
            }
            for position_id in sorted(ALLOWED_POSITION_IDS)
        ],
    }


def _closed_manifest_payload():
    payload = _manifest_payload()
    payload.update({
        "manifest_version": MANIFEST_VERSION,
        "generated_from_git_revision": "1" * 40,
        "generated_at": "2026-08-02T18:00:00+00:00",
        "fingerprint_contract_version": FINGERPRINT_CONTRACT_VERSION,
        "proof_contract_version": PROOF_CONTRACT_VERSION,
    })
    for index, row in enumerate(payload["positions"], start=1):
        row["semantic_fingerprint"] = f"{index:064x}"
    return payload


def test_manifest_is_closed_and_rejects_forbidden_incident(tmp_path):
    payload = _closed_manifest_payload()
    payload["positions"][0]["entry_order_id"] = next(iter(FORBIDDEN_EXCHANGE_ORDER_IDS))
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RuntimeError, match="FORBIDDEN_INCIDENT_IDENTITY"):
        RepairManifest.load(path)


def test_normal_manifest_rejects_placeholder_and_candidate_loader_is_explicit(
    tmp_path,
):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(_manifest_payload()), encoding="utf-8")
    with pytest.raises(RuntimeError, match="MANIFEST_FINGERPRINT_PLACEHOLDER"):
        RepairManifest.load(path)
    candidate = RepairManifest.load(path, allow_placeholders=True)
    assert {row.semantic_fingerprint for row in candidate.positions} == {
        PLACEHOLDER_FINGERPRINT,
    }


def test_manifest_rejects_missing_fingerprint_and_loads_closed_metadata(tmp_path):
    payload = _closed_manifest_payload()
    del payload["positions"][0]["semantic_fingerprint"]
    path = tmp_path / "missing.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RuntimeError, match="MANIFEST_POSITION_FIELDS_INVALID"):
        RepairManifest.load(path)

    payload = _closed_manifest_payload()
    path = tmp_path / "closed.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    manifest = RepairManifest.load(path)
    assert manifest.manifest_version == MANIFEST_VERSION
    assert manifest.generated_from_git_revision == "1" * 40
    assert manifest.fingerprint_contract_version == FINGERPRINT_CONTRACT_VERSION
    assert manifest.proof_contract_version == PROOF_CONTRACT_VERSION


def test_candidate_mode_cannot_combine_with_apply():
    with pytest.raises(SystemExit):
        parser().parse_args([
            "--apply", "--emit-manifest-candidate", "--manifest", "x",
            "--expected-git-sha", "1" * 40,
            "--expected-database", "trading_live",
        ])


def test_candidate_generation_is_one_time_and_rejects_closed_manifest():
    runtime = RuntimeIdentity(
        "OKX", "LIVE", "local-live", "1" * 40, "PROCESS_SUPERVISOR",
    )
    positions = tuple(
        ManifestPosition(
            position_id, f"entry-{position_id}", f"exit-{position_id}", "1" * 64,
        )
        for position_id in sorted(ALLOWED_POSITION_IDS)
    )
    service = BoundedResidualRepairService(
        NoConnection(), NoExchange(), runtime,
        RepairManifest("LIVE", "local-live", positions),
        expected_git_sha="1" * 40, expected_database=EXPECTED_DATABASE,
    )
    with pytest.raises(RuntimeError, match="CANDIDATE_REQUIRES_ALL_PLACEHOLDERS"):
        service.generate_manifest_candidate()


def test_stable_proof_evidence_ignores_sequence_id_and_sorts():
    base = {
        "ingestion_id": 8,
        "position_id": 3084,
        "proof_version": PROOF_CONTRACT_VERSION,
        "proof_type": "LEGACY_CANONICAL_OKX_EQUIVALENCE",
        "equivalence_state": "PROVEN",
        "proof_status": "VALID",
        "exchange_order_id": "order",
        "exchange_trade_id": "trade",
        "canonical_local_fill_id": 1,
        "latest_observed_fingerprint": "1" * 64,
        "canonical_fill_fingerprint": "2" * 64,
        "okx_truth_fingerprint": "2" * 64,
        "fill_mutation_required": False,
        "repair_impact": "NONE",
        "idempotency_key": "3" * 64,
    }
    first = stable_equivalence_proof_evidence({8: {**base, "proof_id": 1}})
    second = stable_equivalence_proof_evidence({8: {**base, "proof_id": 999}})
    assert first == second
    assert "proof_id" not in first[0]


def test_no_ingestion_history_uses_direct_canonical_okx_trust():
    trust, proofs = resolve_correction_trust(None, ())
    assert trust == "CANONICAL_OKX_DIRECT_EVIDENCE"
    assert proofs == {}


def test_semantic_payload_detects_economic_and_proof_drift():
    payload = {
        "position_before": {"id": 3079},
        "fills": [{"executed_qty": "1", "commission_amount": "0.01"}],
        "equivalence_proofs": [{"canonical_fill_fingerprint": "1" * 64}],
        "classification": {"status": "FULLY_EXECUTED_CLOSE"},
        "financial_truth": {"authoritative_net_pnl": "2.50"},
    }
    original = semantic_repair_fingerprint(payload)
    mutations = (
        ("fills", 0, "executed_qty", "2"),
        ("fills", 0, "commission_amount", "0.02"),
        ("equivalence_proofs", 0, "canonical_fill_fingerprint", "2" * 64),
    )
    for container, index, field, value in mutations:
        changed = deepcopy(payload)
        changed[container][index][field] = value
        assert semantic_repair_fingerprint(changed) != original
    changed = deepcopy(payload)
    changed["classification"]["status"] = "TERMINAL_DUST_CLOSE"
    assert semantic_repair_fingerprint(changed) != original
    changed = deepcopy(payload)
    changed["financial_truth"]["authoritative_net_pnl"] = "2.51"
    assert semantic_repair_fingerprint(changed) != original


def test_candidate_renderer_does_not_write_manifest(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("unchanged\n", encoding="utf-8")
    output = render_manifest_candidate(
        RunPlan((), (), (), False),
        generated_from_git_revision="1" * 40,
        generated_at=datetime(2026, 8, 2, tzinfo=timezone.utc),
    )
    assert json.loads(output)["candidate_manifest"]["generated_at"].endswith("+00:00")
    assert manifest_path.read_text(encoding="utf-8") == "unchanged\n"


def test_position_allowlist_excludes_all_economic_identity_fields():
    forbidden = {
        "entry_time", "entry_price", "entry_order_id", "exit_order_id",
        "symbol", "interval", "strategy", "side",
    }
    assert POSITION_UPDATE_ALLOWLIST.isdisjoint(forbidden)
    assert POSITION_UPDATE_ALLOWLIST == {
        "inventory_evidence_status", "gross_entry_executed_qty",
        "entry_base_fee_qty", "net_entry_inventory_qty",
        "cumulative_exit_executed_qty", "exit_inventory_reduction_qty",
        "remaining_inventory_qty", "qty", "terminal_dust_qty",
        "terminal_reason", "inventory_calculated_at", "status",
        "exit_price", "exit_time", "exit_reason",
    }
    assert FORBIDDEN_DB_ORDER_ROW_IDS == {3758, 3760, 3762}
    assert FORBIDDEN_INGESTION_IDS == {22, 23, 24, 25}


class NoConnection:
    def __call__(self):
        raise AssertionError("DB must not be reached before identity gates")


class NoExchange:
    place_order_calls = 0
    cancel_order_calls = 0

    def pending_spot_orders(self):
        raise AssertionError("exchange must not be reached before identity gates")


def service_for_gate(runtime):
    manifest = RepairManifest(
        "LIVE", "local-live",
        tuple(),
    )
    return BoundedResidualRepairService(
        NoConnection(), NoExchange(), runtime, manifest,
        expected_git_sha="1" * 40, expected_database=EXPECTED_DATABASE,
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
def test_runtime_identity_gates_fail_before_db(runtime, error):
    with pytest.raises(RuntimeError, match=error):
        service_for_gate(runtime).plan()


def test_apply_env_flag_is_independent_gate(monkeypatch):
    monkeypatch.delenv(APPLY_ENABLE_ENV, raising=False)
    runtime = RuntimeIdentity(
        "OKX", "LIVE", "local-live", "1" * 40, "PROCESS_SUPERVISOR",
    )
    with pytest.raises(RuntimeError, match="APPLY_ENV_FLAG_DISABLED"):
        service_for_gate(runtime).apply(
            apply_requested=True, environment="LIVE",
            deployment_id="local-live", manifest_path="manifest.json",
        )


def test_wrong_expected_database_fails_before_db():
    runtime = RuntimeIdentity(
        "OKX", "LIVE", "local-live", "1" * 40, "PROCESS_SUPERVISOR",
    )
    service = BoundedResidualRepairService(
        NoConnection(), NoExchange(), runtime,
        RepairManifest("LIVE", "local-live", tuple()),
        expected_git_sha="1" * 40, expected_database="not_trading_live",
    )
    with pytest.raises(RuntimeError, match="EXPECTED_DATABASE_IDENTITY"):
        service.plan()

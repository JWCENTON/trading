from __future__ import annotations

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
    FORBIDDEN_DB_ORDER_ROW_IDS,
    FORBIDDEN_EXCHANGE_ORDER_IDS,
    FORBIDDEN_INGESTION_IDS,
    POSITION_UPDATE_ALLOWLIST,
    RepairManifest,
    RuntimeIdentity,
)


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


def test_manifest_is_closed_and_rejects_forbidden_incident(tmp_path):
    payload = _manifest_payload()
    payload["positions"][0]["entry_order_id"] = next(iter(FORBIDDEN_EXCHANGE_ORDER_IDS))
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RuntimeError, match="FORBIDDEN_INCIDENT_IDENTITY"):
        RepairManifest.load(path)


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

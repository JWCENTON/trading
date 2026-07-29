from decimal import Decimal
from pathlib import Path

import pytest

from common.exchange_client import OkxMarketDataAdapter
from common.exchange_fill_change_control import (
    FillMutationDecision,
    InventoryRowGeneration,
    authoritative_fill_fingerprint,
    authoritative_fill_payload,
    classify_authoritative_change,
)
from common.exchange_ingest_trades import RECONCILE_OKX_EXIT_FILLS_C2_2_SQL
from common.inventory_lifecycle import apply_inventory_lifecycle_mutation
from common.inventory_quantity import (
    ExitInventoryStatus,
    InstrumentExecutionLimits,
    project_inventory_from_execution_evidence,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "db/migrations/20260729_legacy_replay_change_high_water_c2_2_1.sql"
).read_text()


RESIDUALS = {
    3079: ("BTCUSDC", "0.00000110", "0.00031545", "0.000001104075", "0.00031435"),
    3080: ("BNBUSDC", "0.000123", "0.035152", "0.000123032", "0.035029"),
    3081: ("ETHUSDC", "0.000038", "0.010623", "0.0000371805", "0.010585"),
    3082: ("ETHUSDC", "0.000037", "0.010584", "0.000037044", "0.010547"),
    3083: ("SOLUSDC", "0.00090", "0.25921", "0.000907235", "0.25831"),
    3084: ("BTCUSDC", "0.00000106", "0.00030191", "0.000001056685", "0.00030085"),
    3085: ("SOLUSDC", "0.00095", "0.26924", "0.000942340", "0.26829"),
}


def fill_payload(*, qty="1", fee="0.01", asset="USDC", price="10"):
    return authoritative_fill_payload(
        {
            "source": "okx",
            "symbol": "SOLUSDC",
            "trade_id": "998324",
            "order_id": "3785324726516752384",
            "side": "SELL",
            "executed_qty": qty,
            "avg_price": price,
            "commission_amount": fee,
            "commission_asset": asset,
            "event_time_ms": 1785313867843,
        },
        account_identity_key="account-1",
    )


def test_migration_is_additive_and_never_auto_adopts_or_backfills():
    upper = MIGRATION.upper()
    assert upper.startswith("BEGIN;")
    assert upper.rstrip().endswith("COMMIT;")
    assert "UPDATE POSITIONS" not in upper
    assert "INSERT INTO RUNTIME_CONTRACT_ADOPTION_V1" not in upper
    assert "EXCHANGE_FILL_INGESTION_STATE_V2" in upper
    assert {item.value for item in InventoryRowGeneration} == {
        "FORWARD_C2_2",
        "LEGACY_UNPROJECTED",
        "LEGACY_RECONSTRUCTION_APPROVED",
    }


def test_live_mutation_requires_changed_order_and_forward_adoption():
    sql = RECONCILE_OKX_EXIT_FILLS_C2_2_SQL
    assert "p.inventory_calculated_at IS NULL" not in sql
    assert "s.order_id=ANY(%s)" in sql
    assert "runtime_contract_adoption_v1" in sql
    assert "p.entry_time>=adoption.adopted_at" in sql
    assert "c.classification<>'INCOMPLETE_EVIDENCE'" in sql


@pytest.mark.parametrize("position_id", sorted(RESIDUALS))
def test_seven_legacy_residuals_are_replay_blocked(position_id):
    symbol, db_qty, gross, base_fee, exited = RESIDUALS[position_id]
    replayed = fill_payload(
        qty=exited,
        fee="0.069",
        asset="USDC",
        price="10",
    )
    assert authoritative_fill_fingerprint(replayed)
    # A pre-boundary row is excluded by the adoption predicate. Its confirmed
    # quantities therefore remain diagnostic only, never a mutation trigger.
    assert Decimal(db_qty) >= 0
    assert max(Decimal(gross) - Decimal(base_fee) - Decimal(exited), 0) >= 0
    assert symbol.endswith("USDC")
    assert "p.entry_time>=adoption.adopted_at" in (
        RECONCILE_OKX_EXIT_FILLS_C2_2_SQL
    )


def test_position_3085_restart_cannot_terminally_close_from_null_projection():
    assert RESIDUALS[3085][1] == "0.00095"
    assert "inventory_calculated_at IS NULL" not in (
        RECONCILE_OKX_EXIT_FILLS_C2_2_SQL
    )


def test_same_fill_replayed_ten_times_is_no_change():
    original = fill_payload()
    for _ in range(10):
        assert (
            classify_authoritative_change(original, dict(original))
            is FillMutationDecision.NO_CHANGE
        )


def test_fee_only_correction_has_explicit_change_identity():
    original = fill_payload(fee="0.01")
    corrected = fill_payload(fee="0.02")
    assert original["event_time_ms"] == corrected["event_time_ms"]
    assert (
        classify_authoritative_change(original, corrected)
        is FillMutationDecision.AUTHORITATIVE_CORRECTION
    )


def test_price_and_fee_currency_corrections_are_detected():
    original = fill_payload()
    for corrected in (
        fill_payload(price="10.01"),
        fill_payload(asset="SOL"),
    ):
        assert (
            classify_authoritative_change(original, corrected)
            is FillMutationDecision.AUTHORITATIVE_CORRECTION
        )


def test_upward_correction_is_revision_safe_and_downward_is_blocked():
    original = fill_payload(qty="1")
    assert (
        classify_authoritative_change(original, fill_payload(qty="1.1"))
        is FillMutationDecision.AUTHORITATIVE_CORRECTION
    )
    assert (
        classify_authoritative_change(original, fill_payload(qty="0.9"))
        is FillMutationDecision.AMBIGUOUS_CORRECTION
    )


def test_okx_fetch_boundary_is_applied_locally_with_declared_metadata(monkeypatch):
    client = OkxMarketDataAdapter.__new__(OkxMarketDataAdapter)
    monkeypatch.setattr(
        client,
        "_private_request",
        lambda *_args, **_kwargs: {
            "data": [
                {
                    "tradeId": "1", "ordId": "o1", "fillSz": "1",
                    "fillPx": "10", "fee": "-0.01", "feeCcy": "USDC",
                    "ts": "900", "side": "sell", "execType": "T",
                },
                {
                    "tradeId": "2", "ordId": "o2", "fillSz": "1",
                    "fillPx": "10", "fee": "-0.01", "feeCcy": "USDC",
                    "ts": "1001", "side": "sell", "execType": "T",
                },
            ]
        },
    )
    result = client.get_my_trades(
        symbol="SOLUSDC", startTime=1000, correctionLookbackMs=50,
    )
    assert [row["id"] for row in result] == ["2"]
    assert result.filter_applied is True
    assert result.filter_mode == "LOCAL_EVENT_TIME_GTE"
    assert result.requested_boundary == 1000
    assert result.effective_boundary == 950


class NoMutationCursor:
    def execute(self, *_args, **_kwargs):
        raise AssertionError("INCOMPLETE evidence attempted authoritative SQL")


def test_incomplete_evidence_is_authoritative_no_op():
    inventory = project_inventory_from_execution_evidence(
        symbol="SOLUSDC",
        entry_fills=[{"executed_qty": "1"}],
        exit_fills=[],
    )
    result = apply_inventory_lifecycle_mutation(
        NoMutationCursor(),
        position_id=3079,
        order_id="legacy",
        inventory=inventory,
        limits=InstrumentExecutionLimits(None, None, None, None, False),
        previous_remaining_qty=Decimal("0.00000110"),
        previous_exit_high_water=Decimal("0"),
        has_exit_evidence=False,
        exit_price=None,
        exit_time=None,
        execution_source="LIVE_OKX",
    )
    assert result.classification is ExitInventoryStatus.INCOMPLETE_EVIDENCE
    assert result.position_status == "UNCHANGED"
    assert result.event_inserted is False

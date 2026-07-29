from decimal import Decimal

from common.inventory_quantity import (
    ExitInventoryStatus,
    InstrumentExecutionLimits,
    InventoryEvidenceStatus,
    InventoryQuantity,
    classify_exit_inventory,
)


def fill(qty, fee, asset):
    return {
        "executed_qty": qty,
        "commission_amount": fee,
        "commission_asset": asset,
    }


def limits(*, lot="0.00001", minimum="0.01", notional="0", price="73.71"):
    return InstrumentExecutionLimits(
        Decimal(lot), Decimal(minimum), Decimal(notional),
        Decimal(price), True,
    )


def test_position_3085_terminal_dust_reproduction():
    inventory = InventoryQuantity.from_fills(
        symbol="SOLUSDC",
        entry_fills=[fill("0.269240000", "0.000942340", "SOL")],
        exit_fills=[fill("0.268290000", "0.06921479565", "USDC")],
    )
    assert inventory.evidence_status is InventoryEvidenceStatus.COMPLETE
    assert inventory.net_entry_inventory_qty == Decimal("0.268297660")
    assert inventory.remaining_inventory_qty == Decimal("0.000007660")

    result = classify_exit_inventory(
        previous_remaining_qty="0.268297660",
        cumulative_exit_inventory_reduction_qty="0.268290000",
        previous_cumulative_exit_inventory_reduction_qty="0",
        inventory=inventory,
        limits=limits(),
    )
    assert result.status is ExitInventoryStatus.TERMINAL_DUST_CLOSE
    assert result.dust_qty == Decimal("0.000007660")
    assert result.executable_inventory_qty == 0


def test_quote_and_third_asset_fees_do_not_reduce_base_inventory():
    for asset in ("USDC", "BNB"):
        inventory = InventoryQuantity.from_fills(
            symbol="SOLUSDC",
            entry_fills=[fill("1", "0.1", asset)],
        )
        assert inventory.entry_base_fee_qty == 0
        assert inventory.net_entry_inventory_qty == 1


def test_multiple_fills_and_base_fee_are_additive():
    inventory = InventoryQuantity.from_fills(
        symbol="ETHUSDC",
        entry_fills=[
            fill("0.6", "0.0006", "ETH"),
            fill("0.4", "0.0004", "ETH"),
        ],
        exit_fills=[fill("0.5", "0.01", "USDC")],
    )
    assert inventory.gross_entry_executed_qty == 1
    assert inventory.entry_base_fee_qty == Decimal("0.0010")
    assert inventory.remaining_inventory_qty == Decimal("0.4990")


def test_missing_fee_evidence_never_claims_complete():
    inventory = InventoryQuantity.from_fills(
        symbol="BTCUSDC",
        entry_fills=[{"executed_qty": "1"}],
    )
    assert inventory.evidence_status is InventoryEvidenceStatus.INCOMPLETE
    result = classify_exit_inventory(
        previous_remaining_qty="1",
        cumulative_exit_inventory_reduction_qty="0.5",
        previous_cumulative_exit_inventory_reduction_qty="0",
        inventory=inventory,
        limits=limits(),
    )
    assert result.status is ExitInventoryStatus.INCOMPLETE_EVIDENCE


def test_duplicate_high_water_is_no_new_fill():
    inventory = InventoryQuantity.from_fills(
        symbol="SOLUSDC",
        entry_fills=[fill("1", "0", "USDC")],
        exit_fills=[fill("0.5", "0", "USDC")],
    )
    result = classify_exit_inventory(
        previous_remaining_qty="0.5",
        cumulative_exit_inventory_reduction_qty="0.5",
        previous_cumulative_exit_inventory_reduction_qty="0.5",
        inventory=inventory,
        limits=limits(),
    )
    assert result.status is ExitInventoryStatus.NO_NEW_FILL

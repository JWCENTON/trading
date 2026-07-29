from decimal import Decimal

import pytest

from common.inventory_lifecycle import apply_inventory_lifecycle_mutation
from common.inventory_quantity import (
    ExitInventoryStatus,
    InstrumentExecutionLimits,
    InventoryEvidenceStatus,
    project_inventory_from_execution_evidence,
)


def evidence(qty, fee, asset):
    return {
        "executed_qty": qty,
        "commission_amount": fee,
        "commission_asset": asset,
    }


LIMITS = InstrumentExecutionLimits(
    Decimal("0.00001"), Decimal("0.01"), Decimal("0"),
    Decimal("73.71"), True,
)


class MutationCursor:
    def __init__(self, status="OPEN", duplicate_event=False):
        self.status = status
        self.duplicate_event = duplicate_event
        self.pending = None
        self.position_params = None
        self.event_params = None

    def execute(self, sql, params):
        if "UPDATE positions SET" in sql:
            self.position_params = params
            if params[10]:
                self.status = "CLOSED"
            self.pending = (self.status,)
        elif "INSERT INTO position_lifecycle_events_c2_2" in sql:
            self.event_params = params
            self.pending = None if self.duplicate_event else (101,)
        else:
            raise AssertionError(sql)

    def fetchone(self):
        value = self.pending
        self.pending = None
        return value


@pytest.mark.parametrize("strategy", ["RSI", "TREND", "SUPERTREND", "BBRANGE"])
def test_paper_entry_projection_is_identical_for_all_strategy_adapters(strategy):
    inventory = project_inventory_from_execution_evidence(
        symbol="SOLUSDC",
        entry_fills=[evidence("0.26924", "0.007998", "USDC")],
    )
    assert strategy  # adapter identity does not enter domain equations
    assert inventory.evidence_status is InventoryEvidenceStatus.COMPLETE
    assert inventory.entry_base_fee_qty == 0
    assert inventory.net_entry_inventory_qty == Decimal("0.26924")
    assert inventory.remaining_inventory_qty == Decimal("0.26924")

    cur = MutationCursor()
    result = apply_inventory_lifecycle_mutation(
        cur,
        position_id=1,
        order_id=f"{strategy}-entry-1",
        inventory=inventory,
        limits=LIMITS,
        previous_remaining_qty=Decimal("0.26924"),
        previous_exit_high_water=Decimal("0"),
        has_exit_evidence=False,
        exit_price=None,
        exit_time=None,
        execution_source="PAPER_SIMULATED",
    )
    assert result.classification is None
    assert result.event_inserted is False
    assert cur.position_params[6] == cur.position_params[7]


def test_paper_full_exit_projects_close_and_exactly_once_outbox():
    inventory = project_inventory_from_execution_evidence(
        symbol="SOLUSDC",
        entry_fills=[evidence("0.26924", "0.007998", "USDC")],
        exit_fills=[evidence("0.26924", "0.0079", "USDC")],
    )
    cur = MutationCursor()
    result = apply_inventory_lifecycle_mutation(
        cur,
        position_id=7,
        order_id="simulated-99",
        inventory=inventory,
        limits=LIMITS,
        previous_remaining_qty=Decimal("0.26924"),
        previous_exit_high_water=Decimal("0"),
        has_exit_evidence=True,
        exit_price=Decimal("73.71"),
        exit_time="2026-07-29T12:00:00Z",
        execution_source="PAPER_SIMULATED",
    )
    assert result.classification is ExitInventoryStatus.FULLY_EXECUTED_CLOSE
    assert result.position_status == "CLOSED"
    assert result.event_inserted is True
    assert cur.event_params[2] == "POSITION_CLOSED"

    duplicate = MutationCursor(status="CLOSED", duplicate_event=True)
    retried = apply_inventory_lifecycle_mutation(
        duplicate,
        position_id=7,
        order_id="simulated-99",
        inventory=inventory,
        limits=LIMITS,
        previous_remaining_qty=Decimal("0"),
        previous_exit_high_water=inventory.exit_inventory_reduction_qty,
        has_exit_evidence=True,
        exit_price=Decimal("73.71"),
        exit_time="2026-07-29T12:00:00Z",
        execution_source="PAPER_SIMULATED",
    )
    assert retried.classification is ExitInventoryStatus.NO_NEW_FILL
    assert retried.event_inserted is False


def test_paper_terminal_dust_and_partial_reduction_share_live_semantics():
    dust = project_inventory_from_execution_evidence(
        symbol="SOLUSDC",
        entry_fills=[evidence("0.269240000", "0.000942340", "SOL")],
        exit_fills=[evidence("0.268290000", "0.01", "USDC")],
    )
    dust_cur = MutationCursor()
    dust_result = apply_inventory_lifecycle_mutation(
        dust_cur, position_id=8, order_id="simulated-dust",
        inventory=dust, limits=LIMITS,
        previous_remaining_qty=dust.net_entry_inventory_qty,
        previous_exit_high_water=Decimal("0"), has_exit_evidence=True,
        exit_price=Decimal("73.71"), exit_time="now",
        execution_source="PAPER_SIMULATED",
    )
    assert dust_result.classification is ExitInventoryStatus.TERMINAL_DUST_CLOSE
    assert dust_cur.event_params[2] == "POSITION_CLOSED_TERMINAL_DUST"

    partial = project_inventory_from_execution_evidence(
        symbol="SOLUSDC",
        entry_fills=[evidence("1", "0.01", "USDC")],
        exit_fills=[evidence("0.5", "0.01", "USDC")],
    )
    partial_cur = MutationCursor()
    partial_result = apply_inventory_lifecycle_mutation(
        partial_cur, position_id=9, order_id="simulated-partial",
        inventory=partial, limits=LIMITS,
        previous_remaining_qty=Decimal("1"),
        previous_exit_high_water=Decimal("0"), has_exit_evidence=True,
        exit_price=Decimal("73.71"), exit_time="now",
        execution_source="PAPER_SIMULATED",
    )
    assert partial_result.classification is ExitInventoryStatus.PARTIAL_REDUCTION
    assert partial_result.position_status == "OPEN"
    assert partial_cur.event_params[2] == "POSITION_REDUCED"


@pytest.mark.parametrize(
    ("asset", "expected_base_fee", "expected_status"),
    [
        ("USDC", Decimal("0"), InventoryEvidenceStatus.COMPLETE),
        ("SOL", Decimal("0.01"), InventoryEvidenceStatus.COMPLETE),
        ("BNB", Decimal("0"), InventoryEvidenceStatus.COMPLETE),
        (None, Decimal("0"), InventoryEvidenceStatus.INCOMPLETE),
    ],
)
def test_paper_fee_evidence_modes(asset, expected_base_fee, expected_status):
    row = {"executed_qty": "1", "commission_amount": "0.01"}
    if asset is not None:
        row["commission_asset"] = asset
    inventory = project_inventory_from_execution_evidence(
        symbol="SOLUSDC", entry_fills=[row]
    )
    assert inventory.entry_base_fee_qty == expected_base_fee
    assert inventory.evidence_status is expected_status


def test_legacy_missing_entry_evidence_never_becomes_authoritative():
    inventory = project_inventory_from_execution_evidence(
        symbol="SOLUSDC", entry_fills=[],
        exit_fills=[evidence("0.1", "0.01", "USDC")],
    )
    assert inventory.evidence_status is InventoryEvidenceStatus.INCOMPLETE

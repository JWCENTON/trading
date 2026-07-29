from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path

import pytest

from common.exchange_fill_change_control import (
    FillMutationDecision,
    InventoryRowGeneration,
    RegisteredFillChange,
    classify_inventory_row_generation,
    is_existing_projected_c2_2_compatible,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "db/migrations/20260729_adoption_generation_lifecycle_c2_2_2.sql"
).read_text()


def projected(**overrides):
    row = {
        "inventory_evidence_status": "COMPLETE",
        "gross_entry_executed_qty": "1",
        "entry_base_fee_qty": "0.001",
        "net_entry_inventory_qty": "0.999",
        "cumulative_exit_executed_qty": "0.5",
        "exit_inventory_reduction_qty": "0.5",
        "remaining_inventory_qty": "0.499",
        "inventory_calculated_at": "2026-07-29T12:00:00Z",
        "qty": "0.499",
    }
    row.update(overrides)
    return row


def compatible(row=None, **kwargs):
    return is_existing_projected_c2_2_compatible(
        row or projected(),
        has_authoritative_entry_evidence=kwargs.get("has_entry", True),
        entry_evidence_gross_qty=kwargs.get("entry_gross", "1"),
        entry_evidence_base_fee_qty=kwargs.get("entry_fee", "0.001"),
        exit_evidence_gross_qty=kwargs.get("exit_gross", "0.5"),
    )


def test_projected_pre_boundary_complete_evidence_is_compatible():
    assert compatible()


@pytest.mark.parametrize(
    "row,kwargs",
    [
        (projected(inventory_evidence_status="INCOMPLETE"), {}),
        (projected(), {"has_entry": False}),
        (projected(), {"entry_gross": None}),
        (projected(net_entry_inventory_qty="0.998"), {}),
        (projected(qty="0.498"), {}),
        (projected(exit_inventory_reduction_qty="0.4"), {}),
        (projected(inventory_calculated_at=None), {}),
        (projected(), {"exit_gross": "0.4"}),
    ],
)
def test_false_projected_rows_fail_closed(row, kwargs):
    assert not compatible(row, **kwargs)


@pytest.mark.parametrize(
    "generation,decision,expected",
    [
        (
            InventoryRowGeneration.FORWARD_C2_2,
            FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE,
            True,
        ),
        (
            InventoryRowGeneration.FORWARD_C2_2,
            FillMutationDecision.AUTHORITATIVE_CORRECTION,
            True,
        ),
        (
            InventoryRowGeneration.EXISTING_PROJECTED_C2_2,
            FillMutationDecision.AUTHORITATIVE_CORRECTION,
            True,
        ),
        (
            InventoryRowGeneration.LEGACY_UNPROJECTED,
            FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE,
            False,
        ),
        (
            InventoryRowGeneration.ADOPTION_GENERATION_MISMATCH,
            FillMutationDecision.AUTHORITATIVE_CORRECTION,
            False,
        ),
        (
            InventoryRowGeneration.FORWARD_C2_2,
            FillMutationDecision.AMBIGUOUS_CORRECTION,
            False,
        ),
        (
            InventoryRowGeneration.EXISTING_PROJECTED_C2_2,
            FillMutationDecision.NO_CHANGE,
            False,
        ),
    ],
)
def test_mutation_gate_requires_generation_and_accepted_evidence(
    generation, decision, expected,
):
    change = RegisteredFillChange(
        1, decision, "fingerprint", 0, generation, 7, 2
    )
    assert change.permits_mutation is expected


def test_explicit_fail_closed_mutation_decisions():
    mismatch = RegisteredFillChange(
        1, FillMutationDecision.AUTHORITATIVE_CORRECTION, "f", 1,
        InventoryRowGeneration.ADOPTION_GENERATION_MISMATCH, 1, 1,
    )
    legacy = RegisteredFillChange(
        2, FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE, "f", 0,
        InventoryRowGeneration.LEGACY_UNPROJECTED, 1, 1,
    )
    inactive = RegisteredFillChange(
        3, FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE, "f", 0,
    )
    projected = RegisteredFillChange(
        4, FillMutationDecision.AUTHORITATIVE_CORRECTION, "f", 1,
        InventoryRowGeneration.EXISTING_PROJECTED_C2_2, 2, 2,
    )
    assert (
        mismatch.mutation_decision
        is FillMutationDecision.ADOPTION_GENERATION_MISMATCH
    )
    assert (
        legacy.mutation_decision
        is FillMutationDecision.LEGACY_RECONSTRUCTION_BLOCKED
    )
    assert inactive.mutation_decision is FillMutationDecision.ADOPTION_NOT_ACTIVE
    assert (
        projected.mutation_decision
        is FillMutationDecision.EXISTING_PROJECTED_EVIDENCE
    )


def test_migration_is_additive_and_has_generation_lifecycle():
    upper = MIGRATION.upper()
    assert upper.startswith("BEGIN;")
    assert upper.rstrip().endswith("COMMIT;")
    assert "UPDATE POSITIONS" not in upper
    assert "SELECT PREPARE_CONTRACT_ADOPTION(" not in upper
    assert "SELECT ACTIVATE_CONTRACT_ADOPTION(" not in upper
    assert "WHERE STATUS = 'ACTIVE'" in upper
    assert "PREPARE_CONTRACT_ADOPTION" in upper
    assert "ACTIVATE_CONTRACT_ADOPTION" in upper
    assert "ROLLBACK_CONTRACT_ADOPTION" in upper
    assert "SUPERSEDE_CONTRACT_ADOPTION" in upper


def test_decimal_tolerance_is_explicit():
    assert compatible(projected(qty=Decimal("0.4990000000005")))
    assert not compatible(projected(qty=Decimal("0.499000000002")))


def test_rollback_gap_row_is_not_reclassified_by_next_generation():
    utc = timezone.utc
    generation_2_boundary = datetime(2026, 7, 29, 15, 0, tzinfo=utc)
    old_runtime_row = datetime(2026, 7, 29, 14, 30, tzinfo=utc)
    new_runtime_row = datetime(2026, 7, 29, 15, 1, tzinfo=utc)
    common = {
        "active_adopted_at": generation_2_boundary,
        "active_adoption_id": 2,
        "active_generation": 2,
        "position_adoption_id": None,
        "position_generation": None,
        "existing_projected_compatible": False,
    }
    assert classify_inventory_row_generation(
        entry_time=old_runtime_row, **common
    ) is InventoryRowGeneration.LEGACY_UNPROJECTED
    assert classify_inventory_row_generation(
        entry_time=new_runtime_row, **common
    ) is InventoryRowGeneration.FORWARD_C2_2
    assert classify_inventory_row_generation(
        entry_time=new_runtime_row,
        **{
            **common,
            "position_adoption_id": 1,
            "position_generation": 1,
        },
    ) is InventoryRowGeneration.ADOPTION_GENERATION_MISMATCH

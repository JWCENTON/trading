from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from common.inventory_quantity import (
    ExitInventoryStatus,
    InstrumentExecutionLimits,
    InventoryQuantity,
    classify_exit_inventory,
)


@dataclass(frozen=True)
class InventoryMutationResult:
    position_id: int
    classification: ExitInventoryStatus | None
    event_inserted: bool
    position_status: str


def apply_inventory_lifecycle_mutation(
    cur,
    *,
    position_id: int,
    order_id: str,
    inventory: InventoryQuantity,
    limits: InstrumentExecutionLimits,
    previous_remaining_qty: Decimal,
    previous_exit_high_water: Decimal,
    has_exit_evidence: bool,
    exit_price: Decimal | None,
    exit_time,
    execution_source: str,
) -> InventoryMutationResult:
    """Atomically project inventory and append committed lifecycle evidence.

    The caller owns the transaction containing execution-fill persistence.
    """
    classification = None
    if has_exit_evidence:
        classification = classify_exit_inventory(
            previous_remaining_qty=previous_remaining_qty,
            cumulative_exit_inventory_reduction_qty=(
                inventory.exit_inventory_reduction_qty
            ),
            previous_cumulative_exit_inventory_reduction_qty=(
                previous_exit_high_water
            ),
            inventory=inventory,
            limits=limits,
        )

    terminal = classification and classification.status in {
        ExitInventoryStatus.FULLY_EXECUTED_CLOSE,
        ExitInventoryStatus.TERMINAL_DUST_CLOSE,
    }
    terminal_dust = bool(
        classification
        and classification.status is ExitInventoryStatus.TERMINAL_DUST_CLOSE
    )
    evidence_status = inventory.evidence_status.value
    remaining = max(inventory.remaining_inventory_qty, Decimal("0"))

    cur.execute(
        """
        UPDATE positions SET
          inventory_evidence_status=%s,
          gross_entry_executed_qty=%s,
          entry_base_fee_qty=%s,
          net_entry_inventory_qty=%s,
          cumulative_exit_executed_qty=%s,
          exit_inventory_reduction_qty=%s,
          remaining_inventory_qty=%s,
          qty=%s,
          terminal_dust_qty=%s,
          terminal_reason=%s,
          inventory_calculated_at=clock_timestamp(),
          status=CASE WHEN %s THEN 'CLOSED' ELSE status END,
          exit_price=CASE WHEN %s THEN COALESCE(%s,exit_price) ELSE exit_price END,
          exit_time=CASE WHEN %s THEN COALESCE(%s,exit_time) ELSE exit_time END,
          exit_reason=CASE
            WHEN %s THEN 'TERMINAL_DUST'
            ELSE exit_reason
          END
        WHERE id=%s
        RETURNING status
        """,
        (
            evidence_status,
            inventory.gross_entry_executed_qty,
            inventory.entry_base_fee_qty,
            inventory.net_entry_inventory_qty,
            inventory.cumulative_exit_executed_qty,
            inventory.exit_inventory_reduction_qty,
            remaining,
            remaining,
            remaining if terminal_dust else None,
            "TERMINAL_DUST" if terminal_dust else None,
            bool(terminal),
            bool(terminal),
            exit_price,
            bool(terminal),
            exit_time,
            terminal_dust,
            int(position_id),
        ),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("inventory mutation position missing")
    position_status = str(row[0])

    event_inserted = False
    event_kind = None
    if classification is not None:
        event_kind = {
            ExitInventoryStatus.PARTIAL_REDUCTION: "POSITION_REDUCED",
            ExitInventoryStatus.FULLY_EXECUTED_CLOSE: "POSITION_CLOSED",
            ExitInventoryStatus.TERMINAL_DUST_CLOSE: (
                "POSITION_CLOSED_TERMINAL_DUST"
            ),
        }.get(classification.status)
    if event_kind:
        cur.execute(
            """
            INSERT INTO position_lifecycle_events_c2_2(
              position_id,order_id,mutation_kind,mutation_high_water,payload
            ) VALUES (
              %s,%s,%s,%s,
              jsonb_build_object(
                'position_id',%s,'order_id',%s,
                'gross_entry_executed_qty',%s,
                'entry_base_fee_qty',%s,
                'net_entry_inventory_qty',%s,
                'cumulative_exit_executed_qty',%s,
                'remaining_inventory_qty',%s,
                'terminal_dust_qty',%s,
                'dust_qty',%s,
                'terminal_reason',%s,
                'lotSz',%s,'minSz',%s,'min_notional',%s,
                'financial_truth_status','UNKNOWN',
                'execution_source',%s
              )
            )
            ON CONFLICT DO NOTHING
            RETURNING event_id
            """,
            (
                int(position_id), str(order_id), event_kind,
                inventory.exit_inventory_reduction_qty,
                int(position_id), str(order_id),
                inventory.gross_entry_executed_qty,
                inventory.entry_base_fee_qty,
                inventory.net_entry_inventory_qty,
                inventory.cumulative_exit_executed_qty,
                remaining,
                remaining if terminal_dust else Decimal("0"),
                remaining if terminal_dust else Decimal("0"),
                "TERMINAL_DUST" if terminal_dust else None,
                limits.lot_size, limits.min_size, limits.min_notional,
                str(execution_source),
            ),
        )
        event_inserted = cur.fetchone() is not None

    return InventoryMutationResult(
        int(position_id),
        classification.status if classification else None,
        event_inserted,
        position_status,
    )

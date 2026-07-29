# Fee-aware inventory and terminal dust lifecycle C2.2

## Contract

For authoritative fills:

```text
net_entry_inventory_qty
  = gross_entry_executed_qty - entry_base_fee_qty

remaining_inventory_qty
  = net_entry_inventory_qty - exit_inventory_reduction_qty
```

Only fees whose authoritative currency equals the instrument base asset reduce
entry inventory. Quote-asset and third-asset fees do not. Missing fee currency
or quantity makes inventory evidence `INCOMPLETE`; no value is inferred or
coalesced to an authoritative zero.

`positions.qty` remains a compatibility/runtime projection. C2.2 rows retain
the monotonic execution evidence and explicit remaining inventory separately.
The migration performs no historical backfill.

The mode-neutral domain entry point is
`project_inventory_from_execution_evidence(...)`. Its adapters are:

```text
LIVE  -> authoritative binance_order_fills/OKX evidence
PAPER -> simulated_execution_fills_v1 evidence
```

PAPER preserves its existing quote-asset fee model (`PAPER_FEE_RATE=0.0004`).
Its normal entry base fee is therefore zero and net entry inventory equals
gross executed quantity. Base-fee fixtures do not change PAPER economics.

## Exit classification

The shared classifier returns exactly one of:

```text
FULLY_EXECUTED_CLOSE
TERMINAL_DUST_CLOSE
PARTIAL_REDUCTION
NO_NEW_FILL
INCOMPLETE_EVIDENCE
```

Terminal dust uses remaining inventory, lot size, minimum size, minimum
notional, fresh price evidence, and numeric tolerance. It never submits an
additional order. A terminal-dust close stores the unsold quantity in
`terminal_dust_qty`, sets `terminal_reason=TERMINAL_DUST`, and closes only the
trading lifecycle.

## Telemetry

Committed mutations append one unique row to
`position_lifecycle_events_c2_2`:

```text
POSITION_REDUCED
POSITION_CLOSED
POSITION_CLOSED_TERMINAL_DUST
```

The unique mutation identity is position, order, event kind, and cumulative
exit high-water. The outbox insert and position update share one transaction.

For PAPER, all four strategies already call the shared
`record_simulated_fill_evidence(...)` adapter. Simulated-fill insertion,
inventory projection, and outbox insertion commit together. The existing
simulated-order guard and position materialization precede that transaction; a
crash in this narrow interval remains detectable as missing fill evidence and
must never be inferred as authoritative.

## Financial Truth

Financial Truth remains independent of position lifecycle. The existing
calculator already consumes gross entry, base fee, net inventory, exit
reduction, and remaining inventory. A terminal-dust position may become
`COMPLETE` only when fills, fee valuation, account identity, instrument
metadata, and the closed lifecycle are complete. Otherwise it remains
`INCOMPLETE`; C2.2 adds no zero fallback.

## Rollback

Runtime rollback is the previous image. Schema columns and the event outbox are
additive and should remain in place during rollback. Dropping them would remove
audit evidence and is outside the operational rollback boundary.

## Legacy and rollout

Rows created before C2.2 retain NULL inventory fields. A legacy OPEN position
is projected only from complete existing simulated fills. Missing entry
evidence yields `INCOMPLETE_EVIDENCE`, without an authoritative close event.

Rollout order is tests, LOCAL PAPER migration/build/observation, then a separate
LOCAL LIVE review. This document states implementation readiness, not completed
PAPER runtime validation.

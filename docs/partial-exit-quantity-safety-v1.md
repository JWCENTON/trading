# Partial Exit Quantity Safety V1

## Scope and root cause

This change covers live exits in RSI, TREND, SUPERTREND, and BBRANGE. Previously,
some callers treated `live_ok`, `executed`, or `operation_succeeded` as permission
to close the whole local position. Those fields can also describe a confirmed
partial fill, so the exchange position and the local SSOT could diverge.

Entry behavior, signals, strategy parameters, sizing, ORC, configuration, and
deployment are outside this change.

## Execution contract

| Producer/path | Field | Meaning | Quantity form | Unit | Partial/retry behavior | Evidence |
|---|---|---|---|---|---|---|
| `place_live_order` | `requested_qty` | submitted order quantity | request | base asset | fixed | request |
| `place_live_order` | `executed_qty` | quantity executed for the order at observation time | cumulative per order | base asset | may be partial and repeat | exchange order status |
| `place_live_order` | `fill_evidence` | order identity plus executed high-water | cumulative per order | base asset | repeat-safe | exchange `order_id` and client ID |
| maker exit | `executed_qty` | maker plus fallback execution | aggregate across legs | base asset | may be partial | maker and market responses |
| maker exit | `fill_evidence` | separate high-water for every order leg | cumulative per leg | base asset | repeat-safe | each leg's order ID |
| normalized result | `executed` | at least one confirmed unit executed | boolean | n/a | can describe partial | normalized response |
| normalized result | `fully_executed` | requested quantity reached within existing precision tolerance | boolean | n/a | false for partial | status and quantities |
| normalized result | `live_ok` | legacy caller gate | boolean | n/a | forced false after an applied partial | quantity helper result |

`executed_qty` is not assumed to be a new delta. The durable order high-water is
subtracted before the local position is changed.

## Position and mutation semantics

`positions.qty` is the current remaining open quantity. A single transaction:

1. inserts or finds every exit order by `(exchange_source, symbol, order_id)`;
2. locks its `binance_orders` row and calculates
   `max(0, cumulative_qty - reconciled_executed_qty)`;
3. locks the open position;
4. subtracts only the sum of new deltas, clamped to the current quantity;
5. advances each order high-water and commits both changes together.

Order locks are acquired in deterministic order and before the position lock,
matching exit-fill reconciliation. Missing or conflicting durable order identity
fails closed instead of performing a non-idempotent read/update.

The helper returns `NO_FILL`, `PARTIALLY_REDUCED`, `FULLY_CLOSED`,
`ALREADY_APPLIED`, or `CONFLICT`. A remaining quantity within 0.1% of the current
quantity (with an absolute floor of `1e-12`) is clamped to zero and closed.
Quantity never becomes negative or increases.

## Zero, partial, and full matrix

| Confirmed result | Position mutation | Status | Event | FinalDecision |
|---|---|---|---|---|
| zero fill | none | unchanged | existing failure/suppression events | existing failure/suppression subtype |
| new partial delta | subtract delta once | `OPEN` | `POSITION_REDUCED`, `execution_status=PARTIAL` | `PARTIAL_EXECUTION` |
| full remaining fill | existing full-close path | `CLOSED` | existing full-close event | existing full-exit outcome |
| duplicate cumulative fill | none | unchanged | no full-close event | partial observation remains non-closing |

Partial fills never emit `POSITION_CLOSED` or `PAPER_POSITION_CLOSED`. Existing
full-exit reason strings and full-exit behavior are preserved.

## Retry and reconciliation

Strategy-side mutation and OKX fill reconciliation share
`binance_orders.reconciled_executed_qty`. Therefore either component may process a
fill first; the second sees a zero delta. A later larger cumulative observation
applies only the increase. Distinct maker and market order IDs are accounted for
independently.

The reconciler now reduces an open position for a partial delta and closes it only
when that delta consumes the current remaining quantity within the same 99.9%
threshold. Its order high-water update and position mutation remain one database
transaction.

## Test matrix

Characterization coverage exercises zero, partial, and full exit outcomes across
all four strategies, including panic and representative managed-exit reasons,
single execution attempts, `RUN_END`, event semantics, and FinalDecision mapping.
Shared tests cover cumulative growth, duplicate retries, distinct orders,
multi-leg maker/fallback fills, non-negative clamping, and both strategy/reconciler
processing orders.

## Known limitations, rollout, and rollback

This version depends on the existing `binance_orders` linkage and
`reconciled_executed_qty`; no schema migration is introduced. A confirmed partial
response without an exchange order ID is deliberately rejected for immediate
mutation and must be recovered by normal fill ingestion/reconciliation.

The code change has no rollout, runtime restart, or configuration requirement in
this task. If rolled back later, any quantities already reduced remain valid SSOT
state, but the old code would again be unsafe for subsequent partial exits; rollback
should therefore be paired with suppression of live exits or an equivalent safety
control.

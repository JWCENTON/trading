# Pending Entry Fill Reconciliation V1

## Safety invariant

A LIVE entry ACK is not a fill. Reconciliation may change `positions` only
when the chain `accepted order -> positive exchange fill -> exact position`
is unambiguous. Requested quantity is metadata and is never used as executed
quantity. The reconciler performs no exchange request, order placement, retry
order, runtime DDL or decision persistence.

## Exact identity

The order/fill identity is:

`normalize(source) + symbol + exchange order_id`.

`normalize_exchange_source(value)` applies `(value or "").strip().lower()` in
the executor, strategy event metadata and fill ingest. Automatic entry recovery
also requires `order_purpose=ENTRY`, `order_accepted=true`, `strategy`,
`interval`, one fill side matching the accepted order side and positive fill
quantity.

An existing position is exact only when it has the same symbol/strategy/
interval and matches `entry_order_id`, `entry_client_order_id`, or a previously
trusted `reconciled_position_id`. Slot-only matching is forbidden. An unrelated
OPEN position makes the order `OPEN_POSITION_ORDER_MISMATCH`; it is never
updated and its ID is not copied to the order.

## Source-aware uniqueness and rollout pre-audit

The historical `ux_binance_orders_symbol_order_id` constraint is replaced by:

- `ux_binance_orders_source_symbol_order_id` for the canonical three-column
  exchange identity;
- `ux_binance_orders_legacy_null_source_symbol_order_id` for old writers that
  still insert `exchange_source=NULL` during a mixed rollout.

Before changing the constraint, the migration aborts if non-null source-aware
duplicates already exist. No ambiguous source is backfilled. Rollout must first
run the migration precheck on a database clone and inspect all legacy NULL-source
orders. The new trigger uses a source-aware conflict target; legacy events with
no source use conflict-safe insert without automatic reconciliation.

## Partial fills and fees

Positive fills are unique by `(source, trade_id)` and aggregated by
`(source, symbol, order_id)`:

- qty: `SUM(executed_qty)`;
- price: `SUM(qty * price) / SUM(qty)`;
- first/last time: `MIN/MAX(event_time)`;
- fees: known `commission_usdc` values, rounded to the precision of
  `positions.fees_usdc`.

The first partial may create the OPEN position. Later aggregates update the
same exact position and never decrease qty. Fee repricing can trigger an update
without a quantity change. A partial order that never fills further remains a
correct partial position; requested qty does not define completion.

## CLOSED and late fills

For an exact CLOSED position:

- aggregate qty equal to the position qty is historical
  `ENTRY_FILL_ALREADY_RECONCILED`;
- aggregate qty above the closed qty is
  `LATE_ENTRY_FILL_AFTER_POSITION_CLOSED`.

The alarm does not mutate or reopen the CLOSED position. The order stores no
new position linkage, retains at most the qty already assigned to the closed
position, and exposes `unreconciled_qty = aggregate - closed qty`. The status is
terminal manual-review state: later due cycles leave it visible without
reprocessing it until an operator explicitly resets the status.

## Insert races

The order row is selected with `FOR UPDATE SKIP LOCKED`. The partial unique OPEN
slot index is the final insert guard. After losing `ON CONFLICT DO NOTHING`, the
reconciler re-reads the winner and validates exact order/client identity, status,
qty, weighted price and fees. An exact partial winner is updated to the current
aggregate. A winner belonging to another order is an alarm-only mismatch with
no position mutation or linkage.

## Bounded due runner and retry

`run_pending_entry_reconciliation_if_due()` owns a database due gate in
`automation_kv`. Defaults are enabled, 30 seconds and batch size 100. It runs:

- after every fill ingest, including cycles that fetched zero new fills;
- periodically from the existing LIVE automation runner.

One invocation processes at most one deterministic batch, ordered by oldest
first fill and stable order-row ID. `has_more` is recorded as
`BACKLOG_REMAINS`; the next due cycle drains the next batch.
There is no unbounded loop or background thread. Ambiguous, mismatch and late
CLOSED alarms are terminal manual-review states and cannot starve retryable
work. Per-order savepoints preserve other ingested fills; failures become
`ENTRY_FILL_RECONCILIATION_ERROR` without advancing reconciled counters and are
retried on later due cycles.

## Schema readiness

The schema marker is `pending_entry_reconciliation_schema_version=1`.
Readiness performs SELECT-only checks before the write path for:

- all columns used in orders, fills, positions and `automation_kv`;
- exact definitions of the source identity, legacy compatibility, pending
  lookup, fill lookup, fill uniqueness and OPEN-slot indexes;
- `mirror_live_order_sent_to_binance_orders()`;
- `trg_mirror_live_orders` attached to `strategy_events` and that function;
- the version marker.

Missing contract returns `SCHEMA_NOT_READY`, creates no position and performs no
reconciliation write. The normal bot supervisor also fails before starting
strategy children. The migration must precede code rollout; old code tolerates
the additive columns and legacy NULL-source unique index.

## Audit status priority

`v_pending_entry_fill_reconciliation_audit` is alarm-first and exposes the last
action separately from current status. Its mutually exclusive priority is:

1. `CANARY_IGNORE`
2. `EXIT_ACK_PENDING` / `EXIT_FILL`
3. incomplete or rejected identity as `AMBIGUOUS_ENTRY_FILL`
4. `PENDING_ENTRY_ACK`
5. `LATE_ENTRY_FILL_AFTER_POSITION_CLOSED`
6. `ENTRY_FILL_RECONCILIATION_ERROR`
7. `OPEN_POSITION_ORDER_MISMATCH`
8. successful created/updated/already-reconciled actions
9. `MATCHED_ENTRY_FILL` only with an exact or reconciled position
10. `ORPHAN_ENTRY_FILL`

Fills without an order row are independently classified as `CANARY_IGNORE`,
`MANUAL_OR_EXTERNAL_FILL` or `ORPHAN_ENTRY_FILL`. An EXIT ACK without a fill is
never labeled `EXIT_FILL`.

## Transaction and semantic equivalence

For a new fill batch the order remains: fill insert, fee pricing, entry
reconciliation, existing exit reconciliation, commit. With no new fill only the
due-gated DB reconciliation runs. Existing signals, filters, sizing, entry and
exit decisions, regime/ORC, worker commands, exchange requests, submitted order
count and decision contract are unchanged. RSI still persists ACK metadata via
its existing strategy-event transaction and mirror trigger; it gets no new DB
connection or commit.

## PostgreSQL validation boundary

Unit tests emulate deterministic state transitions, failure retry, 150-row
drain, source normalization and readiness failures. They do not prove real
PostgreSQL locking, trigger execution, migration replay, partial-index conflict
or view semantics. The repository includes an isolated PostgreSQL validation
script for a disposable clone/test database. Successful execution of that
script is a rollout blocker, not evidence provided by the unit suite. Never run
it against production.

### Disposable PostgreSQL rollout gate

**Never run these commands against LOCAL LIVE, LOCAL PAPER or any production
database.** Use a disposable clone whose database name ends in `_test`. Before
running either gate, an operator must independently create the second guard:

```sql
INSERT INTO automation_kv(key, value, updated_at)
VALUES ('waltrade_disposable_test_db', 'true', now())
ON CONFLICT (key) DO UPDATE SET value='true', updated_at=now();
```

Both the SQL script and Python integration tests refuse to continue unless the
name suffix and marker are present. Point the environment variable only at that
isolated database:

```bash
export WALTRADE_TEST_PG_DSN='postgresql://USER:PASSWORD@HOST:PORT/waltrade_reconcile_test'

psql "$WALTRADE_TEST_PG_DSN" \
  -f tests/postgres/pending_entry_fill_reconciliation_v1.sql

python3 -m pytest \
  tests/postgres/test_pending_entry_fill_reconciliation_pg.py -q
```

With no `WALTRADE_TEST_PG_DSN`, the Python module is reported as `SKIPPED`; it
must never be interpreted as a successful PostgreSQL rollout gate.

The SQL gate executes the migration twice and asserts the schema marker after
both runs. It checks trigger/function linkage, index definitions and
cardinality, source-aware and legacy NULL-source uniqueness, RSI ACK mirror
accept/reject/missing-ID/replay behavior, the complete mutually-exclusive audit
status matrix, separation of current audit status from the historical last
action, direct-executor/trigger coexistence, CLOSED-history compatibility with
the partial OPEN-slot index and view compilation. Expected uniqueness failures
are checked against the exact PostgreSQL constraint/index name. Fixture changes
are rolled back; the two migration runs intentionally remain in the disposable
clone.

The Python gate calls the real `reconcile_pending_entry_fills()` implementation
against PostgreSQL. It proves the 100+50+0 drain, real `SKIP LOCKED` behavior,
per-order savepoint isolation and clean retry, same-order idempotency with two
real reconciliation workers, and a two-worker foreign-order mismatch without
sticky linkage. Every test connection sets bounded lock, statement and
idle-in-transaction timeouts; worker joins are also bounded and cancel an
unfinished database operation before cleanup.

The plan gate imports the exact production `_CANDIDATES_SQL` constant instead
of maintaining a second SQL copy. After representative fixtures and `ANALYZE`,
it runs `EXPLAIN (FORMAT JSON)`, recursively walks the plan and requires the
specific `ix_binance_orders_pending_entry_reconcile` index. A source-identity or
fill-uniqueness index alone cannot satisfy this assertion. This exact-query
gate lives in the Python integration test because a standalone `psql` script
cannot import the production Python constant without introducing drift.

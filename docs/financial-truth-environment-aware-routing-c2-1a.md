# Financial Truth environment-aware routing — C2.1A

## Problem and root cause

The C2 reader combined simulated evidence with an exchange query selected only
by the existence of the legacy-named `binance_orders` table. Table names do not
define the active exchange or runtime environment. A legacy PAPER schema could
therefore enter a query whose required columns did not exist.

## Exclusive routing

The caller now supplies an immutable `ExecutionEvidenceContext` containing the
canonical environment, exchange identity and deployment identity.

- PAPER reads only `simulated_execution_fills_v1` and its provenance tables.
- LIVE reads only the exchange execution tables. Their historical
  `binance_*` names are storage-compatibility names; active exchange identity
  comes from the explicit context.
- Unknown environments fail closed before an evidence connection or query.
- PAPER and LIVE evidence are never mixed or used as mutual fallbacks.

## Schema capabilities and controlled results

Before source SQL is executed, one catalog inspection verifies every table and
column used by that source query. Missing PAPER capability produces
`SIMULATED_EXECUTION_SCHEMA_UNSUPPORTED`; missing LIVE capability produces
`EXCHANGE_EXECUTION_SCHEMA_UNSUPPORTED`. A supported but empty source produces
`NO_EXECUTION_EVIDENCE`. These are controlled UNKNOWN results, not SQL
exceptions. Detection never relies on catching `UndefinedColumn`.

## Scope and data policy

C2.1A changes no strategy, trading decision, order, position, sizing or risk
behavior. It adds no migration and performs no backfill. Historical positions
without persisted C2 evidence remain UNKNOWN or INCOMPLETE. RSI, TREND and
BBRANGE hooks are unchanged.

SUPERTREND evidence is explicitly out of scope because its PAPER lifecycle
does not currently create a canonical position required by the non-null
evidence linkage. That lifecycle decision is deferred to Patch C2.1B.

## Rollout and rollback

Roll out the reviewed image to LOCAL PAPER first, validate both real LOCAL and
legacy VPS PAPER schema shapes, then deploy the same immutable image to VPS
PAPER. LIVE rollout remains separate. Rollback is an image rollback; there are
no schema or data changes to reverse.

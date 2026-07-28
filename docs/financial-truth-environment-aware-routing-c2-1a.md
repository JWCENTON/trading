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
- Current active exchange identity is `EXCHANGE=OKX`.
- `binance_orders` and `binance_order_fills` are legacy storage compatibility
  names; they do not determine current exchange identity.
- Unknown environments fail closed before an evidence connection or query.
- PAPER and LIVE evidence are never mixed or used as mutual fallbacks.

## Schema capabilities and controlled results

Before source SQL is executed, one catalog inspection verifies every table and
column used by that source query. Missing PAPER capability produces
`SIMULATED_EXECUTION_SCHEMA_UNSUPPORTED`; missing LIVE capability produces
`EXCHANGE_EXECUTION_SCHEMA_UNSUPPORTED`. A supported but empty source produces
`NO_EXECUTION_EVIDENCE`. These are controlled UNKNOWN results, not SQL
exceptions. Detection never relies on catching `UndefinedColumn`.

All three controlled source-readiness outcomes are a typed, categorical
non-write boundary:

- `NO_EXECUTION_EVIDENCE`
- `SIMULATED_EXECUTION_SCHEMA_UNSUPPORTED`
- `EXCHANGE_EXECUTION_SCHEMA_UNSUPPORTED`

They never produce a canonical or canonical-audit write, including when
`apply` was requested. A Financial Truth calculation reached with valid source
evidence retains the existing canonical write policy; the boundary does not
disable apply generally.

## Scope and data policy

C2.1A changes no strategy, trading decision, order, position, sizing or risk
behavior. It adds no migration and performs no backfill. Historical positions
without persisted C2 evidence remain UNKNOWN or INCOMPLETE. RSI, TREND and
BBRANGE hooks are unchanged.

At the C2.1A stage, SUPERTREND evidence was explicitly out of scope because its PAPER lifecycle
does not currently create a canonical position required by the non-null
evidence linkage. That lifecycle decision is deferred to Patch C2.1B.

## Rollout and rollback

That prerequisite was subsequently implemented by C2.1B. C2.1C aligns exit
outcomes with conditional position-close state across all four strategies.
The combined C2.1 series is still not deployed and still requires repeated
combined review before any runtime rollout. Only after that review may planning
start
with LOCAL PAPER; LIVE remains a separate decision. There are no schema or data
changes in C2.1A to reverse.

C2.1D adds lifecycle qualification without changing this routing: COMPLETE
requires `positions.status=CLOSED`. OPEN plus EXIT evidence is a typed
non-canonical conflict and causes zero canonical or audit writes.

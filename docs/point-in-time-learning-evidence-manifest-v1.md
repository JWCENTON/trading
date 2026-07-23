# Point-in-Time Learning Evidence Manifest V1

V1 freezes the exact decision membership used by every new Learning Feedback
proposal observation. It is observability and provenance only: it does not apply
Learning recommendations, activate experiments, update `bot_control`, or alter
trading, Learning, validation, ORC, scoring, selection, sizing, or execution.

## Transaction boundary

The original guard compared a newly inserted header's `xmin` with
`txid_current()`. A header created inside a PL/pgSQL exception block can carry a
subtransaction XID while `txid_current()` identifies the top-level transaction,
so a valid natural run was rejected. Replacing either side with another XID
function would retain the same category error: transaction identity is neither
a logical construction identity nor proof that the guarded capture API created
the row.

The existing V1.3 validation trigger runs when a V1.2 feedback run transitions
to `OK`. A second, alphabetically later trigger captures evidence manifests in
that same transaction. Any source/header/child count mismatch raises an error
and rolls back the feedback run, validation observation, manifest, and children.
The separately committed V1.4 shadow publisher has a fail-closed trigger: a new
or refreshed proposal requires a `COMPLETE` exact manifest for its source run and
slot.

## Identity and hashes

Runtime identity is built from two values supplied for every transaction:

- `deployment_instance_id`, for example `local`, `vps`, `vps2` or
  `regional-eu`;
- logical `environment`, exactly `live` or `paper`.

The stored canonical deployment ID is always derived as
`<deployment_instance_id>-<environment>`. Adding another valid instance does
not require a SQL allowlist or migration.

The runner accepts an explicit `DEPLOYMENT_INSTANCE_ID`. For compatibility with
current configurations, it may derive the instance from `DEPLOYMENT_ID` only by
removing the exact final suffix matching `ENVIRONMENT`; it never uses a general
hyphen split. If both variables are present, `DEPLOYMENT_ID` must equal the
derived canonical value. Instance IDs are 1–63 characters and match
`^[a-z0-9]+(?:-[a-z0-9]+)*$`; embedded/double/edge hyphens, whitespace,
uppercase, and instance suffixes `-live` or `-paper` are rejected.

The runner sets only `waltrade.deployment_instance_id` and
`waltrade.environment` using `set_config(..., true)`, PostgreSQL's
parameter-safe equivalent of `SET LOCAL`. COMMIT and ROLLBACK clear the
settings, so a pooled connection must establish them again.
The V1.4 publisher uses a separate transaction and therefore calls the same
helper again.

`decision_registry_v1` retains legacy provenance (`LOCAL/VPS` and
`trading_live/trading_paper`). It is checked separately for evidence isolation
and is never the runtime identity SSOT. No database-name or registry fallback is
permitted.

The natural header identity is deployment, environment, feedback run, slot and
window. Membership is sorted by canonical `decision_key`; ordinal is derived
from that order. `manifest_hash` covers the ordered decision key/canonical UUID
pairs. Optional outcome fields do not change membership identity. Each child has
a separate fingerprint over the captured point-in-time payload. The aggregate
payload is canonical JSONB and has its own SHA-256 `aggregate_hash`.

## Data availability

`evidence_window_start` and `evidence_window_end` are event/sample boundaries:
the feedback engine derives them from the minimum and maximum decision-source
`refreshed_at` values selected by its existing sample policy. They are not an
as-of boundary. In particular, an entry and exit may be inside that window while
the authoritative outcome is calculated later.

`evidence_cutoff_at` is the immutable as-of boundary of the natural feedback
run. V1 uses the V1.2 run's `started_at`, created at the beginning of the same
transaction. `source_snapshot_at` records the same logical boundary. Neither is
derived from the sample end, a maximum source timestamp, `clock_timestamp()`,
nor a later statement timestamp.

PnL is required by the existing feedback source. Position, canonical decision
UUID, fees, gross PnL, MFE, MAE and regime are captured when their authoritative
sources contain them. Otherwise they remain SQL `NULL`; explicit per-row flags,
coverage counts and missing counts prevent NULL from being misrepresented as
zero. Exact membership requires warehouse creation, registry ingestion, outcome
creation, and outcome calculation at or before the frozen cutoff. Thus an
outcome after the event-window end but at or before the run cutoff is valid,
whereas an outcome after the run cutoff is look-ahead and is excluded before
aggregate construction. Equality with the cutoff is included (`<=`).

PostgreSQL's default `READ COMMITTED` isolation gives each statement a new MVCC
snapshot. V1 therefore does not claim that event timestamps alone prove prior
availability. Explicit `created_at`/`ingested_at` filters provide the as-of
boundary; any change between the aggregate selection and child insertion causes
count/hash/parity failure. No global isolation-level change is made.

The successfully inserted immutable membership is the frozen decision-set
record. Aggregate count, wins, losses and net PnL are checked against the
feedback statistics, and manifest/aggregate hashes are derived from the same
ordered selection. Later changes to the mutable analytical view, fees, MFE, MAE
or outcomes cannot mutate a `COMPLETE` manifest. A retry with a changed source
set fails as `LEARNING_EVIDENCE_IDEMPOTENCY_CONFLICT`.

Forward evaluation is defined strictly as decisions or outcomes with timestamp
greater than `evidence_cutoff_at`; frozen membership rows cannot simultaneously
belong to that forward set.

## Canonical aggregate semantics

Feedback V1.2 remains the historical semantic authority. Both fresh feedback
installations and Manifest V1 call
`learning_canonical_profit_factor_v1(decisions, pnl_coverage_count,
gross_profit_usdc, gross_loss_usdc)`. Existing feedback rows are not rewritten.

The per-slot profit-factor contract is:

| Qualifying decisions | PnL coverage | Gross profit | Absolute gross loss | Result |
|---:|---:|---:|---:|---:|
| 0 | 0 | NULL | NULL | NULL |
| >0 | 0 | NULL | NULL | NULL |
| >0 | >0 | >0 | 0 | 999 |
| >0 | >0 | <=0 or NULL | 0 | 0 |
| >0 | >0 | any, with NULL treated as zero numerator | >0 | `COALESCE(gross_profit,0) / ABS(gross_loss)` |

The all-loss case is intentionally zero, not NULL. The feedback engine has
always coalesced the missing positive-PnL sum to zero before division. Empty or
all-PnL-missing sets remain NULL and are distinguished from genuine zero-PnL
decisions by decision and PnL coverage counts. The helper rounds its NUMERIC
result to 12 decimal places, matching the historical
`learning_slot_statistics_v1.profit_factor NUMERIC(28,12)` storage contract.

Membership is deduplicated by `decision_key` before aggregation and ordered by
that key for hashing. Decisions, wins, losses, breakeven, gross positive PnL,
gross negative PnL, net PnL, expectancy, profit factor and win rate are compared
against the feedback slot aggregate with PostgreSQL NUMERIC/integer semantics;
NULL uses `IS DISTINCT FROM` and is never silently equated to zero. Fees, MFE,
MAE and regime have no feedback V1.2 aggregate counterpart. They are frozen
structurally in the manifest with explicit coverage and missing counts, and
participate in the deterministic aggregate hash.

## Immutability and retry

Every new header starts as `BUILDING`. The capture function generates a random
UUID capability, stores it in the header, and installs it in the transaction-local
`waltrade.learning_manifest_construction_token` setting. A second internal
capture marker prevents a caller from authorizing a header merely by choosing a
GUC value. Child and aggregate triggers require both a matching `BUILDING`
header and matching deployment context. There is no `xmin`, XID, process ID,
session ID, or timestamp comparison; consequently a PL/pgSQL exception block's
subtransaction does not change the construction identity.

After child-count, aggregate-count, membership-hash, and aggregate-hash parity
checks, the controlled finalizer performs the only permitted header mutation:
`BUILDING` to `COMPLETE`, with every other column byte-for-byte unchanged. The
capability settings are cleared after capture and are also cleared inherently by
COMMIT/ROLLBACK. A deferred trigger rejects any `BUILDING` header at transaction
end, so failure leaves no partial header, membership, or aggregate residue.

Completed header, membership and aggregate relations reject every UPDATE and DELETE.
The natural unique key and per-manifest decision-key constraint prevent
conflicting duplicates. Retrying an already completed source run recomputes the
source count and hashes: exact equality is a no-op; any drift fails with
`LEARNING_EVIDENCE_IDEMPOTENCY_CONFLICT`. A stale `BUILDING` row is never reused.
Historical observations are represented only as `LEGACY_AGGREGATE_ONLY` with
`exact_membership_available=false`; V1 never reconstructs or fabricates their
missing decision IDs. Their retained historical cutoff-shaped value is only a
legacy window/run boundary; it is not represented as an exact source snapshot.

## Privilege boundary

The audited LOCAL runtime role owns the relevant relations and is currently a
PostgreSQL superuser. Grants, revokes, `SECURITY DEFINER`, function ownership,
and `search_path` therefore cannot form a meaningful security boundary: that
role can always bypass ordinary ACLs and replace database objects. This patch
does not pretend otherwise and does not introduce a misleading privilege model.
Its minimum fail-closed boundary is trigger-enforced integrity for the normal
runtime path: unscoped, mismatched, late, cross-deployment, malformed, or
post-completion writes fail. Hard isolation requires a future deployment change
to a non-owner, non-superuser runtime role with direct table DML revoked and
EXECUTE granted only on a hardened, schema-qualified capture API.

## PostgreSQL 16 regression harness

The offline harness applies the migration twice to a schema-only clone, then
drives the real `RUNNING` to `OK` feedback-run update. The normal V1.3 trigger
runs before the alphabetically later manifest trigger, while the transition is
wrapped in a nested PL/pgSQL exception block. It verifies natural `COMPLETE`
creation, child/aggregate parity, no `BUILDING` residue, exact retry no-op,
caller-token rejection, child rejection without capability, immutable history,
transaction-local cleanup, and a publisher insert in a separate transaction.
It also reproduces the production failure dates: entry/exit before 10 July,
event-window end on 10 July, outcome on 12 July, and run cutoff after that.
The outcome is accepted because it was visible before the run cutoff. A
backdated row whose availability is after the cutoff is excluded, and a
post-COMMIT source mutation leaves the completed manifest unchanged while a
retry reports an idempotency conflict.

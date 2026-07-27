# WalTrade Database Baseline Read-Only Audit V1

This checkpoint catalogs and compares PostgreSQL schema state without changing
the database. It does not include production migrations or baseline adoption.
It changes no trading table, PnL formula, reconciliation function, active
trigger, strategy, execution, ORC, Learning, or FinalDecision behavior.

## Artifacts

- `waltrade_database_baseline_v1.json` is the OID-free canonical manifest.
- `expected_environment_differences_v1.json` is fail-closed: an unlisted
  environment mismatch is unexpected.
- `waltrade_schema_baseline_v1.py gate` performs catalog reads only and emits an
  object-level diff.

Definitions are normalized by removing dump/session noise, SQL comments and
non-semantic formatting. Function bodies, predicates, trigger enabled state,
columns (type/default/nullability), constraints, view logic, identity arguments
and environment applicability remain part of the contract.

## Observed and proposed provenance

The manifest contains observed catalog identities only. A proposed migration
does not add an identity to the observed baseline. `TRACKED_CURRENT` and
`TRACKED_RUNTIME_DDL` require `source_path` to exist in the candidate Git index;
an ignored, untracked, or missing path fails manifest validation.

`RUNTIME_OBSERVED_PENDING_ADOPTION` means the identity was present in captured
catalog input but has no canonical implementation source in the checkpoint.
It remains catalogued and blocks adoption. A local proposed source may exist
outside the commit, but it is not represented as tracked provenance.

## Operation

Inventory and gate are explicit read-only operations:

```text
PYTHONPATH=. python scripts/waltrade_schema_baseline_v1.py inventory --environment LIVE
PYTHONPATH=. python scripts/waltrade_schema_baseline_v1.py gate --environment LIVE
```

The operational environment must provide the standard WalTrade DB variables.
`gate` opens a read-only transaction. The CLI has no migration execution,
adoption-record writer, trigger installer, or data-repair command.

## Hardening contract

Inventory discovery scans the complete application-owned catalog in the
approved `public` schema. It includes tables, partitioned tables, views,
materialized views, sequences, functions, procedures, triggers, constraints,
indexes, policies, non-internal rules and event triggers. Discovery is not
restricted by known names or function patterns. PostgreSQL internal objects
are excluded or marked `INTERNAL`; extension membership is read from
`pg_depend` and marked `EXTENSION`. Extension-managed objects are not
application drift, while a manifested extension name or version change is
reported.

The environment-difference contract uses exact identities: type, schema, name,
routine identity arguments and parent relation. Wildcard approval is
unsupported and an unlisted mismatch fails closed. The ten definition
differences known at this revision are `BLOCKED_PENDING_DECISION`; neither PnL
view difference is accepted without source-migration and semantic evidence.

Application-owned objects have an explicit owner contract. Owner, trigger
enabled state and normalized definition are compared independently. Duplicate
manifest and runtime identities fail before a dictionary can overwrite them.

Credentials have no built-in values. The tool accepts a password-free DSN or
explicit libpq-style variables with `PGPASSWORD`, `PGPASSFILE` or `.pgpass`;
passwords are never printed or persisted.

Exit code `0` means READY and `2` means a fail-closed gate result. The tool does
not repair drift, change owners, write provenance records, or alter trading
semantics.

## Hierarchical provenance

The V1 manifest covers the full application catalog but does not treat every
catalog identity as a separate manual provenance decision. Each entry has one
of four persisted roles:

- `PROVENANCE_ROOT` for tables, views, materialized views, policies and other
  independently sourced schema objects;
- `INDEPENDENT_EXECUTABLE` for application functions, procedures, triggers and
  rules whose logic can change data or behavior;
- `OWNED_CHILD` for constraints, indexes and owned sequences;
- `EXTENSION_MANAGED` for extension headers and member objects.

Every child retains its exact identity and fingerprint. Its `root_identity`
comes from catalog ownership (`pg_constraint`, `pg_index` or sequence
`pg_depend`), not a naming convention. View/relation and trigger/function
dependencies are stored in deterministic `dependency_identities`. PL/pgSQL
dependencies absent from `pg_depend` require a separately reviewed manual edge
with a reason and source.

`provenance_status`, `source_path` and `source_commit` identify tracked current,
historical, runtime-DDL, local-untracked or observed-pending evidence. Children
inherit root risk and blocking state. Runtime-only P0/P1 roots and executables
remain `BLOCKED_NO_SOURCE`; cataloguing them never silently approves them.

Coverage and adoption are separate gates. `CATALOG_COVERAGE_READY` requires
every application identity, role, root and fingerprint to be present.
`ADOPTION_READY` additionally requires no blocked source, canonical-definition
or environment-contract decisions. Consequently, complete catalog coverage
with blocked adoption is an expected safe state.

The ten previously identified LIVE/PAPER differences retain
`BLOCKED_CANONICAL_DEFINITION_REQUIRED`. Their definitions are not reconciled
by the baseline tool. The full-catalog pass also records `allocation_policy` as
unexpected drift rather than approving its LIVE/PAPER shape difference.

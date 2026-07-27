# Database Baseline Artifact Contract V1

`DATABASE_BASELINE_ARTIFACT_CONTRACT_V1` is the self-describing envelope for
offline catalog evidence. The immutable raw CSV is the only input. The
canonicalizer never connects to PostgreSQL, invokes `psql` or Docker, executes
SQL, or contains migration/adoption behavior.

The report separates run metadata (`contract`) from `semantic_payload`.
`generated_at_utc` belongs only to run metadata and is excluded from
`deterministic_payload_sha256`.

## Identity and fingerprint specification

The object identity key is the ordered tuple:

```text
object_type, schema, object_name, identity_arguments, parent_relation
```

Empty identity arguments and parent relations are represented by `""`.
Nullable catalog attributes use JSON `null`; booleans use JSON `true` and
`false`. Inventory, drift, blocker, and financial lists are sorted
deterministically. Canonical structural JSON uses UTF-8, sorted keys, and fixed
compact separators.

Definition fingerprints retain the existing V1 algorithm:

```text
SHA-256(UTF-8(normalize_sql(definition or "")))
```

`normalize_sql` removes SQL comments and dump/session noise, collapses
whitespace, and normalizes whitespace around `(),;=` without changing function
bodies, predicates, literals, ordering, NULL semantics, or casts.

## Fail-closed rules

The canonicalizer rejects unknown CSV headers, missing fields, duplicate
identities, unsupported object types or management states, unknown provenance,
dangling tracked sources, contract-version mismatches, unsupported environment
identities, and optional expected raw hashes that do not match.

The output directory is mandatory and explicit. Generated reports and raw
captures are evidence artifacts and must remain outside Git.

## Primary classification and coverage

Every observed identity has exactly one scalar `primary_classification`:

- `APPLICATION_OWNED_TRACKED`
- `TRACKED_RUNTIME_DDL`
- `RUNTIME_OBSERVED_PENDING_ADOPTION`
- `EXTENSION_MANAGED`
- `INTERNAL_METADATA`
- `EXPECTED_ENVIRONMENT_SPECIFIC`
- `HISTORICAL_ORPHAN_PENDING_DECISION`

Risk, manual-decision, canonical-blocker, and expected-difference markers are
independent secondary flags. They never replace or alter primary ownership.
Coverage is true only when every observed identity has exactly one allowed
primary classification; consequently a covered report always has
`unclassified = 0`.

## Counter semantics

All counters operate on unique catalog identities unless explicitly described
as decisions:

- `observed_identities`: every parsed identity.
- `application_owned`: identities whose raw catalog management is
  `APPLICATION`. It may overlap provenance and secondary-flag counters.
- `extension_managed` and `internal_metadata`: identities with the matching
  primary classification. These sets are mutually exclusive from every other
  primary classification.
- `unclassified`: identities without one allowed primary classification.
- `tracked_current`, `tracked_runtime_ddl`, and
  `runtime_observed_pending_adoption`: identities with the matching provenance
  status. Internal metadata is excluded from the pending-adoption counter.
- `manual_decisions`: distinct identities whose `manual_decision` secondary
  flag is true. It is not defined as P0 plus P1.
- `pending_p0` and `pending_p1`: distinct manual-decision identities having
  the respective risk. They are disjoint subsets of `manual_decisions`; other
  risk classes mean their sum need not equal `manual_decisions`.
- `canonical_common_blockers`: distinct identities whose independent
  `canonical_blocker` flag is true.
- `unknown_differences`: distinct `(identity, kind)` drift records.
- `blocked_contract_differences`: the subset of drift records whose kind is
  `BLOCKED_PENDING_DECISION`.

Invariants include:

```text
observed_identities = application_owned + extension_managed + internal_metadata
unclassified = 0 iff coverage = true
pending_p0 <= manual_decisions
pending_p1 <= manual_decisions
blocked_contract_differences <= unknown_differences
```

The first invariant describes the raw management partition. Primary
classification is a separate, exact-one provenance partition.

## Historical report status

Earlier VPS reports are historical, non-canonical evidence. Their aggregation
mixed financial-candidate lists with manual decisions and did not independently
count blockers or uncovered identities. Old and canonical counter values are
therefore not directly comparable without an explicit semantic mapping.

Artifact Contract V1 is the only supported canonical report format. Immutable
raw captures remain the evidence source, and canonical reports can always be
reproduced offline from those captures plus the tracked manifest, difference
contract, and canonicalizer.

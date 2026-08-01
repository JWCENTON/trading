# Legacy repair Learning quarantine V1

## Contract

A reconstructed PAPER outcome can remain complete financial evidence without
becoming trusted Learning evidence:

```text
legacy evidence
  -> append-only Learning exclusion
  -> position CLOSED
  -> lifecycle + Financial Truth + audit + provenance
  -> reporting eligible
  -> Learning ineligible
```

`learning_outcome_exclusion_v1` is authoritative for Learning trust. Its
identity is `(environment, deployment_id, position_id)` and its only V1 reason
and source are `LEGACY_REPAIR` and `LEGACY_POSITION_REPAIR`. Rows are
append-only, carry the reviewed semantic fingerprint V2 and Git SHA, and are
not backfilled by the migration.

`v_learning_eligible_closed_positions_v1` expresses the canonical closed and
not-excluded predicate. Destination guards enforce the same predicate at every
currently identified position-linked Learning ingress: exit trace V1/V2/V3,
shadow recommendations, feature warehouse, decision replay, decision registry,
and decision outcomes. This write-boundary protection also covers existing
refresh/backfill functions without rewriting their broader selection logic.
Exit-learning aggregates inherit the boundary from exit trace and have no
position-level linkage of their own.

## Atomic apply

The transaction order is fixed:

1. writer-side schema and ledger readiness;
2. position, order, fill, audit, provenance, and exclusion identity locks;
3. evidence re-plan and semantic fingerprint V2 comparison;
4. locked artifact snapshot classification and fingerprint comparison;
5. exclusion insert;
6. position close, lifecycle and order-state changes;
7. canonical Financial Truth, audit, and immutable provenance;
8. postcondition, unchanged-artifact and reader-exclusion checks;
9. commit.

Because the exclusion is visible in the same transaction, a close trigger or
refresh writer cannot persist a Learning row for the repaired position. Any
error rolls back the exclusion together with all repair state. An already
committed identical repair is verified and returns `ALREADY_APPLIED` with zero
writes; conflicting state fails closed.

## Ingress audit

| Learning ingress | Direct positions reader | Quarantine enforcement |
| --- | ---: | --- |
| Exit trace trigger / refresh | Yes | canonical predicate plus exit-trace destination guard |
| Shadow recommendations refresh | Yes (direct or exit-trace attribution) | shadow destination guard |
| Decision registry / outcomes refresh | Yes | registry and outcome destination guards |
| Feature warehouse refresh | No; shadow source | warehouse destination guard |
| Decision replay refresh | No; warehouse source | replay destination guard |
| Exit-learning aggregate | No; exit-trace source | inherited from protected exit trace |

V1 deliberately does not delete or rebuild historical Learning data.

## Existing artifact policy V1

Artifact presence alone is no longer a blocker. The writer classifies one
locked, fingerprinted snapshot as exactly one of:

- `NO_ARTIFACTS`;
- `BENIGN_OPEN_INCOMPLETE_ARTIFACTS`;
- `TERMINAL_OR_AMBIGUOUS_ARTIFACTS`.

The benign classification is deliberately narrow. Exit trace V1/V2/V3 and
decision outcome counts must be zero. At most one artifact of each remaining
type may exist, identities and decision keys must be consistent, and statuses
must equal the following complete strings:

| Artifact | Only allowed status |
| --- | --- |
| Shadow recommendation | `OBSERVE_INCOMPLETE_PNL` with `SHADOW_OBSERVE_ONLY` |
| Feature warehouse | `OPEN_OR_INCOMPLETE` |
| Decision replay | `REPLAY_OPEN_OR_INCOMPLETE` |
| Registry `decision_payload.position_status` | `OPEN` |

The local PAPER legacy namespaces are also explicit: `trading_paper` is the
stored PAPER environment, warehouse/replay `legacy-unknown` is accepted only
with `causal_linkage_status=LEGACY_NOT_ATTRIBUTABLE`, and registry `LOCAL` is
accepted for the `local-paper` repair identity. No VPS alias is accepted.
Shadow V1 has no deployment column, so its exact `position_id` and shared
decision key provide its bounded linkage.

Any exit trace, outcome, duplicate, unknown status, trusted marker, terminal
field, inconsistent decision identity, environment/deployment mismatch, or
pre-existing terminal Financial Truth produces
`TERMINAL_OR_AMBIGUOUS_ARTIFACTS` and fails as
`LEARNING_TERMINAL_OR_AMBIGUOUS_ARTIFACT`.

For an allowed benign repair, the artifact rows are never updated or deleted.
Their complete immutable snapshot is part of semantic fingerprint V2 and
repair provenance. The exclusion-aware artifact views omit them from future
Learning source reads, while base tables retain them as historical evidence.
The existing destination guards continue to suppress refresh/backfill inserts
and updates for an excluded position.

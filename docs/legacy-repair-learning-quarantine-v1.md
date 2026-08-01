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
4. absence check across all position-linked Learning artifacts;
5. exclusion insert;
6. position close, lifecycle and order-state changes;
7. canonical Financial Truth, audit, and immutable provenance;
8. postcondition and artifact-absence checks;
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

The apply precondition additionally rejects a position if any protected
downstream artifact already exists. V1 deliberately does not delete or rebuild
historical Learning data.

# C2.2.2 projected-row compatibility and adoption generations

## Classification hierarchy

Inventory mutation uses this fail-closed order:

1. explicitly approved historical reconstruction;
2. `EXISTING_PROJECTED_C2_2`, verified from a complete, internally consistent
   projection and matching authoritative LIVE or simulated PAPER fills;
3. `FORWARD_C2_2`, attributed to the active generation or created after its
   database adoption boundary;
4. `LEGACY_UNPROJECTED`;
5. `ADOPTION_GENERATION_MISMATCH`.

`inventory_evidence_status=COMPLETE` alone is never sufficient. Quantity
identities, `positions.qty`, entry/exit high-water, calculation timestamp,
account identity and instrument/fill evidence are checked together.

## Adoption generation lifecycle

`runtime_contract_adoption_v2` records immutable generation identity and the
states `PREPARED`, `ACTIVE`, `DEACTIVATED`, `ROLLED_BACK`, and `SUPERSEDED`.
A partial unique index permits at most one ACTIVE generation per contract,
environment and deployment.

Use the transactional database operations:

- `prepare_contract_adoption`
- `activate_contract_adoption`
- `rollback_contract_adoption`
- `supersede_contract_adoption`

Activation validates the expected Git revision, environment and deployment,
takes a transaction-scoped advisory lock, refuses an existing ACTIVE
generation, and assigns `adopted_at` from the database clock. A rolled-back
generation cannot be reactivated.

V1 rows remain historical evidence. The migration does not copy, activate, or
backfill them.

## Position and fill attribution

The nullable `positions.inventory_contract_adoption_id` and
`positions.inventory_contract_generation` columns are populated when a
forward position first commits an accepted lifecycle mutation. Legacy rows
remain NULL. The ingestion ledger stores the active adoption and generation
resolved before evidence application.

An attributed position is mutable only while its exact generation is ACTIVE.
Unattributed pre-C2.2.2 rows may continue only through the evidence-backed
existing-projection predicate. An attribution mismatch fails closed.

## Mutation gate

Only new authoritative evidence or an unambiguous authoritative correction
may mutate a `FORWARD_C2_2` or `EXISTING_PROJECTED_C2_2` row. Legacy,
generation mismatch, inactive adoption, incomplete evidence, ambiguous
downward corrections and identical replay are no-ops.

Lifecycle outbox insertion remains in the same transaction as the position
mutation. Its unique identity prevents a replay from producing another event.

## Controlled rollout

1. Create and verify a compressed database dump.
2. Apply C2.2, C2.2.1 and C2.2.2 migrations and verify no data backfill.
3. Build an immutable image.
4. Prepare a new generation with the target Git and container revisions.
5. Verify the candidate image identity while trading is disabled.
6. Activate the prepared generation immediately before restarting only the
   candidate runtime.
7. Verify container revision equals adoption Git/container revision before
   enabling normal mutation.
8. Compare legacy residuals, ingestion ledger and lifecycle high-water, then
   perform bounded observation.

The runtime must fail closed while no matching ACTIVE generation exists. This
keeps the activation-to-start gap safe.

## Rollback and re-adoption

Before any forward mutation: disable trading, stop the candidate, mark its
generation `ROLLED_BACK`, restore the old runtime, and prove no position was
created during the gap. A later rollout always prepares a higher generation.

After a forward mutation: disable trading, stop the candidate, preserve logs,
fills, ingestion ledger and outbox, restore the verified pre-rollout database
dump, restore the old runtime, and verify database/adoption/runtime parity.
Runtime rollback alone is not a data rollback.

Keep the dump until the bounded observation and rollback window close and all
high-water comparisons pass.

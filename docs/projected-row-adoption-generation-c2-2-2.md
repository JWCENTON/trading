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

`generation` is the contract compatibility generation. `git_revision` and
`container_revision` are immutable provenance for the artifact which activated
that generation; they are not the identity of every later compatible runtime.
The current runtime SHA remains separate deployment evidence and is reported
alongside adoption provenance. A mismatch is diagnostic, not an inventory or
lifecycle eligibility blocker.

V1 rows remain historical evidence. The migration does not copy, activate, or
backfill them.

## Position and fill attribution

The nullable `positions.inventory_contract_adoption_id` and
`positions.inventory_contract_generation` columns are populated when a
forward position first commits an accepted lifecycle mutation. Legacy rows
remain NULL. The ingestion ledger stores the active adoption and generation
resolved before evidence application.

An attributed position from the current generation follows the forward path.
A position owned by an older generation may continue only through the
evidence-backed existing-projection predicate and retains its immutable
ownership. An attribution mismatch without complete compatible evidence fails
closed.

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
7. Verify the candidate runtime image and Git revision independently, and
   report whether they match the immutable adoption provenance.
8. Compare legacy residuals, ingestion ledger and lifecycle high-water, then
   perform bounded observation.

The runtime must fail closed while no compatible ACTIVE generation exists.
Stopping the old mutation-capable runtime before activating a genuinely new
generation keeps the activation-to-start gap safe.

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

## C2.2.4 atomic active-generation replacement

`replace_active_contract_adoption` replaces one explicitly named ACTIVE row
with one explicitly named PREPARED row. The caller supplies both adoption IDs,
the expected old generation and Git revision, the candidate Git revision,
environment, deployment and reason. The helper takes the same
transaction-scoped advisory lock as activation, locks both rows `FOR UPDATE`,
checks scope and generation ordering, and reads `clock_timestamp()` once.
The old `deactivated_at` and new `adopted_at` therefore identify one transition
boundary. The partial unique ACTIVE index remains the final database guard.

A successful transition leaves the old generation `SUPERSEDED`, the candidate
`ACTIVE`, and links `supersedes_adoption_id` to the old row. An exact retry
returns `ALREADY_REPLACED` without mutation. Partial or mismatched state fails
closed with a specific replacement error; lifecycle history is never repaired
or rewritten implicitly.

Every mutation lookup binds to the ACTIVE contract scope and generation, not
to exact equality between activation provenance and runtime `GIT_SHA`.
Position ownership remains separate: a complete evidence-backed position
attributed to an older generation keeps that attribution and follows
existing-projected compatibility. New forward positions receive the current
contract generation. Runtime/adoption SHA parity remains visible in rollout
and audit diagnostics.

A later runtime patch that does not change the adopted contract does not
require a new contract adoption generation. A new generation is required only
when contract semantics, schema requirements, inventory equations, or the
compatibility boundary change.

The operational order is build and verify candidate images, create PREPARED,
stop the old mutation-capable bot-runner, atomically replace ACTIVE, start the
candidate immediately, and verify its image/Git identity.

If the candidate cannot start, never reactivate a historical `SUPERSEDED` row.
Prepare a higher recovery generation for the prior runtime and use
`rollback_active_to_prepared_contract_adoption` to atomically mark the failed
candidate `ROLLED_BACK` and activate recovery. History remains immutable: old
`SUPERSEDED`, failed `ROLLED_BACK`, recovery `ACTIVE`.

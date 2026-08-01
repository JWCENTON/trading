# Legacy recovery rollout V2

The legacy-recovery tool has a read-only planner for PAPER and LIVE, plus one
bounded writer command for PAPER positions. `apply-position` repairs exactly
one position and always writes the Learning exclusion before changing the
position to `CLOSED`. LIVE apply is forbidden.

## Safety boundary

- Use one position per invocation.
- Never supply quantities, prices, fees, PnL, timestamps, order IDs, or trust
  classifications manually. They are derived from immutable database evidence.
- Every legacy position repair is reporting-eligible and Financial-Truth-
  eligible when complete, but Learning-ineligible with reason `LEGACY_REPAIR`.
- Existing Learning artifacts are not automatically fatal. Only explicitly
  enumerated open/incomplete shadow, warehouse, replay and OPEN registry rows
  may remain. Exit traces, outcomes, trusted/terminal/unknown states, identity
  mismatches and duplicates always block the repair.
- Existing benign artifacts are immutable historical evidence: never delete,
  update, close, rebuild or backfill them. Their snapshot is fingerprinted and
  written into repair provenance; the exclusion removes them from eligible
  future Learning reads.
- Do not use one-off SQL. The service owns readiness checks, bounded locks,
  re-planning, the semantic CAS, all writes, postconditions, commit, and rollback.
- Do not use `apply-position` against LIVE. It returns
  `LIVE_APPLY_NOT_AUTHORIZED` before opening a database connection.

The DSN is read from the explicitly named environment variable and is never
rendered. The independently supplied database name and environment must match
the DSN identity.

## Operator sequence

All examples below are PAPER-only. Keep the global arguments before the
subcommand for consistency.

1. Verify the base recovery schema and quarantine schema:

   ```bash
   python -m tools.legacy_recovery \
     --database-url-env PAPER_DATABASE_URL \
     --environment PAPER \
     --expected-database trading_paper \
     --deployment-id local-paper \
     check-schema
   ```

   Both `schema_status` and `quarantine_schema.status` must be
   `PRESENT_VALID`.

2. Build the plan for one explicit position:

   ```bash
   python -m tools.legacy_recovery \
     --database-url-env PAPER_DATABASE_URL \
     --environment PAPER \
     --expected-database trading_paper \
     --deployment-id local-paper \
     plan-position --position-id 3080
   ```

3. Review `semantic_fingerprint_v2`, all evidence identities, the planned
   mutations, `financial_truth_status=COMPLETE`, `reporting_eligible=true`,
   and `learning_eligible=false`. Confirm independently that the artifact
   counts for exit trace, shadow, feature warehouse, replay, registry, and
   outcomes are zero. Review `learning_artifact_gate.classification`, every
   artifact ID/status and the exact reason for any blocker. The writer locks
   and reclassifies the same snapshot transactionally.

4. Apply exactly the reviewed fingerprint:

   ```bash
   python -m tools.legacy_recovery \
     --database-url-env PAPER_DATABASE_URL \
     --environment PAPER \
     --expected-database trading_paper \
     --deployment-id local-paper \
     --git-sha 0123456789abcdef0123456789abcdef01234567 \
     apply-position --position-id 3080 \
     --expected-fingerprint-v2 FINGERPRINT_FROM_PLAN \
     --confirm-apply
   ```

   Success is machine-readable JSON with `status=APPLIED`,
   `learning_excluded=true`, `transaction_committed=true`, and the exclusion,
   audit, and provenance IDs. A changed position or fill produces `PLAN_STALE`.
   A terminal, trusted, unknown or ambiguous artifact produces
   `LEARNING_TERMINAL_OR_AMBIGUOUS_ARTIFACT`; neither it nor a stale plan writes
   partial state. `BENIGN_OPEN_INCOMPLETE_ARTIFACTS` is allowed only after the
   exclusion becomes visible to every canonical reader view and guard.

5. Repeat the identical apply command. It must return
   `status=ALREADY_APPLIED` and `writes=0`.

6. Validate post-state: the exclusion, closed position, lifecycle event,
   complete canonical Financial Truth, repair audit, and provenance exist;
   exit trace, shadow, warehouse, replay, registry, and outcome rows for the
   position remain absent. Pre-existing benign artifacts must have an identical
   physical snapshot but return zero rows through exclusion-aware reader views.

## Other read-only commands

`plan-fill`, `classify-external`, `audit-open-cohort`, and
`audit-unresolved-closed` remain read-only. They retain the existing explicit
source identity and evidence contracts.

## Migration and rollback boundary

The additive migration is
`20260801_legacy_repair_learning_quarantine_v1.sql`. It requires the canonical
migration ledger and `LEGACY_RECOVERY_SCHEMA_V2`, performs no backfill, and
installs the append-only exclusion contract plus Learning ingress guards. Run
it only in a separately authorized schema rollout. This code change does not
apply it to any runtime database.

The earlier legacy recovery rollback remains permitted only while its audit and
provenance histories are empty. Once a repair is committed, neither repair
history nor Learning exclusions may be updated or deleted.

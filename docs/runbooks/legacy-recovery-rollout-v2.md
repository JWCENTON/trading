# Legacy recovery rollout V2

The legacy-recovery tool has read-only planners for PAPER and LIVE and two
strictly separate, one-position PAPER writers. `apply-position` reconstructs a
historical outcome only when authoritative exit evidence already exists.
`apply-open-retirement` creates a new, current-time administrative PAPER exit
for a still-open legacy position that has no historical exit evidence. Both
write the Learning exclusion before changing the position to `CLOSED`. LIVE
apply is forbidden.

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

   Global `schema_status` is `PRESENT_VALID` only when all three independent
   gates pass: `migration_schema_status`, `planner_readiness_status`, and
   `writer_readiness_status`. Inspect `planner_readiness.order_evidence_source`
   and its capability flags; a valid migration ledger alone is not readiness.

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

## PAPER legacy open-position retirement V1

This is not an extension of `apply-position` and must never be used to invent a
historical exit. It is a new canonical PAPER execution with:

- `exit_reason` and `outcome_origin` equal to
  `LEGACY_ADMINISTRATIVE_CLOSE`;
- one canonical `SELL` row in `simulated_orders` and one canonical EXIT row in
  `simulated_execution_fills_v1`;
- the normal PAPER fee rate, instrument precision, inventory lifecycle
  projector, and Financial Truth writer;
- a price taken only from the latest canonical candle for the exact
  symbol/interval, under the existing 20-minute freshness contract;
- `learning_eligible=false`, trust
  `LEGACY_RECONSTRUCTED_NOT_TRUSTED_FORWARD`, and
  `reporting_eligible=false`.

The administrative outcome is excluded from 24-hour performance, win rate,
strategy expectancy, exit learning, shadow recommendations, warehouse,
replay, decision outcomes, and future trusted-outcome consumers. The PAPER
account reconstruction explicitly includes it so the retired inventory and
realized account value remain coherent.

Plan exactly one position (read-only):

```bash
python -m tools.legacy_recovery \
  --database-url-env PAPER_DATABASE_URL \
  --environment PAPER \
  --expected-database trading_paper \
  --deployment-id local-paper \
  plan-open-retirement --position-id 10326
```

Review `status=READY`, the market row identity and timestamp, freshness,
quantity, precision snapshot, artifact gate, and `semantic_fingerprint_v2`.
There are no manual price, quantity, fee, timestamp, order-ID, or trust inputs.
Then, under a separate apply authorization, submit the exact fingerprint:

```bash
python -m tools.legacy_recovery \
  --database-url-env PAPER_DATABASE_URL \
  --environment PAPER \
  --expected-database trading_paper \
  --deployment-id local-paper \
  --git-sha 0123456789abcdef0123456789abcdef01234567 \
  apply-open-retirement --position-id 10326 \
  --expected-fingerprint-v2 FINGERPRINT_FROM_PLAN \
  --confirm-apply
```

The transaction uses SERIALIZABLE isolation, 5-second lock timeout, 60-second
statement timeout, exact bounded evidence locks, re-planning, and semantic CAS.
It writes exclusion, order, fill, position linkage, terminal lifecycle,
Financial Truth, audit, and provenance in that order. Any error rolls back the
whole transaction. A repeated identical apply returns
`status=ALREADY_RETIRED` and `writes=0`.

Eligibility is fail-closed: PAPER, OPEN, pre-adoption legacy classification,
exactly one entry order/fill, positive fully executable remaining inventory,
no executed or ambiguous exit evidence, no terminal lifecycle or complete
Financial Truth, no prior repair/retirement/exclusion/provenance, a benign or
empty artifact snapshot, and fresh current market evidence. LIVE plan and
apply both return
`LIVE_RETIREMENT_NOT_AUTHORIZED` before opening a database connection.

### Historical unfilled exit-intent gate

An old unfilled PAPER simulated SELL intent is not an exit execution. The
planner classifies the exact slot snapshot as one of:

- `NO_EXIT_INTENTS`;
- `BENIGN_UNFILLED_LEGACY_EXIT_INTENTS`;
- `EXECUTED_OR_AMBIGUOUS_EXIT_EVIDENCE`.

Only the first two allow retirement. Benign classification requires every row
to be an exact post-entry PAPER simulated `SELL`/`is_exit` intent for the
position's symbol, strategy, interval and full remaining quantity, with the
derived status `PAPER_SIMULATED_UNFILLED_INTENT`. There must be zero fills,
filled quantity, inventory reduction, lifecycle linkage, terminal Financial
Truth, exchange/external identity, overlapping same-slot position, source
conflict or conflicting duplicate identity. A fill row, including a partial
fill, an unknown shape/status, an oversized or mismatched quantity, external
identity or ambiguous linkage blocks with
`EXIT_EVIDENCE_EXECUTED_OR_AMBIGUOUS` and a specific reason.

The public plan reports bounded counts, first/last identity, distributions and
hashes rather than thousands of rows. The semantic fingerprint includes a
hash of every ordered intent identity and content, status and quantity
distributions, fill state, external identity, lifecycle and Financial Truth.
Any new or changed intent or fill makes the old plan stale.

Historical benign rows are never deleted, updated, marked executed or linked
retroactively. A successful administrative close creates its own new
`LEGACY_ADMINISTRATIVE_CLOSE` order, fill and lifecycle evidence. The writer
uses an exact-slot transaction advisory lock shared by canonical PAPER exit
order/fill writers, then bounded row locks and a locked re-plan. It never takes
a table lock.

For benign legacy artifacts only, deployment compatibility is narrow:
shadow `NULL` requires the same position/environment/source identity;
warehouse/replay `legacy-unknown` additionally require their exact incomplete
status and `LEGACY_NOT_ATTRIBUTABLE`; registry `LOCAL` maps only to
`local-paper`, and registry `VPS` only to `vps-paper`. Unknown, terminal,
trusted, duplicated, conflicting, or source-mismatched rows block.

There is no batch command, one-off SQL path, LIVE variant, or migration in this
workflow. Apply it to one explicitly reviewed PAPER position at a time.

## Order-evidence capability contract V1

Source selection is based on the explicit `--environment`, deployment ID,
current database identity, and actual tables/columns. An env-file name is never
an authority. The supported variants are:

- `PAPER_SIMULATED_ORDER_SOURCE`: PAPER with canonical `simulated_orders` and
  `simulated_execution_fills_v1` evidence. It never queries `binance_orders`.
- `LEGACY_ORDER_SOURCE`: PAPER with a sufficient legacy `binance_orders` and
  `binance_order_fills` contract.
- `LIVE_EXCHANGE_ORDER_SOURCE`: LIVE exchange order/fill evidence. Incidental
  simulated tables do not change LIVE routing.
- `UNSUPPORTED_ORDER_SOURCE`: no executable contract; planning returns
  `ORDER_EVIDENCE_SOURCE_UNSUPPORTED` as JSON.

When both PAPER sources exist, simulated evidence has deterministic precedence.
An authoritative legacy linkage for the same position is not silently ignored:
it blocks with `ORDER_EVIDENCE_SOURCE_CONFLICT`.

`reconciled_position_id` is an optional historical linkage hint, not the only
authoritative identity. Explicit exchange/client order identity and
`position_id` remain sufficient. A legacy table without the optional column is
read with a typed `NULL::BIGINT` projection and never by a query that names the
absent column.

For old PAPER positions without stored order IDs, entry reconstruction requires
one exact candidate matching environment, symbol, strategy, interval, side,
derived order purpose, quantity, price, `created_at <= entry_time`, and a
maximum five-second delta. The timestamp only bounds an already exact match;
the planner never picks the nearest row. Zero candidates produce
`ENTRY_ORDER_EVIDENCE_NOT_FOUND`; multiple candidates produce
`ENTRY_ORDER_EVIDENCE_AMBIGUOUS`. Exit evidence follows explicit or canonical
fill linkage and the same exact-match rule where all fields exist.

Plan fingerprint V2 includes source type/table/primary key, exchange/client
identity, linkage classification, exact matching criteria, timestamp delta,
quantity, price, and status. Apply re-detects capabilities, locks those exact
source and fill rows, and re-resolves the candidate set. A changed source, row,
or new competing candidate produces `PLAN_STALE` before business writes.

Missing tables, optional columns, unsupported variants, and missing or
ambiguous order evidence must be controlled blockers, never PostgreSQL
tracebacks. No unambiguous evidence means no repair; the planner never guesses
order identity.

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

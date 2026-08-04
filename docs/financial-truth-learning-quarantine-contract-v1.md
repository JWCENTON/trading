# Financial Truth Learning quarantine contract V1

## Eligibility boundary

An outcome is available to a new Learning generation only when all four
conditions hold:

```text
positions.status = CLOSED
  + positions.exit_time is present
  + canonical_financial_truth_v1 status = COMPLETE for the same position_id
  + no active learning_outcome_exclusion_v1
```

`learning_outcome_is_eligible_v1(position_id)` is the shared predicate. Missing
Financial Truth, every non-`COMPLETE` status, an open position, a mismatched
Financial Truth identity, and an active exclusion all fail closed.

## Append-only compensation

Exclusion rows remain immutable. The expanded exclusion vocabulary supports
Financial Truth, inventory-ownership, and provenance containment while keeping
the original `LEGACY_REPAIR` contract valid.

An authorized reversal is a new
`learning_outcome_exclusion_resolution_v1` row with:

```text
resolution_action = REVOKE
source_type = MANUAL_GOVERNANCE_DECISION
non-empty reason, source_reference, and created_by
```

The resolution table is append-only and permits one terminal `REVOKE` per
exclusion. `v_learning_active_outcome_exclusions_v1` derives active state from
the exclusion and compensation histories; no mutable active flag exists.

## Dependency graph

```text
positions + canonical_financial_truth_v1
                    |
learning_outcome_exclusion_v1
                    |
learning_outcome_exclusion_resolution_v1
                    v
learning_outcome_is_eligible_v1
        |                         |
eligible artifact views          |
        |                         |
        +-------------------------+
                    v
learning_canonical_evidence_universe_live_v1
                    v
new frozen snapshots / memberships / feedback generations
```

The previously deployed live universe is preserved as
`learning_canonical_evidence_universe_pre_ft_quarantine_v1`. The public live
universe wraps it with the shared eligibility predicate. Existing physical
warehouse, shadow, snapshot, and membership rows are neither updated nor
deleted; they remain auditable but cannot enter a new canonical generation
through the live source.

## Migrations

Apply in order:

1. `20260804_learning_quarantine_vocabulary_v1.sql`
2. `20260804_learning_quarantine_resolution_v1.sql`
3. `20260804_learning_ft_eligibility_v1.sql`
4. `20260804_learning_canonical_universe_ft_quarantine_v1.sql`

Each migration is transactional and idempotent. None backfills Financial Truth,
repairs positions, or changes order, fill, strategy, execution, ORC, or bot
control state.

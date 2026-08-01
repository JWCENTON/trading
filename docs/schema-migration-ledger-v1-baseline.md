# Schema migration ledger V1 baseline

`schema_migration_ledger_v1` is the technical source of truth for completed
schema migrations. The tracked legacy position/fill recovery migration consumes
this ledger, while the repository previously had no tracked migration that
created it. A local ignored SQL artifact explained the runtime object but could
not serve as reproducible repository provenance.

`20260801_schema_migration_ledger_v1_baseline.sql` closes that gap. It must run
before `20260730_legacy_position_fill_recovery_v1.sql`. The baseline creates the
canonical table and lookup index when absent, then validates the complete
column, default, primary/check/unique/foreign-key, sequence, and index contract.
An existing incompatible object fails closed inside the transaction.

The baseline creates no self-registration row, performs no backfill, and does
not change positions, orders, fills, adoption, or any other trading data. It
preserves existing compatible ledger rows. Runtime rollout and database apply
remain separate operations.

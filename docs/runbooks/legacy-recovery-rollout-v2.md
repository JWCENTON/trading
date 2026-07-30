# Legacy recovery operator tooling

The legacy-recovery CLI is an operator-invoked, read-only tool owned by the
API image. It has no scheduler, HTTP endpoint, or `apply` command. PostgreSQL
enforces a read-only transaction for every command.

## Required configuration

Set a DSN in an explicitly named environment variable. The DSN is read but
never rendered. Pass the expected database and environment independently:

```bash
python -m tools.legacy_recovery \
  --database-url-env LIVE_DATABASE_URL \
  --environment LIVE \
  --expected-database trading_live \
  check-schema
```

The tool rejects a missing DSN, a database-name mismatch, an environment
mismatch, or a schema that does not match `LEGACY_RECOVERY_SCHEMA_V2`.

## Index contract

All rollout indexes are explicit and non-unique:

- `ix_legacy_repair_audit_incident_history` orders one incident's audit
  history by `recorded_at DESC, audit_id DESC`.
- `ix_legacy_repair_audit_semantic_expected` supports semantic-CAS audit
  lookup.
- `ix_legacy_repair_provenance_fingerprint` supports immutable evidence
  fingerprint lookup.
- `ix_legacy_repair_provenance_instrument_observed` supports instrument
  provenance ordered by observation time.
- `ix_exchange_fill_ingestion_recovery_lookup` resolves one explicit
  source/symbol/order/trade recovery identity.
- `ix_exchange_fill_ingestion_application` supports unapplied/application
  status classification.

Uniqueness is expressed separately by bounded identity constraints:
`invocation_identity` for audit idempotency and
`(evidence_source, source_identity)` for provenance conflict detection.

## Plans

Position planning requires one explicit ID:

```bash
python -m tools.legacy_recovery \
  --database-url-env LIVE_DATABASE_URL \
  --environment LIVE \
  --expected-database trading_live \
  --output-json /tmp/legacy-position-plan.json \
  plan-position --position-id 3080
```

Fill planning requires the complete source identity:

```bash
python -m tools.legacy_recovery \
  --database-url-env LIVE_DATABASE_URL \
  --environment LIVE \
  --expected-database trading_live \
  plan-fill --source okx --trade-id 341287 \
  --order-id 3788537826749489152
```

External classification reads an immutable provenance record. It never links
by symbol or time:

```bash
python -m tools.legacy_recovery \
  --database-url-env LIVE_DATABASE_URL \
  --environment LIVE \
  --expected-database trading_live \
  classify-external --source okx --trade-id 341617 \
  --order-id 3789163681263689728
```

Exit code `0` means the schema and requested read operation passed. Exit code
`2` is a configuration, evidence, identity, or safety failure. Exit code `3`
means `check-schema` completed but the schema is not ready. Output contains
database identity, evidence status, blocking reasons, actions, expected
changes, and invariants. It never contains database credentials.

## Migration and rollback

The forward migration registers
`20260730_legacy_position_fill_recovery_v1.sql` in the existing append-only
`schema_migration_ledger_v1` and validates its canonical manifest checksum.
Run it twice in rollout review to prove idempotency.

Rollback is allowed only while both recovery history tables are empty. The
rollback fails closed after any provenance or audit record exists. It restores
the prior ingestion status constraint and removes only objects introduced by
the forward migration. It does not use `CASCADE` and never changes positions,
orders, fills, or Financial Truth. The applied ledger entry remains as
immutable historical evidence.

## Future apply boundary

Writer services are internal and inaccessible from this CLI. A future bounded
apply command requires explicit incident IDs, invocation identity, expected
semantic fingerprint, environment confirmation, transactional CAS, and an
append-only audit record. Startup and planning never perform repair writes.

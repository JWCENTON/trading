# Learning Quarantine canonical fingerprint V1

The repository manifest and collector provide a portable comparison for the
Financial Truth Learning Quarantine Contract V1.

## Scope

The explicit manifest contains 53 identities:

| Type | Count |
| --- | ---: |
| Table | 2 |
| Column | 20 |
| Constraint | 8 |
| Index | 6 |
| Trigger | 2 |
| Function | 5 |
| View | 10 |

The CHECK definitions carry the exclusion and resolution vocabulary. No row
data, owner, ACL, OID, timestamps, statistics, sequence value, unrelated
Learning object, extension object, warehouse row, or historical snapshot is
collected.

## Canonical serialization

`POSTGRES_CATALOG_CANONICAL_JSON_V1` means:

1. read structural fields from `pg_catalog` in a `BEGIN READ ONLY` transaction;
2. set the deparse `search_path` to `pg_catalog` for deterministic schema
   qualification;
3. normalize CRLF/CR to LF;
4. remove SQL comments and collapse whitespace outside quoted values;
5. preserve single-quoted values, quoted identifiers, token/operator order and
   recursively normalized dollar-quoted bodies;
6. represent absent values as JSON `null`;
7. sort by `object_type`, `schema`, `object_name`, `subidentity`;
8. serialize compact JSON as UTF-8 with sorted keys;
9. calculate per-object SHA-256 and then SHA-256 of the canonical contract
   document, including contract/manifest/normalization versions and PostgreSQL
   major version.

The artifact is compact JSON terminated by one LF. The global hash is not a
hash of display formatting or of the artifact's redundant hash fields.

## LOCAL example

```bash
python3 scripts/learning_quarantine_contract_fingerprint_v1.py \
  --docker-container trading-paper-db-1 \
  --psql-arg=-U --psql-arg=botuser \
  --psql-arg=-d --psql-arg=trading_paper \
  --output audit_artifacts/learning_quarantine_contract_v1_local_paper.json
```

Compare two saved artifacts:

```bash
python3 scripts/learning_quarantine_contract_fingerprint_v1.py \
  --diff PAPER.json LIVE.json
```

The diff reports exact missing, extra and changed identities. Changed records
also report the structural definition fields and per-object hashes that differ.

## Read-only VPS handoff command

After pulling the commit, VPS Codex can run the following locally on each VPS
database host. It must substitute only the existing container name, database
name and user; it must not run a migration or copy a hand-written schema.

```bash
python3 scripts/learning_quarantine_contract_fingerprint_v1.py \
  --docker-container <postgres-container> \
  --psql-arg=-U --psql-arg=<database-user> \
  --psql-arg=-d --psql-arg=<database-name> \
  --output audit_artifacts/learning_quarantine_contract_v1_<environment>.json
```

The generated SQL is bounded to catalog reads and ends with `ROLLBACK`.

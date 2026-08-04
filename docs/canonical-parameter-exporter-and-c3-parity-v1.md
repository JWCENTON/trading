# Canonical parameter exporter and C3 parity V1

## Existing exporter RCA

The repository previously contained no versioned effective-parameter exporter or
parameter-parity artifact contract. LOCAL and VPS evidence was produced by
separate, task-local SQL/shell capture flows. They were useful for the targeted
RCA, but they were not safe as a durable C3 gate.

| Dimension | Previous LOCAL capture | Previous VPS capture | V1 closure |
|---|---|---|---|
| Field names | DB-shaped `param_name`/`param_value` plus RCA annotations | report-shaped parameter/value fields | one schema with `parameter_name`/`effective_value` |
| Record identity | implicit DB row/slot tuple | implicit report tuple | strategy + symbol + interval + parameter name |
| Source layering | current row and separate history inspection | effective value with task-local provenance | current DB layer plus latest matching history source |
| Slot identity | columns interpreted by the audit | serialized task-local label | four explicit identity fields |
| Runtime child | not part of a stable record contract | not consistently represented | deterministic `strategy:symbol:interval` |
| Sorting | query/report dependent | report dependent | environment, strategy, symbol, interval, parameter |
| Serialization | SQL/text/task JSON | task JSON | UTF-8 canonical JSON with LF |
| Null handling | tool dependent | tool dependent | explicit JSON null permitted by schema |
| Timestamps | could affect report comparison | could affect report comparison | retained as evidence, excluded from canonical hash |
| Hashing | no shared semantic projection | no shared semantic projection | SHA-256 over one documented semantic projection |

There is therefore no second repository exporter to retain or translate. V1
replaces both ad-hoc capture shapes with the same executable on LOCAL and VPS.

## Contract and normalization

The formal schema is `contracts/waltrade_parameter_export_v1.schema.json`.
The allowed PAPER drift decision is
`contracts/parameter_parity_allowed_differences_v1.json`.

The exporter reads only `strategy_params` under `BEGIN READ ONLY` and resolves
provenance from the newest matching `strategy_params_history` row. A row without
matching history receives the explicit identity `strategy_params:current_row`.
It never reads environment variables, credentials, exchange account identity,
hostnames, database hostnames or container metadata.

NUMERIC values are canonical decimal strings. Records are sorted by environment,
strategy, symbol, interval and parameter name. JSON is UTF-8, LF-terminated,
key-sorted and compact. The SHA-256 semantic projection includes effective value,
value type, source layer/identity/priority, slot identity and runtime consumer.
It excludes generated/source timestamps, deployment ID, Git/OCI metadata and all
host execution context.

## Export commands

The same command supports a DSN, a Docker container execution context, or a
Compose project. DSNs should use `PG*`, a service definition or `.pgpass`; do not
place secrets in an artifact.

Example container-context export:

```bash
python3 scripts/export_effective_parameters_v1.py \
  --db-container "$DB_CONTAINER" \
  --deployment-id "$DEPLOYMENT_ID" \
  --environment "$MODE" --mode "$MODE" \
  --runtime-git-sha "$RUNTIME_GIT_SHA" \
  --oci-revision "$OCI_REVISION" \
  --output "$OUTPUT_PATH"
```

Example Compose-context export:

```bash
python3 scripts/export_effective_parameters_v1.py \
  --compose-project "$COMPOSE_PROJECT" --compose-file "$COMPOSE_FILE" \
  --db-service db --deployment-id "$DEPLOYMENT_ID" \
  --environment "$MODE" --mode "$MODE" \
  --runtime-git-sha "$RUNTIME_GIT_SHA" \
  --oci-revision "$OCI_REVISION" --output "$OUTPUT_PATH"
```

## Deliverable LOCAL baseline bundle

Generated captures remain ignored under `audit_artifacts/`. Create a small,
secret-free hand-off bundle after both LOCAL exports:

```bash
python3 -m zipfile -c audit_artifacts/local_parameter_baseline_v1.zip \
  audit_artifacts/parameter_parity/local-paper.json \
  audit_artifacts/parameter_parity/local-live.json \
  contracts/waltrade_parameter_export_v1.schema.json \
  contracts/parameter_parity_allowed_differences_v1.json
sha256sum audit_artifacts/local_parameter_baseline_v1.zip
```

The zip is an operator-deliverable artifact, not a repository dependency. The VPS
operator must verify its communicated SHA-256 before comparison.

## Exact read-only VPS flow

After pulling the exporter commit and receiving the verified LOCAL bundle:

```bash
python3 -m zipfile -e /path/to/local_parameter_baseline_v1.zip audit_artifacts/local-baseline

python3 scripts/export_effective_parameters_v1.py \
  --db-container "$VPS_PAPER_DB_CONTAINER" \
  --deployment-id vps-paper --environment PAPER --mode PAPER \
  --runtime-git-sha "$VPS_PAPER_RUNTIME_GIT_SHA" \
  --oci-revision "$VPS_PAPER_OCI_REVISION" \
  --output audit_artifacts/parameter_parity/vps-paper.json

python3 scripts/export_effective_parameters_v1.py \
  --db-container "$VPS_LIVE_DB_CONTAINER" \
  --deployment-id vps-live --environment LIVE --mode LIVE \
  --runtime-git-sha "$VPS_LIVE_RUNTIME_GIT_SHA" \
  --oci-revision "$VPS_LIVE_OCI_REVISION" \
  --output audit_artifacts/parameter_parity/vps-live.json

python3 scripts/compare_effective_parameters_v1.py \
  --local audit_artifacts/local-baseline/audit_artifacts/parameter_parity/local-paper.json \
  --vps audit_artifacts/parameter_parity/vps-paper.json \
  --allowed-differences contracts/parameter_parity_allowed_differences_v1.json \
  --output audit_artifacts/parameter_parity/paper-comparison.json

python3 scripts/compare_effective_parameters_v1.py \
  --local audit_artifacts/local-baseline/audit_artifacts/parameter_parity/local-live.json \
  --vps audit_artifacts/parameter_parity/vps-live.json \
  --allowed-differences contracts/parameter_parity_allowed_differences_v1.json \
  --output audit_artifacts/parameter_parity/live-comparison.json
```

Exit code zero means every record is `MATCH` or an exact manifest-authorized
`ALLOWED_DIFFERENCE`. Missing, extra, unknown value or source-provenance drift is
non-zero. These commands perform no VPS writes and do not compare PAPER to LIVE.

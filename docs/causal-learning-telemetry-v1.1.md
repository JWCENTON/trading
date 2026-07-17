# Causal Learning Telemetry V1.1

V1.1 is an additive schema foundation over V1. It adds mandatory deployment
identity end-to-end, immutable decision observations, append-only promotion
consumption, explicit would-trade semantics, directional-only outcome statuses,
deployment-safe projections and audit views.

Existing rows are backfilled as `legacy-unknown`; the migration never guesses
local or VPS provenance. New runtime data must use an explicit supported
`DEPLOYMENT_ID`. V1.1 does not activate shadow observation, experiments or
auto-apply. Both related automation flags remain `0`.

The canonical V1 manifest remains available. Select V1.1 with:

```bash
python3 scripts/causal_learning_telemetry_fingerprint_v1.py \
  --manifest-version causal_learning_telemetry_v1_1 \
  --psql-arg=-d --psql-arg=DATABASE --output /tmp/causal-v1-1.json
```

The V1.1 manifest is explicit and covers 89 V1.1-owned records: tables, added columns,
constraints, indexes, triggers, function, audit views and flags. The canonical
fingerprint is
`29d0dd928f80501634a5b76ad6b1570fc0800efac9dffc976b320de795f3bb0f`.

Schema foundation, decision observation, future shadow attribution,
directional counterfactual analysis, future experiments and future auto-apply
are separate authorities. This migration implements only the first two.

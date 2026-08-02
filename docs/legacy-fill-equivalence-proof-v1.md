# Legacy Fill Equivalence Proof V1

This contract records evidence that a closed, explicitly enumerated legacy
correction cohort is semantically equal across three independent surfaces:
the latest ingestion payload, the canonical local fill, and a fresh read-only
OKX response. It is not application proof and never changes the correction
ledger or the canonical fill.

## Fixed semantics

Every row has:

```text
proof_type             = LEGACY_CANONICAL_OKX_EQUIVALENCE
equivalence_state      = PROVEN
fill_mutation_required = false
repair_impact          = NONE
application_state      = NOT_APPLIED (evidence payload only)
```

The only admitted cohort is ingestion `8, 10, 12, 14, 16, 18, 19, 20`,
linked to positions `3079, 3081, 3082, 3084, 3085`. Ingestion
`22, 23, 24, 25`, DB orders `3758, 3760, 3762`, and OKX order
`3789163681263689728` are quarantined and cannot receive proof.

## Schema

`legacy_fill_equivalence_proof_v1` is append-only. Row triggers reject
`UPDATE` and `DELETE`; a statement trigger rejects `TRUNCATE`. The base
identity is unique, so the same identity with changed fingerprints conflicts.
The wider evidence identity is also unique, and identical replay is an
idempotent no-op.

`v_legacy_fill_equivalence_proof_status_v1` compares immutable proof with the
current ingestion and canonical fill states and returns:

```text
VALID
STALE_INGESTION_REVISION
STALE_OBSERVED_FINGERPRINT
STALE_CANONICAL_FILL
MISSING_CANONICAL_FILL
IDENTITY_CONFLICT
```

The view never calls OKX. A fresh OKX GET is mandatory during proof planning,
proof apply, and any later residual-repair diagnostic.

## Fingerprints

The latest-observed fingerprint is verified with the existing
`authoritative_fill_fingerprint` contract. Canonical and OKX fingerprints use
the same semantic payload: source, account identity key, symbol, trade/order
identity, side, Decimal-normalized quantity/price/quote quantity/fee, fee
currency, event time in milliseconds, and the contextual canonical fill ID.
JSON keys are sorted, separators are compact, nulls are explicit, and SHA-256
is calculated over UTF-8.

## CLI and apply gates

The CLI defaults to PLAN:

```bash
python3 tools/legacy_fill_equivalence_proof_v1.py \
  --database trading_live \
  --manifest config/legacy_fill_equivalence_proof_v1.json \
  --expected-git-sha "$(git rev-parse HEAD)"
```

PLAN works before schema rollout and reports `schema_status=MISSING` without
DB writes. Apply additionally requires all explicit identity arguments and
`LEGACY_FILL_EQUIVALENCE_PROOF_APPLY_ENABLED=1`. The eight ingestion and eight
canonical fill identities are locked and inserted in one serializable
transaction. Any mismatch rolls the entire cohort back.

Schema rollout and proof apply require separate Product Owner authorization.
This implementation task does neither on LOCAL LIVE.

# Canonical Learning identity-conflict characterization V1

## Evidence scope

The immutable LOCAL LIVE production-data baseline
`waltrade-local-live-validation-baseline-20260723_114513` was restored into an
isolated PostgreSQL 16 clone and upgraded through the current forward
sequence. Its 30-day canonical universe contained 20 `ELIGIBLE` rows and zero
`EXCLUDED_CONFLICTING_IDENTITY` rows after the authorized exact 98b4 repair.

The same read-only audit was run against LOCAL PAPER to characterize the
previously observed conflict population. The complete per-key output is
reproducible with:

```text
scripts/learning_canonical_identity_conflict_audit_v1.sql
```

It emitted 708 rows, one for every unique decision key. The sorted key-set MD5
is `06e627dfab02fec35f0f291076c78d37`; the first key is
`00253a5476f93832f3caf52e072882ed` and the last is
`ffa021685022779ca8739bb68771ee0f`.

## Per-key result

Every one of the 708 rows has the same structural classification:

- registry decision IDs: none;
- outcome decision IDs: none;
- registry deployment instance/deployment/environment: none;
- registry source table and registry timestamps: none;
- registry rows: 0;
- outcome rows: 0;
- Feedback source candidate: yes;
- Manifest membership: no;
- warehouse/position linkage: present;
- complete lifecycle/provenance: false because canonical registry/outcome
  provenance is absent;
- legacy-identity-only conflict: false;
- exact reason:
  `FALSE_CONFLICT_MISSING_REGISTRY_COUNT_STAR`.

All 708 keys have a position link; 88 also have an entry or exit client-order
link. Warehouse evidence has two rows for 47 keys, three rows for 215 keys and
four rows for 446 keys. These are repeated analytical warehouse projections,
not duplicate canonical identities.

The script emits symbol, timeframe, strategy, position ID, available
order/client-order linkage, warehouse timestamps and row cardinalities for
each key. Registry/outcome timestamps and identity fields are correctly NULL
because those records do not exist.

## Aggregate classification

| Classification | Count |
|---|---:|
| unique decision keys | 708 |
| registry-only | 0 |
| outcome-only | 0 |
| real decision-ID mismatch | 0 |
| real deployment mismatch | 0 |
| real environment mismatch | 0 |
| canonical provenance missing | 708 |
| chronology mismatch | 0 |
| true duplicate identity | 0 |
| legacy-compatible complete identity | 0 |
| unrepairable/unknown | 0 |

Slot distribution:

| Symbol | Interval | Strategy | Keys |
|---|---|---|---:|
| BNBUSDC | 1m | BBRANGE | 31 |
| BNBUSDC | 1m | TREND | 3 |
| BNBUSDC | 5m | BBRANGE | 22 |
| BNBUSDC | 5m | TREND | 9 |
| BTCUSDC | 1m | BBRANGE | 58 |
| BTCUSDC | 1m | RSI | 88 |
| BTCUSDC | 1m | TREND | 58 |
| BTCUSDC | 5m | BBRANGE | 29 |
| BTCUSDC | 5m | TREND | 14 |
| ETHUSDC | 1m | BBRANGE | 100 |
| ETHUSDC | 1m | TREND | 14 |
| ETHUSDC | 5m | BBRANGE | 59 |
| ETHUSDC | 5m | TREND | 23 |
| SOLUSDC | 1m | BBRANGE | 122 |
| SOLUSDC | 1m | TREND | 15 |
| SOLUSDC | 5m | BBRANGE | 51 |
| SOLUSDC | 5m | TREND | 12 |

## Root cause and disposition

The deployed canonical function computes `registry_rows` with `count(*)` over
a `LEFT JOIN`. PostgreSQL returns one grouped join row even when the registry
side is NULL. The subsequent classifier therefore sees
`registry_rows = 1`, skips `EXCLUDED_MISSING_REGISTRY`, then sees
`registry_ids = 0` and incorrectly labels the key
`EXCLUDED_CONFLICTING_IDENTITY`.

The additive V1.1 migration replaces only that aggregate with `count(r.*)`.
For the audited PAPER population this deterministically reclassifies all 708
rows as `EXCLUDED_MISSING_REGISTRY`; eligibility and Manifest membership stay
unchanged.

No data repair is justified. There is no registry/outcome identity to merge,
no duplicate to delete and no safe provenance from which to synthesize 708
canonical decisions. Missing producer history remains excluded and explicit.

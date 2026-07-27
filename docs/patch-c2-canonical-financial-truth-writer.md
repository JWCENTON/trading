# Patch C2 — Canonical Financial Truth Writer

Patch C2 adds the only canonical writer:
`FINANCIAL_TRUTH_RECONCILER_V1`. It reads persisted execution evidence and
writes `canonical_financial_truth_v1` plus an append-only semantic audit.
No API, strategy, ORC, Learning, Replay, Warehouse or execution component may
write canonical Financial Truth.

## Product-owner contracts

- `executed_entry_qty`/`gross_entry_qty` and
  `executed_exit_qty`/`gross_exit_qty` are gross fill facts.
- `net_entry_inventory_qty` subtracts verified base-asset entry fees.
- `net_exit_inventory_reduction_qty` is distinct from gross exit execution.
- `remaining_qty` remains the backward-compatible economic inventory value and
  mirrors `remaining_inventory_qty`; gross execution remainder is separate.
- Candle-derived third-asset conversions are estimated. They cannot populate
  authoritative fees/net PnL or permit COMPLETE.
- Fee role is evidence-based: BASE and QUOTE require matching persisted
  instrument metadata, while THIRD requires both base and quote assets to be
  known and the fee asset to differ from both. Missing or partial metadata is
  `FEE_ASSET_ROLE_UNKNOWN`, never evidence of a third-asset fee. A persisted
  fee valuation without that metadata remains estimated and is reported as
  `MISSING_INSTRUMENT_METADATA` plus `FEE_VALUATION_ESTIMATED`.
- Exchange evidence requires UID/mainUid from read-only OKX account config.
  Environment, deployment and credentials are never account identity.
- Quantity tolerance comes from the persisted execution-time instrument
  snapshot. Without it, a non-zero remainder cannot be classified as dust.
- Missing provenance is INCOMPLETE. Conflicting verified provenance is FAILED.

## Sources

LIVE uses reconciled exchange fills linked through `binance_orders`, with
exchange UID and execution-time instrument metadata. PAPER uses new
`simulated_execution_fills_v1` rows. A simulated order alone and all
position-local values are non-authoritative.

PAPER simulator evidence uses the versioned
`PAPER_SIMULATOR_FINANCIAL_MODEL_V1`: one persisted fill per accepted simulated
order and a quote-currency fee of 0.0004 of fill notional, matching the existing
PAPER lifecycle fee model. A random simulated account UUID is generated once,
persisted per deployment, and reused. It is not derived from a database or
environment label.

## Activation

Defaults are fail-closed:

```text
FINANCIAL_TRUTH_WRITER_ENABLED=0
FINANCIAL_TRUTH_WRITER_MODE=disabled
FINANCIAL_TRUTH_WRITER_ENV_ALLOWLIST=paper
```

`disabled` performs no calculation or write. `dry-run` and `shadow` calculate
without writes. `apply` requires all three gates and is limited to PAPER in C2.
No startup hook, GET side effect, automatic scanner or unbounded batch exists.

Example:

```bash
python -m tools.financial_truth_reconcile \
  --environment paper --position-id 123 --mode dry-run --limit 1 --json
```

## Concurrency and audit

Apply takes a transaction-scoped advisory lock per position, reads evidence
inside the transaction, and compares the deterministic source fingerprint.
Identical evidence produces neither canonical churn nor a duplicate audit.
Every semantic transition stores previous/new status, fingerprint and values
in `canonical_financial_truth_audit_v1`.

Historical records are not enriched or backfilled. The legacy reconciliation
threshold `0.999` remains runtime behavior and is not a canonical tolerance.

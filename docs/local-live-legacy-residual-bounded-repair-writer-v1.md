# LOCAL LIVE legacy residual bounded repair writer V1

## Correction trust boundary

Pending legacy corrections are accepted only through one of two explicit
trust sources:

```text
NATIVE_APPLICATION_PROOF
LEGACY_EQUIVALENCE_PROOF
```

Positions with no ingestion history at all use the separate
`CANONICAL_OKX_DIRECT_EVIDENCE` source. It is valid only when the complete
canonical entry/exit fill identity and Decimal evidence match fresh filled OKX
orders and trades. A partially present ingestion history still fails closed;
this source never substitutes for a missing correction proof.

Native proof requires a matching applied fingerprint, application timestamp,
local fill, adoption and generation. The legacy alternative requires a
`VALID` row from `v_legacy_fill_equivalence_proof_status_v1`,
`fill_mutation_required=false`, `repair_impact=NONE`, and a fresh OKX GET whose
semantic fingerprint still matches the immutable proof. Missing proof raises
`BLOCKED_BY_MISSING_EQUIVALENCE_PROOF`; stale proof raises
`BLOCKED_BY_STALE_EQUIVALENCE_PROOF`. Mere `CORRECTION_PENDING` or current
canonical/OKX equality is never sufficient.

## Scope and root cause

The bounded cohort is exactly LOCAL LIVE positions 3079–3085. Their authoritative
OKX entry and exit fills exist, but the legacy lifecycle projection did not account
for entry fees charged in the base asset. That left the rows OPEN after their exits.
The repair is data-only because it reconstructs inventory and Financial Truth from
the already committed order/fill evidence; it never creates or changes an exchange
order or fill.

DB order rows 3758, 3760 and 3762, ingestion rows 22–25, and OKX manual SELL
3789163681263689728 are a separate quarantined incident. They are not attributable
to this cohort and must never receive position linkage, outcome synthesis, Financial
Truth, or a mutation from this writer.

## Contract

`tools/local_live_legacy_residual_repair.py` defaults to PLAN. The closed manifest
contains only LIVE/local-live, the seven position IDs, exact entry/exit order IDs,
and semantic fingerprints. Each run rereads the position, orders, fills, ingestion
high-water, instrument limits, current OKX orders/fills/pending orders, account
identity, lifecycle, Financial Truth, repair provenance/audit, Learning exclusion,
and incomplete Learning artifacts.

The writer fails closed unless the runtime is OKX LIVE/local-live at the exact Git
revision and `trading_live` database, all 32 LIVE order switches are disabled,
pending OKX SPOT orders are empty, panic state is known, authority is
`automation_runner`, and the orchestrator role is `PROCESS_SUPERVISOR`. Apply also
requires `--apply`, explicit LIVE/local-live, the manifest, exact Git SHA, exact DB,
and `LOCAL_LIVE_LEGACY_RESIDUAL_REPAIR_APPLY_ENABLED=1`.

Inventory uses the shared C2.2 Decimal contract:

```text
gross entry = sum(entry fill quantity)
base fee = sum(entry fee charged in base asset)
net entry = gross entry - base fee
exit reduction = sum(exit fill quantity plus any exit fee charged in base)
raw remaining = net entry - exit reduction
```

Only a small negative quantization delta within the instrument lot tolerance is
normalized to zero. A positive non-executable remainder is a canonical terminal
dust close. The shared canonical Financial Truth calculator and writer derive
notionals, fees, gross PnL and net PnL only from matched fills and fee evidence.

## Mutation, quarantine, and recovery policy

Future apply uses one SERIALIZABLE transaction per position. It locks the position,
orders and fills, repeats all evidence and fingerprint checks, inserts the canonical
Learning exclusion first, applies the shared inventory/lifecycle mutation, writes
canonical Financial Truth and its audit, then appends repair audit and provenance.
Postconditions are checked before commit. Any error rolls back that entire position
and stops the run; there is no automatic compensation or continuation.

The exclusion uses the existing `LEGACY_REPAIR` / `LEGACY_POSITION_REPAIR`
contract. Existing incomplete Replay and Feature Warehouse rows remain append-only,
while all eligible Learning views exclude the repaired outcome. A complete repeated
run with the same fingerprint is an `ALREADY_REPAIRED` no-op. Partial artifacts or
any fingerprint conflict fail closed.

## Production apply runbook

Production apply is intentionally not authorized by this patch. After a fresh PLAN
passes and Product Owner gives a separate explicit approval, record pre-mutation
counts and run the CLI with every apply gate and the canonical manifest. Verify each
position transaction independently, the 4 full closes and 3 terminal-dust closes,
complete Financial Truth, exclusion from every eligible Learning view, append-only
audit/provenance, zero pending orders, and zero OKX place/cancel calls. On the first
failure, stop and preserve the rolled-back evidence for review; do not retry with a
changed manifest or bypass a gate.

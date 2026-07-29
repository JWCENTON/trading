# C2.2.1 legacy replay and correction safety

C2.2.1 separates observing exchange evidence from permission to mutate an
authoritative position. A repeated API response, a restart, or a NULL inventory
projection is never a mutation trigger.

## Row generations and adoption

LIVE rows have one of three explicit generations:

- `FORWARD_C2_2`: the position entry time is at or after the persisted
  `FEE_AWARE_INVENTORY_C2_2` adoption boundary for its environment and
  deployment.
- `LEGACY_UNPROJECTED`: the row predates that boundary and its C2.2 fields may
  remain NULL.
- `LEGACY_RECONSTRUCTION_APPROVED`: reserved for a separate, explicitly scoped
  and audited command. C2.2.1 does not implement historical repair.

`runtime_contract_adoption_v1` stores contract, environment, deployment,
adoption time, Git revision, and migration version. Its migration inserts no
adoption row. Rollout must persist the boundary deliberately; schema
installation alone cannot adopt legacy positions.

## Fetch and ingestion boundaries

OKX fills history does not accept the Binance-style `startTime` used by the
caller. The adapter therefore applies a declared local event-time boundary and
returns diagnostic metadata: whether filtering was applied, its mode, the
requested boundary, and the effective correction-lookback boundary.

Filtering is only the first replay guard. `exchange_fill_ingestion_state_v2`
uses stable exchange/account/instrument/trade identity. It records first and
last observation, authoritative and applied fingerprints, correction revision,
application status, and the last mutation decision.

The authoritative fingerprint includes exchange, account identity, instrument,
trade and order IDs, side, quantity, price, fee quantity/currency, and event
time. Consequently fee-only, price, fee-currency, and upward-quantity
corrections remain visible when event time and fill count do not change.
Downward quantity corrections are `AMBIGUOUS_CORRECTION` and are blocked.

## Mutation decisions

Only `NEW_AUTHORITATIVE_EVIDENCE` and `AUTHORITATIVE_CORRECTION` may reach LIVE
inventory mutation. `NO_CHANGE`, `LEGACY_RECONSTRUCTION_BLOCKED`,
`INCOMPLETE_EVIDENCE`, and `AMBIGUOUS_CORRECTION` are position and lifecycle
no-ops.

LIVE exit reconciliation additionally requires the changed order identity and
a forward adoption match. It never uses `inventory_calculated_at IS NULL` as a
trigger. Incomplete projection does not update `qty`, status, remaining
inventory, terminal fields, or lifecycle outbox.

PAPER simulated fills retain their existing transaction and quantity contract.
They do not call private exchange APIs and continue to keep
`positions.qty = remaining_inventory_qty`.

## Duplicate, retry, and rollout semantics

An already stored fill with the same fingerprint is bootstrapped into the v2
ledger as `DUPLICATE`; ten re-fetches remain ten observations and zero new
mutations. An accepted correction increments its revision. Lifecycle outbox
uniqueness remains `(position_id, order_id, mutation_kind,
mutation_high_water)`, so retry cannot duplicate an identical event.

Rollout prerequisites are: tests, both additive migrations, an explicit
adoption record captured at the deployment database boundary, immutable image
identity, and a pre/post snapshot proving that legacy quantities and statuses
did not move. Migration plus restart alone must leave every legacy row and the
outbox unchanged.

# Long Horizon L3 direct LOCAL PAPER V2

`STATUS=CORRECTED_IDEMPOTENT_PRE_ROLLOUT_CAPACITY_WAIT`

V2 supersedes activation authority of the terminated
`LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V1` contract. V1 remains immutable
historical evidence; it is never reactivated and its migration ledger is not
rewritten.

## Frozen V2 contract

- `CONTRACT_VERSION=LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V2`
- `TREATMENT_FINGERPRINT=5ac39665922d9fc5fa4b5d5a56482b760f72dfca61c601104ca1caa7ef1c5b15`
- `SAMPLING_FINGERPRINT=759b2a8a55c778719fe5e78d27d371584f23bb4c57de45e3c776a4c77bdcb161`
- `ENTRY_NOTIONAL=9_USDC`
- `ALLOW_SAMPLING=13.194281540_PERCENT`
- `BLOCK_SAMPLING=7.920637611_PERCENT`
- `SAMPLING_SALT=0487462154f625b36982d5437a9d039ff8ceb5db5e2835afc0d47e6908a16057`
- `TARGET=+3_PERCENT_REALIZABLE_NET_AFTER_ALL_FEES`
- `REQUIRED_COMPLETED_ALLOW_EPISODES=33`
- `REQUIRED_COMPLETED_BLOCK_EPISODES=53`
- `INDEPENDENCE_UNIT=CANONICAL_SAME_THESIS_EPISODE`
- `REGIME_COMPUTED_AND_PERSISTED=YES`
- `L3_REGIME_ENTRY_AUTHORITY=NON_BLOCKING`

V2 admits only gate events whose immutable `created_at` is at or after the V2
contract `start_cutoff`. Any earlier position or gate event is excluded as
`PRE_L3_TRANSITIONAL_EXCLUDED`. The cutoff is created only by the successful
first application; no cutoff has started while V2 remains undeployed.

## Migration idempotency and rollback

The V2 migration updates `bot_control` only when `regime_enabled` or
`regime_mode` changes semantically. A repeated application does not change
`updated_at`, contract timestamps, contract payload, fingerprints, sequences,
or migration ledger. The contract captures the exact pre-activation
`bot_control` values and timestamps so the V2 rollback restores those 32 rows
exactly while retaining a terminated V2 contract and rollback provenance.

The migration is LOCAL PAPER only and rejects every other deployment before
writes. It also refuses activation while any audited transitional position is
still open.

## Transitional entries and capacity

The short unintended V1 `DRY_RUN` window admitted five entries that the regime
policy marked `POLICY_WOULD_BLOCK`. They are preserved without mutation:

| Position | Decision | Notional (USDC) | Status | Classification |
| ---: | --- | ---: | --- | --- |
| 13546 | `ca66d210-fe4b-5527-b42b-cfb5e528c917` | 19.999693300 | OPEN | PRE_L3_TRANSITIONAL_EXCLUDED |
| 13547 | `16c4ea88-81d5-5927-96ce-a2d32d084e17` | 19.998664290 | OPEN | PRE_L3_TRANSITIONAL_EXCLUDED |
| 13548 | `e906db42-d075-5bec-910d-5530725a289f` | 19.999265940 | OPEN | PRE_L3_TRANSITIONAL_EXCLUDED |
| 13549 | `df7bcf89-d8ba-5252-91c0-fdb93abeaca7` | 19.999497000 | OPEN | PRE_L3_TRANSITIONAL_EXCLUDED |
| 13550 | `f4de0b6b-17ea-50a4-96d0-c9ad8a308fcf` | 20.005531650 | OPEN | PRE_L3_TRANSITIONAL_EXCLUDED |

At the read-only audit, total transitional occupancy is
`100.002652180 USDC`; canonical managed equity is `635.083134663183792231
USDC`. Because these are all would-block entries, the 20% BLOCK sleeve has
only `27.0139747526367584462 USDC`, or three full 9 USDC slots, free. The
required 14 BLOCK slots are not available. Global 60% heat has 31 full 9 USDC
slots available and free cash is `535.080482483183792231 USDC`, but those facts
do not override the BLOCK sleeve failure.

`CAPACITY_PASS=NO`

V2 may be reconsidered only after all five positions close naturally and a
fresh read-only capacity preflight proves at least 14 BLOCK slots plus the
global-heat and minimum-free-cash constraints. No manual close is authorized.

## Authority

`MIGRATION_APPLIED_TO_ACTIVE_TRADING_PAPER=NO`

`RUNTIME_REBUILT=NO`

`RUNTIME_RECREATED=NO`

`NEW_L3_CUTOFF=NOT_STARTED`

`LOCAL_LIVE_CHANGED=NO`

`VPS_CHANGED=NO`

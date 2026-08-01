# PAPER exit preflight containment

## Scope and root cause

RSI, TREND, SUPERTREND, and BBRANGE reserved and committed a
`simulated_orders` exit row before the PAPER position mutation path evaluated
the C2.2 adoption/generation contract. A denied mutation therefore left an
orphan exit intent and produced misleading `POSITION_CLOSE_FAILED` telemetry.
The boolean mutation guard also could not explain the rejection.

The old order was:

```text
exit trigger -> simulated order commit -> fill/lifecycle path -> mutation guard
```

The new order is:

```text
exit trigger -> read-only diagnostic preflight -> existing execution path
             \-> deny: PAPER_EXIT_PREFLIGHT_BLOCKED only
```

The preflight returns `allowed`, `reason_code`, position identity/status,
position adoption/generation, active adoption/generation, and legacy
compatibility. Denials distinguish `POSITION_NOT_FOUND`,
`POSITION_ALREADY_CLOSED`, `MISSING_ADOPTION_ID`, `MISSING_GENERATION`,
`GENERATION_MISMATCH`, `LEGACY_NOT_COMPATIBLE`,
`ENTRY_BEFORE_ACTIVE_ADOPTION`, `INVENTORY_CONTRACT_INCOMPLETE`, and the
fail-closed fallback `MUTATION_NOT_ALLOWED_OTHER`.

## Atomicity and behavior

The preflight transaction takes a transaction-scoped PostgreSQL advisory lock
for the PAPER deployment/symbol/strategy/interval slot and holds it through the
existing execution path. This serializes competing patched workers without a
durable database mutation. A retry observes the position after the preceding
worker completes and cannot reserve another exit order.

The preflight does not replace mutation-time validation. RSI retains its SSOT
guard, TREND and BBRANGE retain their close guards, and SUPERTREND now applies
the same guard before its conditional close. The exact preflight position ID is
carried into mutation so an intervening writer cannot redirect the exit to a
different position. The conditional `status='OPEN'` update remains the final
TOCTOU barrier.

Legacy positions that are not compatible with the active contract remain OPEN.
Each qualifying exit trigger emits `PAPER_EXIT_PREFLIGHT_BLOCKED` and creates no
simulated order, fill, position mutation, lifecycle row, or
`POSITION_CLOSE_FAILED`. No durable suppression marker is introduced, so a
later bounded repair can make the position eligible.

For compatible positions, control enters the unchanged simulated execution,
fill evidence, inventory lifecycle, fee, PnL, and Financial Truth path. Entry
logic and all LIVE paths bypass this PAPER-only preflight.

## Verification and rollout separation

Unit coverage exercises the diagnostic matrix, fail-closed zero-side-effect
behavior, all four strategy entry points, and LIVE isolation. Disposable
PostgreSQL coverage proves sequential/concurrent serialization produces at most
one order and one close. Existing lifecycle, simulated execution, Financial
Truth, adoption/generation, and full regression suites protect successful-path
financial semantics.

This is forward containment only. It neither repairs historical positions nor
removes historical orders. Runtime rollout, observation, and any bounded repair
of positions 10326/10333/10340 are separate authorized operations.

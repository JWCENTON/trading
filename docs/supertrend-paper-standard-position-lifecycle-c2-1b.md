# SUPERTREND PAPER standard position lifecycle — C2.1B

## Root cause

SUPERTREND originally materialized PAPER positions. SSOT refactor `801be28`
correctly moved LIVE position creation behind a confirmed exchange fill, but
left the common PAPER early return without an equivalent position writer. The
later stateful test characterized that mismatch as a known legacy gap; it was
not a normative business contract.

## PAPER lifecycle

After a successful, idempotent `simulated_orders` insert, PAPER entry creates
one standard OPEN `positions` row from the already evaluated symbol, interval,
price, quantity and candle time. The concrete position ID and simulated-order
ID are then passed directly to `record_simulated_fill_evidence()`. Only after
the position mutation succeeds may the strategy emit `POSITION_OPENED`.

PAPER exit first identifies the existing OPEN position, creates the simulated
exit order, and attempts direct EXIT evidence using that same position ID.
The existing strategy caller then closes exactly that position. This ordering
prevents an evidence row from referring to a different position and retains
the established fail-open evidence policy: evidence failure is logged but
does not roll back an already successful trading lifecycle action.

The shared evidence helper uses cached instrument filters only
(`allow_remote=False`). C2.1B performs no ticker, candle, sizing, metadata or
private exchange lookup.

## State, duplicate prevention and recovery

The persisted OPEN position is the position-state SSOT. Existing
`get_open_position()` guards later candles and continues to work after process
restart. The existing simulated-order uniqueness constraint blocks same-candle
retries before any position or evidence mutation. Evidence remains idempotent
by simulated-order/fill identity.

## LIVE invariant

LIVE execution is unchanged. It does not use the PAPER position writer or
simulated C2 evidence path. A LIVE position is still created only after
confirmed exchange execution, through the existing ACK path. Signals, gates,
sizing, risk controls, ORC decisions, exchange execution and FinalDecision
semantics are unchanged.

## Forward-only data policy

This patch adds no schema or migration and performs no backfill. Historical
SUPERTREND records remain `LEGACY_NOT_ATTRIBUTABLE` or `OBSERVATION_ONLY`;
canonical linkage must never be reconstructed heuristically. The new lifecycle
begins only with runtime events produced by a future reviewed rollout.

Rollback is an image rollback. No database reversal or historical repair is
part of C2.1B.

## North Star alignment

All active PAPER strategies now use the standard Position Lifecycle and direct
Financial Truth execution evidence contract. SUPERTREND PAPER can therefore
participate in restart-safe position management, UI and ORC position readers,
Financial Truth, Learning outcomes and the common decision-quality loop.

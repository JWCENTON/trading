# Four-strategy exit close outcome contract — C2.1C

For RSI, TREND, BBRANGE, and SUPERTREND, a successful exit requires both a
recorded execution and a successful conditional close of the exact OPEN
position. A simulated exit order or EXIT evidence alone is not a successful
lifecycle outcome.

The close boolean is authoritative:

- `True`: emit `POSITION_CLOSED` exactly once and allow successful EXIT.
- `False`: keep the position logically OPEN, emit/log
  `POSITION_CLOSE_FAILED`, and return a non-successful FinalDecision.
- operational exception: preserve it as position-close failure; never classify
  it as missing Financial Truth evidence.

PAPER evidence may be attempted before or after the close depending on the
existing strategy boundary. Evidence represents an execution event and remains
fail-open. Evidence failure after a successful close may leave Financial Truth
UNKNOWN or INCOMPLETE, but does not falsify lifecycle state or trigger a second
close.

PAPER and LIVE branches are explicit. Unknown mode fails before simulated
order, position, evidence, or exchange mutation. LIVE ACK/fill gating,
execution, risk, sizing, signals, and exit ordering are unchanged.

This contract is forward-only. It performs no schema migration, backfill,
historical repair, or reconstruction. C2.1 is not deployed and requires a
repeated combined final review before any image build or rollout.

C2.1D further requires `positions.status=CLOSED` before Financial Truth can be
COMPLETE. Execution evidence alone is not lifecycle completion. An OPEN
position with EXIT evidence is the typed, non-canonical
`POSITION_LIFECYCLE_NOT_CLOSED` conflict. It is never written by apply.
BBRANGE close exceptions now fail closed, and a missing
`position_close_succeeded` field never defaults to EXIT success.

C2.1E makes telemetry follow the same committed mutation boundary. A success
event is emitted exactly once and only after OPEN/CLOSE mutation success.
Skipped, false, rolled-back, or exceptional mutations use failure/blocked
telemetry and never an event type containing OPENED or CLOSED.

# Four-strategy position telemetry contract — C2.1E

For RSI, TREND, BBRANGE, and SUPERTREND, position telemetry follows committed
database lifecycle state:

- successful OPEN mutation emits `POSITION_OPENED` exactly once;
- failed or skipped OPEN emits no event type containing `OPENED`;
- successful conditional CLOSE emits `POSITION_CLOSED` exactly once;
- false, rolled-back, or exceptional CLOSE emits no event type containing
  `CLOSED` and uses explicit failure/blocked telemetry.

Each strategy has one success authority. RSI entry and close success are
reported by callers after the position-producing helper result. TREND position
helpers own OPEN/CLOSE success. BBRANGE PAPER entry uses its caller while its
close helper owns close success. SUPERTREND entry uses its caller and its close
helper owns close success.

Telemetry is not lifecycle truth. Position mutation results remain authoritative
for FinalDecision, and `positions.status` plus authoritative evidence remain
authoritative for Financial Truth. Event delivery cannot turn a failed mutation
into a successful outcome.

This contract is forward-only. It adds no migration, schema change, backfill, or
historical event reconstruction. It changes no signal, entry/exit condition,
sizing, risk, execution ordering, ORC, LIVE ACK behavior, or PAPER/LIVE source
routing. C2.1 remains local and not deployed pending final combined review.

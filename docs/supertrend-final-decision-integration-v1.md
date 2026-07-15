# SUPERTREND FinalDecision integration

SUPERTREND returns the shared, immutable `FinalDecision` as the terminal result
of each evaluated strategy cycle. The production loop continues to ignore the
return value. There is no decision sink, schema change, persistence, logging or
runtime configuration added by this integration.

## Boundary and mapping

The decision is constructed only after the legacy branch has emitted its
events and, where applicable, completed execution and position mutation. HALT,
disabled hours/bot and readiness outcomes map to suppressed or not-evaluated
results; `NO_SIGNAL` maps to `NO_TRADE`; an unchanged open position maps to
`POSITION_HOLD`; regime and risk gates map to blocked results. Entry and exit
execution results are normalized into paper simulation, confirmed execution,
suppression, rejection, pending fill, partial execution or ledger failure.

Legacy reason strings remain in `reason_text` or `details.legacy_reason` when a
more immediate execution-block reason occupies `reason_text`. Symbol, interval,
strategy, candle identity, price, side, position identity and execution fields
are retained in the evaluation context and decision metadata.

## Preserved invariants

Event order and payloads, heartbeat behavior, regime checks, sizing, execution
calls, confirmed-fill semantics, position lifecycle, profit-lock persistence,
and the `RUN_END` finally event are unchanged. FinalDecision construction is
observational and cannot initiate an order, position mutation or event write.
The stateful characterization harness asserts the existing sequences and adds
contract assertions for no-signal, hold, gates, entries, TP/SL, profit lock,
flip, metadata, reasons and exactly-once effects.

## Legacy findings outside scope

The existing strategy emits some non-signal and volatility outcomes as legacy
`BLOCKED` events, and partial live exits close the position according to the
pre-existing `live_ok` semantics. These behaviors are characterized and were
not changed. Panic mode also retains its existing mode-transition behavior.

No deployment, Docker, environment, database, ORC, sizing, execution,
BBRANGE, Learning Engine, runtime or VPS files are changed.

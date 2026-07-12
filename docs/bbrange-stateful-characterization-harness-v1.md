# BBRANGE Stateful Characterization & Parity Harness V1

## 1. Scope

Offline golden-master tests for `bot_bbrange.main.run_strategy`, its `execute_and_record` boundary and import safety. The only runtime change is lazy creation of the same process-wide exchange client; database, adapter policy, execution behavior, containers and services are unchanged.

## 2. Why BBRANGE

BBRANGE is the least complex representative pilot: one spot-long entry direction, explicit ENTRY/EXIT sections, 29 returns and six execution calls. It still covers regime, risk, sizing, PAPER lifecycle, LIVE suppression/failure/fill and existing-position exits.

## 3. Existing Lifecycle Map

```text
main_loop new closed candle
-> RUN_START
-> data/runtime snapshot + heartbeat
-> HALT/PANIC
-> existing-position exit management, or
-> entry gates -> signal -> sizing -> execute_and_record
-> RUN_END in finally
```

`NO_NEW_CANDLE` is handled in `main_loop` and does not call `run_strategy`. Every characterized `run_strategy` path returns legacy `None`.

## 4. Dependency Map

| Class | Dependencies | Harness treatment |
|---|---|---|
| Import-time state | ENV parsing and `RuntimeConfig.from_env`; exchange client is lazy | synthetic ENV; blocked-I/O isolated import |
| Easy monkeypatch | events, heartbeat, runtime snapshot, position lookup, regime, sizing, settings, win streak, dataframe read | recorder/stubs |
| Fake adapter | DB connections/cursors/commit/rollback; exchange access | strict fakes; unexpected exchange call fails |
| Time-sensitive | `datetime.now()` in profit-lock/time-exit age | fixed old entry time and deterministic decision stubs |
| Hidden globals | thresholds, disable hours, PAPER explore flags, cached lazy client | patched per test and restored by pytest |
| Side effects | strategy/entry events, simulated ledger, LIVE order, position open/close/attach, mode | captured in ordered recorder |

No sleep or main loop is invoked. No `.env` file is loaded.

## 5. Early-Return Inventory

All 29 source returns were classified. “Covered” may share a parameterized terminal category test.

| Line | Condition/category | Entry/exit | Baseline reason/effect | Status |
|---:|---|---|---|---|
| 1347 | no row | pre-entry | NO_ROW / SKIP | covered |
| 1353 | missing close | pre-entry | CANDLE_MISSING_CLOSE | classified with data-not-ready; not direct |
| 1363 | indicators missing | pre-entry | INDICATORS_NOT_READY | covered |
| 1409 | HALT | pre-entry | BOT_MODE_HALT | covered |
| 1426 | PANIC short in spot | exit safety | ERROR + HALT | impossible from valid LONG fixture |
| 1444 | PANIC completion | operational | no-position has no terminal reason | covered/risk |
| 1465 | existing short | exit safety | ERROR + HALT | impossible under spot invariant |
| 1499 | take profit | exit | execution + close TAKE_PROFIT | covered |
| 1526 | stop loss | exit | execution + close STOP_LOSS | covered |
| 1640 | profit lock | exit | exit signal/execution/close | covered |
| 1684 | time exit | exit | EXIT_TIME/execution/close | covered |
| 1687 | position hold | exit | POSITION_OPEN_NO_EXIT | covered |
| 1702 | disabled hour | entry | DISABLE_HOURS | covered |
| 1706 | bot disabled | entry | BOT_DISABLED | covered |
| 1763 | daily loss | entry | DAILY_MAX_LOSS_POSITIONS | covered |
| 1782 | insufficient candles | entry | NOT_ENOUGH_CANDLES | covered |
| 1794 | BB unavailable | entry | BB_NOT_READY | covered |
| 1807 | BB too narrow | entry | BB_WIDTH_TOO_LOW | covered |
| 1828 | trend rejected | entry | TREND_NOT_FLAT | covered |
| 1848 | no signal | entry | NO_SIGNAL | covered |
| 1859 | extreme RSI | entry | RSI_EXTREME_BLOCK | covered |
| 1868 | RSI maximum | entry | RSI_LONG_MAX_BLOCK | covered |
| 1879 | defensive SELL in spot | entry | SPOT_SHORT_BLOCK | unreachable: decision is hard-coded BUY |
| 1907 | regime blocked | entry | REGIME_BLOCK | covered |
| 1997 | zero sizing | entry | SIZING_QTY_ZERO | covered |
| 2014 | ledger/duplicate failure | entry | helper event; outer returns silently | covered/risk |
| 2019 | LIVE not attempted | entry | helper emits LIVE_ENTRY_NOT_ATTEMPTED | covered |
| 2030 | LIVE attempted, not filled | entry | LIVE_ENTRY_NOT_FILLED | covered |
| 2041 | successful completion | entry | POSITION_OPENED | covered PAPER/LIVE boundary |

Terminal categories: 25 directly reproduced, three structurally unreachable/invariant-only, one data-ready sibling classified but not directly reproduced. All 29 are accounted for; 0 unknown returns.

## 6. Entry/Exit Boundary

The harness separates scenarios by prepared `get_open_position()`. A position fixture enters exit management; `None` enters full entry evaluation. It never interprets POSITION_OPEN_NO_EXIT as a NO_TRADE entry result.

## 7. Test Architecture

`tests/bot_bbrange/fixtures.py` defines immutable recorded operations, observed behavior, deterministic candle/dataframe fixtures, fake DB and strict exchange. The test imports production source under a unique module name, verifies that the client cache is initially empty, then injects the factory result. Helper boundaries are monkeypatched only after import.

## 8. Fake DB

`FakeConnection`/`FakeCursor` record SQL, parameters, cursor lifecycle, commit and rollback. Most strategy tests mock higher-level helpers, which avoids pretending to parse PostgreSQL. The LIVE fill test uses fake connection lifecycle for order-ID attachment.

## 9. Fake Exchange

`StrictFakeExchange` raises on every unexpected attribute access. LIVE order outcomes are injected by monkeypatching the already imported `place_live_order` boundary. Therefore an accidental direct network/exchange call fails immediately.

## 10. Time Control

Candles use the fixed UTC time `2026-07-12T12:00:00+00:00`. Exit-age tests use a fixed older entry time and deterministic profit-lock result. Wall clock does not appear in golden assertions. Three suite runs produced identical test counts/results.

## 11. Scenario Matrix

37 tests cover the original 28 offline characterization scenarios plus exactly-once FinalDecision sink behavior for entry, zero-emission behavior outside the entry boundary and fail-open sink exception behavior.

## 12. Golden Behavior Model

`BbrangeObservedBehavior` freezes returned value, terminal reason, ordered event types/reasons and all recorded operations. `Recorder` retains operation order. Assertions include legacy return (`None`), event reason/type, execution count and position/simulation effects.

## 13. Characterized Risks

1. PANIC with no position emits RUN_START/RUN_END and changes mode to HALT but has no terminal strategy reason.
2. Duplicate ledger guard emits DB_GUARD_DUPLICATE inside `execute_and_record`; outer `run_strategy` returns silently.
3. A successful PAPER helper reports `live_ok=True`, despite no LIVE attempt; callers use this as generic success.
4. LIVE suppression writes `SIM_ORDER_CREATED` before `LIVE_ENTRY_NOT_ATTEMPTED`; this ledger artifact can make retry hit DB_GUARD_DUPLICATE.
5. POSITION_OPENED is emitted after PAPER success as well as LIVE success; its name does not encode environment.
6. NO_NEW_CANDLE is high-volume IDLE telemetry outside entry evaluation.

These are baselines, not fixes.

## 14. Uncovered Paths

Direct fixtures do not reproduce invalid SHORT positions in spot mode, the hard-coded-unreachable defensive SELL block, or the exact missing-close sibling of other data-not-ready paths. Maker/cancel exit internals are outside `run_strategy` helper boundary. These gaps are explicit and require no production seam for the FinalDecision entry refactor.

## 15. Import-Time Side Effect and Root Cause

The former module-level `client = get_market_data_client()` called the exchange-neutral factory while `bot_bbrange.main` was imported. With `EXCHANGE` absent, the shared factory defaults to `BINANCE`; `BinanceMarketDataAdapter.__init__` constructs `python-binance`'s `Client`, whose constructor performs an exchange `ping`. The exact chain was:

```text
import bot_bbrange.main
-> module-level get_market_data_client()
-> EXCHANGE default BINANCE
-> BinanceMarketDataAdapter()
-> binance.client.Client(...)
-> Client.ping()
-> HTTPS api.binance.com
```

The side effect was factory-wide at the BBRANGE call site: an explicitly configured OKX runtime would also have constructed its adapter during import, although the observed network request was Binance-specific because of the global factory fallback.

## 16. Lazy Client Model and Runtime Lifecycle

`get_exchange_client()` now owns a module-local `_exchange_client` cache. Import leaves the cache empty and makes zero factory calls. First runtime use calls the existing exchange-neutral `get_market_data_client()` exactly once; later uses return the same instance. `main_loop()` resolves the cached client before schema or DB setup, preserving the worker's former one-client lifecycle and initialization ordering once the process is actually started.

All former global-client consumers use the accessor: candle fetch, sizing, order placement and runtime trade ingestion. No adapter selection or execution-layer behavior changed. A factory exception remains unmasked, leaves the cache empty and occurs only on runtime use. The shared factory logs its selected adapter before construction, matching the existing error-observability model.

## 17. Import Safety Tests

`tests/bot_bbrange/test_import_safety.py` loads the production file under an isolated module name while blocking socket connects, `requests`, `urllib3`, `psycopg2.connect` and `time.sleep`. It proves import performs no network/DB connection or worker sleep and leaves the client cache empty. Lifecycle tests prove the `0 -> 1 -> 1` factory-call sequence and deferred, propagated initialization failure.

The canonical clean-process import is also validated from repository root:

```text
python3 -c "import bot_bbrange.main; print('IMPORT_OK')"
```

## 18. Known Global Exchange Fallback Risk

The shared `get_market_data_client()` still defaults to Binance when `EXCHANGE` is missing. This is a repository-wide policy outside this slice. BBRANGE import no longer activates that fallback; normal runtime configuration must still set the intended exchange before first client use.

## 19. Test Results

```text
pre-refactor characterization run 1: 28 passed in 0.62s
pre-refactor characterization run 2: 28 passed in 0.62s
pre-refactor characterization run 3: 28 passed in 0.60s
post-refactor characterization + sink: 36 passed
existing offline contract tests: 18 passed in 0.08s
import-safety/lifecycle/failure tests: 3 passed
current contract tests: 17 passed
current BBRANGE tests: 40 passed (37 parity + 3 import/client tests)
combined run 1: 57 passed in 0.78s
combined run 2: 57 passed in 0.71s
combined run 3: 57 passed in 0.72s
Learning Engine tests: 18 passed in 0.08s
canonical import: IMPORT_OK
blocked-I/O import: BLOCKED_IO_IMPORT_OK
py_compile: PASS
git diff --check: PASS
```

## 20. Coverage Results

- source returns: 29;
- classified returns: 29;
- directly reproduced returns/categories: 25;
- scenario tests: 37;
- characterized risks: 6;
- unknown paths: 0;
- explicitly uncovered/invariant paths: 4.

Percentage line coverage was not collected because `pytest-cov` is not a repository dependency and behavioral terminal coverage is the acceptance metric.

## 21. Readiness for FinalDecision Refactor

The harness establishes stable baselines for the entry/exit boundary and the critical PAPER/LIVE transitions. A future refactor can extract the entry section and compare event/order/position recorder sequences against these tests. The uncovered invalid-state paths should remain outer SYSTEM_NOT_EVALUATED/technical paths rather than entry decisions.

## 22. Explicit Non-Goals

No new EvaluationContext/FinalDecision behavior, canonical event/identity, DB ingestion, replay, strategy/execution/profit-lock/duplicate-guard fix, deployment, restart, migration, commit or push in the import-safety slice.

## Verdict

**READY TO RESUME COMMIT VALIDATION**

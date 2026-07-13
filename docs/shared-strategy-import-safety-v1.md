# Shared Strategy Import Safety V1

## 1. Problem Statement

`bot.main`, `bot_trend.main` and `bot_supertrend.main` each constructed an exchange client at module scope. A plain import therefore selected an adapter and could perform network I/O before a worker was started. BBRANGE had already established the safe reference pattern.

## 2. BBRANGE Reference Pattern

BBRANGE keeps an empty module-local `_exchange_client` cache and exposes `get_exchange_client()`. The existing exchange-neutral factory is called on first runtime use, its result is cached for the process, and initialization errors are logged and re-raised without populating the cache.

## 3. RSI Root Cause

The former initialization was `bot/main.py:132`:

```text
import bot.main
-> module-level get_market_data_client()
-> EXCHANGE (default BINANCE)
-> BinanceMarketDataAdapter()
-> binance.client.Client(...)
-> Client.ping()
-> HTTPS api.binance.com
```

The single global client was shared by bid/ask reads, maker/market exits, live preflight/order execution, klines, live sizing and trade ingestion.

## 4. TREND Root Cause

The former initialization was `bot_trend/main.py:93`, with the same factory and Binance fallback chain. The singleton was shared by order execution, sizing, klines and trade ingestion.

## 5. SUPERTREND Root Cause

The former initialization was `bot_supertrend/main.py:68`, with the same factory and Binance fallback chain. The singleton was shared by symbol-filter metadata, order execution, sizing, klines and trade ingestion.

Blocked-I/O tests found no additional import-time DB connection, scheduler, worker loop or sleep in these modules once client construction was removed. Environment and runtime configuration objects are still read at import, but they perform no external I/O.

## 6. Chosen Lazy Initialization Pattern

Each strategy now has a local `_exchange_client` cache and `get_exchange_client()` accessor. Import makes zero factory calls. The first runtime call creates one client through `common.exchange_client.get_market_data_client()`; later calls return the identical object. Factory exceptions are logged, propagated and leave the cache empty so a later call may retry.

## 7. Why a Shared Helper Was Not Used

Three local getters preserve the existing ownership boundary: one singleton per independently loaded worker module. A shared cache in `common/` would couple strategy modules loaded in the same process and broaden public runtime behavior. The small duplication is intentional and lower risk for this cleanup.

## 8. Runtime Client Lifecycle

Every `main_loop()` resolves `runtime_client = get_exchange_client()` before `ensure_schema()`, `upsert_defaults()` and the first DB connection. This preserves the former startup ordering, where module-level client construction completed before `main_loop()` began. Market-data, sizing, execution and ingestion continue to receive the same cached instance.

All direct global-client references were replaced by either `get_exchange_client()` at the existing call site or the `runtime_client` captured once at worker startup. The factory still receives no arguments and returns the same configured adapter type as before.

## 9. Factory Failure Semantics

Initialization failures occur only when runtime first requests the client. Each strategy logs a strategy-specific initialization error and re-raises the original exception. No sentinel or partially initialized value enters the cache. A subsequent request retries the unchanged factory.

## 10. Import Safety Tests

`tests/test_shared_strategy_import_safety.py` imports each production file under a fresh isolated module name. During import it blocks:

- `socket.socket.connect`;
- `socket.create_connection`;
- `requests.Session.request`;
- `urllib3` connection;
- `psycopg2.connect`;
- `time.sleep`.

The suite verifies zero I/O, zero factory calls, an empty cache after import, one cached factory result, failure logging/propagation/retry, and client-before-schema startup order.

## 11. Runtime Compatibility

No signal, threshold, gate, bot-control, sizing formula, execution helper, exit, DB query or public strategy function signature changed. The same exchange-neutral factory and same client object serve both market-data and execution paths. BBRANGE is unchanged.

## 12. Global EXCHANGE Fallback Risk

`get_market_data_client()` still defaults to Binance when `EXCHANGE` is absent. This cleanup prevents imports from activating that fallback but deliberately does not change repository-wide adapter policy. Runtime deployments must continue to provide the intended exchange configuration.

## 13. Test Results

```text
canonical RSI import: RSI_IMPORT_OK
canonical TREND import: TREND_IMPORT_OK
canonical SUPERTREND import: SUPERTREND_IMPORT_OK
shared import/lifecycle run 1: 12 passed in 0.69s
shared import/lifecycle run 2: 12 passed in 0.57s
shared import/lifecycle run 3: 12 passed in 0.59s
BBRANGE + Decision Contract regression: 57 passed in 0.76s
Learning Engine regression: 18 passed in 0.08s
py_compile: PASS
git diff --check: PASS
```

## 14. Rollout Plan

After code review and separate authorization, rebuild and recreate only the shared `bot-runner` service in each environment, LOCAL LIVE first and LOCAL PAPER only after LIVE parity passes. Confirm configured adapter selection, one successful client initialization per worker process before schema setup, normal heartbeat/candle ingestion and unchanged order/DB telemetry for all strategies. BBRANGE code is unchanged but must pass runtime smoke checks because it shares the image.

## 15. Rollback Plan

Revert this isolated commit and redeploy the previous worker image/configuration. No schema, migration, persisted contract or data rollback is required. Do not replace the lazy accessor with an import-time client as an ad-hoc production edit.

## 16. Explicit Non-Goals

This change does not add FinalDecision to RSI/TREND/SUPERTREND, build characterization harnesses, persist NO_TRADE decisions, alter Decision Registry ingestion, change the global exchange fallback, tune ORC/Learning/strategies, modify execution, or deploy/restart services.

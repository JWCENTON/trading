# Strategy Startup Schema Convoy Removal V1

## 1. Incident Summary

Recreating the shared `bot-runner` starts 28 LIVE strategy processes concurrently. Each of the four strategy entrypoints called `common.schema.ensure_schema()` before its loop. Heartbeats and RUN_END events were delayed for minutes even though processes remained alive.

## 2. Confirmed Root Cause

Every child took session advisory lock `987654321` and ran a full idempotent DDL catalog. The first session waited for an `ACCESS EXCLUSIVE` relation lock while a long ORC reader held its transaction. Other schema sessions queued on the advisory lock, and ordinary `positions` reads queued behind the waiting DDL. PostgreSQL showed 25 sessions blocked by `ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_order_id text`, 22 relation waits and four advisory waits.

`IF NOT EXISTS` avoids duplicate objects but does not avoid the relation lock. The session lock was explicitly released in `finally`; a broken connection would also release it. The convoy was reproduced on the clean rollback SHA, so the import-safety patch was not its cause.

## 3. Current DDL Startup Map

Before this change all four paths were identical:

```text
process import and exchange client initialization
-> main_loop
-> ensure_schema
-> session advisory lock 987654321
-> CREATE/ALTER/CREATE INDEX catalog
-> upsert bot defaults and seed params
-> heartbeat and strategy loop
```

Calls existed in `bot/main.py`, `bot_bbrange/main.py`, `bot_trend/main.py` and `bot_supertrend/main.py`. Additional runtime DDL existed in `common.user_settings` and the first `common.worker_heartbeat` write.

## 4. Four-Strategy Schema Requirements

The shared required schema covers candles and indicators, positions/order identifiers, simulated orders, strategy parameters/history, strategy and worker heartbeats, bot control, market regime and gate events, panic state, user settings, UI audit and watchdog events. LIVE and PAPER have every required table, column and critical index. They contain harmless additional columns and indexes from later feature migrations.

## 5. Schema SSOT Decision

`db/migrations` is now the sole DDL SSOT. `common.schema.ensure_schema()` remains only as a read-only compatibility wrapper and cannot repair schema. Runtime code fails fast with an instruction to apply migrations.

## 6. Migration Coverage

`20260712_strategy_runtime_schema_consolidation_v1.sql` consolidates the former bootstrap catalog and the user-settings/worker-heartbeat additions. It is transactional and takes a transaction-scoped migration advisory lock. Catalog checks guard every DDL statement, so an already migrated database executes no table/index DDL. It performs no drops, data rewrites, triggers or order-path changes.

The migration inserts bootstrap singleton rows idempotently and records `strategy_runtime_schema_version` in existing `automation_kv` when available.

## 7. Runtime Readiness Validation

`common.schema_readiness.validate_strategy_runtime_schema()` uses two read-only catalog SELECTs to verify required columns and critical indexes. The supervisor calls it once after connecting and before reading bot_control or spawning children. Failure is logged, written best-effort to the existing worker heartbeat table and re-raised. No child starts on an incomplete schema.

## 8. Advisory Lock Removal

Normal strategy workers no longer import or call `ensure_schema` and cannot take lock `987654321`. The lock remains only in the controlled migration as `pg_advisory_xact_lock`, which is automatically released at transaction end. Heartbeat write serialization lock `917263002` is unrelated to DDL and remains for deadlock avoidance.

## 9. ORC Reader Finding

The direct DDL blocker was a regular ORC readiness SELECT over strategy/market-memory views. It ran inside a transaction long enough to block the `positions` DDL. The observed ORC session was active, not idle-in-transaction. Separate idle-in-transaction sessions were visible and should be audited independently, but ORC logic is outside this patch. Removing runtime DDL eliminates the harmful reader/DDL interaction.

## 10. Test Architecture

Static AST tests prove none of the four strategies imports or calls `ensure_schema`. SQL recorder tests reject ALTER, CREATE, DROP, TRUNCATE, REINDEX and VACUUM in runtime paths. Tests prove readiness is SELECT-only, missing schema fails fast, supervisor readiness precedes config/child startup, and user-settings/worker-heartbeat runtime paths contain no schema DDL.

Existing import-safety, BBRANGE characterization/FinalDecision and Learning Engine regressions remain mandatory. Schema startup tests run three times.

## 11. Immutable Image Tagging

Before recreation, the running image receives an immutable `previous-<timestamp>-<sha>` tag and the candidate receives a `candidate-<full-git-sha>` tag. `latest` may still be used by Compose, but rollback retags the preserved previous image and recreates without rebuild.

## 12. LOCAL LIVE Rollout

Record all 28 enabled slots, locks, positions, ORC/Learning and service IDs. Apply the migration twice, run offline tests, preserve the previous image, build/tag the candidate and recreate only `bot-runner`. Require 28/28 fresh enabled heartbeats, 1m and 5m cycles, no strategy DDL/advisory convoy, no restarts and no severe logs.

The controlled candidate rollout proved readiness ran once before child startup and produced zero strategy DDL, zero schema advisory/relation waits and immediate TREND 1m cycles. It reached 25/28 fresh heartbeats; the three missing SUPERTREND 1m heartbeats were already stale in the 25/28 pre-state. Because the acceptance contract nevertheless required 28/28, LIVE was rolled back with the immutable previous image and PAPER was not started.

## 13. LOCAL PAPER Rollout

Proceed only after LIVE passes. Repeat migration twice and immutable tagging, then recreate only PAPER `bot-runner`. Require supervisor plus 32 children, preserved positions/simulated orders, fresh 32/32 heartbeats and no schema locks.

Not executed: LIVE did not meet the independent 28/28 heartbeat gate.

## 14. DB Lock Evidence

Pre-change evidence included `ALTER TABLE positions`, advisory lock queues and relation waits. Post-rollout queries must show zero strategy DDL, zero lock `987654321` from workers and zero relation waits caused by schema readiness.

## 15. Rollback Procedure

Retag the immutable previous image as the Compose service image and run `up -d --no-deps --force-recreate bot-runner`. Do not rebuild, migrate backward or modify data. The migration is additive and needs no DB rollback.

## 16. Known Gaps

The repository has no general migration ledger/runner; controlled `psql -v ON_ERROR_STOP=1` remains the migration mechanism. Long ORC queries and unrelated idle-in-transaction sessions need separate lifecycle audits. A permanent Compose release-tag mechanism is future infrastructure work.

## 17. Explicit Non-Goals

No signal, threshold, gate, execution, position lifecycle, ORC, Learning, FinalDecision, exchange fallback, Compose, Dockerfile or VPS behavior is changed.

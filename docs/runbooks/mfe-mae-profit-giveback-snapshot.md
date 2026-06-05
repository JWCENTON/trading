# MFE / MAE / Profit Giveback Snapshot

Status: read-only analytics layer. This version replaces heavy live MFE views with a snapshot table and refresh function.

## What it creates

```text
trade_mfe_mae_snapshot
refresh_trade_mfe_mae_snapshot(days_back integer)
v_trade_mfe_mae
v_trade_mfe_mae_strategy_7d
v_trade_mfe_mae_exit_reason_14d
v_profit_lock_giveback_14d
```

The bot, ORC, runtime params, `bot_control` and `positions` write path are not changed.

## Apply order

```text
1. LOCAL LIVE
2. LOCAL PAPER
3. VPS LIVE
4. VPS PAPER
```

## Apply migration

LIVE:

```bash
docker compose -p trading-live --env-file .env.live \
  -f docker-compose.yaml -f docker-compose.live.override.yaml \
  exec -T db sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < db/migrations/20260605_mfe_mae_profit_giveback_snapshot.sql
```

PAPER:

```bash
docker compose -p trading-paper --env-file .env.paper \
  -f docker-compose.yaml -f docker-compose.paper.override.yaml \
  --profile legacy-paper-ui \
  exec -T db sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < db/migrations/20260605_mfe_mae_profit_giveback_snapshot.sql
```

## Refresh snapshot

Start with 7 days. If that is fast, use 14 days.

LIVE:

```bash
time docker compose -p trading-live --env-file .env.live \
  -f docker-compose.yaml -f docker-compose.live.override.yaml \
  exec -T db sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT * FROM refresh_trade_mfe_mae_snapshot(7);
"'
```

PAPER:

```bash
time docker compose -p trading-paper --env-file .env.paper \
  -f docker-compose.yaml -f docker-compose.paper.override.yaml \
  --profile legacy-paper-ui \
  exec -T db sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT * FROM refresh_trade_mfe_mae_snapshot(7);
"'
```

## Fast validation

```sql
SELECT COUNT(*), MIN(exit_time), MAX(exit_time)
FROM trade_mfe_mae_snapshot;

SELECT *
FROM v_trade_mfe_mae_strategy_7d
ORDER BY net ASC;

SELECT *
FROM v_trade_mfe_mae_exit_reason_14d
ORDER BY net ASC
LIMIT 20;

SELECT id, strategy, symbol, interval, exit_time, mfe_pct, mae_pct, exit_pct, giveback_pct, net_pnl_usdc
FROM v_trade_mfe_mae
ORDER BY exit_time DESC
LIMIT 10;
```

## Operational note

Refreshing the snapshot can be slower than reading the views, but it is controlled and explicit. The views should be fast because they read `trade_mfe_mae_snapshot`, not `positions × candles` live.

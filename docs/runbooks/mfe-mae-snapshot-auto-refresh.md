# MFE / MAE snapshot auto-refresh

This runbook covers the automation-runner refresh for `trade_mfe_mae_snapshot`.

## Purpose

`trade_mfe_mae_snapshot` stores precomputed MFE, MAE and profit giveback analytics so UI/SQL reports do not execute the heavy `positions × candles` join on every read.

The automation-runner periodically executes:

```sql
SELECT * FROM refresh_trade_mfe_mae_snapshot(30);
```

This is analytics-only. It does not update trading controls, ORC picks, orders, positions, panic state or risk settings.

## Environment variables

```text
MFE_MAE_SNAPSHOT_REFRESH_ENABLED=1
MFE_MAE_SNAPSHOT_REFRESH_INTERVAL_SECONDS=300
MFE_MAE_SNAPSHOT_DAYS_BACK=30
```

Defaults are safe if the variables are not set.

## Validation

```sql
SELECT value
FROM automation_kv
WHERE key IN (
  'mfe_mae_snapshot_refresh_last_status',
  'mfe_mae_snapshot_refresh_last_stats_json'
);

SELECT COUNT(*) FROM trade_mfe_mae_snapshot;
SELECT * FROM v_trade_mfe_mae_strategy_7d LIMIT 20;
```

## Disable

Set:

```text
MFE_MAE_SNAPSHOT_REFRESH_ENABLED=0
```

and restart automation-runner.

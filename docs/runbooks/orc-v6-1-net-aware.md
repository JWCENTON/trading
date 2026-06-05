# ORC V6.1 Net-Aware Apply

ORC V6.1 changes `v_slot_profitability_3d_v5` and `v_orc_picks_v5`.

Rules:

- use `net_pnl_usdc`
- require `net_sum_3d > 0`
- require `profit_factor_3d >= 1.05`
- require `n_trades_3d >= 5`
- remove lossy softfill/bootstrap picks
- max one strategy per symbol+interval
- dynamic max_picks if available, fallback to 8

Apply order:

1. local LIVE
2. local PAPER
3. VPS LIVE
4. VPS PAPER

Validation:

```sql
SELECT *
FROM v_orc_picks_v5
ORDER BY final_rn;

SELECT
  COUNT(*) FILTER (WHERE live_orders_enabled=true) AS live_entries_on,
  COUNT(*) FILTER (WHERE live_orders_enabled=false) AS live_entries_off
FROM bot_control;

SELECT symbol, interval, strategy, live_orders_enabled, reason
FROM bot_control
WHERE live_orders_enabled=true
ORDER BY symbol, interval, strategy;

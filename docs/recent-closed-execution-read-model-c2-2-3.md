# Recent closed-position execution read model (C2.2.3)

The `/ui/recent-closed` read model resolves `entry_notional_usdc` from the
first non-zero source in this order:

1. authoritative LIVE real-execution notional;
2. authoritative PAPER simulated ENTRY fills;
3. the existing estimated entry notional;
4. legacy `entry_price * qty`;
5. `NULL` when no denominator evidence exists.

PAPER evidence is aggregated from `simulated_execution_fills_v1` only for the
already limited set of recent closed positions. ENTRY and EXIT notionals use
`SUM(fill_qty * fill_price)` and the corresponding `order_purpose`. Real
execution evidence remains higher priority, so simulated evidence cannot
replace an available LIVE denominator.

`pnl_pct` is the net PnL numerator divided by the resolved gross executed entry
notional, multiplied by 100. Entry fees are not deducted from the denominator:
the net PnL already contains the applicable cost model. A missing or zero
denominator produces `NULL`, never an invented zero percent.

The response contract is unchanged. In particular, `entry_notional_usdc`,
`pnl_usdc`, and `pnl_pct` retain their existing field names; the patch only
populates values that were previously absent for PAPER C2.2 closed positions.

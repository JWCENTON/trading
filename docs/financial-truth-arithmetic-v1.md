# Financial Truth arithmetic V1

`FINANCIAL_TRUTH_ARITHMETIC_V1` is the single arithmetic contract used by the
canonical Financial Truth calculator and bounded legacy repair writers.
`FINANCIAL_TRUTH_DECIMAL_PRECISION_V1` fixes the Python implementation to a
local Decimal context with precision 120 and `ROUND_HALF_UP`; ambient process
Decimal settings never participate.

Authoritative inputs are exchange-confirmed entry and exit fills: quantity,
price, stored quote notional, fee quantity, fee asset, and authoritative fee
valuation in quote currency. No binary floating-point value participates.

The formulas are:

```text
gross_entry = sum(entry quantity)
entry_base_fee = sum(entry fees charged in base)
net_entry = gross_entry - entry_base_fee
gross_exit = sum(exit quantity)
exit_base_fee = sum(exit fees charged in base)
net_exit_reduction = gross_exit + exit_base_fee
raw_remaining = net_entry - net_exit_reduction

gross_ratio = min(gross_exit / gross_entry, 1)
inventory_ratio = min(net_exit_reduction / net_entry, 1)
gross_pnl = exit_notional - entry_notional * gross_ratio
fees = entry_fees + exit_fees
net_pnl = gross_pnl - entry_fees * inventory_ratio - exit_fees
```

Both allocation ratios have an explicit scale of 20 decimal places with
`ROUND_HALF_UP`. This is the versioned PostgreSQL `NUMERIC` compatibility
boundary exposed by the original authoritative validation vector. All other
operations run at precision 120 without intermediate quantization.

Canonical Financial Truth persistence columns are unconstrained `NUMERIC`, so
V1 applies no final persistence quantization. The legacy `positions` outcome
columns remain `NUMERIC(18,8)` compatibility fields and are not Canonical FT
SSOT. Reporting views may round for presentation only.

Terminal dust is preserved as remaining inventory and does not add synthetic
proceeds or a synthetic dust valuation. It changes terminal classification,
not exchange-derived cash flow. A future change to context precision, ratio
scale, rounding, dust valuation, or persistence quantization requires new
arithmetic and precision contract versions.

# Recent Closed Positions Division Safety V1

## Incident

The issue was revealed on VPS LIVE by `GET /ui/recent-closed?limit=10`.
The database contained recent CLOSED positions, but the endpoint returned an
HTTP 200 legacy error envelope with an empty `items` array after PostgreSQL
raised `DivisionByZero`.

The reproducing position semantics were:

```text
status=CLOSED
qty=0
entry_price>0
real.entry_exec_notional_est≈20
exit_time IS NOT NULL
```

No production row is copied into the regression fixture.

## Root cause

The guard and division used different `COALESCE` precedence. The guard selected
real execution notional before the historical `entry_price × qty` fallback,
while the division selected the historical expression first. With `qty=0`,
that expression evaluated to zero and became the divisor even though real SSOT
notional was available.

## Canonical denominator and SSOT precedence

The endpoint now calculates one value:

```sql
NULLIF(
    COALESCE(
        real.entry_exec_notional_est,
        est.entry_notional_usdc,
        p.entry_price * p.qty
    ),
    0
) AS entry_notional_safe
```

Precedence is:

1. real execution SSOT;
2. estimated entry notional;
3. historical `entry_price × qty`;
4. missing or zero value becomes `NULL`.

Both the displayed entry notional and percentage calculation use this value.

## NULL and zero behavior

When no non-zero denominator is available, the CLOSED record remains in the
response and dependent percentage fields are `NULL`. The endpoint does not
replace unknown values with `1` or present them as `0%`.

## Error contract

Unexpected backend failures now produce a controlled HTTP 500 with a generic
message. Raw SQL error text is logged server-side and is not returned to the
client.

The frontend client also rejects the historical HTTP 200 envelope whenever it
contains `error` or `error_type`. The dashboard renders its normal API error
state instead of a valid-looking empty Recent Closed table.

## Regression test matrix

- standard non-zero quantity and real notional;
- VPS LIVE reproduction: zero quantity with positive real SSOT notional;
- estimated-notional fallback;
- historical price-times-quantity fallback;
- no safe denominator returns a record with a NULL percentage;
- precedence selects real SSOT before a zero historical expression;
- backend controlled HTTP 500 contract;
- frontend rejection of the legacy error envelope.

Tests use source-contract assertions and synthetic fixtures. They do not write
to a real database.

## GET write-side-effect finding

The endpoint still performs:

```text
GET /ui/recent-closed
→ create_trade_position_notifications()
→ commit
```

This is intentionally not refactored in this patch. It is a separate follow-up:

```text
Recent Closed Notifications Read/Write Separation V1
```

## Rollout plan

1. LOCAL LIVE: restart API and frontend, then validate CLOSED rows and UI.
2. LOCAL PAPER: restart API and frontend and repeat isolation checks.
3. Commit and push from LOCAL after both validations.
4. VPS LIVE: pull-only deployment, restart API and frontend only.
5. VPS PAPER: deploy the same reviewed commit.

Do not restart trading workers, control-plane services, or PostgreSQL.

Post-rollout validation:

- the database contains CLOSED positions;
- the API returns non-empty `items` where rows exist;
- no `error` or `error_type` envelope is present;
- Last Closed Positions renders in the UI;
- no API traceback or database regression occurs.

## Rollback plan

Restore the previous API and frontend images and recreate only those services.
Do not modify positions, backfill quantity, change SSOT, or roll back database
schema. Preserve API logs and the failing response evidence for diagnosis.

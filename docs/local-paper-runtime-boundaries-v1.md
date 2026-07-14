# LOCAL PAPER runtime boundaries v1

## Database read lifecycle

Strategy history reads must not hold a PostgreSQL transaction while pandas
computes indicators. A read-only strategy connection is rolled back and closed
immediately after the dataframe has been loaded. Indicator persistence uses a
separate writer connection and retains the existing transactional behavior.

The invariant applies to RSI, BBRANGE, TREND, and SUPERTREND:

- normal read: rollback and close;
- early return: rollback and close;
- read exception: rollback and close;
- repeated cycles: no read transaction survives the load phase.

## PAPER-to-LIVE promotions

The promotions publisher is an existing, intentional cross-environment
control-plane flow. When `PROMOTIONS_ENABLED=1`, PAPER automation reads PAPER
ranking data and publishes it to `LIVE_API_BASE`, normally the `live-api` alias
on the shared `trading-edge` network.

The LIVE endpoint writes only promotion control-plane tables:

- `promoted_candidates` or `promoted_regime_candidates`;
- `promotion_events` for idempotency and audit.

It does not write positions, orders, or fills and does not submit an exchange
order. Promotion rows are, however, an input to the LIVE orchestrator, so they
can influence later slot eligibility through the established control-plane.
The flow therefore does not mean that the LIVE data plane is completely
unmodified: promotion control-plane tables may change, while the rollout gate
requires no corresponding changes in `positions`, orders, or fills.

For a LOCAL PAPER code-only rollout, "LOCAL LIVE untouched" therefore means:

- LIVE containers, images, and restart counts are unchanged;
- no direct changes to trading positions, orders, or fills are caused by the
  rollout;
- the existing, authenticated promotions upsert to the LIVE control-plane is
  allowed and remains auditable.

## Exchange-call classification

PAPER strategies may use public market-data endpoints required for simulation,
including klines, ticker data, and order-book data. PAPER must not use private
account endpoints, submit or cancel real orders, or mutate real balances and
fills.

Runtime reporting must distinguish public market-data reads from private or
order-mutating exchange calls; a target of zero public calls would prevent the
simulation from operating.

## LOCAL PAPER definition of done

Public market-data calls remain allowed. A completed LOCAL PAPER runtime gate
requires:

- 0 private exchange calls;
- 0 real order submissions;
- 0 real cancel operations;
- 0 real account or balance mutations;
- 0 pending-entry reconciliation DB writes;
- 0 idle-in-transaction sessions after completed strategy cycles;
- 0 waiting locks;
- 32/32 strategy workers fresh.

## Disposable PostgreSQL rollout gate

The optional integration gate must use only an explicitly configured
`WALTRADE_TEST_PG_DSN` whose database name ends in `_test` and contains the
operator-created `waltrade_disposable_test_db=true` marker. With a unique
`application_name`, it should materialize a pandas SELECT through
`read_only_db_conn()`, observe the session from a second autocommit connection,
and confirm after context exit that no helper session, idle transaction, or
transaction-level lock remains. An attempted DML statement must fail with a
read-only transaction violation. The gate must be skipped when the disposable
DSN is absent and must never target LOCAL LIVE or LOCAL PAPER databases.

## Dump and repository policy

- LOCAL LIVE: create a dump before a migration or risky rollout; remove it only
  after PASS and closure of the rollback window.
- LOCAL PAPER: remove a temporary dump after PASS. A dump from a safely blocked
  rollout that made no mutations may be removed immediately.
- VPS LIVE: do not create a rollout dump because of storage constraints.
- VPS PAPER: do not create a dump.
- VPS repositories are pull-only and must never push.

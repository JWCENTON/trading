# Bot-Runner Bounded Startup Stagger V1

## 1. Incident Summary

After startup schema convoy removal, VPS workers reached OKX almost
simultaneously. The supervisor remained healthy, but the rollout gate stopped
on a concentrated history-candles request burst and HTTP 429 responses.

## 2. Confirmed VPS Evidence

The first ten seconds contained 14 requests to
`/api/v5/market/history-candles`, with a maximum of nine requests in one second
and eight HTTP 429 responses. RSI, BBRANGE, TREND and SUPERTREND were affected.

## 3. Why LOCAL Did Not Reproduce

LOCAL uses a different exchange path and rate-limit context. Its prior rollout
showed the same launch concentration—28 children in about 1.24 seconds and 32
in about 0.91 seconds—but did not receive 429 responses. Freshness alone could
therefore not validate a safe VPS startup request profile.

## 4. Existing Spawn Model

`bot_control` was queried without `ORDER BY`, inserted into a dictionary and
iterated in database-return order. Every enabled missing worker was passed to
`Popen` in the same reconcile loop. Crash restarts used a separate immediate
path with a per-worker backoff.

## 5. Deterministic Worker Ordering

Workers are ordered by explicit strategy rank (`RSI`, `BBRANGE`, `TREND`,
`SUPERTREND`), then symbol and interval. SQL uses the same ordering, while the
supervisor sorts independently as a safety boundary. Grouping by strategy
separates workers sharing the same symbol and interval instead of launching all
four strategies for one market slot consecutively.

## 6. Bounded Stagger

`BOT_RUNNER_STARTUP_STAGGER_SECONDS` controls a deterministic delay between
consecutive attempts in a multi-worker batch. The delay is central to
bot-runner, is not random, can be set to zero, and is never applied after the
last worker. Worker commands and environments are unchanged.

## 7. Selected Interval

The safe default is 1.5 seconds. It limits the deterministic model to at most
one spawn in any integer-second bucket while retaining a startup measured in
seconds rather than minutes. No `.env` change is required; an optional override
remains available for controlled testing.

## 8. Startup Duration

The expected stagger component is 40.5 seconds for 28 workers and 46.5 seconds
for 32 workers, plus process creation overhead. This remains well below the
strategy stale threshold and normal SUPERTREND long-cycle duration.

## 9. Reconcile and Restart Semantics

The existing crash-restart path remains immediate after its backoff. A single
newly enabled or missing worker has no delay. Multiple missing workers in one
reconcile use the same bounded batch behavior to avoid recreating a request
burst. Disable and stop behavior is unchanged.

## 10. Shutdown Handling

Stagger waits poll shutdown state at intervals no longer than 100 ms. SIGTERM
or SIGINT prevents further child starts and transfers control to the existing
termination path without waiting for the full configured stagger.

## 11. Observability

Logs record batch size, configured stagger, sequence number, worker identity
and spawn timestamp, followed by requested/started totals. This supports exact
counts for the first 5, 10 and 30 seconds, per-second maxima and total startup
duration without exposing environment secrets.

## 12. Test Matrix

Offline tests cover stable ordering, between-start waits, no trailing wait,
zero stagger, shutdown interruption, disabled/running exclusion, immediate
single-worker restart, unchanged command/environment and a 28-worker burst
schedule. Tests use fake clocks, waits and processes only.

## 13. LOCAL Rollout Plan

Build and tag immutable previous/candidate images, recreate only LOCAL LIVE
bot-runner, preserve external logs and measure the full spawn timeline. Require
15 minutes of 28/28 freshness, zero 429/severe/DDL/lock/restart events and valid
SUPERTREND progress. Proceed to PAPER only after LIVE passes, then require 20
minutes at 32/32 with identical checks and state parity.

## 14. VPS Rollout Plan

VPS may only fetch, pull, test and deploy the committed candidate. Capture
external logs and inspect data before recreate, verify the measured spawn
distribution and stop immediately on any 429 or freshness regression.

## 15. Rollback

Retag the recorded immutable previous image and force-recreate only the affected
bot-runner. No database, schema, strategy, ORC or Learning rollback is involved.

## 16. Explicit Non-Goals

V1 does not change strategies, signals, thresholds, exchange endpoints,
adapter retries, order sizing, execution, positions, exits, regime, ORC,
Learning, FinalDecision, stale thresholds, schema, market-data centralization or
worker-level sleeps/jitter.

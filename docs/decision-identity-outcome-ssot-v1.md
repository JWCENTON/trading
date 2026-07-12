# Decision Identity & Outcome SSOT Foundation V1

## 1. Problem Statement

Legacy `decision_key` is derived from `symbol|interval|strategy|entry_time` inside the shadow-recommendation branch. It is not a universal decision identity, does not cover NO TRADE, has no LOCAL/VPS provenance and makes replay downstream of recommendations. V1 establishes a neutral registry and actual-trade outcome store without changing trading.

## 2. Architecture Decision

The dependency is `source decision -> decision_registry_v1 -> decision_outcomes_v1 -> future recommendation evaluation`. Neither table requires a recommendation, replay row or warehouse row. Existing learning/replay objects remain unchanged.

V1 ingestion is deliberately limited to `TRADE_EXECUTED` from `positions.id`. This source is durable and one row represents one executed position. NO TRADE is not ingested yet because `strategy_events` and entry traces contain durable row IDs but can contain multiple telemetry events per strategy cycle; no proven one-event-per-decision contract exists.

## 3. Universal Decision Identity

`decision_id` is RFC 4122 UUIDv5 produced by `waltrade_uuid_v5_v1`. The fixed WalTrade decision namespace is `c966214a-6a82-50e9-913b-5144237cdf44`.

Canonical V1 payload:

```text
deployment_id|environment|positions|position_id|TRADE_EXECUTED
```

The payload excludes mutable descriptions, PnL, status, timestamp and recommendation data. The same source decision therefore retains its UUID across refreshes; LOCAL/VPS and LIVE/PAPER necessarily differ.

## 4. Decision Types

The schema accepts `TRADE_EXECUTED`, `NO_TRADE`, `SIGNAL_REJECTED`, `ENTRY_BLOCKED`, `ENTRY_SUPPRESSED`, and `PAPER_SIMULATION`. Only the first is ingested in V1. `position_id` is required only for `TRADE_EXECUTED`.

## 5. Deployment and Environment Provenance

Rows store `deployment_id`, database environment, engine/schema versions, source table/record/time, observation/ingestion times and run ID. Allowed deployments are `LOCAL`, `VPS`, `UNKNOWN`; UNKNOWN remains auditable and is never production-ready. Refresh rejects an environment different from `current_database()` and never infers deployment from hostname. LOCAL rollout passes `LOCAL` explicitly. A future scheduled caller must obtain `WALTRADE_DEPLOYMENT_ID` explicitly and reject a missing/UNKNOWN production value; V1 does not modify env or runner files.

## 6. Source Identity Rules

| Decision class | Canonical source natural key | V1 status |
|---|---|---|
| TRADE_EXECUTED | deployment + environment + `positions.id` + type | implemented |
| NO_TRADE | deployment + environment + future canonical decision-event ID + type | blocked pending event-grain contract |
| SIGNAL_REJECTED | same future decision-event contract | schema-ready only |
| ENTRY_BLOCKED | same future decision-event contract | schema-ready only |
| ENTRY_SUPPRESSED | same future decision-event contract | schema-ready only |
| PAPER_SIMULATION | deployment + trading_paper + durable simulated-order/event ID | schema-ready only |

The unique source identity is `(deployment_id, environment, source_table, source_record_id, decision_type)`. A partial unique index also prevents multiple executed decisions for one position in one deployment/environment.

## 7. Decision Registry Schema

`decision_registry_v1` stores the UUID, optional legacy key, provenance, slot, decision metadata, immutable source identity, JSON evidence reference, optional position/recommendation IDs and run audit link. Descriptive fields are refreshed; identity fields and `decision_id` are not changed by conflict updates.

## 8. Outcome SSOT Schema

`decision_outcomes_v1` represents one decision/outcome type/horizon. V1 creates only `ACTUAL_TRADE` for closed positions. Its deterministic UUIDv5 uses a separate outcome namespace and payload `decision_id|ACTUAL_TRADE|positions|position_id`. PnL and fees come from `positions`; MFE/MAE/giveback are optional enrichment from `exit_trace_v1`. No recommendation data is read. Forward outcomes are schema-ready but not fabricated.

## 9. Idempotency Model

Source and outcome unique constraints plus deterministic UUIDv5 prevent duplicates. Re-running refresh updates descriptions/outcomes and run provenance without changing identity. A transaction-scoped advisory lock serializes each deployment/environment. Every invocation uses a distinct audit `run_id`.

## 10. Backfill Model

`refresh_decision_identity_outcome_v1(lookback_hours, environment, deployment_id, run_id)` reads positions in the requested entry-time window, upserts registry rows, then upserts closed-position outcomes. It returns JSON stats, makes no internal commit, and writes only the three new tables. Invalid provenance fails before ingestion. A processing error is returned as explicit `FAILED` and persisted in run audit; errors before run creation are raised.

## 11. Run Audit

`decision_identity_runs_v1` records RUNNING, OK, FAILED or SKIPPED_LOCKED, counts, error text and result JSON. It is append-only by run ID. The function never silently ignores an error.

## 12. Compatibility with Legacy `decision_key`

Legacy keys remain references, not identity. A registry row receives a legacy key only when its `position_id` maps to exactly one distinct warehouse key in the same environment. Zero or multiple keys produce NULL rather than a false 1:1 claim. Existing tables and refresh functions are untouched.

## 13. Relationship to Replay

The new registry and outcome tables do not consume `decision_replay_v1`. Actual outcome is sourced from the accounting position and optional exit-path telemetry. Future replay must reference `decision_id` and produce a versioned horizon outcome; it must not be required for decision existence.

## 14. Relationship to Recommendations

Recommendations are future consumers of decisions/outcomes. `recommendation_id` is nullable compatibility metadata and is not populated in V1. No recommendation table participates in UUID identity or actual outcome creation.

## 15. Safety Model

- One migration creates only new objects and a pure UUID helper.
- No triggers and no runtime-path changes.
- No writes to positions, strategy events, traces, replay, warehouse, Learning, ORC or bot control.
- Database/environment mismatch is rejected.
- Constraints enforce source presence, supported decision/outcome types, actual-trade position presence and FK integrity.
- Diagnostic views expose duplicates, missing outcomes and provenance violations.

## 16. Known Gaps

NO TRADE/event-grain identity, forward horizons, simulated-order identity, account identity, scheduled runner integration and VPS provenance are not implemented. `position_id` is database-local and safe only because deployment/environment are in identity. Legacy key coverage is expected to be partial. `exit_trace_v1` may not cover every historical position.

## 17. NO TRADE Ingestion Plan

Define one canonical strategy-decision event emitted once per evaluated opportunity, with durable ID, decision timestamp, action/type, reason, engine version and links to related telemetry rows. Map auxiliary strategy/entry events to that parent. Only then ingest NO_TRADE and rejection/block/suppression types using the parent ID; never infer one decision by grouping timestamps or candles.

## 18. Rollback Procedure

Before commit/rollout, rollback is database-local and requires explicit authorization: stop callers, verify no consumers, then drop only the four views, refresh/helper functions and three V1 tables in dependency order. The migration does not provide automatic destructive rollback. Existing runtime/learning data require no restoration because they are never modified.

## 19. Validation Plan for Four Environments

Roll out in order LOCAL LIVE, LOCAL PAPER, commit/push, VPS LIVE, VPS PAPER. At every stage record Git SHA, database name, deployment ID, schema/function hashes, pre/post runtime counts, two migration runs, two refresh runs, deterministic sample UUIDs, duplicates, FK/orphans, provenance audit, legacy mapping and Learning V1.4/ORC/bot-control checks. Compare IDs across environments and require separation. This task stops after LOCAL PAPER.

## 20. Explicit Non-goals

No Recommendation Outcome Attribution V2, confidence evolution, adaptive calibration, ORC/realtime/MME weight learning, promotion/PAPER experiment engine, LIVE apply, capital allocation, strategy/exit tuning, NO TRADE heuristic aggregation, runtime automation, Docker/service change or VPS action.

## Local Rollout Evidence — 2026-07-12

Repository pre-state was `main` at `444b4f2f00990f877c2660ff4454656fcfbd75af`, equal to `origin/main`. Existing untracked backup/audit artifacts were not touched. The architecture audit was already tracked by commit `444b4f2`.

The initial LIVE attempt found an ambiguous column in the summary view and rolled back atomically before commit. After qualification, the migration completed; its second run completed using only idempotent notices. A later run-audit subtransaction hardening was applied idempotently to both databases.

| Result | LOCAL LIVE | LOCAL PAPER |
|---|---:|---:|
| source positions | 3,043 | 9,117 |
| first refresh inserted decisions | 3,043 | 9,117 |
| second refresh inserted decisions | 0 | 0 |
| ACTUAL_TRADE outcomes | 3,043 | 9,112 |
| decisions without outcome | 0 | 5 (all OPEN) |
| duplicate identities / multi-mapped positions | 0 | 0 |
| orphan outcomes / UUID mismatches / provenance violations | 0 | 0 |
| rows with one unambiguous legacy key | 19 | 277 |
| legacy 1:N mappings | 0 | 0 |

Twenty-record recomputation samples in each database produced zero unstable IDs. Full-set comparison found zero common UUIDs across 3,043 LIVE and 9,117 PAPER identities. Example payloads and results:

```text
LOCAL|trading_live|positions|1|TRADE_EXECUTED
-> a9641543-63c4-537f-b5ca-d1f122a381c3

LOCAL|trading_paper|positions|1|TRADE_EXECUTED
-> 5f8f1db0-fc80-5c59-ad42-07b76755668d

LOCAL|trading_paper|positions|2|TRADE_EXECUTED
-> d166e40e-e083-53fa-87e4-4d3cf3a4f469
```

Protected pre/post evidence: LIVE positions remained 3,043 with ID sum 4,646,132; PAPER positions remained 9,117 with ID sum 41,976,522; bot-control remained 32 rows in each database. Strategy telemetry continued to append concurrently (LIVE +189 and PAPER +264 during validation), but the migration/function contains no write statement targeting `strategy_events`. Learning V1.4 remained `apply_enabled=0`, status `ok`; ORC remained `ORC_V6_3 / COOLDOWN_PROMOTE_HYSTERESIS`.

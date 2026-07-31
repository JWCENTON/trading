# Live Entry Fill Attribution V1 (LEI1C)

LEI1C records authoritative exchange fill evidence and classifies its
application state. It deliberately stops before position, inventory, lifecycle,
or Financial Truth projection; those mutations belong to LEI1D and later
contracts.

## Boundary

The evidence flow is:

1. authoritative exchange fill observation;
2. exact order/CID lineage resolution;
3. immutable `live_entry_fill_evidence_v1` commit;
4. append-only `live_entry_fill_applications_v1` decision;
5. no position or inventory write.

Evidence and application decisions commit independently. This preserves and
makes retryable the crash window in which evidence is durable but no
application decision exists yet.

`LIVE_ENTRY_FILL_ATTRIBUTION_MODE` accepts `OFF`, `SHADOW`, or `ENFORCE` and
defaults to `OFF`. `OFF` performs no repository or schema access. This patch
does not set the variable, apply the schema, activate a writer, or change a
runtime image. `SHADOW` contains setup, schema, repository, and row failures on
a connection separate from legacy ingestion; `ENFORCE` fails closed.

## Canonical identity and fingerprint

The evidence natural key is exactly:

- environment;
- deployment ID;
- exchange source;
- exchange trade ID.

The source fingerprint additionally binds the exchange order ID, exact
exchange-observed client order ID, symbol, side, executed quantity, price,
notional, fee, fee asset, and execution timestamp. Decimal values never pass
through binary floating point. Observation time, producer identity, and local
attribution are intentionally excluded from the source fingerprint.

The same natural key and source fingerprint is `IDEMPOTENT_EXISTING`. The same
natural key with a different fingerprint is `IDEMPOTENCY_CONFLICT`; the first
evidence row remains immutable and the conflicting incoming semantic payload is
preserved in an application-decision payload.

Each partial fill has its own exchange trade ID and therefore its own evidence
row. Multiple partial fills may reference one intent/submission/ACK. LEI1C does
not sum or project them.

## Attribution

Resolution is exact and fail-closed in this order:

1. exchange order ID to LEI1B ACK;
2. exchange client order ID to ACK/submission/intent;
3. exact intent natural identity through the CID;
4. exact legacy bot order evidence;
5. external/manual evidence;
6. ambiguity or conflict.

Symbol, side, timestamp proximity, and similar quantity are never linkage
keys. Order-ID and CID candidates that point to different ACKs are conflicted.
Multiple exact candidates are ambiguous. Environment, deployment, adoption,
generation, runtime Git SHA, exchange, symbol, side, and ENTRY purpose must
match before full bot ownership is accepted.

For a fill that arrives after an adoption rollover, an exact ACK (or exact
partial LEI1B lineage) retains the historical adoption, generation, and Git
identity that submitted the order. A foreign PAPER/LIVE candidate does not
make an otherwise unique candidate ambiguous. Multiple exact candidates in
the same runtime domain, or disagreement between order ID and CID, fail
closed. A non-active adoption is accepted only when it was previously active
and the evidence carries matching typed LEI1B lineage; external or legacy
evidence cannot select an arbitrary historical generation.

OKX normalizes the deterministic producer CID to alphanumeric characters and
32 bytes on the wire. LEI1C stores both the original LEI1B CID and the exact
wire CID; its CID fallback applies that one deterministic normalization and
does not use fuzzy matching.

Full LEI1B lineage with existing position proof is
`BOT_OWNED_ATTRIBUTED`. The same exact lineage without a position is the valid
pre-LEI1D state `BOT_OWNED_MISSING_POSITION`. Partial LEI1B evidence is
`BOT_OWNED_MISSING_LINEAGE`; exact pre-LEI1B order evidence is
`LEGACY_BOT_OWNED`. No exact bot evidence is
`EXTERNAL_OR_MANUAL_UNLINKED`; no synthetic intent or position is created.
An exact legacy EXIT order is explicitly `UNKNOWN` with
`OUTSIDE_LEI1C_ENTRY_SCOPE`; it is not mislabeled as an entry conflict and does
not block normal exit ingestion in `ENFORCE`.

Observation-time attribution in the evidence row is immutable. A recovered
ACK or newly available exact position link is an append-only application
decision and only a monotonic refinement: every previously known intent,
submission, ACK, CID, strategy, interval, purpose, and position identity must
remain compatible. A different identity is a durable conflict. Concurrent
decision writers serialize on the evidence row; unresolved conflict,
correction, and ambiguity take precedence over benign replay.

## Applied-state semantics

Source equality and application proof are separate facts.

`OBSERVED_NOT_APPLIED` applies whenever any required application proof is
missing, including a canonical local fill identity, matching applied
fingerprint, applied timestamp, application target, or stable attribution
identity.

`TRUE_DUPLICATE_APPLIED` is derived only when all of those facts exist and the
applied fingerprint equals the immutable source fingerprint. The target is
canonicalized as `binance_order_fills:<local_fill_id>` and validated against
the exact immutable fill semantics. When the pre-LEI1C ingestion ledger
already contains a committed application, LEI1C may append an `APPLIED`
bridge decision only after both the canonical local fill and the ingestion
ledger's authoritative payload, generation, applied fingerprint, and applied
timestamp all match. The legacy and LEI1C fingerprint algorithms are not
treated as interchangeable. A replay of an
unapplied observation remains `OBSERVED_NOT_APPLIED`; repeated delivery alone
never upgrades it to a duplicate-applied state.

An incoming changed payload for the same source identity is
`IDEMPOTENCY_CONFLICT` and cannot carry application proof. A separately
recorded `CORRECTION_PENDING` decision also remains untrusted and cannot be
reported as duplicate-applied. `APPLIED` is reserved for a future explicit
LEI1D application writer or the narrowly validated bridge to application
proof already committed by the existing ingestion writer. LEI1C never infers
it from source equality or creates a local fill itself.

Initial external/manual and ambiguous observations have dedicated application
states and no invented local fill or position linkage. A later hard decision
may retain previously accepted lineage evidence, but never application proof.

## Legacy ingestion defect containment

The earlier mutable ingestion state conflated source equality with application:
it bootstrapped a pre-existing fill as `DUPLICATE/NO_CHANGE`, and replayed a
matching ledger fingerprint as `DUPLICATE`, without reading `local_fill_id`,
`applied_fingerprint`, or `applied_at`.

The compatibility path now keeps `NO_CHANGE` only as an internal source-change
fact. Its persisted application classification is
`OBSERVED_NOT_APPLIED` until complete, matching application proof exists.
Pending corrections and ambiguous evidence are not overwritten by replay.
There is no automatic historical backfill or data mutation in the migration.

The forward producer uses the later of the per-symbol ingestion cursor and
the first successful, matching-checksum LEI1C migration-ledger `applied_at`
timestamp, rounded up to the next millisecond. Thus an initial correction
lookback cannot become an implicit historical scan. A pre-boundary row is
processed only when immutable LEI1C evidence for that exact natural identity
already exists; explicit recovery remains the only way to introduce older
evidence.

Forward ingestion is not the recovery mechanism. Operators or a bounded
recovery tool must construct one authoritative `EntryFillObservation` and call
`recover_entry_fill_attribution(...)`; that explicit entry point processes only
the supplied observation in `ENFORCE` mode and never scans history.

## Migration and rollback

`20260731_live_entry_fill_attribution_v1.sql` is additive and idempotent. It
creates only the two LEI1C ledgers, their explicit constraints/indexes, exact
lineage validators, append-only blockers, and migration-ledger record. It does
not update orders, fills, positions, prior ledgers, or adoption state.

The JSON manifest pins the forward and rollback SHA-256 values and enumerates
all contract objects. `20260731_live_entry_fill_attribution_v1_rollback.sql`
succeeds repeatedly only while both ledgers are absent or empty. Partial schema
or any evidence/application row fails closed.

No migration in this patch is applied to LOCAL LIVE, LOCAL PAPER, or VPS.
The current schema and producer are explicitly LIVE-only because their proof
adapters depend on `binance_orders` and `binance_order_fills`. PAPER remains
`OFF`; a later PAPER SHADOW rollout requires a separate simulated-fill and
lineage adapter rather than silently reusing LIVE projection tables.

## Events

The service exposes these explicit events:

- `ENTRY_FILL_OBSERVED`;
- `ENTRY_FILL_EVIDENCE_CREATED`;
- `ENTRY_FILL_EVIDENCE_IDEMPOTENT`;
- `ENTRY_FILL_ATTRIBUTED`;
- `ENTRY_FILL_OBSERVED_NOT_APPLIED`;
- `ENTRY_FILL_TRUE_DUPLICATE_APPLIED`;
- `ENTRY_FILL_EXTERNAL_UNLINKED`;
- `ENTRY_FILL_AMBIGUOUS`;
- `ENTRY_FILL_CONFLICT`;
- `ENTRY_FILL_CORRECTION_PENDING`.

There is no premature `ENTRY_FILL_APPLIED` event.

## LEI1D prerequisites

LEI1D must consume only trusted, non-conflicted immutable evidence and provide
an explicit application target, canonical local fill identity, matching
fingerprint, timestamp, and attribution identity. It must aggregate partial
fills idempotently, account for fee assets, establish inventory high-water, and
write position/lifecycle state without mutating either LEI1C ledger.

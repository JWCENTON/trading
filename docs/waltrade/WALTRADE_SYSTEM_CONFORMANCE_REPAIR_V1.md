# WalTrade System Conformance Repair V1

STATUS=MR1_MR2_LOCAL_PAPER_IMPLEMENTED_AND_ACCEPTED

SCOPE=LOCAL_PAPER,VPS_PAPER,LOCAL_LIVE,VPS_LIVE

AUDITS_CONSOLIDATED=4

This document preserves the bounded repair design produced from the four
completed system-conformance audits and records the accepted LOCAL PAPER MR1
and MR2 execution. New economic experiments remain paused and LIVE new-entry
authority remains disabled.

`VPS_PAPER_RUNTIME_REVISION_STATUS=NON_UNIFORM_NOT_ACCEPTED` under the four
conformance audits. The formerly reported `d60c451...` value is superseded
historical evidence and is not accepted as current VPS PAPER runtime truth.

`H12=PRESERVED_UNINSPECTED_BUT_NOT_CURRENT_WORK`; it is neither an active NEXT
item nor repair acceptance evidence.

The North Star and Constitution remain unchanged. The ordering principle is:

`RELEASE TRUTH -> REGIME SSOT -> ACTIVE CAPABILITIES -> SCHEMA/DEPLOYMENT/UI -> DIRECT PAPER ENFORCEMENT`

## 0. Current minimum execution scope

`CURRENT_EXECUTION_SCOPE=MINIMUM_WALTRADE_CONFORMANCE_RECOVERY`

The complete defect register remains authoritative, but current execution is
limited to three minimum-recovery units:

### MR1_MINIMAL_RELEASE_TRUTH

- Identify the exact Git SHA, image and effective configuration only for
  services touched by the repair.
- Reuse existing labels and scripts and add at most one small read-only
  verifier.
- Create no service, deployment platform or database ledger.

### MR2_CORE_ADMISSION_REPAIR

- Use one authoritative regime SSOT for worker, gate and watchdog.
- Add causal freshness and fail-closed behavior, canonical `SUPERTREND`, a
  verified 20/20 policy matrix and unambiguous effective-mode precedence.
- Make PANIC uncertainty fail closed and link the gate decision to
  entry/position identity.
- Require focused unit, integration and natural E2E tests.

MR2 resolved the policy-source ambiguity before implementation. The corrected
971-position population artifact reproduces the LOCAL PAPER matrix exactly:
245 policy-allowed positions and `-34.148466549209 USDC` net. Section 2A
records the selected policy and the rejected historical-comment variant.

### MR3_DIRECT_PAPER_ENFORCEMENT_AND_ECONOMIC_ACCEPTANCE

- Use no shadow substitute: direct LOCAL PAPER enforcement is active following
  MR1 and MR2 acceptance.
- After LOCAL PASS, commit and push the exact SHA; VPS may only fetch, inspect,
  pull the approved history and validate independently before direct VPS PAPER.
- Keep LIVE frozen.
- Measure trades, fees, net PnL, rejected trades, rejected promising trades
  and the delta against the frozen baseline.

At the first entry/runtime-semantic change, close affected existing forward
holdouts at the exact deployment cutoff as
`PRE_CONFORMANCE_LEGACY_EVIDENCE`. Do not pool their observations with the new
post-repair cohort.

`DEFERRED_SCOPE=ORIGINAL_WAVE_3_REMAINDER,H12,RISK_BUDGET,SLOT_BRAIN,ORC,LEARNING,MIGRATION_WIDE_CLEANUP,VPS_PARITY_ITEMS`

Every deferred capability remains authority-OFF. A capability may be recorded
as verified inactive, but not as functionally repaired or accepted without its
own proof.

## 0A. Executed LOCAL PAPER recovery

`MR1_IMPLEMENTED=YES`; commit `bb0957a` added the bounded read-only release
truth verifier. It distinguishes repository HEAD, OCI image revision/digest,
container runtime revision, Compose inputs, explicit env file and selected
non-secret effective configuration. Before repair it proved non-uniform bot,
regime and automation revisions; after rollout every touched service reported
the same implementation revision.

`MR2_IMPLEMENTED=YES`; commit `649dc42` established
`REGIME_GATE_MARKET_REGIME_SSOT_V1`, canonical `SUPERTREND`, causal as-of
lookup, shared freshness validation, fail-closed missing/stale/NULL/UNKNOWN/
missing-policy paths, unambiguous effective-mode resolution, entry-only PANIC
fail-closed behavior and immutable gate/source/policy linkage propagated into
the admission/position path. Commit `03e5bec` exposed the minimum API/UI
process, data, semantic and authority truth required by the audit.

The idempotent LOCAL PAPER migration
`20260906_regime_ssot_direct_paper_enforcement_v1` is ledgered at implementation
SHA `03e5bec329520e9d16966078c5614f8ed666af09`, checksum
`92bb110882177ce1cde10343ac3ecdd07bc7ab811f923ee1ddd21726b190658f`, with
20 canonical policy rows, zero active `SUPER_TREND` rows and 32/32 enabled
`ENFORCE` slots. Reapplication produced no additional ledger row and no policy
or authority semantic change.

The implementation code was accepted first at
`03e5bec329520e9d16966078c5614f8ed666af09`; the touched LOCAL PAPER images are
subsequently labelled and recreated from the final documentation-inclusive
repository HEAD. The MR1 verifier output, rather than this prose, is the
canonical current release identity. A bounded six-minute acceptance window
showed 32/32 fresh strategy slots, 5/5 healthy support workers, zero
blocked DB sessions, zero critical errors/tracebacks and stable restart count
zero. Gate and watchdog both reported source `market_regime` and contract
`REGIME_GATE_MARKET_REGIME_SSOT_V1`. Deterministic PAPER-only E2E proof, with
no order or position creation, proved ALLOW, policy BLOCK, missing, stale,
PANIC and canonical SUPERTREND behavior. Edge routes returned HTTP 200 for
both PAPER and unchanged LIVE API/UI after a configuration-preserving nginx
upstream reload.

`FORWARD_BASELINE_CUTOFF=2026-09-06T20:33:57.907250026Z`, the exact start of
the new bot-runner carrying direct entry semantics. Earlier affected economic
cohorts are closed at this boundary as `PRE_CONFORMANCE_REPAIR`, `UNTREATED`,
`REGIME_NOT_ENFORCED` and must not be pooled with forward evidence.

## 1. Scope and evidence convention

Environment codes: `LP` = LOCAL PAPER, `VP` = VPS PAPER, `LL` = LOCAL LIVE,
`VL` = VPS LIVE, and `ALL` = all four environments.

`ALL` identifies a shared contract whose conformance must be proved
independently in every environment. Environment-specific counts, timestamps,
revisions and outcomes must be recovered from that environment's audit
artifact during acceptance; they must never be inferred from another
environment. Business-data differences are not parity failures.

## 2. Consolidated defect register and exact environment disposition

The four audits reduce to 24 unique issues after merging repeated symptoms.
Each environment cell is the audit disposition for that specific contract:
`PASS`, `FAIL`, `PARTIAL`, or `NOT_APPLICABLE`. `PASS` does not mean that
another environment's evidence was reused.

| ID | Priority | Consolidated issue | LP | VP | LL | VL | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SC-P0-01 | P0 | Regime writer, gate and watchdog do not share one source contract | FAIL | FAIL | FAIL | FAIL | Wave 2 |
| SC-P0-02 | P0 | Regime lookup lacks causal freshness authority and fails open on stale, NULL, UNKNOWN or missing policy under ENFORCE | FAIL | FAIL | FAIL | FAIL | Wave 2 |
| SC-P0-03 | P0 | `SUPERTREND` conflicts with legacy `SUPER_TREND`; policy coverage is not 20/20 | FAIL | FAIL | FAIL | FAIL | Wave 2 |
| SC-P0-04 | P0 | Effective regime authority can disagree between environment defaults and per-slot runtime control | PARTIAL | PARTIAL | FAIL | FAIL | Waves 1-2 |
| SC-P0-05 | P0 | PANIC/control database-read failure can permit entry evaluation to continue | FAIL | FAIL | FAIL | FAIL | Wave 3 |
| SC-P1-01 | P1 | Service images and running containers execute non-uniform or unproved revisions | FAIL | FAIL | FAIL | FAIL | Wave 1 |
| SC-P1-02 | P1 | Risk Budget emits `MISSING_POLICY` or runs an older event contract | PARTIAL | PARTIAL | FAIL | FAIL | Wave 3 |
| SC-P1-03 | P1 | Slot Brain coverage is partial and absence is not an explicit audited state | PARTIAL | PARTIAL | PARTIAL | PARTIAL | Wave 3 |
| SC-P1-04 | P1 | ORC has multiple apparent apply/authority surfaces and no single writer contract | PARTIAL | PARTIAL | PARTIAL | PARTIAL | Wave 3 |
| SC-P1-05 | P1 | Documented capabilities lack deployment and natural end-to-end activation proof | FAIL | FAIL | FAIL | FAIL | Waves 1, 3-4 |
| SC-P1-06 | P1 | H12 executable estimator exists locally but was not promoted or shown reproducible on VPS | PASS | FAIL | NOT_APPLICABLE | NOT_APPLICABLE | Wave 4 |
| SC-P1-07 | P1 | Canonical 120-minute outcome capability is absent on VPS PAPER | PASS | FAIL | NOT_APPLICABLE | NOT_APPLICABLE | Wave 4 |
| SC-P1-08 | P1 | Market-data direct dependency contract drifts between LOCAL and VPS deployments | PASS | FAIL | PASS | FAIL | Waves 1 and 4 |
| SC-P1-09 | P1 | VPS LIVE Learning data/runtime freshness is stale | NOT_APPLICABLE | NOT_APPLICABLE | NOT_APPLICABLE | FAIL | Wave 4 |
| SC-P2-01 | P2 | Image labels, build SHA and runtime-reported revision are unreliable or stale | FAIL | FAIL | FAIL | FAIL | Wave 1 |
| SC-P2-02 | P2 | Migration ledger does not fully explain schema objects used by runtime | FAIL | FAIL | FAIL | FAIL | Wave 4 |
| SC-P2-03 | P2 | No permanent four-part deployment acceptance gate exists | FAIL | FAIL | FAIL | FAIL | Waves 1 and 4 |
| SC-P2-04 | P2 | No canonical Capability Activation Ledger exists | FAIL | FAIL | FAIL | FAIL | Waves 1 and 4 |
| SC-P2-05 | P2 | Regime gate lacks exact gate-to-decision-to-snapshot-to-position lineage | PARTIAL | PARTIAL | PARTIAL | PARTIAL | Wave 2 |
| SC-P2-06 | P2 | VPS PAPER position-to-240-minute-outcome linkage is incomplete | PASS | FAIL | NOT_APPLICABLE | NOT_APPLICABLE | Wave 4 |
| SC-P2-07 | P2 | API/UI omits separate semantic and authority health | PARTIAL | PARTIAL | FAIL | FAIL | Wave 4 |
| SC-P3-01 | P3 | Legacy `regime_state` remains after `market_regime` becomes authoritative | FAIL | FAIL | FAIL | FAIL | Backlog after Wave 2 |
| SC-P3-02 | P3 | Stale slot-intelligence and orchestrator snapshot stores remain | PARTIAL | PARTIAL | PARTIAL | PARTIAL | Backlog |
| SC-P3-03 | P3 | Reconciled legacy order/status rows and obsolete UI/runtime labels remain | NOT_APPLICABLE | NOT_APPLICABLE | PARTIAL | PARTIAL | Backlog unless required by Wave 4 |

`UNIQUE_P0=5`, `UNIQUE_P1=9`, `UNIQUE_P2=7`, `UNIQUE_P3=3`.

## 2A. Regime policy source comparison — resolved before implementation

`POLICY_SEMANTICS_UNRESOLVED=NO`

`POLICY_SOURCE_CHECK_STATUS=RESOLVED_BY_CORRECTED_POPULATION_REPRODUCTION`

`CANONICAL_POLICY_VERSION=REGIME_POLICY_20260906_V1`

The accepted source is the corrected immutable 971-position research
population together with its decision-time policy evidence, reconciled to the
active LOCAL PAPER policy table and canonicalized only from `SUPER_TREND` to
`SUPERTREND`. It reproduces `ALLOWED=245` and
`COUNTERFACTUAL_NET=-34.148466549209 USDC` exactly. The older source comments
in commit `17a3df6...` imply two different rows and reproduce 291 allowed
positions with `-38.400551987143 USDC`; they are retained as superseded design
history, not chosen because of outcome favorability. The selection is based
on exact provenance agreement with the already accepted 245-position
counterfactual.

| Strategy | RANGE_LOWVOL | RANGE_HIGHVOL | TREND_UP | TREND_DOWN | SHOCK |
| --- | --- | --- | --- | --- | --- |
| RSI | ALLOW | ALLOW | BLOCK | BLOCK | BLOCK |
| TREND | BLOCK | BLOCK | ALLOW | ALLOW | BLOCK |
| SUPERTREND | BLOCK | ALLOW | ALLOW | ALLOW | BLOCK |
| BBRANGE | BLOCK | ALLOW | BLOCK | BLOCK | BLOCK |

Fingerprint input is UTF-8, LF-terminated, sorted by strategy then regime,
with one line `STRATEGY|REGIME|ALLOW_OR_BLOCK`. Its exact 20-line payload is:

```text
BBRANGE|RANGE_HIGHVOL|ALLOW
BBRANGE|RANGE_LOWVOL|BLOCK
BBRANGE|SHOCK|BLOCK
BBRANGE|TREND_DOWN|BLOCK
BBRANGE|TREND_UP|BLOCK
RSI|RANGE_HIGHVOL|ALLOW
RSI|RANGE_LOWVOL|ALLOW
RSI|SHOCK|BLOCK
RSI|TREND_DOWN|BLOCK
RSI|TREND_UP|BLOCK
SUPERTREND|RANGE_HIGHVOL|ALLOW
SUPERTREND|RANGE_LOWVOL|BLOCK
SUPERTREND|SHOCK|BLOCK
SUPERTREND|TREND_DOWN|ALLOW
SUPERTREND|TREND_UP|ALLOW
TREND|RANGE_HIGHVOL|BLOCK
TREND|RANGE_LOWVOL|BLOCK
TREND|SHOCK|BLOCK
TREND|TREND_DOWN|ALLOW
TREND|TREND_UP|ALLOW
```

`CANONICAL_POLICY_FINGERPRINT_SHA256=585ab57f906dff274e5df344475eb24de6f4977a3985535427edb7852093eb3e`

Observed LOCAL PAPER differences after canonicalizing `SUPER_TREND` to
`SUPERTREND`:

| Strategy/regime | History-derived candidate | LOCAL PAPER DB |
| --- | --- | --- |
| `BBRANGE/RANGE_LOWVOL` | ALLOW | BLOCK |
| `SUPERTREND/RANGE_LOWVOL` | ALLOW | BLOCK |

The rejected history-comment variant fingerprint is
`bef65250b06141fe64baadf4e01245a9ace7dbe25948978e4af516be7a5f1d94`.
The two-row delta contains 46 BBRANGE/RANGE_LOWVOL positions and zero
SUPERTREND/RANGE_LOWVOL positions in the corrected population. The chosen
matrix was frozen before code/config/DB mutation.

## 3. Repair contracts by issue

### P0 contracts

| ID | Root cause and affected surface | Economic/safety impact | Dependencies and minimum repair | Required tests | Rollout, rollback, acceptance evidence |
| --- | --- | --- | --- | --- | --- |
| SC-P0-01 | `regime-worker` writes fresh `public.market_regime`, while gate code reads legacy `public.regime_state`; watchdog health is based on a different source. Code, SQL reader/writer, watchdog, schema dependency and runtime are affected. | A healthy watchdog can coexist with a stale trading decision source; declared regime protection is not the protection exercised. | After Wave 1, make `market_regime` the only gate and watchdog source through one shared resolver. No dual-read fallback. Retain `regime_state` only until parity is proved. | SQL-contract tests; writer-reader-watchdog source identity; four strategies; unavailable source; causal as-of boundary; restart; four-environment dependency manifest. | Use the canonical rollout sequence. Roll back to the pinned image and stop admission authority; never restore split-source authority. Accept only when source/query/writer/watchdog identity and natural path evidence agree per environment. |
| SC-P0-02 | Gate accepts non-causal/latest or stale state and missing/NULL/UNKNOWN/missing-policy paths are permissive. Shared gate code, policy lookup, freshness config and decision ledger are affected. | Future leakage invalidates evidence; fail-open ENFORCE can admit real orders contrary to policy. | Depends on SC-P0-01. Resolve newest finalized `market_regime` with `source_time <= decision_source_time`; validate timestamp/age using a versioned cadence contract; ENFORCE fails closed; DRY_RUN admits but emits explicit `would_block` and reason. | Future-row exclusion, boundary age, stale/NULL/UNKNOWN/absent regime, absent policy, duplicates, DB error, DRY_RUN vs ENFORCE, four strategies and PAPER/LIVE paths. | Same order as SC-P0-01. Rollback means authority OFF and experiment pause, not fail-open or shadow completion. Accept deterministic replay equality and natural evidence with source time, freshness, reason and mode. |
| SC-P0-03 | Runtime canonical strategy is `SUPERTREND`, while legacy policy rows use `SUPER_TREND`; coverage is below 4 strategies x 5 regimes. Identifiers, policy data/migration, validation and config are affected. | SUPERTREND can bypass policy or resolve the wrong row. | Freeze `SUPERTREND` as the only runtime identity, migrate/normalize legacy rows, enforce uniqueness and require exactly 20/20 canonical rows before readiness. | Alias migration idempotency; conflict fail closed; 20/20 matrix; missing/duplicate row; all strategy-regime pairs; existing evidence attribution. | Use the canonical rollout sequence; the policy data step precedes dependent activation. Roll back code while retaining compatible mapping. Accept 20 canonical keys, zero legacy runtime reads and four-strategy proof. |
| SC-P0-04 | Environment defaults and database `bot_control`/slot settings can disagree about `regime_mode`; readiness does not reject ambiguity. Config precedence, startup validation, control reader and manifest are affected. | Operators can believe ENFORCE is active while runtime is DRY_RUN, or unintentionally grant authority. | Declare per-slot runtime control as authority, with env values as startup constraint/default only; disagreement fails readiness without auto-mutation. LIVE new-entry freeze overrides every regime result. | Precedence matrix; missing/mixed rows; env/control mismatch; restart; LIVE freeze precedence; exit availability. | Use the canonical rollout sequence without changing authority until acceptance; frozen LIVE last. Roll back image, preserve controls. Accept expected-slot effective-mode attestations and zero unexplained mismatch. |
| SC-P0-05 | PANIC or safety-control lookup exceptions are treated as permission to continue. Permission/control reader and entry path are affected. | A database/control outage can create an entry when safety authority is unknowable. | After Wave 1, add the minimum entry-only fail-closed wrapper. Preserve reconciliation and exit/close. | Timeout/error/NULL; PANIC on/off; entry denied on uncertainty; exits/reconciliation available; four strategies; LIVE freeze precedence. | Use the canonical rollout sequence. Roll back image with entry authority disabled. Accept failure injection with zero entry intents and functioning exit/reconciliation paths. |

### P1 contracts

| ID | Root cause and affected surface | Economic/safety impact | Dependencies and minimum repair | Required tests | Rollout, rollback, acceptance evidence |
| --- | --- | --- | --- | --- | --- |
| SC-P1-01 | Builds are not uniformly pinned to clean source; labels and files can diverge. | Results and incidents cannot be assigned to one contract; rollback is ambiguous. | Wave 1: clean-source build, immutable digest, embedded commit/dirty flag, runtime version endpoint, compose/env manifest and per-service bill of materials. | Reproducible metadata, dirty-build rejection, file-hash sampling, mixed-revision readiness failure. | Establish before semantic rollout. Roll back by immutable digest. Accept when Git, image, runtime and compose attestations agree for every service. |
| SC-P1-02 | Approved Risk Budget immutable-event code is not consistently deployed and policy resolution ends in `MISSING_POLICY`. | Evidence is incomplete and future influence could use no policy; influence must remain OFF. | After Waves 1-2, deploy the approved event path and complete policy contract. Keep `RISK_BUDGET_INFLUENCE=OFF` pending separate authority. This is a functional repair, not a shadow substitute. | Same-cutoff idempotency, revision-only provenance, late evidence, true conflict fail closed, valid/missing policy, restart retry. | Use the canonical rollout sequence. Roll back digest with influence OFF. Accept natural canonical `STATE_EVALUATION`, zero new conflicts and exact revision provenance. |
| SC-P1-03 | Slot Brain materialization is sparse and missing contexts are indistinguishable from inactive/broken contexts. | Consumers can treat absence as neutral without knowing why; ACTIVE cannot be claimed. | Define eligibility and explicit `ACTIVE`, `NOT_APPLICABLE`, `STALE`, `ERROR` for every configured slot; do not fabricate intelligence. | All slots accounted for, causal freshness, no-evidence state, restart, aliases and consumer behavior. | Use the canonical rollout sequence. Roll back consumer authority to OFF. Accept expected-slot ledger and natural observation for every eligible class. |
| SC-P1-04 | ORC action flags, automation apply config and manual controls do not compose into one obvious writer authority. | Conflicting writers could mutate controls; manual state currently masks ambiguity. | Select one apply writer and one master authority gate; all other components evidence-only. Manual/LIVE freeze prevails. | Single-writer invariant, concurrent attempts, OFF, manual override, idempotency, restart, DB failure and audit event. | Use the canonical rollout sequence with apply OFF until accepted. Roll back writer OFF. Accept one writer, zero unauthorized mutations and exact decision-to-control audit. |
| SC-P1-05 | Documentation records completion without systematic per-environment activation proof. | Research can rely on code/schema runtime does not execute. | Introduce the Capability Activation Ledger and require `END_TO_END_OBSERVED` before ACTIVE claims. Correct superseded claims; invent no historical proof. | Ledger validation, evidence-link existence, transition rules and environment/revision uniqueness. | Create Wave 1; populate Waves 2-4. Document rollback only. Accept no ACTIVE capability without all applicable ledger dimensions, natural normal-path proof and evidence IDs. |
| SC-P1-06 | The H12 executable estimator is local-only and lacks an approved, reproducible VPS artifact/path. | A LOCAL result cannot establish shared deployability or VPS parity. H12 remains non-authoritative and is not current work. | Wave 1 release identity and Wave 4 dependency/portability proof. Promote nothing until exact source, inputs and deterministic output contract are reproducible. | Same frozen input artifact produces the same fingerprint/output locally and on VPS; no holdout efficacy inspection. | Use the canonical rollout sequence only if separately approved. Roll back by leaving H12 inactive. Accept exact artifact, input, revision and deterministic-output parity. |
| SC-P1-07 | VPS PAPER lacks the canonical 120-minute outcome capability required by the accepted evidence contract. | Cross-environment maturity and outcome comparisons are incomplete; no trade authority impact today. | After schema/dependency inventory, deploy the existing canonical outcome contract without reconstructing outcomes from non-authoritative data. | OPEN/COMPLETE/INSUFFICIENT states, cutoff causality, no future leakage, idempotency, LOCAL/VPS contract parity. | Canonical rollout sequence; rollback reader/worker OFF while preserving rows. Accept a natural VPS lifecycle with authoritative 120m outcome identity. |
| SC-P1-08 | Market-data services do not have one accepted direct-dependency and revision contract across LOCAL and VPS. | A strategy or evidence worker can consume a different candle/source semantic despite matching application Git. | Wave 1 manifest plus Wave 4 direct-dependency fingerprints for image, source, schema and freshness. | Immutable external-image digest, candle schema/source identity, finalized-candle rules, freshness and routing parity. | Canonical rollout sequence; roll back to accepted digest. Accept direct dependency and natural candle-path parity independently on VPS. |
| SC-P1-09 | VPS LIVE Learning inputs/runtime are stale. | Learning evidence cannot be claimed current; auto-apply must remain OFF and no capital authority may derive from it. | Wave 4 freshness/read-path repair only after release/schema truth. Do not backfill by copying LOCAL data. | Freshness states, causal source, restart, no cross-environment data copy, auto-apply OFF, UI health visibility. | Frozen LIVE last in canonical sequence. Roll back Learning reader/worker OFF. Accept natural VPS LIVE input freshness with zero auto-apply. |

### P2 contracts

| ID | Root cause and affected surface | Economic/safety impact | Dependencies and minimum repair | Required tests | Rollout, rollback, acceptance evidence |
| --- | --- | --- | --- | --- | --- |
| SC-P2-01 | OCI labels, health/version strings and running source can name different commits. | Incident and experiment provenance is unreliable. | Generate labels from build-context commit and expose the same immutable manifest at runtime. | Label/runtime/digest equality, dirty-tree rejection, stale-label negative test. | Use the canonical rollout sequence; rollback immutable image; accept exact equality and hash-bound manifest. |
| SC-P2-02 | Schema objects exist beyond the recorded migration ledger or cannot be traced to one. | Reproduction and safe rollback are uncertain; dependencies can vanish elsewhere. | Inventory direct SQL dependencies; add a reconciliation migration/attestation rather than rewrite history; prohibit manual schema. | Fresh migration, upgrade from observed states, schema fingerprint, idempotency and compatibility. | Use the canonical rollout sequence with backups and frozen LIVE. Correct forward, never reset. Accept ledger-object-runtime equality. |
| SC-P2-03 | Deployment uses ad hoc checks rather than a permanent AND gate. | Partial parity can be called complete. | Implement the four-part gate in Section 6 as release artifact and operator/CI contract. | Every dimension independently blocks; evidence expiry; wrong env; mixed revision; missing object. | Design Wave 1, mandatory Wave 4 onward. Roll back last fully accepted release. Accept immutable result per env/release. |
| SC-P2-04 | Capability presence, deployment, activity and observation are conflated. | Inactive functions appear complete. | Implement Section 7 ledger from build, schema, config and runtime attestations. | Transition validation, stale evidence invalidation and duplicate rejection. | Wave 1 design, Wave 4 enforcement. Accept complete rows for every in-scope capability. |
| SC-P2-05 | Gate result, source snapshot, admission decision and position cannot always be joined by one immutable identity. | Counterfactual cohorts and incident attribution can be wrong. | Wave 2 causal identity propagated through gate, decision, snapshot, atomic entry and position. Reuse existing IDs/provenance; schema only if direct proof finds no field. | One-to-one linkage, rejection has no position, retries, four strategies, restart and missing-lineage failure. | PAPER first; LIVE code under freeze. Preserve append-only evidence on rollback. Accept exact join cardinalities and source times. |
| SC-P2-06 | VPS PAPER position identity does not reliably join to its 240-minute outcome record. | Maturity classification can be missing or attached incorrectly; economic comparisons remain incomplete. | Wave 4 direct lineage repair, reusing position/decision/source IDs and append-only evidence. No inferred outcomes. | One-to-one join, OPEN/PENDING, missing source, duplicates, cutoff causality and complete lifecycle. | Canonical rollout sequence; roll back writer OFF and preserve data. Accept a natural VPS position-to-240m join with zero orphans/duplicates. |
| SC-P2-07 | API/UI health combines or omits semantic and authority state. | A green process/data screen can conceal wrong decision semantics or active authority. | Wave 4 adds the four health dimensions and actual revision/source/mode/policy/entry-authority fields to canonical API/UI/edge reads. | Dimension independence, overall AND, revision split, stale gate source, mode mismatch, incomplete policy, authority OFF/ON, 501/502 routing failures. | Canonical rollout sequence; roll back UI/API artifact without changing trading authority. Accept edge-to-UI display against the exact runtime state. |

### P3 backlog contracts

| ID | Root cause / impact | Minimum later action, tests, rollback and acceptance |
| --- | --- | --- |
| SC-P3-01 | Legacy regime table becomes maintenance debt after readers are removed. | After Wave 2, prove zero readers/writers and deprecate only by separate approval. Test dependency inventory. Rollback is retention; no deletion in V1. |
| SC-P3-02 | Old snapshot stores obscure authority. | After Waves 3-4, label non-authoritative stores and remove readers before any cleanup. Test reader inventory; leave data on rollback. |
| SC-P3-03 | Reconciled status/UI residue creates noise but does not grant exchange authority. | Address only if it blocks Wave 4 proof. Test canonical UI reads and local-history vs exchange-pending distinction. No destructive cleanup. |

## 3A. Holdout and historical-cohort transition contract

Existing Movement V1, P4 and H12 evidence remains preserved and uninspected,
but it is not current work. At the first deployment that changes entry or
runtime decision semantics, record one exact UTC deployment cutoff and close
every affected pre-repair economic cohort at that boundary as:

- `PRE_CONFORMANCE_LEGACY_EVIDENCE`
- `UNTREATED`
- `REGIME_NOT_ENFORCED`

Post-repair observations start a new cohort and must never be pooled with the
pre-repair cohort. No efficacy inspection occurs during repair. These labels
apply only to economic cohorts whose interpretation depends on the regime gate
not being enforced. Independent Fee V2, Financial Truth, entry atomicity,
failure-injection and infrastructure proofs retain their existing labels and
must not be downgraded to `REGIME_NOT_ENFORCED`.

## 3B. Canonical rollout sequence

Every wave that changes a shared artifact uses exactly:

`LOCAL implementation/tests -> commit -> push exact SHA to GitHub -> LOCAL PAPER deploy exact SHA -> LOCAL acceptance -> VPS fetch and inspect -> VPS pull --ff-only exact approved history -> VPS PAPER deploy -> independent VPS acceptance -> frozen LIVE environments last`

VPS never commits, pushes or creates a VPS-only shared fix. Natural business
data is never copied between LOCAL and VPS. Frozen LIVE deployment, when
applicable, occurs only after both PAPER acceptances and does not enable LIVE
entries.

## 4. Five implementation waves

### WAVE_1_RELEASE_TRUTH

Make the software actually running independently identifiable before semantic
repairs: immutable release manifest, Git/origin/dirty state, image digest,
per-service code hashes, compose and explicit env-file names, schema/config
fingerprints and runtime self-report. Mixed revisions fail readiness. Create
the initial Capability Activation Ledger as a static, versioned, hash-bound
manifest/artifact only, with unknown states recorded honestly and no DB
migration. Runtime/schema enforcement of the ledger starts only in Wave 4.
LIVE entry freeze and exit/reconciliation availability are preconditions.

External database and base images use `CODE_PRESENT=NOT_APPLICABLE` for a
WalTrade source SHA. They must instead provide an immutable image digest,
upstream version and supply-chain identity. A false WalTrade Git SHA on an
external image is an acceptance failure.

Exit gate: candidate `GIT_PARITY=PASS` and a recoverable immutable rollback
digest for every service. No authority changes.

### WAVE_2_REGIME_SSOT

Frozen design:

- `public.market_regime` is the sole gate and watchdog source.
- One shared resolver performs both reads; no `regime_state` fallback.
- Lookup selects finalized evidence whose source time is not later than the
  entry decision/source-candle time. Later rows are never visible.
- Persist source identity/time, decision time, age, freshness contract/version
  and result.
- Freshness is a deterministic configured-worker-cadence plus explicit
  ingestion-grace contract, frozen before implementation and never tuned on
  outcomes.
- `SUPERTREND` is the sole canonical runtime identity.
- Policy readiness requires 20/20 unique strategy-regime rows.
- ENFORCE denies entry for stale, NULL, UNKNOWN, absent regime/policy,
  ambiguous identity or lookup failure. Exits/reconciliation remain available.
- DRY_RUN uses the identical resolver, admits, and emits unambiguous
  `would_block=true/false` with exact reason.
- One identity links gate -> decision -> snapshot -> atomic position, or an
  explicit no-position outcome.
- Tests cover RSI, TREND, SUPERTREND, BBRANGE, both intervals, PAPER/LIVE
  permissions, retries, future leakage and every failure state.
- Watchdog PASS requires the exact gate source/freshness contract to pass.

Wave 2 implementation is blocked if
`POLICY_SEMANTICS_UNRESOLVED=YES`, the accepted 20/20 matrix cannot be
reproduced, or its fingerprint differs.

Use the canonical rollout sequence. LIVE code conformance does not change LIVE
entry authority.

### WAVE_3_ACTIVE_CAPABILITIES

Repair only the audited active-capability gaps: PANIC uncertainty fails closed
for entry while exits remain; Risk Budget executes its approved event/policy
contract with influence OFF; Slot Brain has explicit complete eligibility
state; ORC has one writer/master gate; every claim is ledger-backed.

Exit gate: intended authority observed end to end, zero unauthorized mutations,
and no `MISSING_POLICY` for eligible canonical Risk Budget evaluations.

### WAVE_4_SCHEMA_DEPLOYMENT_UI

Inventory direct readers/writers, reconcile migration provenance without
destructive history rewrite, fingerprint schemas/configs, make both permanent
gates release-blocking, and re-prove API/UI canonical authority against the
actual deployed revision and dependencies rather than Git or old labels.

API/UI/edge acceptance must traverse `edge-nginx` and actual routing, prove no
501/502 response, and display rather than mask: per-service runtime revision
split, gate source and freshness, effective regime mode, policy coverage and
actual new-entry authority. A healthy process alone cannot produce a green
semantic or authority result.

Exit gate: all active dependencies are migration-explained and four-environment
parity passes wherever the capability applies.

### WAVE_5_DIRECT_PAPER_ENFORCEMENT

After Waves 1-4 and separate Product Owner approval, enable the repaired
ENFORCE contract directly on bounded LOCAL PAPER. Prove policy-denied signals
create no positions and allowed signals preserve atomic entry/Financial Truth;
then repeat independently on VPS PAPER. This is conformance validation, not a
new economic hypothesis. Shadow is not repair acceptance. LIVE remains frozen.

Before activation, freeze separate baseline and activation timestamps and a
direct PAPER economic acceptance contract. Report, after authoritative
maturity and without pooling pre/post cohorts:

- trade-count reduction;
- fee reduction;
- net-loss reduction;
- net expectancy per admitted trade;
- gross result before fees;
- full-cost-cover rate;
- drawdown;
- capital utilization; and
- `CASH` as the explicit no-trade baseline.

Technical E2E acceptance and economic verdict are separate. Technical PASS
requires correct direct enforcement and lineage but cannot qualify economics.
Economic PASS requires the frozen metrics after costs and separate Product
Owner review; it cannot repair a technical failure. No metric threshold is
invented during implementation.

Rollback: disable PAPER admission authority, preserve evidence, stop further
experiments and restore the last accepted image. An incomplete repair cannot
be relabeled a shadow PASS.

## 5. Dependency and rollout order

`W1 release identity -> W2 regime contract -> W3 capability correctness -> W4 deployment/schema/UI enforcement -> W5 direct PAPER enforcement`

No later wave waives an earlier exit gate. Every shared change uses the exact
canonical rollout sequence in Section 3B. LIVE keeps entry disabled and proves
exit/reconciliation continuity. Required schema work is additive,
migration-ledgered, backup-protected and accepted before dependent code.

## 6. Permanent deployment acceptance gate

Acceptance is logical AND for the exact environment/release/capability:

| Gate | Required proof | Failure action |
| --- | --- | --- |
| `GIT_PARITY` | For WalTrade-built images: HEAD = approved origin SHA = image source SHA; clean build; expected digest running. For external database/base images: WalTrade SHA is `NOT_APPLICABLE`, while immutable digest/upstream version is mandatory. | Reject; restore last accepted digest. |
| `CONTRACT_PARITY` | Code contract, config fingerprint, authority, identifiers, policy and environment invariants match manifest | Reject readiness; never infer or auto-mutate authority. |
| `DIRECT_SCHEMA_DEPENDENCY_PARITY` | Every directly used object/column/index/function/view has approved semantics and migration provenance | Block activation; reviewed additive migration or compatible rollback. |
| `RUNTIME_PATH_SEMANTIC_PARITY` | A natural normal-path event traverses the actual deployed writer -> reader -> decision -> audit path with the declared result; controlled functional and failure proofs are separate mandatory evidence where applicable | Not accepted; code/schema presence or controlled-only proof cannot pass the normal path. |

Results are immutable, timestamped and hash-bound, expire when image/config/
schema changes, and link evidence IDs. Business-data equality is not required.
The parity gate is machine-readable and automatically evaluated in CI and
deployment acceptance; no manual green override can convert a failed dimension
to PASS.

Health is evaluated in four independent dimensions:

| Dimension | Required meaning |
| --- | --- |
| `PROCESS_HEALTH` | Expected process/container/worker is running, stable and not restart-looping. |
| `DATA_HEALTH` | Required source exists, is complete enough and fresh under its contract. |
| `SEMANTIC_HEALTH` | Actual writer-reader-decision path and contract version match the accepted semantics. |
| `AUTHORITY_HEALTH` | Effective authority equals approved authority; freezes and human gates have precedence. |

`OVERALL_READINESS=PASS` only when every applicable dimension is PASS. A
process or fresh table cannot mask semantic/authority failure.

## 7. Capability Activation Ledger

One row per capability x environment x release:

| Field | PASS meaning |
| --- | --- |
| `CODE_PRESENT` | Approved source/contract exists in the artifact. |
| `SCHEMA_PRESENT` | Direct dependencies have approved fingerprints/provenance, or justified `NOT_APPLICABLE`. |
| `CONFIG_PRESENT` | Explicit safe config and authority resolve without implicit defaults. |
| `DEPLOYED` | Accepted immutable digest is running. |
| `RUNTIME_ACTIVE` | Intended workers/readers/writers are fresh and execute at declared authority. |
| `CONTROLLED_FUNCTIONAL_PROOF` | Deterministic safe fixture proves nominal mechanics without being called natural operation. |
| `CONTROLLED_FAILURE_PROOF` | Failure injection proves fail-closed behavior and rollback boundaries. |
| `NATURAL_OPERATIONAL_E2E_PROOF` | A natural normal-path event on the deployed environment proves the actual end-to-end path and declared result. |
| `END_TO_END_OBSERVED` | PASS only when required natural normal-path proof exists; controlled proofs remain separately linked. |

Mandatory keys also include capability/version, environment, service, SHA,
digest, runtime revision, schema/config fingerprints, authority, evidence IDs,
timestamp, gate result and approver. States are `PRESENT_NOT_ACTIVE`,
`ACTIVE_NOT_OBSERVED`, `ACCEPTED`, `NOT_APPLICABLE`, `FAILED`. `ACTIVE` or
`ACCEPTED` requires `NATURAL_OPERATIONAL_E2E_PROOF=PASS` for the normal path.
Failure paths may and should use `CONTROLLED_FAILURE_PROOF`; controlled proof
cannot impersonate natural operation.

The ledger covers four strategies, regime worker/gate/policy/watchdog,
Realtime Engine, Slot Brain, ORC, Movement, Economic Floor/evidence collectors,
profit lock/exit guards, Fee V2, Financial Truth, opportunity/outcome evidence,
Learning, Risk Budget, Portfolio State, Capital Allocation, Managed Capital,
automation, API and UI. Historical experiment completion does not prove current
activation.

## 8. Definition of done

`DEFINITION_OF_DONE_COMPLETE=YES` for the complete repair contract. LOCAL
PAPER MR1/MR2 technical acceptance is complete; deferred four-environment
items and independent VPS PAPER acceptance remain open.
The repair program completes only when:

1. All five P0s close with failure-injection and end-to-end proof.
2. All nine P1s function at intended authority in each applicable environment;
   OFF is valid only when it is the declared approved contract.
3. All seven P2 gaps have immutable, queryable evidence.
4. P3 stays backlog unless a direct repair dependency requires it; no broad or
   destructive cleanup occurs.
5. Every applicable capability has an accepted ledger row and all four gates
   pass.
6. LIVE entry freeze remains active; exits, reconciliation, managed equity,
   Financial Truth, API/UI and monitoring remain available.
7. Direct PAPER enforcement follows the exact canonical rollout sequence in
   Section 3B and requires separate LOCAL PAPER and VPS PAPER acceptance after
   separate approval. LIVE enforcement is not implied.
8. No holdout efficacy is inspected, reset, relabeled or used to tune repair.

## 9. North Star and Equity Curve First

- `CAPITAL_FIRST`: entry uncertainty denies authority; LIVE freeze is invariant.
- `EQUITY_CURVE_FIRST`: economic research waits for reliable paths, costs,
  Financial Truth and provenance.
- `NET_AFTER_ALL_COSTS`: Fee V2 and COMPLETE Financial Truth remain dependencies.
- `EVIDENCE_BEFORE_AUTHORITY`: end-to-end evidence precedes PAPER enforcement;
  LIVE needs later explicit approval.
- `REUSE_BEFORE_BUILD`, `REWIRE_BEFORE_REWRITE`: reuse `market_regime`, atomic
  entry, evidence IDs, controls and migrations; no new engine.
- `HUMAN_APPROVAL`: each authority change, Wave 5 and any LIVE decision require
  Product Owner approval.
- `LOCAL_VPS_PARITY`: LOCAL promotes shared contracts; VPS independently
  accepts them; natural business data stay separate.

`SHADOW_REQUIRED=NO_UNLESS_NEW_CONCRETE_SAFETY_REASON_IS_PROVEN`.

`NEXT_ACTION=VPS_PAPER_INDEPENDENT_PARITY_AND_DIRECT_ACCEPTANCE`.

# Learning / Promotion Effectiveness Audit V1

Audit date: 2026-07-16 UTC

## 1. Executive verdict

**E. AUDIT BLOCKED — INSUFFICIENT CAUSAL TELEMETRY**

The Learning and Promotion pipelines are operational, deterministic, and
shadow-only, but their economic effectiveness cannot currently be proven or
disproved causally.

The decisive finding is:

```text
LOCAL LIVE decision_registry_v1:  0 / 3,043 decisions linked by recommendation_id
LOCAL PAPER decision_registry_v1: 0 / 9,117 decisions linked by recommendation_id
```

Recommendation/proposal keys exist in Learning tables, but they are not
persisted on subsequent decisions. There is no immutable activation record
that binds a frozen recommendation version, evidence cutoff, expiry, baseline
policy, later decision, and outcome. Time-and-slot matching is possible only as
descriptive analysis and is not causal proof.

Pipeline health therefore remains distinct from learning effectiveness.

## 2. Git pre-state

- branch: `main`
- HEAD: `2648370b379f811a2d4398debbde47a098ddbc61`
- origin/main: `2648370b379f811a2d4398debbde47a098ddbc61`
- tracked worktree at audit start: clean
- no pre-existing untracked artifacts were modified

This report is the only repository change produced by the audit.

## 3. Learning object inventory

The actual schemas were inspected through `information_schema`, `pg_views`,
`pg_proc`, and table definitions in both LOCAL LIVE and LOCAL PAPER.

| Object | Environment | Key and time | Main evidence | Consumer |
|---|---|---|---|---|
| `decision_registry_v1` | LIVE/PAPER | `decision_id`; `decision_timestamp` | decision source/action/payload, regime, optional `recommendation_id` | outcomes and audit views |
| `decision_outcomes_v1` | LIVE/PAPER | `outcome_id`, `decision_id`; `calculated_at` | gross, fees, net, MFE, MAE, giveback, actual-trade flag | decision intelligence |
| `decision_replay_v1` | LIVE/PAPER | `decision_key`; entry/exit timestamps | replay vector and entry/MME/exit/learning contexts | Feature Warehouse |
| `learning_feature_warehouse_v1` | LIVE/PAPER | `decision_key`; entry/exit timestamps | net/gross/fees, regime, MFE-context status, fee pressure | Learning recommendations |
| `learning_feedback_shadow_recommendations` | LIVE/PAPER | `decision_key`, position | action, confidence, priority, evidence, creation time | feedback aggregation |
| `learning_slot_statistics_v1` | LIVE/PAPER | environment + slot + window | sample range, decisions, net, PF, expectancy, coverage | calibration proposals |
| `learning_calibration_proposals_v1` | LIVE/PAPER | `proposal_key`; first/last/refreshed | action, confidence, evidence sample/PnL/PF, approval/apply fields | validation |
| `learning_proposal_observations_v1` | LIVE/PAPER | run + `proposal_key`; `observed_at` | repeated proposal snapshots | validation state |
| `learning_proposal_validation_state_v1` | LIVE/PAPER | environment + slot + window | action/status, sequence, confidence/sample/span gates | V1.4 shadow confidence |
| `learning_shadow_confidence_proposals_v1` | LIVE/PAPER | proposal/source keys | proposed confidence delta, ACTIVE/SUPERSEDED/EXPIRED | shadow audit only |
| `learning_feedback_refresh_runs_v1` | LIVE/PAPER | run ID and timestamps | engine version, thresholds, status | automation observability |
| `learning_shadow_confidence_runs_v1` | LIVE/PAPER | run/source refresh | status and `apply_enabled=false` | automation observability |
| `slot_brain_snapshot` | LIVE/PAPER | slot + window + calculated time | net, fees, PF, MFE/MAE, drawdown-adjacent scores | analytics/ORC inputs |
| MME tables/views | LIVE/PAPER | event/context timestamps | readiness, sequence, opportunity, ranking | ORC context |
| ORC Integration V2 views | LIVE/PAPER | slot at current view state | eligible/picked/context state | authoritative automation apply on LIVE |
| `promotion_events` | LIVE | event ID, hash, source/created timestamps | transport hash, version, row count | bridge audit |
| `promoted_candidates` | LIVE | slot | paper score/sample/net, eligibility, version | legacy promotion consumers |
| `promoted_regime_candidates` | LIVE | slot + regime | net/PF/fees/sample/eligibility | regime bootstrap view |

PAPER publishes promotions to LIVE; promotion tables are not present in the
PAPER database.

## 4. Recommendation lifecycle

Observed lifecycle:

```text
position/trade
→ decision_registry_v1 / decision_outcomes_v1
→ decision_replay_v1
→ learning_feature_warehouse_v1
→ learning_feedback_shadow_recommendations
→ learning_slot_statistics_v1
→ learning_calibration_proposals_v1
→ learning_proposal_observations_v1
→ learning_proposal_validation_state_v1
→ learning_shadow_confidence_proposals_v1 (eligible action classes only)
```

Separately:

```text
PAPER slot/regime performance
→ automation-runner promotion payload
→ LIVE internal upsert
→ promotion_events + promoted candidate tables
→ observer/legacy allocator analytics and regime bootstrap view
```

Idempotency exists at refresh/run/proposal/hash levels. Validation resets when
the proposal action changes or requirements cease to hold. V1.4 supports
ACTIVE/SUPERSEDED/EXPIRED shadow proposals and constrains apply to false.

The missing transition is:

```text
frozen recommendation/proposal
→ explicit activation/exposure
→ later decision carrying recommendation/version/arm
→ counterfactual and actual outcome
```

## 5. Causal linkage assessment

`decision_registry_v1.recommendation_id` is the only obvious recommendation
link on future decisions, and it is empty in both environments.

Additional blockers:

- proposal keys are not recorded on future decisions;
- validation state has no regime dimension;
- no immutable evidence cutoff is persisted with an activation;
- `stable_at` records validation stability, not runtime activation;
- `approved_at` and `applied_at` are NULL for current proposals;
- no experiment/control-arm identifier exists;
- no baseline policy version is frozen per recommendation exposure;
- no counterfactual outcome ledger exists for blocked decisions.

Result: every current recommendation is **NOT_EVALUABLE causally**.

## 6. Experimental unit

Current validation and calibration use:

```text
environment + strategy + symbol + interval + window_days
```

Feature evidence includes `market_regime`, but validation state does not.
Therefore regime-specific effectiveness must not be inferred from current
slot-level validation. A future regime-specific recommendation requires:

```text
environment + strategy + symbol + interval + market_regime
```

with the regime frozen at decision time.

## 7. Recommendation class semantics

| Class | Current semantics | Runtime effect | Expected effect | Main failure mode |
|---|---|---|---|---|
| `OBSERVE` | retain evidence only | none | learn without intervention | no economic test |
| `BLOCK_CANDIDATE` | negative-edge proposal | none | avoid future negative net expectancy | false block of recovered edge |
| `REDUCE_CONFIDENCE` | proposed negative confidence delta | none | reduce loss/drawdown/fees | underexposure to recovery |
| `INCREASE_CONFIDENCE` | proposed positive confidence delta | none | increase profitable exposure | amplified drawdown/fees |
| `PROMOTE_CANDIDATE` | positive-edge promotion concept | no Learning apply; bridge is separate | admit proven edge | false promotion or regime drift |

`OBSERVE_ONLY`, `VALIDATING`, `STABLE`, `RESET`, `ACTIVE`,
`SUPERSEDED`, and `EXPIRED` are lifecycle states. `STABLE` is not LIVE
authorization.

## 8. Evidence and forward-window methodology

For a causally evaluable future record:

1. freeze recommendation key/version and `evidence_cutoff`;
2. exclude every decision/outcome at or before the cutoff from forward proof;
3. record activation, expiry, reset, and policy arm;
4. include only decisions generated after activation and before expiry;
5. join outcomes by `decision_id`;
6. retain regime and environment boundaries;
7. score only completed or explicitly censored forward outcomes.

Current proposal evidence uses a 30-day aggregation updated repeatedly.
`stable_at` cannot reconstruct the exact rows used by every prior observation.

## 9. Baseline definition

Preferred baseline order:

1. actual policy/version immediately before activation;
2. concurrently persisted shadow control decision;
3. prior version frozen at activation;
4. matched untreated slot/regime/window;
5. otherwise `NOT_EVALUABLE`.

No current activation record freezes any of these baselines. Always-on and
always-off are not accepted substitutes.

## 10. Counterfactual methodology

- `BLOCK_CANDIDATE`: retain would-trade decisions and simulate only entries
  occurring after activation under unchanged execution/fees.
- `REDUCE_CONFIDENCE`: requires a real, versioned mapping from confidence to
  sizing/exposure. Without it, report direction only.
- `INCREASE_CONFIDENCE`: same sizing requirement; otherwise do not invent
  position sizes.
- `PROMOTE_CANDIDATE`: compare promoted eligibility against a frozen baseline
  decision stream using contemporaneous candles, fees, regime, and execution
  constraints.

The present schema cannot perform these counterfactuals reliably.

## 11. Metric definitions

Required per baseline and candidate:

- decision/trade counts;
- gross PnL, fees, net PnL;
- net expectancy, PF, win rate, average win/loss;
- path-based max drawdown, downside deviation, return/drawdown;
- turnover and fee pressure;
- MFE, MAE, capture, giveback;
- hold and exposure time;
- false-block and false-promote rates.

Gross, fees, net, MFE, MAE, giveback, and hold time exist for many completed
trades. Reliable policy-arm drawdown, exposure, turnover, false-block, and
false-promote are `DATA_UNAVAILABLE` without causal exposure records.

## 12. Sample sufficiency

Configured validation gates are:

```text
required observations = 3
required span = 24 hours
minimum action sample = 30
minimum confidence = 0.60
```

They are explicit and deterministic but are heuristic thresholds, not
demonstrated statistical guarantees. The code/tests prove enforcement, not
calibration.

Future calibration should use bootstrap intervals for net expectancy/PF,
Bayesian posterior probability of positive after-fee edge, and stability
across walk-forward windows and regimes.

## 13. Walk-forward results

Available trade counts:

| Environment | 3d | 7d | 14d |
|---|---:|---:|---:|
| LOCAL LIVE | 0 | 4 | 26 |
| LOCAL PAPER | 273 | 576 | 1,273 |

LIVE cannot support the requested 3d/1d, 7d/2d, or 14d/3d evaluation at slot
level. PAPER has enough aggregate trades, but historical frozen
recommendations/arms are missing. Thus all formal walk-forward effectiveness
results are `NOT_EVALUABLE`.

The future method should run:

```text
3d evidence → 1d frozen forward
7d evidence → 2d frozen forward
14d evidence → 3d frozen forward
```

and reject a window when the slot/regime sample is below its predeclared
minimum.

## 14. LOCAL LIVE assessment

- 3,043 decisions and 3,043 outcomes exist.
- zero decisions carry a recommendation link;
- only seven validation states exist, all `OBSERVE_ONLY/OBSERVE`;
- no shadow-confidence proposals exist;
- recent trading sample is insufficient: zero trades in 3d and four in 7d.

Effectiveness is not evaluable.

## 15. LOCAL PAPER assessment

- 9,117 decisions, 9,112 outcomes, 655 replay rows, 1,997 feature rows, and
  1,999 shadow recommendations exist;
- zero decisions carry a recommendation link;
- 17 current validation states include four STABLE BLOCK candidates;
- V1.4 shadow-confidence table currently contains zero proposals because
  V1.4 accepts confidence actions, while current STABLE states are blocks;
- auto-apply remains off.

Descriptive outcomes after action-first-observed timestamps:

| Slot | Action | Trades | Net PnL | PF | Status |
|---|---|---:|---:|---:|---|
| BTCUSDC 1m BBRANGE | BLOCK | 16 | -0.203 | 0.709 | direction supports block, not causal |
| SOLUSDC 1m BBRANGE | BLOCK | 73 | -1.484 | 0.565 | direction supports block, not causal |
| BTCUSDC 1m RSI | BLOCK | 44 | -0.848 | 0.381 | direction supports block, not causal |
| BTCUSDC 1m TREND | BLOCK | 12 | -0.282 | 0.466 | direction supports block, not causal |
| BNBUSDC 1m BBRANGE | INCREASE | 11 | -0.085 | 0.776 | direction contradicts increase |
| ETHUSDC 5m BBRANGE | INCREASE | 12 | +0.174 | 1.492 | direction supports increase |
| SOLUSDC 5m BBRANGE | REDUCE | 13 | +0.129 | 1.342 | direction contradicts reduce |

These are untreated shadow observations. They cannot establish treatment
effectiveness.

## 16. Promotion bridge effectiveness

Transport health is strong:

- 41,217 promotion events;
- 41,217 distinct payload hashes;
- six historical policy versions;
- latest active publisher: `paper_rank_v2`, 10-day window;
- current global promoted candidates: 25, with zero globally eligible;
- regime candidates: five, with three eligible.

Publishing, hash deduplication, acceptance, and current storage are observable.
Regime promotions feed `v_orc_v632_regime_bootstrap_candidates`; promotion
tables are also read by legacy orchestrator analytics. The orchestrator is now
REPORT_ONLY.

No decision records the promotion event/hash/version that affected it.
Therefore transport is proven, consumption paths exist, but economic
effectiveness and even per-decision causal consumption are not proven.

## 17. Current recommendation cohorts

PAPER:

- STABLE `BLOCK_CANDIDATE`: 4 slots;
- VALIDATING `INCREASE_CONFIDENCE`: 2 slots;
- VALIDATING `REDUCE_CONFIDENCE`: 1 slot;
- RESET `REDUCE_CONFIDENCE`: 1 slot;
- OBSERVE_ONLY: 9 slots;
- `PROMOTE_CANDIDATE`: none.

LIVE:

- OBSERVE_ONLY: 7 slots;
- actionable/stable classes: none.

Every cohort has `evaluation_status=NOT_EVALUABLE_CAUSALLY`.

## 18. False-block analysis

Formal definition:

```text
FALSE_BLOCK =
activated block with sufficient forward sample whose untreated counterfactual
has durable positive after-fee expectancy and acceptable drawdown.
```

Suggested future minimum: at least 30 eligible decisions and 15 closed
counterfactual trades, lower confidence bound of net expectancy above zero,
PF above 1.05, and stability in at least two forward subwindows/regimes.

Current false-block rate: `DATA_UNAVAILABLE`. Descriptive post-timestamp data
for all four current block slots remains negative, but no block was activated
and no control/treatment counterfactual was recorded.

## 19. False-promote analysis

Formal definition:

```text
FALSE_PROMOTE =
activated promotion/increase with sufficient forward sample whose after-fee
expectancy is negative or whose drawdown exceeds the predefined budget.
```

Suggested future gate: same sample minimum, posterior/interval evidence of
positive net expectancy, PF above 1.05, bounded drawdown, and regime
persistence.

Current false-promote rate: `DATA_UNAVAILABLE`. There are no current
`PROMOTE_CANDIDATE` proposals. The two validating increase candidates have
opposite descriptive forward directions.

## 20. LOCAL versus VPS comparison plan

No VPS action was performed. A later read-only comparison must align:

- ORC candidates, `eligible_v63`, picked/core/want-on;
- bot-control enabled/live flags;
- Slot Brain scores and sample windows;
- MME context and regime distributions;
- slot/regime performance;
- promoted candidates and versions;
- latest decision/trade timestamps;
- signal/block-reason distributions.

VPS reports alone are not a substitute for the underlying dataset.

## 21. Best BLOCK_CANDIDATE candidate

For a future experiment, the strongest current candidate is:

```text
LOCAL PAPER / SOLUSDC / 1m / BBRANGE
proposal_key = 426b7d19dcc6d2096449564d4d0bdd2d
status = STABLE
confidence = 0.99
evidence decisions = 104
evidence net PnL = -2.421 USDC
evidence PF = 0.532
descriptive post-action-first-observed:
73 trades, -1.484 USDC, PF 0.565
```

This is a candidate for methodology design only. It must not be activated
until causal telemetry exists.

## 22. PAPER BLOCK_CANDIDATE EXPERIMENT V1

Proposed contract:

- exactly one LOCAL PAPER slot;
- immutable experiment ID and frozen proposal key/version;
- activation/evidence cutoff and automatic expiry;
- baseline arm remains the actual unchanged decision stream;
- treatment arm suppresses entry execution only in an isolated PAPER
  experiment gate;
- retain every would-trade decision and compute its counterfactual outcome;
- maximum 14 days or 30 eligible decisions, hard maximum 21 days;
- global Learning auto-apply remains off;
- no LIVE writes, no other-slot changes, no sizing/parameter changes;
- kill switch on missing heartbeat, schema/version drift, stale proposal,
  missing audit, real-execution attempt, or counterfactual gap.

PASS requires avoided negative net PnL without material positive opportunity
cost. FAIL occurs when the blocked slot develops durable positive forward edge
or when causal evaluation is incomplete.

The experiment is currently blocked because counterfactual outcomes for
blocked entries are not durably linked to an experiment/recommendation.

## 23. Required telemetry gaps

Required before any experiment:

1. immutable recommendation/exposure table with ID, version/hash, environment,
   slot/regime, evidence cutoff, activation, expiry, reset, baseline version;
2. `recommendation_id`, experiment ID, and arm on every future decision;
3. would-trade decision retained when treatment blocks execution;
4. counterfactual outcome linked by decision ID;
5. frozen sizing/execution assumptions or direction-only classification;
6. regime-specific validation key when recommendation semantics are
   regime-specific;
7. policy/promotion event/hash consumed by each ORC decision;
8. complete fee, turnover, exposure, and equity-path data per arm.

## 24. Existing test coverage

Existing tests cover:

- Learning V1.4 shadow mode and apply-off constraint;
- allowed confidence actions and bounded deltas;
- idempotent refresh and superseding;
- scheduler observability and not-due behavior;
- absence of bot-control apply paths;
- canonical environment contracts;
- decision identity basics.

## 25. Missing test matrix

Required future tests:

- recommendation ID propagated to future decisions/outcomes;
- evidence cutoff excludes training rows from forward scoring;
- regime-specific key isolation;
- activation/expiry/reset state machine;
- baseline/treatment arm identity;
- blocked would-trade and counterfactual outcome persistence;
- fee/net/drawdown metric correctness;
- false-block/false-promote classification;
- rolling walk-forward with embargo;
- promotion publish/accept/consume linkage;
- promotion expiry/superseding;
- LOCAL PAPER/LIVE and LOCAL/VPS isolation;
- kill switch and no-real-execution guarantees.

## 26. Risks and limitations

- descriptive forward losses can be mistaken for causal effectiveness;
- current 30-day evidence windows overlap with later refreshes;
- validation aggregates regimes;
- current thresholds are heuristic;
- promotion event volume proves transport, not value;
- LIVE inactivity prevents meaningful recent validation;
- blocked counterfactuals are not persisted;
- no VPS dataset was inspected.

## 27. Recommended next action

Implement causal telemetry only—without auto-apply—then collect a frozen
forward PAPER cohort. Re-run this audit before activating the single-slot
experiment. Do not change runtime policy based on the descriptive results in
this report.

## 28. Explicit no-change confirmation

During this audit:

- no production code, strategy, ORC, Learning, sizing, execution, or runtime
  parameter was changed;
- no database write SQL or migration was executed;
- `bot_control` was not modified;
- auto-apply remained off;
- no service was restarted or rolled out;
- no order was placed;
- no VPS action was performed;
- no commit or push was performed.

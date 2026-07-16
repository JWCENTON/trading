# Control Plane Authority & Stability Audit V1

Audit date: 2026-07-16 UTC
Scope: LOCAL LIVE and LOCAL PAPER
Repository candidate: `9a818392904fc0b7bbf9da29139ff06039a9e736`

## 1. Executive verdict

**AUDIT BLOCKED — conflicting control-plane writers**

LOCAL LIVE has two active, independent materializers of the same `bot_control`
fields:

1. `automation-runner.run_orc_v5_apply()` computes desired state from
   `v_orc_integration_v2_picks` under the `ORC_V6_3` /
   `COOLDOWN_PROMOTE_HYSTERESIS` labels.
2. `bot-runner-orchestrator.apply_v2_enforce()` independently computes desired
   state from `v_orc_integration_v2_picks`/profit-first inputs under
   `orc_v2_mode=ENFORCE`.

Both write `live_orders_enabled`, `regime_enabled`, `regime_mode`, `reason`,
`live_since`, and `last_disabled_at`. Only automation-runner also normalizes
`control_mode/control_source` and clears manual override metadata. There is no
shared generation, fencing token, compare-and-swap rule, cross-process
cooldown, or explicit precedence between these paths.

The conflict is active, not theoretical:

- 8,051 LIVE audit rows in 24h and 65,657 in 7d;
- 4,016 audit rows with `ORC_INTEGRATION_V2:*` reasons and 4,008 with
  `ORC_V2:*` reasons in 24h;
- 7,989 cross-writer reason flips in 24h, average separation about 4m59s;
- 28 OFF→ON and 28 ON→OFF transitions in 24h;
- automation-runner reports repeated `touched_off=27/28`;
- orchestrator subsequently reports `[v2] ENFORCE ... off=27`;
- `orc_writer_primary=V5` fences only automation-runner implementations. The
  orchestrator v2 writer does not check it.

The current state happens to converge mostly to entries OFF, but this is
last-writer-wins convergence rather than deterministic authority.

Learning Engine remains SHADOW and all observed apply flags are `0`. A
Learning Promotion Effectiveness Audit can be built safely, but no promotion
experiment should mutate `bot_control` until control-plane authority is made
single and fenced.

## 2. Git pre-state

- branch: `main`
- HEAD: `9a818392904fc0b7bbf9da29139ff06039a9e736`
- origin/main: `9a818392904fc0b7bbf9da29139ff06039a9e736`
- tracked worktree at audit start: clean
- untracked artifacts at audit start: none reported
- latest eight commits start with:
  - `9a81839 Define canonical OKX environment for local paper`
  - `6d4e5e9 Preserve position quantity on partial exits`
  - `2d4b0ab Integrate FinalDecision into BBRANGE`

This report is the only intended repository change from the audit.

## 3. Writer inventory

### 3.1 Automation-runner ORC materializer

- Component: `automation-runner`
- File/function: `automation_runner/main.py::run_orc_v5_apply`
- Trigger: automation loop, hard env flag `ORC_V5_APPLY_ENABLED=1`, KV
  `orc_v5_apply_enabled=1`, interval KV/default.
- Active policy labels: `ORC_V6_3`, `COOLDOWN_PROMOTE_HYSTERESIS`.
- Active view: `v_orc_integration_v2_picks`.
- Writes:
  - `live_orders_enabled`
  - `regime_enabled=true`
  - `regime_mode=ENFORCE|DRY_RUN`
  - `reason`
  - `control_mode=AUTO`
  - `control_source=ORC`
  - clears manual override fields
  - `live_since`, `last_disabled_at`, `updated_at`
- Conditions: LIVE only, AUTO rows only, fixed 32-slot universe, primary-writer
  KV accepted.
- Idempotency: SQL `IS DISTINCT FROM` includes reason and metadata. It is
  idempotent only relative to its own desired representation.
- Throttle: local due timestamp `orc_v5_apply_last_ts_s`; no DB advisory lock.
- Decision identity: stores a `picks_hash` in stats, but the hash is not written
  to `bot_control` and is not used as a fencing generation.
- Transaction: one connection transaction, explicit `conn.commit()`.
- Audit attribution: database trigger records `changed_by=current_user`
  (`botuser`), so it cannot distinguish this process from orchestrator.

### 3.2 Bot-runner-orchestrator v2 materializer

- Component: `bot-runner-orchestrator`
- File/functions:
  - `services/bot_runner_orchestrator/main.py::run_orc_v2_profit_first`
  - `apply_v2_enforce`
  - `_v2_apply_bot_control`
- Trigger: orchestrator poll loop; LIVE KV `orc_v2_mode=ENFORCE`;
  `orc_v1_actions_enabled=true`.
- Active policy: `v2_profit_first`.
- Inputs: v2 best/profitability rows and pick-set SSOT.
- Writes:
  - `live_orders_enabled`
  - `regime_enabled=true`
  - `regime_mode=ENFORCE|DRY_RUN`
  - `reason`
  - `live_since`, `last_disabled_at`, `updated_at`
- Does not set/check `control_mode` and does not check manual override mode in
  `_v2_apply_bot_control`.
- Cooldown: `min_live_hours` and `min_off_hours`, calculated locally from row
  timestamps. It does not coordinate with automation-runner's due gate.
- Idempotency: `IS DISTINCT FROM`, including reason. A reason written by the
  other writer makes the same boolean state non-idempotent.
- Decision identity: per-slot `orc_v2_last_fp:*` exists in `automation_kv` for
  decision-log deduplication, but is not a bot-control generation/fence.
- Transaction: entire orchestrator tick on one transaction, commit after
  `run_orchestrator_v1`.

### 3.3 Orchestrator safety/v1 writer paths

The same service also contains older safety and allocation update helpers:

- `set_live_orders_enabled`
- `set_entries_and_regime`
- `SQL_DISABLE_LIVE_ORDERS`
- allocator/safety updates around lines 752–789

These are real mutation paths, not merely compatibility labels. Their use is
guarded by the v1 policy/actions flow. They write subsets of the same state and
use `reason` for protocol signaling.

### 3.4 Automation failsafe

`automation_runner.main.disable_live_orders()` is a disable-only global writer:

- writes `live_orders_enabled=false`, `reason`, `updated_at`;
- never enables;
- safety semantics are legitimate but no formal precedence/generation is
  recorded against ORC materializers.

### 3.5 API/UI writers

`api/main.py` exposes authenticated administrative writers:

- `/ui/control/slot`: `enabled`, `live_orders_enabled`, `reason`;
- `/ui/control/slot/manual`: all main fields plus
  `control_mode=MANUAL`, `control_source=USER`;
- `/ui/control/slot/auto`: returns ownership to AUTO/ORC;
- `/ui/control/regime`: `regime_enabled`, `regime_mode`, `reason`.

UI actions are separately recorded in UI audit data. The manual endpoint
creates an intended precedence boundary, but orchestrator v2 does not filter
`control_mode=MANUAL`; therefore the boundary is incomplete.

### 3.6 Strategy processes

All four strategies call `common.bot_control.upsert_defaults()` at startup.
Its conflict branch is deliberately disabled with `WHERE 1=0`, so it inserts a
missing row but does not overwrite an existing row.

All four also contain `set_mode()` helpers that can update `mode`, `reason`,
and `updated_at` for HALT/panic behavior. This is a separate safety/runtime
field, but it shares the unversioned `reason` column with ORC policy writers.

### 3.7 Database/migrations and audit trigger

Migrations define `bot_control`, timestamps, control metadata, and
`bot_control_audit`. The audit table contains:

- `changed_at`, `changed_by`, slot key;
- full `old_row` and `new_row`.

It lacks component identity, policy generation, writer instance, request ID,
decision hash, or causal parent.

## 4. Actual authority graph

```text
v_orc_integration_v2_picks
  ├─> automation-runner / ORC_V6_3
  │     └─> independent desired-state calculation
  │           └─> bot_control mutation
  └─> orchestrator v2/profit-first inputs
        └─> independent pick/profit/cooldown calculation
              └─> bot_control mutation

promoted_candidates / promoted_regime_candidates / risk state
  └─> orchestrator v1 safety and gating
        └─> decisions and additional bot_control mutation paths

admin API
  └─> manual/AUTO state mutation

bot_control
  └─> bot-runner desired process set
        └─> strategy processes read enabled/live/regime/mode
              └─> permissions and execution gating
```

### Field ownership observed

| Field | Current writers | Deterministic owner |
|---|---|---|
| `enabled` | API/manual, historical operations | API/operator intended |
| `live_orders_enabled` | automation ORC, orchestrator v2/v1, failsafe, API | none |
| `regime_enabled` | automation ORC, orchestrator v2/v1, API | none |
| `regime_mode` | automation ORC, orchestrator v2/v1, API | none |
| `control_mode` | automation ORC, API | intended API/AUTO boundary, incomplete |
| `control_source` | automation ORC, API | descriptive, not enforced by v2 |
| `reason` | all writers and strategy `set_mode` | none; overloaded |

There is no explicit precedence between the two active automatic materializers.
Both can compute different results because their gates, profitability inputs,
cooldowns, universes, and reason protocols differ. Restart can replay whichever
writer runs first. The persisted timestamps reduce some boolean flapping, but
reason differences themselves trigger repeated updates.

## 5. Policy/version semantic map

| Name | Proven meaning |
|---|---|
| `orchestrator_v1` | logging/runtime component label; service contains active v1 safety plus v2 materializer |
| `policy_version=orc_v1` | real v1 safety policy configuration read from `automation_kv`; not cosmetic |
| `[v2] ENFORCE` | active mutation protocol in orchestrator, applying profit-first pick state |
| `ORC_V6_3` | automation-runner active apply/version label and pick-source label |
| `ORC Integration V2` | active view/reason/materialization family using MME/context readiness |
| `v_orc_integration_v2_picks` | active database view selected by automation-runner |
| `orc_v5_apply` | backward-compatible function/KV namespace; currently materializes ORC_V6_3 |
| `orc_v5_db_apply` | historical active-policy label in KV, not sufficient to identify current calculation |

The names mix component generation, view generation, policy generation,
compatibility namespace, and runtime protocol. They must not be compared as a
single version number.

## 6. Database mutation evidence

### LOCAL LIVE

- Audit range: 660,394 rows from 2026-01-06 through audit time.
- Last 24h: 8,051 rows.
- Last 7d: 65,657 rows.
- Last 24h transitions:
  - OFF→ON: 28
  - ON→OFF: 28
  - unchanged `live_orders_enabled`: 7,995
- Last 7d:
  - OFF→ON: 236
  - ON→OFF: 237
  - same boolean: 65,184
- Typical enabled slot: 290–297 audit writes in 24h.
- Reason families in 24h:
  - `ORC_INTEGRATION_V2`: 4,016
  - `ORC_V2`: 4,008
- Cross-family consecutive flips: 7,989.
- Both writers use DB actor `botuser`, so source separation requires reason/time
  inference.
- 56 audit rows occurred near the latest automation/orchestrator restart
  interval; restart replay is possible and not fenced.

Example:

1. automation-runner sets 27 slots to
   `ORC_INTEGRATION_V2: ... OFF, DRY_RUN`;
2. orchestrator changes the same slots to
   `ORC_V2: not picked (entries OFF, DRY_RUN)`;
3. next automation due run sees a different reason and touches them again.

This is redundant write oscillation even where the boolean state agrees. When
pick sets disagree, the same mechanism can become semantic ON/OFF oscillation.

### LOCAL PAPER

- Audit range: 204 rows total.
- No writes in the last 24h or 7d.
- Current state is PAPER-safe and mostly historical/manual/failsafe.
- `orc_v1_actions_enabled=false`.

PAPER is not currently exhibiting the dual-writer loop.

### Stability classification

**D. TWO_UNCOORDINATED_WRITERS**

- ON→OFF and OFF→ON: potentially genuine conflicting decisions; current
  evidence contains paired transitions.
- repeated `touched_off`: redundant resynchronization caused by different
  reason protocols, not healthy idempotency.
- `skipped_by_cooldown`: local orchestrator hysteresis only; automation-runner
  can still rewrite the row during the cooldown.
- restart replay: possible because no generation/fence is persisted.

## 7. Restart and concurrency analysis

- Automation-runner's `orc_writer_primary` is not a distributed lock. It is a
  string allowlist and the orchestrator does not participate.
- Both services use normal row updates, so PostgreSQL serializes individual
  conflicting statements, but the later transaction wins.
- There is no stale-generation rejection.
- There is no shared advisory lock.
- There is no common decision table consumed by a single materializer.
- There is no atomic “decision generation N may update only generation < N”
  condition.
- Process restart can recalculate from its own inputs and overwrite the other
  process's newer representation.
- Manual override is protected in automation-runner through `control_mode`,
  but not demonstrably protected in orchestrator v2.

Recommended architecture: one materializer owns mutable control fields.
Other components publish versioned recommendations/decisions only. Safety
disable-only paths may remain separate only with explicit higher precedence and
monotonic/fenced semantics.

## 8. Existing test coverage

Found tests cover Learning Engine contracts:

- SHADOW mode and apply disabled;
- due gate;
- idempotent proposal refresh;
- proposal superseding;
- no writes to `bot_control`, strategy params, or runtime params;
- allowed confidence actions and bounds.

No dedicated tests were found for:

- automation ORC bot-control apply;
- orchestrator v2 enforce;
- writer-primary fencing across both processes;
- simultaneous writer execution;
- restart recovery;
- shared cooldown;
- stale generation;
- manual override preservation against v2;
- source/reason audit consistency.

### Missing test matrix

| Case | Required assertion |
|---|---|
| two writers concurrently | exactly one may materialize; loser publishes recommendation only |
| restart during OFF→ON | committed newer generation survives restart |
| stale generation after new | update rejected |
| identical apply twice | second apply produces zero audit rows |
| conflicting decisions | precedence is explicit and logged |
| cooldown with two components | one shared cooldown/fence governs both |
| manual override | no automatic writer changes protected fields |
| safety disable vs allocator enable | safety precedence wins monotonically |
| reason/source audit | component, policy, generation, decision hash preserved |
| transaction failure | no partial bot-control state |

## 9. TIME_EXIT configuration inventory

### Hard gate

`common/adaptive_time_exit.py` makes hard time exits OFF by default.

- LIVE runtime: `TIME_EXIT_FORCE_ENABLED=0`
- LIVE policy: `TIME_EXIT_POLICY=DIAGNOSTIC_ONLY`
- PAPER has no force-enable env and therefore also defaults OFF.

Thus the minute values are currently diagnostic/dormant unless the hard flag is
changed.

### Code defaults

- RSI: 450 minutes, `TIME_EXIT_ENABLED=1` soft parameter.
- TREND: 90 minutes.
- SUPERTREND: 90 minutes.
- BBRANGE: 90 minutes.

Effective hard enable is the conjunction of strategy/local config and
`hard_time_exit_enabled()`.

### DB slot values

The active legacy `strategy_params` layer contains slot overrides:

- LIVE `ETHUSDC RSI 1m = 5`, but PAPER equivalent is 450.
- `BNBUSDC RSI = 450` for 1m and 5m.
- most TREND slots = 90;
- `BTCUSDC TREND 1m = 180`;
- `BTCUSDC RSI 1m = 60`;
- `BTCUSDC BBRANGE 1m = 180`.

No TIME_EXIT entries were found in the newer
`strategy_runtime_config`/`strategy_symbol_interval_config` tables.

### Precedence

1. code/env default initializes the module value;
2. `strategy_params` per symbol/strategy/interval overrides it during runtime
   parameter load;
3. strategy-specific clamps apply;
4. the hard `TIME_EXIT_FORCE_ENABLED` flag decides whether the exit is active.

The unusual LIVE ETH RSI 1m value of 5 is an old DB override dated
2026-01-11, not a code default and not mirrored in PAPER. There is no nearby
policy metadata proving intent. Classify it as **historical and unverified**,
not as a confirmed deliberate current policy. It is non-operative while hard
time exit remains disabled.

Recommended next audit: counterfactual exit analysis by slot comparing actual
exit with exits at 5/60/90/180/450 minutes using net PnL after fees, MFE, MAE,
giveback, expectancy, and exposure duration. Do not enable TIME_EXIT based on
raw gross-PnL comparison.

## 10. Learning Promotion Effectiveness Audit V1 design

### Objective

Evaluate baseline policy versus a versioned proposed policy for:

- `BLOCK_CANDIDATE`
- `REDUCE_CONFIDENCE`
- `INCREASE_CONFIDENCE`
- `PROMOTE_CANDIDATE`

without applying recommendations to LIVE or contaminating training evidence.

### Point-in-time dataset

Every evaluation record must store:

- policy candidate ID/version/hash;
- slot and environment;
- evidence cutoff timestamp;
- exact feature/view versions;
- recommendation available at cutoff;
- baseline decision;
- proposed decision;
- forward outcome window boundaries.

Only data whose event/ingest timestamp is at or before the decision cutoff may
enter the evidence set. Late-arriving outcomes belong to later walk-forward
runs, never retroactively to the original decision.

### Windows

Recommended initial design:

- evidence/train window: trailing 28 days, ending at cutoff;
- embargo: one maximum holding horizon plus ingestion-lag allowance;
- forward validation: next 7 days or next 30 closed trades, whichever is later;
- walk-forward: weekly steps over at least 8 windows;
- minimum evidence: 30 decisions and 15 closed trades for confidence actions;
  50 decisions and 25 trades for block/promote actions;
- minimum three independent market-regime buckets where available;
- candidate expiry: 14 days without revalidation;
- action cooldown: 7 days after activation or rollback;
- reset when action changes, feature/policy version changes, evidence decreases,
  confidence falls below threshold, or source data is corrected.

### Counterfactual rules

- `BLOCK_CANDIDATE`: compare actual baseline trades against a counterfactual
  that removes only trades whose entry decision occurred after activation.
- `PROMOTE_CANDIDATE`: simulate proposed entries using contemporaneous candle,
  fee, sizing, liquidity, and execution rules; never use realized future fill
  knowledge.
- Confidence actions: freeze the baseline rank/threshold calculation at cutoff
  and alter only the proposed confidence field.
- Open positions crossing a window boundary are attributed by entry decision
  and marked separately until closed.

### Metrics

- net PnL after fees;
- profit factor;
- expectancy per trade and per unit turnover;
- maximum drawdown;
- trade/decision count;
- turnover and fee pressure;
- MFE capture and MAE;
- false-block rate: blocked trades that would have met positive outcome target;
- false-promote rate: promoted trades failing risk-adjusted threshold;
- stability/sign agreement across walk-forward windows;
- confidence interval/bootstrap uncertainty;
- LOCAL PAPER vs VPS PAPER agreement on recommendation, eligibility, and
  outcome classification.

### Decision gates

PASS requires:

- no look-ahead or cutoff violations;
- required sample and window stability;
- net PnL and expectancy non-inferior after fees;
- drawdown within predefined budget;
- false-block/false-promote below action-specific caps;
- agreement between PAPER environments or a documented data-source cause;
- no material degradation in any protected regime.

## 11. First PAPER experiment: STABLE BLOCK_CANDIDATE

Do not start this experiment until the authority conflict is resolved. The
experiment must not write `bot_control`; it should gate a dedicated PAPER
counterfactual decision stream or an isolated experiment ledger.

### Slot selection

Choose exactly one PAPER slot that:

- has a `STABLE` BLOCK_CANDIDATE validation state;
- has at least 50 evidence decisions and 25 closed trades;
- has negative after-fee expectancy in at least three consecutive
  walk-forward windows;
- has adequate context coverage;
- has no open position at activation;
- is not the only representative of a strategy/regime;
- has matching LOCAL PAPER and VPS PAPER recommendation evidence.

### Experiment contract

- environment: one designated PAPER deployment only;
- policy ID: immutable version/hash;
- baseline arm: unchanged decision stream;
- treatment arm: suppress entries only for the selected slot;
- duration: 14 days or 30 eligible decisions, maximum 21 days;
- full event log with baseline/treatment decision and causal reason;
- no effect on LIVE, ORC thresholds, or general `bot_control`;
- automatic stop/rollback on schema/version drift, missing heartbeat,
  recommendation expiry/change, data lag, unexpected order path, or audit gap.

### PASS

- zero LIVE effect and zero real exchange actions;
- 100% eligible-decision audit coverage;
- baseline counterfactual reproducible;
- treatment avoids negative after-fee expectancy without unacceptable
  false-block rate;
- no worse simulated drawdown;
- result direction stable in at least two subwindows;
- LOCAL/VPS PAPER evidence agrees.

### FAIL

- any authority leakage or real execution;
- missing/corrupt baseline;
- positive-opportunity false-block rate above predefined cap;
- insufficient sample at expiry;
- recommendation instability/reset;
- material disagreement between PAPER environments.

## 12. Risks

1. Boolean state currently appears conservative, but reason churn creates high
   audit volume and masks real semantic transitions.
2. A future disagreement in pick sets can turn representation oscillation into
   execution ON/OFF oscillation.
3. Orchestrator v2 can bypass intended manual ownership metadata.
4. `changed_by=botuser` prevents reliable writer attribution.
5. Multiple version namespaces make operational diagnosis error-prone.
6. Local cooldowns do not create distributed precedence.
7. Learning promotion would add a third authority unless it publishes
   recommendations to a single materializer.
8. TIME_EXIT legacy overrides lack provenance and differ between environments.

## 13. Recommended next action

Prepare a separate, reviewed patch plan—without changing strategy semantics—to:

1. designate one automatic `bot_control` materializer;
2. convert the other ORC paths into versioned recommendation publishers;
3. add a monotonic `generation`/decision hash and stale-generation rejection;
4. enforce MANUAL and safety-disable precedence in one place;
5. add writer component/instance/policy/generation to audit evidence;
6. make reason descriptive rather than part of idempotency state;
7. add the concurrency/restart test matrix above;
8. rerun this audit before any Learning Engine apply experiment.

Until then, keep Learning Engine apply disabled.

## 14. Explicit no-change confirmation

During this audit:

- no trading semantics were changed;
- no strategies or ORC thresholds were changed;
- no `bot_control` values were changed by the auditor;
- no auto-apply was enabled;
- no migrations or write SQL were executed;
- no services were restarted or rolled out;
- no orders or cancels were submitted;
- VPS was not accessed;
- no commit or push was performed.

# WALTRADE RELEASE f4e5e8c

## STATUS: STABILIZATION FREEZE

## CURRENT GOAL: FOUR-ENVIRONMENT PARITY

## NEXT PHASE: OBSERVATION

## NEW ARCHITECTURE: FORBIDDEN

## NEW FEATURES: FORBIDDEN

## ALLOWED: PARITY, VALIDATION, CRITICAL RELEASE BLOCKERS ONLY

## A. RELEASE IDENTITY

Release SHA:

f4e5e8c854143ad461961ce02de89687ffc3c391

Release baseline:

immutable until 4/4 parity or explicit Product Owner cancellation

Current branch:

main

Release name:

WalTrade Observation Foundation Release

Operational roles:

- Jacek = Product Owner
- ChatGPT = Architect
- Codex LOCAL = LOCAL implementer/operator
- Codex VPS = VPS pull-only implementer/operator
- GitHub = single source of truth

## B. WHY THIS RELEASE EXISTS

- Zakończyć synchronizację czterech środowisk.
- Aktywować wiarygodne observation-only evidence.
- Nie wpływać na execution ani kapitał.
- Rozpocząć zbieranie danych zgodnych z North Star.
- Zakończyć dalsze rozwijanie fundamentu Learning w tym release.

Docelowa sekwencja:

```text
four-environment parity
→ observation collection
→ evidence
→ Replay/PAPER/Shadow validation
→ powrót do Decision Quality
```

## C. RELEASE FREEZE RULES

Do czasu 4/4 parity zabronione są:

- nowe moduły;
- nowe warstwy architektury;
- nowe strategie;
- nowe ORC policies;
- nowe Learning versions poza krytycznym release blockerem;
- zmiany execution;
- zmiany sizing;
- zmiany risk;
- TREATMENT;
- PAPER_EXPERIMENT;
- automatic apply;
- rozszerzenie topologii;
- migracja consolidated bot-runner na profiled workers;
- zmiana release SHA bez decyzji Product Ownera.

Dozwolone są:

- pull-only;
- wdrożenie już zatwierdzonych migracji;
- wdrożenie już zatwierdzonych images;
- poprawa konfiguracji deployment identity;
- parity validation;
- rollback bieżącego komponentu;
- krytyczny fix integralności blokujący release;
- pomiar storage;
- observation-only validation.

## D. FOUR-ENVIRONMENT RELEASE BOARD

| Warstwa                            | LOCAL LIVE   | LOCAL PAPER   | VPS LIVE     | VPS PAPER                    |
| ---------------------------------- | ------------ | ------------- | ------------ | ---------------------------- |
| Git SHA `f4e5e8c`                  | PASS         | PASS          | PASS         | PASS                         |
| Deployment identity                | `local-live` | `local-paper` | `vps-live`   | BLOCKED: missing `vps-paper` |
| Classification V1.1                | PENDING      | PENDING       | PASS         | PENDING                      |
| Frozen Snapshot V2                 | PENDING      | PENDING       | PASS         | PENDING                      |
| Snapshot COMPLETE                  | PENDING      | PENDING       | PASS         | PENDING                      |
| Exact membership                   | PENDING      | PENDING       | PASS         | PENDING                      |
| Automation immutable SHA           | PENDING      | PENDING       | PASS         | PENDING                      |
| ORC git_sha/writer_version         | PENDING      | PENDING       | PASS         | PENDING                      |
| Consolidated bot image             | PENDING      | PENDING       | PENDING      | PENDING                      |
| FinalDecision observation producer | PENDING      | PENDING       | PENDING      | PENDING                      |
| Learning SHADOW                    | PASS         | PASS          | PASS         | PASS                         |
| apply=false                        | PASS         | PASS          | PASS         | PASS                         |
| TREATMENT=0                        | PASS         | PASS          | PASS         | PASS                         |
| PAPER_EXPERIMENT=0                 | PASS         | PASS          | PASS         | PASS                         |
| Execution policy unchanged         | PASS         | PASS          | PASS         | PASS                         |
| Storage healthy                    | PASS         | PASS          | PASS         | PASS                         |
| Final environment acceptance       | PENDING      | PENDING       | PARTIAL PASS | BLOCKED                      |

> Git parity nie oznacza schema, image, runtime ani configuration parity.

## E. CURRENT VERIFIED VPS LIVE STATE

- Git `f4e5e8c`;
- Classification V1.1 PASS;
- Frozen Snapshot V2 PASS;
- feedback run 130;
- snapshot token `92a2c2e8-3313-4c00-b580-2510ff34204b`;
- snapshot status COMPLETE;
- source/eligible `58/58`;
- exact manifests 6;
- BUILDING 0;
- no mismatch;
- retry/hash idempotency PASS;
- automation image `sha256:70fc5cb0777640785abf61a47b997f2823efe85cc5cd3c61dd4a6d7f72d53744`;
- ORC writer `ORC_APPLY_WRITER_V1_3`;
- 28/28 LIVE heartbeats;
- Learning SHADOW/apply=false;
- DB healthy on `/dev/sdb`;
- PAPER untouched.

## F. RELEASE COMPLETION ORDER

```text
1. LOCAL PAPER
2. VPS PAPER identity correction and PAPER rollout
3. LOCAL LIVE
4. Consolidated FinalDecision observation producer activation
   PAPER first, then LIVE
5. Four-environment parity audit
6. Observation phase
```

Nie przechodzimy do następnego środowiska bez PASS poprzedniego etapu.

## G. ENVIRONMENT ACCEPTANCE

Każde środowisko musi mieć:

- exact Git SHA;
- właściwy deployment identity;
- wymagane migracje;
- immutable image metadata;
- snapshot COMPLETE;
- exact membership;
- BUILDING=0;
- FinalDecision observation-only producer;
- healthy DB;
- healthy workers;
- duplicate identities=0;
- Learning SHADOW;
- apply=false;
- TREATMENT=0;
- PAPER_EXPERIMENT=0;
- brak zmian execution/sizing/risk.

## H. OBSERVATION PHASE ENTRY GATE

Observation może rozpocząć się dopiero po 4/4 parity.

Observation obejmuje:

- FinalDecision observations;
- rejected/no-trade decisions;
- executed decisions;
- net PnL after fees;
- MFE;
- MAE;
- giveback;
- fees;
- spread;
- slippage;
- ORC/Slot/Regime context;
- equity impact.

Observation nie może:

- zmieniać `bot_control`;
- aktywować TREATMENT;
- wpływać na execution;
- zmieniać sizing;
- zmieniać risk;
- promować rekomendacji do LIVE.

## I. NEXT NORTH STAR WORK AFTER RELEASE

Po zakończeniu parity i rozpoczęciu observation wracamy do:

```text
Decision Quality
Realtime Engine
Entry Quality
Opportunity Ranking
Rejected Decision Analysis
ORC scoring evidence
Adaptive Exit and Risk
```

Nie wracamy automatycznie do kolejnego Learning V3.

## J. RELEASE CHANGE CONTROL

Każda zmiana statusu musi zawierać:

- timestamp UTC;
- actor;
- environment;
- Git SHA;
- schema state;
- image IDs;
- container IDs tylko w raporcie operacyjnym, nie jako stały kontrakt;
- evidence;
- verdict;
- next allowed action.

Release SHA może się zmienić tylko po:

```text
Product Owner decision
→ Architect scope
→ LOCAL validation
→ new explicit release baseline
```

## K. STOP CONDITIONS

Natychmiastowy STOP przy:

- Learning write wpływającym na LIVE;
- apply=true;
- TREATMENT > 0;
- PAPER_EXPERIMENT > 0 bez autoryzacji;
- duplicate orders/trades/positions;
- DB/storage regression;
- schema mismatch;
- BUILDING residue;
- runtime topology overlap;
- innym Git SHA niż release baseline;
- propozycji nowej architektury podczas stabilization freeze.

## L. RELEASE COMPLETION DEFINITION

Release jest zakończony wyłącznie gdy:

```text
4/4 environments PASS
Git parity PASS
Schema parity PASS
Image parity PASS
Runtime parity PASS
Configuration parity PASS
Learning SHADOW PASS
Observation producer PASS
Execution unchanged PASS
Final parity report committed
```

## NORTH STAR GATE

```text
Equity Curve First
Risk First
PAPER learns, LIVE earns
AI proposes, data proves
Every decision must be replayable and explainable
No automatic LIVE influence without Replay, PAPER and Shadow evidence
No architecture expansion during stabilization
```

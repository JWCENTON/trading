# WalTrade Constitution

STATUS=FROZEN

WalTrade is an **Autonomous Capital Management Platform**. Its primary asset is **CAPITAL** and its primary KPI is **EQUITY CURVE HEALTH**. Trading is a tool; it is not the objective. Every system decision must increase the probability of durable managed-capital growth after all costs, under controlled risk and drawdown.

## Mission

WalTrade exists to make progressively better capital decisions. It must understand the market, its own evidence, portfolio state, risk, execution quality, and the opportunity cost of changing or retaining exposure. Success means repeatable improvement in the portfolio equity curve—not trade count, isolated winners, gross PnL, or headline win rate.

## Hard principles

- CAPITAL FIRST.
- EQUITY CURVE FIRST.
- RISK FIRST.
- NET PNL AFTER ALL COSTS.
- NO TRADE IS A DECISION.
- PAPER LEARNS / EXPERIMENTS.
- LIVE EARNS.
- AI PROPOSES / DATA PROVES.
- RISK BUDGET > CAPITAL ALLOCATION.
- NO MAGIC THRESHOLDS.
- NO HEURISTIC FINANCIAL TRUTH.
- HUMAN APPROVAL BEFORE NEW LIVE INFLUENCE.
- REUSE BEFORE BUILD.
- REWIRE BEFORE REWRITE.
- PORTFOLIO BEFORE STRATEGY.
- ECONOMICS BEFORE INDICATORS.
- EVIDENCE BEFORE AUTHORITY.

Every decision must be reproducible, explainable, auditable, and evaluated after fees, spread, slippage, losses, and execution costs. WalTrade does not fabricate missing fills, fees, timestamps, inventory, or PnL. When authoritative evidence is absent, truth remains `UNKNOWN`, `INCOMPLETE`, or `EXCLUDED`.

## RESEARCH BEFORE INVENT

Before designing a new WalTrade mechanism:

1. Define the economic problem.
2. Check existing WalTrade capability.
3. Check established external methods.
4. Reuse or adapt an established mechanism where appropriate.
5. Prove relevance with WalTrade data.
6. Perform LOCAL PAPER causal validation.
7. Perform VPS PAPER independent validation.
8. Invent something new only if established methods and existing WalTrade capabilities are insufficient.

This principle does not make an external method automatically suitable. External support establishes prior art; WalTrade evidence establishes local relevance and authority.

## Operating environments

| Environment | Role |
| --- | --- |
| LOCAL PAPER | LAB / RESEARCH / EXPERIMENT / EXPLORATION |
| VPS PAPER | INDEPENDENT ACCEPTANCE / PRE-PROD / OUT-OF-SAMPLE |
| VPS LIVE | PRODUCTION |
| LOCAL LIVE | NON-EXPERIMENTAL / FROZEN unless separately approved |

Canonical environment files are mandatory:

- `PAPER=.env.okx.paper`
- `LIVE=.env.okx.live`

Never use `.env`, `.env.paper`, `.env.live`, or an implicit Compose environment.

## Parity invariant

Git SHA alone does not prove deployment parity. Where applicable, acceptance requires all of:

- GIT PARITY
- CONTRACT PARITY
- DIRECT SCHEMA DEPENDENCY PARITY
- RUNTIME / SEMANTIC PARITY

No LOCAL-only or VPS-only schema magic may substitute for a shared, promoted contract. Business data is not copied between LOCAL and VPS to manufacture parity.

## Capital, risk, and allocation

Capital allocation is subordinate to the active Risk Budget. Increased exposure must follow increased evidence quality, not a recent winning streak or an urge to recover losses. Cash is a valid allocation. The system must compare opening new risk with keeping existing risk and doing nothing.

Numeric Risk Budget policy and dynamic Capital Allocation require separate economic proof and Product Owner approval. Their existence in the architecture does not authorize influence today.

## PAPER, LIVE, AI, and Learning authority

PAPER may explore bounded hypotheses. LIVE may receive only evidence-backed, explicitly approved influence after replay, PAPER, independent validation, and a safe promotion path. AI analyzes, compares, explains, and proposes; it does not grant itself financial authority. Learning auto-apply remains off until separately proven and approved.

## Historical truth policy

Open legacy positions must be resolved because they affect current inventory and risk. Recent closed outcomes are repaired only when authoritative evidence supports current Financial Truth, reconciliation, reporting, or Learning. Older unverifiable outcomes are preserved for audit but excluded from trusted performance and Learning. Historical repair exists to improve present capital truth or future decisions—not to make history appear perfect.

## Strategic direction

WalTrade may eventually support multiple asset classes through versioned market-data and execution adapters. Shared intelligence, evidence, replay, risk, and allocation layers should remain asset-class neutral. This direction must not delay completion and proof of the current crypto/OKX system.

## Documentation governance

| Document | Authority and lifecycle |
| --- | --- |
| `WALTRADE_CONSTITUTION.md` | Frozen. Semantic changes require explicit Product Owner approval. |
| `WALTRADE_MASTER_EXECUTION_ROADMAP.md` | Living execution plan ordered by evidence and dependency. |
| `WALTRADE_DAILY_STATUS.md` | Current truth only; stale state is replaced, not accumulated. |
| `STOP_LOSING_MASTER_CHECKLIST.md` | Living evidence and completion tracker. |
| `EXTERNAL_EVIDENCE_CATALOG.md` | Known-method and do-not-reinvent reference. |
| `IDEA_BACKLOG.md` | Unapproved hypotheses and ideas; not roadmap authority. |

Git history provides historical versions. Do not create timestamped or numbered duplicates.

## Constitutional authority

This constitution defines why WalTrade exists and the rules under which it evolves. Roadmaps, implementations, thresholds, models, and individual experiments may change. The North Star does not. Any semantic change to this document after its initial canonical promotion requires explicit Product Owner approval.

# WalTrade Idea Backlog

This file preserves explicitly supplied Product Owner / Architect ideas that are not approved roadmap scope. A backlog entry does not imply implementation.

Promotion from BACKLOG to ROADMAP requires:

- economic problem defined;
- existing capability checked;
- established external method checked;
- WalTrade evidence collected;
- explicit Product Owner decision.

## Backlog entry template

IDEA_ID=
DATE=
IDEA=
PROBLEM=
WHY_IT_MIGHT_HELP=
KNOWN_METHOD_CHECK=
WALTRADE_DATA_NEEDED=
CHEAPEST_TEST=
NORTH_STAR_ALIGNMENT=
STATUS=BACKLOG

## IDEA-MULTI-ASSET-ADAPTERS

IDEA_ID=IDEA-MULTI-ASSET-ADAPTERS
DATE=2026-08-28
IDEA=Extend WalTrade through versioned adapters to additional asset classes such as FX, equities, ETFs, commodities/metals, and futures.
PROBLEM=The long-term capital-management platform should be able to compare opportunities across more than crypto without coupling shared intelligence to one venue or asset class.
WHY_IT_MIGHT_HELP=Broader opportunity sets may improve capital utilization and diversification.
KNOWN_METHOD_CHECK=Multi-asset portfolio construction and adapter-based market/execution isolation are established concepts; exact sources remain to be curated.
WALTRADE_DATA_NEEDED=Completed and economically proven crypto/OKX system, asset-specific market/execution requirements, portfolio correlation, liquidity, cost, and risk evidence.
CHEAPEST_TEST=Documentation and interface-boundary review only after crypto/OKX completion; no adapter implementation now.
NORTH_STAR_ALIGNMENT=Aligned strategically, but must not delay proof of the current system.
STATUS=BACKLOG

## Owner-supplied ideas — 2026-09-13

Research ideas below remain BACKLOG / NOT_APPROVED_FOR_IMPLEMENTATION;
UI_WARSAW_TIME has the explicitly scoped operator-reported deployment status below. Before
designing any experiment, inspect existing WalTrade mechanisms and established
earlier methods; reuse before build. No parameter, treatment, LIVE/PAPER change
or research execution is authorized by listing an idea.

| IDEA_ID | Idea / economic question | Existing-mechanism and prior-method check required | Evidence needed before proposing a test |
| --- | --- | --- | --- |
| DYNAMIC_ENTRY_NOTIONAL | Owner clarification 2026-09-14: future PAPER test of dynamic TREND entry notional versus FIXED 9; proposed minimum 9 USDC plus fee is a hypothesis, not an active rule | First inspect existing sizing, exchange-minimum rounding, reconciled cash/reserves, portfolio/correlation risk and prior methods; only then a separate experiment plan | Compare NET, drawdown, capital-hours and missed opportunities. Larger sizing would require signal quality, verified news/regime context and portfolio capacity; insufficient cash or failed gates may still mean no entry. No maximum, multiplier or algorithm selected |
| RECOVERY_ENTRY_SELECTION | Better selection of Recovery entries without conflating exit policy and entry quality | Existing regime/context/admission evidence and prior precision-first selection research | All actual OPEN/CLOSED entries, causal input availability, common 15/60/240-minute outcomes and false rejection of winners |
| NEWS_UNKNOWN_COVERAGE | Owner clarification 2026-09-14: candidate future VPS PAPER Recovery test of relevant macro/regulatory news, including Fed/CPI and CLARITY | First inspect existing ingestion, attribution, TTL, coverage/veto semantics and prior methods. VPS operator reports supplied by the owner say some material was fetched but assigned to no coin; not independently verified by LOCAL | Assess coverage, correctness, availability time, decision impact, avoided losses and missed good opportunities. Preserve source/published/observed provenance; UNKNOWN is not benign news. Binance may be comparative material, not source of truth or an automatic trading instruction |
| POSITION_AGE_CAPITAL_DEMAND | Observe position age, maximum exposure and cash demand | Existing portfolio/owner-flow ledgers, age and capital-utilization reporting; established liquidity/occupancy analysis | Holding-time distribution, simultaneous exposure, minimum free cash and rejected opportunities; owner flows separate from trading PnL |
| UI_WARSAW_TIME | 2026-09-14 operator-reported DEPLOYED on VPS PAPER/LIVE: Europe/Warsaw UI presentation, UTC data/calculations; LOCAL deployment NOT_ASSERTED | Existing frontend timezone formatting and IANA/DST handling; operator evidence supplied by owner, not an independent LOCAL read | Retain timestamp provenance and DST checks. Full production browser-interaction acceptance remains incomplete; no new LOCAL implementation authority |

The two clarified ideas remain BACKLOG / NOT_APPROVED_FOR_IMPLEMENTATION.
Do not combine a news change and a sizing change in an initial experiment.
Any proposed larger TREND entry must respect reconciled cash, reserves, fees,
exchange constraints and aggregate portfolio risk, including correlation.
A positive news item or the last winning trade alone cannot justify a larger
position. The proposed 9 USDC minimum does not override no-entry decisions.
LOCAL V4 and all LIVE behavior remain unchanged; these notes grant no trading
or implementation authority in any environment.

## Governance

Ideas remain here until the promotion gate is satisfied. Do not create implementation tasks, schemas, engines, services, or runtime experiments directly from a backlog entry. Git history records additions, removals, and promotion decisions; do not create numbered backlog copies.

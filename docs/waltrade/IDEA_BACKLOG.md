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

All five ideas below are BACKLOG / NOT_APPROVED_FOR_IMPLEMENTATION. Before
designing any experiment, inspect existing WalTrade mechanisms and established
earlier methods; reuse before build. No parameter, treatment, LIVE/PAPER change
or research execution is authorized by listing an idea.

| IDEA_ID | Idea / economic question | Existing-mechanism and prior-method check required | Evidence needed before proposing a test |
| --- | --- | --- | --- |
| DYNAMIC_ENTRY_NOTIONAL | Dynamic entry notional versus current FIXED 9: capital efficiency under risk constraints | Existing sizing, exchange-minimum rounding, sleeves/cash guards; established position-sizing methods | Fee-aware outcomes normalized per allocated USDC, capacity, drawdown and liquidity; no size selected |
| RECOVERY_ENTRY_SELECTION | Better selection of Recovery entries without conflating exit policy and entry quality | Existing regime/context/admission evidence and prior precision-first selection research | All actual OPEN/CLOSED entries, causal input availability, common 15/60/240-minute outcomes and false rejection of winners |
| NEWS_UNKNOWN_COVERAGE | Effect of UNKNOWN news on entry availability and quality | Existing news TTL, coverage labels and veto semantics; established missing-data/coverage analysis | Source/published/observed timestamps, explicit no-coverage versus no-veto, decisions and mature outcomes; UNKNOWN is not benign news |
| POSITION_AGE_CAPITAL_DEMAND | Observe position age, maximum exposure and cash demand | Existing portfolio/owner-flow ledgers, age and capital-utilization reporting; established liquidity/occupancy analysis | Holding-time distribution, simultaneous exposure, minimum free cash and rejected opportunities; owner flows separate from trading PnL |
| UI_WARSAW_TIME | Display UI time in Europe/Warsaw while retaining UTC in stored data/calculations | Existing frontend timezone formatting and established IANA/DST handling | Timestamp provenance, UTC/PL examples including DST boundaries; presentation-only proposal, not a timestamp rewrite |

## Governance

Ideas remain here until the promotion gate is satisfied. Do not create implementation tasks, schemas, engines, services, or runtime experiments directly from a backlog entry. Git history records additions, removals, and promotion decisions; do not create numbered backlog copies.

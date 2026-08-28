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

## Governance

Ideas remain here until the promotion gate is satisfied. Do not create implementation tasks, schemas, engines, services, or runtime experiments directly from a backlog entry. Git history records additions, removals, and promotion decisions; do not create numbered backlog copies.

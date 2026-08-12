"""Forward-only canonical market-regime identity for PAPER decisions."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable


CANONICAL_REGIME_ATTRIBUTION_VERSION = "CANONICAL_REGIME_ATTRIBUTION_V1"
CANONICAL_REGIME_SOURCE = "market_regime"


@dataclass(frozen=True)
class CanonicalRegimeSnapshot:
    symbol: str
    interval: str
    ts: datetime
    regime: str
    created_at: datetime
    confidence: Decimal | None

    def provenance(self) -> dict[str, Any]:
        return {
            "regime_attribution_version": CANONICAL_REGIME_ATTRIBUTION_VERSION,
            "regime_source": CANONICAL_REGIME_SOURCE,
            "regime_source_symbol": self.symbol,
            "regime_source_interval": self.interval,
            "regime_source_ts": self.ts,
            "regime_source_created_at": self.created_at,
            "regime_source_confidence": self.confidence,
        }


def no_regime_provenance(*, symbol: str, interval: str) -> dict[str, Any]:
    """Explicitly record that the exact bounded source lookup found no row."""
    return {
        "regime_attribution_version": CANONICAL_REGIME_ATTRIBUTION_VERSION,
        "regime_source": CANONICAL_REGIME_SOURCE,
        "regime_source_symbol": str(symbol).upper(),
        "regime_source_interval": str(interval).lower(),
        "regime_source_ts": None,
        "regime_source_created_at": None,
        "regime_source_confidence": None,
    }


def load_canonical_regime_at_decision(
    connection_factory: Callable[[], Any],
    *,
    symbol: str,
    interval: str,
    decision_candle_timestamp: datetime,
    logger: logging.Logger | None = None,
) -> CanonicalRegimeSnapshot | None:
    """Read exactly the latest persisted row at or before the decision candle."""
    conn = None
    cur = None
    try:
        conn = connection_factory()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol,interval,ts,regime,created_at,confidence
            FROM market_regime
            WHERE symbol=%s AND interval=%s AND ts<=%s
              AND regime IS NOT NULL
            ORDER BY ts DESC
            LIMIT 1
            """,
            (
                str(symbol).upper(),
                str(interval).lower(),
                decision_candle_timestamp,
            ),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return CanonicalRegimeSnapshot(
            symbol=str(row[0]).upper(),
            interval=str(row[1]).lower(),
            ts=row[2],
            regime=str(row[3]),
            created_at=row[4],
            confidence=(None if row[5] is None else Decimal(str(row[5]))),
        )
    except Exception:
        (logger or logging.getLogger(__name__)).exception(
            "CANONICAL_REGIME_ATTRIBUTION_LOOKUP_FAILED",
            extra={
                "symbol": symbol,
                "interval": interval,
                "decision_candle_timestamp": decision_candle_timestamp,
            },
        )
        return None
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def evaluation_regime_fields(
    connection_factory: Callable[[], Any],
    *,
    symbol: str,
    interval: str,
    decision_candle_timestamp: datetime,
    paper_mode: bool,
    logger: logging.Logger | None = None,
) -> tuple[str | None, Decimal | None, dict[str, Any]]:
    """Return frozen PAPER fields while leaving the LIVE context untouched."""
    if not paper_mode:
        return None, None, {}
    snapshot = load_canonical_regime_at_decision(
        connection_factory,
        symbol=symbol,
        interval=interval,
        decision_candle_timestamp=decision_candle_timestamp,
        logger=logger,
    )
    if snapshot is None:
        return None, None, no_regime_provenance(symbol=symbol, interval=interval)
    return snapshot.regime, snapshot.confidence, snapshot.provenance()


def frozen_regime_provenance(evaluation) -> dict[str, Any]:
    """Copy only the frozen canonical provenance from an EvaluationContext."""
    keys = (
        "regime_attribution_version",
        "regime_source",
        "regime_source_symbol",
        "regime_source_interval",
        "regime_source_ts",
        "regime_source_created_at",
        "regime_source_confidence",
    )
    values = {key: evaluation.context.get(key) for key in keys}
    for key, value in tuple(values.items()):
        if isinstance(value, datetime):
            values[key] = value.isoformat()
        elif isinstance(value, Decimal):
            values[key] = format(value, "f")
    return values

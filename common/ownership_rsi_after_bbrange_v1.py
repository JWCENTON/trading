"""Frozen PAPER-only RSI-after-BBRANGE ownership admission candidate."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
from typing import Callable


MODE_ENV = "OWNERSHIP_RSI_AFTER_BBRANGE_V1_MODE"
ALLOWED_MODES = {"OFF", "CONTROL", "TREATMENT"}
TREATMENT_NAME = "OWNERSHIP_RSI_AFTER_BBRANGE_V1"
BLOCK_REASON = "STOP_LOSING_OWNERSHIP_RSI_AFTER_BBRANGE"
CONTROL_REASON = BLOCK_REASON + "_CONTROL_OBSERVED"


@dataclass(frozen=True)
class OwnershipAdmissionDecision:
    mode: str
    effective: bool
    affected: bool
    blocked: bool
    reason: str
    evidence_status: str
    open_bbrange_positions: tuple[dict, ...]
    market_regime: str | None
    observed_at: datetime

    def details(self) -> dict:
        return {
            "contract_version": "OWNERSHIP_RSI_AFTER_BBRANGE_V1",
            "treatment_name": TREATMENT_NAME,
            "treatment_status": self.mode,
            "treatment_effective": self.effective,
            "base_decision": "BUY",
            "treatment_decision": "NO_TRADE" if self.blocked else "BUY",
            "treatment_reason": self.reason,
            "ownership_affected": self.affected,
            "ownership_blocked": self.blocked,
            "evidence_status": self.evidence_status,
            "open_bbrange_positions": list(self.open_bbrange_positions),
            "evaluation_context_market_regime": self.market_regime,
            "observed_at": self.observed_at,
        }


def configured_mode(environ: dict[str, str] | None = None) -> str:
    values = os.environ if environ is None else environ
    mode = str(values.get(MODE_ENV, "OFF")).strip().upper()
    if mode not in ALLOWED_MODES:
        raise RuntimeError(f"INVALID_{MODE_ENV}={mode}")
    return mode


def evaluate_ownership_admission(
    connection_factory: Callable,
    *,
    trading_mode: str,
    symbol: str,
    strategy: str,
    observed_at: datetime,
    market_regime: str | None,
    environ: dict[str, str] | None = None,
) -> OwnershipAdmissionDecision:
    mode = configured_mode(environ)
    timestamp = observed_at
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    is_paper_rsi = (
        str(trading_mode).strip().upper() == "PAPER"
        and str(strategy).strip().upper() == "RSI"
    )
    if mode == "OFF" or not is_paper_rsi:
        reason = "TREATMENT_OFF" if mode == "OFF" else "NOT_APPLICABLE"
        return OwnershipAdmissionDecision(
            mode=mode,
            effective=False,
            affected=False,
            blocked=False,
            reason=reason,
            evidence_status="NOT_READ",
            open_bbrange_positions=(),
            market_regime=market_regime,
            observed_at=timestamp,
        )

    conn = connection_factory()
    try:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, interval, remaining_inventory_qty
                FROM positions
                WHERE symbol=%s
                  AND strategy='BBRANGE'
                  AND status='OPEN'
                  AND remaining_inventory_qty>0
                ORDER BY id
                """,
                (symbol,),
            )
            rows = cur.fetchall()
        conn.rollback()
    finally:
        conn.close()

    positions = tuple(
        {
            "position_id": int(row[0]),
            "interval": str(row[1]),
            "remaining_inventory_qty": str(row[2]),
        }
        for row in rows
    )
    affected = bool(positions)
    blocked = affected and mode == "TREATMENT"
    return OwnershipAdmissionDecision(
        mode=mode,
        effective=mode == "TREATMENT",
        affected=affected,
        blocked=blocked,
        reason=(BLOCK_REASON if blocked else CONTROL_REASON if affected else "NO_OPEN_BBRANGE"),
        evidence_status="AVAILABLE",
        open_bbrange_positions=positions,
        market_regime=market_regime,
        observed_at=timestamp,
    )

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping


class PositionLifecycle(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class FinancialTruthLifecycle(str, Enum):
    UNKNOWN = "UNKNOWN"
    INCOMPLETE = "INCOMPLETE"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


@dataclass(frozen=True)
class FinancialTruthEvidence:
    executed_entry_qty: Decimal | None = None
    executed_exit_qty: Decimal | None = None
    remaining_qty: Decimal | None = None
    authoritative_entry_fees_usdc: Decimal | None = None
    authoritative_exit_fees_usdc: Decimal | None = None
    authoritative_gross_pnl: Decimal | None = None
    authoritative_net_pnl: Decimal | None = None
    authoritative_source: str | None = None
    authoritative_evidence: Mapping[str, Any] | None = None


def complete_evidence_missing(evidence: FinancialTruthEvidence) -> tuple[str, ...]:
    required = {
        "executed_entry_qty": evidence.executed_entry_qty,
        "executed_exit_qty": evidence.executed_exit_qty,
        "remaining_qty": evidence.remaining_qty,
        "authoritative_entry_fees_usdc": evidence.authoritative_entry_fees_usdc,
        "authoritative_exit_fees_usdc": evidence.authoritative_exit_fees_usdc,
        "authoritative_gross_pnl": evidence.authoritative_gross_pnl,
        "authoritative_net_pnl": evidence.authoritative_net_pnl,
        "authoritative_source": evidence.authoritative_source,
        "authoritative_evidence": evidence.authoritative_evidence,
    }
    return tuple(
        name for name, value in required.items()
        if value is None or value == "" or value == {}
    )


def validate_financial_truth(
    status: FinancialTruthLifecycle,
    evidence: FinancialTruthEvidence,
) -> None:
    quantities = (
        evidence.executed_entry_qty,
        evidence.executed_exit_qty,
        evidence.remaining_qty,
    )
    if any(value is not None and value < 0 for value in quantities):
        raise ValueError("canonical quantities must be non-negative")

    if status is FinancialTruthLifecycle.COMPLETE:
        missing = complete_evidence_missing(evidence)
        if missing:
            raise ValueError(
                "COMPLETE requires authoritative evidence: " + ", ".join(missing)
            )


def financial_truth_api_values(
    *,
    authoritative_gross_pnl: Decimal | None,
    authoritative_net_pnl: Decimal | None,
    estimated_gross_pnl: Decimal | None,
    estimated_net_pnl: Decimal | None,
) -> dict[str, Decimal | None]:
    """Keep authoritative and estimated values separate at the API boundary."""
    return {
        "authoritative_gross_pnl": authoritative_gross_pnl,
        "authoritative_net_pnl": authoritative_net_pnl,
        "estimated_gross_pnl": estimated_gross_pnl,
        "estimated_net_pnl": estimated_net_pnl,
    }
